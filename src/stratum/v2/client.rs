use super::{CodecError, Message, PROTOCOL, frame::next_frame, unhex};
use crate::stratum::protocol::{ClientMessage, JobNotice, ServerMessage, SubscribeResult};
use bytes::BytesMut;
use serde_json::Value;
use std::collections::{BTreeMap, VecDeque};
use tokio_util::codec::{Decoder, Encoder};

const MAX_OUTSTANDING: usize = 4_096;

#[derive(Clone, Debug, PartialEq)]
pub enum ClientEvent {
    Message(ServerMessage),
    /// The server asks the miner to connect again; empty host and port 0 mean the same server.
    Reconnect {
        host: String,
        port: u16,
    },
}

/// The miner's side of [`super::ServerCodec`]: V1-shaped requests out, V1-shaped answers in.
#[derive(Debug, Default)]
pub struct ClientCodec {
    subscribe: Value,
    authorize: Option<(u32, Value)>,
    channel: Option<u32>,
    outstanding: BTreeMap<u32, Value>,
    ready: VecDeque<ClientEvent>,
    single_block: Option<[u8; 48]>,
}

impl Encoder<ClientMessage> for ClientCodec {
    type Error = CodecError;

    fn encode(&mut self, message: ClientMessage, dst: &mut BytesMut) -> Result<(), CodecError> {
        let message = match message {
            ClientMessage::Subscribe {
                id,
                agent,
                tag,
                pow_algorithm_id,
                spec_revision,
            } => {
                self.subscribe = id;
                Message::SetupConnection {
                    agent,
                    tag,
                    pow_algorithm_id,
                    spec_revision,
                }
            }
            ClientMessage::Authorize {
                id,
                login,
                password,
            } => {
                let request_id = small(&id, "channel request id")?;
                self.authorize = Some((request_id, id));
                Message::OpenChannel {
                    request_id,
                    login,
                    password,
                }
            }
            ClientMessage::Submit {
                id, job_id, nonce, ..
            } => {
                let channel_id = self
                    .channel
                    .ok_or(CodecError::Malformed("share before the channel opened"))?;
                let sequence_number = small(&id, "share id")?;
                if self.outstanding.len() >= MAX_OUTSTANDING {
                    self.outstanding.pop_first();
                }
                self.outstanding.insert(sequence_number, id);
                Message::SubmitShare {
                    channel_id,
                    sequence_number,
                    job_id: unhex(&job_id, "job id")?,
                    nonce: unhex(&nonce, "nonce")?,
                }
            }
            ClientMessage::Unknown { .. } => return Ok(()),
        };
        message.encode(dst)
    }
}

impl Decoder for ClientCodec {
    type Item = ClientEvent;
    type Error = CodecError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<ClientEvent>, CodecError> {
        loop {
            if let Some(event) = self.ready.pop_front() {
                return Ok(Some(event));
            }
            let Some(frame) = next_frame(src)? else {
                return Ok(None);
            };
            if let Some(message) = Message::decode(&frame)? {
                self.inbound(message);
            }
        }
    }
}

impl ClientCodec {
    fn inbound(&mut self, message: Message) {
        match message {
            Message::SetupConnectionSuccess {
                nonce_prefix,
                pool_address,
                pow_algorithm_id,
                spec_revision,
                chain_id,
            } => {
                let result = SubscribeResult {
                    protocol: PROTOCOL.to_owned(),
                    nonce_prefix: hex::encode(nonce_prefix),
                    pool_address,
                    pow_algorithm_id,
                    spec_revision,
                    chain_id: hex::encode(chain_id),
                };
                let result = serde_json::to_value(result).expect("a subscribe result is JSON");
                self.answer(self.subscribe.clone(), Ok(result));
            }
            Message::SetupConnectionError { code, message } => {
                self.answer(self.subscribe.clone(), Err((code.into(), message)));
            }
            Message::OpenChannelSuccess {
                request_id,
                channel_id,
            } => {
                if let Some(id) = self.request(request_id) {
                    self.channel = Some(channel_id);
                    self.answer(id, Ok(Value::Bool(true)));
                }
            }
            Message::OpenChannelError {
                request_id,
                code,
                message,
            } => {
                if let Some(id) = self.request(request_id) {
                    self.answer(id, Err((code.into(), message)));
                }
            }
            Message::SingleBlockJob { channel_id, job_id } if self.channel == Some(channel_id) => {
                self.single_block = Some(job_id);
            }
            Message::NewJob(job) if self.channel == Some(job.channel_id) => {
                let single_block = self.single_block.take() == Some(job.job_id);
                self.ready
                    .push_back(ClientEvent::Message(ServerMessage::Notify(JobNotice {
                        job_id: hex::encode(job.job_id),
                        header: hex::encode(job.header),
                        share_target: hex::encode(job.share_target),
                        target: hex::encode(job.target),
                        registration_height: job.registration_height,
                        network: job.network,
                        clean: job.clean,
                        single_block,
                    })));
            }
            Message::SetTarget {
                channel_id,
                share_target,
            } if self.channel == Some(channel_id) => {
                self.ready
                    .push_back(ClientEvent::Message(ServerMessage::SetTarget(hex::encode(
                        share_target,
                    ))));
            }
            Message::SubmitSharesSuccess {
                channel_id,
                last_sequence_number,
                ..
            } if self.channel == Some(channel_id) => {
                let accepted: Vec<_> = self
                    .outstanding
                    .range(..=last_sequence_number)
                    .map(|(sequence, _)| *sequence)
                    .collect();
                for sequence in accepted {
                    if let Some(id) = self.outstanding.remove(&sequence) {
                        self.answer(id, Ok(Value::Bool(true)));
                    }
                }
            }
            Message::SubmitSharesError {
                channel_id,
                sequence_number,
                code,
                message,
            } if self.channel == Some(channel_id) => {
                if let Some(id) = self.outstanding.remove(&sequence_number) {
                    self.answer(id, Err((code.into(), message)));
                }
            }
            Message::Reconnect { host, port } => {
                self.ready.push_back(ClientEvent::Reconnect { host, port });
            }
            _ => {}
        }
    }

    fn request(&mut self, request_id: u32) -> Option<Value> {
        match self.authorize.take() {
            Some((expected, id)) if expected == request_id => Some(id),
            other => {
                self.authorize = other;
                None
            }
        }
    }

    fn answer(&mut self, id: Value, outcome: Result<Value, (i64, String)>) {
        self.ready
            .push_back(ClientEvent::Message(ServerMessage::Response {
                id,
                outcome,
            }));
    }
}

fn small(id: &Value, what: &'static str) -> Result<u32, CodecError> {
    id.as_u64()
        .and_then(|value| u32::try_from(value).ok())
        .ok_or(CodecError::Malformed(what))
}
