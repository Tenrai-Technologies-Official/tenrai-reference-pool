use super::{CHANNEL, CodecError, Job, Message, frame::next_frame, unhex};
use crate::stratum::protocol::{ClientMessage, RejectReason, ServerMessage, SubscribeResult};
use bytes::BytesMut;
use serde_json::Value;
use std::collections::VecDeque;
use tokio_util::codec::{Decoder, Encoder};

const SETUP_ID: &str = "setup";
const OPEN_ID: &str = "open";
const UNSUPPORTED: u16 = RejectReason::Unsupported.code() as u16;

/// Translates V2 frames into V1 mining messages and the answers back into frames.
#[derive(Debug, Default)]
pub struct ServerCodec {
    setup: bool,
    opens: VecDeque<u32>,
    channel: Option<u32>,
}

impl Decoder for ServerCodec {
    type Item = ClientMessage;
    type Error = CodecError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<ClientMessage>, CodecError> {
        let Some(frame) = next_frame(src)? else {
            return Ok(None);
        };
        let message = Message::decode(&frame)?.ok_or(CodecError::Malformed("message type"))?;
        self.inbound(message).map(Some)
    }
}

impl Encoder<ServerMessage> for ServerCodec {
    type Error = CodecError;

    fn encode(&mut self, message: ServerMessage, dst: &mut BytesMut) -> Result<(), CodecError> {
        if let ServerMessage::Notify(job) = &message
            && job.single_block
        {
            Message::SingleBlockJob {
                channel_id: CHANNEL,
                job_id: unhex(&job.job_id, "job id")?,
            }
            .encode(dst)?;
        }
        match self.outbound(message)? {
            Some(message) => message.encode(dst),
            None => Ok(()),
        }
    }
}

impl Encoder<Message> for ServerCodec {
    type Error = CodecError;

    fn encode(&mut self, message: Message, dst: &mut BytesMut) -> Result<(), CodecError> {
        message.encode(dst)
    }
}

impl ServerCodec {
    fn inbound(&mut self, message: Message) -> Result<ClientMessage, CodecError> {
        match message {
            Message::SetupConnection {
                agent,
                tag,
                pow_algorithm_id,
                spec_revision,
            } if !self.setup => {
                self.setup = true;
                Ok(ClientMessage::Subscribe {
                    id: Value::from(SETUP_ID),
                    agent,
                    tag,
                    pow_algorithm_id,
                    spec_revision,
                })
            }
            Message::SetupConnection { .. } => {
                Err(CodecError::Malformed("second connection setup"))
            }
            Message::OpenChannel {
                request_id,
                login,
                password,
            } if self.setup => {
                self.opens.push_back(request_id);
                Ok(ClientMessage::Authorize {
                    id: Value::from(OPEN_ID),
                    login,
                    password,
                })
            }
            Message::OpenChannel { .. } => Err(CodecError::Malformed("channel before the setup")),
            Message::SubmitShare {
                channel_id,
                sequence_number,
                job_id,
                nonce,
            } if self.channel == Some(channel_id) => Ok(ClientMessage::Submit {
                id: Value::from(sequence_number),
                job_id: hex::encode(job_id),
                nonce: hex::encode(nonce),
            }),
            Message::SubmitShare { .. } => Err(CodecError::Malformed("share outside the channel")),
            _ => Err(CodecError::Malformed("message for the miner")),
        }
    }

    fn outbound(&mut self, message: ServerMessage) -> Result<Option<Message>, CodecError> {
        let channel_id = CHANNEL;
        Ok(Some(match message {
            ServerMessage::Response { id, outcome } => match (id.as_str(), id.as_u64()) {
                (Some(SETUP_ID), _) => match outcome {
                    Ok(result) => {
                        let result: SubscribeResult = serde_json::from_value(result)
                            .map_err(|_| CodecError::Malformed("connection setup answer"))?;
                        Message::SetupConnectionSuccess {
                            nonce_prefix: hex::decode(&result.nonce_prefix)
                                .map_err(|_| CodecError::Malformed("nonce prefix"))?,
                            pool_address: result.pool_address,
                            pow_algorithm_id: result.pow_algorithm_id,
                            spec_revision: result.spec_revision,
                            chain_id: unhex(&result.chain_id, "chain id")?,
                        }
                    }
                    Err((code, message)) => Message::SetupConnectionError {
                        code: code_of(code),
                        message,
                    },
                },
                (Some(OPEN_ID), _) => {
                    let request_id = self
                        .opens
                        .pop_front()
                        .ok_or(CodecError::Malformed("channel answer"))?;
                    match refusal(outcome) {
                        None => {
                            self.channel = Some(channel_id);
                            Message::OpenChannelSuccess {
                                request_id,
                                channel_id,
                            }
                        }
                        Some((code, message)) => Message::OpenChannelError {
                            request_id,
                            code,
                            message,
                        },
                    }
                }
                (_, Some(sequence)) => {
                    let sequence_number =
                        u32::try_from(sequence).map_err(|_| CodecError::Malformed("share id"))?;
                    match refusal(outcome) {
                        None => Message::SubmitSharesSuccess {
                            channel_id,
                            last_sequence_number: sequence_number,
                            accepted: 1,
                        },
                        Some((code, message)) => Message::SubmitSharesError {
                            channel_id,
                            sequence_number,
                            code,
                            message,
                        },
                    }
                }
                _ => return Ok(None),
            },
            ServerMessage::Notify(job) => Message::NewJob(Job {
                channel_id,
                job_id: unhex(&job.job_id, "job id")?,
                header: hex::decode(&job.header).map_err(|_| CodecError::Malformed("header"))?,
                share_target: unhex(&job.share_target, "share target")?,
                target: unhex(&job.target, "target")?,
                registration_height: job.registration_height,
                network: job.network,
                clean: job.clean,
            }),
            ServerMessage::SetTarget(target) => Message::SetTarget {
                channel_id,
                share_target: unhex(&target, "share target")?,
            },
        }))
    }
}

/// `true` is the only success; any other answer goes out as an error.
fn refusal(outcome: Result<Value, (i64, String)>) -> Option<(u16, String)> {
    match outcome {
        Ok(Value::Bool(true)) => None,
        Ok(other) => Some((UNSUPPORTED, other.to_string())),
        Err((code, message)) => Some((code_of(code), message)),
    }
}

fn code_of(code: i64) -> u16 {
    u16::try_from(code).unwrap_or(UNSUPPORTED)
}
