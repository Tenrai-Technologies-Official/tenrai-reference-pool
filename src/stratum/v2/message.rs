use super::{
    CodecError,
    fields::{Reader, Writer},
    frame::{Frame, write_frame},
};
use bytes::BytesMut;

const SETUP_CONNECTION: u8 = 0x00;
const SETUP_CONNECTION_SUCCESS: u8 = 0x01;
const SETUP_CONNECTION_ERROR: u8 = 0x02;
const RECONNECT: u8 = 0x04;
const OPEN_CHANNEL: u8 = 0x10;
const OPEN_CHANNEL_SUCCESS: u8 = 0x11;
const OPEN_CHANNEL_ERROR: u8 = 0x12;
const NEW_JOB: u8 = 0x15;
const SINGLE_BLOCK_JOB: u8 = 0x16;
const SUBMIT_SHARE: u8 = 0x1a;
const SUBMIT_SHARES_SUCCESS: u8 = 0x1c;
const SUBMIT_SHARES_ERROR: u8 = 0x1d;
const SET_TARGET: u8 = 0x21;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Message {
    SetupConnection {
        agent: String,
        tag: String,
        pow_algorithm_id: u8,
        spec_revision: u32,
    },
    SetupConnectionSuccess {
        nonce_prefix: Vec<u8>,
        /// Empty unless the server is a pool whose blocks pay this address.
        pool_address: String,
        pow_algorithm_id: u8,
        spec_revision: u32,
        chain_id: [u8; 48],
    },
    SetupConnectionError {
        code: u16,
        message: String,
    },
    /// Empty host and port 0 mean the same server.
    Reconnect {
        host: String,
        port: u16,
    },
    OpenChannel {
        request_id: u32,
        login: String,
        password: String,
    },
    OpenChannelSuccess {
        request_id: u32,
        channel_id: u32,
    },
    OpenChannelError {
        request_id: u32,
        code: u16,
        message: String,
    },
    NewJob(Job),
    /// Sent before the job it names; older miners skip the unknown type.
    SingleBlockJob {
        channel_id: u32,
        job_id: [u8; 48],
    },
    SubmitShare {
        channel_id: u32,
        sequence_number: u32,
        job_id: [u8; 48],
        nonce: [u8; 32],
    },
    /// Accepts every unanswered share up to `last_sequence_number`.
    SubmitSharesSuccess {
        channel_id: u32,
        last_sequence_number: u32,
        accepted: u32,
    },
    SubmitSharesError {
        channel_id: u32,
        sequence_number: u32,
        code: u16,
        message: String,
    },
    SetTarget {
        channel_id: u32,
        share_target: [u8; 32],
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Job {
    pub channel_id: u32,
    pub job_id: [u8; 48],
    pub header: Vec<u8>,
    pub share_target: [u8; 32],
    pub target: [u8; 32],
    pub registration_height: u64,
    pub network: String,
    pub clean: bool,
}

impl Message {
    const fn kind(&self) -> (u8, bool) {
        match self {
            Self::SetupConnection { .. } => (SETUP_CONNECTION, false),
            Self::SetupConnectionSuccess { .. } => (SETUP_CONNECTION_SUCCESS, false),
            Self::SetupConnectionError { .. } => (SETUP_CONNECTION_ERROR, false),
            Self::Reconnect { .. } => (RECONNECT, false),
            Self::OpenChannel { .. } => (OPEN_CHANNEL, false),
            Self::OpenChannelSuccess { .. } => (OPEN_CHANNEL_SUCCESS, false),
            Self::OpenChannelError { .. } => (OPEN_CHANNEL_ERROR, false),
            Self::NewJob(_) => (NEW_JOB, true),
            Self::SingleBlockJob { .. } => (SINGLE_BLOCK_JOB, true),
            Self::SubmitShare { .. } => (SUBMIT_SHARE, true),
            Self::SubmitSharesSuccess { .. } => (SUBMIT_SHARES_SUCCESS, true),
            Self::SubmitSharesError { .. } => (SUBMIT_SHARES_ERROR, true),
            Self::SetTarget { .. } => (SET_TARGET, true),
        }
    }

    pub fn encode(&self, dst: &mut BytesMut) -> Result<(), CodecError> {
        let mut out = Writer::default();
        match self {
            Self::SetupConnection {
                agent,
                tag,
                pow_algorithm_id,
                spec_revision,
            } => {
                out.str255(agent, "agent")?
                    .str255(tag, "tag")?
                    .array(&[*pow_algorithm_id])
                    .u32(*spec_revision);
            }
            Self::SetupConnectionSuccess {
                nonce_prefix,
                pool_address,
                pow_algorithm_id,
                spec_revision,
                chain_id,
            } => {
                out.bytes255(nonce_prefix, "nonce prefix")?
                    .str255(pool_address, "pool address")?
                    .array(&[*pow_algorithm_id])
                    .u32(*spec_revision)
                    .array(chain_id);
            }
            Self::SetupConnectionError { code, message } => {
                out.u16(*code).str255(message, "error message")?;
            }
            Self::Reconnect { host, port } => {
                out.str255(host, "host")?.u16(*port);
            }
            Self::OpenChannel {
                request_id,
                login,
                password,
            } => {
                out.u32(*request_id)
                    .str255(login, "login")?
                    .str255(password, "password")?;
            }
            Self::OpenChannelSuccess {
                request_id,
                channel_id,
            } => {
                out.u32(*request_id).u32(*channel_id);
            }
            Self::OpenChannelError {
                request_id,
                code,
                message,
            } => {
                out.u32(*request_id)
                    .u16(*code)
                    .str255(message, "error message")?;
            }
            Self::NewJob(job) => {
                out.u32(job.channel_id)
                    .array(&job.job_id)
                    .bytes64k(&job.header, "header")?
                    .array(&job.share_target)
                    .array(&job.target)
                    .u64(job.registration_height)
                    .str255(&job.network, "network")?
                    .bool(job.clean);
            }
            Self::SingleBlockJob { channel_id, job_id } => {
                out.u32(*channel_id).array(job_id);
            }
            Self::SubmitShare {
                channel_id,
                sequence_number,
                job_id,
                nonce,
            } => {
                out.u32(*channel_id)
                    .u32(*sequence_number)
                    .array(job_id)
                    .array(nonce);
            }
            Self::SubmitSharesSuccess {
                channel_id,
                last_sequence_number,
                accepted,
            } => {
                out.u32(*channel_id)
                    .u32(*last_sequence_number)
                    .u32(*accepted);
            }
            Self::SubmitSharesError {
                channel_id,
                sequence_number,
                code,
                message,
            } => {
                out.u32(*channel_id)
                    .u32(*sequence_number)
                    .u16(*code)
                    .str255(message, "error message")?;
            }
            Self::SetTarget {
                channel_id,
                share_target,
            } => {
                out.u32(*channel_id).array(share_target);
            }
        }
        let (kind, channel) = self.kind();
        write_frame(dst, kind, channel, &out.bytes)
    }

    /// Returns `None` for a message type this side does not know.
    pub(super) fn decode(frame: &Frame) -> Result<Option<Self>, CodecError> {
        let payload = &frame.payload[..];
        let message = match frame.kind {
            SETUP_CONNECTION => read(payload, "connection setup", |input| {
                Ok(Self::SetupConnection {
                    agent: input.str255()?,
                    tag: input.str255()?,
                    pow_algorithm_id: input.u8()?,
                    spec_revision: input.u32()?,
                })
            }),
            SETUP_CONNECTION_SUCCESS => read(payload, "connection setup answer", |input| {
                Ok(Self::SetupConnectionSuccess {
                    nonce_prefix: input.bytes255()?,
                    pool_address: input.str255()?,
                    pow_algorithm_id: input.u8()?,
                    spec_revision: input.u32()?,
                    chain_id: input.array()?,
                })
            }),
            SETUP_CONNECTION_ERROR => read(payload, "connection setup error", |input| {
                Ok(Self::SetupConnectionError {
                    code: input.u16()?,
                    message: input.str255()?,
                })
            }),
            RECONNECT => read(payload, "reconnect", |input| {
                Ok(Self::Reconnect {
                    host: input.str255()?,
                    port: input.u16()?,
                })
            }),
            OPEN_CHANNEL => read(payload, "channel request", |input| {
                Ok(Self::OpenChannel {
                    request_id: input.u32()?,
                    login: input.str255()?,
                    password: input.str255()?,
                })
            }),
            OPEN_CHANNEL_SUCCESS => read(payload, "channel answer", |input| {
                Ok(Self::OpenChannelSuccess {
                    request_id: input.u32()?,
                    channel_id: input.u32()?,
                })
            }),
            OPEN_CHANNEL_ERROR => read(payload, "channel error", |input| {
                Ok(Self::OpenChannelError {
                    request_id: input.u32()?,
                    code: input.u16()?,
                    message: input.str255()?,
                })
            }),
            NEW_JOB => read(payload, "job", |input| {
                Ok(Self::NewJob(Job {
                    channel_id: input.u32()?,
                    job_id: input.array()?,
                    header: input.bytes64k()?,
                    share_target: input.array()?,
                    target: input.array()?,
                    registration_height: input.u64()?,
                    network: input.str255()?,
                    clean: input.bool()?,
                }))
            }),
            SINGLE_BLOCK_JOB => read(payload, "single block job", |input| {
                Ok(Self::SingleBlockJob {
                    channel_id: input.u32()?,
                    job_id: input.array()?,
                })
            }),
            SUBMIT_SHARE => read(payload, "share", |input| {
                Ok(Self::SubmitShare {
                    channel_id: input.u32()?,
                    sequence_number: input.u32()?,
                    job_id: input.array()?,
                    nonce: input.array()?,
                })
            }),
            SUBMIT_SHARES_SUCCESS => read(payload, "share answer", |input| {
                Ok(Self::SubmitSharesSuccess {
                    channel_id: input.u32()?,
                    last_sequence_number: input.u32()?,
                    accepted: input.u32()?,
                })
            }),
            SUBMIT_SHARES_ERROR => read(payload, "share error", |input| {
                Ok(Self::SubmitSharesError {
                    channel_id: input.u32()?,
                    sequence_number: input.u32()?,
                    code: input.u16()?,
                    message: input.str255()?,
                })
            }),
            SET_TARGET => read(payload, "share target", |input| {
                Ok(Self::SetTarget {
                    channel_id: input.u32()?,
                    share_target: input.array()?,
                })
            }),
            _ => return Ok(None),
        }?;
        if message.kind().1 != frame.channel {
            return Err(CodecError::Malformed("channel flag"));
        }
        Ok(Some(message))
    }
}

fn read(
    payload: &[u8],
    what: &'static str,
    fields: impl FnOnce(&mut Reader<'_>) -> Result<Message, CodecError>,
) -> Result<Message, CodecError> {
    let mut input = Reader::new(payload, what);
    let message = fields(&mut input)?;
    input.finish()?;
    Ok(message)
}
