use super::{
    protocol::{ClientMessage, MAX_LINE_BYTES, Protocol, ServerMessage},
    v2::{CodecError, Message, ServerCodec},
};
use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{Error as WebError, Message as WebMessage, error::CapacityError},
};
use tokio_util::codec::{Framed, LinesCodec, LinesCodecError};

/// Common session messages over TCP, TLS or WebSocket.
pub(super) enum Wire<S> {
    V1(Framed<S, LinesCodec>),
    V2(Framed<S, ServerCodec>),
    /// One V1 line per WebSocket text frame.
    Web(WebSocketStream<S>),
}

pub(super) enum Inbound {
    Line(String),
    Message(ClientMessage),
    /// WebSocket control frame charged to the request budget.
    Control,
    /// Protocol violation counted against the client address.
    Junk(String),
}

impl<S> Wire<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    pub(super) fn new(socket: S, protocol: Protocol) -> Self {
        match protocol {
            Protocol::V1 => Self::V1(Framed::new(
                socket,
                LinesCodec::new_with_max_length(MAX_LINE_BYTES),
            )),
            Protocol::V2 => Self::V2(Framed::new(socket, ServerCodec::default())),
        }
    }

    pub(super) async fn next(&mut self) -> Option<Result<Inbound>> {
        Some(match self {
            Self::V1(framed) => match framed.next().await? {
                Ok(line) => Ok(Inbound::Line(line)),
                Err(LinesCodecError::MaxLineLengthExceeded) => {
                    Ok(Inbound::Junk("sent a line over the size limit".into()))
                }
                Err(LinesCodecError::Io(error)) => Err(error.into()),
            },
            Self::V2(framed) => match framed.next().await? {
                Ok(message) => Ok(Inbound::Message(message)),
                Err(CodecError::Oversized(_)) => {
                    Ok(Inbound::Junk("sent a frame over the size limit".into()))
                }
                Err(CodecError::Malformed(what)) => {
                    Ok(Inbound::Junk(format!("sent a malformed request ({what})")))
                }
                Err(CodecError::Io(error)) => Err(error.into()),
            },
            Self::Web(stream) => loop {
                match stream.next().await? {
                    Ok(WebMessage::Text(text)) => {
                        break Ok(Inbound::Line(text.as_str().to_owned()));
                    }
                    Ok(WebMessage::Binary(_)) => {
                        break Ok(Inbound::Junk("sent a binary WebSocket message".into()));
                    }
                    Ok(WebMessage::Close(_)) => return None,
                    Ok(WebMessage::Ping(_) | WebMessage::Pong(_)) => break Ok(Inbound::Control),
                    Ok(_) => continue,
                    Err(WebError::Capacity(CapacityError::MessageTooLong { .. })) => {
                        break Ok(Inbound::Junk("sent a message over the size limit".into()));
                    }
                    Err(error) => break Err(error.into()),
                }
            },
        })
    }

    pub(super) async fn send(&mut self, message: ServerMessage) -> Result<()> {
        match self {
            Self::V1(framed) => framed.send(message.to_line()).await?,
            Self::V2(framed) => framed.send(message).await?,
            Self::Web(stream) => stream.send(WebMessage::text(message.to_line())).await?,
        }
        Ok(())
    }

    /// Ask V2 miners to reconnect before closing the TLS stream.
    pub(super) async fn goodbye(&mut self) -> Result<()> {
        match self {
            Self::V1(framed) => framed.get_mut().shutdown().await?,
            Self::Web(stream) => stream.close(None).await?,
            Self::V2(framed) => {
                framed
                    .send(Message::Reconnect {
                        host: String::new(),
                        port: 0,
                    })
                    .await?;
                framed.get_mut().shutdown().await?;
            }
        }
        Ok(())
    }
}
