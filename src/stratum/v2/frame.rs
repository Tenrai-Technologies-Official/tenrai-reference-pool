use super::{CodecError, MAX_FRAME_BYTES};
use bytes::{Buf, BufMut, BytesMut};

const HEADER_BYTES: usize = 6;
const CHANNEL_BIT: u16 = 0x8000;

/// Extension type (u16, top bit marks a channel message), message type (u8), length (u24), payload.
pub(super) struct Frame {
    pub(super) kind: u8,
    pub(super) channel: bool,
    pub(super) payload: BytesMut,
}

pub(super) fn next_frame(src: &mut BytesMut) -> Result<Option<Frame>, CodecError> {
    if src.len() < HEADER_BYTES {
        return Ok(None);
    }
    let extension = u16::from_le_bytes([src[0], src[1]]);
    let kind = src[2];
    let length = usize::from(src[3]) | usize::from(src[4]) << 8 | usize::from(src[5]) << 16;
    if extension & !CHANNEL_BIT != 0 {
        return Err(CodecError::Malformed("frame extension"));
    }
    if length > MAX_FRAME_BYTES {
        return Err(CodecError::Oversized(length));
    }
    if src.len() < HEADER_BYTES + length {
        src.reserve(HEADER_BYTES + length - src.len());
        return Ok(None);
    }
    src.advance(HEADER_BYTES);
    Ok(Some(Frame {
        kind,
        channel: extension & CHANNEL_BIT != 0,
        payload: src.split_to(length),
    }))
}

pub(super) fn write_frame(
    dst: &mut BytesMut,
    kind: u8,
    channel: bool,
    payload: &[u8],
) -> Result<(), CodecError> {
    if payload.len() > MAX_FRAME_BYTES {
        return Err(CodecError::Oversized(payload.len()));
    }
    let length = u32::try_from(payload.len()).expect("bounded by MAX_FRAME_BYTES");
    dst.reserve(HEADER_BYTES + payload.len());
    dst.put_u16_le(if channel { CHANNEL_BIT } else { 0 });
    dst.put_u8(kind);
    dst.put_slice(&length.to_le_bytes()[..3]);
    dst.put_slice(payload);
    Ok(())
}
