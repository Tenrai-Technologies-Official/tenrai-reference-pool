use super::CodecError;

/// Reads the little-endian fields of one message; any shortfall or leftover makes it malformed.
pub(super) struct Reader<'a> {
    bytes: &'a [u8],
    what: &'static str,
}

impl<'a> Reader<'a> {
    pub(super) const fn new(bytes: &'a [u8], what: &'static str) -> Self {
        Self { bytes, what }
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8], CodecError> {
        if self.bytes.len() < count {
            return Err(CodecError::Malformed(self.what));
        }
        let (taken, rest) = self.bytes.split_at(count);
        self.bytes = rest;
        Ok(taken)
    }

    pub(super) fn array<const N: usize>(&mut self) -> Result<[u8; N], CodecError> {
        Ok(self.take(N)?.try_into().expect("taken to length"))
    }

    pub(super) fn u8(&mut self) -> Result<u8, CodecError> {
        Ok(self.take(1)?[0])
    }

    pub(super) fn u16(&mut self) -> Result<u16, CodecError> {
        self.array().map(u16::from_le_bytes)
    }

    pub(super) fn u32(&mut self) -> Result<u32, CodecError> {
        self.array().map(u32::from_le_bytes)
    }

    pub(super) fn u64(&mut self) -> Result<u64, CodecError> {
        self.array().map(u64::from_le_bytes)
    }

    pub(super) fn bool(&mut self) -> Result<bool, CodecError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(CodecError::Malformed(self.what)),
        }
    }

    pub(super) fn bytes255(&mut self) -> Result<Vec<u8>, CodecError> {
        let length = usize::from(self.u8()?);
        Ok(self.take(length)?.to_vec())
    }

    pub(super) fn bytes64k(&mut self) -> Result<Vec<u8>, CodecError> {
        let length = usize::from(self.u16()?);
        Ok(self.take(length)?.to_vec())
    }

    pub(super) fn str255(&mut self) -> Result<String, CodecError> {
        String::from_utf8(self.bytes255()?).map_err(|_| CodecError::Malformed(self.what))
    }

    pub(super) fn finish(self) -> Result<(), CodecError> {
        if self.bytes.is_empty() {
            Ok(())
        } else {
            Err(CodecError::Malformed(self.what))
        }
    }
}

#[derive(Default)]
pub(super) struct Writer {
    pub(super) bytes: Vec<u8>,
}

impl Writer {
    pub(super) fn array(&mut self, bytes: &[u8]) -> &mut Self {
        self.bytes.extend_from_slice(bytes);
        self
    }

    pub(super) fn u16(&mut self, value: u16) -> &mut Self {
        self.array(&value.to_le_bytes())
    }

    pub(super) fn u32(&mut self, value: u32) -> &mut Self {
        self.array(&value.to_le_bytes())
    }

    pub(super) fn u64(&mut self, value: u64) -> &mut Self {
        self.array(&value.to_le_bytes())
    }

    pub(super) fn bool(&mut self, value: bool) -> &mut Self {
        self.array(&[u8::from(value)])
    }

    pub(super) fn bytes255(
        &mut self,
        bytes: &[u8],
        what: &'static str,
    ) -> Result<&mut Self, CodecError> {
        let length = u8::try_from(bytes.len()).map_err(|_| CodecError::Malformed(what))?;
        Ok(self.array(&[length]).array(bytes))
    }

    pub(super) fn bytes64k(
        &mut self,
        bytes: &[u8],
        what: &'static str,
    ) -> Result<&mut Self, CodecError> {
        let length = u16::try_from(bytes.len()).map_err(|_| CodecError::Malformed(what))?;
        Ok(self.u16(length).array(bytes))
    }

    pub(super) fn str255(
        &mut self,
        text: &str,
        what: &'static str,
    ) -> Result<&mut Self, CodecError> {
        self.bytes255(text.as_bytes(), what)
    }
}
