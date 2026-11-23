use std::io;
use windows_sys::Win32::System::SystemInformation::{GlobalMemoryStatusEx, MEMORYSTATUSEX};

pub(super) fn available_memory_bytes() -> io::Result<u64> {
    let mut status = MEMORYSTATUSEX {
        dwLength: size_of::<MEMORYSTATUSEX>() as u32,
        dwMemoryLoad: 0,
        ullTotalPhys: 0,
        ullAvailPhys: 0,
        ullTotalPageFile: 0,
        ullAvailPageFile: 0,
        ullTotalVirtual: 0,
        ullAvailVirtual: 0,
        ullAvailExtendedVirtual: 0,
    };
    if unsafe { GlobalMemoryStatusEx(&mut status) } == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(status.ullAvailPhys)
}
