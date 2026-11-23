use std::{ffi::CStr, io, ptr};

pub(super) fn available_memory_bytes() -> io::Result<u64> {
    let total = sysctl::<u64>(c"hw.memsize")?;
    // Percent of memory the kernel counts as available.
    let level = sysctl::<u32>(c"kern.memorystatus_level")?;
    Ok(total / 100 * u64::from(level.min(100)))
}

fn sysctl<T: Copy + Default>(name: &CStr) -> io::Result<T> {
    let mut value = T::default();
    let mut size = size_of::<T>();
    if unsafe {
        libc::sysctlbyname(
            name.as_ptr(),
            (&raw mut value).cast(),
            &mut size,
            ptr::null_mut(),
            0,
        )
    } != 0
    {
        return Err(io::Error::last_os_error());
    }
    if size != size_of::<T>() {
        return Err(io::Error::other("the sysctl value has an unexpected size"));
    }
    Ok(value)
}
