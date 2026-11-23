use std::{
    fs, io,
    path::{Path, PathBuf},
};

const CGROUP_ROOT: &str = "/sys/fs/cgroup";

pub(super) fn available_memory_bytes() -> io::Result<u64> {
    let system = mem_available(&fs::read_to_string("/proc/meminfo")?)
        .ok_or_else(|| io::Error::other("/proc/meminfo has no MemAvailable value"))?;
    let limited = fs::read_to_string("/proc/self/cgroup")
        .ok()
        .and_then(|own| cgroup_room(&own));
    Ok(limited.map_or(system, |room| room.min(system)))
}

fn mem_available(meminfo: &str) -> Option<u64> {
    let kib = meminfo
        .lines()
        .find_map(|line| line.strip_prefix("MemAvailable:"))?
        .trim()
        .strip_suffix("kB")?
        .trim()
        .parse::<u64>()
        .ok()?;
    kib.checked_mul(1024)
}

/// Headroom under the memory limits of this cgroup and its parents.
fn cgroup_room(own: &str) -> Option<u64> {
    if let Some(path) = own.lines().find_map(|line| line.strip_prefix("0::")) {
        let root = Path::new(CGROUP_ROOT);
        let mut dir = root.join(path.trim().trim_start_matches('/'));
        let mut least = None;
        loop {
            if let Some(room) = room_in(&dir, "memory.max", "memory.current", "inactive_file") {
                least = Some(least.map_or(room, |least: u64| least.min(room)));
            }
            if dir == root || !dir.pop() {
                return least;
            }
        }
    }
    room_in(
        &PathBuf::from(CGROUP_ROOT).join("memory"),
        "memory.limit_in_bytes",
        "memory.usage_in_bytes",
        "total_inactive_file",
    )
}

/// None if unlimited ("max" in cgroup v2). Reclaimable page cache counts as free.
fn room_in(dir: &Path, limit: &str, usage: &str, reclaimable: &str) -> Option<u64> {
    let read = |name: &str| fs::read_to_string(dir.join(name)).ok();
    let limit = read(limit)?.trim().parse::<u64>().ok()?;
    let used = read(usage)?.trim().parse::<u64>().ok()?;
    let cache = stat_value(&read("memory.stat").unwrap_or_default(), reclaimable);
    Some(limit.saturating_sub(used.saturating_sub(cache)))
}

fn stat_value(stat: &str, name: &str) -> u64 {
    stat.lines()
        .filter_map(|line| line.split_once(' '))
        .find(|(key, _)| *key == name)
        .and_then(|(_, value)| value.trim().parse().ok())
        .unwrap_or(0)
}

#[cfg(test)]
#[path = "tests/linux_memory.rs"]
mod tests;
