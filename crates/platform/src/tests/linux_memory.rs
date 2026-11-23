use super::*;

#[test]
fn mem_available_is_read_in_bytes() {
    let meminfo =
        "MemTotal:       16318412 kB\nMemFree:          812344 kB\nMemAvailable:    2097152 kB\n";
    assert_eq!(mem_available(meminfo), Some(2 << 30));
    assert_eq!(mem_available("MemTotal: 1 kB\n"), None);
}

#[test]
fn reclaimable_page_cache_counts_as_free() {
    let stat = "anon 1073741824\ninactive_file 536870912\nactive_file 1\n";
    assert_eq!(stat_value(stat, "inactive_file"), 512 << 20);
    assert_eq!(stat_value(stat, "total_inactive_file"), 0);
    assert_eq!(stat_value("inactive_file x\n", "inactive_file"), 0);
}

#[test]
fn a_cgroup_limit_leaves_its_room_and_max_means_none() {
    let dir = std::env::temp_dir().join(format!("tenrai-cgroup-{}", std::process::id()));
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join("memory.max"), "8589934592\n").unwrap();
    fs::write(dir.join("memory.current"), "7516192768\n").unwrap();
    fs::write(
        dir.join("memory.stat"),
        "anon 6442450944\ninactive_file 1073741824\n",
    )
    .unwrap();
    let room = room_in(&dir, "memory.max", "memory.current", "inactive_file");
    fs::write(dir.join("memory.max"), "max\n").unwrap();
    let unlimited = room_in(&dir, "memory.max", "memory.current", "inactive_file");
    fs::remove_dir_all(&dir).unwrap();
    assert_eq!(room, Some(2 << 30));
    assert_eq!(unlimited, None);
}
