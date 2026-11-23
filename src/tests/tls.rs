use super::*;
use crate::test_support::temp_path;

#[test]
fn the_pool_makes_its_certificate_once_and_serves_with_it() {
    let folder = temp_path("own-tls");
    let (cert, key) = (folder.join("stratum.crt"), folder.join("stratum.key"));
    let names = vec![
        "localhost".to_owned(),
        "192.0.2.7".to_owned(),
        "pool.example".to_owned(),
    ];
    assert!(create_self_signed(&cert, &key, &names).unwrap());
    let first = std::fs::read(&cert).unwrap();
    assert!(!create_self_signed(&cert, &key, &names).unwrap());
    assert_eq!(std::fs::read(&cert).unwrap(), first);
    acceptor(&cert, &key, b"stratum").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&key).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    std::fs::remove_file(&key).unwrap();
    assert!(create_self_signed(&cert, &key, &names).is_err());
    let _ = std::fs::remove_dir_all(folder);
}
