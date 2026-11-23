use super::*;
use crate::test_support::work;

fn job(id: u8, work: &Arc<Work>) -> Job {
    Job {
        id: Hash384([id; 48]),
        work: Arc::clone(work),
        extranonce: [id; 32],
        share_target: [u8::MAX; 32],
        weight: 1,
    }
}

#[test]
fn a_nonce_counts_once_per_job() {
    let work = work([u8::MAX; 32], 10);
    let mut book = JobBook::default();
    book.insert(job(1, &work));
    assert!(book.claim(Hash384([1; 48]), [5; 32], 10).is_ok());
    assert_eq!(
        book.claim(Hash384([1; 48]), [5; 32], 10).err(),
        Some(RejectReason::Duplicate)
    );
    assert!(book.claim(Hash384([1; 48]), [6; 32], 10).is_ok());
    book.release(Hash384([1; 48]), [5; 32]);
    assert!(book.claim(Hash384([1; 48]), [5; 32], 10).is_ok());
}

#[test]
fn the_same_nonce_on_another_job_is_new_work() {
    let work = work([u8::MAX; 32], 10);
    let mut book = JobBook::default();
    book.insert(job(1, &work));
    book.insert(job(2, &work));
    assert!(book.claim(Hash384([1; 48]), [5; 32], 10).is_ok());
    assert!(book.claim(Hash384([2; 48]), [5; 32], 10).is_ok());
}

#[test]
fn unknown_and_expired_jobs_are_stale() {
    let work = work([u8::MAX; 32], 10);
    let mut book = JobBook::default();
    book.insert(job(1, &work));
    assert_eq!(
        book.claim(Hash384([9; 48]), [5; 32], 10).err(),
        Some(RejectReason::StaleJob)
    );
    let end = work.template.registration_window_end;
    assert!(book.claim(Hash384([1; 48]), [5; 32], end).is_ok());
    assert_eq!(
        book.claim(Hash384([1; 48]), [6; 32], end + 1).err(),
        Some(RejectReason::StaleJob)
    );
    assert!(!book.contains(Hash384([1; 48])));
}

#[test]
fn only_the_newest_jobs_stay_open() {
    let work = work([u8::MAX; 32], 10);
    let mut book = JobBook::default();
    for id in 1..=(SESSION_JOBS as u8 + 2) {
        book.insert(job(id, &work));
    }
    assert!(!book.contains(Hash384([1; 48])));
    assert!(!book.contains(Hash384([2; 48])));
    assert!(book.contains(Hash384([3; 48])));
    assert!(book.contains(Hash384([SESSION_JOBS as u8 + 2; 48])));
}

#[test]
fn retiring_a_template_ends_its_jobs() {
    let work = work([u8::MAX; 32], 10);
    let mut book = JobBook::default();
    book.insert(job(1, &work));
    book.insert(job(2, &work));
    book.retire_template(work.template.id);
    assert!(!book.contains(Hash384([1; 48])));
    assert!(!book.contains(Hash384([2; 48])));
}

#[test]
fn issue_numbers_never_repeat() {
    let mut book = JobBook::default();
    let first = book.next_issue();
    assert!(book.next_issue() > first);
}
