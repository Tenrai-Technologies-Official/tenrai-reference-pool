use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn requests_that_miss_together_build_the_answer_once() {
    let answers = Answers::default();
    let builds = Arc::new(AtomicUsize::new(0));
    let waiting = (0..50).map(|_| {
        let answers = answers.clone();
        let builds = Arc::clone(&builds);
        tokio::spawn(async move {
            answers
                .get_or_build("pool", Duration::from_secs(2), || async move {
                    builds.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Ok::<_, ()>(Some(Bytes::from_static(b"{}")))
                })
                .await
        })
    });
    for answer in joined(waiting.collect()).await {
        assert_eq!(answer, Ok(Some(Bytes::from_static(b"{}"))));
    }
    assert_eq!(builds.load(Ordering::SeqCst), 1);
    assert!(answers.building.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_failed_or_empty_build_is_not_kept() {
    let answers = Answers::default();
    let failed = answers
        .get_or_build("block", Duration::from_secs(30), || async {
            Err::<Option<Bytes>, _>("busy")
        })
        .await;
    assert_eq!(failed, Err("busy"));
    let missing = answers
        .get_or_build("block", Duration::from_secs(30), || async {
            Ok::<_, ()>(None)
        })
        .await;
    assert_eq!(missing, Ok(None));
    let built = answers
        .get_or_build("block", Duration::from_secs(30), || async {
            Ok::<_, ()>(Some(Bytes::from_static(b"1")))
        })
        .await;
    assert_eq!(built, Ok(Some(Bytes::from_static(b"1"))));
}

#[tokio::test]
async fn a_flood_of_new_keys_leaves_the_answers_in_use_in_place() {
    let answers = Answers::default();
    let builds = AtomicUsize::new(0);
    let busy = || async {
        builds.fetch_add(1, Ordering::SeqCst);
        Ok::<_, ()>(Some(Bytes::from_static(b"{}")))
    };
    let page = || async { Ok::<_, ()>(Some(Bytes::from_static(b"[]"))) };
    let ttl = Duration::from_secs(60);
    for flood in 0..MAX_KEPT * 2 {
        answers.get_or_build("pool", ttl, busy).await.unwrap();
        answers
            .get_or_build(&format!("blocks:{flood}"), ttl, page)
            .await
            .unwrap();
    }
    assert_eq!(
        builds.load(Ordering::SeqCst),
        1,
        "the answer in use was never pushed out"
    );
    let store = locked(&answers.kept);
    assert_eq!(store.entries.len(), MAX_KEPT);
    assert!(
        store
            .entries
            .contains_key(&format!("blocks:{}", MAX_KEPT * 2 - 1))
    );
}

#[tokio::test]
async fn kept_answers_stay_within_their_byte_budget() {
    let answers = Answers::default();
    let page = Bytes::from(vec![b'x'; 1 << 20]);
    let ttl = Duration::from_secs(60);
    for key in 0..100 {
        let page = page.clone();
        answers
            .get_or_build(&format!("public:blocks:{key}"), ttl, || async move {
                Ok::<_, ()>(Some(page))
            })
            .await
            .unwrap();
    }
    let mut store = locked(&answers.kept);
    assert!(store.bytes <= MAX_BYTES);
    assert!(store.entries.len() < 100, "older pages made room");
    assert!(store.entries.contains_key("public:blocks:99"));
    let counted = store.bytes;
    store.sweep(Instant::now());
    assert_eq!(
        store.bytes, counted,
        "the running count matches the kept answers"
    );
}

#[tokio::test]
async fn expired_answers_go_when_the_cache_sweeps() {
    let answers = Answers::default();
    let one = || async { Ok::<_, ()>(Some(Bytes::from_static(b"1"))) };
    answers
        .get_or_build("old", Duration::ZERO, one)
        .await
        .unwrap();
    answers
        .get_or_build("live", Duration::from_secs(60), one)
        .await
        .unwrap();
    let mut store = locked(&answers.kept);
    store.sweep(Instant::now());
    assert!(!store.entries.contains_key("old"));
    assert!(store.entries.contains_key("live"));
    assert_eq!(store.bytes, "live".len() + 1);
}

async fn joined<T>(handles: Vec<tokio::task::JoinHandle<T>>) -> Vec<T> {
    let mut answers = Vec::with_capacity(handles.len());
    for handle in handles {
        answers.push(handle.await.unwrap());
    }
    answers
}
