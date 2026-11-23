use std::future::Future;
use tokio::{sync::watch, task::JoinHandle};

/// Owns only a fixed number of service jobs. Connection jobs belong to their listener JoinSet.
#[derive(Default)]
pub(crate) struct Group {
    handles: Vec<JoinHandle<()>>,
}
impl Group {
    pub(crate) fn push(&mut self, handle: JoinHandle<()>) {
        self.handles.push(handle);
    }
    pub(crate) fn extend(&mut self, handles: Vec<JoinHandle<()>>) {
        self.handles.extend(handles);
    }
    pub(crate) async fn join(&mut self) {
        for handle in &mut self.handles {
            if let Err(error) = handle.await {
                tracing::warn!("pool background task stopped: {error}");
            }
        }
        self.handles.clear();
    }
}
impl Drop for Group {
    fn drop(&mut self) {
        for handle in &self.handles {
            handle.abort();
        }
    }
}

pub(crate) fn spawn(
    shutdown: watch::Receiver<bool>,
    task: impl Future<Output = ()> + Send + 'static,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        tokio::select! {biased;_ = stopped(shutdown)=>{},_ = task=>{}}
    })
}
async fn stopped(mut shutdown: watch::Receiver<bool>) {
    loop {
        if *shutdown.borrow() {
            return;
        }
        if shutdown.changed().await.is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    struct Dropped(Arc<AtomicBool>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    #[tokio::test]
    async fn an_idle_background_job_is_dropped_and_joined_on_stop() {
        let (sender, receiver) = watch::channel(false);
        let dropped = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&dropped);
        let (started, ready) = tokio::sync::oneshot::channel();
        let mut group = Group::default();
        group.push(spawn(receiver, async move {
            let _guard = Dropped(flag);
            let _ = started.send(());
            std::future::pending::<()>().await;
        }));
        ready.await.unwrap();
        sender.send(true).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), group.join())
            .await
            .unwrap();
        assert!(dropped.load(Ordering::SeqCst));
    }
}
