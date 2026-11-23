//! Hold database ownership through physical TenQ setup.
use super::{Hashers, Request, Source};
use anyhow::{Result, anyhow};
use std::{
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch},
    task::JoinHandle,
};

pub(super) struct BuildLease {
    _permit: OwnedSemaphorePermit,
    _owner: Option<crate::db::ProcessLease>,
}
impl BuildLease {
    pub(super) fn process_owner(&self) -> Option<crate::db::ProcessLease> {
        self._owner.clone()
    }
}
struct State {
    closed: bool,
    background: Option<JoinHandle<()>>,
}
pub(super) struct BuildLifetime {
    owner: Option<crate::db::ProcessLease>,
    physical: Arc<Semaphore>,
    state: Mutex<State>,
    #[cfg(test)]
    hook: Mutex<Option<BuildHook>>,
    #[cfg(test)]
    waiting: std::sync::atomic::AtomicUsize,
}
impl BuildLifetime {
    pub(super) fn new(owner: Option<crate::db::ProcessLease>) -> Self {
        Self {
            owner,
            physical: Arc::new(Semaphore::new(1)),
            state: Mutex::new(State {
                closed: false,
                background: None,
            }),
            #[cfg(test)]
            hook: Mutex::new(None),
            #[cfg(test)]
            waiting: std::sync::atomic::AtomicUsize::new(0),
        }
    }
    pub(super) fn closed(&self) -> bool {
        self.state.lock().map_or(true, |state| state.closed)
    }
    pub(super) fn ensure_open(&self) -> Result<()> {
        if self.closed() {
            return Err(anyhow!("TenQ setup has closed"));
        }
        Ok(())
    }
    pub(super) fn stop(&self) {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .closed = true;
    }
    pub(super) async fn begin(&self) -> Result<BuildLease> {
        self.ensure_open()?;
        // Queued builds retain the process lease until they finish or are dropped.
        let owner = self.owner.clone();
        #[cfg(test)]
        let _waiting = Waiting::new(&self.waiting);
        let permit = Arc::clone(&self.physical).acquire_owned().await?;
        self.ensure_open()?;
        Ok(BuildLease {
            _permit: permit,
            _owner: owner,
        })
    }
    pub(super) fn spawn_prefetch<F>(&self, task: impl FnOnce(Option<JoinHandle<()>>) -> F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if state.closed
            || state
                .background
                .as_ref()
                .is_some_and(|task| !task.is_finished())
        {
            return;
        }
        let previous = state.background.take();
        state.background = Some(tokio::spawn(task(previous)));
    }
    #[cfg(test)]
    pub(super) fn prefetch_running(&self) -> bool {
        self.state.lock().is_ok_and(|state| {
            state
                .background
                .as_ref()
                .is_some_and(|task| !task.is_finished())
        })
    }
    pub(super) async fn close(&self) -> Result<()> {
        self.stop();
        let background = self
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .background
            .take();
        if let Some(background) = background
            && let Err(error) = background.await
        {
            tracing::warn!("TenQ prefetch stopped: {error}");
        }
        // The blocking build holds this slot across async cancellation.
        let _finished = Arc::clone(&self.physical).acquire_owned().await?;
        Ok(())
    }
    #[cfg(test)]
    pub(super) fn pause_next(
        &self,
        entered: tokio::sync::oneshot::Sender<()>,
        release: std::sync::mpsc::Receiver<()>,
    ) {
        *self.hook.lock().unwrap() = Some(BuildHook { entered, release });
    }
    #[cfg(test)]
    pub(super) fn take_hook(&self) -> Option<BuildHook> {
        self.hook.lock().unwrap().take()
    }
    #[cfg(test)]
    pub(super) fn waiters(&self) -> usize {
        self.waiting.load(Ordering::SeqCst)
    }
}
#[cfg(test)]
pub(super) struct BuildHook {
    entered: tokio::sync::oneshot::Sender<()>,
    release: std::sync::mpsc::Receiver<()>,
}
#[cfg(test)]
impl BuildHook {
    pub(super) fn wait(self) {
        let _ = self.entered.send(());
        self.release
            .recv()
            .expect("the test releases physical setup");
    }
}
#[cfg(test)]
struct Waiting<'a>(&'a std::sync::atomic::AtomicUsize);
#[cfg(test)]
impl<'a> Waiting<'a> {
    fn new(value: &'a std::sync::atomic::AtomicUsize) -> Self {
        value.fetch_add(1, Ordering::SeqCst);
        Self(value)
    }
}
#[cfg(test)]
impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

pub(super) struct SourceControl {
    closed: AtomicBool,
    stop: watch::Sender<bool>,
    task: Mutex<Option<JoinHandle<()>>>,
    hashers: Arc<Hashers>,
}
impl SourceControl {
    pub(super) fn start(source: Source, receiver: mpsc::Receiver<Request>, rate: u32) -> Arc<Self> {
        let hashers = Arc::clone(&source.hashers);
        let (stop, mut stopped) = watch::channel(false);
        let task = tokio::spawn(async move {
            tokio::select! { biased; _ = stopped.changed() => {}, _ = source.run(receiver, rate) => {} }
        });
        Arc::new(Self {
            closed: AtomicBool::new(false),
            stop,
            task: Mutex::new(Some(task)),
            hashers,
        })
    }
    pub(super) fn closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
    pub(super) async fn close(&self) -> Result<()> {
        self.closed.store(true, Ordering::Release);
        self.hashers.lifetime.stop();
        let _ = self.stop.send(true);
        let task = self
            .task
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        if let Some(task) = task
            && let Err(error) = task.await
        {
            tracing::warn!("pool work source stopped: {error}");
        }
        self.hashers.close().await
    }
}
impl Drop for SourceControl {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::Release);
        self.hashers.lifetime.stop();
        let _ = self.stop.send(true);
        if let Some(task) = self
            .task
            .get_mut()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            task.abort();
        }
        // Keep the OS lease until the abandoned build and its allocations are dropped.
    }
}
