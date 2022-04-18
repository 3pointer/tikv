// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use std::sync::{Arc, RwLock};

use crate::debug;
use crate::utils::SegmentSet;
use crate::{try_send, utils};
use dashmap::mapref::one::RefMut;
use dashmap::DashMap;
use engine_traits::KvEngine;
use kvproto::metapb::Region;
use raft::StateRole;
use raftstore::coprocessor::*;
use resolved_ts::Resolver;
use tikv_util::worker::Scheduler;
use tikv_util::{info, warn, HandyRwLock};
use txn_types::TimeStamp;

use crate::endpoint::{ObserveOp, Task};

/// An Observer for Backup Stream.
///
/// It observes raftstore internal events, such as:
///   1. Apply command events.
#[derive(Clone)]
pub struct BackupStreamObserver {
    scheduler: Scheduler<Task>,
    // Note: maybe wrap those fields to methods?
    pub ranges: Arc<RwLock<SegmentSet<Vec<u8>>>>,
}

impl BackupStreamObserver {
    /// Create a new `BackupStreamObserver`.
    ///
    /// Events are strong ordered, so `scheduler` must be implemented as
    /// a FIFO queue.
    pub fn new(scheduler: Scheduler<Task>) -> BackupStreamObserver {
        BackupStreamObserver {
            scheduler,
            ranges: Default::default(),
        }
    }

    pub fn register_to(&self, coprocessor_host: &mut CoprocessorHost<impl KvEngine>) {
        let registry = &mut coprocessor_host.registry;

        // use 0 as the priority of the cmd observer. should have a higher priority than
        // the `resolved-ts`'s cmd observer
        registry.register_cmd_observer(0, BoxCmdObserver::new(self.clone()));
        registry.register_role_observer(100, BoxRoleObserver::new(self.clone()));
        registry.register_region_change_observer(100, BoxRegionChangeObserver::new(self.clone()));
    }

    /// The internal way to register a region.
    /// It delegate the initial scanning and modify of the subs to the endpoint.
    #[cfg(test)]
    fn register_region(&self, region: &Region) {
        if let Err(err) = self
            .scheduler
            .schedule(Task::ModifyObserve(ObserveOp::Start {
                region: region.clone(),
                needs_initial_scanning: true,
            }))
        {
            use crate::errors::Error;
            Error::from(err).report(format_args!(
                "failed to schedule role change for region {}",
                region.get_id()
            ))
        }
    }

    /// Test whether a region should be observed by the observer.
    fn should_register_region(&self, region: &Region) -> bool {
        // If the end key is empty, it actually meant infinity.
        // However, this way is a little hacky, maybe we'd better make a
        // `RangesBound<R>` version for `is_overlapping`.
        let mut end_key = region.get_end_key();
        if end_key.is_empty() {
            end_key = &[0xffu8; 32];
        }
        self.ranges
            .rl()
            .is_overlapping((region.get_start_key(), end_key))
    }
}

impl Coprocessor for BackupStreamObserver {}

/// A utility to tracing the regions being subscripted.
#[derive(Clone, Default, Debug)]
pub struct SubscriptionTracer(Arc<DashMap<u64, RegionSubscription>>);

pub struct RegionSubscription {
    pub meta: Region,
    handle: ObserveHandle,
    resolver: TwoPhaseResolver,
}

impl std::fmt::Debug for RegionSubscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("RegionSubscription")
            .field(&self.meta.get_id())
            .field(&self.handle)
            .finish()
    }
}

impl RegionSubscription {
    pub fn new(region: Region, handle: ObserveHandle, start_ts: Option<TimeStamp>) -> Self {
        let resolver = TwoPhaseResolver::new(region.get_id(), start_ts);
        Self {
            handle,
            meta: region,
            resolver,
        }
    }

    pub fn stop_observing(&self) {
        self.handle.stop_observing()
    }

    pub fn is_observing(&self) -> bool {
        self.handle.is_observing()
    }

    pub fn resolver(&mut self) -> &mut TwoPhaseResolver {
        &mut self.resolver
    }
}

impl SubscriptionTracer {
    // Register a region as tracing.
    // The `start_ts` is used to tracking the progress of initial scanning.
    // (Note: the `None` case of `start_ts` is for testing / refresh region status when split / merge,
    //    maybe we'd better provide some special API for those cases and remove the `Option`?)
    pub fn register_region(
        &self,
        region: &Region,
        handle: ObserveHandle,
        start_ts: Option<TimeStamp>,
    ) {
        info!("start listen stream from store"; "observer" => ?handle, "region_id" => %region.get_id());
        if let Some(o) = self.0.insert(
            region.get_id(),
            RegionSubscription::new(region.clone(), handle, start_ts),
        ) {
            warn!("register region which is already registered"; "region_id" => %region.get_id());
            o.stop_observing();
        }
    }

    /// try advance the resolved ts with the min ts of in-memory locks.
    pub fn resolve_with(&self, min_ts: TimeStamp) -> TimeStamp {
        self.0
            .iter_mut()
            .map(|mut s| s.resolver.resolve(min_ts))
            .min()
            // If there isn't any region observed, the `min_ts` can be used as resolved ts safely.
            .unwrap_or(min_ts)
    }

    pub fn warn_if_gap_too_huge(&self, ts: TimeStamp) {
        if TimeStamp::physical_now() - ts.physical() >= 10 * 60 * 1000
        /* 10 mins */
        {
            let far_resolver = self
                .0
                .iter()
                .min_by_key(|r| r.value().resolver.resolved_ts());
            warn!("stream backup resolver ts advancing too slow";
            "far_resolver" => %{match far_resolver {
                Some(r) => format!("{:?}", r.value().resolver),
                None => "BUG[NoResolverButResolvedTSDoesNotAdvance]".to_owned()
            }});
        }
    }

    /// try to mark a region no longer be tracked by this observer.
    /// returns whether success (it failed if the region hasn't been observed when calling this.)
    pub fn deregister_region(
        &self,
        region: &Region,
        if_cond: impl FnOnce(&Region, &Region) -> bool,
    ) -> bool {
        let region_id = region.get_id();
        let remove_result = self.0.remove_if(&region_id, |_, old_region| {
            if_cond(&old_region.meta, region)
        });
        match remove_result {
            Some(o) => {
                o.1.stop_observing();
                info!("stop listen stream from store"; "observer" => ?o.1, "region_id"=> %region_id);
                true
            }
            None => {
                warn!("trying to deregister region not registered"; "region_id" => %region_id);
                false
            }
        }
    }

    /// try update the subscription status by the new region info.
    ///
    /// # return
    ///
    /// If the status can be updated internally without deregister-and-register, returns false.
    pub fn try_update_region(&self, new_region: &Region) -> bool {
        let mut sub = match self.get_subscription_of(new_region.get_id()) {
            Some(sub) => sub,
            None => {
                warn!("backup stream observer refreshing void subscription."; "new_region" => ?new_region);
                return true;
            }
        };

        let mut subscription = sub.value_mut();

        let old_epoch = subscription.meta.get_region_epoch();
        let new_epoch = new_region.get_region_epoch();
        if old_epoch.version == new_epoch.version {
            subscription.meta = new_region.clone();
            return true;
        }

        false
    }

    /// check whether the region_id should be observed by this observer.
    pub fn is_observing(&self, region_id: u64) -> bool {
        let mut exists = false;

        // The region traced, check it whether is still be observing,
        // if not, remove it.
        let still_observing = self
            .0
            // Assuming this closure would be called iff the key exists.
            // So we can elide a `contains` check.
            .remove_if(&region_id, |_, o| {
                exists = true;
                !o.is_observing()
            })
            .is_none();
        exists && still_observing
    }

    pub fn get_subscription_of(&self, region_id: u64) -> Option<RefMut<u64, RegionSubscription>> {
        self.0.get_mut(&region_id)
    }
}

/// This enhanced version of `Resolver` allow some unorder of lock events.  
/// The name "2-phase" means this is used for 2 *concurrency* phases of observing a region:
/// 1. Doing the initial scanning.
/// 2. Listening at the incremental data.
///
/// ```text
/// +->(Start TS Of Task)            +->(Task registered to KV)
/// +--------------------------------+------------------------>
/// ^~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~^ ^~~~~~~~~~~~~~~~~~~~~~~~~
/// |                                 +-> Phase 2: Listening incremtnal data.
/// +-> Phase 1: Initial scanning scans writes between start ts and now.
/// ```
///
/// In backup-stream, we execute these two tasks parallelly. Which may make some race conditions:
/// - When doing initial scanning, there may be a flush triggered, but the defult resolver
///   would probably resolved to the tip of incremental events.
/// - When doing initial scanning, we meet and track a lock already meet by the incremental events,
///   then the default resolver cannot untrack this lock any more.
///
/// This version of resolver did some change for solve these problmes:
/// - The resolver won't advance the resolved ts to greater than `stable_ts` if there is some. This
///   can help us prevent resolved ts from advancing when initial scanning hasn't finished yet.
/// - When we `untrack` a lock haven't been tracked, this would record it, and skip this lock if we want to track it then.
///   This would be safe because:
///   - untracking a lock not be tracked is no-op for now.
///   - tracking a lock have already being untracked (unordered call of `track` and `untrack`) wouldn't happen at phase 2 for same region.
///     but only when phase 1 and phase 2 happend concurrently, at that time, we wouldn't and cannot advance the resolved ts.
pub struct TwoPhaseResolver {
    resolver: Resolver,
    future_locks: Vec<FutureLock>,
    /// When `Some`, is the start ts of the initial scanning.
    /// And implies the phase 1 (initial scanning) is keep running asynchronously.
    stable_ts: Option<TimeStamp>,
}

enum FutureLock {
    Lock(Vec<u8>, TimeStamp),
    Unlock(Vec<u8>),
}

impl std::fmt::Debug for FutureLock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Lock(arg0, arg1) => f
                .debug_tuple("Lock")
                .field(&format_args!("{}", utils::redact(arg0)))
                .field(arg1)
                .finish(),
            Self::Unlock(arg0) => f
                .debug_tuple("Unlock")
                .field(&format_args!("{}", utils::redact(arg0)))
                .finish(),
        }
    }
}

impl TwoPhaseResolver {
    pub fn in_phase_one(&self) -> bool {
        self.stable_ts.is_some()
    }

    pub fn track_phase_one_lock(&mut self, start_ts: TimeStamp, key: Vec<u8>) {
        if !self.in_phase_one() {
            warn!("backup stream tracking lock as if in phase one"; "start_ts" => %start_ts, "key" => %utils::redact(&key))
        }
        self.resolver.track_lock(start_ts, key, None)
    }

    pub fn track_lock(&mut self, start_ts: TimeStamp, key: Vec<u8>) {
        if self.in_phase_one() {
            self.future_locks.push(FutureLock::Lock(key, start_ts));
            return;
        }
        self.resolver.track_lock(start_ts, key, None)
    }

    pub fn untrack_lock(&mut self, key: &[u8]) {
        // If we are still in phase one, tracking all unpaired locks.
        if self.in_phase_one() {
            self.future_locks.push(FutureLock::Unlock(key.to_owned()));
            return;
        }
        self.resolver.untrack_lock(key, None)
    }

    fn handle_future_lock(&mut self, lock: FutureLock) {
        match lock {
            FutureLock::Lock(key, ts) => self.resolver.track_lock(ts, key, None),
            FutureLock::Unlock(key) => self.resolver.untrack_lock(&key, None),
        }
    }

    pub fn resolve(&mut self, min_ts: TimeStamp) -> TimeStamp {
        if let Some(stable_ts) = self.stable_ts {
            return min_ts.min(stable_ts);
        }

        self.resolver.resolve(min_ts)
    }

    pub fn resolved_ts(&self) -> TimeStamp {
        if let Some(stable_ts) = self.stable_ts {
            return stable_ts;
        }

        self.resolver.resolved_ts()
    }

    pub fn new(region_id: u64, stable_ts: Option<TimeStamp>) -> Self {
        Self {
            resolver: Resolver::new(region_id),
            future_locks: Default::default(),
            stable_ts,
        }
    }

    pub fn phase_one_done(&mut self) {
        debug!("phase one done"; "resolver" => ?self.resolver, "future_locks" => ?self.future_locks);
        for lock in std::mem::take(&mut self.future_locks).into_iter() {
            self.handle_future_lock(lock);
        }
        self.stable_ts = None
    }
}

impl std::fmt::Debug for TwoPhaseResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("TwoPhaseResolver")
            .field(&self.stable_ts)
            .field(&self.resolver)
            .finish()
    }
}

impl<E: KvEngine> CmdObserver<E> for BackupStreamObserver {
    // `BackupStreamObserver::on_flush_applied_cmd_batch` should only invoke if `cmd_batches` is not empty
    // and only leader will trigger this.
    fn on_flush_applied_cmd_batch(
        &self,
        max_level: ObserveLevel,
        cmd_batches: &mut Vec<CmdBatch>,
        _engine: &E,
    ) {
        assert!(!cmd_batches.is_empty());
        debug!(
            "observe backup stream kv";
            "cmd_batches len" => cmd_batches.len(),
            "level" => ?max_level,
        );

        if max_level != ObserveLevel::All {
            return;
        }

        // TODO may be we should filter cmd batch here, to reduce the cost of clone.
        let cmd_batches: Vec<_> = cmd_batches
            .iter()
            .filter(|cb| !cb.is_empty() && cb.level == ObserveLevel::All)
            .cloned()
            .collect();
        if cmd_batches.is_empty() {
            return;
        }
        try_send!(self.scheduler, Task::BatchEvent(cmd_batches));
    }

    fn on_applied_current_term(&self, role: StateRole, region: &Region) {
        if role == StateRole::Leader && self.should_register_region(region) {
            try_send!(
                self.scheduler,
                Task::ModifyObserve(ObserveOp::Start {
                    region: region.clone(),
                    needs_initial_scanning: true,
                })
            );
        }
    }
}

impl RoleObserver for BackupStreamObserver {
    fn on_role_change(&self, ctx: &mut ObserverContext<'_>, r: &RoleChange) {
        if r.state != StateRole::Leader {
            try_send!(
                self.scheduler,
                Task::ModifyObserve(ObserveOp::Stop {
                    region: ctx.region().clone(),
                })
            );
        }
    }
}

impl RegionChangeObserver for BackupStreamObserver {
    fn on_region_changed(
        &self,
        ctx: &mut ObserverContext<'_>,
        event: RegionChangeEvent,
        role: StateRole,
    ) {
        if role != StateRole::Leader {
            return;
        }
        match event {
            RegionChangeEvent::Destroy => {
                try_send!(
                    self.scheduler,
                    Task::ModifyObserve(ObserveOp::CheckEpochAndStop {
                        region: ctx.region().clone(),
                    })
                );
            }
            RegionChangeEvent::Update => {
                try_send!(
                    self.scheduler,
                    Task::ModifyObserve(ObserveOp::RefreshResolver {
                        region: ctx.region().clone(),
                    })
                );
            }
            // No need for handling `Create` -- once it becomes leader, it would start by
            // `on_applied_current_term`.
            _ => {}
        }
    }
}

#[cfg(test)]

mod tests {
    use std::assert_matches::assert_matches;
    use std::time::Duration;

    use engine_panic::PanicEngine;
    use kvproto::metapb::Region;
    use raft::StateRole;
    use raftstore::coprocessor::{
        Cmd, CmdBatch, CmdObserveInfo, CmdObserver, ObserveHandle, ObserveLevel, ObserverContext,
        RegionChangeEvent, RegionChangeObserver, RoleChange, RoleObserver,
    };

    use tikv_util::worker::dummy_scheduler;
    use tikv_util::HandyRwLock;

    use crate::endpoint::{ObserveOp, Task};

    use super::{BackupStreamObserver, SubscriptionTracer};

    fn fake_region(id: u64, start: &[u8], end: &[u8]) -> Region {
        let mut r = Region::new();
        r.set_id(id);
        r.set_start_key(start.to_vec());
        r.set_end_key(end.to_vec());
        r
    }

    #[test]
    fn test_observer_cancel() {
        let (sched, mut rx) = dummy_scheduler();

        // Prepare: assuming a task wants the range of [0001, 0010].
        let o = BackupStreamObserver::new(sched);
        let subs = SubscriptionTracer::default();
        assert!(o.ranges.wl().add((b"0001".to_vec(), b"0010".to_vec())));

        // Test regions can be registered.
        let r = fake_region(42, b"0008", b"0009");
        o.register_region(&r);
        let task = rx.recv_timeout(Duration::from_secs(0)).unwrap().unwrap();
        let handle = ObserveHandle::new();
        if let Task::ModifyObserve(ObserveOp::Start { ref region, .. }) = task {
            subs.register_region(region, handle.clone(), None);
        } else {
            panic!("unexpected message received: it is {}", task);
        }
        assert!(subs.is_observing(42));
        handle.stop_observing();
        assert!(!subs.is_observing(42));
    }

    #[test]
    fn test_observer_basic() {
        let mock_engine = PanicEngine;
        let (sched, mut rx) = dummy_scheduler();

        // Prepare: assuming a task wants the range of [0001, 0010].
        let o = BackupStreamObserver::new(sched);
        let subs = SubscriptionTracer::default();
        assert!(o.ranges.wl().add((b"0001".to_vec(), b"0010".to_vec())));

        // Test regions can be registered.
        let r = fake_region(42, b"0008", b"0009");
        o.register_region(&r);
        let task = rx.recv_timeout(Duration::from_secs(0)).unwrap().unwrap();
        let handle = ObserveHandle::new();
        if let Task::ModifyObserve(ObserveOp::Start { ref region, .. }) = task {
            subs.register_region(region, handle.clone(), None);
        } else {
            panic!("not match, it is {:?}", task);
        }

        // Test events with key in the range can be observed.
        let observe_info = CmdObserveInfo::from_handle(handle.clone(), ObserveHandle::new());
        let mut cb = CmdBatch::new(&observe_info, 42);
        cb.push(&observe_info, 42, Cmd::default());
        let mut cmd_batches = vec![cb];
        o.on_flush_applied_cmd_batch(ObserveLevel::All, &mut cmd_batches, &mock_engine);
        let task = rx.recv_timeout(Duration::from_secs(0)).unwrap().unwrap();
        assert_matches!(task, Task::BatchEvent(batches) if
            batches.len() == 1 && batches[0].region_id == 42 && batches[0].cdc_id == handle.id
        );

        // Test event from other region should not be send.
        let observe_info = CmdObserveInfo::from_handle(ObserveHandle::new(), ObserveHandle::new());
        let mut cb = CmdBatch::new(&observe_info, 43);
        cb.push(&observe_info, 43, Cmd::default());
        cb.level = ObserveLevel::None;
        let mut cmd_batches = vec![cb];
        o.on_flush_applied_cmd_batch(ObserveLevel::None, &mut cmd_batches, &mock_engine);
        let task = rx.recv_timeout(Duration::from_millis(20));
        assert!(task.is_err(), "it is {:?}", task);

        // Test region out of range won't be added to observe list.
        let r = fake_region(43, b"0010", b"0042");
        let mut ctx = ObserverContext::new(&r);
        o.on_role_change(&mut ctx, &RoleChange::new(StateRole::Leader));
        let task = rx.recv_timeout(Duration::from_millis(20));
        assert!(task.is_err(), "it is {:?}", task);
        assert!(!subs.is_observing(43));

        // Test newly created region out of range won't be added to observe list.
        let mut ctx = ObserverContext::new(&r);
        o.on_region_changed(&mut ctx, RegionChangeEvent::Create, StateRole::Leader);
        let task = rx.recv_timeout(Duration::from_millis(20));
        assert!(task.is_err(), "it is {:?}", task);
        assert!(!subs.is_observing(43));

        // Test give up subscripting when become follower.
        let r = fake_region(42, b"0008", b"0009");
        let mut ctx = ObserverContext::new(&r);
        o.on_role_change(&mut ctx, &RoleChange::new(StateRole::Follower));
        let task = rx.recv_timeout(Duration::from_millis(20));
        assert_matches!(
            task,
            Ok(Some(Task::ModifyObserve(ObserveOp::Stop { region, .. }))) if region.id == 42
        );
    }
}
