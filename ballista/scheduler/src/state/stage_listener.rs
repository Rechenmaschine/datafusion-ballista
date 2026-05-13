// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

//! CARMA stage-completion listener hook.
//!
//! Part of the CARMA-specific patch maintained on top of upstream Ballista.
//! Exposes process-wide listeners that fire once when a `RunningStage`
//! transitions to `SuccessfulStage`, with full read access to the stage's
//! plan, partition metadata, task timings, and metrics.
//!
//! Multiple listeners may be registered; they run in registration order from
//! `StaticExecutionGraph::succeed_stage`. Typical install sites:
//!   * the built-in metrics printer in `stage_metrics_printer` (env-gated),
//!   * CARMA's `carma-trace-ballista` ClusterTraceWriter (cluster runs).
//!
//! Both can be active in the same process.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::prelude::SessionConfig;

use crate::state::execution_stage::{StageOutput, TaskInfo};

/// Hook invoked once per stage when it transitions
/// `RunningStage → SuccessfulStage`. Fires only on the final successful
/// attempt — earlier failed-then-rerun attempts do not emit.
pub trait StageCompletionListener: Send + Sync + 'static {
    fn on_stage_succeeded(&self, ctx: StageCompletionContext<'_>);
}

/// Read-only snapshot of the stage at the moment of completion. All
/// references borrow from the freshly-constructed `SuccessfulStage` and are
/// only valid for the duration of the `on_stage_succeeded` call.
pub struct StageCompletionContext<'a> {
    pub job_id: &'a str,
    pub stage_id: usize,
    pub stage_attempt_num: usize,
    pub partitions: usize,
    pub output_links: &'a [usize],
    pub plan: &'a Arc<dyn ExecutionPlan>,
    pub inputs: &'a HashMap<usize, StageOutput>,
    pub task_infos: &'a [TaskInfo],
    pub stage_metrics: &'a [MetricsSet],
    pub session_config: &'a SessionConfig,
}

type ListenerVec = Vec<Arc<dyn StageCompletionListener>>;

fn registry() -> &'static RwLock<ListenerVec> {
    static REG: OnceLock<RwLock<ListenerVec>> = OnceLock::new();
    REG.get_or_init(|| RwLock::new(Vec::new()))
}

/// Append a process-wide stage-completion listener. Multiple listeners may
/// be registered and they fire in registration order.
pub fn add_stage_completion_listener(listener: Arc<dyn StageCompletionListener>) {
    let mut g = registry()
        .write()
        .expect("stage-listener registry poisoned");
    g.push(listener);
}

/// Snapshot of currently-installed listeners. Returns an empty `Vec` if
/// none are installed, so call sites can iterate unconditionally. Cloning
/// the `Arc`s under a short read lock keeps the hot-path lock-free during
/// dispatch.
pub(crate) fn stage_completion_listeners() -> ListenerVec {
    match registry().read() {
        Ok(g) => g.clone(),
        Err(p) => p.into_inner().clone(),
    }
}
