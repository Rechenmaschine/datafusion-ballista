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
//! This module is part of the small CARMA-specific patch maintained on top of
//! upstream Ballista. It exposes a process-wide listener that fires once when
//! a `RunningStage` transitions to `SuccessfulStage`, with full read access to
//! the stage's plan, partition metadata, task timings, and metrics.
//!
//! The listener is installed via `set_stage_completion_listener` once at
//! process startup and then runs inline from `StaticExecutionGraph::
//! succeed_stage`. Use cases: capturing per-stage execution traces for
//! offline analysis or simulation. CARMA's `carma-trace-ballista` crate
//! installs a listener that converts each event into a normalized
//! `BallistaTrace` record.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

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

static LISTENER: OnceLock<Arc<dyn StageCompletionListener>> = OnceLock::new();

/// Install the process-wide stage-completion listener. Returns `Err` if a
/// listener has already been installed (only one allowed per process).
pub fn set_stage_completion_listener(
    listener: Arc<dyn StageCompletionListener>,
) -> Result<(), Arc<dyn StageCompletionListener>> {
    LISTENER.set(listener)
}

pub(crate) fn stage_completion_listener() -> Option<&'static Arc<dyn StageCompletionListener>> {
    LISTENER.get()
}
