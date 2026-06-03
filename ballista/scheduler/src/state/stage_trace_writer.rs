// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

//! CARMA stage-trace writer.
//!
//! Env-gated `StageCompletionListener` that emits one JSONL record per
//! successful stage to a file. Captures per-task timing + executor placement,
//! the full physical plan tree of the stage, the upstream stage outputs that
//! fed it (with executor placement of each input partition), and per-task
//! shuffle-write partition stats. Designed for thesis-grade cost-model traces.
//!
//! Companion to `stage_metrics_printer` — that one emits aggregated per-stage
//! summaries; this one emits the full per-task record. Both can be installed
//! at once.
//!
//! Install path: `install_from_env()` in `bin/main.rs`. Gating env var:
//!   `BALLISTA_STAGE_TRACE_FILE=/var/log/carma/stages.jsonl`
//! The file is opened in append mode at install time; each line is flushed
//! synchronously after write so a crash loses at most the in-flight line.

use std::env;
use std::fs::OpenOptions;
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

use ballista_core::serde::protobuf::task_status;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::metrics::{MetricValue, MetricsSet};

use crate::state::execution_stage::TaskInfo;
use crate::state::stage_listener::{
    StageCompletionContext, StageCompletionListener, add_stage_completion_listener,
};

/// One JSONL line per successful stage. The line format is documented inline
/// in `write_record`; the writer holds a single mutex-guarded `BufWriter`
/// so concurrent stage completions don't interleave bytes.
pub struct StageTraceWriter {
    sink: Arc<Mutex<BufWriter<Box<dyn Write + Send>>>>,
}

impl StageTraceWriter {
    /// Build a writer that emits JSONL to `sink`. Each `on_stage_succeeded`
    /// call writes one line plus a trailing newline and flushes.
    pub fn new(sink: Box<dyn Write + Send>) -> Self {
        Self {
            sink: Arc::new(Mutex::new(BufWriter::new(sink))),
        }
    }

    /// Convenience constructor: open `path` in append+create mode.
    pub fn to_file(path: &Path) -> io::Result<Self> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        Ok(Self::new(Box::new(f)))
    }
}

impl StageCompletionListener for StageTraceWriter {
    fn on_stage_succeeded(&self, ctx: StageCompletionContext<'_>) {
        let mut buf = String::with_capacity(2048);
        write_record(&mut buf, &ctx);
        buf.push('\n');

        let mut sink = match self.sink.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        // Listener must not crash the scheduler — swallow I/O errors.
        let _ = sink.write_all(buf.as_bytes());
        let _ = sink.flush();
    }
}

/// Read `BALLISTA_STAGE_TRACE_FILE` and install a writer if it's set to a
/// non-empty path. Unset or empty → no-op (so production builds don't pay).
pub fn install_from_env() {
    let path = match env::var("BALLISTA_STAGE_TRACE_FILE") {
        Ok(s) if !s.trim().is_empty() => s.trim().to_owned(),
        _ => return,
    };
    let writer = match StageTraceWriter::to_file(Path::new(&path)) {
        Ok(w) => w,
        Err(e) => {
            tracing::error!(
                path = %path,
                error = %e,
                "BALLISTA_STAGE_TRACE_FILE set but writer init failed; trace disabled"
            );
            return;
        }
    };
    add_stage_completion_listener(Arc::new(writer));
    tracing::info!(
        path = %path,
        "carma stage-trace writer installed (BALLISTA_STAGE_TRACE_FILE)"
    );
}

// ---------------------------------------------------------------------------
// Record serialization
// ---------------------------------------------------------------------------

fn write_record(out: &mut String, ctx: &StageCompletionContext<'_>) {
    use std::fmt::Write as _;

    let _ = write!(
        out,
        "{{\"kind\":\"stage_trace\",\
         \"job_id\":{job},\
         \"stage_id\":{sid},\
         \"attempt\":{att},\
         \"partitions\":{parts},\
         \"is_root\":{root},\
         \"output_links\":[",
        job = json_str(ctx.job_id),
        sid = ctx.stage_id,
        att = ctx.stage_attempt_num,
        parts = ctx.partitions,
        root = ctx.output_links.is_empty(),
    );
    let mut first = true;
    for ol in ctx.output_links {
        if !first {
            out.push(',');
        }
        let _ = write!(out, "{}", ol);
        first = false;
    }
    out.push_str("],\"inputs\":{");
    let mut first = true;
    // Stable iteration order for readability — sort by parent stage id.
    let mut parents: Vec<(&usize, _)> = ctx.inputs.iter().collect();
    parents.sort_by_key(|(k, _)| **k);
    for (parent_stage_id, stage_output) in parents {
        if !first {
            out.push(',');
        }
        let _ = write!(out, "\"{}\":[", parent_stage_id);
        let mut inner_first = true;
        // partition_locations: HashMap<usize, Vec<PartitionLocation>>
        let mut parts: Vec<(&usize, _)> =
            stage_output.partition_locations.iter().collect();
        parts.sort_by_key(|(k, _)| **k);
        for (part, locs) in parts {
            for loc in locs {
                if !inner_first {
                    out.push(',');
                }
                let _ = write!(
                    out,
                    "{{\"part\":{},\"map_part\":{},\"exec\":{}}}",
                    part,
                    loc.map_partition_id,
                    json_str(&loc.executor_meta.id),
                );
                inner_first = false;
            }
        }
        out.push(']');
        first = false;
    }
    out.push_str("},\"tasks\":[");
    let mut first = true;
    // `task_infos` is dense and indexed by partition, so the loop index `i` is
    // this task's partition id. `stage_metrics` is indexed by *operator*, with
    // each metric tagged by partition — so per-task compute is the sum of
    // ElapsedCompute across all operators for partition `i` (NOT
    // `stage_metrics[i]`, which would pick an unrelated operator).
    for (i, t) in ctx.task_infos.iter().enumerate() {
        if !first {
            out.push(',');
        }
        write_task(out, t, task_compute_ns(ctx.stage_metrics, i));
        first = false;
    }
    out.push_str("],\"plan\":");
    let plan_str = DisplayableExecutionPlan::new(ctx.plan.as_ref())
        .indent(false)
        .to_string();
    out.push_str(&json_str(&plan_str));
    out.push('}');
}

/// Sum DataFusion `ElapsedCompute` (ns) across every operator of the stage that
/// recorded it for `partition` — i.e. one task's compute. Returns `None` if no
/// operator reported compute for that partition.
///
/// Note: DataFusion's `elapsed_compute` is a wall-clock "busy-while-polling"
/// timer, so for stages that read shuffle inputs it includes fetch/wait time and
/// is not pure CPU; it is still the correct per-task aggregate of that metric.
fn task_compute_ns(stage_metrics: &[MetricsSet], partition: usize) -> Option<u64> {
    let mut total: u64 = 0;
    let mut seen = false;
    for op in stage_metrics {
        for metric in op.iter() {
            if metric.partition() == Some(partition) {
                if let MetricValue::ElapsedCompute(time) = metric.value() {
                    total = total.saturating_add(time.value() as u64);
                    seen = true;
                }
            }
        }
    }
    seen.then_some(total)
}

fn write_task(out: &mut String, t: &TaskInfo, compute_ns: Option<u64>) {
    use std::fmt::Write as _;

    let (status_label, executor_id, partitions) = match &t.task_status {
        task_status::Status::Successful(s) => {
            ("Successful", Some(s.executor_id.as_str()), Some(&s.partitions))
        }
        task_status::Status::Running(r) => {
            // Listener fires on the winning attempt, but defend against the
            // event being routed here mid-state.
            ("Running", Some(r.executor_id.as_str()), None)
        }
        task_status::Status::Failed(_) => ("Failed", None, None),
    };

    let _ = write!(
        out,
        "{{\"task_id\":{tid},\
         \"status\":{status},\
         \"executor_id\":{exec},\
         \"scheduled_ms\":{sch},\
         \"launch_ms\":{lau},\
         \"start_exec_ms\":{se},\
         \"end_exec_ms\":{ee},\
         \"finish_ms\":{fin},\
         \"elapsed_compute_ns\":{cpu},\
         \"partitions\":[",
        tid = t.task_id,
        status = json_str(status_label),
        exec = match executor_id {
            Some(e) => json_str(e),
            None => "null".into(),
        },
        sch = t.scheduled_time,
        lau = t.launch_time,
        se = t.start_exec_time,
        ee = t.end_exec_time,
        fin = t.finish_time,
        cpu = compute_ns
            .map(|ns| ns.to_string())
            .unwrap_or_else(|| "null".into()),
    );
    if let Some(ps) = partitions {
        let mut first = true;
        for p in ps {
            if !first {
                out.push(',');
            }
            let _ = write!(
                out,
                "{{\"id\":{id},\"rows\":{r},\"bytes\":{b},\"batches\":{ba}}}",
                id = p.partition_id,
                r = p.num_rows,
                b = p.num_bytes,
                ba = p.num_batches,
            );
            first = false;
        }
    }
    out.push_str("]}");
}

/// Minimal JSON string encoder — escapes `"`, `\`, control chars. Avoids
/// pulling in `serde_json` for this single use site (matches the existing
/// `stage_metrics_printer` style).
fn json_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                use std::fmt::Write as _;
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}
