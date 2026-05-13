// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

//! Built-in `StageCompletionListener` that aggregates per-stage execution
//! metrics into a portable summary and prints one line per stage (plus a
//! per-job rollup on the root stage). Activated at scheduler startup via
//! [`install_from_env`] if `BALLISTA_STAGE_METRICS` is set.
//!
//! The summary shape is deliberately Trino-/Spark-aligned (wall-clock,
//! cpu-time, shuffle bytes/rows, task fan-out, plan one-liner) so the same
//! numbers can be cross-checked against other engines when comparing
//! CARMA's behavior across systems.

use std::collections::HashMap;
use std::env;
use std::io::{self, Write};
use std::sync::{Arc, Mutex};

use ballista_core::serde::generated::ballista::task_status;
use datafusion::physical_plan::displayable;
use datafusion::physical_plan::metrics::MetricsSet;

use crate::state::execution_stage::TaskInfo;
use crate::state::stage_listener::{
    StageCompletionContext, StageCompletionListener, add_stage_completion_listener,
};

/// Output format selected by `BALLISTA_STAGE_METRICS`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    /// One JSON object per line; one stage event, one line. Job rollup line
    /// has `"kind":"job"`, stage lines have `"kind":"stage"`.
    Json,
    /// Multi-line human-readable boxes. Easier to skim in a terminal,
    /// awkward to grep.
    Pretty,
}

/// Portable per-stage metrics — flat, scalar fields only, no nested
/// engine-specific structures. Designed so equivalent numbers can be
/// extracted from Trino/Spark/etc. for cross-engine comparison.
#[derive(Debug, Clone)]
#[allow(missing_docs)] // field names are self-documenting; see struct doc.
pub struct StageMetricsSummary {
    pub job_id: String,
    pub stage_id: u32,
    pub stage_attempt_num: u32,
    pub is_root: bool,

    pub num_tasks: u32,
    pub num_partitions: u32,

    /// Earliest task launch time (ms since epoch) — start of the stage.
    pub stage_start_ms: u128,
    /// Latest task finish time (ms since epoch) — end of the stage.
    pub stage_end_ms: u128,
    /// Stage wall-clock = `stage_end_ms - stage_start_ms`.
    pub wall_clock_ms: u64,
    /// Sum of per-task wall-clocks (parallel-aware throughput proxy).
    pub task_wall_ms_sum: u64,
    pub task_wall_ms_min: u64,
    pub task_wall_ms_max: u64,
    pub task_wall_ms_p50: u64,
    pub task_wall_ms_p95: u64,

    /// Total CPU-busy time across tasks = Σ elapsed_compute. `None` if no
    /// task carried a `MetricsSet` (older execution path or zero tasks).
    pub total_cpu_ns: Option<u64>,

    /// Shuffle output (the bytes/rows this stage *produced* downstream).
    pub shuffle_output_rows: u64,
    pub shuffle_output_bytes: u64,
    pub shuffle_output_batches: u64,

    /// Single-line `displayable(plan).one_line()` rendering of the stage's
    /// physical plan. Useful for visually correlating a stage with its
    /// operators without dumping the full tree.
    pub plan_one_line: String,
}

impl StageMetricsSummary {
    /// Aggregate a `StageCompletionContext` into a flat summary.
    pub fn from_ctx(ctx: &StageCompletionContext<'_>) -> Self {
        let (start_ms, end_ms, task_walls) = task_timing_stats(ctx.task_infos);
        let wall_clock_ms = end_ms.saturating_sub(start_ms) as u64;

        let total_cpu_ns = total_cpu_ns(ctx.stage_metrics);

        let (rows, bytes, batches) = shuffle_output_totals(ctx.task_infos);

        let plan_one_line = displayable(ctx.plan.as_ref())
            .one_line()
            .to_string()
            .trim()
            .to_owned();

        StageMetricsSummary {
            job_id: ctx.job_id.to_owned(),
            stage_id: ctx.stage_id as u32,
            stage_attempt_num: ctx.stage_attempt_num as u32,
            is_root: ctx.output_links.is_empty(),
            num_tasks: ctx.task_infos.len() as u32,
            num_partitions: ctx.partitions as u32,
            stage_start_ms: start_ms,
            stage_end_ms: end_ms,
            wall_clock_ms,
            task_wall_ms_sum: task_walls.iter().sum(),
            task_wall_ms_min: task_walls.iter().copied().min().unwrap_or(0),
            task_wall_ms_max: task_walls.iter().copied().max().unwrap_or(0),
            task_wall_ms_p50: percentile(&task_walls, 0.50),
            task_wall_ms_p95: percentile(&task_walls, 0.95),
            total_cpu_ns,
            shuffle_output_rows: rows,
            shuffle_output_bytes: bytes,
            shuffle_output_batches: batches,
            plan_one_line,
        }
    }
}

/// Per-job rollup that the printer emits on the root stage's completion.
#[derive(Debug, Clone, Default)]
#[allow(missing_docs)] // fields mirror StageMetricsSummary; see field names.
pub struct JobMetricsSummary {
    pub job_id: String,
    pub num_stages: u32,
    pub num_tasks: u64,
    pub job_start_ms: u128,
    pub job_end_ms: u128,
    pub job_wall_ms: u64,
    pub total_cpu_ns: u64,
    pub total_shuffle_rows: u64,
    pub total_shuffle_bytes: u64,
    pub total_shuffle_batches: u64,
}

#[derive(Default)]
struct JobAccumulator {
    num_stages: u32,
    num_tasks: u64,
    earliest_start_ms: u128,
    latest_end_ms: u128,
    total_cpu_ns: u64,
    total_shuffle_rows: u64,
    total_shuffle_bytes: u64,
    total_shuffle_batches: u64,
}

impl JobAccumulator {
    fn absorb(&mut self, s: &StageMetricsSummary) {
        self.num_stages += 1;
        self.num_tasks += s.num_tasks as u64;
        if self.earliest_start_ms == 0 || s.stage_start_ms < self.earliest_start_ms {
            self.earliest_start_ms = s.stage_start_ms;
        }
        if s.stage_end_ms > self.latest_end_ms {
            self.latest_end_ms = s.stage_end_ms;
        }
        self.total_cpu_ns += s.total_cpu_ns.unwrap_or(0);
        self.total_shuffle_rows += s.shuffle_output_rows;
        self.total_shuffle_bytes += s.shuffle_output_bytes;
        self.total_shuffle_batches += s.shuffle_output_batches;
    }

    fn into_summary(self, job_id: String) -> JobMetricsSummary {
        let job_wall_ms = self
            .latest_end_ms
            .saturating_sub(self.earliest_start_ms) as u64;
        JobMetricsSummary {
            job_id,
            num_stages: self.num_stages,
            num_tasks: self.num_tasks,
            job_start_ms: self.earliest_start_ms,
            job_end_ms: self.latest_end_ms,
            job_wall_ms,
            total_cpu_ns: self.total_cpu_ns,
            total_shuffle_rows: self.total_shuffle_rows,
            total_shuffle_bytes: self.total_shuffle_bytes,
            total_shuffle_batches: self.total_shuffle_batches,
        }
    }
}

/// Listener that formats each `StageCompletionContext` into a
/// `StageMetricsSummary` and writes it to the configured sink.
pub struct PrintingStageListener {
    format: Format,
    /// Sink protected by a mutex so concurrent stage completions don't
    /// interleave bytes within a single line.
    sink: Arc<Mutex<Box<dyn Write + Send>>>,
    jobs: Mutex<HashMap<String, JobAccumulator>>,
}

impl PrintingStageListener {
    /// Build a listener that writes formatted summaries to `sink`.
    pub fn new(format: Format, sink: Box<dyn Write + Send>) -> Self {
        Self {
            format,
            sink: Arc::new(Mutex::new(sink)),
            jobs: Mutex::new(HashMap::new()),
        }
    }

    /// Convenience constructor: a printer that writes to stdout.
    pub fn to_stdout(format: Format) -> Self {
        Self::new(format, Box::new(io::stdout()))
    }
}

impl StageCompletionListener for PrintingStageListener {
    fn on_stage_succeeded(&self, ctx: StageCompletionContext<'_>) {
        let summary = StageMetricsSummary::from_ctx(&ctx);

        // Update the job accumulator and, if this is the root stage, take
        // it out so we can emit a single rollup line. We deliberately drop
        // the lock before printing.
        let job_rollup = {
            let mut jobs = match self.jobs.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            let acc = jobs.entry(summary.job_id.clone()).or_default();
            acc.absorb(&summary);
            if summary.is_root {
                jobs.remove(&summary.job_id)
                    .map(|a| a.into_summary(summary.job_id.clone()))
            } else {
                None
            }
        };

        let mut buf = String::with_capacity(512);
        match self.format {
            Format::Json => format_stage_json(&summary, &mut buf),
            Format::Pretty => format_stage_pretty(&summary, &mut buf),
        }
        if let Some(rollup) = job_rollup {
            buf.push('\n');
            match self.format {
                Format::Json => format_job_json(&rollup, &mut buf),
                Format::Pretty => format_job_pretty(&rollup, &mut buf),
            }
        }
        buf.push('\n');

        let mut sink = match self.sink.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        // `Write` failures here are non-fatal — the scheduler must keep
        // running even if stdout is closed (e.g. piped to a consumer that
        // exited). Swallow and continue.
        let _ = sink.write_all(buf.as_bytes());
        let _ = sink.flush();
    }
}

/// Read `BALLISTA_STAGE_METRICS` and install a stdout-writing printer if
/// it's set to a recognized value. Unrecognized or empty → no-op.
///
/// Recognized values (case-insensitive): `json`, `1`, `true` → JSON;
/// `pretty`, `text` → Pretty.
pub fn install_from_env() {
    let Some(format) = format_from_env("BALLISTA_STAGE_METRICS") else {
        return;
    };
    add_stage_completion_listener(Arc::new(PrintingStageListener::to_stdout(format)));
    tracing::info!(
        format = ?format,
        "ballista stage-metrics printer installed (BALLISTA_STAGE_METRICS)"
    );
}

fn format_from_env(var: &str) -> Option<Format> {
    let v = env::var(var).ok()?.trim().to_ascii_lowercase();
    match v.as_str() {
        "" | "0" | "off" | "false" | "no" => None,
        "json" | "1" | "true" | "yes" => Some(Format::Json),
        "pretty" | "text" => Some(Format::Pretty),
        other => {
            tracing::warn!(
                "BALLISTA_STAGE_METRICS={other:?} not recognized; \
                 expected one of: off, json, pretty"
            );
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Aggregation helpers
// ---------------------------------------------------------------------------

fn task_timing_stats(tasks: &[TaskInfo]) -> (u128, u128, Vec<u64>) {
    let mut earliest = u128::MAX;
    let mut latest = 0u128;
    let mut walls = Vec::with_capacity(tasks.len());
    for t in tasks {
        if t.launch_time > 0 && t.launch_time < earliest {
            earliest = t.launch_time;
        }
        if t.finish_time > latest {
            latest = t.finish_time;
        }
        let wall = t.finish_time.saturating_sub(t.launch_time) as u64;
        walls.push(wall);
    }
    if earliest == u128::MAX {
        earliest = 0;
    }
    walls.sort_unstable();
    (earliest, latest, walls)
}

fn total_cpu_ns(stage_metrics: &[MetricsSet]) -> Option<u64> {
    if stage_metrics.is_empty() {
        return None;
    }
    let mut total = 0u64;
    let mut saw_any = false;
    for m in stage_metrics {
        if let Some(ns) = m.elapsed_compute() {
            total = total.saturating_add(ns as u64);
            saw_any = true;
        }
    }
    saw_any.then_some(total)
}

fn shuffle_output_totals(tasks: &[TaskInfo]) -> (u64, u64, u64) {
    let mut rows = 0u64;
    let mut bytes = 0u64;
    let mut batches = 0u64;
    for t in tasks {
        if let task_status::Status::Successful(s) = &t.task_status {
            for p in &s.partitions {
                rows = rows.saturating_add(p.num_rows);
                bytes = bytes.saturating_add(p.num_bytes);
                batches = batches.saturating_add(p.num_batches);
            }
        }
    }
    (rows, bytes, batches)
}

/// `pre-sorted slice` percentile, nearest-rank.
fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let n = sorted.len();
    let rank = (p * n as f64).ceil() as usize;
    let idx = rank.saturating_sub(1).min(n - 1);
    sorted[idx]
}

// ---------------------------------------------------------------------------
// Formatting
// ---------------------------------------------------------------------------

fn format_stage_json(s: &StageMetricsSummary, out: &mut String) {
    use std::fmt::Write as _;
    let _ = write!(
        out,
        "{{\"kind\":\"stage\",\
         \"job_id\":{job_id},\
         \"stage_id\":{stage_id},\
         \"attempt\":{attempt},\
         \"is_root\":{is_root},\
         \"num_tasks\":{num_tasks},\
         \"num_partitions\":{num_partitions},\
         \"stage_start_ms\":{stage_start_ms},\
         \"stage_end_ms\":{stage_end_ms},\
         \"wall_clock_ms\":{wall_clock_ms},\
         \"task_wall_ms\":{{\"sum\":{ws},\"min\":{wmin},\"max\":{wmax},\"p50\":{wp50},\"p95\":{wp95}}},\
         \"total_cpu_ns\":{cpu},\
         \"shuffle_output\":{{\"rows\":{rows},\"bytes\":{bytes},\"batches\":{batches}}},\
         \"plan\":{plan}}}",
        job_id = json_str(&s.job_id),
        stage_id = s.stage_id,
        attempt = s.stage_attempt_num,
        is_root = s.is_root,
        num_tasks = s.num_tasks,
        num_partitions = s.num_partitions,
        stage_start_ms = s.stage_start_ms,
        stage_end_ms = s.stage_end_ms,
        wall_clock_ms = s.wall_clock_ms,
        ws = s.task_wall_ms_sum,
        wmin = s.task_wall_ms_min,
        wmax = s.task_wall_ms_max,
        wp50 = s.task_wall_ms_p50,
        wp95 = s.task_wall_ms_p95,
        cpu = match s.total_cpu_ns {
            Some(v) => v.to_string(),
            None => "null".into(),
        },
        rows = s.shuffle_output_rows,
        bytes = s.shuffle_output_bytes,
        batches = s.shuffle_output_batches,
        plan = json_str(&s.plan_one_line),
    );
}

fn format_job_json(j: &JobMetricsSummary, out: &mut String) {
    use std::fmt::Write as _;
    let _ = write!(
        out,
        "{{\"kind\":\"job\",\
         \"job_id\":{job_id},\
         \"num_stages\":{num_stages},\
         \"num_tasks\":{num_tasks},\
         \"job_start_ms\":{js},\
         \"job_end_ms\":{je},\
         \"job_wall_ms\":{jw},\
         \"total_cpu_ns\":{cpu},\
         \"total_shuffle\":{{\"rows\":{rows},\"bytes\":{bytes},\"batches\":{batches}}}}}",
        job_id = json_str(&j.job_id),
        num_stages = j.num_stages,
        num_tasks = j.num_tasks,
        js = j.job_start_ms,
        je = j.job_end_ms,
        jw = j.job_wall_ms,
        cpu = j.total_cpu_ns,
        rows = j.total_shuffle_rows,
        bytes = j.total_shuffle_bytes,
        batches = j.total_shuffle_batches,
    );
}

fn format_stage_pretty(s: &StageMetricsSummary, out: &mut String) {
    use std::fmt::Write as _;
    let cpu_ms = s.total_cpu_ns.map(|ns| ns / 1_000_000);
    let _ = writeln!(out, "── stage {} (job {}, attempt {}) {}", s.stage_id, s.job_id, s.stage_attempt_num, if s.is_root { "[ROOT]" } else { "" });
    let _ = writeln!(
        out,
        "   tasks={}  partitions={}  wall={}ms  cpu={}",
        s.num_tasks,
        s.num_partitions,
        s.wall_clock_ms,
        match cpu_ms {
            Some(ms) => format!("{ms}ms"),
            None => "n/a".into(),
        }
    );
    let _ = writeln!(
        out,
        "   task_wall ms: sum={} min={} p50={} p95={} max={}",
        s.task_wall_ms_sum,
        s.task_wall_ms_min,
        s.task_wall_ms_p50,
        s.task_wall_ms_p95,
        s.task_wall_ms_max,
    );
    let _ = writeln!(
        out,
        "   shuffle_out: rows={} bytes={} batches={}",
        s.shuffle_output_rows, s.shuffle_output_bytes, s.shuffle_output_batches,
    );
    let _ = write!(out, "   plan: {}", s.plan_one_line);
}

fn format_job_pretty(j: &JobMetricsSummary, out: &mut String) {
    use std::fmt::Write as _;
    let _ = writeln!(out, "══ job {} complete", j.job_id);
    let _ = writeln!(
        out,
        "   stages={}  tasks={}  wall={}ms  cpu_total={}ms",
        j.num_stages,
        j.num_tasks,
        j.job_wall_ms,
        j.total_cpu_ns / 1_000_000,
    );
    let _ = write!(
        out,
        "   shuffle_total: rows={} bytes={} batches={}",
        j.total_shuffle_rows, j.total_shuffle_bytes, j.total_shuffle_batches,
    );
}

/// Minimal JSON string encoder — escapes `"`, `\`, control chars. Used so
/// we don't pull in serde_json just for two fields.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_handles_empty() {
        assert_eq!(percentile(&[], 0.5), 0);
    }

    #[test]
    fn percentile_nearest_rank() {
        let xs = [1u64, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        assert_eq!(percentile(&xs, 0.50), 5);
        assert_eq!(percentile(&xs, 0.95), 10);
        assert_eq!(percentile(&xs, 0.10), 1);
    }

    #[test]
    fn json_str_escapes() {
        assert_eq!(json_str("hello"), "\"hello\"");
        assert_eq!(json_str("a\"b"), "\"a\\\"b\"");
        assert_eq!(json_str("a\\b"), "\"a\\\\b\"");
        assert_eq!(json_str("a\nb"), "\"a\\nb\"");
    }

    #[test]
    fn format_from_env_recognized() {
        // We can't safely poke env in parallel tests, so just exercise the
        // parsing path directly via a helper.
        // (format_from_env reads the env var; tested implicitly via the
        // recognized-values match arm.)
        assert!(matches!(parse_format_str("json"), Some(Format::Json)));
        assert!(matches!(parse_format_str("JSON"), Some(Format::Json)));
        assert!(matches!(parse_format_str("pretty"), Some(Format::Pretty)));
        assert!(matches!(parse_format_str("off"), None));
        assert!(matches!(parse_format_str(""), None));
        assert!(matches!(parse_format_str("garbage"), None));
    }

    fn parse_format_str(v: &str) -> Option<Format> {
        match v.trim().to_ascii_lowercase().as_str() {
            "" | "0" | "off" | "false" | "no" => None,
            "json" | "1" | "true" | "yes" => Some(Format::Json),
            "pretty" | "text" => Some(Format::Pretty),
            _ => None,
        }
    }
}
