# CARMA fork of apache/datafusion-ballista

This repo is a small fork of
[apache/datafusion-ballista](https://github.com/apache/datafusion-ballista),
maintained for the **CARMA** stage-level cache simulator. CARMA needs
visibility into per-stage completion events that upstream Ballista
doesn't expose, so we keep a tiny in-tree patch.

- **Upstream base:** `de0aca3da0c959149711d62cfd1266bc98903095`
  (commit "chore(deps): bump rand from 0.9.4 to 0.10.1", #1565)
- **Patch branch:** `carma-listener`
- **Default branch:** still `main` (mirrors upstream)

The patch lives as one extra commit on top of the upstream base. Run
`git log carma-listener ^main` to see it.

## What the patch does

Exposes a per-stage completion event via a process-wide listener that
the scheduler fires once per successful stage transition. Three files
touched:

```
   NEW    ballista/scheduler/src/state/stage_listener.rs
              StageCompletionListener trait
              StageCompletionContext<'a> snapshot type
              OnceLock-backed process-wide listener registry

   MOD    ballista/scheduler/src/state/mod.rs
              `pub mod stage_listener;`

   MOD    ballista/scheduler/src/state/execution_graph.rs
              StaticExecutionGraph::succeed_stage now fires the listener
              once per stage, before reinserting it as Successful.
              (Search for `CARMA out-of-tree hook` to find the call site.)
```

No proto/gRPC changes. Stock Ballista executors at the same git rev
remain protocol-compatible with this patched scheduler.

## Why a fork rather than upstream PR

CARMA's design goals (frozen-rev replays, capture-vs-simulate split)
are too narrow to motivate an upstream contribution, and the thesis is
short-horizon. The patch is small enough that re-rebasing on Ballista
bumps is cheap.

## Updating to a newer upstream rev

```bash
git fetch origin
git checkout main
git merge --ff-only origin/main           # keep main aligned with upstream
git checkout carma-listener
git rebase main                            # forward-port the patch
# resolve conflicts (the touch points are tiny — succeed_stage()
# in execution_graph.rs and the mod.rs declaration)
git push --force-with-lease origin carma-listener
```

After that, update `MeasurementManifest::ballista_git_rev` (in
`carma-plan/src/manifest.rs` of the carma-simulator repo) to the new
rev so traces remain traceable to the version they were captured
against.

## Building

This is a normal cargo workspace — same as upstream. From the repo
root:

```bash
cargo build -p ballista-scheduler -p ballista-executor --release
```

The patched scheduler binary is `target/release/ballista-scheduler`.
The carma-simulator's `carma_scheduler` binary wraps this and
installs a CARMA `ClusterTraceWriter` listener at startup.
