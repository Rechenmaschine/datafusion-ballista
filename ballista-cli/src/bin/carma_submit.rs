//! Work-conserving, closed-loop query submitter for the CARMA Ballista benchmark.
//!
//! Spawns `--concurrency` persistent client sessions (one remote SessionContext
//! each, tables registered once via `--setup`), then has them pull queries from
//! one shared, arrival-ordered queue until it drains. Each session runs one
//! query at a time and grabs the next the instant it finishes, so exactly
//! `--concurrency` queries are in flight throughout -- uniform load, no
//! per-query reconnect or re-registration (unlike a fresh `ballista-cli` per
//! query).

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use ballista::prelude::SessionContextExt;
use ballista_core::object_store::{runtime_env_with_s3_support, session_config_with_s3_support};
use clap::Parser;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;

type BoxErr = Box<dyn std::error::Error + Send + Sync>;

#[derive(Parser, Debug)]
#[command(about = "Closed-loop, work-conserving query submitter (CARMA benchmark)")]
struct Args {
    #[arg(long)]
    host: String,
    #[arg(long, default_value_t = 50050)]
    port: u16,
    #[arg(long, default_value_t = 1)]
    concurrency: usize,
    /// Directory of per-query .sql files, run in sorted (arrival) order.
    #[arg(long)]
    queries_dir: PathBuf,
    /// SQL run once per session before queries (e.g. CREATE EXTERNAL TABLE ...).
    #[arg(long)]
    setup: Option<PathBuf>,
}

async fn connect(host: &str, port: u16) -> Result<SessionContext, BoxErr> {
    // S3-enabled session so `CREATE EXTERNAL TABLE ... LOCATION 's3://...'`
    // infers schema client-side (object store built from AWS_* env). Mirrors the
    // scheduler/executor registry; harmless for local-path tables.
    let cfg = session_config_with_s3_support();
    let rt = runtime_env_with_s3_support(&cfg)?;
    let state = SessionStateBuilder::new()
        .with_config(cfg)
        .with_runtime_env(rt)
        .with_default_features()
        .build();
    Ok(SessionContext::remote_with_state(&format!("df://{host}:{port}"), state).await?)
}

async fn run(ctx: &SessionContext, sql: &str) -> Result<(), BoxErr> {
    let _ = ctx.sql(sql).await?.collect().await?;
    Ok(())
}

fn split_statements(sql: &str) -> Vec<String> {
    sql.split(';')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty() && !s.starts_with("--"))
        .map(|s| s.to_string())
        .collect()
}

#[tokio::main]
async fn main() -> Result<(), BoxErr> {
    let args = Args::parse();

    let mut files: Vec<PathBuf> = fs::read_dir(&args.queries_dir)?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().map(|x| x == "sql").unwrap_or(false))
        .collect();
    files.sort();
    let total = files.len();
    let files = Arc::new(files);

    let setup = Arc::new(match &args.setup {
        Some(p) => split_statements(&fs::read_to_string(p)?),
        None => Vec::new(),
    });

    let k = args.concurrency.max(1);
    eprintln!(
        "carma_submit: {total} queries, concurrency {k}, scheduler {}:{}",
        args.host, args.port
    );

    let next = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(AtomicUsize::new(0));
    let failed = Arc::new(AtomicUsize::new(0));
    let start = Instant::now();

    let mut workers = Vec::new();
    for w in 0..k {
        let (files, setup, next, done, failed) = (
            files.clone(),
            setup.clone(),
            next.clone(),
            done.clone(),
            failed.clone(),
        );
        let host = args.host.clone();
        let port = args.port;
        workers.push(tokio::spawn(async move {
            let ctx = match connect(&host, port).await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("worker {w}: connect failed: {e}");
                    return;
                }
            };
            for stmt in setup.iter() {
                if let Err(e) = run(&ctx, stmt).await {
                    eprintln!("worker {w}: setup failed: {e}");
                    return;
                }
            }
            loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                if i >= files.len() {
                    break;
                }
                match fs::read_to_string(&files[i]) {
                    Ok(sql) => {
                        if let Err(e) = run(&ctx, &sql).await {
                            eprintln!("query {:?} failed: {e}", files[i]);
                            failed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(e) => {
                        eprintln!("read {:?}: {e}", files[i]);
                        failed.fetch_add(1, Ordering::Relaxed);
                    }
                }
                let d = done.fetch_add(1, Ordering::Relaxed) + 1;
                if d % 100 == 0 {
                    eprintln!("  completed {d}/{total}");
                }
            }
        }));
    }
    for h in workers {
        let _ = h.await;
    }

    eprintln!(
        "carma_submit: done {}/{} ({} failed) in {:.1}s",
        done.load(Ordering::Relaxed),
        total,
        failed.load(Ordering::Relaxed),
        start.elapsed().as_secs_f64()
    );
    Ok(())
}
