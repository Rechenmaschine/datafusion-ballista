//! `carma_plan_only` — produce per-stage physical plans for a SQL workload
//! without ever executing them.
//!
//! Takes a Redbench-style workload CSV (column `sql`, optionally filtered by
//! `query_type='select'`), registers the IMDB schema as Parquet external
//! tables, and pipes each SQL through DataFusion's planner + Ballista's
//! `DefaultDistributedPlanner`. For every produced stage it writes one JSONL
//! line in the same shape the runtime stage-trace writer emits, so the
//! existing Python analysis (substructure reuse, cache-opportunity curves,
//! etc.) drops in unchanged.
//!
//! Use case: cross-cluster substructure-reuse analysis without paying for
//! cluster execution. Plan all Redshift clusters' workloads on a laptop,
//! count subtree hashes, see the cache opportunity.

use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::time::Instant;

use ballista_scheduler::planner::{DefaultDistributedPlanner, DistributedPlanner};
use clap::Parser;
use csv::ReaderBuilder;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::displayable;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};

const IMDB_TABLES: &[&str] = &[
    "aka_name", "aka_title", "cast_info", "char_name", "comp_cast_type",
    "company_name", "company_type", "complete_cast", "info_type", "keyword",
    "kind_type", "link_type", "movie_companies", "movie_info", "movie_info_idx",
    "movie_keyword", "movie_link", "name", "person_info", "role_type", "title",
];

#[derive(Parser, Debug)]
#[command(name = "carma_plan_only", about = "Plan a SQL workload through Ballista without executing")]
struct Args {
    /// Directory containing one `<table>.parquet` per IMDB table.
    #[arg(long)]
    imdb_parquet_dir: PathBuf,

    /// Redbench-generated workload CSV (must have a `sql` column).
    #[arg(long)]
    workload_csv: PathBuf,

    /// Output JSONL path. One line per produced stage. `-` for stdout.
    #[arg(long, default_value = "stages.jsonl")]
    output: String,

    /// Cap on queries to plan. 0 means all.
    #[arg(long, default_value_t = 0)]
    limit: usize,

    /// Skip rows whose `query_type` isn't this value. Pass `""` to keep all rows.
    #[arg(long, default_value = "select")]
    only_query_type: String,

    /// Synthetic job_id prefix. Each query gets `<prefix>-<i>` so analysis can
    /// group stages back to their parent query.
    #[arg(long, default_value = "synth")]
    job_id_prefix: String,

    /// Stop after this many failures in a row (planner choking on a query).
    #[arg(long, default_value_t = 200)]
    max_consecutive_failures: usize,

    /// JSONL path where each query the planner fails on is recorded with its
    /// row number, query_id, SQL, and the error message. Useful for filing
    /// upstream Ballista bug reports. `-` for stderr, empty for off.
    #[arg(long, default_value = "failures.jsonl")]
    failed_out: String,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Open output sink.
    let mut writer: Box<dyn Write + Send> = if args.output == "-" {
        Box::new(BufWriter::new(std::io::stdout()))
    } else {
        Box::new(BufWriter::new(
            OpenOptions::new()
                .create(true).truncate(true).write(true)
                .open(&args.output)?,
        ))
    };

    // Failure sink (also JSONL, one record per failing query).
    let mut failed_writer: Option<Box<dyn Write + Send>> = if args.failed_out.is_empty() {
        None
    } else if args.failed_out == "-" {
        Some(Box::new(BufWriter::new(std::io::stderr())))
    } else {
        Some(Box::new(BufWriter::new(
            OpenOptions::new()
                .create(true).truncate(true).write(true)
                .open(&args.failed_out)?,
        )))
    };

    // Build the DataFusion session.
    let session_config = SessionConfig::new()
        .with_target_partitions(16)        // match cloudlab default
        .with_information_schema(true);
    let state = SessionStateBuilder::new()
        .with_config(session_config)
        .with_default_features()
        .build();
    let ctx = SessionContext::new_with_state(state);

    // Register every IMDB table as Parquet.
    for table in IMDB_TABLES {
        let path = args.imdb_parquet_dir.join(format!("{table}.parquet"));
        if !path.exists() {
            eprintln!("warning: {} not found, skipping", path.display());
            continue;
        }
        ctx.register_parquet(
            *table,
            path.to_str().unwrap(),
            ParquetReadOptions::default(),
        ).await?;
    }
    eprintln!("registered {} IMDB tables", IMDB_TABLES.len());

    // Stream the workload CSV.
    let f = std::fs::File::open(&args.workload_csv)?;
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_reader(f);

    let headers = rdr.headers()?.clone();
    let sql_idx = headers.iter().position(|c| c == "sql")
        .ok_or("workload CSV has no `sql` column")?;
    let qt_idx = headers.iter().position(|c| c == "query_type");
    let qid_idx = headers.iter().position(|c| c == "query_id");

    let mut n_total = 0usize;
    let mut n_planned = 0usize;
    let mut n_failed = 0usize;
    let mut consecutive_fail = 0usize;
    let mut total_stages = 0usize;
    let t0 = Instant::now();

    for (i, rec) in rdr.records().enumerate() {
        if args.limit > 0 && n_planned >= args.limit { break; }
        n_total += 1;
        let rec = match rec {
            Ok(r) => r,
            Err(e) => { eprintln!("row {i}: bad record: {e}"); continue; }
        };
        if !args.only_query_type.is_empty() {
            if let Some(j) = qt_idx {
                if rec.get(j).unwrap_or("") != args.only_query_type { continue; }
            }
        }
        let sql = rec.get(sql_idx).unwrap_or("").trim();
        if sql.is_empty() { continue; }
        let qid = qid_idx.and_then(|j| rec.get(j)).unwrap_or("?").to_string();
        let job_id = format!("{}-{}", args.job_id_prefix, n_planned);

        match plan_one(&ctx, sql).await {
            Ok(stage_plans) => {
                for (stage_id, plan_text) in stage_plans.into_iter().enumerate() {
                    let line = build_jsonl(&job_id, stage_id, &qid, &plan_text);
                    writer.write_all(line.as_bytes())?;
                    writer.write_all(b"\n")?;
                    total_stages += 1;
                }
                n_planned += 1;
                consecutive_fail = 0;
                if n_planned % 100 == 0 {
                    eprintln!(
                        "[{:>6}] planned={} stages={} failed={} ({:.0}/s)",
                        n_total, n_planned, total_stages, n_failed,
                        n_planned as f64 / t0.elapsed().as_secs_f64().max(0.001),
                    );
                }
            }
            Err(e) => {
                n_failed += 1;
                consecutive_fail += 1;
                let err_str = e.to_string();
                if consecutive_fail <= 3 {
                    eprintln!("row {i} qid={qid}: plan failed: {err_str}");
                }
                if let Some(fw) = failed_writer.as_mut() {
                    let mut line = String::with_capacity(256 + sql.len());
                    line.push_str("{\"row\":");
                    use std::fmt::Write as _;
                    let _ = write!(line, "{i}");
                    line.push_str(",\"query_id\":");
                    push_json_str(&mut line, &qid);
                    line.push_str(",\"error\":");
                    push_json_str(&mut line, &err_str);
                    line.push_str(",\"sql\":");
                    push_json_str(&mut line, sql);
                    line.push_str("}\n");
                    let _ = fw.write_all(line.as_bytes());
                }
                if consecutive_fail >= args.max_consecutive_failures {
                    eprintln!("too many consecutive failures, stopping");
                    break;
                }
            }
        }
    }

    writer.flush()?;
    if let Some(fw) = failed_writer.as_mut() { let _ = fw.flush(); }
    eprintln!(
        "done: read {} rows, planned {} queries -> {} stages, {} failed in {:.1}s",
        n_total, n_planned, total_stages, n_failed, t0.elapsed().as_secs_f64(),
    );
    if n_failed > 0 && !args.failed_out.is_empty() && args.failed_out != "-" {
        eprintln!("       failing queries logged to {}", args.failed_out);
    }
    Ok(())
}

async fn plan_one(
    ctx: &SessionContext,
    sql: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let df = ctx.sql(sql).await?;
    let logical = df.into_optimized_plan()?;
    let logical = ctx.state().optimize(&logical)?;
    let physical = ctx.state().create_physical_plan(&logical).await?;

    // The Ballista stage slicer has internal `assert!`s (planner.rs:~396)
    // that can fire on unusual broadcast-join shapes. Catch the panic so
    // one bad query doesn't kill the batch.
    let options = ctx.state().config().options().clone();
    let job_uuid = uuid::Uuid::new_v4().to_string();
    let stages = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut planner = DefaultDistributedPlanner::new();
        planner.plan_query_stages(&job_uuid, physical, &options)
    }))
    .map_err(|e| -> Box<dyn std::error::Error> {
        let msg = if let Some(s) = e.downcast_ref::<&str>() { (*s).to_string() }
                  else if let Some(s) = e.downcast_ref::<String>() { s.clone() }
                  else { "planner panicked".to_string() };
        format!("planner panic: {msg}").into()
    })??;

    Ok(stages.into_iter()
        .map(|s| displayable(s.as_ref()).indent(false).to_string())
        .collect())
}

/// Hand-roll JSON to match the stage-trace writer's record shape — just
/// enough fields for `analyze_substructure.py` and friends to consume.
fn build_jsonl(job_id: &str, stage_id: usize, qid: &str, plan: &str) -> String {
    let mut out = String::with_capacity(256 + plan.len() * 2);
    out.push_str("{\"kind\":\"stage_trace_synth\",\"job_id\":");
    push_json_str(&mut out, job_id);
    out.push_str(",\"stage_id\":");
    use std::fmt::Write;
    let _ = write!(out, "{stage_id}");
    out.push_str(",\"attempt\":0,\"partitions\":0,\"is_root\":");
    out.push_str(if stage_id == 0 { "true" } else { "false" });
    out.push_str(",\"output_links\":[],\"inputs\":{},\"tasks\":[],\"query_id\":");
    push_json_str(&mut out, qid);
    out.push_str(",\"plan\":");
    push_json_str(&mut out, plan);
    out.push('}');
    out
}

fn push_json_str(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                use std::fmt::Write;
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}
