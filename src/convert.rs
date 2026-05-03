use std::path::Path;

use anyhow::{Context, Result, bail};
use rayon::ThreadPoolBuilder;
use rayon::prelude::*;

use crate::cli::Cli;
use crate::discovery::{Job, discover_jobs};
use crate::formats::{RegionFormat, encode_region, read_region, region_uses_external_chunks};
use crate::io_util::atomic_write;

#[derive(Debug)]
pub struct RunSummary {
    pub thread_count: usize,
    pub target_format: RegionFormat,
    pub compression_level: i32,
    pub total_jobs: usize,
    pub successful_jobs: usize,
    pub failed_jobs: usize,
    pub total_chunks_written: usize,
    pub total_discarded_chunks: usize,
    pub total_warnings: usize,
    pub job_reports: Vec<JobReport>,
}

#[derive(Debug)]
pub enum JobReport {
    Success(JobSuccess),
    Failure(JobFailure),
}

#[derive(Debug)]
pub struct JobSuccess {
    pub source_file: String,
    pub destination_file: String,
    pub source_format: RegionFormat,
    pub target_format: RegionFormat,
    pub chunk_count: usize,
    pub discarded_chunks: usize,
    pub warnings: Vec<String>,
}

#[derive(Debug)]
pub struct JobFailure {
    pub source_file: String,
    pub destination_file: String,
    pub source_format: RegionFormat,
    pub target_format: RegionFormat,
    pub error: String,
}

pub fn run(cli: Cli) -> Result<RunSummary> {
    let thread_count = cli.thread_count()?;
    let target_format = cli.target_format();
    let compression_level = cli.resolved_compression_level()?;
    let jobs = discover_jobs(
        &cli.inputs,
        &cli.output,
        cli.forced_source_format(),
        target_format,
    )?;

    let pool = ThreadPoolBuilder::new()
        .num_threads(thread_count)
        .build()
        .context("failed to build the Rayon thread pool")?;

    let mut job_reports = pool.install(|| {
        jobs.par_iter()
            .map(|job| process_job(job, target_format, compression_level))
            .collect::<Vec<_>>()
    });
    job_reports.sort_by_key(|report| match report {
        JobReport::Success(report) => report.source_file.clone(),
        JobReport::Failure(report) => report.source_file.clone(),
    });

    let mut successful_jobs = 0;
    let mut failed_jobs = 0;
    let mut total_chunks_written = 0;
    let mut total_discarded_chunks = 0;
    let mut total_warnings = 0;

    for report in &job_reports {
        match report {
            JobReport::Success(report) => {
                successful_jobs += 1;
                total_chunks_written += report.chunk_count;
                total_discarded_chunks += report.discarded_chunks;
                total_warnings += report.warnings.len();
            }
            JobReport::Failure(_) => {
                failed_jobs += 1;
            }
        }
    }

    Ok(RunSummary {
        thread_count,
        target_format,
        compression_level,
        total_jobs: jobs.len(),
        successful_jobs,
        failed_jobs,
        total_chunks_written,
        total_discarded_chunks,
        total_warnings,
        job_reports,
    })
}

fn process_job(job: &Job, target_format: RegionFormat, compression_level: i32) -> JobReport {
    let source_file = job.source_file.display().to_string();
    let destination_file = job.destination_file.display().to_string();

    match try_process_job(job, target_format, compression_level) {
        Ok(success) => JobReport::Success(JobSuccess {
            source_file,
            destination_file,
            source_format: success.source_format,
            target_format,
            chunk_count: success.chunk_count,
            discarded_chunks: success.discarded_chunks,
            warnings: success.warnings,
        }),
        Err(error) => JobReport::Failure(JobFailure {
            source_file,
            destination_file,
            source_format: job.source_format,
            target_format,
            error: format!("{error:#}"),
        }),
    }
}

fn try_process_job(
    job: &Job,
    target_format: RegionFormat,
    compression_level: i32,
) -> Result<ProcessedJob> {
    let source_format = match job.source_format {
        RegionFormat::Blinear => crate::formats::detect_format(&job.source_file)?,
        format => format,
    };

    let mut read = read_region(&job.source_file, source_format)
        .with_context(|| format!("failed to decode {}", job.source_file.display()))?;
    let chunk_count = read.region.chunk_count();

    let mut encoded =
        encode_region(&read.region, target_format, compression_level).with_context(|| {
            format!(
                "failed to encode {} as {}",
                job.source_file.display(),
                target_format
            )
        })?;

    write_encoded_region(target_format, &job.destination_file, &encoded)
        .with_context(|| format!("failed to write {}", job.destination_file.display()))?;

    let mut warnings = Vec::new();
    warnings.append(&mut read.warnings);
    warnings.append(&mut encoded.warnings);

    Ok(ProcessedJob {
        source_format,
        chunk_count,
        discarded_chunks: read.discarded_chunks,
        warnings,
    })
}

fn write_encoded_region(
    target_format: RegionFormat,
    destination_file: &Path,
    encoded: &crate::formats::EncodedRegion,
) -> Result<()> {
    let destination_dir = destination_file
        .parent()
        .context("destination file does not have a parent directory")?;

    if target_format == RegionFormat::Mca
        && !encoded.sidecar_files.is_empty()
        && destination_file.exists()
        && region_uses_external_chunks(destination_file)?
    {
        bail!(
            "refusing to overwrite {} because it already uses external .mcc chunks and replacing the sidecars plus header atomically is not currently safe",
            destination_file.display()
        );
    }

    for sidecar in &encoded.sidecar_files {
        atomic_write(&destination_dir.join(&sidecar.file_name), &sidecar.bytes)?;
    }
    atomic_write(destination_file, &encoded.main_file_bytes)?;

    Ok(())
}

struct ProcessedJob {
    source_format: RegionFormat,
    chunk_count: usize,
    discarded_chunks: usize,
    warnings: Vec<String>,
}
