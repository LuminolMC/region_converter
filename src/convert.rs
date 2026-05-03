use std::path::{Path, PathBuf};
use std::sync::mpsc::sync_channel;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use rayon::ThreadPoolBuilder;
use rayon::prelude::*;

use crate::cli::Cli;
use crate::discovery::{InputDiscovery, Job, discover_jobs_with_summary};
use crate::formats::{RegionFormat, encode_region, read_region, region_uses_external_chunks};
use crate::io_util::atomic_write;

#[derive(Debug)]
pub struct RunPlan {
    pub input_paths: Vec<PathBuf>,
    pub output_root: PathBuf,
    pub source_format: Option<RegionFormat>,
    pub requested_thread_count: usize,
    pub target_format: RegionFormat,
    pub thread_count: usize,
    pub compression_level: i32,
    pub total_jobs: usize,
    pub total_region_directories: usize,
    pub source_breakdown: Vec<FormatBreakdown>,
    pub input_summaries: Vec<InputDiscovery>,
}

#[derive(Debug)]
pub struct FormatBreakdown {
    pub format: RegionFormat,
    pub job_count: usize,
}

#[derive(Debug)]
pub struct RunSummary {
    pub thread_count: usize,
    pub target_format: RegionFormat,
    pub compression_level: i32,
    pub total_jobs: usize,
    pub total_region_directories: usize,
    pub successful_jobs: usize,
    pub failed_jobs: usize,
    pub total_chunks_written: usize,
    pub total_discarded_chunks: usize,
    pub total_warnings: usize,
    pub elapsed: Duration,
}

#[derive(Clone, Debug)]
pub struct ProgressSnapshot {
    pub total_jobs: usize,
    pub completed_jobs: usize,
    pub successful_jobs: usize,
    pub failed_jobs: usize,
    pub successful_chunks: usize,
    pub discarded_chunks: usize,
    pub warnings: usize,
    pub elapsed: Duration,
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
    pub discarded_chunks: usize,
    pub warnings: Vec<String>,
    pub error: String,
}

pub trait RunObserver {
    fn on_plan(&mut self, _plan: &RunPlan) -> Result<()> {
        Ok(())
    }

    fn on_job_report(&mut self, _report: &JobReport, _progress: &ProgressSnapshot) -> Result<()> {
        Ok(())
    }

    fn on_finish(&mut self, _summary: &RunSummary) -> Result<()> {
        Ok(())
    }
}

pub fn run(cli: Cli) -> Result<RunSummary> {
    let mut observer = NoopRunObserver;
    run_with_observer(cli, &mut observer)
}

pub fn run_with_observer(cli: Cli, observer: &mut dyn RunObserver) -> Result<RunSummary> {
    let requested_thread_count = cli.thread_count()?;
    let source_format = cli.forced_source_format();
    let target_format = cli.target_format();
    let compression_level = cli.resolved_compression_level()?;
    let discovery =
        discover_jobs_with_summary(&cli.inputs, &cli.output, source_format, target_format)?;
    let thread_count = requested_thread_count.min(discovery.jobs.len().max(1));

    let plan = build_run_plan(
        &cli,
        source_format,
        target_format,
        requested_thread_count,
        thread_count,
        compression_level,
        &discovery.jobs,
        discovery.summary.inputs,
        discovery.summary.total_region_directories,
    );
    observer.on_plan(&plan)?;

    let summary = execute_jobs(
        discovery.jobs,
        target_format,
        compression_level,
        thread_count,
        plan.total_region_directories,
        observer,
    )?;
    observer.on_finish(&summary)?;
    Ok(summary)
}

impl ProgressSnapshot {
    pub fn region_rate_per_second(&self) -> f64 {
        ratio_per_second(self.completed_jobs, self.elapsed)
    }

    pub fn chunk_rate_per_second(&self) -> f64 {
        ratio_per_second(self.successful_chunks, self.elapsed)
    }

    pub fn estimated_remaining(&self) -> Option<Duration> {
        if self.completed_jobs == 0 || self.completed_jobs >= self.total_jobs {
            return None;
        }

        let elapsed_secs = self.elapsed.as_secs_f64();
        if elapsed_secs <= 0.0 {
            return None;
        }

        let remaining_jobs = (self.total_jobs - self.completed_jobs) as f64;
        let completed_jobs = self.completed_jobs as f64;
        let remaining_secs = elapsed_secs * (remaining_jobs / completed_jobs);
        Some(Duration::from_secs_f64(remaining_secs))
    }
}

fn execute_jobs(
    jobs: Vec<Job>,
    target_format: RegionFormat,
    compression_level: i32,
    thread_count: usize,
    total_region_directories: usize,
    observer: &mut dyn RunObserver,
) -> Result<RunSummary> {
    let total_jobs = jobs.len();
    let pool = ThreadPoolBuilder::new()
        .num_threads(thread_count)
        .build()
        .context("failed to build the Rayon thread pool")?;
    let queue_depth = thread_count.saturating_mul(2).max(1);
    let (sender, receiver) = sync_channel::<JobReport>(queue_depth);

    let worker = thread::spawn(move || {
        pool.install(|| {
            jobs.into_par_iter()
                .for_each_with(sender, |job_sender, job| {
                    let report = process_job(job, target_format, compression_level);
                    let _ = job_sender.send(report);
                });
        });
    });

    let started_at = Instant::now();
    let mut accumulator = SummaryAccumulator::new(
        total_jobs,
        total_region_directories,
        thread_count,
        target_format,
        compression_level,
    );
    let mut observer_error = None;

    for report in receiver {
        accumulator.apply(&report, started_at.elapsed());
        if observer_error.is_none() {
            if let Err(error) = observer.on_job_report(&report, &accumulator.snapshot) {
                observer_error = Some(error);
            }
        }
    }

    worker
        .join()
        .map_err(|_| anyhow::anyhow!("conversion worker thread panicked"))?;

    if let Some(error) = observer_error {
        return Err(error);
    }

    Ok(accumulator.finish(started_at.elapsed()))
}

fn build_run_plan(
    cli: &Cli,
    source_format: Option<RegionFormat>,
    target_format: RegionFormat,
    requested_thread_count: usize,
    thread_count: usize,
    compression_level: i32,
    jobs: &[Job],
    input_summaries: Vec<InputDiscovery>,
    total_region_directories: usize,
) -> RunPlan {
    RunPlan {
        input_paths: cli.inputs.clone(),
        output_root: cli.output.clone(),
        source_format,
        requested_thread_count,
        target_format,
        thread_count,
        compression_level,
        total_jobs: jobs.len(),
        total_region_directories,
        source_breakdown: build_source_breakdown(jobs),
        input_summaries,
    }
}

fn build_source_breakdown(jobs: &[Job]) -> Vec<FormatBreakdown> {
    let formats = [
        RegionFormat::Mca,
        RegionFormat::Linear,
        RegionFormat::Blinear,
        RegionFormat::BlinearV2,
        RegionFormat::BlinearV3,
    ];
    let mut breakdown = Vec::new();

    for format in formats {
        let job_count = jobs
            .iter()
            .filter(|job| job.source_format == format)
            .count();
        if job_count > 0 {
            breakdown.push(FormatBreakdown { format, job_count });
        }
    }

    breakdown
}

fn process_job(job: Job, target_format: RegionFormat, compression_level: i32) -> JobReport {
    let source_file = job.source_file.display().to_string();
    let destination_file = job.destination_file.display().to_string();
    let mut resolved_source_format = job.source_format;
    let mut warnings = Vec::new();
    let mut discarded_chunks = 0;

    let result = (|| -> Result<ProcessedJob> {
        resolved_source_format = match job.source_format {
            RegionFormat::Blinear => crate::formats::detect_format(&job.source_file)?,
            format => format,
        };

        let mut read = read_region(&job.source_file, resolved_source_format)
            .with_context(|| format!("failed to decode {}", job.source_file.display()))?;
        let chunk_count = read.region.chunk_count();
        discarded_chunks = read.discarded_chunks;
        warnings.append(&mut read.warnings);

        let mut encoded = encode_region(&read.region, target_format, compression_level)
            .with_context(|| {
                format!(
                    "failed to encode {} as {}",
                    job.source_file.display(),
                    target_format
                )
            })?;
        warnings.append(&mut encoded.warnings);

        write_encoded_region(target_format, &job.destination_file, &encoded)
            .with_context(|| format!("failed to write {}", job.destination_file.display()))?;

        Ok(ProcessedJob { chunk_count })
    })();

    match result {
        Ok(success) => JobReport::Success(JobSuccess {
            source_file,
            destination_file,
            source_format: resolved_source_format,
            target_format,
            chunk_count: success.chunk_count,
            discarded_chunks,
            warnings,
        }),
        Err(error) => JobReport::Failure(JobFailure {
            source_file,
            destination_file,
            source_format: resolved_source_format,
            target_format,
            discarded_chunks,
            warnings,
            error: format!("{error:#}"),
        }),
    }
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

fn ratio_per_second(value: usize, elapsed: Duration) -> f64 {
    let elapsed_secs = elapsed.as_secs_f64();
    if elapsed_secs <= 0.0 {
        0.0
    } else {
        value as f64 / elapsed_secs
    }
}

struct SummaryAccumulator {
    total_jobs: usize,
    total_region_directories: usize,
    thread_count: usize,
    target_format: RegionFormat,
    compression_level: i32,
    successful_jobs: usize,
    failed_jobs: usize,
    total_chunks_written: usize,
    total_discarded_chunks: usize,
    total_warnings: usize,
    snapshot: ProgressSnapshot,
}

impl SummaryAccumulator {
    fn new(
        total_jobs: usize,
        total_region_directories: usize,
        thread_count: usize,
        target_format: RegionFormat,
        compression_level: i32,
    ) -> Self {
        Self {
            total_jobs,
            total_region_directories,
            thread_count,
            target_format,
            compression_level,
            successful_jobs: 0,
            failed_jobs: 0,
            total_chunks_written: 0,
            total_discarded_chunks: 0,
            total_warnings: 0,
            snapshot: ProgressSnapshot {
                total_jobs,
                completed_jobs: 0,
                successful_jobs: 0,
                failed_jobs: 0,
                successful_chunks: 0,
                discarded_chunks: 0,
                warnings: 0,
                elapsed: Duration::ZERO,
            },
        }
    }

    fn apply(&mut self, report: &JobReport, elapsed: Duration) {
        match report {
            JobReport::Success(report) => {
                self.successful_jobs += 1;
                self.total_chunks_written += report.chunk_count;
                self.total_discarded_chunks += report.discarded_chunks;
                self.total_warnings += report.warnings.len();
            }
            JobReport::Failure(report) => {
                self.failed_jobs += 1;
                self.total_discarded_chunks += report.discarded_chunks;
                self.total_warnings += report.warnings.len();
            }
        }

        self.snapshot = ProgressSnapshot {
            total_jobs: self.total_jobs,
            completed_jobs: self.successful_jobs + self.failed_jobs,
            successful_jobs: self.successful_jobs,
            failed_jobs: self.failed_jobs,
            successful_chunks: self.total_chunks_written,
            discarded_chunks: self.total_discarded_chunks,
            warnings: self.total_warnings,
            elapsed,
        };
    }

    fn finish(self, elapsed: Duration) -> RunSummary {
        RunSummary {
            thread_count: self.thread_count,
            target_format: self.target_format,
            compression_level: self.compression_level,
            total_jobs: self.total_jobs,
            total_region_directories: self.total_region_directories,
            successful_jobs: self.successful_jobs,
            failed_jobs: self.failed_jobs,
            total_chunks_written: self.total_chunks_written,
            total_discarded_chunks: self.total_discarded_chunks,
            total_warnings: self.total_warnings,
            elapsed,
        }
    }
}

struct ProcessedJob {
    chunk_count: usize,
}

struct NoopRunObserver;

impl RunObserver for NoopRunObserver {}
