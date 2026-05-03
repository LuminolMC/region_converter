use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow};

use crate::cli::Cli;
use crate::diagnostic::{Diagnostic, DiagnosticCode, warning_count};
use crate::discovery::Job;
use crate::formats::{RegionFormat, SourceFormatHint, decode_region, encode_region};
use crate::pipeline::stream_parallel;
use crate::planner::plan_conversion;
use crate::writer::write_encoded_region;

pub use crate::planner::{FormatBreakdown, RunPlan};

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
    pub source_file: PathBuf,
    pub destination_file: PathBuf,
    pub source_format: Option<RegionFormat>,
    pub target_format: RegionFormat,
    pub chunk_count: usize,
    pub discarded_chunks: usize,
    pub diagnostics: Vec<Diagnostic>,
}

#[derive(Debug)]
pub struct JobFailure {
    pub source_file: PathBuf,
    pub destination_file: PathBuf,
    pub source_format: Option<RegionFormat>,
    pub target_format: RegionFormat,
    pub discarded_chunks: usize,
    pub diagnostics: Vec<Diagnostic>,
    pub error: Diagnostic,
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
    let plan = plan_conversion(&cli)?;
    observer.on_plan(&plan.run_plan)?;

    let summary = execute_jobs(
        plan.jobs,
        plan.target_format,
        plan.compression_level,
        plan.run_plan.thread_count,
        plan.run_plan.total_region_directories,
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
    let started_at = Instant::now();
    let mut accumulator = SummaryAccumulator::new(
        total_jobs,
        total_region_directories,
        thread_count,
        target_format,
        compression_level,
    );

    stream_parallel(
        jobs,
        thread_count,
        move |job| process_job(job, target_format, compression_level),
        |report| {
            accumulator.apply(&report, started_at.elapsed());
            observer.on_job_report(&report, &accumulator.snapshot)?;
            Ok(())
        },
    )?;

    Ok(accumulator.finish(started_at.elapsed()))
}

fn process_job(job: Job, target_format: RegionFormat, compression_level: i32) -> JobReport {
    let mut resolved_source_format = hinted_region_format(job.source_format);
    let mut diagnostics = Vec::new();
    let mut discarded_chunks = 0;
    let mut stage = JobStage::Decode;

    let result = (|| -> Result<usize> {
        let mut decoded = decode_region(&job.source_file, job.source_format).map_err(|error| {
            anyhow!("failed to decode {}: {error:#}", job.source_file.display())
        })?;
        resolved_source_format = Some(decoded.format);
        diagnostics.append(&mut decoded.outcome.diagnostics);
        discarded_chunks = decoded.outcome.discarded_chunks;
        let chunk_count = decoded.outcome.region.chunk_count();

        stage = JobStage::Encode;
        let mut encoded = encode_region(&decoded.outcome.region, target_format, compression_level)
            .map_err(|error| {
                anyhow!(
                    "failed to encode {} as {}: {error:#}",
                    job.source_file.display(),
                    target_format
                )
            })?;
        diagnostics.append(&mut encoded.diagnostics);

        stage = JobStage::Write;
        write_encoded_region(target_format, &job.destination_file, &encoded).map_err(|error| {
            anyhow!(
                "failed to write {}: {error:#}",
                job.destination_file.display()
            )
        })?;

        Ok(chunk_count)
    })();

    match result {
        Ok(chunk_count) => JobReport::Success(JobSuccess {
            source_file: job.source_file,
            destination_file: job.destination_file,
            source_format: resolved_source_format,
            target_format,
            chunk_count,
            discarded_chunks,
            diagnostics,
        }),
        Err(error) => JobReport::Failure(JobFailure {
            source_file: job.source_file.clone(),
            destination_file: job.destination_file.clone(),
            source_format: resolved_source_format,
            target_format,
            discarded_chunks,
            diagnostics,
            error: build_failure_diagnostic(stage, &job.source_file, &job.destination_file, error),
        }),
    }
}

fn hinted_region_format(hint: SourceFormatHint) -> Option<RegionFormat> {
    match hint {
        SourceFormatHint::Mca => Some(RegionFormat::Mca),
        SourceFormatHint::Linear => Some(RegionFormat::Linear),
        SourceFormatHint::BlinearFamily => None,
        SourceFormatHint::BlinearV2 => Some(RegionFormat::BlinearV2),
        SourceFormatHint::BlinearV3 => Some(RegionFormat::BlinearV3),
    }
}

fn build_failure_diagnostic(
    stage: JobStage,
    source_file: &Path,
    destination_file: &Path,
    error: anyhow::Error,
) -> Diagnostic {
    match stage {
        JobStage::Decode => Diagnostic::error(DiagnosticCode::CorruptRegion, format!("{error:#}"))
            .with_path(source_file),
        JobStage::Encode => {
            Diagnostic::error(DiagnosticCode::InvalidMetadata, format!("{error:#}"))
                .with_path(source_file)
        }
        JobStage::Write => {
            let code = if error
                .to_string()
                .contains("replacing the sidecars plus header atomically is not currently safe")
            {
                DiagnosticCode::OutputSafety
            } else {
                DiagnosticCode::Io
            };
            Diagnostic::error(code, format!("{error:#}")).with_path(destination_file)
        }
    }
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
                self.total_warnings += warning_count(&report.diagnostics);
            }
            JobReport::Failure(report) => {
                self.failed_jobs += 1;
                self.total_discarded_chunks += report.discarded_chunks;
                self.total_warnings += warning_count(&report.diagnostics);
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

#[derive(Clone, Copy)]
enum JobStage {
    Decode,
    Encode,
    Write,
}

struct NoopRunObserver;

impl RunObserver for NoopRunObserver {}
