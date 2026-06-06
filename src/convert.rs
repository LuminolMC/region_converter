use std::cmp::Reverse;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow};

use crate::cli::Cli;
use crate::diagnostic::{Diagnostic, DiagnosticCode, warning_count};
use crate::discovery::{InputDiscovery, InputKind, Job, RegionFileGroup};
use crate::formats::{
    EncodeProfile, RegionFormat, SourceFormatHint, decode_region, encode_region_to_writer_profiled,
};
use crate::pipeline::stream_parallel;
use crate::planner::{ConversionPlan, plan_conversion};
use crate::runtime::RuntimeResources;
use crate::writer::{WriteTransactionProfile, write_region_with_transaction_profiled};

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
    pub profile: Option<ProfileSummary>,
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
    pub recent_chunk_rate_per_second: f64,
    pub elapsed: Duration,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct JobProfile {
    pub estimated_size_bytes: u64,
    pub wait_memory: Duration,
    pub wait_decode_io: Duration,
    pub wait_write_io: Duration,
    pub decode: Duration,
    pub encode_compress: Duration,
    pub encode_other_cpu: Duration,
    pub encode_file_write: Duration,
    pub output_commit: Duration,
    pub encode_write: Duration,
    pub total: Duration,
    pub encoded_units: usize,
    pub raw_payload_bytes: u64,
    pub compressed_payload_bytes: u64,
}

#[derive(Clone, Debug, Default)]
pub struct ProfileSummary {
    pub memory_budget_bytes: u64,
    pub estimated_input_bytes: u64,
    pub wait_memory: Duration,
    pub wait_decode_io: Duration,
    pub wait_write_io: Duration,
    pub decode: Duration,
    pub encode_compress: Duration,
    pub encode_other_cpu: Duration,
    pub encode_file_write: Duration,
    pub output_commit: Duration,
    pub encode_write: Duration,
    pub total_job_time: Duration,
    pub encoded_units: usize,
    pub raw_payload_bytes: u64,
    pub compressed_payload_bytes: u64,
    pub slowest_jobs: Vec<ProfiledJobSummary>,
}

#[derive(Clone, Debug)]
pub struct ProfiledJobSummary {
    pub source_file: PathBuf,
    pub estimated_size_bytes: u64,
    pub encoded_units: usize,
    pub raw_payload_bytes: u64,
    pub compressed_payload_bytes: u64,
    pub decode: Duration,
    pub encode_compress: Duration,
    pub encode_file_write: Duration,
    pub output_commit: Duration,
    pub encode_write: Duration,
    pub total: Duration,
}

#[derive(Clone, Debug)]
pub struct RunStage {
    pub input_index: usize,
    pub input_path: PathBuf,
    pub input_kind: InputKind,
    pub file_group: RegionFileGroup,
    pub total_jobs: usize,
}

#[derive(Clone, Debug)]
pub struct RunInputSummary {
    pub input_index: usize,
    pub input_path: PathBuf,
    pub input_kind: InputKind,
    pub total_jobs: usize,
    pub successful_jobs: usize,
    pub failed_jobs: usize,
    pub total_chunks_written: usize,
    pub total_discarded_chunks: usize,
    pub total_warnings: usize,
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
    pub profile: JobProfile,
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
    pub profile: JobProfile,
}

pub trait RunObserver {
    fn on_plan(&mut self, _plan: &RunPlan) -> Result<()> {
        Ok(())
    }

    fn on_stage_start(&mut self, _stage: &RunStage) -> Result<()> {
        Ok(())
    }

    fn on_job_report(&mut self, _report: &JobReport, _progress: &ProgressSnapshot) -> Result<()> {
        Ok(())
    }

    fn on_input_finish(&mut self, _summary: &RunInputSummary) -> Result<()> {
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

    let summary = execute_jobs(plan, observer)?;
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

fn execute_jobs(plan: ConversionPlan, observer: &mut dyn RunObserver) -> Result<RunSummary> {
    let jobs = plan.jobs;
    let input_summaries = plan.run_plan.input_summaries;
    let target_format = plan.target_format;
    let compression_level = plan.compression_level;
    let thread_count = plan.run_plan.thread_count;
    let total_region_directories = plan.run_plan.total_region_directories;
    let profile_enabled = plan.run_plan.profile;
    let total_jobs = jobs.len();
    let started_at = Instant::now();
    let resources = RuntimeResources::for_thread_count(thread_count);
    let mut accumulator = SummaryAccumulator::new(
        total_jobs,
        total_region_directories,
        thread_count,
        target_format,
        compression_level,
        resources.memory_budget_bytes(),
        profile_enabled,
    );

    let mut jobs_by_input = Vec::new();
    jobs_by_input.resize_with(input_summaries.len(), Vec::new);
    for job in jobs {
        let input_index = job.input_index;
        if let Some(input_jobs) = jobs_by_input.get_mut(input_index) {
            input_jobs.push(job);
        }
    }

    for (input_index, input) in input_summaries.iter().enumerate() {
        let input_started_at = Instant::now();
        let mut input_accumulator = InputRunAccumulator::new(input);
        let mut jobs_by_group = Vec::new();
        jobs_by_group.resize_with(RegionFileGroup::ORDERED.len(), Vec::new);
        for job in jobs_by_input
            .get_mut(input_index)
            .map(std::mem::take)
            .unwrap_or_default()
        {
            jobs_by_group[group_index(job.file_group)].push(job);
        }

        for file_group in RegionFileGroup::ORDERED {
            let stage_jobs = std::mem::take(&mut jobs_by_group[group_index(file_group)]);
            if stage_jobs.is_empty() {
                continue;
            }

            observer.on_stage_start(&RunStage {
                input_index,
                input_path: input.input_path.clone(),
                input_kind: input.input_kind,
                file_group,
                total_jobs: stage_jobs.len(),
            })?;

            let stage_started_at = Instant::now();
            let mut stage_accumulator = StageProgressAccumulator::new(stage_jobs.len());
            let resources_for_stage = resources.clone();

            stream_parallel(
                stage_jobs,
                thread_count,
                move |job| {
                    process_job(
                        job,
                        target_format,
                        compression_level,
                        resources_for_stage.clone(),
                        profile_enabled,
                    )
                },
                |report| {
                    let total_elapsed = started_at.elapsed();
                    accumulator.apply(&report, total_elapsed);
                    input_accumulator.apply(&report);
                    stage_accumulator.apply(&report, stage_started_at.elapsed());
                    observer.on_job_report(&report, &stage_accumulator.snapshot)?;
                    Ok(())
                },
            )?;
        }

        observer.on_input_finish(&input_accumulator.finish(
            input_index,
            input,
            input_started_at.elapsed(),
        ))?;
    }

    Ok(accumulator.finish(started_at.elapsed()))
}

fn group_index(group: RegionFileGroup) -> usize {
    match group {
        RegionFileGroup::Regions => 0,
        RegionFileGroup::Entities => 1,
        RegionFileGroup::Poi => 2,
    }
}

fn process_job(
    job: Job,
    target_format: RegionFormat,
    compression_level: i32,
    resources: RuntimeResources,
    profile_enabled: bool,
) -> JobReport {
    let mut resolved_source_format = hinted_region_format(job.source_format);
    let mut diagnostics = Vec::new();
    let mut discarded_chunks = 0;
    let mut stage = JobStage::Decode;
    let total_started_at = Instant::now();
    let mut profile = JobProfile {
        estimated_size_bytes: job.estimated_size_bytes,
        ..JobProfile::default()
    };

    let result = (|| -> Result<usize> {
        let wait_started_at = Instant::now();
        let _memory_guard = resources.acquire_memory_for_job(job.estimated_size_bytes);
        profile.wait_memory = wait_started_at.elapsed();

        let decode_started_at = Instant::now();
        let decoded = {
            let wait_started_at = Instant::now();
            let _io_guard = resources.acquire_decode_io();
            profile.wait_decode_io = wait_started_at.elapsed();
            decode_region(&job.source_file, job.source_format).map_err(|error| {
                anyhow!("failed to decode {}: {error:#}", job.source_file.display())
            })?
        };
        profile.decode = decode_started_at.elapsed();

        let mut decoded = decoded;
        resolved_source_format = Some(decoded.format);
        diagnostics.append(&mut decoded.outcome.diagnostics);
        discarded_chunks = decoded.outcome.discarded_chunks;
        let chunk_count = decoded.outcome.region.chunk_count();

        stage = JobStage::Encode;
        let encode_write_started_at = Instant::now();
        let mut encoded_diagnostics = Vec::new();
        let mut encode_profile = profile_enabled.then(EncodeProfile::default);
        let mut transaction_profile = WriteTransactionProfile::default();
        {
            let wait_started_at = Instant::now();
            let _io_guard = resources.acquire_write_io();
            profile.wait_write_io = wait_started_at.elapsed();
            stage = JobStage::Write;
            write_region_with_transaction_profiled(
                target_format,
                &job.destination_file,
                profile_enabled.then_some(&mut transaction_profile),
                |target| {
                    encoded_diagnostics = encode_region_to_writer_profiled(
                        &decoded.outcome.region,
                        target_format,
                        compression_level,
                        target,
                        encode_profile.as_mut(),
                    )?;
                    Ok(())
                },
            )
            .map_err(|error| {
                anyhow!(
                    "failed to encode and write {} as {}: {error:#}",
                    job.source_file.display(),
                    target_format
                )
            })?;
        }
        profile.encode_write = encode_write_started_at.elapsed();
        if let Some(encode_profile) = encode_profile {
            profile.encode_compress = encode_profile.compress;
            profile.encode_file_write = encode_profile.file_write;
            profile.encoded_units = encode_profile.encoded_units;
            profile.raw_payload_bytes = encode_profile.raw_payload_bytes;
            profile.compressed_payload_bytes = encode_profile.compressed_payload_bytes;
        }
        if profile_enabled {
            profile.output_commit = transaction_profile.commit;
            let accounted = profile
                .encode_compress
                .saturating_add(profile.encode_file_write)
                .saturating_add(profile.output_commit);
            profile.encode_other_cpu = profile.encode_write.saturating_sub(accounted);
        }
        diagnostics.append(&mut encoded_diagnostics);

        Ok(chunk_count)
    })();
    profile.total = total_started_at.elapsed();

    match result {
        Ok(chunk_count) => JobReport::Success(JobSuccess {
            source_file: job.source_file,
            destination_file: job.destination_file,
            source_format: resolved_source_format,
            target_format,
            chunk_count,
            discarded_chunks,
            diagnostics,
            profile,
        }),
        Err(error) => JobReport::Failure(JobFailure {
            source_file: job.source_file.clone(),
            destination_file: job.destination_file.clone(),
            source_format: resolved_source_format,
            target_format,
            discarded_chunks,
            diagnostics,
            error: build_failure_diagnostic(stage, &job.source_file, &job.destination_file, error),
            profile,
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
    profile: Option<ProfileSummary>,
    recent_samples: VecDeque<(Duration, usize)>,
    snapshot: ProgressSnapshot,
}

struct StageProgressAccumulator {
    total_jobs: usize,
    successful_jobs: usize,
    failed_jobs: usize,
    total_chunks_written: usize,
    total_discarded_chunks: usize,
    total_warnings: usize,
    recent_samples: VecDeque<(Duration, usize)>,
    snapshot: ProgressSnapshot,
}

struct InputRunAccumulator {
    total_jobs: usize,
    successful_jobs: usize,
    failed_jobs: usize,
    total_chunks_written: usize,
    total_discarded_chunks: usize,
    total_warnings: usize,
}

impl SummaryAccumulator {
    fn new(
        total_jobs: usize,
        total_region_directories: usize,
        thread_count: usize,
        target_format: RegionFormat,
        compression_level: i32,
        memory_budget_bytes: u64,
        profile_enabled: bool,
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
            profile: if profile_enabled {
                Some(ProfileSummary {
                    memory_budget_bytes,
                    ..ProfileSummary::default()
                })
            } else {
                None
            },
            recent_samples: VecDeque::new(),
            snapshot: ProgressSnapshot {
                total_jobs,
                completed_jobs: 0,
                successful_jobs: 0,
                failed_jobs: 0,
                successful_chunks: 0,
                discarded_chunks: 0,
                warnings: 0,
                recent_chunk_rate_per_second: 0.0,
                elapsed: Duration::ZERO,
            },
        }
    }

    fn apply(&mut self, report: &JobReport, elapsed: Duration) {
        let mut completed_chunks = 0_usize;
        match report {
            JobReport::Success(report) => {
                self.successful_jobs += 1;
                self.total_chunks_written += report.chunk_count;
                self.total_discarded_chunks += report.discarded_chunks;
                self.total_warnings += warning_count(&report.diagnostics);
                completed_chunks = report.chunk_count;
                self.apply_profile(&report.source_file, report.profile);
            }
            JobReport::Failure(report) => {
                self.failed_jobs += 1;
                self.total_discarded_chunks += report.discarded_chunks;
                self.total_warnings += warning_count(&report.diagnostics);
                self.apply_profile(&report.source_file, report.profile);
            }
        }

        self.recent_samples.push_back((elapsed, completed_chunks));
        let recent_chunk_rate_per_second = self.recent_chunk_rate(elapsed);

        self.snapshot = ProgressSnapshot {
            total_jobs: self.total_jobs,
            completed_jobs: self.successful_jobs + self.failed_jobs,
            successful_jobs: self.successful_jobs,
            failed_jobs: self.failed_jobs,
            successful_chunks: self.total_chunks_written,
            discarded_chunks: self.total_discarded_chunks,
            warnings: self.total_warnings,
            recent_chunk_rate_per_second,
            elapsed,
        };
    }

    fn apply_profile(&mut self, source_file: &Path, profile: JobProfile) {
        let Some(summary) = self.profile.as_mut() else {
            return;
        };
        summary.estimated_input_bytes += profile.estimated_size_bytes;
        summary.wait_memory += profile.wait_memory;
        summary.wait_decode_io += profile.wait_decode_io;
        summary.wait_write_io += profile.wait_write_io;
        summary.decode += profile.decode;
        summary.encode_compress += profile.encode_compress;
        summary.encode_other_cpu += profile.encode_other_cpu;
        summary.encode_file_write += profile.encode_file_write;
        summary.output_commit += profile.output_commit;
        summary.encode_write += profile.encode_write;
        summary.total_job_time += profile.total;
        summary.encoded_units += profile.encoded_units;
        summary.raw_payload_bytes += profile.raw_payload_bytes;
        summary.compressed_payload_bytes += profile.compressed_payload_bytes;
        summary.slowest_jobs.push(ProfiledJobSummary {
            source_file: source_file.to_path_buf(),
            estimated_size_bytes: profile.estimated_size_bytes,
            encoded_units: profile.encoded_units,
            raw_payload_bytes: profile.raw_payload_bytes,
            compressed_payload_bytes: profile.compressed_payload_bytes,
            decode: profile.decode,
            encode_compress: profile.encode_compress,
            encode_file_write: profile.encode_file_write,
            output_commit: profile.output_commit,
            encode_write: profile.encode_write,
            total: profile.total,
        });
        summary
            .slowest_jobs
            .sort_by_key(|job| Reverse(job.encode_write));
        summary.slowest_jobs.truncate(5);
    }

    fn recent_chunk_rate(&mut self, elapsed: Duration) -> f64 {
        recent_chunk_rate(&mut self.recent_samples, self.total_chunks_written, elapsed)
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
            profile: self.profile,
            elapsed,
        }
    }
}

fn recent_chunk_rate(
    recent_samples: &mut VecDeque<(Duration, usize)>,
    total_chunks_written: usize,
    elapsed: Duration,
) -> f64 {
    const WINDOW: Duration = Duration::from_secs(30);
    if recent_samples.len() < 2 {
        return ratio_per_second(total_chunks_written, elapsed);
    }

    while let Some((sample_time, _)) = recent_samples.front() {
        if elapsed.saturating_sub(*sample_time) <= WINDOW {
            break;
        }
        recent_samples.pop_front();
    }

    if recent_samples.len() < 2 {
        return ratio_per_second(total_chunks_written, elapsed);
    }

    let Some((oldest_time, _)) = recent_samples.front() else {
        return 0.0;
    };
    let window = elapsed.saturating_sub(*oldest_time);
    let chunks = recent_samples
        .iter()
        .map(|(_, chunks)| *chunks)
        .sum::<usize>();
    let seconds = window.as_secs_f64().max(0.001);
    chunks as f64 / seconds
}

impl StageProgressAccumulator {
    fn new(total_jobs: usize) -> Self {
        Self {
            total_jobs,
            successful_jobs: 0,
            failed_jobs: 0,
            total_chunks_written: 0,
            total_discarded_chunks: 0,
            total_warnings: 0,
            recent_samples: VecDeque::new(),
            snapshot: ProgressSnapshot {
                total_jobs,
                completed_jobs: 0,
                successful_jobs: 0,
                failed_jobs: 0,
                successful_chunks: 0,
                discarded_chunks: 0,
                warnings: 0,
                recent_chunk_rate_per_second: 0.0,
                elapsed: Duration::ZERO,
            },
        }
    }

    fn apply(&mut self, report: &JobReport, elapsed: Duration) {
        let stats = report_stats(report);
        self.successful_jobs += stats.successful_jobs;
        self.failed_jobs += stats.failed_jobs;
        self.total_chunks_written += stats.chunks_written;
        self.total_discarded_chunks += stats.discarded_chunks;
        self.total_warnings += stats.warnings;

        self.recent_samples
            .push_back((elapsed, stats.chunks_written));
        let recent_chunk_rate_per_second =
            recent_chunk_rate(&mut self.recent_samples, self.total_chunks_written, elapsed);

        self.snapshot = ProgressSnapshot {
            total_jobs: self.total_jobs,
            completed_jobs: self.successful_jobs + self.failed_jobs,
            successful_jobs: self.successful_jobs,
            failed_jobs: self.failed_jobs,
            successful_chunks: self.total_chunks_written,
            discarded_chunks: self.total_discarded_chunks,
            warnings: self.total_warnings,
            recent_chunk_rate_per_second,
            elapsed,
        };
    }
}

impl InputRunAccumulator {
    fn new(input: &InputDiscovery) -> Self {
        Self {
            total_jobs: input.discovered_jobs,
            successful_jobs: 0,
            failed_jobs: 0,
            total_chunks_written: 0,
            total_discarded_chunks: 0,
            total_warnings: 0,
        }
    }

    fn apply(&mut self, report: &JobReport) {
        let stats = report_stats(report);
        self.successful_jobs += stats.successful_jobs;
        self.failed_jobs += stats.failed_jobs;
        self.total_chunks_written += stats.chunks_written;
        self.total_discarded_chunks += stats.discarded_chunks;
        self.total_warnings += stats.warnings;
    }

    fn finish(
        self,
        input_index: usize,
        input: &InputDiscovery,
        elapsed: Duration,
    ) -> RunInputSummary {
        RunInputSummary {
            input_index,
            input_path: input.input_path.clone(),
            input_kind: input.input_kind,
            total_jobs: self.total_jobs,
            successful_jobs: self.successful_jobs,
            failed_jobs: self.failed_jobs,
            total_chunks_written: self.total_chunks_written,
            total_discarded_chunks: self.total_discarded_chunks,
            total_warnings: self.total_warnings,
            elapsed,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct ReportStats {
    successful_jobs: usize,
    failed_jobs: usize,
    chunks_written: usize,
    discarded_chunks: usize,
    warnings: usize,
}

fn report_stats(report: &JobReport) -> ReportStats {
    match report {
        JobReport::Success(report) => ReportStats {
            successful_jobs: 1,
            chunks_written: report.chunk_count,
            discarded_chunks: report.discarded_chunks,
            warnings: warning_count(&report.diagnostics),
            ..ReportStats::default()
        },
        JobReport::Failure(report) => ReportStats {
            failed_jobs: 1,
            discarded_chunks: report.discarded_chunks,
            warnings: warning_count(&report.diagnostics),
            ..ReportStats::default()
        },
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
