use std::fmt;
use std::io::{self, Write};
use std::time::Duration;

use anyhow::Result;
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressState, ProgressStyle};

use region_converter::convert::{
    JobFailure, JobReport, ProfileSummary, ProgressSnapshot, RunInputSummary, RunObserver, RunPlan,
    RunStage, RunSummary,
};
use region_converter::discovery::{InputKind, RegionFileGroup};
use region_converter::formats::SourceFormatHint;
use region_converter::info::{FormatCount, GroupInfoCount, InfoSummary, RegionInfoEntry};

const BAR_WIDTH: usize = 28;
const PROGRESS_REFRESH_HZ: u8 = 20;
const STEADY_TICK_INTERVAL: Duration = Duration::from_millis(80);

pub struct ConsoleReporter {
    progress: Option<IndicatifProgress>,
    total_inputs: usize,
}

impl ConsoleReporter {
    pub fn new() -> Self {
        Self {
            progress: None,
            total_inputs: 0,
        }
    }

    pub fn print_info_summary(&mut self, summary: &InfoSummary) -> Result<()> {
        println!("Save info:");
        println!("  Inputs ({}):", summary.inputs.len());
        for (input_index, input) in summary.inputs.iter().enumerate() {
            println!("  - {} [{}]", input.input_path.display(), input.input_kind);
            println!("    Region files: {}", input.region_files);
            if input.input_kind == InputKind::WorldDirectory {
                print_group_breakdown(&input.group_breakdown);
            }
            println!(
                "    Total size: {} ({} bytes)",
                format_bytes(input.total_size_bytes),
                input.total_size_bytes
            );
            println!(
                "    Chunks ok: {} | Discarded chunks: {} | Warnings: {}",
                input.chunk_count, input.discarded_chunks, input.warnings
            );
            println!(
                "    Formats: {}",
                format_format_breakdown(&input.format_breakdown)
            );

            if input.input_kind == InputKind::RegionFile
                && let Some(entry) = summary
                    .entries
                    .iter()
                    .find(|entry| entry.input_index == input_index)
            {
                print_single_region_details(entry);
            }
        }

        println!("Overall:");
        println!("  Threads used: {}", summary.thread_count);
        println!("  Region files: {}", summary.total_region_files);
        println!(
            "  Readable regions: {} | Failed regions: {}",
            summary.readable_regions, summary.failed_regions
        );
        println!(
            "  Total size: {} ({} bytes)",
            format_bytes(summary.total_size_bytes),
            summary.total_size_bytes
        );
        println!(
            "  Chunks ok: {} | Discarded chunks: {} | Warnings: {}",
            summary.chunk_count, summary.discarded_chunks, summary.warnings
        );
        println!("  Completed in {}", format_duration(summary.elapsed));

        print_info_issues(summary);
        io::stdout().flush()?;
        Ok(())
    }
}

impl RunObserver for ConsoleReporter {
    fn on_plan(&mut self, plan: &RunPlan) -> Result<()> {
        self.total_inputs = plan.input_summaries.len();
        println!("Conversion plan:");
        println!("  Inputs ({}):", plan.input_paths.len());
        for input in &plan.input_summaries {
            println!("  - {} [{}]", input.input_path.display(), input.input_kind);
        }
        println!("  Output root: {}", plan.output_root.display());
        println!(
            "  Format: {} -> {}",
            format_source_mode(plan.source_format),
            plan.target_format
        );
        if plan.thread_count == plan.requested_thread_count {
            println!("  Threads: {}", plan.thread_count);
        } else {
            println!(
                "  Threads: {} (capped from {} to match discovered work)",
                plan.thread_count, plan.requested_thread_count
            );
        }
        println!("  Compression level: {}", plan.compression_level);
        if plan.profile {
            println!("  Profiling: enabled");
        }
        println!("  Region files: {}", plan.total_jobs);
        io::stdout().flush()?;
        Ok(())
    }

    fn on_stage_start(&mut self, stage: &RunStage) -> Result<()> {
        if let Some(bar) = self.progress.take() {
            bar.finish();
        }

        self.progress = Some(IndicatifProgress::new(stage.total_jobs, stage.file_group));
        Ok(())
    }

    fn on_job_report(&mut self, report: &JobReport, progress: &ProgressSnapshot) -> Result<()> {
        if let Some(bar) = self.progress.as_mut() {
            match report {
                JobReport::Success(report) => {
                    for warning in &report.diagnostics {
                        bar.println(&format!(
                            "warning [{} -> {}]: {}",
                            report.source_file.display(),
                            report.destination_file.display(),
                            warning
                        ));
                    }
                }
                JobReport::Failure(report) => {
                    print_failure(bar, report);
                }
            }

            bar.update(progress);
        }

        Ok(())
    }

    fn on_input_finish(&mut self, summary: &RunInputSummary) -> Result<()> {
        if let Some(bar) = self.progress.take() {
            bar.finish();
        }

        if let Some(line) = format_input_completion_line(summary, self.total_inputs) {
            println!("{line}");
            io::stdout().flush()?;
        }
        Ok(())
    }

    fn on_finish(&mut self, summary: &RunSummary) -> Result<()> {
        if let Some(bar) = self.progress.take() {
            bar.finish();
        }

        println!(
            "Completed in {}. Average: {:.1} chunk/s. Success: {}. Failed: {}. Chunks written: {}. Discarded chunks: {}. Warnings: {}.",
            format_duration(summary.elapsed),
            average_chunk_rate(summary.total_chunks_written, summary.elapsed),
            summary.successful_jobs,
            summary.failed_jobs,
            summary.total_chunks_written,
            summary.total_discarded_chunks,
            summary.total_warnings
        );
        if let Some(profile) = &summary.profile {
            print_profile_summary(profile, summary.elapsed, summary.thread_count);
        }
        io::stdout().flush()?;
        Ok(())
    }
}

fn print_failure(bar: &IndicatifProgress, report: &JobFailure) {
    for warning in &report.diagnostics {
        bar.println(&format!(
            "warning [{} -> {}]: {}",
            report.source_file.display(),
            report.destination_file.display(),
            warning
        ));
    }

    bar.println(&format!(
        "error [{} -> {}]: {}",
        report.source_file.display(),
        report.destination_file.display(),
        report.error
    ));
}

fn format_source_mode(source_format: Option<SourceFormatHint>) -> String {
    match source_format {
        Some(format) => format.to_string(),
        None => "auto".to_string(),
    }
}

fn format_duration(duration: Duration) -> String {
    let total_seconds = duration.as_secs();
    let hours = total_seconds / 3_600;
    let minutes = (total_seconds % 3_600) / 60;
    let seconds = total_seconds % 60;

    if hours > 0 {
        format!("{hours:02}:{minutes:02}:{seconds:02}")
    } else {
        format!("{minutes:02}:{seconds:02}")
    }
}

fn format_duration_seconds(duration: Duration) -> String {
    format!("{:.1}s", duration.as_secs_f64())
}

fn format_input_completion_line(summary: &RunInputSummary, total_inputs: usize) -> Option<String> {
    if total_inputs <= 1 {
        return None;
    }

    Some(format!(
        "Completed [{}] in {}. Success: {}. Failed: {}. Chunks written: {}. Discarded chunks: {}. Warnings: {}.",
        summary.input_index + 1,
        format_duration_seconds(summary.elapsed),
        summary.successful_jobs,
        summary.failed_jobs,
        summary.total_chunks_written,
        summary.total_discarded_chunks,
        summary.total_warnings,
    ))
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];

    let mut value = bytes as f64;
    let mut unit_index = 0;
    while value >= 1024.0 && unit_index + 1 < UNITS.len() {
        value /= 1024.0;
        unit_index += 1;
    }

    if unit_index == 0 {
        format!("{bytes} {}", UNITS[unit_index])
    } else {
        format!("{value:.2} {}", UNITS[unit_index])
    }
}

fn print_profile_summary(profile: &ProfileSummary, elapsed: Duration, thread_count: usize) {
    let effective_workers = if elapsed.as_secs_f64() > 0.0 {
        profile.total_job_time.as_secs_f64() / elapsed.as_secs_f64()
    } else {
        0.0
    };

    println!("Profile:");
    println!(
        "  Estimated input: {} | Memory budget: {}",
        format_bytes(profile.estimated_input_bytes),
        format_bytes(profile.memory_budget_bytes)
    );
    println!(
        "  Wait: memory {} | read IO {} | write IO {}",
        format_duration_precise(profile.wait_memory),
        format_duration_precise(profile.wait_decode_io),
        format_duration_precise(profile.wait_write_io)
    );
    println!(
        "  Work: decode {} | encode+write {} | total worker time {}",
        format_duration_precise(profile.decode),
        format_duration_precise(profile.encode_write),
        format_duration_precise(profile.total_job_time)
    );
    println!(
        "  Parallelism: wall {} | effective {:.2}/{} workers",
        format_duration_precise(elapsed),
        effective_workers,
        thread_count
    );
    println!(
        "  Encode detail: compress {} | format cpu {} | file write {} | commit {}",
        format_duration_precise(profile.encode_compress),
        format_duration_precise(profile.encode_other_cpu),
        format_duration_precise(profile.encode_file_write),
        format_duration_precise(profile.output_commit)
    );
    println!(
        "  Encoded payload: units {} | raw {} | compressed {}",
        profile.encoded_units,
        format_bytes(profile.raw_payload_bytes),
        format_bytes(profile.compressed_payload_bytes)
    );
    if !profile.slowest_jobs.is_empty() {
        println!("  Slowest regions:");
        for job in &profile.slowest_jobs {
            println!(
                "    {} | input {} | units {} | decode {} | compress {} | write {} | total {}",
                job.source_file.display(),
                format_bytes(job.estimated_size_bytes),
                job.encoded_units,
                format_duration_precise(job.decode),
                format_duration_precise(job.encode_compress),
                format_duration_precise(job.encode_file_write + job.output_commit),
                format_duration_precise(job.total)
            );
        }
    }
}

fn format_duration_precise(duration: Duration) -> String {
    let seconds = duration.as_secs_f64();
    if seconds >= 60.0 {
        format!("{} ({seconds:.2}s)", format_duration(duration))
    } else {
        format!("{seconds:.3}s")
    }
}

fn render_progress_stats(snapshot: &ProgressSnapshot) -> String {
    format!(
        "chunks ok {} discarded {} warn {} | avg {:.1}/s recent {:.1}/s",
        snapshot.successful_chunks,
        snapshot.discarded_chunks,
        snapshot.warnings,
        snapshot.chunk_rate_per_second(),
        snapshot.recent_chunk_rate_per_second,
    )
}

fn average_chunk_rate(chunks: usize, elapsed: Duration) -> f64 {
    let elapsed_secs = elapsed.as_secs_f64();
    if elapsed_secs <= 0.0 {
        0.0
    } else {
        chunks as f64 / elapsed_secs
    }
}

fn format_format_breakdown(formats: &[FormatCount]) -> String {
    if formats.is_empty() {
        return "unknown".to_string();
    }

    formats
        .iter()
        .map(|entry| format!("{} x{}", entry.format, entry.count))
        .collect::<Vec<_>>()
        .join(", ")
}

fn print_group_breakdown(groups: &[GroupInfoCount]) {
    for group in groups {
        println!(
            "    {}: {} files | readable {} | failed {}",
            group.file_group, group.region_files, group.readable_regions, group.failed_regions
        );
    }
}

fn print_single_region_details(entry: &RegionInfoEntry) {
    if let Some(format) = entry.storage_format {
        println!("    Format: {}", format);
    }

    if let (Some(region_x), Some(region_z)) = (entry.region_x, entry.region_z) {
        println!("    Region coords: ({region_x}, {region_z})");
    }

    if let Some(size_bytes) = entry.size_bytes {
        println!(
            "    File size: {} ({} bytes)",
            format_bytes(size_bytes),
            size_bytes
        );
    }

    if let Some(error) = &entry.error {
        println!("    Status: failed");
        println!("    Error: {error}");
    }
}

fn print_info_issues(summary: &InfoSummary) {
    let warning_entries = summary
        .entries
        .iter()
        .filter(|entry| !entry.diagnostics.is_empty())
        .collect::<Vec<_>>();
    if !warning_entries.is_empty() {
        println!("Warnings:");
        for entry in warning_entries {
            for warning in &entry.diagnostics {
                println!("  - [{}] {}", entry.source_file.display(), warning);
            }
        }
    }

    let error_entries = summary
        .entries
        .iter()
        .filter(|entry| entry.error.is_some())
        .collect::<Vec<_>>();
    if !error_entries.is_empty() {
        println!("Errors:");
        for entry in error_entries {
            if let Some(error) = &entry.error {
                println!("  - [{}] {}", entry.source_file.display(), error);
            }
        }
    }
}

fn draw_target() -> ProgressDrawTarget {
    ProgressDrawTarget::stderr_with_hz(PROGRESS_REFRESH_HZ)
}

fn progress_style(file_group: RegionFileGroup) -> ProgressStyle {
    let template = format!(
        "[{{bar:{BAR_WIDTH}}}] {{percent_1dp}}% {{pos}}/{{len}} {} | {{msg}}",
        file_group.label()
    );

    ProgressStyle::with_template(&template)
        .expect("progress template should be valid")
        .with_key(
            "percent_1dp",
            |state: &ProgressState, writer: &mut dyn fmt::Write| {
                let percent = state.fraction() as f64 * 100.0;
                let _ = write!(writer, "{percent:>5.1}");
            },
        )
        .progress_chars("##-")
}

struct IndicatifProgress {
    bar: ProgressBar,
}

impl IndicatifProgress {
    fn new(total_jobs: usize, file_group: RegionFileGroup) -> Self {
        let bar = ProgressBar::with_draw_target(Some(total_jobs as u64), draw_target());
        bar.set_style(progress_style(file_group));
        bar.set_message("chunks ok 0 discarded 0 warn 0 | avg 0.0/s recent 0.0/s".to_string());
        bar.enable_steady_tick(STEADY_TICK_INTERVAL);
        Self { bar }
    }

    fn update(&self, snapshot: &ProgressSnapshot) {
        self.bar.set_position(snapshot.completed_jobs as u64);
        self.bar.set_message(render_progress_stats(snapshot));
    }

    fn println(&self, line: &str) {
        self.bar.println(line);
    }

    fn finish(&self) {
        self.bar.finish_and_clear();
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn progress_stats_use_the_simplified_chunk_focused_format() {
        let snapshot = ProgressSnapshot {
            total_jobs: 5,
            completed_jobs: 1,
            successful_jobs: 1,
            failed_jobs: 0,
            successful_chunks: 485,
            discarded_chunks: 0,
            warnings: 0,
            recent_chunk_rate_per_second: 1107.3,
            elapsed: Duration::from_millis(438),
        };

        assert_eq!(
            render_progress_stats(&snapshot),
            "chunks ok 485 discarded 0 warn 0 | avg 1107.3/s recent 1107.3/s"
        );
    }

    #[test]
    fn average_chunk_rate_is_derived_from_elapsed_time() {
        assert_eq!(average_chunk_rate(2004, Duration::from_secs(10)), 200.4);
    }

    #[test]
    fn multi_input_completion_line_uses_compact_numbered_format() {
        let summary = RunInputSummary {
            input_index: 0,
            input_path: PathBuf::from("/tmp/world"),
            input_kind: InputKind::WorldDirectory,
            total_jobs: 3,
            successful_jobs: 2,
            failed_jobs: 1,
            total_chunks_written: 4096,
            total_discarded_chunks: 7,
            total_warnings: 5,
            elapsed: Duration::from_millis(12_340),
        };

        assert_eq!(
            format_input_completion_line(&summary, 2),
            Some(
                "Completed [1] in 12.3s. Success: 2. Failed: 1. Chunks written: 4096. Discarded chunks: 7. Warnings: 5."
                    .to_string()
            )
        );
    }

    #[test]
    fn single_input_completion_line_is_suppressed() {
        let summary = RunInputSummary {
            input_index: 0,
            input_path: PathBuf::from("/tmp/world"),
            input_kind: InputKind::WorldDirectory,
            total_jobs: 1,
            successful_jobs: 1,
            failed_jobs: 0,
            total_chunks_written: 1024,
            total_discarded_chunks: 0,
            total_warnings: 0,
            elapsed: Duration::from_millis(250),
        };

        assert_eq!(format_input_completion_line(&summary, 1), None);
    }
}
