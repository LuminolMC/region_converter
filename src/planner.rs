use std::path::PathBuf;

use anyhow::Result;

use crate::cli::Cli;
use crate::discovery::{
    DiscoverySummary, InputDiscovery, Job, RegionSource, discover_jobs_with_summary,
    discover_sources_with_summary,
};
use crate::formats::{RegionFormat, SourceFormatHint};
use crate::pipeline::resolve_thread_count;

#[derive(Debug)]
pub struct RunPlan {
    pub input_paths: Vec<PathBuf>,
    pub output_root: PathBuf,
    pub source_format: Option<SourceFormatHint>,
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
    pub format: SourceFormatHint,
    pub job_count: usize,
}

pub struct ConversionPlan {
    pub run_plan: RunPlan,
    pub jobs: Vec<Job>,
    pub target_format: RegionFormat,
    pub compression_level: i32,
}

pub struct InspectionPlan {
    pub sources: Vec<RegionSource>,
    pub discovery_summary: DiscoverySummary,
    pub thread_count: usize,
}

pub fn plan_conversion(cli: &Cli) -> Result<ConversionPlan> {
    cli.validate()?;
    let requested_thread_count = cli.thread_count()?;
    let source_format = cli.forced_source_format();
    let target_format = cli.target_format()?;
    let compression_level = cli.resolved_compression_level()?;
    let discovery = discover_jobs_with_summary(
        &cli.inputs,
        cli.output_root()?,
        source_format,
        target_format,
    )?;
    let thread_count = resolve_thread_count(requested_thread_count, discovery.jobs.len());

    let run_plan = RunPlan {
        input_paths: cli.inputs.clone(),
        output_root: cli
            .output
            .clone()
            .expect("conversion mode should always have an output root"),
        source_format,
        requested_thread_count,
        target_format,
        thread_count,
        compression_level,
        total_jobs: discovery.jobs.len(),
        total_region_directories: discovery.summary.total_region_directories,
        source_breakdown: build_source_breakdown(&discovery.jobs),
        input_summaries: discovery.summary.inputs.clone(),
    };

    Ok(ConversionPlan {
        run_plan,
        jobs: discovery.jobs,
        target_format,
        compression_level,
    })
}

pub fn plan_inspection(cli: &Cli) -> Result<InspectionPlan> {
    cli.validate()?;
    let requested_thread_count = cli.thread_count()?;
    let discovery = discover_sources_with_summary(&cli.inputs, None, None)?;
    let thread_count = resolve_thread_count(requested_thread_count, discovery.sources.len());

    Ok(InspectionPlan {
        sources: discovery.sources,
        discovery_summary: discovery.summary,
        thread_count,
    })
}

fn build_source_breakdown(jobs: &[Job]) -> Vec<FormatBreakdown> {
    let formats = [
        SourceFormatHint::Mca,
        SourceFormatHint::Linear,
        SourceFormatHint::BlinearFamily,
        SourceFormatHint::BlinearV2,
        SourceFormatHint::BlinearV3,
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
