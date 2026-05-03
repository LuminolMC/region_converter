use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use rayon::ThreadPoolBuilder;
use rayon::prelude::*;

use crate::cli::Cli;
use crate::discovery::{InputKind, discover_sources_with_summary};
use crate::formats::{
    RegionStorageFormat, detect_storage_format, read_region, region_storage_size,
};

#[derive(Clone, Debug)]
pub struct FormatCount {
    pub format: RegionStorageFormat,
    pub count: usize,
}

#[derive(Clone, Debug)]
pub struct RegionInfoEntry {
    pub input_index: usize,
    pub source_file: PathBuf,
    pub storage_format: Option<RegionStorageFormat>,
    pub size_bytes: Option<u64>,
    pub region_x: Option<i32>,
    pub region_z: Option<i32>,
    pub chunk_count: usize,
    pub discarded_chunks: usize,
    pub warnings: Vec<String>,
    pub error: Option<String>,
}

#[derive(Clone, Debug)]
pub struct InputInfo {
    pub input_path: PathBuf,
    pub input_kind: InputKind,
    pub region_files: usize,
    pub readable_regions: usize,
    pub failed_regions: usize,
    pub total_size_bytes: u64,
    pub chunk_count: usize,
    pub discarded_chunks: usize,
    pub warnings: usize,
    pub format_breakdown: Vec<FormatCount>,
}

#[derive(Clone, Debug)]
pub struct InfoSummary {
    pub thread_count: usize,
    pub inputs: Vec<InputInfo>,
    pub entries: Vec<RegionInfoEntry>,
    pub total_region_files: usize,
    pub readable_regions: usize,
    pub failed_regions: usize,
    pub total_size_bytes: u64,
    pub chunk_count: usize,
    pub discarded_chunks: usize,
    pub warnings: usize,
    pub elapsed: Duration,
}

pub fn inspect(cli: Cli) -> Result<InfoSummary> {
    cli.validate()?;
    let requested_thread_count = cli.thread_count()?;
    let discovery = discover_sources_with_summary(&cli.inputs, None, None)?;
    let thread_count = requested_thread_count.min(discovery.sources.len().max(1));

    let started_at = Instant::now();
    let pool = ThreadPoolBuilder::new()
        .num_threads(thread_count)
        .build()
        .context("failed to build the Rayon thread pool")?;
    let entries = pool.install(|| {
        discovery
            .sources
            .par_iter()
            .map(inspect_source)
            .collect::<Vec<_>>()
    });
    let elapsed = started_at.elapsed();

    let mut builders = discovery
        .summary
        .inputs
        .iter()
        .map(|input| InputInfoBuilder::new(input.input_path.clone(), input.input_kind))
        .collect::<Vec<_>>();

    let mut total_region_files = 0_usize;
    let mut readable_regions = 0_usize;
    let mut failed_regions = 0_usize;
    let mut total_size_bytes = 0_u64;
    let mut chunk_count = 0_usize;
    let mut discarded_chunks = 0_usize;
    let mut warnings = 0_usize;

    for entry in &entries {
        total_region_files += 1;
        total_size_bytes += entry.size_bytes.unwrap_or(0);
        chunk_count += entry.chunk_count;
        discarded_chunks += entry.discarded_chunks;
        warnings += entry.warnings.len();

        let builder = builders
            .get_mut(entry.input_index)
            .context("discovered info entry points at an unknown input")?;
        builder.apply(entry);

        if entry.error.is_some() {
            failed_regions += 1;
        } else {
            readable_regions += 1;
        }
    }

    Ok(InfoSummary {
        thread_count,
        inputs: builders.into_iter().map(InputInfoBuilder::finish).collect(),
        entries,
        total_region_files,
        readable_regions,
        failed_regions,
        total_size_bytes,
        chunk_count,
        discarded_chunks,
        warnings,
        elapsed,
    })
}

fn inspect_source(source: &crate::discovery::RegionSource) -> RegionInfoEntry {
    let file_size_bytes = source
        .source_file
        .metadata()
        .ok()
        .map(|metadata| metadata.len());

    let storage_format = match detect_storage_format(&source.source_file) {
        Ok(format) => format,
        Err(error) => {
            return RegionInfoEntry {
                input_index: source.input_index,
                source_file: source.source_file.clone(),
                storage_format: None,
                size_bytes: file_size_bytes,
                region_x: None,
                region_z: None,
                chunk_count: 0,
                discarded_chunks: 0,
                warnings: Vec::new(),
                error: Some(format!("{error:#}")),
            };
        }
    };

    let size_bytes = match region_storage_size(&source.source_file, storage_format) {
        Ok(size) => size,
        Err(error) => {
            return RegionInfoEntry {
                input_index: source.input_index,
                source_file: source.source_file.clone(),
                storage_format: Some(storage_format),
                size_bytes: file_size_bytes,
                region_x: None,
                region_z: None,
                chunk_count: 0,
                discarded_chunks: 0,
                warnings: Vec::new(),
                error: Some(format!("{error:#}")),
            };
        }
    };

    match read_region(&source.source_file, storage_format.family()) {
        Ok(read) => RegionInfoEntry {
            input_index: source.input_index,
            source_file: source.source_file.clone(),
            storage_format: Some(storage_format),
            size_bytes: Some(size_bytes),
            region_x: Some(read.region.region_x),
            region_z: Some(read.region.region_z),
            chunk_count: read.region.chunk_count(),
            discarded_chunks: read.discarded_chunks,
            warnings: read.warnings,
            error: None,
        },
        Err(error) => RegionInfoEntry {
            input_index: source.input_index,
            source_file: source.source_file.clone(),
            storage_format: Some(storage_format),
            size_bytes: Some(size_bytes),
            region_x: None,
            region_z: None,
            chunk_count: 0,
            discarded_chunks: 0,
            warnings: Vec::new(),
            error: Some(format!("{error:#}")),
        },
    }
}

struct InputInfoBuilder {
    input_path: PathBuf,
    input_kind: InputKind,
    region_files: usize,
    readable_regions: usize,
    failed_regions: usize,
    total_size_bytes: u64,
    chunk_count: usize,
    discarded_chunks: usize,
    warnings: usize,
    formats: HashMap<RegionStorageFormat, usize>,
}

impl InputInfoBuilder {
    fn new(input_path: PathBuf, input_kind: InputKind) -> Self {
        Self {
            input_path,
            input_kind,
            region_files: 0,
            readable_regions: 0,
            failed_regions: 0,
            total_size_bytes: 0,
            chunk_count: 0,
            discarded_chunks: 0,
            warnings: 0,
            formats: HashMap::new(),
        }
    }

    fn apply(&mut self, entry: &RegionInfoEntry) {
        self.region_files += 1;
        self.total_size_bytes += entry.size_bytes.unwrap_or(0);
        self.chunk_count += entry.chunk_count;
        self.discarded_chunks += entry.discarded_chunks;
        self.warnings += entry.warnings.len();

        if let Some(format) = entry.storage_format {
            self.formats
                .entry(format)
                .and_modify(|count| *count += 1)
                .or_insert(1);
        }

        if entry.error.is_some() {
            self.failed_regions += 1;
        } else {
            self.readable_regions += 1;
        }
    }

    fn finish(self) -> InputInfo {
        let mut format_breakdown = self
            .formats
            .into_iter()
            .map(|(format, count)| FormatCount { format, count })
            .collect::<Vec<_>>();
        format_breakdown
            .sort_by(|left, right| left.format.to_string().cmp(&right.format.to_string()));

        InputInfo {
            input_path: self.input_path,
            input_kind: self.input_kind,
            region_files: self.region_files,
            readable_regions: self.readable_regions,
            failed_regions: self.failed_regions,
            total_size_bytes: self.total_size_bytes,
            chunk_count: self.chunk_count,
            discarded_chunks: self.discarded_chunks,
            warnings: self.warnings,
            format_breakdown,
        }
    }
}
