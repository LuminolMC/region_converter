use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::fmt::{Display, Formatter};
use std::fs;
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, bail};
use walkdir::WalkDir;

use crate::formats::{
    RegionFormat, SourceFormatHint, guess_format_from_path, looks_like_region_file,
    parse_region_coords_from_path, xxhash32,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum InputKind {
    RegionFile,
    RegionDirectory,
    WorldDirectory,
}

#[derive(Clone, Debug)]
pub struct Job {
    pub source_file: PathBuf,
    pub source_format: SourceFormatHint,
    pub destination_file: PathBuf,
}

#[derive(Clone, Debug)]
pub struct RegionSource {
    pub input_index: usize,
    pub input_kind: InputKind,
    pub source_file: PathBuf,
    pub source_format: SourceFormatHint,
    pub relative_region_dir: PathBuf,
}

#[derive(Clone, Debug)]
pub struct DiscoveryResult {
    pub jobs: Vec<Job>,
    pub summary: DiscoverySummary,
}

#[derive(Clone, Debug)]
pub struct SourceDiscoveryResult {
    pub sources: Vec<RegionSource>,
    pub summary: DiscoverySummary,
}

#[derive(Clone, Debug)]
pub struct DiscoverySummary {
    pub inputs: Vec<InputDiscovery>,
    pub total_region_directories: usize,
}

#[derive(Clone, Debug)]
pub struct InputDiscovery {
    pub input_path: PathBuf,
    pub input_kind: InputKind,
    pub discovered_jobs: usize,
    pub region_directories: usize,
}

impl Display for InputKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RegionFile => f.write_str("region file"),
            Self::RegionDirectory => f.write_str("region directory"),
            Self::WorldDirectory => f.write_str("world directory"),
        }
    }
}

pub fn discover_jobs(
    inputs: &[PathBuf],
    output_root: &Path,
    forced_format: Option<SourceFormatHint>,
    target_format: RegionFormat,
) -> Result<Vec<Job>> {
    Ok(discover_jobs_with_summary(inputs, output_root, forced_format, target_format)?.jobs)
}

pub fn discover_jobs_with_summary(
    inputs: &[PathBuf],
    output_root: &Path,
    forced_format: Option<SourceFormatHint>,
    target_format: RegionFormat,
) -> Result<DiscoveryResult> {
    let discovery = discover_sources_with_summary(inputs, forced_format, Some(output_root))?;
    let directory_mounts = build_directory_mounts(inputs, &discovery.summary.inputs);
    let mut jobs = Vec::with_capacity(discovery.sources.len());

    for source in discovery.sources {
        let (region_x, region_z) = parse_region_coords_from_path(&source.source_file)?;
        let destination_dir = match source.input_kind {
            InputKind::RegionFile => output_root.to_path_buf(),
            InputKind::RegionDirectory => {
                let mount = directory_mounts
                    .get(&source.input_index)
                    .context("missing output mount for region directory input")?;
                output_root.join(mount)
            }
            InputKind::WorldDirectory => {
                let mount = directory_mounts
                    .get(&source.input_index)
                    .context("missing output mount for world directory input")?;
                output_root.join(mount).join(&source.relative_region_dir)
            }
        };

        let destination_file =
            destination_dir.join(target_format.region_file_name(region_x, region_z));
        jobs.push(Job {
            source_file: source.source_file,
            source_format: source.source_format,
            destination_file,
        });
    }

    validate_jobs(&jobs)?;
    jobs.sort_by(|left, right| left.source_file.cmp(&right.source_file));

    Ok(DiscoveryResult {
        jobs,
        summary: discovery.summary,
    })
}

pub fn discover_sources_with_summary(
    inputs: &[PathBuf],
    forced_format: Option<SourceFormatHint>,
    excluded_root: Option<&Path>,
) -> Result<SourceDiscoveryResult> {
    let mut sources = Vec::new();
    let mut input_summaries = Vec::new();
    let normalized_output_root = excluded_root.map(normalize_path_for_compare);

    for (input_index, input) in inputs.iter().enumerate() {
        if input.is_file() {
            if !looks_like_region_file(input) {
                bail!(
                    "input file {} is not a supported region file",
                    input.display()
                );
            }

            sources.push(RegionSource {
                input_index,
                input_kind: InputKind::RegionFile,
                source_file: input.clone(),
                source_format: resolve_source_format(input, forced_format)?,
                relative_region_dir: PathBuf::new(),
            });
            input_summaries.push(InputDiscovery {
                input_path: input.clone(),
                input_kind: InputKind::RegionFile,
                discovered_jobs: 1,
                region_directories: 0,
            });
            continue;
        }

        if !input.is_dir() {
            bail!(
                "input path {} is neither a supported region file nor a directory",
                input.display()
            );
        }

        let direct_files = supported_region_files_in_dir(input)?;
        if !direct_files.is_empty() {
            let job_start = sources.len();
            append_sources(
                &mut sources,
                input_index,
                InputKind::RegionDirectory,
                &direct_files,
                Path::new(""),
                forced_format,
            )?;
            input_summaries.push(InputDiscovery {
                input_path: input.clone(),
                input_kind: InputKind::RegionDirectory,
                discovered_jobs: sources.len() - job_start,
                region_directories: 1,
            });
            continue;
        }

        let mut region_dirs = Vec::new();
        for entry in WalkDir::new(input)
            .follow_links(false)
            .into_iter()
            .filter_entry(|entry| {
                !should_skip_recursive_entry(entry.path(), input, normalized_output_root.as_deref())
            })
            .filter_map(Result::ok)
            .filter(|entry| entry.file_type().is_dir())
        {
            let files = supported_region_files_in_dir(entry.path())?;
            if files.is_empty() {
                continue;
            }

            let relative = entry
                .path()
                .strip_prefix(input)
                .with_context(|| {
                    format!(
                        "failed to compute a relative path under {}",
                        input.display()
                    )
                })?
                .to_path_buf();
            region_dirs.push((relative, files));
        }

        if region_dirs.is_empty() {
            bail!(
                "input {} does not contain any supported region files",
                input.display()
            );
        }

        let job_start = sources.len();
        for (relative_region_dir, files) in &region_dirs {
            append_sources(
                &mut sources,
                input_index,
                InputKind::WorldDirectory,
                files,
                relative_region_dir,
                forced_format,
            )?;
        }

        input_summaries.push(InputDiscovery {
            input_path: input.clone(),
            input_kind: InputKind::WorldDirectory,
            discovered_jobs: sources.len() - job_start,
            region_directories: region_dirs.len(),
        });
    }

    if sources.is_empty() {
        bail!("no region files were discovered under the provided input paths");
    }

    let total_region_directories = input_summaries
        .iter()
        .map(|summary| summary.region_directories)
        .sum();

    Ok(SourceDiscoveryResult {
        sources,
        summary: DiscoverySummary {
            inputs: input_summaries,
            total_region_directories,
        },
    })
}

fn append_sources(
    sources: &mut Vec<RegionSource>,
    input_index: usize,
    input_kind: InputKind,
    files: &[PathBuf],
    relative_region_dir: &Path,
    forced_format: Option<SourceFormatHint>,
) -> Result<()> {
    for source_file in files {
        sources.push(RegionSource {
            input_index,
            input_kind,
            source_file: source_file.clone(),
            source_format: resolve_source_format(source_file, forced_format)?,
            relative_region_dir: relative_region_dir.to_path_buf(),
        });
    }

    Ok(())
}

fn resolve_source_format(
    path: &Path,
    forced_format: Option<SourceFormatHint>,
) -> Result<SourceFormatHint> {
    match forced_format {
        Some(format) => Ok(format),
        None => guess_format_from_path(path),
    }
}

fn validate_jobs(jobs: &[Job]) -> Result<()> {
    let mut seen_destinations = HashSet::new();
    for job in jobs {
        if job.source_file == job.destination_file {
            bail!(
                "refusing to overwrite the source file in place: {}",
                job.source_file.display()
            );
        }

        if !seen_destinations.insert(job.destination_file.clone()) {
            bail!(
                "multiple inputs map to the same output file {}",
                job.destination_file.display()
            );
        }
    }

    Ok(())
}

fn build_directory_mounts(
    inputs: &[PathBuf],
    input_summaries: &[InputDiscovery],
) -> HashMap<usize, String> {
    let mut mounts = HashMap::new();

    for (input_index, summary) in input_summaries.iter().enumerate() {
        if summary.input_kind == InputKind::RegionFile {
            continue;
        }

        let label = directory_mount_base_name(&summary.input_path);
        let normalized = normalize_path_for_compare(&inputs[input_index]);
        let hash = xxhash32(0, normalized.to_string_lossy().as_bytes());
        mounts.insert(input_index, format!("{label}__{hash:08x}"));
    }

    mounts
}

fn supported_region_files_in_dir(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();

    for entry in fs::read_dir(dir).with_context(|| format!("failed to list {}", dir.display()))? {
        let entry = entry?;
        let path = entry.path();
        if looks_like_region_file(&path) {
            files.push(path);
        }
    }

    files.sort();
    Ok(files)
}

fn directory_mount_base_name(path: &Path) -> String {
    path.file_name()
        .and_then(sanitize_component)
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "input".to_string())
}

fn should_skip_recursive_entry(
    candidate: &Path,
    input_root: &Path,
    normalized_output_root: Option<&Path>,
) -> bool {
    let Some(normalized_output_root) = normalized_output_root else {
        return false;
    };

    candidate != input_root
        && normalize_path_for_compare(candidate).starts_with(normalized_output_root)
}

fn normalize_path_for_compare(path: &Path) -> PathBuf {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    };

    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Normal(part) => normalized.push(part),
        }
    }
    normalized
}

fn sanitize_component(value: &OsStr) -> Option<String> {
    let text = value.to_string_lossy();
    let cleaned = text
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | '.') {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();

    if cleaned.is_empty() {
        None
    } else {
        Some(cleaned)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn directory_mounts_keep_the_folder_name_when_unique() {
        let inputs = vec![PathBuf::from("/tmp/my-world")];
        let summaries = vec![InputDiscovery {
            input_path: inputs[0].clone(),
            input_kind: InputKind::WorldDirectory,
            discovered_jobs: 1,
            region_directories: 1,
        }];

        let mounts = build_directory_mounts(&inputs, &summaries);
        assert!(mounts[&0].starts_with("my-world__"));
    }

    #[test]
    fn directory_mounts_disambiguate_duplicate_folder_names() {
        let inputs = vec![PathBuf::from("/tmp/a/world"), PathBuf::from("/tmp/b/world")];
        let summaries = inputs
            .iter()
            .map(|path| InputDiscovery {
                input_path: path.clone(),
                input_kind: InputKind::WorldDirectory,
                discovered_jobs: 1,
                region_directories: 1,
            })
            .collect::<Vec<_>>();

        let mounts = build_directory_mounts(&inputs, &summaries);
        assert_ne!(mounts.get(&0), mounts.get(&1));
        assert!(mounts[&0].starts_with("world__"));
        assert!(mounts[&1].starts_with("world__"));
    }

    #[test]
    fn directory_mounts_are_stable_across_separate_invocations() {
        let input_a = PathBuf::from("/tmp/a/world");
        let input_b = PathBuf::from("/tmp/b/world");
        let summary_a = vec![InputDiscovery {
            input_path: input_a.clone(),
            input_kind: InputKind::WorldDirectory,
            discovered_jobs: 1,
            region_directories: 1,
        }];
        let summary_b = vec![InputDiscovery {
            input_path: input_b.clone(),
            input_kind: InputKind::WorldDirectory,
            discovered_jobs: 1,
            region_directories: 1,
        }];

        let mounts_a = build_directory_mounts(&[input_a], &summary_a);
        let mounts_b = build_directory_mounts(&[input_b], &summary_b);
        assert_ne!(mounts_a[&0], mounts_b[&0]);
    }
}
