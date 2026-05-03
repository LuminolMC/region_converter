use std::collections::HashSet;
use std::ffi::OsStr;
use std::fs;
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, bail};
use walkdir::WalkDir;

use crate::formats::{
    RegionFormat, detect_format, looks_like_region_file, parse_region_coords_from_path,
};

#[derive(Clone, Debug)]
pub struct Job {
    pub source_file: PathBuf,
    pub source_format: RegionFormat,
    pub destination_file: PathBuf,
}

pub fn discover_jobs(
    inputs: &[PathBuf],
    output_root: &Path,
    forced_format: Option<RegionFormat>,
    target_format: RegionFormat,
) -> Result<Vec<Job>> {
    let mut jobs = Vec::new();
    let multiple_inputs = inputs.len() > 1;

    for input in inputs {
        if !input.is_dir() {
            bail!(
                "input path {} is not a directory; this converter expects world directories or region directories",
                input.display()
            );
        }

        let direct_files = supported_region_files_in_dir(input)?;
        if !direct_files.is_empty() {
            let destination_dir = if multiple_inputs {
                output_root.join(mount_label(input))
            } else {
                output_root.to_path_buf()
            };
            append_jobs(
                &mut jobs,
                &direct_files,
                &destination_dir,
                forced_format,
                target_format,
            )?;
            continue;
        }

        let mut region_dirs = Vec::new();
        for entry in WalkDir::new(input)
            .follow_links(false)
            .into_iter()
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
                "input directory {} does not contain any supported region files",
                input.display()
            );
        }

        let root_mount = if multiple_inputs {
            output_root.join(mount_label(input))
        } else {
            output_root.to_path_buf()
        };

        for (relative_region_dir, files) in region_dirs {
            append_jobs(
                &mut jobs,
                &files,
                &root_mount.join(relative_region_dir),
                forced_format,
                target_format,
            )?;
        }
    }

    if jobs.is_empty() {
        bail!("no region files were discovered under the provided input paths");
    }

    let mut seen_destinations = HashSet::new();
    for job in &jobs {
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

    jobs.sort_by(|left, right| left.source_file.cmp(&right.source_file));
    Ok(jobs)
}

fn append_jobs(
    jobs: &mut Vec<Job>,
    files: &[PathBuf],
    destination_dir: &Path,
    forced_format: Option<RegionFormat>,
    target_format: RegionFormat,
) -> Result<()> {
    for source_file in files {
        let source_format = match forced_format {
            Some(format) => format,
            None => detect_format(source_file)?,
        };
        let (region_x, region_z) = parse_region_coords_from_path(source_file)?;
        let destination_file =
            destination_dir.join(target_format.region_file_name(region_x, region_z));

        jobs.push(Job {
            source_file: source_file.clone(),
            source_format,
            destination_file,
        });
    }

    Ok(())
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

fn mount_label(path: &Path) -> String {
    let mut components = path
        .components()
        .filter_map(component_name)
        .collect::<Vec<_>>();

    if components.is_empty() {
        return "input".to_string();
    }

    if components.len() > 3 {
        components = components.split_off(components.len() - 3);
    }

    components.join("__")
}

fn component_name(component: Component<'_>) -> Option<String> {
    match component {
        Component::Normal(value) => sanitize_component(value),
        _ => None,
    }
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
