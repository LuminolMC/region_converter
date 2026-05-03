use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Result;
use tempfile::tempdir;

use region_converter::cli::{Cli, SourceFormatArg, TargetFormatArg};
use region_converter::convert::run;
use region_converter::discovery::discover_jobs;
use region_converter::formats::{RegionFormat, encode_region};
use region_converter::model::{ChunkData, Region};

fn reference_path(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(relative)
}

fn copy_reference_file(relative: &str, destination: &Path) -> Result<()> {
    fs::create_dir_all(
        destination
            .parent()
            .expect("test destination should always have a parent"),
    )?;
    fs::copy(reference_path(relative), destination)?;
    Ok(())
}

fn write_encoded_region(
    destination_dir: &Path,
    file_name: &str,
    encoded: &region_converter::formats::EncodedRegion,
) -> Result<()> {
    fs::create_dir_all(destination_dir)?;
    fs::write(destination_dir.join(file_name), &encoded.main_file_bytes)?;
    for sidecar in &encoded.sidecar_files {
        fs::write(destination_dir.join(&sidecar.file_name), &sidecar.bytes)?;
    }
    Ok(())
}

fn pseudo_random_bytes(len: usize) -> Vec<u8> {
    let mut state = 0x1234_5678_u32;
    let mut bytes = Vec::with_capacity(len);

    for _ in 0..len {
        state ^= state << 13;
        state ^= state >> 17;
        state ^= state << 5;
        bytes.push((state & 0xff) as u8);
    }

    bytes
}

#[test]
fn auto_detection_defers_corrupt_blinear_failures_to_job_execution() -> Result<()> {
    let temp_dir = tempdir()?;
    let input_dir = temp_dir.path().join("input");
    let output_dir = temp_dir.path().join("output");

    copy_reference_file(
        "reference/minecraft_world_trimmer_mca/test_files/r.-1.-1.mca",
        &input_dir.join("r.-1.-1.mca"),
    )?;
    fs::create_dir_all(&input_dir)?;
    fs::write(input_dir.join("r.0.0.b_linear"), [0_u8, 1, 2, 3])?;

    let summary = run(Cli {
        inputs: vec![input_dir],
        output: output_dir,
        from: SourceFormatArg::Auto,
        to: TargetFormatArg::Linear,
        threads: Some(1),
        compression_level: None,
    })?;

    assert_eq!(summary.total_jobs, 2);
    assert_eq!(summary.successful_jobs, 1);
    assert_eq!(summary.failed_jobs, 1);
    Ok(())
}

#[test]
fn discovery_skips_preexisting_output_trees() -> Result<()> {
    let temp_dir = tempdir()?;
    let world_dir = temp_dir.path().join("world");
    let output_root = world_dir.join("out");

    copy_reference_file(
        "reference/minecraft_world_trimmer_mca/test_files/r.-1.-1.mca",
        &world_dir.join("region/r.-1.-1.mca"),
    )?;
    copy_reference_file(
        "reference/minecraft_world_trimmer_mca/test_files/r.-1.-1.mca",
        &output_root.join("DIM1/region/r.9.9.mca"),
    )?;

    let jobs = discover_jobs(&[world_dir], &output_root, None, RegionFormat::Linear)?;

    assert_eq!(jobs.len(), 1);
    assert!(jobs[0].source_file.ends_with("region/r.-1.-1.mca"));
    Ok(())
}

#[test]
fn multi_input_mounts_do_not_collide_when_trailing_paths_match() -> Result<()> {
    let temp_dir = tempdir()?;
    let input_a = temp_dir.path().join("one/a/archive/world");
    let input_b = temp_dir.path().join("two/a/archive/world");
    let output_root = temp_dir.path().join("output");

    copy_reference_file(
        "reference/minecraft_world_trimmer_mca/test_files/r.-1.-1.mca",
        &input_a.join("region/r.-1.-1.mca"),
    )?;
    copy_reference_file(
        "reference/minecraft_world_trimmer_mca/test_files/r.-1.-1.mca",
        &input_b.join("region/r.-1.-1.mca"),
    )?;

    let jobs = discover_jobs(
        &[input_a, input_b],
        &output_root,
        Some(RegionFormat::Mca),
        RegionFormat::Linear,
    )?;

    assert_eq!(jobs.len(), 2);
    let destinations = jobs
        .iter()
        .map(|job| job.destination_file.clone())
        .collect::<HashSet<_>>();
    assert_eq!(destinations.len(), 2);
    Ok(())
}

#[test]
fn mca_overwrite_is_rejected_when_existing_destination_uses_external_sidecars() -> Result<()> {
    let temp_dir = tempdir()?;
    let input_dir = temp_dir.path().join("input");
    let output_dir = temp_dir.path().join("output");

    let mut region = Region::new(0, 0);
    region.set_chunk(
        0,
        ChunkData {
            timestamp: 1,
            raw_nbt: pseudo_random_bytes(1_400_000),
        },
    )?;
    let encoded = encode_region(&region, RegionFormat::Mca, 6)?;
    assert!(!encoded.sidecar_files.is_empty());

    write_encoded_region(&input_dir, "r.0.0.mca", &encoded)?;
    write_encoded_region(&output_dir, "r.0.0.mca", &encoded)?;

    let original_main = fs::read(output_dir.join("r.0.0.mca"))?;
    let original_sidecars = encoded
        .sidecar_files
        .iter()
        .map(|sidecar| {
            Ok::<_, anyhow::Error>((
                sidecar.file_name.clone(),
                fs::read(output_dir.join(&sidecar.file_name))?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;

    let summary = run(Cli {
        inputs: vec![input_dir],
        output: output_dir.clone(),
        from: SourceFormatArg::Mca,
        to: TargetFormatArg::Mca,
        threads: Some(1),
        compression_level: Some(6),
    })?;

    assert_eq!(summary.total_jobs, 1);
    assert_eq!(summary.successful_jobs, 0);
    assert_eq!(summary.failed_jobs, 1);
    assert_eq!(fs::read(output_dir.join("r.0.0.mca"))?, original_main);
    for (file_name, original_bytes) in original_sidecars {
        assert_eq!(fs::read(output_dir.join(file_name))?, original_bytes);
    }
    Ok(())
}

#[test]
fn worker_threads_are_capped_to_discovered_region_files() -> Result<()> {
    let temp_dir = tempdir()?;
    let input_dir = temp_dir.path().join("input");
    let output_dir = temp_dir.path().join("output");

    copy_reference_file(
        "reference/minecraft_world_trimmer_mca/test_files/r.-1.-1.mca",
        &input_dir.join("r.-1.-1.mca"),
    )?;

    let summary = run(Cli {
        inputs: vec![input_dir],
        output: output_dir,
        from: SourceFormatArg::Mca,
        to: TargetFormatArg::Linear,
        threads: Some(64),
        compression_level: None,
    })?;

    assert_eq!(summary.total_jobs, 1);
    assert_eq!(summary.thread_count, 1);
    Ok(())
}
