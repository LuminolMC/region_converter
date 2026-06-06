use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Result;
use tempfile::tempdir;

use region_converter::cli::{Cli, SourceFormatArg, TargetFormatArg};
use region_converter::convert::{
    JobReport, ProgressSnapshot, RunObserver, RunStage, run, run_with_observer,
};
use region_converter::discovery::{RegionFileGroup, discover_jobs, discover_jobs_with_summary};
use region_converter::formats::{RegionFormat, SourceFormatHint, encode_region_to_writer};
use region_converter::info::inspect;
use region_converter::model::{ChunkData, Region};
use region_converter::writer::write_region_with_transaction;

fn write_region_file(destination: &Path, format: RegionFormat, region: &Region) -> Result<()> {
    write_region_with_transaction(format, destination, |target| {
        encode_region_to_writer(region, format, 6, target)?;
        Ok(())
    })
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

fn write_sample_mca_file(destination: &Path) -> Result<()> {
    let mut region = Region::new(0, 0);
    region.set_chunk(
        0,
        ChunkData {
            timestamp: 1,
            raw_nbt: pseudo_random_bytes(256),
        },
    )?;
    write_region_file(destination, RegionFormat::Mca, &region)
}

fn conversion_cli(
    inputs: Vec<PathBuf>,
    output: PathBuf,
    from: SourceFormatArg,
    to: TargetFormatArg,
) -> Cli {
    Cli {
        inputs,
        output: Some(output),
        info: false,
        from,
        to: Some(to),
        threads: Some(1),
        compression_level: None,
        profile: false,
    }
}

#[test]
fn auto_detection_defers_corrupt_blinear_failures_to_job_execution() -> Result<()> {
    let temp_dir = tempdir()?;
    let input_dir = temp_dir.path().join("input");
    let output_dir = temp_dir.path().join("output");

    write_sample_mca_file(&input_dir.join("r.-1.-1.mca"))?;
    fs::create_dir_all(&input_dir)?;
    fs::write(input_dir.join("r.0.0.b_linear"), [0_u8, 1, 2, 3])?;

    let summary = run(conversion_cli(
        vec![input_dir],
        output_dir,
        SourceFormatArg::Auto,
        TargetFormatArg::Linear,
    ))?;

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

    write_sample_mca_file(&world_dir.join("region/r.-1.-1.mca"))?;
    write_sample_mca_file(&output_root.join("DIM1/region/r.9.9.mca"))?;

    let jobs = discover_jobs(&[world_dir], &output_root, None, RegionFormat::Linear)?;

    assert_eq!(jobs.len(), 1);
    assert!(jobs[0].source_file.ends_with("region/r.-1.-1.mca"));
    assert!(
        jobs[0]
            .destination_file
            .to_string_lossy()
            .contains("/out/world__")
    );
    assert!(jobs[0].destination_file.ends_with("region/r.-1.-1.linear"));
    Ok(())
}

#[test]
fn multi_input_mounts_do_not_collide_when_trailing_paths_match() -> Result<()> {
    let temp_dir = tempdir()?;
    let input_a = temp_dir.path().join("one/a/archive/world");
    let input_b = temp_dir.path().join("two/a/archive/world");
    let output_root = temp_dir.path().join("output");

    write_sample_mca_file(&input_a.join("region/r.-1.-1.mca"))?;
    write_sample_mca_file(&input_b.join("region/r.-1.-1.mca"))?;

    let jobs = discover_jobs(
        &[input_a.clone(), input_b.clone()],
        &output_root,
        Some(SourceFormatHint::Mca),
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
fn directory_mounts_remain_unique_across_separate_runs() -> Result<()> {
    let temp_dir = tempdir()?;
    let input_a = temp_dir.path().join("a/world");
    let input_b = temp_dir.path().join("b/world");
    let output_root = temp_dir.path().join("output");

    write_sample_mca_file(&input_a.join("region/r.-1.-1.mca"))?;
    write_sample_mca_file(&input_b.join("region/r.-1.-1.mca"))?;

    let jobs_a = discover_jobs(
        std::slice::from_ref(&input_a),
        &output_root,
        Some(SourceFormatHint::Mca),
        RegionFormat::Linear,
    )?;
    let jobs_b = discover_jobs(
        std::slice::from_ref(&input_b),
        &output_root,
        Some(SourceFormatHint::Mca),
        RegionFormat::Linear,
    )?;

    assert_eq!(jobs_a.len(), 1);
    assert_eq!(jobs_b.len(), 1);
    assert_ne!(jobs_a[0].destination_file, jobs_b[0].destination_file);
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

    write_region_file(&input_dir.join("r.0.0.mca"), RegionFormat::Mca, &region)?;
    assert!(input_dir.join("c.0.0.mcc").exists());
    let mapped_output_dir = discover_jobs(
        std::slice::from_ref(&input_dir),
        &output_dir,
        Some(SourceFormatHint::Mca),
        RegionFormat::Mca,
    )?[0]
        .destination_file
        .parent()
        .expect("discovered destination should have a parent")
        .to_path_buf();
    write_region_file(
        &mapped_output_dir.join("r.0.0.mca"),
        RegionFormat::Mca,
        &region,
    )?;

    let original_main = fs::read(mapped_output_dir.join("r.0.0.mca"))?;
    let original_sidecars = vec![(
        "c.0.0.mcc".to_string(),
        fs::read(mapped_output_dir.join("c.0.0.mcc"))?,
    )];

    let mut cli = conversion_cli(
        vec![input_dir],
        output_dir.clone(),
        SourceFormatArg::Mca,
        TargetFormatArg::Mca,
    );
    cli.compression_level = Some(6);

    let summary = run(cli)?;

    assert_eq!(summary.total_jobs, 1);
    assert_eq!(summary.successful_jobs, 0);
    assert_eq!(summary.failed_jobs, 1);
    assert_eq!(
        fs::read(mapped_output_dir.join("r.0.0.mca"))?,
        original_main
    );
    for (file_name, original_bytes) in original_sidecars {
        assert_eq!(fs::read(mapped_output_dir.join(file_name))?, original_bytes);
    }
    Ok(())
}

#[test]
fn worker_threads_are_capped_to_discovered_region_files() -> Result<()> {
    let temp_dir = tempdir()?;
    let input_dir = temp_dir.path().join("input");
    let output_dir = temp_dir.path().join("output");

    write_sample_mca_file(&input_dir.join("r.-1.-1.mca"))?;

    let mut cli = conversion_cli(
        vec![input_dir],
        output_dir,
        SourceFormatArg::Mca,
        TargetFormatArg::Linear,
    );
    cli.threads = Some(64);

    let summary = run(cli)?;

    assert_eq!(summary.total_jobs, 1);
    assert_eq!(summary.thread_count, 1);
    Ok(())
}

#[test]
fn profile_reports_encoded_payload_details_for_each_target_format() -> Result<()> {
    let targets = [
        TargetFormatArg::Mca,
        TargetFormatArg::Linear,
        TargetFormatArg::BlinearV2,
        TargetFormatArg::BlinearV3,
    ];

    for target in targets {
        let temp_dir = tempdir()?;
        let input_dir = temp_dir.path().join("input");
        let output_dir = temp_dir.path().join("output");
        let mut region = Region::new(0, 0);
        region.set_chunk(
            0,
            ChunkData {
                timestamp: 123,
                raw_nbt: pseudo_random_bytes(8 * 1024),
            },
        )?;
        region.set_chunk(
            17,
            ChunkData {
                timestamp: 456,
                raw_nbt: pseudo_random_bytes(6 * 1024),
            },
        )?;

        write_region_file(&input_dir.join("r.0.0.mca"), RegionFormat::Mca, &region)?;

        let mut cli = conversion_cli(vec![input_dir], output_dir, SourceFormatArg::Mca, target);
        cli.profile = true;

        let summary = run(cli)?;
        let profile = summary.profile.expect("profile should be captured");

        assert_eq!(summary.successful_jobs, 1);
        assert_eq!(profile.slowest_jobs.len(), 1);
        assert!(profile.encoded_units > 0);
        assert!(profile.raw_payload_bytes > 0);
        assert!(profile.compressed_payload_bytes > 0);
        assert_eq!(profile.slowest_jobs[0].encoded_units, profile.encoded_units);
        assert_eq!(
            profile.slowest_jobs[0].raw_payload_bytes,
            profile.raw_payload_bytes
        );
        assert_eq!(
            profile.slowest_jobs[0].compressed_payload_bytes,
            profile.compressed_payload_bytes
        );
    }

    Ok(())
}

#[test]
fn single_region_file_inputs_write_directly_under_output_root() -> Result<()> {
    let temp_dir = tempdir()?;
    let input_file = temp_dir.path().join("r.-1.-1.mca");
    let output_root = temp_dir.path().join("output");

    write_sample_mca_file(&input_file)?;

    let jobs = discover_jobs(
        std::slice::from_ref(&input_file),
        &output_root,
        Some(SourceFormatHint::Mca),
        RegionFormat::Linear,
    )?;

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].source_file, input_file);
    assert_eq!(jobs[0].destination_file, output_root.join("r.-1.-1.linear"));
    Ok(())
}

#[test]
fn discovery_classifies_world_region_entities_and_poi_groups() -> Result<()> {
    let temp_dir = tempdir()?;
    let world_dir = temp_dir.path().join("world");
    let output_root = temp_dir.path().join("output");

    write_sample_mca_file(&world_dir.join("region/r.-1.-1.mca"))?;
    write_sample_mca_file(&world_dir.join("entities/r.0.0.mca"))?;
    write_sample_mca_file(&world_dir.join("poi/r.1.1.mca"))?;

    let discovery = discover_jobs_with_summary(
        std::slice::from_ref(&world_dir),
        &output_root,
        Some(SourceFormatHint::Mca),
        RegionFormat::Linear,
    )?;

    assert_eq!(discovery.jobs.len(), 3);
    assert_eq!(discovery.summary.inputs[0].group_counts.regions, 1);
    assert_eq!(discovery.summary.inputs[0].group_counts.entities, 1);
    assert_eq!(discovery.summary.inputs[0].group_counts.poi, 1);
    assert!(
        discovery
            .jobs
            .iter()
            .any(|job| job.file_group == RegionFileGroup::Regions)
    );
    assert!(
        discovery
            .jobs
            .iter()
            .any(|job| job.file_group == RegionFileGroup::Entities)
    );
    assert!(
        discovery
            .jobs
            .iter()
            .any(|job| job.file_group == RegionFileGroup::Poi)
    );
    Ok(())
}

#[test]
fn direct_entities_directory_input_is_classified_as_entities() -> Result<()> {
    let temp_dir = tempdir()?;
    let input_dir = temp_dir.path().join("entities");
    let output_root = temp_dir.path().join("output");

    write_sample_mca_file(&input_dir.join("r.-1.-1.mca"))?;

    let discovery = discover_jobs_with_summary(
        std::slice::from_ref(&input_dir),
        &output_root,
        Some(SourceFormatHint::Mca),
        RegionFormat::Linear,
    )?;

    assert_eq!(discovery.jobs.len(), 1);
    assert_eq!(discovery.jobs[0].file_group, RegionFileGroup::Entities);
    assert_eq!(discovery.summary.inputs[0].group_counts.entities, 1);
    Ok(())
}

#[test]
fn conversion_runs_inputs_and_world_groups_in_sequence() -> Result<()> {
    let temp_dir = tempdir()?;
    let input_a = temp_dir.path().join("a_world");
    let input_b = temp_dir.path().join("b_world");
    let output_root = temp_dir.path().join("output");

    write_sample_mca_file(&input_a.join("region/r.-1.-1.mca"))?;
    write_sample_mca_file(&input_a.join("entities/r.0.0.mca"))?;
    write_sample_mca_file(&input_b.join("poi/r.1.1.mca"))?;

    let mut observer = RecordingObserver::default();
    let summary = run_with_observer(
        conversion_cli(
            vec![input_a, input_b],
            output_root,
            SourceFormatArg::Mca,
            TargetFormatArg::Linear,
        ),
        &mut observer,
    )?;

    assert_eq!(summary.successful_jobs, 3);
    assert_eq!(
        observer.events,
        vec![
            "stage 0 regions",
            "stage 0 entities",
            "completed 0",
            "stage 1 poi",
            "completed 1",
        ]
    );
    Ok(())
}

#[derive(Default)]
struct RecordingObserver {
    events: Vec<String>,
}

impl RunObserver for RecordingObserver {
    fn on_stage_start(&mut self, stage: &RunStage) -> Result<()> {
        self.events
            .push(format!("stage {} {}", stage.input_index, stage.file_group));
        Ok(())
    }

    fn on_job_report(&mut self, _report: &JobReport, _progress: &ProgressSnapshot) -> Result<()> {
        Ok(())
    }

    fn on_input_finish(
        &mut self,
        summary: &region_converter::convert::RunInputSummary,
    ) -> Result<()> {
        self.events
            .push(format!("completed {}", summary.input_index));
        Ok(())
    }
}

#[test]
fn info_mode_reports_single_region_file_details() -> Result<()> {
    let temp_dir = tempdir()?;
    let input_file = temp_dir.path().join("r.-1.-1.mca");

    write_sample_mca_file(&input_file)?;

    let summary = inspect(Cli {
        inputs: vec![input_file.clone()],
        output: None,
        info: true,
        from: SourceFormatArg::Auto,
        to: None,
        threads: Some(1),
        compression_level: None,
        profile: false,
    })?;

    assert_eq!(summary.total_region_files, 1);
    assert_eq!(summary.readable_regions, 1);
    assert_eq!(summary.failed_regions, 0);
    assert_eq!(summary.inputs.len(), 1);
    assert_eq!(summary.inputs[0].input_path, input_file);
    assert_eq!(summary.inputs[0].region_files, 1);
    assert!(summary.chunk_count > 0);
    assert_eq!(
        summary.entries[0]
            .storage_format
            .map(|format| format.to_string()),
        Some("mca".to_string())
    );
    Ok(())
}

#[test]
fn info_mode_reports_world_group_readable_counts() -> Result<()> {
    let temp_dir = tempdir()?;
    let world_dir = temp_dir.path().join("world");

    write_sample_mca_file(&world_dir.join("region/r.-1.-1.mca"))?;
    write_sample_mca_file(&world_dir.join("entities/r.0.0.mca"))?;
    write_sample_mca_file(&world_dir.join("poi/r.1.1.mca"))?;

    let summary = inspect(Cli {
        inputs: vec![world_dir],
        output: None,
        info: true,
        from: SourceFormatArg::Auto,
        to: None,
        threads: Some(1),
        compression_level: None,
        profile: false,
    })?;

    assert_eq!(summary.inputs[0].group_breakdown.len(), 3);
    for group in &summary.inputs[0].group_breakdown {
        assert_eq!(group.region_files, 1);
        assert_eq!(group.readable_regions, 1);
        assert_eq!(group.failed_regions, 0);
    }
    Ok(())
}

#[test]
fn info_mode_counts_unreadable_file_sizes_in_totals() -> Result<()> {
    let temp_dir = tempdir()?;
    let input_file = temp_dir.path().join("r.0.0.linear");
    fs::write(&input_file, [0_u8, 1, 2, 3])?;

    let summary = inspect(Cli {
        inputs: vec![input_file.clone()],
        output: None,
        info: true,
        from: SourceFormatArg::Auto,
        to: None,
        threads: Some(1),
        compression_level: None,
        profile: false,
    })?;

    assert_eq!(summary.total_region_files, 1);
    assert_eq!(summary.readable_regions, 0);
    assert_eq!(summary.failed_regions, 1);
    assert_eq!(summary.total_size_bytes, 4);
    assert_eq!(summary.inputs[0].total_size_bytes, 4);
    assert_eq!(summary.entries[0].size_bytes, Some(4));
    Ok(())
}
