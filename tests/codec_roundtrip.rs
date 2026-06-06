use std::path::{Path, PathBuf};

use anyhow::Result;
use tempfile::tempdir;

use region_converter::formats::{
    RegionFormat, RegionStorageFormat, detect_format, detect_storage_format,
    encode_region_to_writer, read_region,
};
use region_converter::model::Region;
use region_converter::writer::write_region_with_transaction;

fn optional_reference_path(relative: &str) -> Option<PathBuf> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(relative);
    if path.exists() {
        Some(path)
    } else {
        eprintln!(
            "skipping sample-dependent test because {} is missing",
            path.display()
        );
        None
    }
}

fn write_region_file(destination: &Path, format: RegionFormat, region: &Region) -> Result<()> {
    write_region_with_transaction(format, destination, |target| {
        encode_region_to_writer(region, format, 6, target)?;
        Ok(())
    })
}

fn assert_region_payloads_eq(expected: &Region, actual: &Region) {
    assert_eq!(expected.chunk_count(), actual.chunk_count());
    for (index, expected_chunk) in expected.iter_chunks() {
        let actual_chunk = actual
            .chunk(index)
            .unwrap_or_else(|| panic!("missing chunk slot {index}"));
        assert_eq!(expected_chunk.raw_nbt, actual_chunk.raw_nbt);
    }
}

#[test]
fn detects_blinear_v3_sample() -> Result<()> {
    let Some(sample) = optional_reference_path("reference/blinear_v3_test_files/r.-3.-3.b_linear")
    else {
        return Ok(());
    };
    assert_eq!(detect_format(&sample)?, RegionFormat::BlinearV3);
    Ok(())
}

#[test]
fn parses_blinear_v3_sample() -> Result<()> {
    let Some(sample) = optional_reference_path("reference/blinear_v3_test_files/r.-3.-3.b_linear")
    else {
        return Ok(());
    };
    let read = read_region(&sample, RegionFormat::BlinearV3)?;
    assert!(read.region.chunk_count() > 0);
    Ok(())
}

#[test]
fn mca_roundtrip_via_blinear_v3_encoding() -> Result<()> {
    let Some(sample) =
        optional_reference_path("reference/minecraft_world_trimmer_mca/test_files/r.-1.-1.mca")
    else {
        return Ok(());
    };
    let read = read_region(&sample, RegionFormat::Mca)?;

    let temp_dir = tempdir()?;
    let output_file = temp_dir.path().join("r.-1.-1.b_linear");
    write_region_file(&output_file, RegionFormat::BlinearV3, &read.region)?;

    let reparsed = read_region(&output_file, RegionFormat::BlinearV3)?;
    assert_region_payloads_eq(&read.region, &reparsed.region);
    for (index, chunk) in read.region.iter_chunks() {
        let reparsed_chunk = reparsed.region.chunk(index).unwrap();
        assert_eq!(reparsed_chunk.timestamp, chunk.timestamp * 1000);
    }
    Ok(())
}

#[test]
fn blinear_v2_roundtrip_stays_stable() -> Result<()> {
    let Some(sample) = optional_reference_path(
        "reference/minecraft_world_trimmer_blinear_v2/test_files/r.0.0.b_linear",
    ) else {
        return Ok(());
    };
    let read = read_region(&sample, RegionFormat::BlinearV2)?;

    let temp_dir = tempdir()?;
    let output_file = temp_dir.path().join("r.0.0.b_linear");
    write_region_file(&output_file, RegionFormat::BlinearV2, &read.region)?;

    let reparsed = read_region(&output_file, RegionFormat::BlinearV2)?;
    assert_eq!(read.region, reparsed.region);
    Ok(())
}

#[test]
fn blinear_to_linear_v3_normalizes_timestamps_to_seconds() -> Result<()> {
    let Some(sample) = optional_reference_path(
        "reference/minecraft_world_trimmer_blinear_v2/test_files/r.0.0.b_linear",
    ) else {
        return Ok(());
    };
    let read = read_region(&sample, RegionFormat::BlinearV2)?;

    let temp_dir = tempdir()?;
    let output_file = temp_dir.path().join("r.0.0.linear");
    write_region_file(&output_file, RegionFormat::Linear, &read.region)?;

    let reparsed = read_region(&output_file, RegionFormat::Linear)?;
    assert_region_payloads_eq(&read.region, &reparsed.region);
    for (index, chunk) in read.region.iter_chunks() {
        let reparsed_chunk = reparsed.region.chunk(index).unwrap();
        assert_eq!(reparsed_chunk.timestamp, chunk.timestamp / 1000);
    }
    Ok(())
}

#[test]
fn linear_target_writes_linear_v3_and_roundtrips() -> Result<()> {
    let Some(sample) =
        optional_reference_path("reference/minecraft_world_trimmer_mca/test_files/r.-1.-1.mca")
    else {
        return Ok(());
    };
    let read = read_region(&sample, RegionFormat::Mca)?;

    let temp_dir = tempdir()?;
    let output_file = temp_dir.path().join("r.-1.-1.linear");
    write_region_file(&output_file, RegionFormat::Linear, &read.region)?;

    assert_eq!(detect_format(&output_file)?, RegionFormat::Linear);
    assert_eq!(
        detect_storage_format(&output_file)?,
        RegionStorageFormat::LinearV3
    );

    let reparsed = read_region(&output_file, RegionFormat::Linear)?;
    assert_eq!(read.region, reparsed.region);
    Ok(())
}
