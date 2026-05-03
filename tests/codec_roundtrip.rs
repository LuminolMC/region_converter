use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Result;
use tempfile::tempdir;

use region_converter::formats::{RegionFormat, detect_format, encode_region, read_region};

fn reference_path(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(relative)
}

#[test]
fn detects_blinear_v3_sample() -> Result<()> {
    let sample = reference_path("reference/blinear_v3_test_files/r.-3.-3.b_linear");
    assert_eq!(detect_format(&sample)?, RegionFormat::BlinearV3);
    Ok(())
}

#[test]
fn parses_blinear_v3_sample() -> Result<()> {
    let sample = reference_path("reference/blinear_v3_test_files/r.-3.-3.b_linear");
    let read = read_region(&sample, RegionFormat::BlinearV3)?;
    assert!(read.region.chunk_count() > 0);
    Ok(())
}

#[test]
fn mca_roundtrip_via_blinear_v3_encoding() -> Result<()> {
    let sample = reference_path("reference/minecraft_world_trimmer_mca/test_files/r.-1.-1.mca");
    let read = read_region(&sample, RegionFormat::Mca)?;
    let encoded = encode_region(&read.region, RegionFormat::BlinearV3, 6)?;

    let temp_dir = tempdir()?;
    let output_file = temp_dir.path().join("r.-1.-1.b_linear");
    fs::write(&output_file, encoded.main_file_bytes)?;
    for sidecar in encoded.sidecar_files {
        fs::write(temp_dir.path().join(sidecar.file_name), sidecar.bytes)?;
    }

    let reparsed = read_region(&output_file, RegionFormat::BlinearV3)?;
    assert_eq!(read.region, reparsed.region);
    Ok(())
}

#[test]
fn blinear_v2_roundtrip_stays_stable() -> Result<()> {
    let sample =
        reference_path("reference/minecraft_world_trimmer_blinear_v2/test_files/r.0.0.b_linear");
    let read = read_region(&sample, RegionFormat::BlinearV2)?;
    let encoded = encode_region(&read.region, RegionFormat::BlinearV2, 6)?;

    let temp_dir = tempdir()?;
    let output_file = temp_dir.path().join("r.0.0.b_linear");
    fs::write(&output_file, encoded.main_file_bytes)?;

    let reparsed = read_region(&output_file, RegionFormat::BlinearV2)?;
    assert_eq!(read.region, reparsed.region);
    Ok(())
}
