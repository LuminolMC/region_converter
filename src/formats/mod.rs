mod blinear_v2;
mod blinear_v3;
mod linear;
mod mca;

use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io::Read;
use std::path::Path;

use anyhow::{Context, Result, bail, ensure};
use xxhash_rust::xxh32::xxh32;

use crate::model::{ChunkData, Region};

pub const BLINEAR_SUPERBLOCK: i64 = -0x2008_1225_0269;
pub const LINEAR_SUPERBLOCK: u64 = 0xc3ff_1318_3cca_9d9a;
pub const BLINEAR_HASH_SEED: u32 = 0x0721;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum RegionFormat {
    Mca,
    Linear,
    Blinear,
    BlinearV2,
    BlinearV3,
}

#[derive(Debug)]
pub struct ReadOutcome {
    pub region: Region,
    pub warnings: Vec<String>,
    pub discarded_chunks: usize,
}

#[derive(Debug)]
pub struct EncodedRegion {
    pub main_file_bytes: Vec<u8>,
    pub sidecar_files: Vec<SidecarFile>,
    pub warnings: Vec<String>,
}

#[derive(Debug)]
pub struct SidecarFile {
    pub file_name: String,
    pub bytes: Vec<u8>,
}

impl RegionFormat {
    pub fn extension(self) -> &'static str {
        match self {
            Self::Mca => "mca",
            Self::Linear => "linear",
            Self::Blinear | Self::BlinearV2 | Self::BlinearV3 => "b_linear",
        }
    }

    pub fn region_file_name(self, region_x: i32, region_z: i32) -> String {
        format!("r.{region_x}.{region_z}.{}", self.extension())
    }

    pub fn default_compression_level(self) -> i32 {
        match self {
            Self::Mca => 6,
            Self::Linear | Self::Blinear | Self::BlinearV2 | Self::BlinearV3 => 6,
        }
    }
}

impl Display for RegionFormat {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Mca => f.write_str("mca"),
            Self::Linear => f.write_str("linear"),
            Self::Blinear => f.write_str("blinear"),
            Self::BlinearV2 => f.write_str("blinear_v2"),
            Self::BlinearV3 => f.write_str("blinear_v3"),
        }
    }
}

pub fn guess_format_from_path(path: &Path) -> Result<RegionFormat> {
    match path.extension().and_then(|ext| ext.to_str()) {
        Some("mca") => Ok(RegionFormat::Mca),
        Some("linear") => Ok(RegionFormat::Linear),
        Some("b_linear") => Ok(RegionFormat::Blinear),
        _ => bail!("unsupported region file extension for {}", path.display()),
    }
}

pub fn detect_format(path: &Path) -> Result<RegionFormat> {
    match path.extension().and_then(|ext| ext.to_str()) {
        Some("mca") => Ok(RegionFormat::Mca),
        Some("linear") => Ok(RegionFormat::Linear),
        Some("b_linear") => detect_blinear_format(path),
        _ => bail!("unsupported region file extension for {}", path.display()),
    }
}

pub fn region_uses_external_chunks(path: &Path) -> Result<bool> {
    mca::region_uses_external_chunks(path)
}

pub fn read_region(path: &Path, format: RegionFormat) -> Result<ReadOutcome> {
    match format {
        RegionFormat::Mca => mca::read_region(path),
        RegionFormat::Linear => linear::read_region(path),
        RegionFormat::Blinear => read_region(path, detect_format(path)?),
        RegionFormat::BlinearV2 => blinear_v2::read_region(path),
        RegionFormat::BlinearV3 => blinear_v3::read_region(path),
    }
}

pub fn encode_region(
    region: &Region,
    format: RegionFormat,
    compression_level: i32,
) -> Result<EncodedRegion> {
    match format {
        RegionFormat::Mca => mca::encode_region(region, compression_level),
        RegionFormat::Linear => linear::encode_region(region, compression_level),
        RegionFormat::Blinear => {
            bail!("generic blinear format is only valid as an input placeholder")
        }
        RegionFormat::BlinearV2 => blinear_v2::encode_region(region, compression_level),
        RegionFormat::BlinearV3 => blinear_v3::encode_region(region, compression_level),
    }
}

pub fn looks_like_region_file(path: &Path) -> bool {
    if !path.is_file() {
        return false;
    }

    match path.extension().and_then(|ext| ext.to_str()) {
        Some("mca") | Some("linear") | Some("b_linear") => {
            parse_region_coords_from_path(path).is_ok()
        }
        _ => false,
    }
}

pub fn parse_region_coords_from_path(path: &Path) -> Result<(i32, i32)> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .with_context(|| format!("region file name is not valid UTF-8: {}", path.display()))?;

    let parts: Vec<&str> = file_name.split('.').collect();
    ensure!(
        parts.len() >= 4 && parts.first() == Some(&"r"),
        "region file name does not match r.<x>.<z>.<ext>: {}",
        file_name
    );

    let region_x = parts[1]
        .parse::<i32>()
        .with_context(|| format!("invalid region x coordinate in {}", file_name))?;
    let region_z = parts[2]
        .parse::<i32>()
        .with_context(|| format!("invalid region z coordinate in {}", file_name))?;

    Ok((region_x, region_z))
}

pub(crate) fn decode_chunk_section(section: &[u8], hash_seed: u32) -> Result<ChunkData> {
    ensure!(
        section.len() >= 16,
        "chunk section is shorter than 16 bytes"
    );

    let declared_len = i32::from_be_bytes(section[0..4].try_into().unwrap());
    ensure!(
        declared_len >= 0,
        "chunk section has a negative payload length"
    );
    let declared_len = declared_len as usize;

    let timestamp = i64::from_be_bytes(section[4..12].try_into().unwrap());
    let expected_hash = u32::from_be_bytes(section[12..16].try_into().unwrap());
    let raw_nbt = &section[16..];

    ensure!(
        raw_nbt.len() == declared_len,
        "chunk section length mismatch: declared {declared_len} bytes but found {} bytes",
        raw_nbt.len()
    );

    let actual_hash = xxhash32(hash_seed, raw_nbt);
    ensure!(
        actual_hash == expected_hash,
        "chunk section xxhash32 mismatch: expected {expected_hash:#010x}, got {actual_hash:#010x}"
    );

    Ok(ChunkData {
        timestamp,
        raw_nbt: raw_nbt.to_vec(),
    })
}

pub(crate) fn encode_chunk_section(chunk: &ChunkData, hash_seed: u32) -> Result<Vec<u8>> {
    let raw_len = i32::try_from(chunk.raw_nbt.len()).context("chunk payload is too large")?;
    let hash = xxhash32(hash_seed, &chunk.raw_nbt);

    let mut section = Vec::with_capacity(16 + chunk.raw_nbt.len());
    section.extend_from_slice(&raw_len.to_be_bytes());
    section.extend_from_slice(&chunk.timestamp.to_be_bytes());
    section.extend_from_slice(&hash.to_be_bytes());
    section.extend_from_slice(&chunk.raw_nbt);
    Ok(section)
}

pub(crate) fn normalize_timestamp_to_u32(timestamp: i64) -> (u32, bool) {
    if timestamp <= 0 {
        return (0, timestamp != 0);
    }

    if let Ok(value) = u32::try_from(timestamp) {
        return (value, false);
    }

    let seconds = timestamp / 1000;
    if let Ok(value) = u32::try_from(seconds) {
        return (value, true);
    }

    (u32::MAX, true)
}

pub(crate) fn xxhash32(seed: u32, bytes: &[u8]) -> u32 {
    xxh32(bytes, seed)
}

fn detect_blinear_format(path: &Path) -> Result<RegionFormat> {
    let mut header = [0_u8; 9];
    let mut file =
        File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    file.read_exact(&mut header)
        .with_context(|| format!("failed to read blinear header from {}", path.display()))?;

    let superblock = i64::from_be_bytes(header[0..8].try_into().unwrap());
    ensure!(
        superblock == BLINEAR_SUPERBLOCK,
        "unknown blinear superblock in {}",
        path.display()
    );

    match header[8] {
        0x02 => Ok(RegionFormat::BlinearV2),
        0x03 => Ok(RegionFormat::BlinearV3),
        version => bail!(
            "unsupported blinear version {version} in {}",
            path.display()
        ),
    }
}
