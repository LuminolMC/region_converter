use std::collections::HashSet;
use std::io::{Read, Write};
use std::path::Path;

use anyhow::{Context, Result, bail, ensure};
use byteorder::{BigEndian, WriteBytesExt};
use flate2::Compression;
use flate2::read::{GzDecoder, ZlibDecoder};
use flate2::write::ZlibEncoder;

use crate::formats::{
    EncodedRegion, ReadOutcome, SidecarFile, normalize_timestamp_to_u32,
    parse_region_coords_from_path,
};
use crate::model::{ChunkData, REGION_CHUNK_COUNT, REGION_SIDE, Region};

const MCA_HEADER_SIZE: usize = 8192;
const MCA_SECTOR_SIZE: usize = 4096;
const MCA_EXTERNAL_FLAG: u8 = 0x80;
const MCA_COMPRESSION_GZIP: u8 = 1;
const MCA_COMPRESSION_ZLIB: u8 = 2;
const MCA_EXTERNAL_ZLIB: u8 = MCA_EXTERNAL_FLAG | MCA_COMPRESSION_ZLIB;
const MAX_INLINE_SECTORS: usize = 255;

pub fn read_region(path: &Path) -> Result<ReadOutcome> {
    let bytes =
        std::fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    ensure!(
        bytes.len() >= MCA_HEADER_SIZE,
        "mca region {} is smaller than the 8 KiB header",
        path.display()
    );

    let (region_x, region_z) = parse_region_coords_from_path(path)?;
    let mut region = Region::new(region_x, region_z);
    let mut warnings = Vec::new();
    let mut discarded_chunks = 0;

    for index in 0..REGION_CHUNK_COUNT {
        let location_offset = index * 4;
        let timestamp_offset = 4096 + index * 4;

        let location = u32::from_be_bytes(
            bytes[location_offset..location_offset + 4]
                .try_into()
                .unwrap(),
        );
        let timestamp = u32::from_be_bytes(
            bytes[timestamp_offset..timestamp_offset + 4]
                .try_into()
                .unwrap(),
        );

        let sector_index = ((location >> 8) & 0x00ff_ffff) as usize;
        let sector_count = (location & 0xff) as usize;

        if sector_index == 0 && sector_count == 0 {
            continue;
        }

        let chunk_label = format!("chunk slot {index} in {}", path.display());

        if sector_index == 0 || sector_count == 0 {
            discarded_chunks += 1;
            warnings.push(format!(
                "{chunk_label} has an invalid sector pointer and was skipped"
            ));
            continue;
        }

        let chunk_offset = sector_index * MCA_SECTOR_SIZE;
        let chunk_limit = chunk_offset + sector_count * MCA_SECTOR_SIZE;
        if chunk_limit > bytes.len() || chunk_offset + 5 > bytes.len() {
            discarded_chunks += 1;
            warnings.push(format!(
                "{chunk_label} points outside the region file and was skipped"
            ));
            continue;
        }

        let chunk_len =
            u32::from_be_bytes(bytes[chunk_offset..chunk_offset + 4].try_into().unwrap()) as usize;
        let compression_type = bytes[chunk_offset + 4];

        let raw_nbt = match decode_chunk_payload(
            path,
            region_x,
            region_z,
            index,
            &bytes,
            chunk_offset,
            chunk_limit,
            chunk_len,
            compression_type,
        ) {
            Ok(raw_nbt) => raw_nbt,
            Err(error) => {
                discarded_chunks += 1;
                warnings.push(format!(
                    "{chunk_label} is corrupted and was skipped: {error:#}"
                ));
                continue;
            }
        };

        if raw_nbt.is_empty() {
            discarded_chunks += 1;
            warnings.push(format!(
                "{chunk_label} decompressed to an empty payload and was skipped"
            ));
            continue;
        }

        region.set_chunk(
            index,
            ChunkData {
                timestamp: i64::from(timestamp),
                raw_nbt,
            },
        )?;
    }

    Ok(ReadOutcome {
        region,
        warnings,
        discarded_chunks,
    })
}

pub fn region_uses_external_chunks(path: &Path) -> Result<bool> {
    let bytes =
        std::fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    ensure!(
        bytes.len() >= MCA_HEADER_SIZE,
        "mca region {} is smaller than the 8 KiB header",
        path.display()
    );

    for index in 0..REGION_CHUNK_COUNT {
        let location_offset = index * 4;
        let location = u32::from_be_bytes(
            bytes[location_offset..location_offset + 4]
                .try_into()
                .unwrap(),
        );

        let sector_index = ((location >> 8) & 0x00ff_ffff) as usize;
        let sector_count = (location & 0xff) as usize;
        if sector_index == 0 || sector_count == 0 {
            continue;
        }

        let chunk_offset = sector_index * MCA_SECTOR_SIZE;
        if chunk_offset + 5 > bytes.len() {
            continue;
        }

        if bytes[chunk_offset + 4] & MCA_EXTERNAL_FLAG != 0 {
            return Ok(true);
        }
    }

    Ok(false)
}

pub fn storage_size(path: &Path) -> Result<u64> {
    let bytes =
        std::fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    ensure!(
        bytes.len() >= MCA_HEADER_SIZE,
        "mca region {} is smaller than the 8 KiB header",
        path.display()
    );

    let mut total_size = bytes.len() as u64;
    let mut seen_sidecars = HashSet::new();
    let (region_x, region_z) = parse_region_coords_from_path(path)?;
    let parent = path
        .parent()
        .context("mca region file is missing a parent directory")?;

    for index in 0..REGION_CHUNK_COUNT {
        let location_offset = index * 4;
        let location = u32::from_be_bytes(
            bytes[location_offset..location_offset + 4]
                .try_into()
                .unwrap(),
        );

        let sector_index = ((location >> 8) & 0x00ff_ffff) as usize;
        let sector_count = (location & 0xff) as usize;
        if sector_index == 0 || sector_count == 0 {
            continue;
        }

        let chunk_offset = sector_index * MCA_SECTOR_SIZE;
        if chunk_offset + 5 > bytes.len() {
            continue;
        }

        if bytes[chunk_offset + 4] & MCA_EXTERNAL_FLAG == 0 {
            continue;
        }

        let local_x = index % REGION_SIDE;
        let local_z = index / REGION_SIDE;
        let chunk_x = region_x * REGION_SIDE as i32 + local_x as i32;
        let chunk_z = region_z * REGION_SIDE as i32 + local_z as i32;
        let sidecar_name = external_chunk_file_name(chunk_x, chunk_z);

        if !seen_sidecars.insert(sidecar_name.clone()) {
            continue;
        }

        let sidecar_path = parent.join(sidecar_name);
        if let Ok(metadata) = sidecar_path.metadata() {
            total_size += metadata.len();
        }
    }

    Ok(total_size)
}

pub fn encode_region(region: &Region, compression_level: i32) -> Result<EncodedRegion> {
    let mut location_table = vec![0_u8; 4096];
    let mut timestamp_table = vec![0_u8; 4096];
    let mut sector_data = Vec::new();
    let mut sidecar_files = Vec::new();
    let mut next_sector = 2_u32;

    let compression = Compression::new(compression_level as u32);

    for (index, chunk) in region.iter_chunks() {
        let compressed = zlib_compress(&chunk.raw_nbt, compression)
            .with_context(|| format!("failed to compress chunk slot {index}"))?;

        let (timestamp, _) = normalize_timestamp_to_u32(chunk.timestamp);
        timestamp_table[index * 4..index * 4 + 4].copy_from_slice(&timestamp.to_be_bytes());

        let local_x = index % REGION_SIDE;
        let local_z = index / REGION_SIDE;

        let mut record = Vec::new();
        let sector_count;
        let compression_type;

        if compressed.len() + 5 > MAX_INLINE_SECTORS * MCA_SECTOR_SIZE {
            compression_type = MCA_EXTERNAL_ZLIB;
            record.write_u32::<BigEndian>(1)?;
            record.write_u8(compression_type)?;
            pad_to_sector_boundary(&mut record);
            sector_count = 1_u8;

            sidecar_files.push(SidecarFile {
                file_name: external_chunk_file_name(
                    region.region_x * REGION_SIDE as i32 + local_x as i32,
                    region.region_z * REGION_SIDE as i32 + local_z as i32,
                ),
                bytes: compressed,
            });
        } else {
            compression_type = MCA_COMPRESSION_ZLIB;
            let inline_len =
                u32::try_from(compressed.len() + 1).context("inline chunk payload is too large")?;
            record.write_u32::<BigEndian>(inline_len)?;
            record.write_u8(compression_type)?;
            record.extend_from_slice(&compressed);
            pad_to_sector_boundary(&mut record);

            let sectors = record.len() / MCA_SECTOR_SIZE;
            let sectors =
                u8::try_from(sectors).context("inline mca chunk requires too many sectors")?;
            sector_count = sectors;
        }

        location_table[index * 4..index * 4 + 4]
            .copy_from_slice(&encode_location(next_sector, sector_count));
        next_sector += u32::from(sector_count);
        sector_data.extend_from_slice(&record);
    }

    let mut main_file_bytes = Vec::with_capacity(MCA_HEADER_SIZE + sector_data.len());
    main_file_bytes.extend_from_slice(&location_table);
    main_file_bytes.extend_from_slice(&timestamp_table);
    main_file_bytes.extend_from_slice(&sector_data);

    Ok(EncodedRegion {
        main_file_bytes,
        sidecar_files,
        warnings: Vec::new(),
    })
}

fn decode_chunk_payload(
    region_path: &Path,
    region_x: i32,
    region_z: i32,
    chunk_index: usize,
    region_bytes: &[u8],
    chunk_offset: usize,
    chunk_limit: usize,
    chunk_len: usize,
    compression_type: u8,
) -> Result<Vec<u8>> {
    let is_external = compression_type & MCA_EXTERNAL_FLAG != 0;
    let compression_id = compression_type & !MCA_EXTERNAL_FLAG;

    let compressed = if is_external {
        let chunk_x = region_x * REGION_SIDE as i32 + (chunk_index % REGION_SIDE) as i32;
        let chunk_z = region_z * REGION_SIDE as i32 + (chunk_index / REGION_SIDE) as i32;
        let parent = region_path
            .parent()
            .context("mca region file is missing a parent directory")?;
        let external_path = parent.join(external_chunk_file_name(chunk_x, chunk_z));
        std::fs::read(&external_path).with_context(|| {
            format!(
                "failed to read external chunk file {}",
                external_path.display()
            )
        })?
    } else {
        ensure!(chunk_len >= 1, "chunk length is too short");
        let payload_start = chunk_offset + 5;
        let payload_end = chunk_offset + 4 + chunk_len;
        ensure!(
            payload_end <= chunk_limit && payload_end <= region_bytes.len(),
            "chunk payload exceeds its declared sector allocation"
        );
        region_bytes[payload_start..payload_end].to_vec()
    };

    match compression_id {
        MCA_COMPRESSION_GZIP => gzip_decompress(&compressed),
        MCA_COMPRESSION_ZLIB => zlib_decompress(&compressed),
        value => bail!("unsupported mca compression type {value}"),
    }
}

fn encode_location(start_sector: u32, sector_count: u8) -> [u8; 4] {
    let location = ((start_sector & 0x00ff_ffff) << 8) | u32::from(sector_count);
    location.to_be_bytes()
}

fn external_chunk_file_name(chunk_x: i32, chunk_z: i32) -> String {
    format!("c.{chunk_x}.{chunk_z}.mcc")
}

fn pad_to_sector_boundary(bytes: &mut Vec<u8>) {
    let remainder = bytes.len() % MCA_SECTOR_SIZE;
    if remainder != 0 {
        bytes.resize(bytes.len() + (MCA_SECTOR_SIZE - remainder), 0);
    }
}

fn zlib_compress(bytes: &[u8], compression: Compression) -> Result<Vec<u8>> {
    let mut encoder = ZlibEncoder::new(Vec::new(), compression);
    encoder.write_all(bytes)?;
    encoder.finish().map_err(Into::into)
}

fn zlib_decompress(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = ZlibDecoder::new(bytes);
    let mut output = Vec::new();
    decoder.read_to_end(&mut output)?;
    Ok(output)
}

fn gzip_decompress(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = GzDecoder::new(bytes);
    let mut output = Vec::new();
    decoder.read_to_end(&mut output)?;
    Ok(output)
}
