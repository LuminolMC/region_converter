use std::io::{Cursor, Read};
use std::path::Path;

use anyhow::{Context, Result, bail, ensure};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use crate::diagnostic::{Diagnostic, DiagnosticCode};
use crate::formats::{
    EncodedRegion, LINEAR_SUPERBLOCK, ReadOutcome, RegionStorageFormat,
    parse_region_coords_from_path, xxhash64,
};
use crate::io_util::read_file_bytes;
use crate::model::{ChunkData, REGION_CHUNK_COUNT, Region};

const CLASSIC_FILE_HEADER_SIZE: usize = 32;
const CLASSIC_FILE_FOOTER_SIZE: usize = 8;
const CLASSIC_INNER_HEADER_SIZE: usize = REGION_CHUNK_COUNT * 8;

const MODERN_GRID_SIZE: usize = 8;
const MODERN_BUCKET_SIDE: usize = 32 / MODERN_GRID_SIZE;
const MODERN_BUCKET_COUNT: usize = MODERN_GRID_SIZE * MODERN_GRID_SIZE;
const MODERN_EXISTENCE_BITMAP_SIZE: usize = REGION_CHUNK_COUNT / 8;
const MODERN_BUCKET_METADATA_SIZE: usize = 13;
const MODERN_BUCKET_HASH_SEED: u64 = 0;
const MODERN_VERSION: u8 = 3;
const MODERN_FILE_FOOTER_SIZE: usize = 8;

pub(crate) fn detect_storage_format(path: &Path) -> Result<RegionStorageFormat> {
    let mut header = [0_u8; 9];
    let mut file =
        std::fs::File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    file.read_exact(&mut header)
        .with_context(|| format!("failed to read linear header from {}", path.display()))?;
    detect_storage_format_from_bytes(path, &header)
}

pub(crate) fn detect_storage_format_from_bytes(
    path: &Path,
    bytes: &[u8],
) -> Result<RegionStorageFormat> {
    ensure!(
        bytes.len() >= 9,
        "linear region {} is too small",
        path.display()
    );

    let superblock = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
    ensure!(
        superblock == LINEAR_SUPERBLOCK,
        "invalid linear superblock in {}",
        path.display()
    );

    match bytes[8] {
        1 => Ok(RegionStorageFormat::LinearV1),
        2 => Ok(RegionStorageFormat::LinearV2),
        3 => Ok(RegionStorageFormat::LinearV3),
        version => bail!("unsupported linear version {version} in {}", path.display()),
    }
}

pub fn read_region(path: &Path) -> Result<ReadOutcome> {
    let bytes = read_file_bytes(path)?;
    let storage_format = detect_storage_format_from_bytes(path, &bytes)?;
    read_region_from_bytes(path, &bytes, storage_format)
}

pub(crate) fn read_region_storage(
    path: &Path,
    storage_format: RegionStorageFormat,
) -> Result<ReadOutcome> {
    let bytes = read_file_bytes(path)?;
    read_region_from_bytes(path, &bytes, storage_format)
}

pub fn encode_region(region: &Region, compression_level: i32) -> Result<EncodedRegion> {
    encode_modern_region(region, compression_level)
}

fn read_region_from_bytes(
    path: &Path,
    bytes: &[u8],
    storage_format: RegionStorageFormat,
) -> Result<ReadOutcome> {
    let (region_x, region_z) = parse_region_coords_from_path(path)?;

    match storage_format {
        RegionStorageFormat::LinearV1 | RegionStorageFormat::LinearV2 => {
            read_classic_region(bytes, path, region_x, region_z)
        }
        RegionStorageFormat::LinearV3 => read_modern_region(bytes, path, region_x, region_z),
        _ => bail!("non-linear storage format routed to linear reader"),
    }
}

fn read_classic_region(
    bytes: &[u8],
    path: &Path,
    region_x: i32,
    region_z: i32,
) -> Result<ReadOutcome> {
    ensure!(
        bytes.len() >= CLASSIC_FILE_HEADER_SIZE + CLASSIC_FILE_FOOTER_SIZE,
        "linear region {} is too small",
        path.display()
    );

    let mut cursor = Cursor::new(bytes);
    let signature = cursor.read_u64::<BigEndian>()?;
    ensure!(signature == LINEAR_SUPERBLOCK, "invalid linear superblock");

    let version = cursor.read_u8()?;
    ensure!(
        version == 1 || version == 2,
        "unsupported classic linear version {version}"
    );

    let _newest_timestamp = cursor.read_u64::<BigEndian>()?;
    let _compression_level = cursor.read_i8()?;
    let chunk_count = cursor.read_u16::<BigEndian>()? as usize;
    let compressed_len = cursor.read_u32::<BigEndian>()? as usize;
    let _reserved = cursor.read_u64::<BigEndian>()?;

    ensure!(
        CLASSIC_FILE_HEADER_SIZE + compressed_len + CLASSIC_FILE_FOOTER_SIZE == bytes.len(),
        "linear region compressed length does not match the file size"
    );

    let footer_offset = CLASSIC_FILE_HEADER_SIZE + compressed_len;
    let footer_signature =
        u64::from_be_bytes(bytes[footer_offset..footer_offset + 8].try_into().unwrap());
    ensure!(
        footer_signature == LINEAR_SUPERBLOCK,
        "invalid linear footer signature"
    );

    let compressed = &bytes[CLASSIC_FILE_HEADER_SIZE..footer_offset];
    let decompressed = zstd::stream::decode_all(Cursor::new(compressed))
        .with_context(|| format!("failed to decompress {}", path.display()))?;

    ensure!(
        decompressed.len() >= CLASSIC_INNER_HEADER_SIZE,
        "linear region payload is smaller than the 1024-entry chunk table"
    );

    let mut region = Region::new(region_x, region_z);
    let mut cursor = Cursor::new(&decompressed);
    let mut sizes = Vec::with_capacity(REGION_CHUNK_COUNT);
    let mut timestamps = Vec::with_capacity(REGION_CHUNK_COUNT);
    let mut real_chunk_count = 0_usize;
    let mut total_payload_len = 0_usize;

    for _ in 0..REGION_CHUNK_COUNT {
        let size = cursor.read_u32::<BigEndian>()? as usize;
        let timestamp = cursor.read_u32::<BigEndian>()?;
        if size > 0 {
            real_chunk_count += 1;
        }
        total_payload_len = total_payload_len
            .checked_add(size)
            .context("linear chunk payload lengths overflowed the region size")?;
        sizes.push(size);
        timestamps.push(timestamp);
    }

    ensure!(
        real_chunk_count == chunk_count,
        "linear region chunk count mismatch: header says {chunk_count}, table says {real_chunk_count}"
    );
    ensure!(
        CLASSIC_INNER_HEADER_SIZE + total_payload_len == decompressed.len(),
        "linear region payload lengths do not match the decompressed size"
    );

    let mut payload_offset = CLASSIC_INNER_HEADER_SIZE;
    for (index, size) in sizes.into_iter().enumerate() {
        if size == 0 {
            continue;
        }

        let payload_end = payload_offset + size;
        ensure!(
            payload_end <= decompressed.len(),
            "linear chunk payload overruns the region buffer"
        );
        region.set_chunk(
            index,
            ChunkData {
                timestamp: i64::from(timestamps[index]),
                raw_nbt: decompressed[payload_offset..payload_end].to_vec(),
            },
        )?;
        payload_offset = payload_end;
    }

    Ok(ReadOutcome {
        region,
        diagnostics: Vec::new(),
        discarded_chunks: 0,
    })
}

fn read_modern_region(
    bytes: &[u8],
    path: &Path,
    region_x: i32,
    region_z: i32,
) -> Result<ReadOutcome> {
    let mut cursor = Cursor::new(bytes);
    let signature = cursor.read_u64::<BigEndian>()?;
    ensure!(signature == LINEAR_SUPERBLOCK, "invalid linear superblock");

    let version = cursor.read_u8()?;
    ensure!(
        version == MODERN_VERSION,
        "invalid linear_v3 version {version}"
    );

    let _newest_timestamp = cursor.read_i64::<BigEndian>()?;
    let grid_size = cursor.read_u8()? as usize;
    ensure!(
        matches!(grid_size, 1 | 2 | 4 | 8 | 16 | 32),
        "invalid linear_v3 grid size {grid_size} in {}",
        path.display()
    );
    let bucket_side = 32 / grid_size;
    let bucket_count = grid_size * grid_size;

    let header_region_x = cursor.read_i32::<BigEndian>()?;
    let header_region_z = cursor.read_i32::<BigEndian>()?;

    let mut existence_bitmap = [0_u8; MODERN_EXISTENCE_BITMAP_SIZE];
    cursor.read_exact(&mut existence_bitmap)?;

    let mut diagnostics = Vec::new();
    if header_region_x != region_x || header_region_z != region_z {
        diagnostics.push(
            Diagnostic::warning(
                DiagnosticCode::FormatMismatch,
                format!(
                    "linear_v3 header coordinates ({header_region_x}, {header_region_z}) do not match file name coordinates ({region_x}, {region_z})"
                ),
            )
            .with_path(path)
            .with_region_coords(region_x, region_z),
        );
    }

    loop {
        let feature_name_len = cursor.read_u8()? as usize;
        if feature_name_len == 0 {
            break;
        }

        let mut feature_name = vec![0_u8; feature_name_len];
        cursor.read_exact(&mut feature_name)?;
        let _feature_value = cursor.read_i32::<BigEndian>()?;
    }

    let mut bucket_sizes = Vec::with_capacity(bucket_count);
    let mut bucket_hashes = Vec::with_capacity(bucket_count);
    for _ in 0..bucket_count {
        bucket_sizes.push(cursor.read_i32::<BigEndian>()?);
        let _compression_level = cursor.read_u8()?;
        bucket_hashes.push(cursor.read_u64::<BigEndian>()?);
    }

    let footer_offset = bytes
        .len()
        .checked_sub(MODERN_FILE_FOOTER_SIZE)
        .context("linear_v3 file is shorter than the footer")?;
    ensure!(
        cursor.position() as usize <= footer_offset,
        "linear_v3 metadata overruns the footer area in {}",
        path.display()
    );

    let mut region = Region::new(region_x, region_z);

    for bx in 0..grid_size {
        for bz in 0..grid_size {
            let bucket_index = bx * grid_size + bz;
            let compressed_len = bucket_sizes[bucket_index];
            if compressed_len <= 0 {
                continue;
            }

            let compressed_len = compressed_len as usize;
            let bucket_start = cursor.position() as usize;
            let bucket_end = bucket_start + compressed_len;
            ensure!(
                bucket_end <= footer_offset,
                "linear_v3 bucket {bucket_index} overruns the footer in {}",
                path.display()
            );

            let compressed = &bytes[bucket_start..bucket_end];
            cursor.set_position(bucket_end as u64);

            let actual_hash = xxhash64(MODERN_BUCKET_HASH_SEED, compressed);
            ensure!(
                actual_hash == bucket_hashes[bucket_index],
                "linear_v3 bucket {bucket_index} hash mismatch in {}",
                path.display()
            );

            let decompressed =
                zstd::stream::decode_all(Cursor::new(compressed)).with_context(|| {
                    format!(
                        "failed to decompress linear_v3 bucket {bucket_index} in {}",
                        path.display()
                    )
                })?;

            let mut local = Cursor::new(&decompressed);
            let mut bucket_valid = true;

            for local_x in 0..bucket_side {
                for local_z in 0..bucket_side {
                    let chunk_index =
                        (bx * bucket_side + local_x) + (bz * bucket_side + local_z) * 32;

                    if local.position() as usize + 12 > decompressed.len() {
                        diagnostics.push(
                            Diagnostic::warning(
                                DiagnosticCode::CorruptBucket,
                                format!(
                                    "linear_v3 bucket {bucket_index} ended early while reading slot {chunk_index}"
                                ),
                            )
                            .with_path(path)
                            .with_region_coords(region_x, region_z)
                            .with_chunk_index(chunk_index),
                        );
                        bucket_valid = false;
                        break;
                    }

                    let chunk_size = local.read_i32::<BigEndian>()?;
                    let timestamp = local.read_i64::<BigEndian>()?;

                    if chunk_size < 0 {
                        diagnostics.push(
                            Diagnostic::warning(
                                DiagnosticCode::InvalidMetadata,
                                format!(
                                    "linear_v3 bucket {bucket_index} contains a negative chunk size at slot {chunk_index}"
                                ),
                            )
                            .with_path(path)
                            .with_region_coords(region_x, region_z)
                            .with_chunk_index(chunk_index),
                        );
                        bucket_valid = false;
                        break;
                    }

                    if chunk_size == 0 {
                        continue;
                    }

                    let declared_len = chunk_size as usize;
                    ensure!(
                        declared_len >= 8,
                        "linear_v3 bucket {bucket_index} in {} declares a chunk shorter than its timestamp header",
                        path.display()
                    );
                    let raw_len = declared_len - 8;
                    let payload_start = local.position() as usize;
                    let payload_end = payload_start + raw_len;

                    if payload_end > decompressed.len() {
                        diagnostics.push(
                            Diagnostic::warning(
                                DiagnosticCode::SkippedData,
                                format!(
                                    "linear_v3 bucket {bucket_index} overruns its buffer at slot {chunk_index}"
                                ),
                            )
                            .with_path(path)
                            .with_region_coords(region_x, region_z)
                            .with_chunk_index(chunk_index),
                        );
                        bucket_valid = false;
                        break;
                    }

                    region.set_chunk(
                        chunk_index,
                        ChunkData {
                            timestamp,
                            raw_nbt: decompressed[payload_start..payload_end].to_vec(),
                        },
                    )?;
                    local.set_position(payload_end as u64);
                }

                if !bucket_valid {
                    break;
                }
            }

            if bucket_valid && local.position() as usize != decompressed.len() {
                diagnostics.push(
                    Diagnostic::warning(
                        DiagnosticCode::SkippedData,
                        format!(
                            "linear_v3 bucket {bucket_index} has trailing bytes after its chunk slots"
                        ),
                    )
                    .with_path(path)
                    .with_region_coords(region_x, region_z),
                );
            }
        }
    }

    let footer = u64::from_be_bytes(bytes[footer_offset..].try_into().unwrap());
    ensure!(
        footer == LINEAR_SUPERBLOCK,
        "invalid linear_v3 footer signature in {}",
        path.display()
    );

    if cursor.position() as usize != footer_offset {
        diagnostics.push(
            Diagnostic::warning(
                DiagnosticCode::SkippedData,
                format!(
                    "linear_v3 has {} bytes between the bucket stream and footer",
                    footer_offset.saturating_sub(cursor.position() as usize)
                ),
            )
            .with_path(path)
            .with_region_coords(region_x, region_z),
        );
    }

    for index in 0..REGION_CHUNK_COUNT {
        let bitmap_present = bitmap_contains(&existence_bitmap, index);
        let actual_present = region.chunk(index).is_some();
        if bitmap_present && !actual_present {
            diagnostics.push(
                Diagnostic::warning(
                    DiagnosticCode::FormatMismatch,
                    format!(
                        "linear_v3 header bitmap marks chunk slot {index} as present, but no payload was decoded"
                    ),
                )
                .with_path(path)
                .with_region_coords(region_x, region_z)
                .with_chunk_index(index),
            );
        }
    }

    Ok(ReadOutcome {
        region,
        diagnostics,
        discarded_chunks: 0,
    })
}

fn encode_modern_region(region: &Region, compression_level: i32) -> Result<EncodedRegion> {
    let mut bucket_metadata = Vec::with_capacity(MODERN_BUCKET_COUNT * MODERN_BUCKET_METADATA_SIZE);
    let mut bucket_bytes = Vec::new();

    for bx in 0..MODERN_GRID_SIZE {
        for bz in 0..MODERN_GRID_SIZE {
            let mut raw_bucket = Vec::new();
            let mut has_data = false;

            for local_x in 0..MODERN_BUCKET_SIDE {
                for local_z in 0..MODERN_BUCKET_SIDE {
                    let chunk_index = (bx * MODERN_BUCKET_SIDE + local_x)
                        + (bz * MODERN_BUCKET_SIDE + local_z) * 32;

                    if let Some(chunk) = region.chunk(chunk_index) {
                        let chunk_len = i32::try_from(chunk.raw_nbt.len())
                            .context("linear_v3 chunk payload is too large")?;
                        raw_bucket.write_i32::<BigEndian>(chunk_len + 8)?;
                        raw_bucket.write_i64::<BigEndian>(chunk.timestamp)?;
                        raw_bucket.extend_from_slice(&chunk.raw_nbt);
                        has_data = true;
                    } else {
                        raw_bucket.write_i32::<BigEndian>(0)?;
                        raw_bucket.write_i64::<BigEndian>(0)?;
                    }
                }
            }

            if has_data {
                let compressed = zstd::bulk::compress(&raw_bucket, compression_level)
                    .context("failed to zstd-compress a linear_v3 bucket")?;
                let compressed_len =
                    i32::try_from(compressed.len()).context("linear_v3 bucket is too large")?;
                let bucket_hash = xxhash64(MODERN_BUCKET_HASH_SEED, &compressed);

                bucket_metadata.write_i32::<BigEndian>(compressed_len)?;
                bucket_metadata.write_u8(compression_level as u8)?;
                bucket_metadata.write_u64::<BigEndian>(bucket_hash)?;
                bucket_bytes.extend_from_slice(&compressed);
            } else {
                bucket_metadata.write_i32::<BigEndian>(0)?;
                bucket_metadata.write_u8(compression_level as u8)?;
                bucket_metadata.write_u64::<BigEndian>(0)?;
            }
        }
    }

    let mut main_file_bytes = Vec::new();
    main_file_bytes.write_u64::<BigEndian>(LINEAR_SUPERBLOCK)?;
    main_file_bytes.write_u8(MODERN_VERSION)?;
    main_file_bytes.write_i64::<BigEndian>(region.newest_timestamp().max(0))?;
    main_file_bytes.write_u8(MODERN_GRID_SIZE as u8)?;
    main_file_bytes.write_i32::<BigEndian>(region.region_x)?;
    main_file_bytes.write_i32::<BigEndian>(region.region_z)?;
    main_file_bytes.extend_from_slice(&serialize_existence_bitmap(region));
    main_file_bytes.write_u8(0)?;
    main_file_bytes.extend_from_slice(&bucket_metadata);
    main_file_bytes.extend_from_slice(&bucket_bytes);
    main_file_bytes.write_u64::<BigEndian>(LINEAR_SUPERBLOCK)?;

    Ok(EncodedRegion {
        main_file_bytes,
        sidecar_files: Vec::new(),
        diagnostics: Vec::new(),
    })
}

fn serialize_existence_bitmap(region: &Region) -> [u8; MODERN_EXISTENCE_BITMAP_SIZE] {
    let mut bitmap = [0_u8; MODERN_EXISTENCE_BITMAP_SIZE];

    for index in 0..REGION_CHUNK_COUNT {
        if region.chunk(index).is_some() {
            let byte_index = index / 8;
            let bit_index = 7 - (index % 8);
            bitmap[byte_index] |= 1 << bit_index;
        }
    }

    bitmap
}

fn bitmap_contains(bitmap: &[u8; MODERN_EXISTENCE_BITMAP_SIZE], index: usize) -> bool {
    let byte_index = index / 8;
    let bit_index = 7 - (index % 8);
    (bitmap[byte_index] >> bit_index) & 1 == 1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_linear_versions() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path().join("r.0.0.linear");

        let mut bytes = Vec::new();
        bytes.write_u64::<BigEndian>(LINEAR_SUPERBLOCK)?;
        bytes.write_u8(3)?;
        std::fs::write(&path, bytes)?;

        assert_eq!(detect_storage_format(&path)?, RegionStorageFormat::LinearV3);
        Ok(())
    }

    #[test]
    fn linear_v3_roundtrip_preserves_region_data() -> Result<()> {
        let mut region = Region::new(3, -4);
        region.set_chunk(
            0,
            ChunkData {
                timestamp: 123,
                raw_nbt: vec![1, 2, 3, 4],
            },
        )?;
        region.set_chunk(
            511,
            ChunkData {
                timestamp: 456,
                raw_nbt: vec![5, 6, 7],
            },
        )?;

        let encoded = encode_region(&region, 6)?;
        let temp_dir = tempfile::tempdir()?;
        let output_file = temp_dir.path().join("r.3.-4.linear");
        std::fs::write(&output_file, &encoded.main_file_bytes)?;

        let reparsed = read_region(&output_file)?;
        assert_eq!(reparsed.region, region);
        assert!(reparsed.diagnostics.is_empty());
        Ok(())
    }
}
