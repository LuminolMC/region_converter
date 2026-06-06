use std::io::{Seek, SeekFrom, Write};
use std::path::Path;
use std::time::Instant;

use anyhow::{Context, Result, ensure};
use byteorder::{BigEndian, WriteBytesExt};

use crate::diagnostic::{Diagnostic, DiagnosticCode};
use crate::formats::{
    BLINEAR_HASH_SEED, BLINEAR_SUPERBLOCK, EncodeProfile, ReadOutcome, decode_chunk_section,
    encode_chunk_section, parse_region_coords_from_path,
};
use crate::io_util::read_file_bytes;
use crate::model::{REGION_CHUNK_COUNT, Region};
use crate::writer::RegionWriteTarget;

const HEADER_SIZE: usize = 14;
const BUCKET_SHIFT: usize = 6;
const BUCKET_SIZE: usize = 1 << BUCKET_SHIFT;
const BUCKET_COUNT: usize = REGION_CHUNK_COUNT / BUCKET_SIZE;
const POSITION_TABLE_SIZE: usize = BUCKET_COUNT * 8;
const DATA_START_OFFSET: usize = HEADER_SIZE + POSITION_TABLE_SIZE;

pub fn read_region(path: &Path) -> Result<ReadOutcome> {
    let bytes = read_file_bytes(path)?;
    read_region_bytes(path, &bytes)
}

pub(crate) fn read_region_bytes(path: &Path, bytes: &[u8]) -> Result<ReadOutcome> {
    ensure!(
        bytes.len() >= DATA_START_OFFSET,
        "blinear_v3 region {} is too small",
        path.display()
    );

    let (region_x, region_z) = parse_region_coords_from_path(path)?;
    let superblock = i64::from_be_bytes(bytes[0..8].try_into().unwrap());
    ensure!(
        superblock == BLINEAR_SUPERBLOCK,
        "invalid blinear_v3 superblock"
    );

    let version = bytes[8];
    ensure!(version == 0x03, "invalid blinear_v3 version {version}");

    let _compression_level = bytes[9];
    let hash_seed = u32::from_be_bytes(bytes[10..14].try_into().unwrap());

    let mut offsets = [0_u64; BUCKET_COUNT];
    let mut cursor = HEADER_SIZE;
    for offset in &mut offsets {
        *offset = u64::from_be_bytes(bytes[cursor..cursor + 8].try_into().unwrap());
        cursor += 8;
    }

    let mut region = Region::new(region_x, region_z);
    let mut diagnostics = Vec::new();
    let mut discarded_chunks = 0;

    for (bucket_index, bucket_offset) in offsets.into_iter().enumerate() {
        if bucket_offset == 0 {
            continue;
        }

        let Ok(bucket_offset) = usize::try_from(bucket_offset) else {
            diagnostics.push(
                Diagnostic::warning(
                    DiagnosticCode::SkippedData,
                    format!(
                        "bucket {bucket_index} offset does not fit this platform and was skipped"
                    ),
                )
                .with_path(path)
                .with_region_coords(region_x, region_z),
            );
            continue;
        };
        let Some(bucket_header_end) = bucket_offset.checked_add(8) else {
            diagnostics.push(
                Diagnostic::warning(
                    DiagnosticCode::SkippedData,
                    format!("bucket {bucket_index} offset overflows the file address space and was skipped"),
                )
                .with_path(path)
                .with_region_coords(region_x, region_z),
            );
            continue;
        };
        if bucket_offset < DATA_START_OFFSET || bucket_header_end > bytes.len() {
            diagnostics.push(
                Diagnostic::warning(
                    DiagnosticCode::SkippedData,
                    format!("bucket {bucket_index} points outside the file and was skipped"),
                )
                .with_path(path)
                .with_region_coords(region_x, region_z),
            );
            continue;
        }

        let original_len =
            i32::from_be_bytes(bytes[bucket_offset..bucket_offset + 4].try_into().unwrap());
        let compressed_len = i32::from_be_bytes(
            bytes[bucket_offset + 4..bucket_offset + 8]
                .try_into()
                .unwrap(),
        );

        if original_len <= 0 || compressed_len <= 0 {
            diagnostics.push(
                Diagnostic::warning(
                    DiagnosticCode::InvalidMetadata,
                    format!("bucket {bucket_index} has an invalid length header and was skipped"),
                )
                .with_path(path)
                .with_region_coords(region_x, region_z),
            );
            continue;
        }

        let original_len = original_len as usize;
        let compressed_len = compressed_len as usize;
        let compressed_start = bucket_header_end;
        let Some(compressed_end) = compressed_start.checked_add(compressed_len) else {
            diagnostics.push(
                Diagnostic::warning(
                    DiagnosticCode::SkippedData,
                    format!("bucket {bucket_index} compressed length overflows the file address space and was skipped"),
                )
                .with_path(path)
                .with_region_coords(region_x, region_z),
            );
            continue;
        };

        if compressed_end > bytes.len() {
            diagnostics.push(
                Diagnostic::warning(
                    DiagnosticCode::SkippedData,
                    format!("bucket {bucket_index} overruns the file and was skipped"),
                )
                .with_path(path)
                .with_region_coords(region_x, region_z),
            );
            continue;
        }

        let decompressed = match zstd::bulk::decompress(
            &bytes[compressed_start..compressed_end],
            original_len,
        ) {
            Ok(data) => data,
            Err(error) => {
                diagnostics.push(
                    Diagnostic::warning(
                        DiagnosticCode::CorruptBucket,
                        format!(
                            "bucket {bucket_index} failed zstd decompression and was skipped: {error}"
                        ),
                    )
                    .with_path(path)
                    .with_region_coords(region_x, region_z),
                );
                continue;
            }
        };

        let first_chunk = bucket_index * BUCKET_SIZE;
        let mut local_offset = 0_usize;
        let mut bucket_valid = true;

        for index in first_chunk..first_chunk + BUCKET_SIZE {
            if local_offset + 4 > decompressed.len() {
                diagnostics.push(
                    Diagnostic::warning(
                        DiagnosticCode::CorruptBucket,
                        format!("bucket {bucket_index} ended early while reading slot {index}"),
                    )
                    .with_path(path)
                    .with_region_coords(region_x, region_z)
                    .with_chunk_index(index),
                );
                bucket_valid = false;
                break;
            }

            let section_len = i32::from_be_bytes(
                decompressed[local_offset..local_offset + 4]
                    .try_into()
                    .unwrap(),
            );
            local_offset += 4;

            if section_len < 0 {
                diagnostics.push(
                    Diagnostic::warning(
                        DiagnosticCode::InvalidMetadata,
                        format!(
                            "bucket {bucket_index} contains a negative section length at slot {index}"
                        ),
                    )
                    .with_path(path)
                    .with_region_coords(region_x, region_z)
                    .with_chunk_index(index),
                );
                bucket_valid = false;
                break;
            }

            if section_len == 0 {
                continue;
            }

            let section_len = section_len as usize;
            if local_offset + section_len > decompressed.len() {
                diagnostics.push(
                    Diagnostic::warning(
                        DiagnosticCode::SkippedData,
                        format!(
                            "bucket {bucket_index} overruns its decompressed buffer at slot {index}"
                        ),
                    )
                    .with_path(path)
                    .with_region_coords(region_x, region_z)
                    .with_chunk_index(index),
                );
                bucket_valid = false;
                break;
            }

            let section = &decompressed[local_offset..local_offset + section_len];
            local_offset += section_len;

            match decode_chunk_section(section, hash_seed) {
                Ok(chunk) => region.set_chunk(index, chunk)?,
                Err(error) => {
                    diagnostics.push(
                        Diagnostic::warning(
                            DiagnosticCode::CorruptChunk,
                            format!("chunk is corrupted and was skipped: {error:#}"),
                        )
                        .with_path(path)
                        .with_region_coords(region_x, region_z)
                        .with_chunk_index(index),
                    );
                    discarded_chunks += 1;
                }
            }
        }

        if bucket_valid && local_offset != decompressed.len() {
            diagnostics.push(
                Diagnostic::warning(
                    DiagnosticCode::SkippedData,
                    format!("bucket {bucket_index} has trailing bytes after its 64 chunk slots"),
                )
                .with_path(path)
                .with_region_coords(region_x, region_z),
            );
        }
    }

    Ok(ReadOutcome {
        region,
        diagnostics,
        discarded_chunks,
    })
}

pub fn encode_region_to_writer(
    region: &Region,
    compression_level: i32,
    target: &mut dyn RegionWriteTarget,
) -> Result<Vec<Diagnostic>> {
    encode_region_to_writer_profiled(region, compression_level, target, None)
}

pub fn encode_region_to_writer_profiled(
    region: &Region,
    compression_level: i32,
    target: &mut dyn RegionWriteTarget,
    mut profile: Option<&mut EncodeProfile>,
) -> Result<Vec<Diagnostic>> {
    let mut offsets = [0_u64; BUCKET_COUNT];
    let main = target.main_file();

    let write_started_at = Instant::now();
    main.write_i64::<BigEndian>(BLINEAR_SUPERBLOCK)?;
    main.write_u8(0x03)?;
    main.write_u8(compression_level as u8)?;
    main.write_u32::<BigEndian>(BLINEAR_HASH_SEED)?;
    let offset_table_start = main.stream_position()?;
    main.write_all(&[0_u8; POSITION_TABLE_SIZE])?;
    if let Some(profile) = profile.as_deref_mut() {
        profile.record_file_write(write_started_at.elapsed());
    }

    for (bucket_index, offset_entry) in offsets.iter_mut().enumerate() {
        let first_chunk = bucket_index * BUCKET_SIZE;
        let mut raw_bucket = Vec::new();
        let mut has_any_chunk = false;

        for index in first_chunk..first_chunk + BUCKET_SIZE {
            if let Some(chunk) = region.chunk(index) {
                let section = encode_chunk_section(chunk, BLINEAR_HASH_SEED)?;
                let section_len =
                    i32::try_from(section.len()).context("blinear_v3 section is too large")?;
                raw_bucket.write_i32::<BigEndian>(section_len)?;
                raw_bucket.extend_from_slice(&section);
                has_any_chunk = true;
            } else {
                raw_bucket.write_i32::<BigEndian>(0)?;
            }
        }

        if !has_any_chunk {
            continue;
        }

        let compress_started_at = Instant::now();
        let compressed = zstd::bulk::compress(&raw_bucket, compression_level)
            .context("failed to zstd-compress a blinear_v3 bucket")?;
        if let Some(profile) = profile.as_deref_mut() {
            profile.record_compress(compress_started_at.elapsed());
            profile.record_unit(raw_bucket.len(), compressed.len());
        }
        let original_len =
            i32::try_from(raw_bucket.len()).context("blinear_v3 bucket is too large")?;
        let compressed_len =
            i32::try_from(compressed.len()).context("compressed blinear_v3 bucket is too large")?;

        let write_started_at = Instant::now();
        *offset_entry = main.stream_position()?;
        main.write_i32::<BigEndian>(original_len)?;
        main.write_i32::<BigEndian>(compressed_len)?;
        main.write_all(&compressed)?;
        if let Some(profile) = profile.as_deref_mut() {
            profile.record_file_write(write_started_at.elapsed());
        }
    }

    let write_started_at = Instant::now();
    let end = main.stream_position()?;
    main.seek(SeekFrom::Start(offset_table_start))?;
    for offset in offsets {
        main.write_u64::<BigEndian>(offset)?;
    }
    main.seek(SeekFrom::Start(end))?;
    if let Some(profile) = profile {
        profile.record_file_write(write_started_at.elapsed());
    }

    Ok(Vec::new())
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    #[test]
    fn oversized_bucket_offsets_are_reported_without_panicking() -> Result<()> {
        let mut bytes = Vec::new();
        bytes.write_i64::<BigEndian>(BLINEAR_SUPERBLOCK)?;
        bytes.write_u8(0x03)?;
        bytes.write_u8(6)?;
        bytes.write_u32::<BigEndian>(BLINEAR_HASH_SEED)?;
        bytes.write_u64::<BigEndian>(u64::MAX)?;
        for _ in 1..BUCKET_COUNT {
            bytes.write_u64::<BigEndian>(0)?;
        }

        let outcome = read_region_bytes(Path::new("r.0.0.b_linear"), &bytes)?;

        assert_eq!(outcome.region.chunk_count(), 0);
        assert_eq!(outcome.diagnostics.len(), 1);
        Ok(())
    }
}
