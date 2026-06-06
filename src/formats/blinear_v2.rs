use std::io::{Cursor, Write};
use std::path::Path;
use std::time::Instant;

use anyhow::{Context, Result, ensure};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use crate::diagnostic::{Diagnostic, DiagnosticCode};
use crate::formats::{
    BLINEAR_HASH_SEED, BLINEAR_SUPERBLOCK, EncodeProfile, ReadOutcome, decode_chunk_section,
    encode_chunk_section, newest_timestamp_millis, parse_region_coords_from_path,
};
use crate::io_util::read_file_bytes;
use crate::model::{REGION_CHUNK_COUNT, Region};
use crate::writer::RegionWriteTarget;

const HEADER_SIZE: usize = 18;

pub fn read_region(path: &Path) -> Result<ReadOutcome> {
    let bytes = read_file_bytes(path)?;
    read_region_bytes(path, &bytes)
}

pub(crate) fn read_region_bytes(path: &Path, bytes: &[u8]) -> Result<ReadOutcome> {
    ensure!(
        bytes.len() >= HEADER_SIZE,
        "blinear_v2 region {} is too small",
        path.display()
    );

    let (region_x, region_z) = parse_region_coords_from_path(path)?;
    let mut cursor = Cursor::new(&bytes);

    let superblock = cursor.read_i64::<BigEndian>()?;
    ensure!(
        superblock == BLINEAR_SUPERBLOCK,
        "invalid blinear_v2 superblock"
    );

    let version = cursor.read_u8()?;
    ensure!(version == 0x02, "invalid blinear_v2 version {version}");

    let _master_timestamp = cursor.read_i64::<BigEndian>()?;
    let _compression_level = cursor.read_u8()?;

    let compressed = &bytes[HEADER_SIZE..];
    let decompressed = zstd::stream::decode_all(Cursor::new(compressed))
        .with_context(|| format!("failed to decompress {}", path.display()))?;

    let mut region = Region::new(region_x, region_z);
    let mut diagnostics = Vec::new();
    let mut discarded_chunks = 0;
    let mut offset = 0_usize;

    for index in 0..REGION_CHUNK_COUNT {
        ensure!(
            offset + 4 <= decompressed.len(),
            "blinear_v2 region {} ended before slot {index}",
            path.display()
        );

        let section_len = i32::from_be_bytes(decompressed[offset..offset + 4].try_into().unwrap());
        offset += 4;
        ensure!(
            section_len >= 0,
            "blinear_v2 region {} has a negative section length at slot {index}",
            path.display()
        );

        if section_len == 0 {
            continue;
        }

        let section_len = section_len as usize;
        if offset + section_len > decompressed.len() {
            diagnostics.push(
                Diagnostic::warning(
                    DiagnosticCode::SkippedData,
                    "chunk points past the decompressed buffer and was skipped",
                )
                .with_path(path)
                .with_region_coords(region_x, region_z)
                .with_chunk_index(index),
            );
            discarded_chunks += 1;
            break;
        }

        let section = &decompressed[offset..offset + section_len];
        offset += section_len;

        match decode_chunk_section(section, BLINEAR_HASH_SEED) {
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

    ensure!(
        offset == decompressed.len(),
        "blinear_v2 region {} has trailing bytes after slot parsing",
        path.display()
    );

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
    let main = target.main_file();
    let write_started_at = Instant::now();
    main.write_i64::<BigEndian>(BLINEAR_SUPERBLOCK)?;
    main.write_u8(0x02)?;
    main.write_i64::<BigEndian>(newest_timestamp_millis(region))?;
    main.write_u8(compression_level as u8)?;
    if let Some(profile) = profile.as_deref_mut() {
        profile.record_file_write(write_started_at.elapsed());
    }

    if profile.is_some() {
        let mut raw = Vec::new();

        for index in 0..REGION_CHUNK_COUNT {
            if let Some(chunk) = region.chunk(index) {
                let section = encode_chunk_section(chunk, BLINEAR_HASH_SEED)?;
                let section_len =
                    i32::try_from(section.len()).context("blinear_v2 section is too large")?;
                raw.write_i32::<BigEndian>(section_len)?;
                raw.extend_from_slice(&section);
            } else {
                raw.write_i32::<BigEndian>(0)?;
            }
        }

        let compress_started_at = Instant::now();
        let compressed = zstd::bulk::compress(&raw, compression_level)
            .context("failed to zstd-compress the blinear_v2 payload")?;
        if let Some(profile) = profile.as_deref_mut() {
            profile.record_compress(compress_started_at.elapsed());
            profile.record_unit(raw.len(), compressed.len());
        }

        let write_started_at = Instant::now();
        main.write_all(&compressed)?;
        if let Some(profile) = profile {
            profile.record_file_write(write_started_at.elapsed());
        }

        return Ok(Vec::new());
    }

    {
        let mut encoder = zstd::stream::write::Encoder::new(main, compression_level)
            .context("failed to create the blinear_v2 zstd encoder")?;

        for index in 0..REGION_CHUNK_COUNT {
            if let Some(chunk) = region.chunk(index) {
                let section = encode_chunk_section(chunk, BLINEAR_HASH_SEED)?;
                let section_len =
                    i32::try_from(section.len()).context("blinear_v2 section is too large")?;
                encoder.write_i32::<BigEndian>(section_len)?;
                encoder.write_all(&section)?;
            } else {
                encoder.write_i32::<BigEndian>(0)?;
            }
        }

        encoder
            .finish()
            .context("failed to finish the blinear_v2 zstd stream")?;
    }

    Ok(Vec::new())
}
