use std::io::{Cursor, Read};
use std::path::Path;

use anyhow::{Context, Result, ensure};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use crate::formats::{
    BLINEAR_HASH_SEED, BLINEAR_SUPERBLOCK, EncodedRegion, ReadOutcome, decode_chunk_section,
    encode_chunk_section, parse_region_coords_from_path,
};
use crate::model::{REGION_CHUNK_COUNT, Region};

const HEADER_SIZE: usize = 18;

pub fn read_region(path: &Path) -> Result<ReadOutcome> {
    let bytes =
        std::fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
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

    let mut compressed = Vec::new();
    cursor.read_to_end(&mut compressed)?;
    let decompressed = zstd::stream::decode_all(Cursor::new(compressed))
        .with_context(|| format!("failed to decompress {}", path.display()))?;

    let mut region = Region::new(region_x, region_z);
    let mut warnings = Vec::new();
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
            warnings.push(format!(
                "chunk slot {index} in {} points past the decompressed buffer and was skipped",
                path.display()
            ));
            discarded_chunks += 1;
            break;
        }

        let section = &decompressed[offset..offset + section_len];
        offset += section_len;

        match decode_chunk_section(section, BLINEAR_HASH_SEED) {
            Ok(chunk) => region.set_chunk(index, chunk)?,
            Err(error) => {
                warnings.push(format!(
                    "chunk slot {index} in {} is corrupted and was skipped: {error:#}",
                    path.display()
                ));
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
        warnings,
        discarded_chunks,
    })
}

pub fn encode_region(region: &Region, compression_level: i32) -> Result<EncodedRegion> {
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

    let compressed = zstd::bulk::compress(&raw, compression_level)
        .context("failed to zstd-compress the blinear_v2 payload")?;

    let mut main_file_bytes = Vec::with_capacity(HEADER_SIZE + compressed.len());
    main_file_bytes.write_i64::<BigEndian>(BLINEAR_SUPERBLOCK)?;
    main_file_bytes.write_u8(0x02)?;
    main_file_bytes.write_i64::<BigEndian>(region.newest_timestamp())?;
    main_file_bytes.write_u8(compression_level as u8)?;
    main_file_bytes.extend_from_slice(&compressed);

    Ok(EncodedRegion {
        main_file_bytes,
        sidecar_files: Vec::new(),
        warnings: Vec::new(),
    })
}
