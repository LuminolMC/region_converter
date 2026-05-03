use std::io::Cursor;
use std::path::Path;

use anyhow::{Context, Result, ensure};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use crate::formats::{
    EncodedRegion, LINEAR_SUPERBLOCK, ReadOutcome, normalize_timestamp_to_u32,
    parse_region_coords_from_path,
};
use crate::model::{ChunkData, REGION_CHUNK_COUNT, Region};

const FILE_HEADER_SIZE: usize = 32;
const FILE_FOOTER_SIZE: usize = 8;
const INNER_HEADER_SIZE: usize = REGION_CHUNK_COUNT * 8;

pub fn read_region(path: &Path) -> Result<ReadOutcome> {
    let bytes =
        std::fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    ensure!(
        bytes.len() >= FILE_HEADER_SIZE + FILE_FOOTER_SIZE,
        "linear region {} is too small",
        path.display()
    );

    let (region_x, region_z) = parse_region_coords_from_path(path)?;
    let mut cursor = Cursor::new(&bytes);

    let signature = cursor.read_u64::<BigEndian>()?;
    ensure!(signature == LINEAR_SUPERBLOCK, "invalid linear superblock");

    let version = cursor.read_u8()?;
    ensure!(
        version == 1 || version == 2,
        "unsupported linear version {version}; this converter writes and reads classic linear v1/v2"
    );

    let _newest_timestamp = cursor.read_u64::<BigEndian>()?;
    let _compression_level = cursor.read_i8()?;
    let chunk_count = cursor.read_u16::<BigEndian>()? as usize;
    let compressed_len = cursor.read_u32::<BigEndian>()? as usize;
    let _reserved = cursor.read_u64::<BigEndian>()?;

    ensure!(
        FILE_HEADER_SIZE + compressed_len + FILE_FOOTER_SIZE == bytes.len(),
        "linear region compressed length does not match the file size"
    );

    let footer_offset = FILE_HEADER_SIZE + compressed_len;
    let footer_signature =
        u64::from_be_bytes(bytes[footer_offset..footer_offset + 8].try_into().unwrap());
    ensure!(
        footer_signature == LINEAR_SUPERBLOCK,
        "invalid linear footer signature"
    );

    let compressed = &bytes[FILE_HEADER_SIZE..footer_offset];
    let decompressed = zstd::stream::decode_all(Cursor::new(compressed))
        .with_context(|| format!("failed to decompress {}", path.display()))?;

    ensure!(
        decompressed.len() >= INNER_HEADER_SIZE,
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
        INNER_HEADER_SIZE + total_payload_len == decompressed.len(),
        "linear region payload lengths do not match the decompressed size"
    );

    let mut payload_offset = INNER_HEADER_SIZE;
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
        warnings: Vec::new(),
        discarded_chunks: 0,
    })
}

pub fn encode_region(region: &Region, compression_level: i32) -> Result<EncodedRegion> {
    let mut table = Vec::with_capacity(INNER_HEADER_SIZE);
    let mut payload = Vec::new();
    let mut chunk_count = 0_u16;
    let mut newest_timestamp = 0_u32;

    for index in 0..REGION_CHUNK_COUNT {
        if let Some(chunk) = region.chunk(index) {
            let size =
                u32::try_from(chunk.raw_nbt.len()).context("linear chunk payload is too large")?;
            let (timestamp, _) = normalize_timestamp_to_u32(chunk.timestamp);
            newest_timestamp = newest_timestamp.max(timestamp);
            chunk_count += 1;
            table.write_u32::<BigEndian>(size)?;
            table.write_u32::<BigEndian>(timestamp)?;
            payload.extend_from_slice(&chunk.raw_nbt);
        } else {
            table.write_u32::<BigEndian>(0)?;
            table.write_u32::<BigEndian>(0)?;
        }
    }

    let mut complete_region = table;
    complete_region.extend_from_slice(&payload);

    let compressed = zstd::bulk::compress(&complete_region, compression_level)
        .context("failed to zstd-compress the linear region payload")?;
    let compressed_len =
        u32::try_from(compressed.len()).context("linear compressed payload is too large")?;

    let mut main_file_bytes =
        Vec::with_capacity(FILE_HEADER_SIZE + compressed.len() + FILE_FOOTER_SIZE);
    main_file_bytes.write_u64::<BigEndian>(LINEAR_SUPERBLOCK)?;
    main_file_bytes.write_u8(1)?;
    main_file_bytes.write_u64::<BigEndian>(u64::from(newest_timestamp))?;
    main_file_bytes.write_i8(compression_level as i8)?;
    main_file_bytes.write_u16::<BigEndian>(chunk_count)?;
    main_file_bytes.write_u32::<BigEndian>(compressed_len)?;
    main_file_bytes.write_u64::<BigEndian>(0)?;
    main_file_bytes.extend_from_slice(&compressed);
    main_file_bytes.write_u64::<BigEndian>(LINEAR_SUPERBLOCK)?;

    Ok(EncodedRegion {
        main_file_bytes,
        sidecar_files: Vec::new(),
        warnings: Vec::new(),
    })
}
