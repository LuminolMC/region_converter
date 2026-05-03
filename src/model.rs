use anyhow::{Result, ensure};

use crate::formats::RegionFormat;

pub const REGION_SIDE: usize = 32;
pub const REGION_CHUNK_COUNT: usize = REGION_SIDE * REGION_SIDE;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkData {
    pub timestamp: i64,
    pub raw_nbt: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Region {
    pub region_x: i32,
    pub region_z: i32,
    chunks: Vec<Option<ChunkData>>,
}

impl Region {
    pub fn new(region_x: i32, region_z: i32) -> Self {
        Self {
            region_x,
            region_z,
            chunks: vec![None; REGION_CHUNK_COUNT],
        }
    }

    pub fn set_chunk(&mut self, index: usize, chunk: ChunkData) -> Result<()> {
        ensure!(
            index < REGION_CHUNK_COUNT,
            "chunk index {index} is outside the 32x32 region grid"
        );
        self.chunks[index] = Some(chunk);
        Ok(())
    }

    pub fn chunk(&self, index: usize) -> Option<&ChunkData> {
        self.chunks.get(index).and_then(Option::as_ref)
    }

    pub fn iter_chunks(&self) -> impl Iterator<Item = (usize, &ChunkData)> {
        self.chunks
            .iter()
            .enumerate()
            .filter_map(|(index, chunk)| chunk.as_ref().map(|chunk| (index, chunk)))
    }

    pub fn chunk_count(&self) -> usize {
        self.iter_chunks().count()
    }

    pub fn newest_timestamp(&self) -> i64 {
        self.iter_chunks()
            .map(|(_, chunk)| chunk.timestamp)
            .max()
            .unwrap_or(0)
    }

    pub fn file_name(&self, format: RegionFormat) -> String {
        format.region_file_name(self.region_x, self.region_z)
    }
}
