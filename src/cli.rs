use std::path::PathBuf;

use anyhow::{Result, bail};
use clap::{Parser, ValueEnum};

use crate::formats::RegionFormat;

#[derive(Debug, Parser)]
#[command(
    author,
    version,
    about = "Convert Minecraft Java region files between mca, linear, blinear_v2, and blinear_v3."
)]
pub struct Cli {
    #[arg(
        value_name = "INPUT",
        required = true,
        help = "Input world directories or region directories."
    )]
    pub inputs: Vec<PathBuf>,

    #[arg(
        short,
        long,
        value_name = "PATH",
        required = true,
        help = "Output root path."
    )]
    pub output: PathBuf,

    #[arg(
        long,
        value_enum,
        default_value_t = SourceFormatArg::Auto,
        help = "Source format. Auto detects from file extension and header."
    )]
    pub from: SourceFormatArg,

    #[arg(long, value_enum, help = "Target format.")]
    pub to: TargetFormatArg,

    #[arg(
        long,
        value_name = "N",
        help = "Worker thread count. Defaults to all available CPU threads."
    )]
    pub threads: Option<usize>,

    #[arg(
        long,
        value_name = "LEVEL",
        help = "Compression level. mca uses zlib 0-9. linear/blinear use zstd 1-22."
    )]
    pub compression_level: Option<i32>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum SourceFormatArg {
    Auto,
    Mca,
    Linear,
    BlinearV2,
    BlinearV3,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum TargetFormatArg {
    Mca,
    Linear,
    BlinearV2,
    BlinearV3,
}

impl Cli {
    pub fn thread_count(&self) -> Result<usize> {
        match self.threads {
            Some(0) => bail!("thread count must be greater than zero"),
            Some(value) => Ok(value),
            None => Ok(std::thread::available_parallelism()
                .map(|count| count.get())
                .unwrap_or(1)),
        }
    }

    pub fn forced_source_format(&self) -> Option<RegionFormat> {
        self.from.into_region_format()
    }

    pub fn target_format(&self) -> RegionFormat {
        self.to.into_region_format()
    }

    pub fn resolved_compression_level(&self) -> Result<i32> {
        let target = self.target_format();
        let level = self
            .compression_level
            .unwrap_or_else(|| target.default_compression_level());

        match target {
            RegionFormat::Mca => {
                if !(0..=9).contains(&level) {
                    bail!("mca compression level must be in the range 0..=9");
                }
            }
            RegionFormat::Blinear => {
                bail!("generic blinear placeholder cannot be used as a target format");
            }
            RegionFormat::Linear | RegionFormat::BlinearV2 | RegionFormat::BlinearV3 => {
                if !(1..=22).contains(&level) {
                    bail!("linear and blinear compression levels must be in the range 1..=22");
                }
            }
        }

        Ok(level)
    }
}

impl SourceFormatArg {
    pub fn into_region_format(self) -> Option<RegionFormat> {
        match self {
            Self::Auto => None,
            Self::Mca => Some(RegionFormat::Mca),
            Self::Linear => Some(RegionFormat::Linear),
            Self::BlinearV2 => Some(RegionFormat::BlinearV2),
            Self::BlinearV3 => Some(RegionFormat::BlinearV3),
        }
    }
}

impl TargetFormatArg {
    pub fn into_region_format(self) -> RegionFormat {
        match self {
            Self::Mca => RegionFormat::Mca,
            Self::Linear => RegionFormat::Linear,
            Self::BlinearV2 => RegionFormat::BlinearV2,
            Self::BlinearV3 => RegionFormat::BlinearV3,
        }
    }
}
