use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use clap::{Parser, ValueEnum};

use crate::formats::{RegionFormat, SourceFormatHint};

#[derive(Debug, Parser)]
#[command(
    author,
    version,
    about = "Convert or inspect Minecraft Java region saves across mca, linear, blinear_v2, and blinear_v3."
)]
pub struct Cli {
    #[arg(
        value_name = "INPUT",
        required = true,
        help = "Input world directories, region directories, or individual region files."
    )]
    pub inputs: Vec<PathBuf>,

    #[arg(short, long, value_name = "PATH", help = "Output root path.")]
    pub output: Option<PathBuf>,

    #[arg(
        long,
        help = "Read the input save(s) and print detailed info without converting."
    )]
    pub info: bool,

    #[arg(
        long,
        value_enum,
        default_value_t = SourceFormatArg::Auto,
        help = "Source format. Auto detects from file extension and header."
    )]
    pub from: SourceFormatArg,

    #[arg(long, value_enum, help = "Target format.")]
    pub to: Option<TargetFormatArg>,

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

    #[arg(
        long,
        help = "Print conversion stage timing and resource limiter details."
    )]
    pub profile: bool,
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
    pub fn validate(&self) -> Result<()> {
        if self.info {
            if self.output.is_some() {
                bail!("--output cannot be used together with --info");
            }
            if self.to.is_some() {
                bail!("--to cannot be used together with --info");
            }
            if self.compression_level.is_some() {
                bail!("--compression-level cannot be used together with --info");
            }
            if self.from != SourceFormatArg::Auto {
                bail!("--from cannot be used together with --info");
            }
            return Ok(());
        }

        if self.output.is_none() {
            bail!("--output is required unless --info is used");
        }
        if self.to.is_none() {
            bail!("--to is required unless --info is used");
        }

        Ok(())
    }

    pub fn thread_count(&self) -> Result<usize> {
        match self.threads {
            Some(0) => bail!("thread count must be greater than zero"),
            Some(value) => Ok(value),
            None => Ok(std::thread::available_parallelism()
                .map(|count| count.get())
                .unwrap_or(1)),
        }
    }

    pub fn forced_source_format(&self) -> Option<SourceFormatHint> {
        self.from.into_source_format_hint()
    }

    pub fn output_root(&self) -> Result<&Path> {
        self.output
            .as_deref()
            .context("output root is only available in conversion mode")
    }

    pub fn target_format(&self) -> Result<RegionFormat> {
        self.to
            .map(TargetFormatArg::into_region_format)
            .context("target format is only available in conversion mode")
    }

    pub fn resolved_compression_level(&self) -> Result<i32> {
        let target = self.target_format()?;
        let level = self
            .compression_level
            .unwrap_or_else(|| target.default_compression_level());

        match target {
            RegionFormat::Mca => {
                if !(0..=9).contains(&level) {
                    bail!("mca compression level must be in the range 0..=9");
                }
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
    pub fn into_source_format_hint(self) -> Option<SourceFormatHint> {
        match self {
            Self::Auto => None,
            Self::Mca => Some(SourceFormatHint::Mca),
            Self::Linear => Some(SourceFormatHint::Linear),
            Self::BlinearV2 => Some(SourceFormatHint::BlinearV2),
            Self::BlinearV3 => Some(SourceFormatHint::BlinearV3),
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
