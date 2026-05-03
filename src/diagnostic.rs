use std::fmt::{Display, Formatter};
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum DiagnosticLevel {
    Warning,
    Error,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum DiagnosticCode {
    CorruptChunk,
    CorruptBucket,
    CorruptRegion,
    InvalidMetadata,
    FormatMismatch,
    SkippedData,
    Io,
    OutputSafety,
    UnsupportedFormat,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RegionLocation {
    pub path: Option<PathBuf>,
    pub region_coords: Option<(i32, i32)>,
    pub chunk_index: Option<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Diagnostic {
    pub level: DiagnosticLevel,
    pub code: DiagnosticCode,
    pub message: String,
    pub location: RegionLocation,
}

impl Diagnostic {
    pub fn warning(code: DiagnosticCode, message: impl Into<String>) -> Self {
        Self {
            level: DiagnosticLevel::Warning,
            code,
            message: message.into(),
            location: RegionLocation::default(),
        }
    }

    pub fn error(code: DiagnosticCode, message: impl Into<String>) -> Self {
        Self {
            level: DiagnosticLevel::Error,
            code,
            message: message.into(),
            location: RegionLocation::default(),
        }
    }

    pub fn with_path(mut self, path: impl AsRef<Path>) -> Self {
        self.location.path = Some(path.as_ref().to_path_buf());
        self
    }

    pub fn with_region_coords(mut self, region_x: i32, region_z: i32) -> Self {
        self.location.region_coords = Some((region_x, region_z));
        self
    }

    pub fn with_chunk_index(mut self, chunk_index: usize) -> Self {
        self.location.chunk_index = Some(chunk_index);
        self
    }

    pub fn is_warning(&self) -> bool {
        self.level == DiagnosticLevel::Warning
    }
}

impl Display for Diagnostic {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

pub fn warning_count(diagnostics: &[Diagnostic]) -> usize {
    diagnostics
        .iter()
        .filter(|diagnostic| diagnostic.is_warning())
        .count()
}
