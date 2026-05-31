use std::fs::{self, File};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};

use crate::formats::{EncodedRegion, RegionFormat, region_uses_external_chunks};
use crate::io_util::{
    commit_prepared_file, discard_prepared_file, prepare_atomic_write, prepare_temp_path,
};

pub fn write_encoded_region(
    target_format: RegionFormat,
    destination_file: &Path,
    encoded: &EncodedRegion,
) -> Result<()> {
    let transaction = OutputTransaction::prepare(target_format, destination_file, encoded)?;
    transaction.commit()
}

pub trait RegionWriteTarget {
    fn main_file(&mut self) -> &mut File;
    fn write_sidecar_file(&mut self, file_name: &str, bytes: &[u8]) -> Result<()>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct WriteTransactionProfile {
    pub prepare: Duration,
    pub commit: Duration,
}

pub fn write_region_with_transaction<F>(
    target_format: RegionFormat,
    destination_file: &Path,
    write_region: F,
) -> Result<()>
where
    F: FnOnce(&mut dyn RegionWriteTarget) -> Result<()>,
{
    write_region_with_transaction_profiled(target_format, destination_file, None, write_region)
}

pub fn write_region_with_transaction_profiled<F>(
    target_format: RegionFormat,
    destination_file: &Path,
    mut profile: Option<&mut WriteTransactionProfile>,
    write_region: F,
) -> Result<()>
where
    F: FnOnce(&mut dyn RegionWriteTarget) -> Result<()>,
{
    let prepare_started_at = Instant::now();
    let mut transaction = StreamingOutputTransaction::prepare(target_format, destination_file)?;
    if let Some(profile) = profile.as_deref_mut() {
        profile.prepare = prepare_started_at.elapsed();
    }

    if let Err(error) = write_region(&mut transaction) {
        transaction.rollback();
        return Err(error);
    }

    let commit_started_at = Instant::now();
    let result = transaction.commit();
    if let Some(profile) = profile {
        profile.commit = commit_started_at.elapsed();
    }
    result
}

struct OutputTransaction {
    prepared_sidecars: Vec<PreparedOutputFile>,
    prepared_main: PreparedOutputFile,
}

struct StreamingOutputTransaction {
    prepared_sidecars: Vec<PreparedOutputFile>,
    prepared_main: PreparedOutputFile,
    main_file: File,
    destination_dir: PathBuf,
}

impl StreamingOutputTransaction {
    fn prepare(target_format: RegionFormat, destination_file: &Path) -> Result<Self> {
        let destination_dir = destination_file
            .parent()
            .context("destination file does not have a parent directory")?
            .to_path_buf();
        fs::create_dir_all(&destination_dir)
            .with_context(|| format!("failed to create {}", destination_dir.display()))?;

        if target_format == RegionFormat::Mca
            && destination_file.exists()
            && region_uses_external_chunks(destination_file)?
        {
            bail!(
                "refusing to overwrite {} because it already uses external .mcc chunks and replacing the sidecars plus header atomically is not currently safe",
                destination_file.display()
            );
        }

        let temp_path = prepare_temp_path(destination_file)?;
        let main_file = File::create(&temp_path)
            .with_context(|| format!("failed to create temporary file {}", temp_path.display()))?;

        Ok(Self {
            prepared_sidecars: Vec::new(),
            prepared_main: PreparedOutputFile {
                destination: destination_file.to_path_buf(),
                temp_path,
            },
            main_file,
            destination_dir,
        })
    }

    fn commit(mut self) -> Result<()> {
        self.main_file.flush().with_context(|| {
            format!(
                "failed to flush temporary file {}",
                self.prepared_main.temp_path.display()
            )
        })?;
        self.main_file.seek(SeekFrom::End(0)).with_context(|| {
            format!(
                "failed to finish temporary file {}",
                self.prepared_main.temp_path.display()
            )
        })?;
        drop(self.main_file);

        let mut committed_sidecars = Vec::new();
        for sidecar in self.prepared_sidecars {
            if let Err(error) = commit_prepared_file(&sidecar.temp_path, &sidecar.destination) {
                discard_prepared_file(&self.prepared_main.temp_path);
                for committed in committed_sidecars {
                    let _ = fs::remove_file(committed);
                }
                return Err(error);
            }
            committed_sidecars.push(sidecar.destination);
        }

        if let Err(error) = commit_prepared_file(
            &self.prepared_main.temp_path,
            &self.prepared_main.destination,
        ) {
            for committed in committed_sidecars {
                let _ = fs::remove_file(committed);
            }
            return Err(error);
        }

        Ok(())
    }

    fn rollback(self) {
        drop(self.main_file);
        discard_prepared_file(&self.prepared_main.temp_path);
        for sidecar in self.prepared_sidecars {
            discard_prepared_file(&sidecar.temp_path);
        }
    }
}

impl RegionWriteTarget for StreamingOutputTransaction {
    fn main_file(&mut self) -> &mut File {
        &mut self.main_file
    }

    fn write_sidecar_file(&mut self, file_name: &str, bytes: &[u8]) -> Result<()> {
        let destination = self.destination_dir.join(file_name);
        let temp_path = prepare_atomic_write(&destination, bytes)?;
        self.prepared_sidecars.push(PreparedOutputFile {
            destination,
            temp_path,
        });
        Ok(())
    }
}

impl OutputTransaction {
    fn prepare(
        target_format: RegionFormat,
        destination_file: &Path,
        encoded: &EncodedRegion,
    ) -> Result<Self> {
        let destination_dir = destination_file
            .parent()
            .context("destination file does not have a parent directory")?;
        fs::create_dir_all(destination_dir)
            .with_context(|| format!("failed to create {}", destination_dir.display()))?;

        if target_format == RegionFormat::Mca
            && !encoded.sidecar_files.is_empty()
            && destination_file.exists()
            && region_uses_external_chunks(destination_file)?
        {
            bail!(
                "refusing to overwrite {} because it already uses external .mcc chunks and replacing the sidecars plus header atomically is not currently safe",
                destination_file.display()
            );
        }

        let mut prepared_sidecars = Vec::with_capacity(encoded.sidecar_files.len());
        for sidecar in &encoded.sidecar_files {
            let destination = destination_dir.join(&sidecar.file_name);
            let temp_path = prepare_atomic_write(&destination, &sidecar.bytes)?;
            prepared_sidecars.push(PreparedOutputFile {
                destination,
                temp_path,
            });
        }

        let prepared_main = PreparedOutputFile {
            destination: destination_file.to_path_buf(),
            temp_path: prepare_atomic_write(destination_file, &encoded.main_file_bytes)?,
        };

        Ok(Self {
            prepared_sidecars,
            prepared_main,
        })
    }

    fn commit(self) -> Result<()> {
        let mut committed_sidecars = Vec::new();
        for sidecar in self.prepared_sidecars {
            if let Err(error) = commit_prepared_file(&sidecar.temp_path, &sidecar.destination) {
                discard_prepared_file(&self.prepared_main.temp_path);
                for committed in committed_sidecars {
                    let _ = fs::remove_file(committed);
                }
                return Err(error);
            }
            committed_sidecars.push(sidecar.destination);
        }

        if let Err(error) = commit_prepared_file(
            &self.prepared_main.temp_path,
            &self.prepared_main.destination,
        ) {
            for committed in committed_sidecars {
                let _ = fs::remove_file(committed);
            }
            return Err(error);
        }

        Ok(())
    }
}

struct PreparedOutputFile {
    destination: PathBuf,
    temp_path: PathBuf,
}
