use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

use crate::formats::{EncodedRegion, RegionFormat, region_uses_external_chunks};
use crate::io_util::{commit_prepared_file, discard_prepared_file, prepare_atomic_write};

pub fn write_encoded_region(
    target_format: RegionFormat,
    destination_file: &Path,
    encoded: &EncodedRegion,
) -> Result<()> {
    let transaction = OutputTransaction::prepare(target_format, destination_file, encoded)?;
    transaction.commit()
}

struct OutputTransaction {
    prepared_sidecars: Vec<PreparedOutputFile>,
    prepared_main: PreparedOutputFile,
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
