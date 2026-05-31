use std::fs::{self, File};
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::process;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};

static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

pub fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let temp_path = prepare_atomic_write(path, bytes)?;
    commit_prepared_file(&temp_path, path)
}

pub fn read_file_bytes(path: &Path) -> Result<Vec<u8>> {
    let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let capacity = file
        .metadata()
        .map(|metadata| metadata.len() as usize)
        .unwrap_or(0);
    let mut reader = BufReader::new(file);
    let mut bytes = Vec::with_capacity(capacity);
    reader
        .read_to_end(&mut bytes)
        .with_context(|| format!("failed to read {}", path.display()))?;
    Ok(bytes)
}

pub fn prepare_atomic_write(path: &Path, bytes: &[u8]) -> Result<std::path::PathBuf> {
    let temp_path = prepare_temp_path(path)?;
    fs::write(&temp_path, bytes)
        .with_context(|| format!("failed to write temporary file {}", temp_path.display()))?;

    Ok(temp_path)
}

pub fn prepare_temp_path(path: &Path) -> Result<PathBuf> {
    let parent = path
        .parent()
        .context("output file does not have a parent directory")?;
    fs::create_dir_all(parent).with_context(|| format!("failed to create {}", parent.display()))?;

    let counter = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let file_name = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .context("output file is missing a file name")?;
    let temp_name = format!(".{file_name}.tmp-{}-{counter}", process::id());
    Ok(parent.join(temp_name))
}

pub fn commit_prepared_file(source: &Path, destination: &Path) -> Result<()> {
    match fs::rename(source, destination) {
        Ok(()) => Ok(()),
        Err(rename_error) => {
            if destination.exists() {
                fs::remove_file(destination).with_context(|| {
                    format!("failed to remove existing file {}", destination.display())
                })?;
                fs::rename(source, destination).with_context(|| {
                    format!(
                        "failed to replace {} with {} after removing the old file",
                        destination.display(),
                        source.display()
                    )
                })?;
                Ok(())
            } else {
                Err(rename_error).with_context(|| {
                    format!(
                        "failed to move temporary file {} into place at {}",
                        source.display(),
                        destination.display()
                    )
                })
            }
        }
    }
}

pub fn discard_prepared_file(path: &Path) {
    let _ = fs::remove_file(path);
}
