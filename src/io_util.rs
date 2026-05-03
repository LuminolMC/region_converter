use std::fs;
use std::path::Path;
use std::process;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};

static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

pub fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
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
    let temp_path = parent.join(temp_name);

    fs::write(&temp_path, bytes)
        .with_context(|| format!("failed to write temporary file {}", temp_path.display()))?;

    replace_file(&temp_path, path)
}

fn replace_file(source: &Path, destination: &Path) -> Result<()> {
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
