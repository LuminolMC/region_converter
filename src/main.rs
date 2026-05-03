use std::process::ExitCode;
use std::{io, io::Write};

use anyhow::Result;
use clap::Parser;

use region_converter::cli::Cli;
use region_converter::convert::{JobReport, run};

fn main() -> ExitCode {
    match real_main() {
        Ok(code) => code,
        Err(error) => {
            eprintln!("fatal error: {error:#}");
            ExitCode::FAILURE
        }
    }
}

fn real_main() -> Result<ExitCode> {
    let cli = Cli::parse();
    let summary = run(cli)?;

    println!(
        "Discovered {} region files. Target format: {}. Threads: {}. Compression level: {}.",
        summary.total_jobs, summary.target_format, summary.thread_count, summary.compression_level
    );
    io::stdout().flush()?;

    for report in &summary.job_reports {
        match report {
            JobReport::Success(report) => {
                for warning in &report.warnings {
                    eprintln!(
                        "warning [{} -> {}]: {}",
                        report.source_file, report.destination_file, warning
                    );
                }
            }
            JobReport::Failure(report) => {
                eprintln!(
                    "error [{} -> {}]: {}",
                    report.source_file, report.destination_file, report.error
                );
            }
        }
    }

    println!(
        "Completed. Success: {}. Failed: {}. Chunks written: {}. Discarded chunks: {}. Warnings: {}.",
        summary.successful_jobs,
        summary.failed_jobs,
        summary.total_chunks_written,
        summary.total_discarded_chunks,
        summary.total_warnings
    );
    io::stdout().flush()?;

    let has_issues = summary.failed_jobs > 0 || summary.total_warnings > 0;
    Ok(if has_issues {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    })
}
