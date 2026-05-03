mod console;

use std::process::ExitCode;

use anyhow::Result;
use clap::Parser;

use console::ConsoleReporter;
use region_converter::cli::Cli;
use region_converter::convert::run_with_observer;

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
    let mut reporter = ConsoleReporter::new();
    let summary = run_with_observer(cli, &mut reporter)?;
    let has_issues = summary.failed_jobs > 0 || summary.total_warnings > 0;

    Ok(if has_issues {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    })
}
