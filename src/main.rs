mod console;

use std::process::ExitCode;

use anyhow::Result;
use clap::Parser;

use console::ConsoleReporter;
use region_converter::cli::Cli;
use region_converter::convert::run_with_observer;
use region_converter::info::inspect;

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
    cli.validate()?;
    let mut reporter = ConsoleReporter::new();

    let has_issues = if cli.info {
        let summary = inspect(cli)?;
        reporter.print_info_summary(&summary)?;
        summary.failed_regions > 0 || summary.warnings > 0
    } else {
        let summary = run_with_observer(cli, &mut reporter)?;
        summary.failed_jobs > 0 || summary.total_warnings > 0
    };

    Ok(if has_issues {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    })
}
