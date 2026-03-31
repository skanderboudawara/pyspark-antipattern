use std::process;

use clap::{Parser, Subcommand};

use pyspark_antipattern::{checker, config, reporter, violation};

#[derive(Parser)]
#[command(
    name = "pyspark-antipattern",
    version = env!("CARGO_PKG_VERSION"),
    about = "PySpark antipattern linter for CI/CD pipelines"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Check a Python file or directory for PySpark antipatterns
    Check {
        /// Path to file or directory to lint
        path: String,

        /// Path to pyproject.toml containing [tool.pyspark-antipattern] config
        #[arg(long, default_value = "pyproject.toml")]
        config: String,
    },
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Command::Check { path, config: config_path } => {
            let config = config::Config::load(std::path::Path::new(&config_path))
                .unwrap_or_else(|e| {
                    eprintln!("Config warning: {e} — using defaults");
                    config::Config::default()
                });

            let mut error_count   = 0usize;
            let mut warning_count = 0usize;

            let file_count = checker::check_path(&path, &config, &mut |violations| {
                for v in &violations {
                    match v.severity {
                        violation::Severity::Error   => error_count   += 1,
                        violation::Severity::Warning => warning_count += 1,
                    }
                }
                if !violations.is_empty() {
                    reporter::print_violations(&violations, &config);
                }
            });

            eprintln!(
                "Checked {} file(s). Found {} error(s), {} warning(s).",
                file_count, error_count, warning_count,
            );

            if error_count > 0 {
                process::exit(1);
            }
        }
    }
}
