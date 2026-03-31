use std::process;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

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

            let error_count   = AtomicUsize::new(0);
            let warning_count = AtomicUsize::new(0);

            // Mutex ensures violation blocks from different files don't interleave
            // on stdout when multiple rayon threads finish simultaneously.
            let stdout_lock = Mutex::new(());

            let file_count = checker::check_path(&path, &config, &|violations| {
                if violations.is_empty() { return; }

                for v in &violations {
                    match v.severity {
                        violation::Severity::Error   => { error_count.fetch_add(1, Ordering::Relaxed); }
                        violation::Severity::Warning => { warning_count.fetch_add(1, Ordering::Relaxed); }
                    }
                }

                let _guard = stdout_lock.lock().unwrap();
                reporter::print_violations(&violations, &config);
            });

            eprintln!(
                "Checked {} file(s). Found {} error(s), {} warning(s).",
                file_count,
                error_count.load(Ordering::Relaxed),
                warning_count.load(Ordering::Relaxed),
            );

            if error_count.load(Ordering::Relaxed) > 0 {
                process::exit(1);
            }
        }
    }
}
