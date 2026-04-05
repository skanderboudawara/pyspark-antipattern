use std::process;

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

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

        /// Comma-separated rule IDs or group prefixes to select (only these are shown)
        /// (e.g. --select=F018,D001)
        #[arg(long = "select", value_delimiter = ',')]
        select: Option<Vec<String>>,

        /// Comma-separated rule IDs or group prefixes to downgrade to warnings
        /// (e.g. --warn=F008,F011)
        #[arg(long = "warn", value_delimiter = ',')]
        warn: Option<Vec<String>>,

        /// Comma-separated rule IDs or group prefixes to silence completely
        /// (e.g. --ignore=F,D001)
        #[arg(long = "ignore", value_delimiter = ',')]
        ignore: Option<Vec<String>>,

        /// Show inline rule explanation for every violation
        #[arg(long = "show_best_practice")]
        show_best_practice: Option<bool>,

        /// Show inline information text for every violation
        #[arg(long = "show_information")]
        show_information: Option<bool>,

        /// S004: fire when weighted .distinct() count exceeds this (default: 5)
        #[arg(long = "distinct_threshold")]
        distinct_threshold: Option<usize>,

        /// S008: fire when weighted explode() count exceeds this (default: 3)
        #[arg(long = "explode_threshold")]
        explode_threshold: Option<usize>,

        /// L001/L002/L003: fire when range(N) loop count exceeds this (default: 10)
        #[arg(long = "loop_threshold")]
        loop_threshold: Option<usize>,

        /// PERF003: fire when shuffle operations without a checkpoint exceed this (default: 9)
        #[arg(long = "max_shuffle_operations")]
        max_shuffle_operations: Option<usize>,

        /// Comma-separated directory names to exclude from recursive scanning
        /// (e.g. --exclude_dirs=vendor,generated)
        #[arg(long = "exclude_dirs", value_delimiter = ',')]
        exclude_dirs: Option<Vec<String>>,

        /// Minimum performance-impact level to report: low, medium, or high
        /// (e.g. --severity=medium shows only MEDIUM and HIGH violations)
        #[arg(long = "severity")]
        severity: Option<String>,

        /// PySpark version of your cluster (e.g. 3.4 or 3.5.1).
        /// Silences rules that require a newer version than your cluster supports.
        #[arg(long = "pyspark-version")]
        pyspark_version: Option<String>,
    },
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Command::Check {
            path,
            config: config_path,
            select,
            warn,
            ignore,
            show_best_practice,
            show_information,
            distinct_threshold,
            explode_threshold,
            loop_threshold,
            max_shuffle_operations,
            exclude_dirs,
            severity,
            pyspark_version,
        } => {
            let mut config = match config::Config::load(std::path::Path::new(&config_path)) {
                Ok(Some(c)) => c,
                Ok(None) => config::Config::default(), // file not found → use defaults silently
                Err(e) => {
                    eprintln!("error: {e}");
                    process::exit(2);
                }
            };

            // CLI flags override pyproject.toml values.
            // When --warn is given without --select, implicitly restrict to only
            // the warned rules — the caller wants to see those rules exclusively.
            let warn_without_select = warn.is_some() && select.is_none();
            if let Some(v) = select {
                config.select = v;
            }
            if let Some(v) = warn {
                if warn_without_select && config.select.is_empty() {
                    config.select = v.clone();
                }
                config.warn = v;
            }
            if let Some(v) = ignore {
                config.ignore = v;
            }
            if let Some(v) = show_best_practice {
                config.show_best_practice = v;
            }
            if let Some(v) = show_information {
                config.show_information = v;
            }
            if let Some(v) = distinct_threshold {
                config.distinct_threshold = v;
            }
            if let Some(v) = explode_threshold {
                config.explode_threshold = v;
            }
            if let Some(v) = loop_threshold {
                config.loop_threshold = v;
            }
            if let Some(v) = max_shuffle_operations {
                config.max_shuffle_operations = v;
            }
            if let Some(v) = exclude_dirs {
                config.exclude_dirs = v;
            }
            if let Some(s) = pyspark_version {
                match s.parse::<violation::PySparkVersion>() {
                    Ok(v) => config.pyspark_version = Some(v),
                    Err(e) => eprintln!("warning: invalid --pyspark-version '{s}': {e}"),
                }
            }
            if let Some(s) = severity {
                match s.to_lowercase().as_str() {
                    "low" => config.severity = Some(violation::Impact::Low),
                    "medium" => config.severity = Some(violation::Impact::Medium),
                    "high" => config.severity = Some(violation::Impact::High),
                    other => eprintln!("warning: unknown --severity value '{other}'; expected low, medium, or high"),
                }
            }

            let mut error_count = 0usize;
            let mut warning_count = 0usize;
            let mut high_count = 0usize;
            let mut medium_count = 0usize;
            let mut low_count = 0usize;

            let (file_count, read_failures) = checker::check_path(&path, &config, &mut |violations| {
                for v in &violations {
                    match v.severity {
                        violation::Severity::Error => error_count += 1,
                        violation::Severity::Warning => warning_count += 1,
                    }
                    match v.impact {
                        violation::Impact::High => high_count += 1,
                        violation::Impact::Medium => medium_count += 1,
                        violation::Impact::Low => low_count += 1,
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
            if error_count > 0 || warning_count > 0 {
                reporter::print_impact_summary(high_count, medium_count, low_count);
            }

            if read_failures > 0 {
                eprintln!("error: {read_failures} file(s) could not be read or parsed.");
            }

            if error_count > 0 || read_failures > 0 {
                process::exit(1);
            }
        }
    }
}
