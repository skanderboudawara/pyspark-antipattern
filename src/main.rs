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

        /// Comma-separated rule IDs or group prefixes to silence completely
        /// (e.g. --ignore_rules=F,D001)
        #[arg(long = "ignore_rules", value_delimiter = ',')]
        ignore_rules: Option<Vec<String>>,

        /// Comma-separated rule IDs or group prefixes to downgrade to warnings
        /// (e.g. --warning_rules=F008,F011)
        #[arg(long = "warning_rules", value_delimiter = ',')]
        warning_rules: Option<Vec<String>>,

        /// Comma-separated rule IDs or group prefixes to treat as hard errors
        /// (e.g. --failing_rules=D001,S)
        #[arg(long = "failing_rules", value_delimiter = ',')]
        failing_rules: Option<Vec<String>>,

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
    },
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Command::Check {
            path,
            config: config_path,
            ignore_rules,
            warning_rules,
            failing_rules,
            show_best_practice,
            show_information,
            distinct_threshold,
            explode_threshold,
            loop_threshold,
            max_shuffle_operations,
            exclude_dirs,
        } => {
            let mut config = config::Config::load(std::path::Path::new(&config_path))
                .unwrap_or_else(|e| {
                    eprintln!("Config warning: {e} — using defaults");
                    config::Config::default()
                });

            // CLI flags override pyproject.toml values.
            if let Some(v) = ignore_rules          { config.ignore_rules          = v; }
            if let Some(v) = warning_rules         { config.warning_rules         = v; }
            if let Some(v) = failing_rules         { config.failing_rules         = v; }
            if let Some(v) = show_best_practice    { config.show_best_practice    = v; }
            if let Some(v) = show_information      { config.show_information      = v; }
            if let Some(v) = distinct_threshold    { config.distinct_threshold    = v; }
            if let Some(v) = explode_threshold     { config.explode_threshold     = v; }
            if let Some(v) = loop_threshold        { config.loop_threshold        = v; }
            if let Some(v) = max_shuffle_operations { config.max_shuffle_operations = v; }
            if let Some(v) = exclude_dirs          { config.exclude_dirs          = v; }

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
