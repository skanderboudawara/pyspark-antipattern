//! Terminal output rendering for violations, impact summaries, and rule metadata.
//! All output is written to stderr using `termcolor` for cross-platform colour support.
use std::io::Write;
use termcolor::{BufferWriter, Color, ColorChoice, ColorSpec, WriteColor};

use crate::{
    config::Config,
    rule_content,
    violation::{Impact, PySparkVersion, Severity, Violation},
};

/// Render `violations` to stderr with coloured, rustc-style diagnostic output.
/// Inline information and best-practice hints are shown when enabled in `config`.
pub fn print_violations(violations: &[Violation], config: &Config) {
    let writer = BufferWriter::stderr(ColorChoice::Auto);
    let mut out = writer.buffer();

    // ── Pass 1: compact violation list ──────────────────────────────────────
    for v in violations {
        let (label, color) = match v.severity {
            Severity::Error => ("error", Color::Red),
            Severity::Warning => ("warning", Color::Yellow),
        };
        let (impact_label, impact_color) = match v.impact {
            Impact::Low => ("LOW", Color::Green),
            Impact::Medium => ("MEDIUM", Color::Yellow),
            Impact::High => ("HIGH", Color::Red),
        };
        let title = rule_title(&v.rule_id.0);
        let gutter = v.line.to_string().len();

        // "error[D001][HIGH]: Avoid using collect()"
        out.set_color(ColorSpec::new().set_fg(Some(color)).set_bold(true)).ok();
        write!(out, "{label}").ok();
        out.set_color(ColorSpec::new().set_bold(true)).ok();
        write!(out, "[{}]", v.rule_id).ok();
        out.set_color(ColorSpec::new().set_fg(Some(impact_color)).set_bold(true))
            .ok();
        write!(out, "[{impact_label}]").ok();
        out.reset().ok();
        writeln!(out, ": {title}").ok();

        // "  --> file.py:42:10"
        out.set_color(ColorSpec::new().set_fg(Some(Color::Cyan)).set_bold(true))
            .ok();
        write!(out, "  --> ").ok();
        out.reset().ok();
        writeln!(out, "{}:{}:{}", v.file, v.line, v.col).ok();

        // gutter + source line + carets
        writeln!(out, "{} |", " ".repeat(gutter)).ok();
        out.set_color(ColorSpec::new().set_fg(Some(Color::Cyan)).set_bold(true))
            .ok();
        write!(out, "{} | ", v.line).ok();
        out.reset().ok();
        writeln!(out, "{}", v.source_line).ok();

        let indent = v.col.saturating_sub(1);
        let carets = "^".repeat(v.span_len.max(1));
        out.set_color(ColorSpec::new().set_fg(Some(color)).set_bold(true)).ok();
        writeln!(out, "{} | {}{}", " ".repeat(gutter), " ".repeat(indent), carets).ok();
        out.reset().ok();
        writeln!(out, "{} |", " ".repeat(gutter)).ok();

        // inline info / best-practice
        if let Some(content) = rule_content::get_content(&v.rule_id.0) {
            if config.show_information && !content.information.is_empty() {
                writeln!(out, "   info:").ok();
                for line in content.information.lines() {
                    let text = rule_content::strip_md(line.trim());
                    if !text.is_empty() {
                        writeln!(out, "     {text}").ok();
                    }
                }
            }
            if config.show_best_practice && !content.best_practice.is_empty() {
                writeln!(out, "   best practice:").ok();
                for line in content.best_practice.lines() {
                    let text = rule_content::strip_md(line.trim());
                    if !text.is_empty() {
                        writeln!(out, "     {text}").ok();
                    }
                }
            }
        }

        writeln!(out).ok();
    }

    // Write everything to stderr in one shot.
    writer.print(&out).ok();
}

/// Return the static performance impact of a rule.
pub fn rule_impact(id: &str) -> Impact {
    match id {
        // ── LOW ─────────────────────────────────────────────────────────────
        "ARR002" | "ARR004" | "ARR005" | "ARR006" => Impact::Low,
        "F001" | "F002" | "F003" | "F005" | "F006" | "F007" | "F008" | "F009" | "F010" | "F011" | "F012" | "F013"
        | "F015" | "F016" | "F017" | "F018" | "F020" => Impact::Low,
        "S012" => Impact::Low,

        // ── MEDIUM ───────────────────────────────────────────────────────────
        "ARR001" | "ARR003" => Impact::Medium,
        "D003" | "D008" => Impact::Medium,
        "F004" | "F014" | "F019" => Impact::Medium,
        "PERF002" | "PERF004" | "PERF005" | "PERF006" | "PERF007" | "PERF008" => Impact::Medium,
        "S001" | "S002" | "S009" => Impact::Medium,

        // ── HIGH ─────────────────────────────────────────────────────────────
        "D001" | "D002" | "D004" | "D005" | "D006" | "D007" | "D009" => Impact::High,
        "L001" | "L002" | "L003" => Impact::High,
        "P001" => Impact::High,
        "PERF001" | "PERF003" => Impact::High,
        "S003" | "S004" | "S005" | "S006" | "S007" | "S008" | "S010" | "S011" | "S013" | "S014" => Impact::High,
        "S015" => Impact::Medium,
        "U001" | "U002" | "U003" | "U004" | "U005" | "U006" | "U007" => Impact::High,

        _ => Impact::Low,
    }
}

/// Return the minimum PySpark version a rule applies to.
/// All rules default to 3.0 — update specific entries as rules are reviewed.
pub fn rule_pyspark_version(id: &str) -> PySparkVersion {
    match id {
        // U005/U006/U007 recommend higher-order functions (transform, forall, exists) — added in PySpark 3.1.0
        "U005" | "U006" | "U007" => PySparkVersion::new(3, 1, 0),

        // D005/D006/D007/D009 recommend .isEmpty() which was added in PySpark 3.3.0
        "D005" | "D006" | "D007" | "D009" => PySparkVersion::new(3, 3, 0),

        // ARR002 recommends array_compact() — added in PySpark 3.4.0
        // F006 recommends withColumnsRenamed() — added in PySpark 3.4.0
        "ARR002" | "F006" => PySparkVersion::new(3, 4, 0),

        // F005 recommends withColumns() — added in PySpark 3.3.0
        "F005" => PySparkVersion::new(3, 3, 0),

        // ARR004 recommends count_distinct() — added in PySpark 3.2.0
        "ARR004" => PySparkVersion::new(3, 2, 0),

        // ARR001/ARR003 — available since PySpark 2.4.0
        "ARR001" | "ARR003" => PySparkVersion::new(2, 4, 0),

        // ARR005/ARR006 — available since PySpark 1.6.0
        "ARR005" | "ARR006" => PySparkVersion::new(1, 6, 0),

        // D001/D002/D003/D004 — available since PySpark 1.3.0
        "D001" | "D002" | "D003" | "D004" => PySparkVersion::new(1, 3, 0),

        // F001/F003/F007 — available since PySpark 1.3.0
        "F001" | "F003" | "F007" => PySparkVersion::new(1, 3, 0),

        // F002 — available since PySpark 1.4.0
        "F002" => PySparkVersion::new(1, 4, 0),

        // F004/F019 — available since PySpark 2.0.0
        "F004" | "F019" => PySparkVersion::new(2, 0, 0),

        // F014 — available since PySpark 2.3.0
        "F014" => PySparkVersion::new(2, 3, 0),

        // F009/F010 — available since PySpark 1.4.0
        "F009" | "F010" => PySparkVersion::new(1, 4, 0),

        // F017/F018 — available since PySpark 1.5.0
        "F017" | "F018" => PySparkVersion::new(1, 5, 0),

        // F012/F015/F020 — available since PySpark 1.3.0
        "F012" | "F015" | "F020" => PySparkVersion::new(1, 3, 0),

        // F008/F011/F013/F016 — available since PySpark 1.0.0
        "F008" | "F011" | "F013" | "F016" => PySparkVersion::new(1, 0, 0),

        // L001/PERF003/PERF006/S008 — available since PySpark 2.3.0
        "L001" | "PERF003" | "PERF006" | "S008" => PySparkVersion::new(2, 3, 0),

        // PERF008 — available since PySpark 2.0.0
        "PERF008" => PySparkVersion::new(2, 0, 0),

        // S010 — available since PySpark 2.1.0
        "S010" => PySparkVersion::new(2, 1, 0),

        // S013 — available since PySpark 1.6.0
        "S013" => PySparkVersion::new(1, 6, 0),

        // S001/S003/S007/S014 — available since PySpark 1.4.0
        "S001" | "S003" | "S007" | "S014" => PySparkVersion::new(1, 4, 0),

        // L003/PERF001/PERF004/PERF005/PERF007/S002/S004/S005/S006/S011/S012/S015 — available since PySpark 1.3.0
        "L003" | "PERF001" | "PERF004" | "PERF005" | "PERF007" | "S002" | "S004" | "S005" | "S006" | "S011"
        | "S012" | "S015" => PySparkVersion::new(1, 3, 0),

        // L002/S009 — available since PySpark 1.0.0
        "L002" | "S009" => PySparkVersion::new(1, 0, 0),

        // U001/U002/U003/U004 — available since PySpark 1.3.0
        "U001" | "U002" | "U003" | "U004" => PySparkVersion::new(1, 3, 0),

        _ => PySparkVersion::new(3, 0, 0),
    }
}

/// Return a short human-readable title for the given rule `id`.
pub fn rule_title(id: &str) -> &'static str {
    match id {
        "ARR001" => "Avoid array_distinct(collect_list()); use collect_set() instead",
        "ARR002" => "Avoid array_except(col, None/lit(None)); use array_compact() instead",
        "ARR003" => "Avoid array_distinct(collect_set()); collect_set() already returns distinct values",
        "ARR004" => "Avoid size(collect_set()) inside .agg(); use count_distinct() instead",
        "ARR005" => "Avoid size(collect_list()) inside .agg(); use count() instead",
        "ARR006" => "Avoid size(collect_list().over(w)); use count().over(w) instead",
        "D001" => "Avoid using collect()",
        "D002" => "Avoid accessing .rdd",
        "D003" => "Avoid .show() in production",
        "D004" => "Avoid .count() on large DataFrames",
        "D005" => "Avoid .rdd.isEmpty(); use .isEmpty() directly",
        "D006" => "Avoid df.count() == 0; use .isEmpty()",
        "D007" => "Avoid .filter(...).count() == 0; use .filter(...).isEmpty()",
        "D008" => "Avoid .display() in production",
        "D009" => "Avoid .count() as a boolean; use .isEmpty()",
        "F001" => "Avoid chaining withColumn() and withColumnRenamed()",
        "F002" => "Avoid drop(); use select() for explicit columns",
        "F003" => "Avoid selectExpr(); prefer select() with col()",
        "F004" => "Avoid spark.sql(); prefer native DataFrame API",
        "F005" => "Avoid stacking multiple withColumn() calls; use withColumns()",
        "F006" => "Avoid stacking multiple withColumnRenamed(); use withColumnsRenamed()",
        "F007" => "Prefer filter() before select() for clarity",
        "F008" => "Avoid print(); prefer the logging module",
        "F009" => "Avoid nested when(); use stacked .when().when().otherwise()",
        "F010" => "Always include otherwise() at the end of a when() chain",
        "F011" => "Avoid backslash line continuation; use parentheses",
        "F012" => "Always wrap literal values with lit()",
        "F013" => "Avoid reserved column names with __ prefix and __ suffix",
        "F014" => "Avoid explode_outer(); handle nulls with higher-order functions",
        "F015" => "Avoid multiple consecutive filter() calls; combine conditions into one",
        "F016" => "Avoid long DataFrame renaming chains; overwrite the same variable instead",
        "F017" => "Avoid expr(); use native PySpark functions instead",
        "F018" => "Use Spark native datetime functions instead of Python datetime objects",
        "F019" => "Avoid inferSchema=True or mergeSchema=True; define an explicit schema",
        "F020" => "Avoid select(\"*\"); use explicit column names",
        "L001" => "Avoid looping without .localCheckpoint() or .checkpoint()",
        "L002" => "Avoid while loops with DataFrames",
        "L003" => "Avoid calling withColumn() inside a loop",
        "P001" => ".toPandas() without enabling Arrow optimization",
        "S001" => "Missing .coalesce() after .union() / .unionByName()",
        "S002" => "Join without a broadcast or merge hint",
        "S003" => ".groupBy() directly followed by .distinct() or .dropDuplicates()",
        "S014" => "Avoid .distinct() or .dropDuplicates() before .groupBy(); the dedup is redundant",
        "S015" => "Avoid first() or last() in .agg() without orderBy() after .agg(); result is non-deterministic",
        "S004" => "Too many .distinct() operations in one file",
        "S005" => ".repartition() with fewer partitions than the Spark default",
        "S006" => ".repartition() with more partitions than the Spark default",
        "S007" => "Avoid repartition(1) or coalesce(1)",
        "S008" => "Overusing explode() / explode_outer()",
        "S009" => "Prefer mapPartitions() over map() for row-level transforms",
        "S010" => "Avoid crossJoin(); produces a Cartesian product",
        "S011" => "Join without join conditions causes a nested-loop scan",
        "S012" => "Avoid inner join followed by filter; prefer leftSemi join",
        "S013" => "Avoid reduceByKey(); use DataFrame groupBy().agg() instead",
        "PERF001" => "Avoid .rdd.collect(); use .toPandas() for driver-side consumption",
        "PERF002" => "Too many getOrCreate() calls; use getActiveSession() everywhere else",
        "PERF003" => "Too many shuffle operations without a checkpoint",
        "PERF004" => "Avoid bare .persist(); always pass an explicit StorageLevel",
        "PERF005" => "DataFrame persisted but never unpersisted",
        "PERF006" => "Avoid bare .checkpoint() / .localCheckpoint(); always pass an explicit eager argument",
        "PERF007" => "DataFrame used 2 or more times without caching",
        "PERF008" => "Avoid spark.read.csv(parallelize()); use spark.createDataFrame(pd.read_csv()) instead",
        "U001" => "Avoid UDFs that return StringType; use built-in string functions",
        "U002" => "Avoid UDFs that return ArrayType; use built-in array functions",
        "U003" => "Avoid UDFs; use Spark built-in functions instead",
        "U004" => "Avoid nested UDF calls; merge logic or use plain Python helpers",
        "U005" => "Avoid loops inside UDF bodies; use higher-order functions (transform, filter)",
        "U006" => "Avoid all() inside a UDF; use Spark's forall() instead",
        "U007" => "Avoid any() inside a UDF; use Spark's exists() instead",
        _ => "Unknown rule",
    }
}

/// Print a coloured summary table showing the count of HIGH / MEDIUM / LOW violations.
pub fn print_impact_summary(high: usize, medium: usize, low: usize) {
    let writer = BufferWriter::stderr(ColorChoice::Auto);
    let mut out = writer.buffer();

    writeln!(out, "\nImpact summary:").ok();

    out.set_color(ColorSpec::new().set_fg(Some(Color::Red)).set_bold(true))
        .ok();
    write!(out, "  HIGH").ok();
    out.reset().ok();
    writeln!(out, "   {high} error(s)").ok();

    out.set_color(ColorSpec::new().set_fg(Some(Color::Yellow)).set_bold(true))
        .ok();
    write!(out, "  MEDIUM").ok();
    out.reset().ok();
    writeln!(out, " {medium} error(s)").ok();

    out.set_color(ColorSpec::new().set_fg(Some(Color::Green)).set_bold(true))
        .ok();
    write!(out, "  LOW").ok();
    out.reset().ok();
    writeln!(out, "    {low} error(s)").ok();

    writer.print(&out).ok();
}
