use std::io::Write;
use termcolor::{BufferWriter, Color, ColorChoice, ColorSpec, WriteColor};

use crate::{
    config::Config,
    rule_content,
    violation::{Severity, Violation},
};

pub fn print_violations(violations: &[Violation], config: &Config) {
    let writer = BufferWriter::stderr(ColorChoice::Auto);
    let mut out = writer.buffer();

    // ── Pass 1: compact violation list ──────────────────────────────────────
    for v in violations {
        let (label, color) = match v.severity {
            Severity::Error => ("error", Color::Red),
            Severity::Warning => ("warning", Color::Yellow),
        };
        let title = rule_title(&v.rule_id.0);
        let gutter = v.line.to_string().len();

        // "error[D001]: Avoid using collect()"
        out.set_color(ColorSpec::new().set_fg(Some(color)).set_bold(true)).ok();
        write!(out, "{label}").ok();
        out.set_color(ColorSpec::new().set_bold(true)).ok();
        write!(out, "[{}]", v.rule_id).ok();
        out.reset().ok();
        writeln!(out, ": {title}").ok();

        // "  --> file.py:42:10"
        out.set_color(ColorSpec::new().set_fg(Some(Color::Cyan)).set_bold(true)).ok();
        write!(out, "  --> ").ok();
        out.reset().ok();
        writeln!(out, "{}:{}:{}", v.file, v.line, v.col).ok();

        // gutter + source line + carets
        writeln!(out, "{} |", " ".repeat(gutter)).ok();
        out.set_color(ColorSpec::new().set_fg(Some(Color::Cyan)).set_bold(true)).ok();
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

pub fn rule_title(id: &str) -> &'static str {
    match id {
        "ARR001" => "Avoid array_distinct(collect_list()); use collect_set() instead",
        "ARR002" => "Avoid array_except(col, None/lit(None)); use array_compact() instead",
        "D001" => "Avoid using collect()",
        "D002" => "Avoid accessing .rdd",
        "D003" => "Avoid .show() in production",
        "D004" => "Avoid .count() on large DataFrames",
        "D005" => "Avoid .rdd.isEmpty(); use .isEmpty() directly",
        "D006" => "Avoid df.count() == 0; use .isEmpty()",
        "D007" => "Avoid .filter(...).count() == 0; use .filter(...).isEmpty()",
        "D008" => "Avoid .display() in production",
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
        "L001" => "Avoid looping without .localCheckpoint() or .checkpoint()",
        "L002" => "Avoid while loops with DataFrames",
        "L003" => "Avoid calling withColumn() inside a loop",
        "P001" => ".toPandas() without enabling Arrow optimization",
        "S001" => "Missing .coalesce() after .union() / .unionByName()",
        "S002" => "Join without a broadcast or merge hint",
        "S003" => ".groupBy() directly followed by .distinct()",
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
        "U001" => "Avoid UDFs that return StringType; use built-in string functions",
        "U002" => "Avoid UDFs that return ArrayType; use built-in array functions",
        "U003" => "Avoid UDFs; use Spark built-in functions instead",
        _ => "Unknown rule",
    }
}
