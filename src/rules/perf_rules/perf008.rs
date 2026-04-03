// PERF008: Avoid spark.read.csv(rdd) where rdd comes from sparkContext.parallelize().
//          Use spark.createDataFrame(pd.read_csv(...)) instead.
//
// The antipattern:
//   rdd = spark.sparkContext.parallelize(data.split("\n"))
//   df  = spark.read.csv(rdd, header=True, sep=';')
//
// This creates an RDD from an in-memory Python object just to hand it back to
// Spark's CSV reader, which then serialises, shuffles and deserialises the data
// through the full Spark serialisation pipeline for no reason.
//
// spark.createDataFrame(pd.read_csv(StringIO(data), sep=";", dtype="str"))
// parses the CSV entirely in the driver process via Pandas and creates a
// DataFrame directly — no RDD, no extra shuffle, no serialisation overhead.
//
// Detects two forms:
//   1. Inline:  spark.read.csv(spark.sparkContext.parallelize(...), ...)
//   2. Split:   rdd = ...parallelize(...)
//               spark.read.csv(rdd, ...)
use rustpython_parser::ast::{Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::expr_violation,
    violation::Violation,
    visitor::{walk_expr, Visitor},
};

const ID: &str = "PERF008";

/// Returns `true` if `expr` is (or ends in) a `.parallelize(...)` call.
fn is_parallelize_call(expr: &Expr) -> bool {
    if let Expr::Call(c) = expr {
        if let Expr::Attribute(a) = c.func.as_ref() {
            return a.attr.as_str() == "parallelize";
        }
    }
    false
}

/// Returns `true` if any node in `expr`'s call chain contains `.parallelize(...)`.
fn contains_parallelize(expr: &Expr) -> bool {
    if is_parallelize_call(expr) {
        return true;
    }
    if let Expr::Call(c) = expr {
        if let Expr::Attribute(a) = c.func.as_ref() {
            return contains_parallelize(a.value.as_ref());
        }
    }
    false
}

/// Collect every `Name` variable that is assigned from a `.parallelize(...)` call
/// anywhere in the top-level statement list.
fn collect_parallelize_vars(stmts: &[Stmt]) -> std::collections::HashSet<String> {
    let mut vars = std::collections::HashSet::new();
    for stmt in stmts {
        if let Stmt::Assign(a) = stmt {
            if a.targets.len() == 1 {
                if let Expr::Name(n) = &a.targets[0] {
                    if contains_parallelize(a.value.as_ref()) {
                        vars.insert(n.id.to_string());
                    }
                }
            }
        }
    }
    vars
}

struct Check<'a> {
    source: &'a str,
    file: &'a str,
    index: &'a LineIndex,
    severity: crate::violation::Severity,
    parallelize_vars: std::collections::HashSet<String>,
    violations: Vec<Violation>,
}

impl<'a> Visitor for Check<'a> {
    fn visit_expr(&mut self, expr: &Expr) {
        if let Expr::Call(c) = expr {
            if let Expr::Attribute(a) = c.func.as_ref() {
                if a.attr.as_str() == "csv" {
                    if let Some(first_arg) = c.args.first() {
                        let fires = is_parallelize_call(first_arg)
                            || matches!(first_arg, Expr::Name(n)
                                if self.parallelize_vars.contains(n.id.as_str()));
                        if fires {
                            self.violations.push(expr_violation(
                                expr,
                                "csv".len(),
                                self.source,
                                self.file,
                                self.index,
                                self.severity,
                                ID,
                            ));
                        }
                    }
                }
            }
        }
        walk_expr(self, expr);
    }
}

pub fn check(
    stmts: &[Stmt],
    source: &str,
    file: &str,
    config: &Config,
    index: &LineIndex,
) -> Vec<Violation> {
    let parallelize_vars = collect_parallelize_vars(stmts);
    let severity = config.severity_of(ID);
    let mut v = Check { source, file, index, severity, parallelize_vars, violations: vec![] };
    for s in stmts {
        v.visit_stmt(s);
    }
    v.violations
}
