//! S006: `.repartition()` with more partitions than the Spark default (200) —
//! excessive partition counts increase task scheduling overhead and small-file pressure.
use rustpython_parser::ast::{Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::{const_int, method_violation},
    violation::Violation,
    visitor::{Visitor, walk_expr},
};

const ID: &str = "S006";
const SPARK_DEFAULT_PARTITIONS: i64 = 200;

struct Check<'a> {
    source: &'a str,
    file: &'a str,
    index: &'a LineIndex,
    severity: crate::violation::Severity,
    violations: Vec<Violation>,
}

impl<'a> Visitor for Check<'a> {
    fn visit_expr(&mut self, expr: &Expr) {
        if let Expr::Call(call) = expr
            && let Expr::Attribute(attr) = call.func.as_ref()
            && attr.attr.as_str() == "repartition"
        {
            // Accept both positional (first arg) and keyword `numPartitions=N` forms.
            let n = call.args.first().and_then(const_int).or_else(|| {
                call.keywords
                    .iter()
                    .find(|kw| kw.arg.as_deref() == Some("numPartitions"))
                    .and_then(|kw| const_int(&kw.value))
            });
            if let Some(n) = n
                && n > SPARK_DEFAULT_PARTITIONS
            {
                self.violations.push(method_violation(
                    attr,
                    "repartition",
                    self.source,
                    self.file,
                    self.index,
                    self.severity,
                    ID,
                ));
            }
        }
        walk_expr(self, expr);
    }
}

/// Scan `stmts` for `.repartition(N)` where N exceeds the configured threshold and flag each.
pub fn check(stmts: &[Stmt], source: &str, file: &str, config: &Config, index: &LineIndex) -> Vec<Violation> {
    let mut v = Check {
        source,
        file,
        index,
        severity: config.severity_of(ID),
        violations: vec![],
    };
    for s in stmts {
        v.visit_stmt(s);
    }
    v.violations
}
