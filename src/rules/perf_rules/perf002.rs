//! PERF002: More than one `getOrCreate()` call in a file — use `getActiveSession()`
//! everywhere except the initial session bootstrap.
use rustpython_parser::ast::{Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::method_violation,
    violation::Violation,
    visitor::{Visitor, walk_expr},
};

const ID: &str = "PERF002";

struct Check<'a> {
    source: &'a str,
    file: &'a str,
    index: &'a LineIndex,
    severity: crate::violation::Severity,
    occurrences: Vec<Violation>,
}

impl<'a> Visitor for Check<'a> {
    fn visit_expr(&mut self, expr: &Expr) {
        if let Expr::Call(call) = expr
            && let Expr::Attribute(attr) = call.func.as_ref()
            && attr.attr.as_str() == "getOrCreate"
        {
            self.occurrences.push(method_violation(
                attr,
                "getOrCreate",
                self.source,
                self.file,
                self.index,
                self.severity,
                ID,
            ));
        }
        walk_expr(self, expr);
    }
}

/// Scan `stmts` for multiple `getOrCreate()` calls and flag each occurrence beyond the first.
pub fn check(stmts: &[Stmt], source: &str, file: &str, config: &Config, index: &LineIndex) -> Vec<Violation> {
    let mut v = Check {
        source,
        file,
        index,
        severity: config.severity_of(ID),
        occurrences: vec![],
    };
    for s in stmts {
        v.visit_stmt(s);
    }

    // Only flag when there is more than one call in the file.
    if v.occurrences.len() > 1 { v.occurrences } else { vec![] }
}
