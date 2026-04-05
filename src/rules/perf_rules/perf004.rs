//! PERF004: `.persist()` called without an explicit `StorageLevel`.
//! Always pass a `StorageLevel` argument so the caching strategy is visible
//! in code review and tunable without guessing the default.
//
// Available levels:
//   StorageLevel.DISK_ONLY            StorageLevel.DISK_ONLY_2
//   StorageLevel.DISK_ONLY_3          StorageLevel.MEMORY_AND_DISK
//   StorageLevel.MEMORY_AND_DISK_2    StorageLevel.MEMORY_AND_DISK_DESER
//   StorageLevel.MEMORY_ONLY          StorageLevel.MEMORY_ONLY_2
//   StorageLevel.NONE                 StorageLevel.OFF_HEAP
use rustpython_parser::ast::{Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::method_violation,
    violation::Violation,
    visitor::{Visitor, walk_expr},
};

const ID: &str = "PERF004";

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
            && attr.attr.as_str() == "persist"
            && call.args.is_empty()
            && call.keywords.is_empty()
        {
            self.violations.push(method_violation(
                attr,
                "persist",
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

/// Scan `stmts` for `.persist()` calls with no arguments and flag each one.
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
