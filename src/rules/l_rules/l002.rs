// L002: Avoid while loops with DataFrames
use rustpython_parser::ast::{Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    violation::{RuleId, Violation},
    visitor::{walk_expr, walk_stmt, Visitor},
};

const ID: &str = "L002";
const WHILE_ASSUMED_ITERS: i64 = 99;

const DF_METHODS: &[&str] = &[
    "filter", "select", "withColumn", "groupBy", "agg", "join",
    "union", "unionByName", "orderBy", "sort", "distinct", "repartition",
    "collect", "count", "show", "write",
];

struct BodyScanner {
    has_df_op: bool,
}

impl Visitor for BodyScanner {
    fn visit_expr(&mut self, expr: &Expr) {
        if let Expr::Call(call) = expr {
            if let Expr::Attribute(attr) = call.func.as_ref() {
                if DF_METHODS.contains(&attr.attr.as_str()) {
                    self.has_df_op = true;
                }
            }
        }
        walk_expr(self, expr);
    }
}

struct Check<'a> {
    source: &'a str,
    file: &'a str,
    index: &'a LineIndex,
    severity: crate::violation::Severity,
    loop_threshold: i64,
    violations: Vec<Violation>,
}

impl<'a> Visitor for Check<'a> {
    fn visit_stmt(&mut self, stmt: &Stmt) {
        if let Stmt::While(w) = stmt {
            let mut scanner = BodyScanner { has_df_op: false };
            for s in &w.body { scanner.visit_stmt(s); }
            if scanner.has_df_op && WHILE_ASSUMED_ITERS > self.loop_threshold {
                let (line, col) = self.index.line_col(w.range.start().into());
                let source_line = self.index.line_text(self.source, line).to_string();
                self.violations.push(Violation {
                    rule_id: RuleId(ID.to_string()),
                    severity: self.severity,
                    impact: crate::violation::Impact::Low,
                    file: self.file.to_string(),
                    line, col,
                    source_line,
                    span_len: 5, // "while"
                });
            }
            // Recurse into body for nested loops
            for s in &w.body { self.visit_stmt(s); }
            for s in &w.orelse { self.visit_stmt(s); }
        } else {
            walk_stmt(self, stmt);
        }
    }
}

pub fn check(
    stmts: &[Stmt],
    source: &str,
    file: &str,
    config: &Config,
    index: &LineIndex,
) -> Vec<Violation> {
    let mut v = Check {
        source, file, index,
        severity: config.severity_of(ID),
        loop_threshold: config.loop_threshold as i64,
        violations: vec![],
    };
    for s in stmts { v.visit_stmt(s); }
    v.violations
}
