// L001: Avoid looping without .localCheckpoint() or .checkpoint()
// Fires on For/While loops that contain DataFrame operations but no checkpoint call.
use rustpython_parser::ast::{Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::{chain_has_method, for_loop_iters},
    violation::{RuleId, Violation},
    visitor::{Visitor, walk_expr, walk_stmt},
};

const ID: &str = "L001";

/// DataFrame-like method names that indicate a loop body is operating on a DF.
const DF_METHODS: &[&str] = &[
    "filter",
    "select",
    "withColumn",
    "groupBy",
    "agg",
    "join",
    "union",
    "unionByName",
    "orderBy",
    "sort",
    "distinct",
    "repartition",
];

struct BodyScanner {
    has_df_op: bool,
    has_checkpoint: bool,
}

impl Visitor for BodyScanner {
    fn visit_expr(&mut self, expr: &Expr) {
        if let Expr::Call(call) = expr
            && let Expr::Attribute(attr) = call.func.as_ref()
        {
            let name = attr.attr.as_str();
            if name == "checkpoint" || name == "localCheckpoint" {
                self.has_checkpoint = true;
            }
            if DF_METHODS.contains(&name) {
                self.has_df_op = true;
            }
        }
        walk_expr(self, expr);
    }
}

const WHILE_ASSUMED_ITERS: i64 = 99;

struct Check<'a> {
    source: &'a str,
    file: &'a str,
    index: &'a LineIndex,
    severity: crate::violation::Severity,
    loop_threshold: i64,
    violations: Vec<Violation>,
}

impl<'a> Check<'a> {
    fn scan_loop_body(&mut self, body: &[Stmt], loop_range_start: u32) {
        let mut scanner = BodyScanner {
            has_df_op: false,
            has_checkpoint: false,
        };
        for s in body {
            scanner.visit_stmt(s);
        }
        if scanner.has_df_op && !scanner.has_checkpoint {
            let (line, col) = self.index.line_col(loop_range_start, self.source);
            let source_line = self.index.line_text(self.source, line).to_string();
            self.violations.push(Violation {
                rule_id: RuleId(ID.to_string()),
                severity: self.severity,
                impact: crate::violation::Impact::Low,
                file: self.file.to_string(),
                line,
                col,
                source_line,
                span_len: 3, // "for" / "while"
            });
        }
    }
}

impl<'a> Visitor for Check<'a> {
    fn visit_stmt(&mut self, stmt: &Stmt) {
        match stmt {
            Stmt::For(f) => {
                let iters = for_loop_iters(&f.iter).unwrap_or(i64::MAX);
                if iters > self.loop_threshold {
                    self.scan_loop_body(&f.body, f.range.start().into());
                }
                for s in &f.body {
                    self.visit_stmt(s);
                }
                for s in &f.orelse {
                    self.visit_stmt(s);
                }
            }
            Stmt::While(w) => {
                if WHILE_ASSUMED_ITERS > self.loop_threshold {
                    self.scan_loop_body(&w.body, w.range.start().into());
                }
                for s in &w.body {
                    self.visit_stmt(s);
                }
                for s in &w.orelse {
                    self.visit_stmt(s);
                }
            }
            _ => walk_stmt(self, stmt),
        }
    }
}

pub fn check(stmts: &[Stmt], source: &str, file: &str, config: &Config, index: &LineIndex) -> Vec<Violation> {
    let _ = chain_has_method; // used in other rules, suppress warning
    let mut v = Check {
        source,
        file,
        index,
        severity: config.severity_of(ID),
        loop_threshold: config.loop_threshold as i64,
        violations: vec![],
    };
    for s in stmts {
        v.visit_stmt(s);
    }
    v.violations
}
