// L003: Avoid calling withColumn() inside a loop
use rustpython_parser::ast::{Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::{for_loop_iters, method_violation},
    violation::Violation,
    visitor::{Visitor, walk_expr, walk_stmt},
};

const ID: &str = "L003";

/// Scans a subtree for withColumn() calls and collects violations.
struct BodyScanner<'a> {
    source: &'a str,
    file: &'a str,
    index: &'a LineIndex,
    severity: crate::violation::Severity,
    violations: Vec<Violation>,
}

impl<'a> Visitor for BodyScanner<'a> {
    fn visit_expr(&mut self, expr: &Expr) {
        if let Expr::Call(call) = expr
            && let Expr::Attribute(attr) = call.func.as_ref()
            && attr.attr.as_str() == "withColumn"
        {
            self.violations.push(method_violation(
                attr,
                "withColumn",
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
    fn scan_body(&mut self, body: &[Stmt]) {
        let mut scanner = BodyScanner {
            source: self.source,
            file: self.file,
            index: self.index,
            severity: self.severity,
            violations: vec![],
        };
        for s in body {
            scanner.visit_stmt(s);
        }
        self.violations.extend(scanner.violations);
    }
}

impl<'a> Visitor for Check<'a> {
    fn visit_stmt(&mut self, stmt: &Stmt) {
        match stmt {
            Stmt::For(f) => {
                let iters = for_loop_iters(&f.iter).unwrap_or(i64::MAX);
                if iters > self.loop_threshold {
                    self.scan_body(&f.body);
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
                    self.scan_body(&w.body);
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
