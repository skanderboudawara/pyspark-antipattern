// U004: Avoid nested UDF calls — calling one UDF from inside another UDF body.
//
// Each UDF boundary is opaque to Spark's optimizer and incurs Python
// serialisation overhead.  Nesting UDFs compounds both penalties: the outer
// UDF already de-opts the plan, and the inner call adds another round-trip
// through Python instead of being fused into the same lambda.
//
// Detection:
//   1. Collect the names of all top-level functions decorated with @udf or
//      @pandas_udf (the "UDF set").
//   2. For each UDF function body, flag every `Call` whose callee name is in
//      the UDF set.
use std::collections::HashSet;

use rustpython_parser::ast::{Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    violation::{RuleId, Severity, Violation},
    visitor::{walk_expr, Visitor},
};

const ID: &str = "U004";

fn is_udf_decorator(expr: &Expr) -> bool {
    match expr {
        Expr::Name(n) => matches!(n.id.as_str(), "udf" | "pandas_udf"),
        Expr::Attribute(a) => matches!(a.attr.as_str(), "udf" | "pandas_udf"),
        Expr::Call(c) => match c.func.as_ref() {
            Expr::Name(n) => matches!(n.id.as_str(), "udf" | "pandas_udf"),
            Expr::Attribute(a) => matches!(a.attr.as_str(), "udf" | "pandas_udf"),
            _ => false,
        },
        _ => false,
    }
}

// ── Inner call scanner ────────────────────────────────────────────────────────

struct CallScanner<'a> {
    udf_names: &'a HashSet<String>,
    source: &'a str,
    file: &'a str,
    index: &'a LineIndex,
    severity: Severity,
    violations: Vec<Violation>,
}

impl<'a> Visitor for CallScanner<'a> {
    fn visit_expr(&mut self, expr: &Expr) {
        if let Expr::Call(call) = expr {
            let callee_name = match call.func.as_ref() {
                Expr::Name(n) => Some(n.id.as_str()),
                _ => None,
            };
            if let Some(name) = callee_name
                && self.udf_names.contains(name) {
                    let start: u32 = call.range.start().into();
                    let (line, col) = self.index.line_col(start);
                    let source_line = self.index.line_text(self.source, line).to_string();
                    self.violations.push(Violation {
                        rule_id: RuleId(ID.to_string()),
                        severity: self.severity,
                        impact: crate::violation::Impact::Low,
                        file: self.file.to_string(),
                        line,
                        col,
                        source_line,
                        span_len: name.len() + 2,
                    });
                }
        }
        walk_expr(self, expr);
    }
}

// ── Public entry point ────────────────────────────────────────────────────────

pub fn check(
    stmts: &[Stmt],
    source: &str,
    file: &str,
    config: &Config,
    index: &LineIndex,
) -> Vec<Violation> {
    // Pass 1: collect UDF function names.
    let mut udf_names: HashSet<String> = HashSet::new();
    for stmt in stmts {
        let (name, decorators) = match stmt {
            Stmt::FunctionDef(f) => (f.name.as_str(), &f.decorator_list),
            Stmt::AsyncFunctionDef(f) => (f.name.as_str(), &f.decorator_list),
            _ => continue,
        };
        if decorators.iter().any(is_udf_decorator) {
            udf_names.insert(name.to_string());
        }
    }

    if udf_names.len() < 2 {
        return vec![]; // need at least two UDFs for nesting to be possible
    }

    // Pass 2: scan each UDF body for calls to other UDFs.
    let severity = config.severity_of(ID);
    let mut violations = vec![];

    for stmt in stmts {
        let (name, body, decorators) = match stmt {
            Stmt::FunctionDef(f) => (f.name.as_str(), &f.body, &f.decorator_list),
            Stmt::AsyncFunctionDef(f) => (f.name.as_str(), &f.body, &f.decorator_list),
            _ => continue,
        };
        if !decorators.iter().any(is_udf_decorator) {
            continue;
        }
        // Only flag calls to *other* UDFs, not self-recursion.
        let others: HashSet<String> = udf_names.iter()
            .filter(|n| n.as_str() != name)
            .cloned()
            .collect();
        let mut scanner = CallScanner {
            udf_names: &others,
            source, file, index, severity,
            violations: vec![],
        };
        for s in body {
            scanner.visit_stmt(s);
        }
        violations.extend(scanner.violations);
    }

    violations
}
