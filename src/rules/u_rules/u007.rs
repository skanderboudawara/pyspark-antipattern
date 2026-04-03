// U007: Avoid any() inside a UDF body — use pyspark.sql.functions.exists instead.
//
// Python's built-in any() used inside a UDF iterates over a collection entirely
// in Python, row by row, with no Spark optimisation:
//   - The array column must be deserialised from the JVM to Python
//   - any() runs in the Python interpreter with no vectorisation
//   - The boolean result is re-serialised back to the JVM
//   - Catalyst cannot see into the predicate or push it down
//
// pyspark.sql.functions.exists(col, predicate) evaluates the predicate over
// every array element using Spark's native execution engine — no UDF boundary,
// no serialisation round-trip, and the predicate is visible to the optimizer.
//
// Reference:
//   https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.exists.html
//
// Detection: any call to any(...) inside a @udf / @pandas_udf body.
// Recursion stops at nested function definitions.
use rustpython_parser::ast::{Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    violation::{RuleId, Severity, Violation},
    visitor::{walk_expr, walk_stmt, Visitor},
};

const ID: &str = "U007";

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

struct BodyScanner<'a> {
    source: &'a str,
    file: &'a str,
    index: &'a LineIndex,
    severity: Severity,
    violations: Vec<Violation>,
}

impl<'a> Visitor for BodyScanner<'a> {
    fn visit_stmt(&mut self, stmt: &Stmt) {
        match stmt {
            Stmt::FunctionDef(_) | Stmt::AsyncFunctionDef(_) => {} // stop at nested defs
            _ => walk_stmt(self, stmt),
        }
    }

    fn visit_expr(&mut self, expr: &Expr) {
        if let Expr::Call(call) = expr {
            let is_any = match call.func.as_ref() {
                Expr::Name(n) => n.id.as_str() == "any",
                _ => false,
            };
            if is_any {
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
                    span_len: "any".len() + 2,
                });
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
    let severity = config.severity_of(ID);
    let mut violations = vec![];

    for stmt in stmts {
        let (body, decorators) = match stmt {
            Stmt::FunctionDef(f) => (&f.body, &f.decorator_list),
            Stmt::AsyncFunctionDef(f) => (&f.body, &f.decorator_list),
            _ => continue,
        };
        if !decorators.iter().any(is_udf_decorator) {
            continue;
        }
        let mut scanner = BodyScanner { source, file, index, severity, violations: vec![] };
        for s in body {
            scanner.visit_stmt(s);
        }
        violations.extend(scanner.violations);
    }

    violations
}
