// U005: Loops inside a UDF body — use pyspark.sql.functions.transform instead.
//
// UDFs already pay a heavy price: every row must be serialised from the JVM
// to Python and back.  Adding a loop (for-statement or comprehension) inside
// the UDF body means the Python interpreter iterates over the array element-
// by-element on the driver/executor before returning the result, rather than
// delegating the work to a vectorised Spark built-in.
//
// pyspark.sql.functions.transform() applies a lambda to each array element
// using Spark's native execution engine — no UDF boundary, no serialisation.
//
// Detection flags, inside any @udf / @pandas_udf decorated function body:
//   Stmt::For           — for x in array: ...
//   Expr::ListComp      — [f(x) for x in array]
//   Expr::SetComp       — {f(x) for x in array}
//   Expr::DictComp      — {k: v for k, v in pairs}
//   Expr::GeneratorExp  — (f(x) for x in array)
//
// Recursion stops at nested function definitions so that inner helper
// functions defined inside the UDF are not also scanned.
use rustpython_parser::ast::{Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::expr_violation,
    violation::{RuleId, Severity, Violation},
    visitor::{Visitor, walk_expr, walk_stmt},
};

const ID: &str = "U005";

// ── Decorator detection (mirrors U004) ───────────────────────────────────────

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

// ── Body scanner ─────────────────────────────────────────────────────────────

struct BodyScanner<'a> {
    source: &'a str,
    file: &'a str,
    index: &'a LineIndex,
    severity: Severity,
    violations: Vec<Violation>,
}

impl<'a> BodyScanner<'a> {
    fn flag_for(&mut self, stmt: &Stmt) {
        // Point at the `for` keyword — start of the statement range.
        let start: u32 = match stmt {
            Stmt::For(f) => f.range.start().into(),
            _ => return,
        };
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
            span_len: 3, // "for"
        });
    }
}

impl<'a> Visitor for BodyScanner<'a> {
    fn visit_stmt(&mut self, stmt: &Stmt) {
        match stmt {
            // Flag for-loops, then recurse to catch nested loops inside the body.
            Stmt::For(_) => {
                self.flag_for(stmt);
                walk_stmt(self, stmt);
            }
            // Stop at nested function defs — their bodies are not part of this UDF.
            Stmt::FunctionDef(_) | Stmt::AsyncFunctionDef(_) => {}
            _ => walk_stmt(self, stmt),
        }
    }

    fn visit_expr(&mut self, expr: &Expr) {
        match expr {
            Expr::ListComp(_) | Expr::SetComp(_) | Expr::DictComp(_) | Expr::GeneratorExp(_) => {
                self.violations.push(expr_violation(
                    expr,
                    1, // opening bracket: `[`, `{`, or `(`
                    self.source,
                    self.file,
                    self.index,
                    self.severity,
                    ID,
                ));
                // Still recurse in case there are nested comprehensions.
                walk_expr(self, expr);
            }
            _ => walk_expr(self, expr),
        }
    }
}

// ── Public entry point ────────────────────────────────────────────────────────

pub fn check(stmts: &[Stmt], source: &str, file: &str, config: &Config, index: &LineIndex) -> Vec<Violation> {
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
        let mut scanner = BodyScanner {
            source,
            file,
            index,
            severity,
            violations: vec![],
        };
        for s in body {
            scanner.visit_stmt(s);
        }
        violations.extend(scanner.violations);
    }

    violations
}
