// ARR001: Avoid array_distinct(collect_list()) — use collect_set() instead.
// Detects two patterns:
//   1. array_distinct(collect_list(...)) — direct nesting in a single expression
//   2. withColumn("col", collect_list(...)) immediately followed by
//      withColumn("col", array_distinct(col("col"))) — split across two calls
use rustpython_parser::ast::{Constant, Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::expr_violation,
    violation::Violation,
    visitor::{walk_expr, Visitor},
};

const ID: &str = "ARR001";

// ── helpers ──────────────────────────────────────────────────────────────────

fn fn_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Name(n) => Some(n.id.as_str()),
        Expr::Attribute(a) => Some(a.attr.as_str()),
        _ => None,
    }
}

fn is_named(expr: &Expr, name: &str) -> bool {
    fn_name(expr) == Some(name)
}

/// Returns the string value if expr is a string constant, else None.
fn str_const(expr: &Expr) -> Option<&str> {
    if let Expr::Constant(c) = expr
        && let Constant::Str(s) = &c.value {
            return Some(s.as_str());
        }
    None
}

/// Returns the column name referenced by col("x") or "x" (bare string).
fn col_ref_name(expr: &Expr) -> Option<&str> {
    // col("x")
    if let Expr::Call(c) = expr
        && is_named(&c.func, "col")
            && let Some(arg) = c.args.first() {
                return str_const(arg);
            }
    // bare "x"
    str_const(expr)
}

/// Strips a trailing `.over(...)` window call, returning the inner expression.
/// `collect_list(...).over(w)` → the `collect_list(...)` Call.
fn unwrap_window(expr: &Expr) -> &Expr {
    if let Expr::Call(c) = expr
        && let Expr::Attribute(a) = c.func.as_ref()
            && a.attr.as_str() == "over" {
                return a.value.as_ref();
            }
    expr
}

// ── pattern 1: array_distinct(collect_list(...)) ─────────────────────────────

struct InlineCheck<'a> {
    source: &'a str,
    file: &'a str,
    index: &'a LineIndex,
    severity: crate::violation::Severity,
    violations: Vec<Violation>,
}

impl<'a> Visitor for InlineCheck<'a> {
    fn visit_expr(&mut self, expr: &Expr) {
        if let Expr::Call(outer) = expr
            && is_named(&outer.func, "array_distinct")
                && let Some(arg) = outer.args.first()
                    && let Expr::Call(inner) = unwrap_window(arg)
                        && is_named(&inner.func, "collect_list") {
                            self.violations.push(expr_violation(
                                expr,
                                "array_distinct".len(),
                                self.source, self.file, self.index,
                                self.severity, ID,
                            ));
                        }
        walk_expr(self, expr);
    }
}

// ── pattern 2: split withColumn form ─────────────────────────────────────────
// Walk the statement list looking for consecutive pairs:
//   withColumn("X", collect_list(...))  →  withColumn("X", array_distinct(col("X")))

fn collect_list_col_name(expr: &Expr) -> Option<&str> {
    // Matches any depth of chained calls whose outermost withColumn arg is collect_list
    if let Expr::Call(c) = expr
        && let Expr::Attribute(a) = c.func.as_ref() {
            if a.attr.as_str() == "withColumn" && c.args.len() >= 2
                && let Expr::Call(inner) = unwrap_window(&c.args[1])
                    && is_named(&inner.func, "collect_list") {
                        return str_const(&c.args[0]);
                    }
            // recurse into chained receiver
            return collect_list_col_name(a.value.as_ref());
        }
    None
}

fn is_array_distinct_of(expr: &Expr, col_name: &str) -> bool {
    if let Expr::Call(c) = expr
        && let Expr::Attribute(a) = c.func.as_ref() {
            if a.attr.as_str() == "withColumn" && c.args.len() >= 2
                && let Some(target) = str_const(&c.args[0])
                    && target == col_name
                        && let Expr::Call(inner) = &c.args[1]
                            && is_named(&inner.func, "array_distinct")
                                && let Some(arg) = inner.args.first()
                                    && let Some(ref_name) = col_ref_name(arg) {
                                        return ref_name == col_name;
                                    }
            return is_array_distinct_of(a.value.as_ref(), col_name);
        }
    false
}

fn scan_split_pattern(
    stmts: &[Stmt],
    source: &str,
    file: &str,
    index: &LineIndex,
    severity: crate::violation::Severity,
    violations: &mut Vec<Violation>,
) {
    // Collect (col_name, expr) for every statement that is an assignment or
    // expression whose value ends in a withColumn(collect_list) call.
    let mut prev: Option<(&str, &Expr)> = None;

    for stmt in stmts {
        let expr = match stmt {
            Stmt::Assign(a) if a.targets.len() == 1 => Some(a.value.as_ref()),
            Stmt::Expr(e) => Some(e.value.as_ref()),
            _ => { prev = None; continue; }
        };

        if let Some(e) = expr {
            // Check if this statement's expression contains the second pattern
            // given the previous statement set up a collect_list column.
            if let Some((prev_col, _)) = prev
                && is_array_distinct_of(e, prev_col) {
                    violations.push(expr_violation(
                        e,
                        "array_distinct".len(),
                        source, file, index, severity, ID,
                    ));
                    prev = None;
                    continue;
                }
            // Check if this statement sets up a collect_list column.
            if let Some(col_name) = collect_list_col_name(e) {
                // `col_name` is a &str pointing into a Constant::Str node inside `e`,
                // and `e` is borrowed from `stmt` inside `stmts`.  Both lifetimes are
                // bound to `stmts`, so storing them together in `prev` is sound.
                prev = Some((col_name, e));
            } else {
                prev = None;
            }
        }
    }
}

// ── public entry point ────────────────────────────────────────────────────────

pub fn check(
    stmts: &[Stmt],
    source: &str,
    file: &str,
    config: &Config,
    index: &LineIndex,
) -> Vec<Violation> {
    let severity = config.severity_of(ID);
    let mut violations = vec![];

    // Pattern 1: inline nesting via expression visitor
    let mut inline = InlineCheck { source, file, index, severity, violations: vec![] };
    for s in stmts { inline.visit_stmt(s); }
    violations.extend(inline.violations);

    // Pattern 2: split withColumn across consecutive statements
    scan_split_pattern(stmts, source, file, index, severity, &mut violations);

    violations
}
