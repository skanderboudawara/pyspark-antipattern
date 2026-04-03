// F018: Use Spark native datetime functions instead of Python datetime objects
//       inside Spark expressions.
//
// Python's `datetime`, `date`, `timedelta` objects are opaque to Spark's
// optimizer.  Passing them into `lit()`, `withColumn()`, `when()`, `filter()`,
// `where()`, or `otherwise()` forces driver-side evaluation and prevents
// partition pruning and predicate push-down.
//
// Prefer Spark built-in functions:
//   datetime.now()         → current_timestamp()
//   date.today()           → current_date()
//   datetime(y, m, d, ...) → to_timestamp(lit("yyyy-MM-dd HH:mm:ss"))
//   date(y, m, d)          → to_date(lit("yyyy-MM-dd"))
//   timedelta(days=N)      → date_add(col, N)  /  expr("interval N days")
//
// Detection: two passes.
//   Pass 1 — collect names imported from the `datetime` stdlib module.
//   Pass 2 — flag any Python datetime call found inside the argument(s) of
//             lit(), withColumn(), when(), filter(), where(), otherwise().
use std::collections::HashSet;

use rustpython_parser::ast::{Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::expr_start,
    violation::{RuleId, Severity, Violation},
    visitor::{walk_expr, Visitor},
};

const ID: &str = "F018";

// ── Pass 1 — collect imported datetime names ──────────────────────────────────

/// Return the set of local names imported from the `datetime` stdlib module.
///
/// Handles:
///   `from datetime import datetime`          → {"datetime"}
///   `from datetime import date, timedelta`   → {"date", "timedelta"}
///   `from datetime import datetime as dt`    → {"dt"}
///   `import datetime`                        → {"datetime"}
fn collect_datetime_names(stmts: &[Stmt]) -> HashSet<String> {
    let mut names = HashSet::new();
    for stmt in stmts {
        match stmt {
            Stmt::ImportFrom(imp) => {
                let module = imp.module.as_ref().map(|m| m.as_str()).unwrap_or("");
                if module == "datetime" {
                    for alias in &imp.names {
                        let local = alias.asname.as_ref()
                            .map(|a| a.to_string())
                            .unwrap_or_else(|| alias.name.to_string());
                        names.insert(local);
                    }
                }
            }
            Stmt::Import(imp) => {
                for alias in &imp.names {
                    if alias.name.as_str() == "datetime" {
                        let local = alias.asname.as_ref()
                            .map(|a| a.to_string())
                            .unwrap_or_else(|| "datetime".to_string());
                        names.insert(local);
                    }
                }
            }
            _ => {}
        }
    }
    names
}

// ── Datetime call detection ───────────────────────────────────────────────────

/// Return `true` if `expr` is a direct Python datetime call, i.e. one of:
///   - `datetime(...)` / `date(...)` / `timedelta(...)` (imported names as constructors)
///   - `datetime.now()` / `date.today()` / etc. (method on imported name)
///   - `datetime.datetime.now()` (method through module alias)
fn is_python_datetime_call(expr: &Expr, dt_names: &HashSet<String>) -> bool {
    let Expr::Call(call) = expr else { return false };
    match call.func.as_ref() {
        // datetime(...) / date(...) / timedelta(...) — direct constructor
        Expr::Name(n) => dt_names.contains(n.id.as_str()),
        Expr::Attribute(a) => {
            match a.value.as_ref() {
                // datetime.now() / date.today() / datetime.utcnow() …
                Expr::Name(n) => dt_names.contains(n.id.as_str()),
                // datetime.datetime.now() — module alias then class
                Expr::Attribute(inner) => {
                    if let Expr::Name(n) = inner.value.as_ref() {
                        dt_names.contains(n.id.as_str())
                    } else {
                        false
                    }
                }
                _ => false,
            }
        }
        _ => false,
    }
}

/// Recursively search `expr` for the first Python datetime call.
/// Returns a reference to that sub-expression so we can point the caret at it.
fn find_datetime_call<'e>(
    expr: &'e Expr,
    dt_names: &HashSet<String>,
) -> Option<&'e Expr> {
    if is_python_datetime_call(expr, dt_names) {
        return Some(expr);
    }
    match expr {
        Expr::Call(c) => {
            for arg in &c.args {
                if let Some(found) = find_datetime_call(arg, dt_names) {
                    return Some(found);
                }
            }
            for kw in &c.keywords {
                if let Some(found) = find_datetime_call(&kw.value, dt_names) {
                    return Some(found);
                }
            }
            None
        }
        Expr::BinOp(b) => {
            find_datetime_call(&b.left, dt_names)
                .or_else(|| find_datetime_call(&b.right, dt_names))
        }
        Expr::Compare(c) => {
            find_datetime_call(&c.left, dt_names).or_else(|| {
                c.comparators.iter().find_map(|cmp| find_datetime_call(cmp, dt_names))
            })
        }
        Expr::BoolOp(b) => b.values.iter().find_map(|v| find_datetime_call(v, dt_names)),
        Expr::UnaryOp(u) => find_datetime_call(&u.operand, dt_names),
        _ => None,
    }
}

// ── Visitor ───────────────────────────────────────────────────────────────────

struct Check<'a> {
    source: &'a str,
    file: &'a str,
    index: &'a LineIndex,
    severity: Severity,
    dt_names: &'a HashSet<String>,
    violations: Vec<Violation>,
}

impl<'a> Check<'a> {
    /// If `arg` contains a Python datetime call, emit a violation pointing at
    /// the datetime call itself.
    fn check_arg(&mut self, arg: &Expr) {
        if let Some(dt_expr) = find_datetime_call(arg, self.dt_names) {
            let offset = expr_start(dt_expr);
            let (line, col) = self.index.line_col(offset);
            let source_line = self.index.line_text(self.source, line).to_string();
            // Approximate span: up to the first `(` or end of name
            let span_len = match dt_expr {
                Expr::Call(c) => match c.func.as_ref() {
                    Expr::Name(n) => n.id.len() + 2,
                    Expr::Attribute(a) => a.attr.len() + 2,
                    _ => 3,
                },
                _ => 3,
            };
            self.violations.push(Violation {
                rule_id: RuleId(ID.to_string()),
                severity: self.severity,
                impact: crate::violation::Impact::Low,
                file: self.file.to_string(),
                line, col,
                source_line,
                span_len,
            });
        }
    }
}

impl<'a> Visitor for Check<'a> {
    fn visit_expr(&mut self, expr: &Expr) {
        if let Expr::Call(call) = expr {
            // Determine which Spark function this is.
            let func_name: Option<&str> = match call.func.as_ref() {
                Expr::Name(n)      => Some(n.id.as_str()),
                Expr::Attribute(a) => Some(a.attr.as_str()),
                _ => None,
            };

            match func_name {
                // lit(datetime_thing)
                Some("lit") => {
                    for arg in &call.args { self.check_arg(arg); }
                }
                // df.withColumn("name", datetime_thing)
                Some("withColumn") => {
                    if let Some(arg) = call.args.get(1) { self.check_arg(arg); }
                }
                // when(cond, datetime_thing)  or  .when(cond, datetime_thing)
                Some("when") => {
                    // condition (arg 0) and value (arg 1) both matter
                    for arg in &call.args { self.check_arg(arg); }
                }
                // .otherwise(datetime_thing)
                Some("otherwise") => {
                    if let Some(arg) = call.args.first() { self.check_arg(arg); }
                }
                // df.filter(col > datetime_thing)  /  df.where(...)
                Some("filter") | Some("where") => {
                    for arg in &call.args { self.check_arg(arg); }
                }
                // df.select(…, datetime_thing, …)
                Some("select") => {
                    for arg in &call.args { self.check_arg(arg); }
                }
                _ => {}
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
    let dt_names = collect_datetime_names(stmts);
    if dt_names.is_empty() {
        return vec![]; // no datetime imports → nothing to flag
    }

    let mut v = Check {
        source, file, index,
        severity: config.severity_of(ID),
        dt_names: &dt_names,
        violations: vec![],
    };
    for s in stmts { v.visit_stmt(s); }
    v.violations
}
