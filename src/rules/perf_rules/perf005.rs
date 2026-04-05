// PERF005: DataFrame persisted but never unpersisted.
//
// Every .persist() call pins data in memory (and/or disk) for the lifetime of
// the Spark session.  Forgetting to call .unpersist() causes:
//   - Memory pressure that grows with each job run
//   - Eviction of other cached data, triggering expensive recomputation
//   - OOM errors in long-running applications
//
// Detection (per-scope analysis — top-level and each function body separately):
//   1. Collect every assignment of the form  `name = ....persist(...)`
//   2. Collect every `.unpersist()` call and extract the receiver variable name
//   3. Flag any variable from step 1 that never appears in step 2
//
// Scoping: nested function definitions are treated as independent scopes so
// that a variable in an inner function does not incorrectly shadow an outer one.
use std::collections::{HashMap, HashSet};

use rustpython_parser::ast::{Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::expr_start,
    violation::{RuleId, Severity, Violation},
    visitor::{Visitor, walk_expr, walk_stmt},
};

const ID: &str = "PERF005";

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Walk down a call/attribute chain and return the leftmost variable name.
/// `df.filter(...).persist()` → "df"
fn root_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Name(n) => Some(n.id.as_str()),
        Expr::Call(c) => {
            if let Expr::Attribute(a) = c.func.as_ref() {
                root_name(a.value.as_ref())
            } else {
                None
            }
        }
        Expr::Attribute(a) => root_name(a.value.as_ref()),
        _ => None,
    }
}

/// True if `expr` is a `.persist(...)` call (any arguments).
fn is_persist_call(expr: &Expr) -> bool {
    if let Expr::Call(c) = expr
        && let Expr::Attribute(a) = c.func.as_ref()
    {
        return a.attr.as_str() == "persist";
    }
    false
}

// ── Per-scope info ────────────────────────────────────────────────────────────

struct PersistInfo {
    var_name: String,
    line: usize,
    col: usize,
    source_line: String,
}

// ── Scope collector ───────────────────────────────────────────────────────────

struct ScopeCollector<'a> {
    source: &'a str,
    index: &'a LineIndex,
    /// Map var_name → last PersistInfo (later persist overwrites earlier one).
    persisted: HashMap<String, PersistInfo>,
    /// Variables on which .unpersist() was called.
    unpersisted: HashSet<String>,
}

impl<'a> ScopeCollector<'a> {
    fn new(source: &'a str, index: &'a LineIndex) -> Self {
        Self {
            source,
            index,
            persisted: HashMap::new(),
            unpersisted: HashSet::new(),
        }
    }
}

impl<'a> Visitor for ScopeCollector<'a> {
    fn visit_stmt(&mut self, stmt: &Stmt) {
        // Stop at nested function defs — they are analysed as separate scopes.
        if matches!(stmt, Stmt::FunctionDef(_) | Stmt::AsyncFunctionDef(_)) {
            return;
        }

        // Detect:  name = ....persist(...)
        if let Stmt::Assign(a) = stmt
            && is_persist_call(&a.value)
            && let Some(target) = a.targets.first()
            && let Expr::Name(n) = target
        {
            // Point violation at the `persist` method name.
            let (line, col) = if let Expr::Call(c) = a.value.as_ref() {
                if let Expr::Attribute(attr) = c.func.as_ref() {
                    let end: u32 = attr.range.end().into();
                    let s = end.saturating_sub("persist".len() as u32);
                    self.index.line_col(s, self.source)
                } else {
                    self.index.line_col(expr_start(&a.value), self.source)
                }
            } else {
                self.index.line_col(expr_start(&a.value), self.source)
            };
            let source_line = self.index.line_text(self.source, line).to_string();
            self.persisted.insert(
                n.id.to_string(),
                PersistInfo {
                    var_name: n.id.to_string(),
                    line,
                    col,
                    source_line,
                },
            );
        }

        walk_stmt(self, stmt);
    }

    fn visit_expr(&mut self, expr: &Expr) {
        // Detect:  name.unpersist()   (anywhere in the expression tree)
        if let Expr::Call(call) = expr
            && let Expr::Attribute(attr) = call.func.as_ref()
            && attr.attr.as_str() == "unpersist"
            && let Some(name) = root_name(attr.value.as_ref())
        {
            self.unpersisted.insert(name.to_string());
        }
        walk_expr(self, expr);
    }
}

// ── Per-scope analysis ────────────────────────────────────────────────────────

fn check_scope(stmts: &[Stmt], source: &str, file: &str, severity: Severity, index: &LineIndex) -> Vec<Violation> {
    let mut collector = ScopeCollector::new(source, index);
    for s in stmts {
        collector.visit_stmt(s);
    }

    let mut violations: Vec<Violation> = collector
        .persisted
        .into_values()
        .filter(|p| !collector.unpersisted.contains(&p.var_name))
        .map(|p| Violation {
            rule_id: RuleId(ID.to_string()),
            severity,
            impact: crate::violation::Impact::Low,
            file: file.to_string(),
            line: p.line,
            col: p.col,
            source_line: p.source_line,
            span_len: "persist".len() + 2,
        })
        .collect();

    // Sort by line so output is deterministic.
    violations.sort_by_key(|v| v.line);
    violations
}

// ── Public entry point ────────────────────────────────────────────────────────

pub fn check(stmts: &[Stmt], source: &str, file: &str, config: &Config, index: &LineIndex) -> Vec<Violation> {
    let severity = config.severity_of(ID);
    let mut violations = vec![];

    // Top-level scope.
    violations.extend(check_scope(stmts, source, file, severity, index));

    // Each top-level function body as its own scope.
    for stmt in stmts {
        let body = match stmt {
            Stmt::FunctionDef(f) => &f.body,
            Stmt::AsyncFunctionDef(f) => &f.body,
            _ => continue,
        };
        violations.extend(check_scope(body, source, file, severity, index));
    }

    violations
}
