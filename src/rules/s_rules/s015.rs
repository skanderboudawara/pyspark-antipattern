//! S015: `first()` or `last()` inside `.agg()` without an `orderBy()` after
//! the `.agg()` — the result is non-deterministic because `groupBy()` shuffles
//! rows into an undefined order across partitions.
use std::collections::HashSet;

use rustpython_parser::ast::{Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::{chain_has_method, method_violation},
    violation::Violation,
    visitor::{Visitor, walk_expr},
};

const ID: &str = "S015";

fn is_named(expr: &Expr, name: &str) -> bool {
    match expr {
        Expr::Name(n) => n.id.as_str() == name,
        Expr::Attribute(a) => a.attr.as_str() == name,
        _ => false,
    }
}

/// Strip a trailing `.alias(...)` call, returning the inner expression.
fn strip_alias(expr: &Expr) -> &Expr {
    if let Expr::Call(c) = expr
        && let Expr::Attribute(a) = c.func.as_ref()
        && a.attr.as_str() == "alias"
    {
        return a.value.as_ref();
    }
    expr
}

/// Returns `true` when `expr` is a call to `first(...)` or `last(...)`,
/// with or without module qualification (e.g. `F.first(...)`).
fn is_first_or_last(expr: &Expr) -> bool {
    if let Expr::Call(call) = expr {
        return is_named(&call.func, "first") || is_named(&call.func, "last");
    }
    false
}

// ── Pass 1: mark .agg() calls that are followed by an ordering step ─────────

/// When an ordering method (`orderBy`, `sort`, `sortWithinPartitions`) appears
/// in the chain *above* an `.agg()` call, record that `.agg()` call's byte
/// offset so pass 2 can skip it.
fn mark_safe_aggs(expr: &Expr, safe: &mut HashSet<u32>) {
    if let Expr::Call(call) = expr {
        if let Expr::Attribute(attr) = call.func.as_ref() {
            let name = attr.attr.as_str();
            if matches!(name, "orderBy" | "sort" | "sortWithinPartitions") {
                // Walk the inner chain to find the .agg() call below this ordering step.
                find_agg_offset(attr.value.as_ref(), safe);
            }
            mark_safe_aggs(attr.value.as_ref(), safe);
        }
        for arg in &call.args {
            mark_safe_aggs(arg, safe);
        }
    }
}

/// Walk the chain downward from `expr` looking for the first `.agg()` call and
/// record its byte offset.
fn find_agg_offset(expr: &Expr, set: &mut HashSet<u32>) {
    if let Expr::Call(call) = expr
        && let Expr::Attribute(attr) = call.func.as_ref()
    {
        if attr.attr.as_str() == "agg" {
            set.insert(call.range.start().into());
            return;
        }
        find_agg_offset(attr.value.as_ref(), set);
    }
}

// ── Pass 2: flag unsafe .agg() calls ────────────────────────────────────────

struct Check<'a> {
    source: &'a str,
    file: &'a str,
    index: &'a LineIndex,
    severity: crate::violation::Severity,
    violations: Vec<Violation>,
    safe_aggs: HashSet<u32>,
}

impl<'a> Visitor for Check<'a> {
    fn visit_expr(&mut self, expr: &Expr) {
        if let Expr::Call(call) = expr
            && let Expr::Attribute(attr) = call.func.as_ref()
            && attr.attr.as_str() == "agg"
        {
            // Only flag when the chain contains groupBy.
            if !chain_has_method(attr.value.as_ref(), "groupBy") {
                walk_expr(self, expr);
                return;
            }

            // Skip if an ordering step follows this .agg() call in the chain.
            let offset: u32 = call.range.start().into();
            if self.safe_aggs.contains(&offset) {
                walk_expr(self, expr);
                return;
            }

            // Check whether any argument to .agg() is first() or last().
            let exprs = call.args.iter().chain(call.keywords.iter().map(|kw| &kw.value));
            for arg in exprs {
                let inner = strip_alias(arg);
                if is_first_or_last(inner) {
                    self.violations.push(method_violation(
                        attr,
                        "agg",
                        self.source,
                        self.file,
                        self.index,
                        self.severity,
                        ID,
                    ));
                    break; // one violation per .agg() call
                }
            }
        }
        walk_expr(self, expr);
    }
}

/// Scan `stmts` for `first()` / `last()` inside `.groupBy().agg()` without an
/// `orderBy()` / `sort()` / `sortWithinPartitions()` **after** the `.agg()`.
pub fn check(stmts: &[Stmt], source: &str, file: &str, config: &Config, index: &LineIndex) -> Vec<Violation> {
    // Pass 1: collect .agg() offsets that have ordering applied after them.
    let mut safe_aggs: HashSet<u32> = HashSet::new();
    struct FirstPass<'a> {
        safe: &'a mut HashSet<u32>,
    }
    impl<'a> Visitor for FirstPass<'a> {
        fn visit_expr(&mut self, expr: &Expr) {
            mark_safe_aggs(expr, self.safe);
            walk_expr(self, expr);
        }
    }
    let mut fp = FirstPass { safe: &mut safe_aggs };
    for s in stmts {
        fp.visit_stmt(s);
    }

    // Pass 2: flag remaining .agg() calls with first()/last().
    let mut v = Check {
        source,
        file,
        index,
        severity: config.severity_of(ID),
        violations: vec![],
        safe_aggs,
    };
    for s in stmts {
        v.visit_stmt(s);
    }
    v.violations
}
