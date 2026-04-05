// PERF003: Too many shuffle operations without a checkpoint
//
// Fires when more than `max_shuffle_operations` shuffle-inducing calls occur
// between two checkpoint / localCheckpoint calls (or between scope entry and
// the first checkpoint).  Function call costs are propagated transitively: if
// a helper function internally performs N shuffles, every call to that helper
// contributes N to the caller's running counter.
use std::collections::HashMap;

use rustpython_parser::ast::{Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    spark_ops::{CHECKPOINT_OPS, SHUFFLE_OPS},
    violation::{RuleId, Severity, Violation},
    visitor::{walk_expr, walk_stmt, Visitor},
};

const ID: &str = "PERF003";

// ── Event model ──────────────────────────────────────────────────────────────

enum EventKind {
    /// A shuffle-inducing method call.  `op_len` is the length of the method
    /// name (used for caret span rendering).
    Shuffle { op_len: usize },
    /// A checkpoint or localCheckpoint call — resets the running counter.
    Checkpoint,
    /// A bare function call by name (no receiver dot), e.g. `process(df)`.
    /// The cost will be looked up in `fn_costs` at scan time.
    CallFn(String),
}

struct Event {
    /// Byte offset of the call / name start in the source file.  Used to sort
    /// events in left-to-right source order and to locate the violation.
    pos: u32,
    kind: EventKind,
}

// ── Event collector ──────────────────────────────────────────────────────────

/// Walks a single statement (without descending into nested `def` bodies) and
/// collects shuffle / checkpoint / plain-call events.
struct Collector {
    events: Vec<Event>,
}

impl Collector {
    fn new() -> Self {
        Self { events: vec![] }
    }

    /// Return events sorted by source position (left-to-right).
    fn into_sorted(mut self) -> Vec<Event> {
        self.events.sort_by_key(|e| e.pos);
        self.events
    }
}

/// Returns true when `expr` is a method call whose method name matches `method`.
/// Used to detect `groupBy().agg()` chains so we don't double-count the stage.
fn receiver_is_method(expr: &Expr, method: &str) -> bool {
    if let Expr::Call(c) = expr
        && let Expr::Attribute(a) = c.func.as_ref() {
            return a.attr.as_str() == method;
        }
    false
}

impl Visitor for Collector {
    /// Stop at nested function / async-function definitions — those scopes are
    /// analysed independently by `scan`.
    fn visit_stmt(&mut self, stmt: &Stmt) {
        match stmt {
            Stmt::FunctionDef(_) | Stmt::AsyncFunctionDef(_) => {}
            _ => walk_stmt(self, stmt),
        }
    }

    fn visit_expr(&mut self, expr: &Expr) {
        if let Expr::Call(call) = expr {
            match call.func.as_ref() {
                // Method call: `receiver.method(...)`
                Expr::Attribute(attr) => {
                    let name = attr.attr.as_str();
                    // Compute the byte start of the attribute name from its end.
                    let end: u32 = attr.range.end().into();
                    let pos = end.saturating_sub(name.len() as u32);

                    if SHUFFLE_OPS.contains(&name) {
                        // `groupBy().agg()` is one Spark stage, not two.
                        // Skip the `agg` event when its receiver is a `groupBy` call
                        // so the pair counts as a single shuffle operation.
                        let skip = name == "agg" && receiver_is_method(&attr.value, "groupBy");
                        if !skip {
                            self.events.push(Event {
                                pos,
                                kind: EventKind::Shuffle { op_len: name.len() },
                            });
                        }
                    } else if CHECKPOINT_OPS.contains(&name) {
                        self.events.push(Event { pos, kind: EventKind::Checkpoint });
                    }
                }
                // Bare name call: `process(df)`
                Expr::Name(n) => {
                    self.events.push(Event {
                        pos: n.range.start().into(),
                        kind: EventKind::CallFn(n.id.to_string()),
                    });
                }
                _ => {}
            }
        }
        walk_expr(self, expr);
    }
}

// ── Function cost computation ─────────────────────────────────────────────────

/// Compute the number of shuffle operations that "escape" from `body`
/// (counter value at the end of the body after resetting on checkpoints and
/// adding transitive fn_costs for called helpers).
fn body_export_cost(body: &[Stmt], fn_costs: &HashMap<String, usize>) -> usize {
    let mut counter: usize = 0;
    for stmt in body {
        let mut coll = Collector::new();
        coll.visit_stmt(stmt);
        for event in coll.into_sorted() {
            match event.kind {
                EventKind::Shuffle { .. } => counter += 1,
                EventKind::Checkpoint => counter = 0,
                EventKind::CallFn(name) => {
                    counter += fn_costs.get(&name).copied().unwrap_or(0);
                }
            }
        }
    }
    counter
}

/// Build a `function_name → export_shuffle_cost` map for all top-level
/// function definitions in `stmts`.  Uses iterative convergence (up to 10
/// rounds) to handle transitive / mutually-recursive calls.
///
/// `pub(crate)` so `checker.rs` can call this during the global pre-pass.
pub(crate) fn build_fn_costs(stmts: &[Stmt]) -> HashMap<String, usize> {
    let mut fn_bodies: Vec<(String, &[Stmt])> = vec![];
    for stmt in stmts {
        match stmt {
            Stmt::FunctionDef(f) => fn_bodies.push((f.name.to_string(), &f.body)),
            Stmt::AsyncFunctionDef(f) => fn_bodies.push((f.name.to_string(), &f.body)),
            _ => {}
        }
    }

    let mut fn_costs: HashMap<String, usize> = HashMap::new();
    for _ in 0..10 {
        let mut changed = false;
        for (name, body) in &fn_bodies {
            let new_cost = body_export_cost(body, &fn_costs);
            let old_cost = fn_costs.get(name.as_str()).copied().unwrap_or(0);
            if new_cost != old_cost {
                fn_costs.insert(name.clone(), new_cost);
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
    fn_costs
}

// ── Linear scanner ────────────────────────────────────────────────────────────

fn make_violation(
    pos: u32,
    span_len: usize,
    source: &str,
    file: &str,
    index: &LineIndex,
    severity: Severity,
) -> Violation {
    let (line, col) = index.line_col(pos);
    let source_line = index.line_text(source, line).to_string();
    Violation {
        rule_id: RuleId(ID.to_string()),
        severity,
        impact: crate::violation::Impact::Low,
        file: file.to_string(),
        line,
        col,
        source_line,
        span_len: span_len.max(1),
    }
}

/// Scan `stmts` linearly, maintaining a running shuffle counter.  Each
/// function-def body is scanned independently (its own counter starting at 0).
/// Bare function calls whose cost is known are added to the caller's counter.
fn scan(
    stmts: &[Stmt],
    fn_costs: &HashMap<String, usize>,
    threshold: usize,
    source: &str,
    file: &str,
    index: &LineIndex,
    severity: Severity,
    violations: &mut Vec<Violation>,
) {
    let mut counter: usize = 0;

    for stmt in stmts {
        // Function definitions → scan body independently with a fresh counter.
        match stmt {
            Stmt::FunctionDef(f) => {
                scan(&f.body, fn_costs, threshold, source, file, index, severity, violations);
                continue;
            }
            Stmt::AsyncFunctionDef(f) => {
                scan(&f.body, fn_costs, threshold, source, file, index, severity, violations);
                continue;
            }
            _ => {}
        }

        let mut coll = Collector::new();
        coll.visit_stmt(stmt);

        for event in coll.into_sorted() {
            match event.kind {
                EventKind::Shuffle { op_len } => {
                    counter += 1;
                    if counter > threshold {
                        violations.push(make_violation(
                            event.pos,
                            op_len + 2, // name + "()"
                            source,
                            file,
                            index,
                            severity,
                        ));
                        // Reset to avoid flooding on every subsequent op.
                        counter = 0;
                    }
                }
                EventKind::Checkpoint => {
                    counter = 0;
                }
                EventKind::CallFn(name) => {
                    let cost = fn_costs.get(&name).copied().unwrap_or(0);
                    if cost > 0 {
                        counter += cost;
                        if counter > threshold {
                            violations.push(make_violation(
                                event.pos,
                                name.len() + 2,
                                source,
                                file,
                                index,
                                severity,
                            ));
                            counter = 0;
                        }
                    }
                }
            }
        }
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
    let threshold = config.max_shuffle_operations;

    // Start with the project-wide costs (functions defined in other files),
    // then overlay with this file's own definitions (local wins on collision).
    let mut fn_costs = config.global_fn_costs.clone();
    fn_costs.extend(build_fn_costs(stmts));

    let mut violations = vec![];
    scan(stmts, &fn_costs, threshold, source, file, index, config.severity_of(ID), &mut violations);
    violations
}
