// PERF006: .checkpoint() or .localCheckpoint() called without an explicit eager argument.
//
// Both methods accept an `eager` boolean parameter that controls whether the
// checkpoint is materialised immediately (eager=True) or lazily on the first
// action after the call (eager=False).
//
// The defaults differ between the two methods:
//   .checkpoint()       defaults to eager=True  (immediate, blocks until done)
//   .localCheckpoint()  defaults to eager=True  (immediate, blocks until done)
//
// Leaving `eager` implicit means:
//   - The next developer has no idea whether the checkpoint will block or not
//   - Subtle performance differences between checkpointing strategies are invisible
//   - Changing the default in a future Spark version would silently alter behaviour
//
// Always pass `eager=True` or `eager=False` (or the positional equivalent)
// so the intent is explicit and visible in code review.
use rustpython_parser::ast::{Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::method_violation,
    violation::Violation,
    visitor::{walk_expr, Visitor},
};

const ID: &str = "PERF006";
const CHECKPOINT_METHODS: &[&str] = &["checkpoint", "localCheckpoint"];

struct Check<'a> {
    source: &'a str,
    file: &'a str,
    index: &'a LineIndex,
    severity: crate::violation::Severity,
    violations: Vec<Violation>,
}

impl<'a> Visitor for Check<'a> {
    fn visit_expr(&mut self, expr: &Expr) {
        if let Expr::Call(call) = expr
            && let Expr::Attribute(attr) = call.func.as_ref() {
                let method = attr.attr.as_str();
                if CHECKPOINT_METHODS.contains(&method)
                    && call.args.is_empty()
                    && call.keywords.is_empty()
                {
                    self.violations.push(method_violation(
                        attr, method, self.source, self.file,
                        self.index, self.severity, ID,
                    ));
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
    let mut v = Check {
        source, file, index,
        severity: config.severity_of(ID),
        violations: vec![],
    };
    for s in stmts { v.visit_stmt(s); }
    v.violations
}
