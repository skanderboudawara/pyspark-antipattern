//! F019: Avoid `inferSchema=True` or `mergeSchema=True` in Spark read options.
//! These options trigger costly runtime schema inference; prefer explicit `StructType` schemas.
//   .csv(..., inferSchema=True)      (keyword argument on any read method)
//   .parquet(..., mergeSchema=True)  (keyword argument on any read method)
use rustpython_parser::ast::{Constant, Expr, Keyword, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::method_violation,
    violation::{RuleId, Severity, Violation},
    visitor::{Visitor, walk_expr},
};

const ID: &str = "F019";
const FLAGGED_OPTIONS: &[&str] = &["inferSchema", "mergeSchema"];

// ── Helpers ───────────────────────────────────────────────────────────────────

/// True if `expr` is the string literal "inferSchema" or "mergeSchema".
fn is_flagged_key(expr: &Expr) -> bool {
    if let Expr::Constant(c) = expr
        && let Constant::Str(s) = &c.value
    {
        return FLAGGED_OPTIONS.contains(&s.as_str());
    }
    false
}

/// True if `expr` is `True` (bool) or the string `"true"` (case-insensitive).
fn is_truthy(expr: &Expr) -> bool {
    if let Expr::Constant(c) = expr {
        return match &c.value {
            Constant::Bool(b) => *b,
            Constant::Str(s) => s.to_lowercase() == "true",
            _ => false,
        };
    }
    false
}

// ── Visitor ───────────────────────────────────────────────────────────────────

struct Check<'a> {
    source: &'a str,
    file: &'a str,
    index: &'a LineIndex,
    severity: Severity,
    violations: Vec<Violation>,
}

impl<'a> Check<'a> {
    /// Emit a violation pointing at the keyword name (e.g. `inferSchema=True`).
    fn flag_keyword(&mut self, kw: &Keyword) {
        let offset: u32 = kw.range.start().into();
        let (line, col) = self.index.line_col(offset, self.source);
        let source_line = self.index.line_text(self.source, line).to_string();
        let span_len = kw.arg.as_ref().map_or(1, |a| a.len());
        self.violations.push(Violation {
            rule_id: RuleId(ID.to_string()),
            severity: self.severity,
            impact: crate::violation::Impact::Low,
            file: self.file.to_string(),
            line,
            col,
            source_line,
            span_len,
        });
    }
}

impl<'a> Visitor for Check<'a> {
    fn visit_expr(&mut self, expr: &Expr) {
        if let Expr::Call(call) = expr
            && let Expr::Attribute(attr) = call.func.as_ref()
        {
            let method = attr.attr.as_str();

            // .option("inferSchema", "true") / .option("mergeSchema", True)
            if method == "option"
                && let (Some(key), Some(val)) = (call.args.first(), call.args.get(1))
                && is_flagged_key(key)
                && is_truthy(val)
            {
                self.violations.push(method_violation(
                    attr,
                    "option",
                    self.source,
                    self.file,
                    self.index,
                    self.severity,
                    ID,
                ));
            }

            // inferSchema=True / mergeSchema=True as keyword args
            for kw in &call.keywords {
                if let Some(arg_name) = &kw.arg
                    && FLAGGED_OPTIONS.contains(&arg_name.as_str())
                    && is_truthy(&kw.value)
                {
                    self.flag_keyword(kw);
                }
            }
        }
        walk_expr(self, expr);
    }
}

// ── Public entry point ────────────────────────────────────────────────────────

/// Scan `stmts` for `.option("inferSchema", ...)` / `.option("mergeSchema", ...)` calls and flag each.
pub fn check(stmts: &[Stmt], source: &str, file: &str, config: &Config, index: &LineIndex) -> Vec<Violation> {
    let mut v = Check {
        source,
        file,
        index,
        severity: config.severity_of(ID),
        violations: vec![],
    };
    for s in stmts {
        v.visit_stmt(s);
    }
    v.violations
}
