// F016: Avoid long DataFrame renaming chains — more than 2 consecutive renames.
// Fires when: a = x.m(), b = a.m(), c = b.m()  (third rename and beyond)
//
// Only DataFrames are tracked: a chain is started only when the RHS contains at
// least one known Spark DataFrame method.  Plain dict/str/list calls (e.g.
// `tokens.get(...)`, `path.split(...)`) are never counted.
use rustpython_parser::ast::{Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::is_non_dataframe_receiver,
    spark_ops::DATAFRAME_METHODS,
    violation::{RuleId, Severity, Violation},
};

const ID: &str = "F016";

/// Walk down a method-call chain and return the root variable name, if any.
/// e.g.  df.filter(...).distinct()  →  "df"
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

/// Return true if `expr` (an assignment RHS) contains at least one Spark
/// DataFrame method anywhere in its method-call chain, on a non-stdlib receiver.
fn has_spark_method(expr: &Expr) -> bool {
    match expr {
        Expr::Call(c) => {
            if let Expr::Attribute(a) = c.func.as_ref() {
                if DATAFRAME_METHODS.contains(&a.attr.as_str())
                    && !is_non_dataframe_receiver(a.value.as_ref())
                {
                    return true;
                }
                return has_spark_method(a.value.as_ref());
            }
            false
        }
        Expr::Attribute(a) => has_spark_method(a.value.as_ref()),
        _ => false,
    }
}

/// Scan a flat list of statements and return violations.
/// `renamed_from` maps each variable name to the variable it was renamed from;
/// `chain_depth` maps each variable name to how many rename steps it is away
/// from the original.
fn scan_stmts(
    stmts: &[Stmt],
    source: &str,
    file: &str,
    index: &LineIndex,
    severity: Severity,
    renamed_from: &mut std::collections::HashMap<String, String>,
    chain_depth: &mut std::collections::HashMap<String, usize>,
    violations: &mut Vec<Violation>,
) {
    for stmt in stmts {
        match stmt {
            Stmt::Assign(a) if a.targets.len() == 1 => {
                if let Expr::Name(target) = &a.targets[0] {
                    let target_name = target.id.as_str();
                    if let Some(src) = root_name(&a.value) {
                        // Only count as a rename when target != source
                        if src != target_name {
                            let src_depth = chain_depth.get(src).copied().unwrap_or(0);
                            // Only start a new chain when the RHS has a Spark method;
                            // continue an existing DataFrame chain unconditionally.
                            if src_depth == 0 && !has_spark_method(&a.value) {
                                continue;
                            }
                            let depth = src_depth + 1;
                            chain_depth.insert(target_name.to_string(), depth);
                            renamed_from.insert(target_name.to_string(), src.to_string());

                            if depth > 2 {
                                let start: u32 = target.range.start().into();
                                let (line, col) = index.line_col(start);
                                let source_line = index.line_text(source, line).to_string();
                                violations.push(Violation {
                                    rule_id: RuleId(ID.to_string()),
                                    severity,
                                    file: file.to_string(),
                                    line,
                                    col,
                                    source_line,
                                    span_len: target_name.len(),
                                });
                            }
                        }
                    }
                }
                // recurse into sub-statements inside the RHS (e.g. lambda bodies)
                // — not needed since assignment RHS is an expression, handled above
            }
            // Recurse into nested scopes with a fresh chain context so that
            // function-local variables don't bleed into the outer scope.
            Stmt::FunctionDef(f) => {
                scan_stmts(
                    &f.body, source, file, index, severity,
                    &mut Default::default(), &mut Default::default(), violations,
                );
            }
            Stmt::AsyncFunctionDef(f) => {
                scan_stmts(
                    &f.body, source, file, index, severity,
                    &mut Default::default(), &mut Default::default(), violations,
                );
            }
            // For if/for/while/with, share the parent scope context so that
            // renames inside blocks count toward the outer chain.
            Stmt::If(i) => {
                scan_stmts(&i.body, source, file, index, severity, renamed_from, chain_depth, violations);
                scan_stmts(&i.orelse, source, file, index, severity, renamed_from, chain_depth, violations);
            }
            Stmt::For(f) => {
                scan_stmts(&f.body, source, file, index, severity, renamed_from, chain_depth, violations);
            }
            Stmt::While(w) => {
                scan_stmts(&w.body, source, file, index, severity, renamed_from, chain_depth, violations);
            }
            Stmt::With(w) => {
                scan_stmts(&w.body, source, file, index, severity, renamed_from, chain_depth, violations);
            }
            Stmt::Try(t) => {
                scan_stmts(&t.body, source, file, index, severity, renamed_from, chain_depth, violations);
            }
            _ => {}
        }
    }
}

pub fn check(
    stmts: &[Stmt],
    source: &str,
    file: &str,
    config: &Config,
    index: &LineIndex,
) -> Vec<Violation> {
    let mut violations = vec![];
    scan_stmts(
        stmts, source, file, index,
        config.severity_of(ID),
        &mut Default::default(),
        &mut Default::default(),
        &mut violations,
    );
    violations
}
