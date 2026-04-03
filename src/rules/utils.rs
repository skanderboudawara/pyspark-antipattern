use rustpython_parser::ast::Expr;

use crate::{
    line_index::LineIndex,
    spark_ops::NON_DATAFRAME_ROOTS,
    violation::{RuleId, Severity, Violation},
};

/// Build a Violation for a method call detected on `call_expr`.
/// `attr_name` is the method name (e.g. "collect").
/// The caret column is placed at the start of the method name.
pub fn method_violation(
    func_attr: &rustpython_parser::ast::ExprAttribute,
    attr_name: &str,
    source: &str,
    file: &str,
    index: &LineIndex,
    severity: Severity,
    rule_id: &str,
) -> Violation {
    // attr.range.end() is right after the last char of the attribute name.
    let end: u32 = func_attr.range.end().into();
    let start = end.saturating_sub(attr_name.len() as u32);
    let (line, col) = index.line_col(start);
    let source_line = index.line_text(source, line).to_string();
    Violation {
        rule_id: RuleId(rule_id.to_string()),
        severity,
        impact: crate::violation::Impact::Low,
        file: file.to_string(),
        line,
        col,
        source_line,
        span_len: attr_name.len() + 2, // name + "()"
    }
}

/// Build a Violation positioned at the start of an arbitrary expression.
pub fn expr_violation(
    expr: &Expr,
    span_len: usize,
    source: &str,
    file: &str,
    index: &LineIndex,
    severity: Severity,
    rule_id: &str,
) -> Violation {
    let start: u32 = expr_start(expr);
    let (line, col) = index.line_col(start);
    let source_line = index.line_text(source, line).to_string();
    Violation {
        rule_id: RuleId(rule_id.to_string()),
        severity,
        impact: crate::violation::Impact::Low,
        file: file.to_string(),
        line,
        col,
        source_line,
        span_len,
    }
}

/// Extract the byte start offset of an expression.
pub fn expr_start(expr: &Expr) -> u32 {
    match expr {
        Expr::BoolOp(e) => e.range.start().into(),
        Expr::NamedExpr(e) => e.range.start().into(),
        Expr::BinOp(e) => e.range.start().into(),
        Expr::UnaryOp(e) => e.range.start().into(),
        Expr::Lambda(e) => e.range.start().into(),
        Expr::IfExp(e) => e.range.start().into(),
        Expr::Dict(e) => e.range.start().into(),
        Expr::Set(e) => e.range.start().into(),
        Expr::ListComp(e) => e.range.start().into(),
        Expr::SetComp(e) => e.range.start().into(),
        Expr::DictComp(e) => e.range.start().into(),
        Expr::GeneratorExp(e) => e.range.start().into(),
        Expr::Await(e) => e.range.start().into(),
        Expr::Yield(e) => e.range.start().into(),
        Expr::YieldFrom(e) => e.range.start().into(),
        Expr::Compare(e) => e.range.start().into(),
        Expr::Call(e) => e.range.start().into(),
        Expr::FormattedValue(e) => e.range.start().into(),
        Expr::JoinedStr(e) => e.range.start().into(),
        Expr::Constant(e) => e.range.start().into(),
        Expr::Attribute(e) => e.range.start().into(),
        Expr::Subscript(e) => e.range.start().into(),
        Expr::Starred(e) => e.range.start().into(),
        Expr::Name(e) => e.range.start().into(),
        Expr::List(e) => e.range.start().into(),
        Expr::Tuple(e) => e.range.start().into(),
        Expr::Slice(e) => e.range.start().into(),
    }
}

/// Walk an expression's method chain and collect each method name called.
/// Returns names from innermost to outermost.
pub fn method_chain<'a>(expr: &'a Expr) -> Vec<&'a str> {
    let mut chain = vec![];
    let mut cur = expr;
    loop {
        if let Expr::Call(c) = cur {
            if let Expr::Attribute(a) = c.func.as_ref() {
                chain.push(a.attr.as_str());
                cur = a.value.as_ref();
                continue;
            }
        }
        break;
    }
    chain
}

/// Return the number of consecutive calls to `method` at the top of the chain.
pub fn consecutive_method_depth(expr: &Expr, method: &str) -> usize {
    if let Expr::Call(c) = expr {
        if let Expr::Attribute(a) = c.func.as_ref() {
            if a.attr.as_str() == method {
                return 1 + consecutive_method_depth(a.value.as_ref(), method);
            }
        }
    }
    0
}

/// Return `true` when `expr` is the receiver of a method call that belongs to a
/// known stdlib/utility namespace rather than a Spark DataFrame.
///
/// Walks the attribute chain to its root `Name` and checks against a deny-list.
/// Examples that return `true`: `os.path`, `sys`, `pathlib`, `str`, `bytes`.
/// Examples that return `false`: `df`, `self.df`, any `Call` result.
pub fn is_non_dataframe_receiver(expr: &Expr) -> bool {
    if matches!(expr, Expr::Constant(c) if matches!(c.value, rustpython_parser::ast::Constant::Str(_))) {
        return true; // string literal receiver: `",".join(...)`
    }
    let mut current = expr;
    loop {
        match current {
            Expr::Attribute(a) => current = a.value.as_ref(),
            Expr::Name(n) => {
                return NON_DATAFRAME_ROOTS.contains(&n.id.as_str());
            }
            _ => return false,
        }
    }
}

/// Check whether a method name appears anywhere in the call chain.
pub fn chain_has_method(expr: &Expr, method: &str) -> bool {
    if let Expr::Call(c) = expr {
        if let Expr::Attribute(a) = c.func.as_ref() {
            if a.attr.as_str() == method {
                return true;
            }
            return chain_has_method(a.value.as_ref(), method);
        }
    }
    false
}

/// Estimate the number of iterations for a `for` loop's iterable expression.
/// Handles `range(stop)`, `range(start, stop)`, `range(start, stop, step)`.
/// Returns `None` when the count cannot be statically determined.
pub fn for_loop_iters(iter_expr: &Expr) -> Option<i64> {
    if let Expr::Call(c) = iter_expr {
        let is_range = match c.func.as_ref() {
            Expr::Name(n) => n.id.as_str() == "range",
            _ => false,
        };
        if is_range {
            match c.args.len() {
                1 => {
                    // range(stop)
                    return const_int(&c.args[0]);
                }
                2 => {
                    // range(start, stop) → stop - start
                    let start = const_int(&c.args[0])?;
                    let stop  = const_int(&c.args[1])?;
                    return Some((stop - start).max(0));
                }
                3 => {
                    // range(start, stop, step)
                    let start = const_int(&c.args[0])?;
                    let stop  = const_int(&c.args[1])?;
                    let step  = const_int(&c.args[2])?;
                    if step == 0 { return None; }
                    let count = ((stop - start) as f64 / step as f64).ceil().max(0.0) as i64;
                    return Some(count);
                }
                _ => {}
            }
        }
    }
    None
}

/// Try to extract a small integer literal value from an expression.
pub fn const_int(expr: &Expr) -> Option<i64> {
    if let Expr::Constant(c) = expr {
        if let rustpython_parser::ast::Constant::Int(n) = &c.value {
            return n.to_string().parse::<i64>().ok();
        }
    }
    None
}
