use rustpython_parser::ast::{ExceptHandler, Expr, Stmt};

pub trait Visitor: Sized {
    fn visit_stmt(&mut self, stmt: &Stmt) {
        walk_stmt(self, stmt);
    }
    fn visit_expr(&mut self, expr: &Expr) {
        walk_expr(self, expr);
    }
}

pub fn walk_stmt<V: Visitor>(v: &mut V, stmt: &Stmt) {
    match stmt {
        Stmt::FunctionDef(f) => {
            for d in &f.decorator_list {
                v.visit_expr(d);
            }
            for arg in f.args.posonlyargs.iter().chain(&f.args.args).chain(&f.args.kwonlyargs) {
                if let Some(default) = &arg.default {
                    v.visit_expr(default);
                }
            }
            for s in &f.body {
                v.visit_stmt(s);
            }
        }
        Stmt::AsyncFunctionDef(f) => {
            for d in &f.decorator_list {
                v.visit_expr(d);
            }
            for arg in f.args.posonlyargs.iter().chain(&f.args.args).chain(&f.args.kwonlyargs) {
                if let Some(default) = &arg.default {
                    v.visit_expr(default);
                }
            }
            for s in &f.body {
                v.visit_stmt(s);
            }
        }
        Stmt::ClassDef(c) => {
            for d in &c.decorator_list {
                v.visit_expr(d);
            }
            for base in &c.bases {
                v.visit_expr(base);
            }
            for s in &c.body {
                v.visit_stmt(s);
            }
        }
        Stmt::Return(r) => {
            if let Some(val) = &r.value {
                v.visit_expr(val);
            }
        }
        Stmt::Assign(a) => {
            v.visit_expr(&a.value);
        }
        Stmt::AugAssign(a) => {
            v.visit_expr(&a.value);
        }
        Stmt::AnnAssign(a) => {
            if let Some(val) = &a.value {
                v.visit_expr(val);
            }
        }
        Stmt::For(f) => {
            v.visit_expr(&f.iter);
            for s in &f.body {
                v.visit_stmt(s);
            }
            for s in &f.orelse {
                v.visit_stmt(s);
            }
        }
        Stmt::AsyncFor(f) => {
            v.visit_expr(&f.iter);
            for s in &f.body {
                v.visit_stmt(s);
            }
            for s in &f.orelse {
                v.visit_stmt(s);
            }
        }
        Stmt::While(w) => {
            v.visit_expr(&w.test);
            for s in &w.body {
                v.visit_stmt(s);
            }
            for s in &w.orelse {
                v.visit_stmt(s);
            }
        }
        Stmt::If(i) => {
            v.visit_expr(&i.test);
            for s in &i.body {
                v.visit_stmt(s);
            }
            for s in &i.orelse {
                v.visit_stmt(s);
            }
        }
        Stmt::With(w) => {
            for item in &w.items {
                v.visit_expr(&item.context_expr);
            }
            for s in &w.body {
                v.visit_stmt(s);
            }
        }
        Stmt::AsyncWith(w) => {
            for item in &w.items {
                v.visit_expr(&item.context_expr);
            }
            for s in &w.body {
                v.visit_stmt(s);
            }
        }
        Stmt::Try(t) => {
            for s in &t.body {
                v.visit_stmt(s);
            }
            for h in &t.handlers {
                let ExceptHandler::ExceptHandler(eh) = h;
                for s in &eh.body {
                    v.visit_stmt(s);
                }
            }
            for s in &t.orelse {
                v.visit_stmt(s);
            }
            for s in &t.finalbody {
                v.visit_stmt(s);
            }
        }
        Stmt::TryStar(t) => {
            for s in &t.body {
                v.visit_stmt(s);
            }
            for h in &t.handlers {
                let ExceptHandler::ExceptHandler(eh) = h;
                for s in &eh.body {
                    v.visit_stmt(s);
                }
            }
            for s in &t.orelse {
                v.visit_stmt(s);
            }
            for s in &t.finalbody {
                v.visit_stmt(s);
            }
        }
        Stmt::Match(m) => {
            for case in &m.cases {
                if let Some(guard) = &case.guard {
                    v.visit_expr(guard);
                }
                for s in &case.body {
                    v.visit_stmt(s);
                }
            }
        }
        Stmt::Expr(e) => {
            v.visit_expr(&e.value);
        }
        _ => {}
    }
}

pub fn walk_expr<V: Visitor>(v: &mut V, expr: &Expr) {
    match expr {
        Expr::BoolOp(b) => {
            for e in &b.values {
                v.visit_expr(e);
            }
        }
        Expr::NamedExpr(n) => {
            v.visit_expr(&n.value);
        }
        Expr::BinOp(b) => {
            v.visit_expr(&b.left);
            v.visit_expr(&b.right);
        }
        Expr::UnaryOp(u) => {
            v.visit_expr(&u.operand);
        }
        Expr::Lambda(l) => {
            v.visit_expr(&l.body);
        }
        Expr::IfExp(i) => {
            v.visit_expr(&i.test);
            v.visit_expr(&i.body);
            v.visit_expr(&i.orelse);
        }
        Expr::Dict(d) => {
            for k in d.keys.iter().flatten() {
                v.visit_expr(k);
            }
            for val in &d.values {
                v.visit_expr(val);
            }
        }
        Expr::Set(s) => {
            for e in &s.elts {
                v.visit_expr(e);
            }
        }
        Expr::ListComp(lc) => {
            v.visit_expr(&lc.elt);
            for comp in &lc.generators {
                v.visit_expr(&comp.iter);
                for cond in &comp.ifs {
                    v.visit_expr(cond);
                }
            }
        }
        Expr::SetComp(sc) => {
            v.visit_expr(&sc.elt);
            for comp in &sc.generators {
                v.visit_expr(&comp.iter);
                for cond in &comp.ifs {
                    v.visit_expr(cond);
                }
            }
        }
        Expr::DictComp(dc) => {
            v.visit_expr(&dc.key);
            v.visit_expr(&dc.value);
            for comp in &dc.generators {
                v.visit_expr(&comp.iter);
                for cond in &comp.ifs {
                    v.visit_expr(cond);
                }
            }
        }
        Expr::GeneratorExp(g) => {
            v.visit_expr(&g.elt);
            for comp in &g.generators {
                v.visit_expr(&comp.iter);
                for cond in &comp.ifs {
                    v.visit_expr(cond);
                }
            }
        }
        Expr::Await(a) => {
            v.visit_expr(&a.value);
        }
        Expr::Yield(y) => {
            if let Some(val) = &y.value {
                v.visit_expr(val);
            }
        }
        Expr::YieldFrom(y) => {
            v.visit_expr(&y.value);
        }
        Expr::Compare(c) => {
            v.visit_expr(&c.left);
            for e in &c.comparators {
                v.visit_expr(e);
            }
        }
        Expr::Call(c) => {
            v.visit_expr(&c.func);
            for a in &c.args {
                v.visit_expr(a);
            }
            for kw in &c.keywords {
                v.visit_expr(&kw.value);
            }
        }
        Expr::FormattedValue(f) => {
            v.visit_expr(&f.value);
        }
        Expr::JoinedStr(j) => {
            for e in &j.values {
                v.visit_expr(e);
            }
        }
        Expr::Attribute(a) => {
            v.visit_expr(&a.value);
        }
        Expr::Subscript(s) => {
            v.visit_expr(&s.value);
            v.visit_expr(&s.slice);
        }
        Expr::Starred(s) => {
            v.visit_expr(&s.value);
        }
        Expr::List(l) => {
            for e in &l.elts {
                v.visit_expr(e);
            }
        }
        Expr::Tuple(t) => {
            for e in &t.elts {
                v.visit_expr(e);
            }
        }
        _ => {}
    }
}
