pub mod utils;
pub mod d_rules;
pub mod f_rules;
pub mod l_rules;
pub mod p_rules;
pub mod s_rules;
pub mod u_rules;

use rustpython_parser::ast::Stmt;

use crate::{config::Config, line_index::LineIndex, violation::Violation};

pub type RuleFn = fn(&[Stmt], &str, &str, &Config, &LineIndex) -> Vec<Violation>;

pub static ALL_RULES: &[RuleFn] = &[
    d_rules::d001::check,
    d_rules::d002::check,
    d_rules::d003::check,
    d_rules::d004::check,
    d_rules::d005::check,
    d_rules::d006::check,
    d_rules::d007::check,
    d_rules::d008::check,
    f_rules::f001::check,
    f_rules::f002::check,
    f_rules::f003::check,
    f_rules::f004::check,
    f_rules::f005::check,
    f_rules::f006::check,
    f_rules::f007::check,
    f_rules::f008::check,
    f_rules::f009::check,
    f_rules::f010::check,
    f_rules::f011::check,
    f_rules::f012::check,
    f_rules::f013::check,
    f_rules::f014::check,
    f_rules::f015::check,
    l_rules::l001::check,
    l_rules::l002::check,
    l_rules::l003::check,
    p_rules::p001::check,
    s_rules::s001::check,
    s_rules::s002::check,
    s_rules::s003::check,
    s_rules::s004::check,
    s_rules::s005::check,
    s_rules::s006::check,
    s_rules::s007::check,
    s_rules::s008::check,
    s_rules::s009::check,
    s_rules::s010::check,
    s_rules::s011::check,
    s_rules::s012::check,
    u_rules::u001::check,
    u_rules::u002::check,
    u_rules::u003::check,
];
