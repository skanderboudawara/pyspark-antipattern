mod common;
use common::{assert_no_violation, assert_violation, check};
use pyspark_antipattern::rules::u_rules::*;

// ── U001: @udf returning StringType ──────────────────────────────────────────
#[test]
fn u001_fires() {
    let src = "@udf(returnType=StringType())\ndef f(x): return x.upper()";
    assert_violation(&check(u001::check, src), "U001", 1);
}
#[test]
fn u001_no_fire_other_type() {
    let src = "@udf(returnType=LongType())\ndef f(x): return x * 2";
    assert_no_violation(&check(u001::check, src), "U001");
}

// ── U002: @udf returning ArrayType ───────────────────────────────────────────
#[test]
fn u002_fires() {
    let src = "@udf(returnType=ArrayType(IntegerType()))\ndef f(x): return [int(v) for v in x.split(',')]";
    assert_violation(&check(u002::check, src), "U002", 1);
}
#[test]
fn u002_no_fire_other_type() {
    let src = "@udf(returnType=LongType())\ndef f(x): return x * 2";
    assert_no_violation(&check(u002::check, src), "U002");
}

// ── U003: any @udf or @pandas_udf ────────────────────────────────────────────
#[test]
fn u003_fires_udf() {
    let src = "@udf(returnType=LongType())\ndef f(x): return x * 2";
    assert_violation(&check(u003::check, src), "U003", 1);
}
#[test]
fn u003_fires_pandas_udf() {
    let src = "@pandas_udf(LongType())\ndef f(s): return s * 2";
    assert_violation(&check(u003::check, src), "U003", 1);
}
#[test]
fn u003_no_false_positive() {
    let src = "def f(x): return x * 2";
    assert_no_violation(&check(u003::check, src), "U003");
}

// ── U004: nested UDF calls ────────────────────────────────────────────────────
#[test]
fn u004_fires_nested_call() {
    // normalize is defined on lines 1-3, process on lines 5-7.
    // The nested call `normalize(x)` is on line 7.
    let src = "@udf(returnType=StringType())\ndef normalize(x):\n    return x.strip().lower()\n\n@udf(returnType=StringType())\ndef process(x):\n    return normalize(x) + '_ok'\n";
    assert_violation(&check(u004::check, src), "U004", 7);
}
#[test]
fn u004_fires_multiple_nested() {
    // clean on lines 1-3, tag on lines 5-7, process on lines 9-11.
    // clean(x) and tag(x) are both on line 11.
    let src = "@udf(returnType=StringType())\ndef clean(x):\n    return x.strip()\n\n@udf(returnType=StringType())\ndef tag(x):\n    return x + '_tag'\n\n@udf(returnType=StringType())\ndef process(x):\n    return clean(x) + tag(x)\n";
    assert_violation(&check(u004::check, src), "U004", 11);
}
#[test]
fn u004_no_self_recursion() {
    // A UDF calling itself should not be flagged by U004
    let src = "@udf(returnType=StringType())\ndef recursive(x):\n    return recursive(x)\n";
    assert_no_violation(&check(u004::check, src), "U004");
}
#[test]
fn u004_no_plain_helper() {
    // Calling a plain (non-UDF) function from inside a UDF is fine
    let src = "def helper(x):\n    return x.strip()\n\n@udf(returnType=StringType())\ndef process(x):\n    return helper(x)\n";
    assert_no_violation(&check(u004::check, src), "U004");
}
#[test]
fn u004_no_single_udf() {
    // Only one UDF — nesting is impossible
    let src = "@udf(returnType=StringType())\ndef process(x):\n    return x.strip()\n";
    assert_no_violation(&check(u004::check, src), "U004");
}
