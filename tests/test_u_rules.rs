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

// ── U005: loops inside a UDF body ─────────────────────────────────────────────
#[test]
fn u005_fires_for_loop() {
    let src = "@udf(returnType=ArrayType(IntegerType()))\ndef f(items):\n    result = []\n    for x in items:\n        result.append(x * 2)\n    return result\n";
    assert_violation(&check(u005::check, src), "U005", 4);
}
#[test]
fn u005_fires_list_comp() {
    let src = "@udf(returnType=ArrayType(StringType()))\ndef f(items):\n    return [x.upper() for x in items]\n";
    assert_violation(&check(u005::check, src), "U005", 3);
}
#[test]
fn u005_fires_set_comp() {
    let src = "@udf(returnType=ArrayType(StringType()))\ndef f(items):\n    return {x.upper() for x in items}\n";
    assert_violation(&check(u005::check, src), "U005", 3);
}
#[test]
fn u005_fires_dict_comp() {
    let src = "@udf(returnType=StringType())\ndef f(pairs):\n    return {k: v for k, v in pairs}\n";
    assert_violation(&check(u005::check, src), "U005", 3);
}
#[test]
fn u005_fires_bare_udf_decorator() {
    let src = "@udf\ndef f(items):\n    return [x for x in items]\n";
    assert_violation(&check(u005::check, src), "U005", 3);
}
#[test]
fn u005_fires_pandas_udf() {
    let src = "@pandas_udf(ArrayType(StringType()))\ndef f(s):\n    return [x.upper() for x in s]\n";
    assert_violation(&check(u005::check, src), "U005", 3);
}
#[test]
fn u005_no_fire_no_udf() {
    // plain function — not a UDF, should not fire
    let src = "def f(items):\n    return [x * 2 for x in items]\n";
    assert_no_violation(&check(u005::check, src), "U005");
}
#[test]
fn u005_no_fire_nested_helper() {
    // loop is inside a nested plain function defined inside the UDF — not flagged
    let src = "@udf(returnType=StringType())\ndef f(items):\n    def helper(xs):\n        return [x for x in xs]\n    return helper(items)[0]\n";
    assert_no_violation(&check(u005::check, src), "U005");
}

// ── U006: all() inside a UDF body ─────────────────────────────────────────────
#[test]
fn u006_fires_return_all_generator() {
    let src = "@udf(returnType=BooleanType())\ndef f(items):\n    return all(x > 0 for x in items)\n";
    assert_violation(&check(u006::check, src), "U006", 3);
}
#[test]
fn u006_fires_return_all_list() {
    let src = "@udf(returnType=BooleanType())\ndef f(items):\n    return all([x > 0 for x in items])\n";
    assert_violation(&check(u006::check, src), "U006", 3);
}
#[test]
fn u006_fires_assigned_all() {
    let src =
        "@udf(returnType=BooleanType())\ndef f(items):\n    result = all(x > 0 for x in items)\n    return result\n";
    assert_violation(&check(u006::check, src), "U006", 3);
}
#[test]
fn u006_fires_bare_udf() {
    let src = "@udf\ndef f(items):\n    return all(items)\n";
    assert_violation(&check(u006::check, src), "U006", 3);
}
#[test]
fn u006_no_fire_no_udf() {
    let src = "def f(items):\n    return all(x > 0 for x in items)\n";
    assert_no_violation(&check(u006::check, src), "U006");
}
#[test]
fn u006_no_fire_nested_helper() {
    let src = "@udf(returnType=BooleanType())\ndef f(items):\n    def check(xs):\n        return all(xs)\n    return check(items)\n";
    assert_no_violation(&check(u006::check, src), "U006");
}

// ── U007: any() inside a UDF body ─────────────────────────────────────────────
#[test]
fn u007_fires_return_any_generator() {
    let src = "@udf(returnType=BooleanType())\ndef f(items):\n    return any(x < 0 for x in items)\n";
    assert_violation(&check(u007::check, src), "U007", 3);
}
#[test]
fn u007_fires_return_any_list() {
    let src = "@udf(returnType=BooleanType())\ndef f(items):\n    return any([x < 0 for x in items])\n";
    assert_violation(&check(u007::check, src), "U007", 3);
}
#[test]
fn u007_fires_assigned_any() {
    let src =
        "@udf(returnType=BooleanType())\ndef f(items):\n    result = any(x < 0 for x in items)\n    return result\n";
    assert_violation(&check(u007::check, src), "U007", 3);
}
#[test]
fn u007_fires_bare_udf() {
    let src = "@udf\ndef f(items):\n    return any(items)\n";
    assert_violation(&check(u007::check, src), "U007", 3);
}
#[test]
fn u007_no_fire_no_udf() {
    let src = "def f(items):\n    return any(x < 0 for x in items)\n";
    assert_no_violation(&check(u007::check, src), "U007");
}
#[test]
fn u007_no_fire_nested_helper() {
    let src = "@udf(returnType=BooleanType())\ndef f(items):\n    def check(xs):\n        return any(xs)\n    return check(items)\n";
    assert_no_violation(&check(u007::check, src), "U007");
}
