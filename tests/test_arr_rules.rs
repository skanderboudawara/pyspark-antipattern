mod common;
use common::{assert_no_violation, assert_violation, check};
use pyspark_antipattern::rules::arr_rules::*;

// ── ARR001: array_distinct(collect_list()) → collect_set() ───────────────────

// pattern 1: inline nesting
#[test]
fn arr001_fires_inline() {
    assert_violation(
        &check(arr001::check, "df.agg(array_distinct(collect_list(col('item'))))"),
        "ARR001",
        1,
    );
}
#[test]
fn arr001_fires_qualified() {
    assert_violation(
        &check(arr001::check, "df.agg(F.array_distinct(F.collect_list(col('item'))))"),
        "ARR001",
        1,
    );
}

// pattern 2: split across two withColumn calls
#[test]
fn arr001_fires_split() {
    let src = "df = df.withColumn('items', collect_list(col('item')))\ndf = df.withColumn('items', array_distinct(col('items')))";
    let v = check(arr001::check, src);
    assert_violation(&v, "ARR001", 2);
    // Caret must point at `array_distinct`, not at `df` (col 20 = "df = df.withColumn('items', " + 1)
    assert_eq!(v[0].col, 29, "violation col should point to array_distinct, not df");
}

// pattern 3: collect_list wrapped in .over(window)
#[test]
fn arr001_fires_inline_over_window() {
    assert_violation(
        &check(
            arr001::check,
            "df.withColumn('a', array_distinct(collect_list(col('item').over(w))))",
        ),
        "ARR001",
        1,
    );
}
#[test]
fn arr001_fires_split_over_window() {
    let src = "df = df.withColumn('items', collect_list(col('item')).over(w))\ndf = df.withColumn('items', array_distinct(col('items')))";
    assert_violation(&check(arr001::check, src), "ARR001", 2);
}

// ── ARR002: array_except(col, None/lit(None)) → array_compact() ──────────────

#[test]
fn arr002_fires_bare_none() {
    assert_violation(
        &check(arr002::check, "df.withColumn('a', array_except(col('items'), None))"),
        "ARR002",
        1,
    );
}
#[test]
fn arr002_fires_lit_none() {
    assert_violation(
        &check(
            arr002::check,
            "df.withColumn('a', array_except(col('items'), lit(None)))",
        ),
        "ARR002",
        1,
    );
}
#[test]
fn arr002_fires_qualified() {
    assert_violation(
        &check(
            arr002::check,
            "df.withColumn('a', F.array_except(col('items'), lit(None)))",
        ),
        "ARR002",
        1,
    );
}

// no false positives
#[test]
fn arr002_no_valid_second_arg() {
    assert_no_violation(
        &check(arr002::check, "df.withColumn('a', array_except(col('x'), col('y')))"),
        "ARR002",
    );
}
#[test]
fn arr002_no_array_compact() {
    assert_no_violation(
        &check(arr002::check, "df.withColumn('a', array_compact(col('items')))"),
        "ARR002",
    );
}

// ── ARR003: array_distinct(collect_set()) — redundant dedup ──────────────────
#[test]
fn arr003_fires_inline() {
    assert_violation(
        &check(arr003::check, "df.agg(array_distinct(collect_set(col('tag'))))"),
        "ARR003",
        1,
    );
}
#[test]
fn arr003_fires_qualified() {
    assert_violation(
        &check(arr003::check, "df.agg(F.array_distinct(F.collect_set(col('tag'))))"),
        "ARR003",
        1,
    );
}
#[test]
fn arr003_fires_over_window() {
    assert_violation(
        &check(
            arr003::check,
            "df.withColumn('tags', array_distinct(collect_set(col('tag')).over(w)))",
        ),
        "ARR003",
        1,
    );
}
#[test]
fn arr003_no_collect_set_alone() {
    assert_no_violation(&check(arr003::check, "df.agg(collect_set(col('tag')))"), "ARR003");
}
#[test]
fn arr003_no_array_distinct_alone() {
    assert_no_violation(
        &check(arr003::check, "df.withColumn('a', array_distinct(col('tags')))"),
        "ARR003",
    );
}
#[test]
fn arr003_no_collect_list() {
    // collect_list is ARR001's concern, not ARR003
    assert_no_violation(
        &check(arr003::check, "df.agg(array_distinct(collect_list(col('tag'))))"),
        "ARR003",
    );
}

// ── ARR004: size(collect_set()) inside .agg() → countDistinct() ──────────────

#[test]
fn arr004_fires_direct() {
    assert_violation(
        &check(arr004::check, "df.agg(size(collect_set(col('x'))))"),
        "ARR004",
        1,
    );
}
#[test]
fn arr004_fires_with_alias() {
    assert_violation(
        &check(arr004::check, "df.agg(size(collect_set(col('x'))).alias('cnt'))"),
        "ARR004",
        1,
    );
}
#[test]
fn arr004_fires_qualified() {
    assert_violation(
        &check(arr004::check, "df.agg(F.size(F.collect_set(col('x'))))"),
        "ARR004",
        1,
    );
}
#[test]
fn arr004_fires_string_col() {
    assert_violation(
        &check(arr004::check, "df.agg(size(collect_set('user_id')).alias('n'))"),
        "ARR004",
        1,
    );
}
#[test]
fn arr004_no_collect_set_alone() {
    assert_no_violation(&check(arr004::check, "df.agg(collect_set(col('x')))"), "ARR004");
}
#[test]
fn arr004_no_size_alone() {
    assert_no_violation(&check(arr004::check, "df.agg(size(col('arr')))"), "ARR004");
}
#[test]
fn arr004_no_count_distinct() {
    assert_no_violation(&check(arr004::check, "df.agg(countDistinct(col('x')))"), "ARR004");
}
#[test]
fn arr004_no_outside_agg() {
    // size(collect_set()) outside .agg() is not flagged by this rule
    assert_no_violation(
        &check(arr004::check, "df.withColumn('n', size(collect_set(col('x'))))"),
        "ARR004",
    );
}
#[test]
fn arr004_fires_keyword_agg_arg() {
    assert_violation(
        &check(arr004::check, "df.agg(total=size(collect_set(col('x'))))"),
        "ARR004",
        1,
    );
}
#[test]
fn arr004_fires_keyword_agg_with_alias() {
    assert_violation(
        &check(arr004::check, "df.agg(total=size(collect_set(col('x'))).alias('cnt'))"),
        "ARR004",
        1,
    );
}

// ── ARR005: size(collect_list()) inside .agg() → count() ─────────────────────

#[test]
fn arr005_fires_direct() {
    assert_violation(
        &check(arr005::check, "df.agg(size(collect_list(col('x'))))"),
        "ARR005",
        1,
    );
}
#[test]
fn arr005_fires_with_alias() {
    assert_violation(
        &check(arr005::check, "df.agg(size(collect_list(col('x'))).alias('cnt'))"),
        "ARR005",
        1,
    );
}
#[test]
fn arr005_fires_qualified() {
    assert_violation(
        &check(arr005::check, "df.agg(F.size(F.collect_list(col('x'))))"),
        "ARR005",
        1,
    );
}
#[test]
fn arr005_fires_string_col() {
    assert_violation(
        &check(arr005::check, "df.agg(size(collect_list('order_id')).alias('n'))"),
        "ARR005",
        1,
    );
}
#[test]
fn arr005_no_collect_list_alone() {
    assert_no_violation(&check(arr005::check, "df.agg(collect_list(col('x')))"), "ARR005");
}
#[test]
fn arr005_no_size_alone() {
    assert_no_violation(&check(arr005::check, "df.agg(size(col('arr')))"), "ARR005");
}
#[test]
fn arr005_no_count() {
    assert_no_violation(&check(arr005::check, "df.agg(count(col('x')))"), "ARR005");
}
#[test]
fn arr005_no_outside_agg() {
    assert_no_violation(
        &check(arr005::check, "df.withColumn('n', size(collect_list(col('x'))))"),
        "ARR005",
    );
}
#[test]
fn arr005_no_collect_set() {
    // collect_set variant is ARR004's concern
    assert_no_violation(&check(arr005::check, "df.agg(size(collect_set(col('x'))))"), "ARR005");
}
#[test]
fn arr005_fires_keyword_agg_arg() {
    // df.agg(total=size(collect_list(col("x")))) — keyword form of .agg()
    assert_violation(
        &check(arr005::check, "df.agg(total=size(collect_list(col('x'))))"),
        "ARR005",
        1,
    );
}
#[test]
fn arr005_fires_keyword_agg_with_alias() {
    assert_violation(
        &check(arr005::check, "df.agg(total=size(collect_list(col('x'))).alias('cnt'))"),
        "ARR005",
        1,
    );
}

// ── ARR006: size(collect_list().over(w)) → count().over(w) ───────────────────

#[test]
fn arr006_fires_withcolumn() {
    assert_violation(
        &check(
            arr006::check,
            "df.withColumn('n', size(collect_list(col('x')).over(w)))",
        ),
        "ARR006",
        1,
    );
}
#[test]
fn arr006_fires_select() {
    assert_violation(
        &check(
            arr006::check,
            "df.select(size(collect_list(col('x')).over(w)).alias('n'))",
        ),
        "ARR006",
        1,
    );
}
#[test]
fn arr006_fires_qualified() {
    assert_violation(
        &check(
            arr006::check,
            "df.withColumn('n', F.size(F.collect_list(col('x')).over(w)))",
        ),
        "ARR006",
        1,
    );
}
#[test]
fn arr006_no_without_over() {
    // size(collect_list()) without .over() is ARR005's concern
    assert_no_violation(&check(arr006::check, "df.agg(size(collect_list(col('x'))))"), "ARR006");
}
#[test]
fn arr006_no_count_over() {
    assert_no_violation(
        &check(arr006::check, "df.withColumn('n', count(col('x')).over(w))"),
        "ARR006",
    );
}
#[test]
fn arr006_no_collect_set_over() {
    // collect_set variant is not this rule
    assert_no_violation(
        &check(arr006::check, "df.withColumn('n', size(collect_set(col('x')).over(w)))"),
        "ARR006",
    );
}

// no false positives
#[test]
fn arr001_no_collect_set() {
    assert_no_violation(&check(arr001::check, "df.agg(collect_set(col('item')))"), "ARR001");
}
#[test]
fn arr001_no_array_distinct_alone() {
    assert_no_violation(
        &check(arr001::check, "df.withColumn('a', array_distinct(col('items')))"),
        "ARR001",
    );
}
#[test]
fn arr001_no_collect_list_alone() {
    assert_no_violation(&check(arr001::check, "df.agg(collect_list(col('item')))"), "ARR001");
}
