mod common;
use common::{assert_no_violation, assert_violation, check};
use pyspark_antipattern::rules::arr_rules::*;

// ── ARR001: array_distinct(collect_list()) → collect_set() ───────────────────

// pattern 1: inline nesting
#[test] fn arr001_fires_inline() {
    assert_violation(&check(arr001::check, "df.agg(array_distinct(collect_list(col('item'))))"), "ARR001", 1);
}
#[test] fn arr001_fires_qualified() {
    assert_violation(&check(arr001::check, "df.agg(F.array_distinct(F.collect_list(col('item'))))"), "ARR001", 1);
}

// pattern 2: split across two withColumn calls
#[test] fn arr001_fires_split() {
    let src = "df = df.withColumn('items', collect_list(col('item')))\ndf = df.withColumn('items', array_distinct(col('items')))";
    assert_violation(&check(arr001::check, src), "ARR001", 2);
}

// pattern 3: collect_list wrapped in .over(window)
#[test] fn arr001_fires_inline_over_window() {
    assert_violation(&check(arr001::check, "df.withColumn('a', array_distinct(collect_list(col('item').over(w))))"), "ARR001", 1);
}
#[test] fn arr001_fires_split_over_window() {
    let src = "df = df.withColumn('items', collect_list(col('item')).over(w))\ndf = df.withColumn('items', array_distinct(col('items')))";
    assert_violation(&check(arr001::check, src), "ARR001", 2);
}

// no false positives
#[test] fn arr001_no_collect_set() {
    assert_no_violation(&check(arr001::check, "df.agg(collect_set(col('item')))"), "ARR001");
}
#[test] fn arr001_no_array_distinct_alone() {
    assert_no_violation(&check(arr001::check, "df.withColumn('a', array_distinct(col('items')))"), "ARR001");
}
#[test] fn arr001_no_collect_list_alone() {
    assert_no_violation(&check(arr001::check, "df.agg(collect_list(col('item')))"), "ARR001");
}
