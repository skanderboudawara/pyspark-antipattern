mod common;
use common::{assert_no_violation, assert_violation, check};
use pyspark_antipattern::rules::p_rules::*;

// ── P001: toPandas() without Arrow config ─────────────────────────────────────
#[test]
fn p001_fires_without_arrow() {
    let src = "pandas_df = df.toPandas()";
    assert_violation(&check(p001::check, src), "P001", 1);
}
#[test]
fn p001_no_fire_with_arrow_config() {
    let src = r#"
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
pandas_df = df.toPandas()
"#;
    assert_no_violation(&check(p001::check, src), "P001");
}
#[test]
fn p001_no_false_positive() {
    let src = "df = df.select('a', 'b')";
    assert_no_violation(&check(p001::check, src), "P001");
}
