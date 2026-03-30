mod common;
use common::{assert_no_violation, assert_violation, check};
use pyspark_antipattern::rules::f_rules::*;

// ── F001: withColumn + withColumnRenamed mixed ────────────────────────────────
#[test] fn f001_fires()           { assert_violation(&check(f001::check, "df.withColumn('a', col('x')).withColumnRenamed('a', 'b')"), "F001", 1); }
#[test] fn f001_no_false_positive(){ assert_no_violation(&check(f001::check, "df.withColumn('a', col('x')).withColumn('b', col('y'))"), "F001"); }

// ── F002: drop() ─────────────────────────────────────────────────────────────
#[test] fn f002_fires()           { assert_violation(&check(f002::check, "df.drop('col')"), "F002", 1); }
#[test] fn f002_no_false_positive(){ assert_no_violation(&check(f002::check, "df.select('a', 'b')"), "F002"); }

// ── F003: selectExpr() ───────────────────────────────────────────────────────
#[test] fn f003_fires()           { assert_violation(&check(f003::check, "df.selectExpr('age * 2')"), "F003", 1); }
#[test] fn f003_no_false_positive(){ assert_no_violation(&check(f003::check, "df.select(col('age') * 2)"), "F003"); }

// ── F004: spark.sql() ────────────────────────────────────────────────────────
#[test] fn f004_fires()           { assert_violation(&check(f004::check, "spark.sql('SELECT * FROM t')"), "F004", 1); }
#[test] fn f004_no_false_positive(){ assert_no_violation(&check(f004::check, "df.select('a')"), "F004"); }

// ── F005: stacked withColumn() ───────────────────────────────────────────────
#[test] fn f005_fires()           { assert_violation(&check(f005::check, "df.withColumn('a', col('x')).withColumn('b', col('y'))"), "F005", 1); }
#[test] fn f005_no_false_positive(){ assert_no_violation(&check(f005::check, "df.withColumn('a', col('x'))"), "F005"); }

// ── F006: stacked withColumnRenamed() ────────────────────────────────────────
#[test] fn f006_fires()           { assert_violation(&check(f006::check, "df.withColumnRenamed('a','b').withColumnRenamed('c','d')"), "F006", 1); }
#[test] fn f006_no_false_positive(){ assert_no_violation(&check(f006::check, "df.withColumnRenamed('a', 'b')"), "F006"); }

// ── F007: filter after select ────────────────────────────────────────────────
#[test] fn f007_fires()           { assert_violation(&check(f007::check, "df.select('a').filter(col('a') > 1)"), "F007", 1); }
#[test] fn f007_no_false_positive(){ assert_no_violation(&check(f007::check, "df.filter(col('a') > 1).select('a')"), "F007"); }

// ── F008: print() ────────────────────────────────────────────────────────────
#[test] fn f008_fires()           { assert_violation(&check(f008::check, "print('hello')"), "F008", 1); }
#[test] fn f008_no_false_positive(){ assert_no_violation(&check(f008::check, "x = 1 + 1"), "F008"); }

// ── F009: nested when() ──────────────────────────────────────────────────────
#[test] fn f009_fires()           { assert_violation(&check(f009::check, "when(when(col('a') > 1, 'x'), 'y')"), "F009", 1); }
#[test] fn f009_fires_double()           { assert_violation(&check(f009::check, "when(when(col('a') > 1, 'x'), when(col('a') > 1, 'x'))"), "F009", 1); }
#[test] fn f009_no_false_positive(){ assert_no_violation(&check(f009::check, "when(col('a') > 1, 'x').otherwise('y')"), "F009"); }

// ── F010: when() without otherwise() ─────────────────────────────────────────
#[test] fn f010_fires()           { assert_violation(&check(f010::check, "df.withColumn('f', when(col('a') > 1, 'x'))"), "F010", 1); }
#[test] fn f010_no_false_positive(){ assert_no_violation(&check(f010::check, "df.withColumn('f', when(col('a') > 1, 'x').otherwise('y'))"), "F010"); }

// ── F011: backslash continuation ─────────────────────────────────────────────
#[test] fn f011_fires()           { assert_violation(&check(f011::check, "x = df.select('a') \\\n    .filter(col('a') > 1)"), "F011", 1); }
#[test] fn f011_no_false_positive(){ assert_no_violation(&check(f011::check, "x = (df.select('a')\n    .filter(col('a') > 1))"), "F011"); }

// ── F012: bare literal in withColumn ─────────────────────────────────────────
#[test] fn f012_fires()           { assert_violation(&check(f012::check, "df.withColumn('n', 42)"), "F012", 1); }
#[test] fn f012_no_false_positive(){ assert_no_violation(&check(f012::check, "df.withColumn('n', lit(42))"), "F012"); }
