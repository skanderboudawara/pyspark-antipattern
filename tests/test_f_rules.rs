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

// ── F012: bare literal must be wrapped with lit() ────────────────────────────
// fires
#[test] fn f012_withcolumn_int()     { assert_violation(&check(f012::check, "df.withColumn('n', 42)"), "F012", 1); }
#[test] fn f012_withcolumn_str()     { assert_violation(&check(f012::check, "df.withColumn('n', 'hello')"), "F012", 1); }
#[test] fn f012_withcolumn_bool()    { assert_violation(&check(f012::check, "df.withColumn('n', True)"), "F012", 1); }
#[test] fn f012_when_value()         { assert_violation(&check(f012::check, "when(col('a') > 1, 99)"), "F012", 1); }
#[test] fn f012_otherwise_value()    { assert_violation(&check(f012::check, "when(col('a') > 1, lit(1)).otherwise(0)"), "F012", 1); }
// no false positives
#[test] fn f012_withcolumn_lit()     { assert_no_violation(&check(f012::check, "df.withColumn('n', lit(42))"), "F012"); }
#[test] fn f012_when_lit()           { assert_no_violation(&check(f012::check, "when(col('a') > 1, lit(99))"), "F012"); }
#[test] fn f012_otherwise_lit()      { assert_no_violation(&check(f012::check, "when(col('a') > 1, lit(1)).otherwise(lit(0))"), "F012"); }
#[test] fn f012_select_str_no_flag() { assert_no_violation(&check(f012::check, "df.select('id', 'name')"), "F012"); }
#[test] fn f012_withcolumn_col()     { assert_no_violation(&check(f012::check, "df.withColumn('n', col('x') + 1)"), "F012"); }

// ── F013: reserved column names (__dunder__) ─────────────────────────────────
#[test] fn f013_withcolumn_reserved()        { assert_violation(&check(f013::check, "df.withColumn('__index__', lit(1))"), "F013", 1); }
#[test] fn f013_withcolumnrenamed_reserved() { assert_violation(&check(f013::check, "df.withColumnRenamed('id', '__natural_order__')"), "F013", 1); }
#[test] fn f013_alias_reserved()             { assert_violation(&check(f013::check, "col('x').alias('__metadata__')"), "F013", 1); }
#[test] fn f013_no_normal_name()             { assert_no_violation(&check(f013::check, "df.withColumn('my_col', lit(1))"), "F013"); }
#[test] fn f013_no_single_underscore()       { assert_no_violation(&check(f013::check, "df.withColumn('_internal', lit(1))"), "F013"); }
#[test] fn f013_no_only_prefix()             { assert_no_violation(&check(f013::check, "df.withColumn('__index', lit(1))"), "F013"); }
#[test] fn f013_no_only_suffix()             { assert_no_violation(&check(f013::check, "df.withColumn('index__', lit(1))"), "F013"); }

// ── F014: explode_outer() instead of null handling ───────────────────────────
#[test] fn f014_fires_free_fn()    { assert_violation(&check(f014::check, "df.withColumn('x', explode_outer(col('items')))"), "F014", 1); }
#[test] fn f014_fires_qualified()  { assert_violation(&check(f014::check, "df.withColumn('x', F.explode_outer(col('items')))"), "F014", 1); }
#[test] fn f014_no_explode()       { assert_no_violation(&check(f014::check, "df.withColumn('x', explode(col('items')))"), "F014"); }
#[test] fn f014_no_transform()     { assert_no_violation(&check(f014::check, "df.withColumn('x', transform(col('items'), lambda i: i))"), "F014"); }

// ── F015: consecutive filter() calls ─────────────────────────────────────────
#[test] fn f015_fires_filter_filter()  { assert_violation(&check(f015::check, "df.filter(col('a') > 1).filter(col('b') == 2)"), "F015", 1); }
#[test] fn f015_fires_where_where()    { assert_violation(&check(f015::check, "df.where(col('a') > 1).where(col('b') == 2)"), "F015", 1); }
#[test] fn f015_fires_filter_where()   { assert_violation(&check(f015::check, "df.filter(col('a') > 1).where(col('b') == 2)"), "F015", 1); }
#[test] fn f015_fires_triple()         { assert_violation(&check(f015::check, "df.filter(col('a') > 1).filter(col('b') == 2).filter(col('c') < 5)"), "F015", 1); }
#[test] fn f015_no_single_filter()     { assert_no_violation(&check(f015::check, "df.filter((col('a') > 1) & (col('b') == 2))"), "F015"); }
#[test] fn f015_no_filter_then_select(){ assert_no_violation(&check(f015::check, "df.filter(col('a') > 1).select('a', 'b')"), "F015"); }

// ── F016: long DataFrame renaming chains ─────────────────────────────────────
#[test] fn f016_fires_three_renames() {
    let src = "df_a = df.filter(col('x') > 1)\ndf_b = df_a.distinct()\ndf_c = df_b.join(ref_df, 'id')";
    assert_violation(&check(f016::check, src), "F016", 3);
}
#[test] fn f016_fires_numbered() {
    let src = "df1 = df.filter(col('x') > 1)\ndf2 = df1.distinct()\ndf3 = df2.join(ref_df, 'id')";
    assert_violation(&check(f016::check, src), "F016", 3);
}
#[test] fn f016_no_two_renames() {
    let src = "df_a = df.filter(col('x') > 1)\ndf_b = df_a.distinct()";
    assert_no_violation(&check(f016::check, src), "F016");
}
#[test] fn f016_no_self_overwrite() {
    let src = "df = df.filter(col('x') > 1)\ndf = df.distinct()\ndf = df.join(ref_df, 'id')";
    assert_no_violation(&check(f016::check, src), "F016");
}
#[test] fn f016_no_exactly_two_renames() {
    let src = "df_active = df.filter(col('active') == True)\ndf_joined = df_active.join(ref_df, 'id')";
    assert_no_violation(&check(f016::check, src), "F016");
}
