/// Single source of truth for all Spark operation classifications.
///
/// Rules reference these constants instead of maintaining their own lists.

// ── Shuffle-inducing operations ───────────────────────────────────────────────

/// Methods on a Spark DataFrame / RDD that trigger a shuffle (network data
/// exchange).  Used by PERF003 to count shuffle operations per file and by
/// F016 to recognise DataFrame method chains.
pub static SHUFFLE_OPS: &[&str] = &[
    // DataFrame / Dataset API
    "groupBy",
    "agg",
    "join",
    "crossJoin",
    "repartition",
    "distinct",
    "dropDuplicates",
    "orderBy",
    "sort",
    "sortWithinPartitions",
    // RDD API
    "reduceByKey",
    "groupByKey",
    "aggregateByKey",
    "combineByKey",
    "cogroup",
    "cartesian",
    "intersection",
    "subtractByKey",
    "leftOuterJoin",
    "rightOuterJoin",
    "fullOuterJoin",
];

// ── Checkpoint operations ─────────────────────────────────────────────────────

/// Methods that materialise a DataFrame / RDD to storage, resetting the
/// lineage.  Used by PERF003 as "free" reset points in the shuffle counter.
pub static CHECKPOINT_OPS: &[&str] = &["checkpoint", "localCheckpoint"];

// ── DataFrame-exclusive methods ───────────────────────────────────────────────

/// Methods that exist *only* on Spark DataFrames / related objects and never
/// on plain Python types (str, dict, list, …).
///
/// Used by F016 to decide whether an assignment RHS is a DataFrame chain, and
/// by any rule that needs to distinguish DataFrame calls from stdlib calls.
pub static DATAFRAME_METHODS: &[&str] = &[
    "__getattr__",
    "__getitem__",
    "agg",
    "alias",
    "approxQuantile",
    "asTable",
    "cache",
    "checkpoint",
    "coalesce",
    "colRegex",
    "collect",
    "columns",
    "corr",
    "count",
    "cov",
    "createGlobalTempView",
    "createOrReplaceGlobalTempView",
    "createOrReplaceTempView",
    "createTempView",
    "crossJoin",
    "crosstab",
    "cube",
    "describe",
    "distinct",
    "drop",
    "dropDuplicates",
    "dropDuplicatesWithinWatermark",
    "drop_duplicates",
    "dropna",
    "dtypes",
    "exceptAll",
    "executionInfo",
    "exists",
    "explain",
    "fillna",
    "filter",
    "first",
    "foreach",
    "foreachPartition",
    "freqItems",
    "groupBy",
    "groupingSets",
    "head",
    "hint",
    "inputFiles",
    "intersect",
    "intersectAll",
    "isEmpty",
    "isLocal",
    "isStreaming",
    "join",
    "limit",
    "lateralJoin",
    "localCheckpoint",
    "mapInPandas",
    "mapInArrow",
    "metadataColumn",
    "melt",
    "na",
    "observe",
    "offset",
    "orderBy",
    "persist",
    "plot",
    "printSchema",
    "randomSplit",
    "rdd",
    "registerTempTable",
    "repartition",
    "repartitionByRange",
    "replace",
    "rollup",
    "sameSemantics",
    "sample",
    "sampleBy",
    "scalar",
    "schema",
    "select",
    "selectExpr",
    "semanticHash",
    "show",
    "sort",
    "sortWithinPartitions",
    "sparkSession",
    "stat",
    "storageLevel",
    "subtract",
    "summary",
    "tail",
    "take",
    "to",
    "toArrow",
    "toDF",
    "toJSON",
    "toLocalIterator",
    "toPandas",
    "transform",
    "transpose",
    "union",
    "unionAll",
    "unionByName",
    "unpersist",
    "unpivot",
    "where",
    "withColumn",
    "withColumns",
    "withColumnRenamed",
    "withColumnsRenamed",
    "withMetadata",
    "withWatermark",
    "write",
    "writeStream",
    "writeTo",
    "mergeInto",
    "pandas_api",
];

// ── Explode operations ────────────────────────────────────────────────────────

/// Functions / methods that explode an array/map column into multiple rows.
/// Used by S008 and F014.
pub static EXPLODE_OPS: &[&str] = &["explode", "explode_outer"];

// ── Non-DataFrame receiver roots ──────────────────────────────────────────────

/// Root `Name` identifiers that indicate the receiver of a method call is a
/// stdlib/utility object — never a Spark DataFrame.
///
/// Used by `is_non_dataframe_receiver()` in `rules/utils.rs` to suppress false
/// positives (e.g. `os.path.join(...)` must not trigger S002 or F016).
pub static NON_DATAFRAME_ROOTS: &[&str] = &[
    "os",
    "sys",
    "pathlib",
    "str",
    "bytes",
    "urllib",
    "posixpath",
    "ntpath",
    "shutil",
    "Path",
    "PurePath",
    "PosixPath",
    "WindowsPath",
];
