//! Configuration types loaded from `[tool.pyspark-antipattern]` in `pyproject.toml`.
//! Provides rule selection, severity filtering, and cross-file cost maps used
//! by the checker pre-pass.
use std::collections::HashMap;

use serde::Deserialize;

/// Top-level `pyproject.toml` wrapper used for TOML deserialization.
#[derive(Debug, Deserialize, Default)]
pub struct PyprojectToml {
    pub tool: Option<ToolSection>,
}

/// The `[tool]` section of `pyproject.toml`, which may contain the
/// `pyspark-antipattern` configuration block.
#[derive(Debug, Deserialize, Default)]
pub struct ToolSection {
    #[serde(rename = "pyspark-antipattern")]
    pub pyspark_antipattern: Option<Config>,
}

/// Runtime configuration for the linter, deserialized from `pyproject.toml`
/// and enriched by the checker pre-pass with cross-file cost maps.
#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct Config {
    pub select: Vec<String>,
    pub warn: Vec<String>,
    pub ignore: Vec<String>,
    pub show_best_practice: bool,
    pub show_information: bool,
    pub distinct_threshold: usize,
    pub explode_threshold: usize,
    pub loop_threshold: usize,
    pub exclude_dirs: Vec<String>,
    pub max_shuffle_operations: usize,
    /// Only report violations with impact >= this level (default: show all).
    pub severity: Option<crate::violation::Impact>,
    /// Cluster PySpark version — silences rules that require a newer version (default: show all).
    pub pyspark_version: Option<crate::violation::PySparkVersion>,
    /// Populated at check time by the pre-pass in checker.rs — not read from pyproject.toml.
    /// Uses i64 (same as distinct/explode costs) so all three cost maps share one type.
    #[serde(skip)]
    pub global_fn_costs: HashMap<String, i64>,
    /// Per-function weighted distinct() cost — populated by the pre-pass.
    #[serde(skip)]
    pub global_fn_distinct_costs: HashMap<String, i64>,
    /// Per-function weighted explode() cost — populated by the pre-pass.
    #[serde(skip)]
    pub global_fn_explode_costs: HashMap<String, i64>,
}

/// Returns the default list of directory names that the linter skips during
/// directory traversal (e.g. `.git`, `venv`, `node_modules`).
pub fn default_exclude_dirs() -> Vec<String> {
    [
        ".bzr",
        ".direnv",
        ".eggs",
        ".git",
        ".git-rewrite",
        ".hg",
        ".mypy_cache",
        ".nox",
        ".pants.d",
        ".pytype",
        ".ruff_cache",
        ".svn",
        ".tox",
        ".venv",
        "__pypackages__",
        "_build",
        "buck-out",
        "dist",
        "node_modules",
        "venv",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect()
}

impl Default for Config {
    fn default() -> Self {
        Self {
            select: vec![],
            warn: vec![],
            ignore: vec![],
            show_best_practice: false,
            show_information: false,
            distinct_threshold: 5,
            explode_threshold: 3,
            loop_threshold: 10,
            exclude_dirs: default_exclude_dirs(),
            max_shuffle_operations: 9,
            severity: None,
            pyspark_version: None,
            global_fn_costs: HashMap::new(),
            global_fn_distinct_costs: HashMap::new(),
            global_fn_explode_costs: HashMap::new(),
        }
    }
}

impl Config {
    /// Returns `true` when `dir_name` matches an entry in the exclusion list.
    pub fn is_excluded_dir(&self, dir_name: &str) -> bool {
        self.exclude_dirs.iter().any(|d| d == dir_name)
    }

    /// Load config from `path`.
    ///
    /// Returns:
    /// - `Ok(Some(config))` — file found and parsed successfully
    /// - `Ok(None)`         — file not found (caller should use `Config::default()`)
    /// - `Err(msg)`         — file found but contains invalid TOML or malformed config
    pub fn load(path: &std::path::Path) -> Result<Option<Self>, String> {
        let raw = match std::fs::read_to_string(path) {
            Ok(s) => s,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(format!("Cannot read {}: {e}", path.display())),
        };
        let parsed: PyprojectToml =
            toml::from_str(&raw).map_err(|e| format!("TOML parse error in {}: {e}", path.display()))?;
        Ok(parsed.tool.and_then(|t| t.pyspark_antipattern))
    }

    /// Returns true if `id` matches a rule entry exactly ("F012") or by group prefix ("F", "PERF", "ARR", …).
    /// Prefix matching requires the first character after the prefix to be a digit, so "S" matches
    /// "S001" but not "SHUFFLE001", and "PERF" matches "PERF003" but not "PERFORM".
    fn matches(entry: &str, id: &str) -> bool {
        if entry == id {
            return true;
        }
        if let Some(rest) = id.strip_prefix(entry) {
            return rest.starts_with(|c: char| c.is_ascii_digit());
        }
        false
    }

    /// Returns `true` when the rule `id` should be suppressed based on the
    /// `select` and `ignore` lists in the configuration.
    pub fn is_ignored(&self, id: &str) -> bool {
        // select acts as a selector: when non-empty, only listed rules are shown
        if !self.select.is_empty() && !self.select.iter().any(|r| Self::matches(r, id)) {
            return true;
        }
        self.ignore.iter().any(|r| Self::matches(r, id))
    }

    /// Returns the configured `Severity` for rule `id` (`Error` by default,
    /// `Warning` when the rule appears in the `warn` list).
    pub fn severity_of(&self, id: &str) -> crate::violation::Severity {
        use crate::violation::Severity;
        if self.warn.iter().any(|r| Self::matches(r, id)) {
            return Severity::Warning;
        }
        Severity::Error
    }

    /// Returns `true` when a violation with the given `impact` should be shown
    /// (i.e. its impact meets or exceeds `min_severity`).
    pub fn meets_min_severity(&self, impact: crate::violation::Impact) -> bool {
        match self.severity {
            None => true,
            Some(min) => impact >= min,
        }
    }

    /// Returns `true` when a rule with the given `since` version should be shown
    /// (i.e. the rule's minimum version is <= the user's configured cluster version).
    pub fn supports_rule_version(&self, since: crate::violation::PySparkVersion) -> bool {
        match self.pyspark_version {
            None => true,
            Some(user_ver) => since <= user_ver,
        }
    }
}
