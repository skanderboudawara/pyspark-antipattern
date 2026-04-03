use std::collections::HashMap;

use serde::Deserialize;

#[derive(Debug, Deserialize, Default)]
pub struct PyprojectToml {
    pub tool: Option<ToolSection>,
}

#[derive(Debug, Deserialize, Default)]
pub struct ToolSection {
    #[serde(rename = "pyspark-antipattern")]
    pub pyspark_antipattern: Option<Config>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct Config {
    pub select:  Vec<String>,
    pub warn:    Vec<String>,
    pub ignore:  Vec<String>,
    pub show_best_practice: bool,
    pub show_information:   bool,
    pub distinct_threshold: usize,
    pub explode_threshold:  usize,
    pub loop_threshold:     usize,
    pub exclude_dirs:            Vec<String>,
    pub max_shuffle_operations:  usize,
    /// Only report violations with impact >= this level (default: show all).
    pub severity: Option<crate::violation::Impact>,
    /// Populated at check time by the pre-pass in checker.rs — not read from pyproject.toml.
    #[serde(skip)]
    pub global_fn_costs: HashMap<String, usize>,
    /// Per-function weighted distinct() cost — populated by the pre-pass.
    #[serde(skip)]
    pub global_fn_distinct_costs: HashMap<String, i64>,
    /// Per-function weighted explode() cost — populated by the pre-pass.
    #[serde(skip)]
    pub global_fn_explode_costs: HashMap<String, i64>,
}

pub fn default_exclude_dirs() -> Vec<String> {
    [
        ".bzr", ".direnv", ".eggs", ".git", ".git-rewrite", ".hg",
        ".mypy_cache", ".nox", ".pants.d", ".pytype", ".ruff_cache",
        ".svn", ".tox", ".venv", "__pypackages__", "_build",
        "buck-out", "dist", "node_modules", "venv",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect()
}

impl Default for Config {
    fn default() -> Self {
        Self {
            select:  vec![],
            warn:    vec![],
            ignore:  vec![],
            show_best_practice: false,
            show_information:   false,
            distinct_threshold: 5,
            explode_threshold:  3,
            loop_threshold:     10,
            exclude_dirs:            default_exclude_dirs(),
            max_shuffle_operations:  9,
            severity:                None,
            global_fn_costs:         HashMap::new(),
            global_fn_distinct_costs: HashMap::new(),
            global_fn_explode_costs:  HashMap::new(),
        }
    }
}

impl Config {
    pub fn is_excluded_dir(&self, dir_name: &str) -> bool {
        self.exclude_dirs.iter().any(|d| d == dir_name)
    }

    pub fn load(path: &std::path::Path) -> Result<Self, String> {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| format!("Cannot read {}: {e}", path.display()))?;
        let parsed: PyprojectToml = toml::from_str(&raw)
            .map_err(|e| format!("TOML parse error: {e}"))?;
        Ok(parsed
            .tool
            .and_then(|t| t.pyspark_antipattern)
            .unwrap_or_default())
    }

    /// Returns true if `id` matches a rule entry exactly ("F012") or by group prefix ("F").
    fn matches(entry: &str, id: &str) -> bool {
        entry == id || (entry.len() == 1 && id.starts_with(entry))
    }

    pub fn is_ignored(&self, id: &str) -> bool {
        // select acts as a selector: when non-empty, only listed rules are shown
        if !self.select.is_empty() {
            if !self.select.iter().any(|r| Self::matches(r, id)) {
                return true;
            }
        }
        self.ignore.iter().any(|r| Self::matches(r, id))
    }

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
            None      => true,
            Some(min) => impact >= min,
        }
    }
}
