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

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct Config {
    pub failing_rules:      Vec<String>,
    pub warning_rules:      Vec<String>,
    pub ignore_rules:       Vec<String>,
    pub show_best_practice: bool,
    pub show_information:   bool,
    pub distinct_threshold: usize,
    pub explode_threshold:  usize,
    pub loop_threshold:     usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            failing_rules:      vec![],
            warning_rules:      vec![],
            ignore_rules:       vec![],
            show_best_practice: false,
            show_information:   false,
            distinct_threshold: 5,
            explode_threshold:  3,
            loop_threshold:     10,
        }
    }
}

impl Config {
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

    pub fn severity_of(&self, id: &str) -> crate::violation::Severity {
        use crate::violation::Severity;
        if self.warning_rules.iter().any(|r| r == id) {
            return Severity::Warning;
        }
        Severity::Error
    }
}
