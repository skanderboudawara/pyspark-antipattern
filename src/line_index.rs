/// Maps byte offsets to 1-based (line, column) pairs.
pub struct LineIndex {
    /// Byte offset of the start of each line (index 0 = line 1).
    line_starts: Vec<u32>,
}

impl LineIndex {
    pub fn new(source: &str) -> Self {
        let mut starts = vec![0u32];
        for (i, b) in source.bytes().enumerate() {
            if b == b'\n' {
                starts.push((i + 1) as u32);
            }
        }
        LineIndex { line_starts: starts }
    }

    /// Convert a byte offset to 1-based (line, char-col).
    ///
    /// Column is counted in Unicode scalar values (chars), not bytes, so that
    /// carets are correctly aligned even in files containing emoji or CJK text.
    pub fn line_col(&self, offset: u32, source: &str) -> (usize, usize) {
        let line_idx = self.line_starts.partition_point(|&s| s <= offset).saturating_sub(1);
        let line_start = self.line_starts[line_idx] as usize;
        let byte_end = (offset as usize).min(source.len());
        // Count Unicode scalar values from the start of the line to `offset`.
        let char_col = source[line_start..byte_end].chars().count();
        (line_idx + 1, char_col + 1)
    }

    /// Get the text of a 1-based line number (without trailing newline).
    pub fn line_text<'a>(&self, source: &'a str, line: usize) -> &'a str {
        let idx = line.saturating_sub(1);
        if idx >= self.line_starts.len() {
            return "";
        }
        let start = self.line_starts[idx] as usize;
        let end = if idx + 1 < self.line_starts.len() {
            (self.line_starts[idx + 1] as usize).saturating_sub(1) // drop the \n
        } else {
            source.len()
        };
        let end = end.min(source.len());
        if start > end {
            return "";
        }
        &source[start..end]
    }
}
