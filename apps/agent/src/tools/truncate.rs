//! Bounded views for tool output.

use serde::{Deserialize, Serialize};

pub const DEFAULT_MAX_LINES: usize = 2_000;
pub const DEFAULT_MAX_BYTES: usize = 50 * 1024;
pub const GREP_MAX_LINE_LENGTH: usize = 500;
pub const GREP_TRUNCATION_SUFFIX: &str = "... [truncated]";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetainedOutput {
    Head,
    Tail,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TruncatedBy {
    Lines,
    Bytes,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TruncationOptions {
    pub max_lines: usize,
    pub max_bytes: usize,
}

impl Default for TruncationOptions {
    fn default() -> Self {
        Self {
            max_lines: DEFAULT_MAX_LINES,
            max_bytes: DEFAULT_MAX_BYTES,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TruncationResult {
    pub content: String,
    pub truncated: bool,
    pub truncated_by: Option<TruncatedBy>,
    pub total_lines: usize,
    pub total_bytes: usize,
    pub output_lines: usize,
    pub output_bytes: usize,
    pub last_line_partial: bool,
    pub first_line_exceeds_limit: bool,
    pub max_lines: usize,
    pub max_bytes: usize,
}

pub fn truncate_head(content: &str, options: TruncationOptions) -> TruncationResult {
    if content.is_empty() {
        return unchanged(content, 0, 0, options);
    }
    let total_bytes = content.len();
    // A trailing newline terminates the preceding line; it does not create
    // another semantic line for the bounded line contract. Keep the original
    // content intact when it fits, including that newline.
    let mut lines = content.split('\n').collect::<Vec<_>>();
    if lines.len() > 1 && lines.last() == Some(&"") {
        lines.pop();
    }
    let total_lines = lines.len();

    if total_lines <= options.max_lines && total_bytes <= options.max_bytes {
        return unchanged(content, total_lines, total_bytes, options);
    }

    if lines
        .first()
        .is_some_and(|line| line.len() > options.max_bytes)
    {
        return TruncationResult {
            content: String::new(),
            truncated: true,
            truncated_by: Some(TruncatedBy::Bytes),
            total_lines,
            total_bytes,
            output_lines: 0,
            output_bytes: 0,
            last_line_partial: false,
            first_line_exceeds_limit: true,
            max_lines: options.max_lines,
            max_bytes: options.max_bytes,
        };
    }

    let mut output = Vec::new();
    let mut bytes = 0usize;
    let mut truncated_by = TruncatedBy::Lines;
    for (index, line) in lines.iter().take(options.max_lines).enumerate() {
        let line_bytes = line.len().saturating_add(usize::from(index > 0));
        if bytes.saturating_add(line_bytes) > options.max_bytes {
            truncated_by = TruncatedBy::Bytes;
            break;
        }
        output.push(*line);
        bytes += line_bytes;
    }
    if output.len() >= options.max_lines && bytes <= options.max_bytes {
        truncated_by = TruncatedBy::Lines;
    }

    let content = output.join("\n");
    TruncationResult {
        output_lines: output.len(),
        output_bytes: content.len(),
        content,
        truncated: true,
        truncated_by: Some(truncated_by),
        total_lines,
        total_bytes,
        last_line_partial: false,
        first_line_exceeds_limit: false,
        max_lines: options.max_lines,
        max_bytes: options.max_bytes,
    }
}

pub fn truncate_tail(content: &str, options: TruncationOptions) -> TruncationResult {
    if content.is_empty() {
        return unchanged(content, 0, 0, options);
    }
    let total_bytes = content.len();
    let mut lines = content.split('\n').collect::<Vec<_>>();
    if lines.len() > 1 && lines.last() == Some(&"") {
        lines.pop();
    }
    let total_lines = lines.len();

    if total_lines <= options.max_lines && total_bytes <= options.max_bytes {
        return unchanged(content, total_lines, total_bytes, options);
    }

    let mut output = Vec::new();
    let mut bytes = 0usize;
    let mut truncated_by = TruncatedBy::Lines;
    let mut last_line_partial = false;

    for line in lines.iter().rev().take(options.max_lines) {
        let line_bytes = line.len().saturating_add(usize::from(!output.is_empty()));
        if bytes.saturating_add(line_bytes) > options.max_bytes {
            truncated_by = TruncatedBy::Bytes;
            if output.is_empty() {
                let tail = string_tail_at_char_boundary(line, options.max_bytes);
                bytes = tail.len();
                output.push(tail);
                last_line_partial = true;
            }
            break;
        }
        output.push(*line);
        bytes += line_bytes;
    }
    output.reverse();
    if output.len() >= options.max_lines && bytes <= options.max_bytes {
        truncated_by = TruncatedBy::Lines;
    }

    let content = output.join("\n");
    TruncationResult {
        output_lines: output.len(),
        output_bytes: content.len(),
        content,
        truncated: true,
        truncated_by: Some(truncated_by),
        total_lines,
        total_bytes,
        last_line_partial,
        first_line_exceeds_limit: false,
        max_lines: options.max_lines,
        max_bytes: options.max_bytes,
    }
}

fn unchanged(
    content: &str,
    total_lines: usize,
    total_bytes: usize,
    options: TruncationOptions,
) -> TruncationResult {
    TruncationResult {
        content: content.to_owned(),
        truncated: false,
        truncated_by: None,
        total_lines,
        total_bytes,
        output_lines: total_lines,
        output_bytes: total_bytes,
        last_line_partial: false,
        first_line_exceeds_limit: false,
        max_lines: options.max_lines,
        max_bytes: options.max_bytes,
    }
}

fn string_tail_at_char_boundary(value: &str, max_bytes: usize) -> &str {
    if max_bytes == 0 {
        return "";
    }
    if value.len() <= max_bytes {
        return value;
    }
    let mut start = value.len() - max_bytes;
    while start < value.len() && !value.is_char_boundary(start) {
        start += 1;
    }
    &value[start..]
}

pub fn truncate_line(line: &str, max_chars: usize) -> (String, bool) {
    let mut indices = line.char_indices();
    let end = match indices.nth(max_chars) {
        Some((index, _)) => index,
        None => return (line.to_owned(), false),
    };
    (format!("{}{GREP_TRUNCATION_SUFFIX}", &line[..end]), true)
}

/// Truncate a line so the complete rendered value, including its suffix, fits
/// within `max_chars` Unicode scalar values.
pub fn truncate_line_total(line: &str, max_chars: usize) -> (String, bool) {
    let suffix_len = GREP_TRUNCATION_SUFFIX.chars().count();
    if line.chars().count() <= max_chars {
        return (line.to_owned(), false);
    }
    let prefix_limit = max_chars.saturating_sub(suffix_len);
    let prefix = line.chars().take(prefix_limit).collect::<String>();
    (format!("{prefix}{GREP_TRUNCATION_SUFFIX}"), true)
}

pub fn format_size(bytes: usize) -> String {
    if bytes < 1024 {
        format!("{bytes}B")
    } else if bytes < 1024 * 1024 {
        format!("{:.1}KB", bytes as f64 / 1024.0)
    } else {
        format!("{:.1}MB", bytes as f64 / (1024.0 * 1024.0))
    }
}

pub fn truncation_note(
    result: &TruncationResult,
    direction: &str,
    artifact_handle: Option<&str>,
) -> Option<String> {
    if !result.truncated {
        return None;
    }
    let shown = if result.content.is_empty() {
        "内容なし".to_owned()
    } else {
        format!("{direction}{}行", result.output_lines)
    };
    let artifact = artifact_handle
        .map(|handle| format!("。全文: {handle}"))
        .unwrap_or_default();
    let reason = match (result.truncated_by, result.first_line_exceeds_limit) {
        (_, true) => "先頭行がバイト上限を超過",
        (Some(TruncatedBy::Bytes), false) => "バイト上限",
        (Some(TruncatedBy::Lines), false) => "行上限",
        (None, false) => "注記用領域の確保",
    };
    let formatted_size = format_size(result.total_bytes);
    let total_bytes = if result.total_bytes < 1024 {
        formatted_size
    } else {
        format!("{}B ({formatted_size})", result.total_bytes)
    };
    Some(format!(
        "[出力 {}行/{} のうち{shown}を表示。続きあり: {reason}{artifact}]",
        result.total_lines, total_bytes,
    ))
}

/// Renders retained output, its truncation note, and terminal status lines
/// inside the single model-visible 50 KiB / 2,000-line envelope.
pub fn render_bounded_output(
    result: &TruncationResult,
    retained: RetainedOutput,
    artifact_handle: Option<&str>,
    terminal_lines: &[String],
) -> String {
    let original = result.clone();
    let mut view = original.clone();

    for _ in 0..32 {
        let note = truncation_note(
            &view,
            match retained {
                RetainedOutput::Head => "先頭",
                RetainedOutput::Tail => "末尾",
            },
            artifact_handle,
        );
        let mut annotations =
            Vec::with_capacity(terminal_lines.len() + usize::from(note.is_some()));
        if let Some(note) = note {
            annotations.push(note);
        }
        annotations.extend(terminal_lines.iter().cloned());

        let annotation_bytes = annotations.iter().map(String::len).sum::<usize>()
            + annotations.len().saturating_sub(1);
        let separator_bytes =
            usize::from(!view.content.is_empty() && !view.content.ends_with('\n'));
        let available_bytes = DEFAULT_MAX_BYTES
            .saturating_sub(annotation_bytes)
            .saturating_sub(separator_bytes);
        let available_lines = DEFAULT_MAX_LINES.saturating_sub(annotations.len());
        let restricted = match retained {
            RetainedOutput::Head => truncate_head(
                &original.content,
                TruncationOptions {
                    max_lines: available_lines,
                    max_bytes: available_bytes,
                },
            ),
            RetainedOutput::Tail => truncate_tail(
                &original.content,
                TruncationOptions {
                    max_lines: available_lines,
                    max_bytes: available_bytes,
                },
            ),
        };
        let mut next = restricted.clone();
        next.total_lines = original.total_lines;
        next.total_bytes = original.total_bytes;
        next.max_lines = DEFAULT_MAX_LINES;
        next.max_bytes = DEFAULT_MAX_BYTES;
        if original.truncated || restricted.truncated {
            next.truncated = true;
            if !restricted.truncated {
                next.truncated_by = original.truncated_by;
                next.first_line_exceeds_limit = original.first_line_exceeds_limit;
                next.last_line_partial = original.last_line_partial;
            }
        }

        if next == view {
            return join_annotations(next.content, annotations);
        }
        view = next;
    }

    // Every current annotation is bounded metadata. This fallback preserves
    // terminal truth even if a future annotation unexpectedly changes size.
    let mut annotations = terminal_lines.to_vec();
    if let Some(note) = truncation_note(
        &view,
        match retained {
            RetainedOutput::Head => "先頭",
            RetainedOutput::Tail => "末尾",
        },
        artifact_handle,
    ) {
        annotations.insert(0, note);
    }
    let annotations = annotations.join("\n");
    if annotations.len() >= DEFAULT_MAX_BYTES {
        return truncate_head(
            &annotations,
            TruncationOptions {
                max_lines: DEFAULT_MAX_LINES,
                max_bytes: DEFAULT_MAX_BYTES,
            },
        )
        .content;
    }
    join_annotations(
        view.content,
        annotations.lines().map(str::to_owned).collect(),
    )
}

fn join_annotations(mut content: String, annotations: Vec<String>) -> String {
    if annotations.is_empty() {
        return content;
    }
    if !content.is_empty() && !content.ends_with('\n') {
        content.push('\n');
    }
    content.push_str(&annotations.join("\n"));
    content
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(max_bytes: usize, max_lines: usize) -> TruncationOptions {
        TruncationOptions {
            max_lines,
            max_bytes,
        }
    }

    #[test]
    fn counts_utf8_bytes() {
        let content = "aé🙂\nb";
        let result = truncate_head(content, options(100, 10));
        assert!(!result.truncated);
        assert_eq!(result.total_bytes, 9);
        assert_eq!(result.output_bytes, 9);
    }

    #[test]
    fn terminal_newline_does_not_create_an_extra_semantic_line_at_boundary() {
        for lines in [
            DEFAULT_MAX_LINES - 1,
            DEFAULT_MAX_LINES,
            DEFAULT_MAX_LINES + 1,
        ] {
            for terminal_newline in [false, true] {
                let mut input = "x\n".repeat(lines);
                if !terminal_newline {
                    input.pop();
                }
                let result = truncate_head(&input, TruncationOptions::default());
                assert_eq!(result.total_lines, lines);
                assert_eq!(result.output_lines, lines.min(DEFAULT_MAX_LINES));
                assert_eq!(result.truncated, lines > DEFAULT_MAX_LINES);
                if lines <= DEFAULT_MAX_LINES {
                    assert_eq!(result.content, input);
                    assert_eq!(result.output_bytes, input.len());
                }
            }
        }
    }

    #[test]
    fn head_uses_utf8_byte_limits_without_partial_lines() {
        let result = truncate_head("éé\nabc", options(4, 10));
        assert_eq!(result.content, "éé");
        assert_eq!(result.truncated_by, Some(TruncatedBy::Bytes));
        assert_eq!(result.output_bytes, 4);
        assert!(!result.first_line_exceeds_limit);
    }

    #[test]
    fn head_reports_oversized_first_line() {
        let result = truncate_head("éé\nabc", options(3, 10));
        assert!(result.content.is_empty());
        assert!(result.first_line_exceeds_limit);
    }

    #[test]
    fn empty_input_has_zero_lines_and_zero_output_lines() {
        for result in [
            truncate_head("", options(10, 10)),
            truncate_tail("", options(10, 10)),
        ] {
            assert_eq!(result.total_lines, 0);
            assert_eq!(result.output_lines, 0);
            assert_eq!(result.total_bytes, 0);
            assert_eq!(result.output_bytes, 0);
        }
    }

    #[test]
    fn tail_stays_on_utf8_boundaries() {
        let result = truncate_tail("aé🙂b", options(5, 10));
        assert_eq!(result.content, "🙂b");
        assert!(result.last_line_partial);
        assert_eq!(result.output_bytes, 5);
    }

    #[test]
    fn tail_handles_oversized_line_with_newline() {
        let input = format!("{}\n", "X".repeat(300_000));
        let result = truncate_tail(&input, options(1024, 100));
        assert_eq!(result.content, "X".repeat(1024));
        assert_eq!(result.output_lines, 1);
        assert!(result.last_line_partial);
    }

    #[test]
    fn tail_drops_character_that_does_not_fit() {
        let result = truncate_tail("abc🙂", options(3, 10));
        assert!(result.content.is_empty());
        assert!(result.last_line_partial);
    }

    #[test]
    fn first_limit_wins() {
        let content = "a\nb\nc\nd";
        let line_limited = truncate_head(content, options(100, 2));
        assert_eq!(line_limited.content, "a\nb");
        assert_eq!(line_limited.truncated_by, Some(TruncatedBy::Lines));

        let byte_limited = truncate_head(content, options(3, 10));
        assert_eq!(byte_limited.content, "a\nb");
        assert_eq!(byte_limited.truncated_by, Some(TruncatedBy::Bytes));
    }

    #[test]
    fn grep_lines_are_char_bounded() {
        let (line, truncated) = truncate_line("日本語abc", 3);
        assert!(truncated);
        assert_eq!(line, "日本語... [truncated]");
    }

    #[test]
    fn grep_rendered_line_limit_reserves_truncation_suffix() {
        let exact = "界".repeat(GREP_MAX_LINE_LENGTH);
        let (line, truncated) = truncate_line_total(&exact, GREP_MAX_LINE_LENGTH);
        assert!(!truncated);
        assert_eq!(line.chars().count(), GREP_MAX_LINE_LENGTH);

        let over = format!("{}x", exact);
        let (line, truncated) = truncate_line_total(&over, GREP_MAX_LINE_LENGTH);
        assert!(truncated);
        assert_eq!(line.chars().count(), GREP_MAX_LINE_LENGTH);
        assert!(line.ends_with(GREP_TRUNCATION_SUFFIX));
    }

    #[test]
    fn note_contains_limits_and_handle() {
        let result = truncate_tail("a\nb\nc", options(100, 2));
        let note = truncation_note(
            &result,
            "末尾",
            Some("artifact://conversation/tool-output/bash-1"),
        )
        .expect("truncated output has a note");
        assert!(note.contains("3行/5B"));
        assert!(note.contains("末尾2行"));
        assert!(note.contains("続きあり: 行上限"));
        assert!(note.contains("artifact://conversation/tool-output/bash-1"));
    }

    #[test]
    fn annotations_and_terminal_truth_share_the_exact_result_envelope() {
        let line_result = truncate_tail(
            &"x\n".repeat(DEFAULT_MAX_LINES),
            TruncationOptions::default(),
        );
        assert!(!line_result.truncated);
        let rendered = render_bounded_output(
            &line_result,
            RetainedOutput::Tail,
            None,
            &["[terminal: exit_code=7]".to_owned()],
        );
        assert!(rendered.len() <= DEFAULT_MAX_BYTES);
        assert!(rendered.lines().count() <= DEFAULT_MAX_LINES);
        assert!(rendered.contains("続きあり:"));
        assert!(rendered.ends_with("[terminal: exit_code=7]"));

        let byte_result =
            truncate_tail(&"x".repeat(DEFAULT_MAX_BYTES), TruncationOptions::default());
        assert!(!byte_result.truncated);
        let rendered = render_bounded_output(
            &byte_result,
            RetainedOutput::Tail,
            None,
            &["[terminal: cancelled=true]".to_owned()],
        );
        assert!(rendered.len() <= DEFAULT_MAX_BYTES);
        assert!(rendered.lines().count() <= DEFAULT_MAX_LINES);
        assert!(rendered.contains("続きあり: バイト上限"));
        assert!(rendered.ends_with("[terminal: cancelled=true]"));
    }
}
