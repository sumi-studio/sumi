pub const SYSTEM_PROMPT: &str = include_str!("../prompts/system.md");
pub const SYSTEM_PROMPT_VERSION: &str = "1";

pub(crate) fn compact_format_instructions() -> &'static str {
    without_trailing_newline(include_str!("../prompts/compact-format-instructions.md"))
}

pub(crate) fn compact_system_prompt() -> &'static str {
    without_trailing_newline(include_str!("../prompts/compact-system.md"))
}

fn without_trailing_newline(prompt: &'static str) -> &'static str {
    // The former inline prompt strings had no line ending, so exclude the
    // Markdown files' terminal line ending from the provider request.
    prompt
        .strip_suffix("\r\n")
        .or_else(|| prompt.strip_suffix('\n'))
        .unwrap_or(prompt)
}

#[cfg(test)]
mod tests {
    use super::{compact_format_instructions, compact_system_prompt};
    use sha2::{Digest, Sha256};

    #[test]
    fn compact_prompt_file_line_endings_are_not_sent() {
        assert!(!compact_system_prompt().ends_with('\n'));
        assert!(!compact_format_instructions().ends_with('\n'));
    }

    #[test]
    fn compact_prompts_preserve_the_extracted_wire_text() {
        // These hashes pin the former inline strings without duplicating the
        // prompt bodies outside their Markdown source files.
        assert_eq!(
            format!("{:x}", Sha256::digest(compact_system_prompt().as_bytes())),
            "25892439577d9bfc78e26d1f3b4547b77fe2e36f7566197aba2dbdf151dfa14d"
        );
        assert_eq!(
            format!(
                "{:x}",
                Sha256::digest(compact_format_instructions().as_bytes())
            ),
            "4e2361a5c0e97c479dd3e7e50e4b84f3390a4d9799c3d1392f2ae2feb88ca195"
        );
    }
}
