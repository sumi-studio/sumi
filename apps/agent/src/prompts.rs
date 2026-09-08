pub const SYSTEM_PROMPT: &str = include_str!("../prompts/system.md");
pub const SYSTEM_PROMPT_VERSION: &str = "4";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CompactPrompt {
    L0ToL1,
    L1ToL2,
    L2Reintegration,
}

impl CompactPrompt {
    pub(crate) fn as_str(self) -> &'static str {
        let prompt = match self {
            Self::L0ToL1 => include_str!("../prompts/compact-l0-to-l1.md"),
            Self::L1ToL2 => include_str!("../prompts/compact-l1-to-l2.md"),
            Self::L2Reintegration => include_str!("../prompts/compact-l2-reintegration.md"),
        };
        without_trailing_newline(prompt)
    }
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
    use super::{CompactPrompt, SYSTEM_PROMPT};

    #[test]
    fn system_prompt_uses_target_tool_routes_not_a_permission_tool() {
        assert!(!SYSTEM_PROMPT.contains("request_permission"));
        assert!(SYSTEM_PROMPT.contains("対象ツール自身を `normal` として呼ぶ"));
        assert!(SYSTEM_PROMPT.contains("対象ツール自身を `elevated` として提案する"));
    }

    #[test]
    fn compact_prompt_loads_without_trailing_newline() {
        let text = CompactPrompt::L0ToL1.as_str();
        assert!(!text.is_empty());
        assert!(!text.ends_with('\n'));
    }
}
