pub const SYSTEM_PROMPT: &str = include_str!("../prompts/system.md");
pub const SYSTEM_PROMPT_VERSION: &str = "3";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CompactPrompt {
    L0ToL1,
    L1ToL2,
    ConsolidateL2,
}

impl CompactPrompt {
    pub(crate) fn as_str(self) -> &'static str {
        let prompt = match self {
            Self::L0ToL1 => include_str!("../prompts/compact-l0-to-l1.md"),
            Self::L1ToL2 => include_str!("../prompts/compact-l1-to-l2.md"),
            Self::ConsolidateL2 => include_str!("../prompts/compact-l2-consolidation.md"),
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
    fn compact_prompts_are_stage_specific_and_self_contained() {
        for (prompt, stage) in [
            (CompactPrompt::L0ToL1, "L0の会話履歴をL1の要約へ"),
            (CompactPrompt::L1ToL2, "複数のL1要約を1つのL2要約へ"),
            (CompactPrompt::ConsolidateL2, "L2の要約群を1つのL2要約へ"),
        ] {
            let text = prompt.as_str();
            assert!(text.contains(stage));
            assert!(text.contains("指定フォーマット:"));
            assert!(text.contains("## 出来事"));
            assert!(text.contains("## 参照"));
            assert!(!text.ends_with('\n'));
        }
    }
}
