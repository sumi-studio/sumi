//! Deterministic L0 batch boundary decisions.

use std::collections::HashSet;

use crate::provider::types::{AssistantContent, ContextMessage, Message};

use super::estimate::TokenCalibration;
use super::{BatchState, L0_BATCH_MIN, L0_FORCED_SEAL_LIMIT, L0Batch};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MessageRole {
    User,
    Assistant,
    ToolResult,
}

impl MessageRole {
    pub fn of_context(context: &ContextMessage) -> Self {
        match context_message(context) {
            Message::User(_) => Self::User,
            Message::Assistant(_) => Self::Assistant,
            Message::ToolResult(_) => Self::ToolResult,
        }
    }
}

/// History facts needed to decide whether an open batch can be sealed before
/// `next`. Pending IDs are bounded by the tool calls present in `history`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundaryContext {
    previous_role: Option<MessageRole>,
    previous_assistant_interrupted: bool,
    next_role: MessageRole,
    next_user_is_steering: bool,
    pending_tool_call_ids: HashSet<String>,
    pending_rejected_tool_call_ids: HashSet<String>,
    malformed_tool_loop: bool,
}

impl BoundaryContext {
    pub fn from_history(
        history: &[ContextMessage],
        next: &ContextMessage,
        next_user_is_steering: bool,
    ) -> Self {
        let previous = history.last().map(context_message);
        let (pending_tool_call_ids, pending_rejected_tool_call_ids, malformed_tool_loop) =
            pending_tool_calls(history);
        Self {
            previous_role: history.last().map(MessageRole::of_context),
            previous_assistant_interrupted: matches!(
                previous,
                Some(Message::Assistant(message)) if message.interrupted
            ),
            next_role: MessageRole::of_context(next),
            next_user_is_steering,
            pending_tool_call_ids,
            pending_rejected_tool_call_ids,
            malformed_tool_loop,
        }
    }

    pub fn next_role(&self) -> MessageRole {
        self.next_role
    }

    pub fn unresolved_tool_loop(&self) -> bool {
        self.malformed_tool_loop
            || !self.pending_tool_call_ids.is_empty()
            || !self.pending_rejected_tool_call_ids.is_empty()
    }

    pub fn pending_tool_call_ids(&self) -> impl Iterator<Item = &str> {
        self.pending_tool_call_ids.iter().map(String::as_str)
    }

    pub fn pending_rejected_tool_call_ids(&self) -> impl Iterator<Item = &str> {
        self.pending_rejected_tool_call_ids
            .iter()
            .map(String::as_str)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SealReason {
    Normal,
    ForcedFootprint,
}

/// Decide whether the open tail may be sealed immediately before `next`.
/// Stored estimates remain raw; the calibration ratio is applied exactly once
/// to their combined total only for the forced-overflow comparison.
pub fn seal_before_next(
    batch: &L0Batch,
    boundary: &BoundaryContext,
    calibration: TokenCalibration,
) -> Option<SealReason> {
    if batch.state != BatchState::Open || !is_safe_boundary(boundary) {
        return None;
    }

    let effective = calibration.effective_tokens(batch.est_tokens, batch.eviction_footprint_tokens);
    let forced = match effective {
        Ok(tokens) => tokens > L0_FORCED_SEAL_LIMIT,
        Err(_) => true,
    };
    if forced {
        return Some(SealReason::ForcedFootprint);
    }

    // Ordinary cuts are strictly before a new user message. Assistant cuts
    // are therefore possible only through the forced fallback above.
    (batch.est_tokens >= L0_BATCH_MIN && boundary.next_role == MessageRole::User)
        .then_some(SealReason::Normal)
}

fn is_safe_boundary(boundary: &BoundaryContext) -> bool {
    // A continuation assistant immediately following a completed tool result
    // remains in the same tool flow, even though no call ID is pending.
    if boundary.unresolved_tool_loop()
        || boundary.next_role == MessageRole::ToolResult
        || (boundary.next_role == MessageRole::Assistant
            && boundary.previous_role == Some(MessageRole::ToolResult))
    {
        return false;
    }
    !(boundary.next_role == MessageRole::User
        && boundary.next_user_is_steering
        && boundary.previous_role == Some(MessageRole::Assistant)
        && boundary.previous_assistant_interrupted)
}

/// Track executable and rejected calls as separate expectations. Results remove
/// only their matching ID and expected kind; malformed, orphan, duplicate, and
/// missing cases remain fail-closed even if another result arrives.
fn pending_tool_calls(history: &[ContextMessage]) -> (HashSet<String>, HashSet<String>, bool) {
    let mut pending = HashSet::new();
    let mut pending_rejected = HashSet::new();
    let mut completed = HashSet::new();
    let mut completed_rejected = HashSet::new();
    let mut malformed = false;
    for context in history {
        match context_message(context) {
            Message::Assistant(message) => {
                // A new assistant can only begin a new flow after every
                // prior call has a matching result.  This permits a reused
                // provider ID across assistant flows while rejecting an
                // overlapping continuation, even when it uses another ID.
                if !pending.is_empty() || !pending_rejected.is_empty() {
                    malformed = true;
                } else {
                    completed.clear();
                    completed_rejected.clear();
                }
                for content in &message.content {
                    match content {
                        AssistantContent::ToolCall { tool_call, .. } => {
                            if pending.contains(&tool_call.id)
                                || pending_rejected.contains(&tool_call.id)
                                || completed.contains(&tool_call.id)
                                || completed_rejected.contains(&tool_call.id)
                            {
                                malformed = true;
                            } else {
                                pending.insert(tool_call.id.clone());
                            }
                        }
                        AssistantContent::RejectedToolCall { rejected, .. } => {
                            if pending.contains(&rejected.id)
                                || pending_rejected.contains(&rejected.id)
                                || completed.contains(&rejected.id)
                                || completed_rejected.contains(&rejected.id)
                            {
                                malformed = true;
                            } else {
                                pending_rejected.insert(rejected.id.clone());
                            }
                        }
                        _ => {}
                    }
                }
            }
            Message::ToolResult(message) => {
                if pending.remove(&message.tool_call_id) {
                    completed.insert(message.tool_call_id.clone());
                } else if pending_rejected.contains(&message.tool_call_id) {
                    if message.is_error {
                        pending_rejected.remove(&message.tool_call_id);
                        completed_rejected.insert(message.tool_call_id.clone());
                    } else {
                        malformed = true;
                    }
                } else {
                    malformed = true;
                }
            }
            // IDs may be reused in a later user turn after a complete loop.
            // A user arriving while a call is pending proves the loop was
            // interrupted; retain the malformed marker even if a late result
            // later happens to remove the pending ID.
            Message::User(_) => {
                if !pending.is_empty() || !pending_rejected.is_empty() {
                    malformed = true;
                }
                completed.clear();
                completed_rejected.clear();
            }
        }
    }
    (pending, pending_rejected, malformed)
}

fn context_message(context: &ContextMessage) -> &Message {
    match context {
        ContextMessage::Persisted { message, .. } | ContextMessage::Synthetic { message } => {
            message
        }
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    fn batch(public: u64, footprint: u64) -> L0Batch {
        L0Batch {
            id: Uuid::now_v7(),
            batch_seq: 1,
            messages: Vec::new(),
            est_tokens: public,
            eviction_footprint_tokens: footprint,
            state: BatchState::Open,
        }
    }

    fn calibration() -> TokenCalibration {
        TokenCalibration::default()
    }

    fn boundary(
        next_role: MessageRole,
        steering: bool,
        pending: &[&str],
        malformed: bool,
    ) -> BoundaryContext {
        BoundaryContext {
            previous_role: Some(MessageRole::Assistant),
            previous_assistant_interrupted: steering,
            next_role,
            next_user_is_steering: steering,
            pending_tool_call_ids: pending.iter().map(|id| (*id).to_owned()).collect(),
            pending_rejected_tool_call_ids: HashSet::new(),
            malformed_tool_loop: malformed,
        }
    }

    #[test]
    fn ordinary_seal_is_before_user_only() {
        let candidate = batch(L0_BATCH_MIN, 0);
        assert_eq!(
            seal_before_next(
                &candidate,
                &boundary(MessageRole::User, false, &[], false),
                calibration()
            ),
            Some(SealReason::Normal)
        );
        assert_eq!(
            seal_before_next(
                &candidate,
                &boundary(MessageRole::Assistant, false, &[], false),
                calibration()
            ),
            None
        );
    }

    #[test]
    fn forced_seal_is_the_only_assistant_fallback() {
        let candidate = batch(1, L0_FORCED_SEAL_LIMIT);
        assert_eq!(
            seal_before_next(
                &candidate,
                &boundary(MessageRole::Assistant, false, &[], false),
                calibration()
            ),
            Some(SealReason::ForcedFootprint)
        );
        assert_eq!(
            seal_before_next(
                &candidate,
                &boundary(MessageRole::ToolResult, false, &[], false),
                calibration()
            ),
            None
        );
    }

    #[test]
    fn interrupted_steering_and_pending_tool_loops_are_never_split() {
        let candidate = batch(L0_FORCED_SEAL_LIMIT + 1, 0);
        assert_eq!(
            seal_before_next(
                &candidate,
                &boundary(MessageRole::User, true, &[], false),
                calibration()
            ),
            None
        );
        assert_eq!(
            seal_before_next(
                &candidate,
                &boundary(MessageRole::User, false, &["call-1"], false),
                calibration()
            ),
            None
        );
    }

    #[test]
    fn calibrated_forced_threshold_checks_exact_below_above_and_overflow() {
        let ratio = TokenCalibration::new(2.0).expect("ratio");
        let assistant = boundary(MessageRole::Assistant, false, &[], false);
        assert_eq!(seal_before_next(&batch(5_000, 0), &assistant, ratio), None);
        assert_eq!(seal_before_next(&batch(4_999, 0), &assistant, ratio), None);
        assert_eq!(
            seal_before_next(&batch(5_001, 0), &assistant, ratio),
            Some(SealReason::ForcedFootprint)
        );
        assert_eq!(
            seal_before_next(&batch(u64::MAX, 1), &assistant, ratio),
            Some(SealReason::ForcedFootprint)
        );
    }

    #[test]
    fn closed_batches_never_seal() {
        for state in [
            BatchState::Sealed,
            BatchState::Compacting,
            BatchState::CompactFailed,
            BatchState::Compacted,
        ] {
            let mut candidate = batch(L0_BATCH_MIN, 0);
            candidate.state = state;
            assert_eq!(
                seal_before_next(
                    &candidate,
                    &boundary(MessageRole::User, false, &[], false),
                    calibration()
                ),
                None
            );
        }
    }

    fn assistant_call(id: &str) -> ContextMessage {
        serde_json::from_value(serde_json::json!({
            "source":"persisted","id":format!("assistant-{id}"),"seq":1,
            "message":{
                "role":"assistant","content":[{"type":"tool_call","tool_call":{"id":id,"name":"read_file","arguments":{"path":"x"}},"wire_item_index":0}],
                "model":"model","provider":"provider","origin":{"provider_instance_id":"provider","protocol":"open_ai_responses","model":"model"},
                "usage":{"input":1,"output":1,"cache_read":0,"cache_write":0,"reasoning":0,"total_tokens":2},"stop_reason":"tool_use","error_message":null,"provider_code":null,"interrupted":false,"timestamp":"2026-07-21T00:00:00Z"
            }
        }))
        .expect("assistant call")
    }

    fn assistant_with_rejected_duplicate(id: &str) -> ContextMessage {
        let mut context = assistant_call(id);
        let rejected: AssistantContent = serde_json::from_value(serde_json::json!({
            "type":"rejected_tool_call",
            "rejected":{"id":id,"name":"read_file","error":"schema_violation"},
            "wire_item_index":1
        }))
        .expect("rejected tool call");
        let ContextMessage::Persisted {
            message: Message::Assistant(message),
            ..
        } = &mut context
        else {
            unreachable!("assistant call fixture is persisted assistant")
        };
        message.content.push(rejected);
        context
    }

    fn rejected_call(id: &str) -> ContextMessage {
        serde_json::from_value(serde_json::json!({
            "source":"persisted","id":format!("rejected-{id}"),"seq":1,
            "message":{
                "role":"assistant","content":[{"type":"rejected_tool_call","rejected":{"id":id,"name":"read_file","error":"schema_violation"},"wire_item_index":0}],
                "model":"model","provider":"provider","origin":{"provider_instance_id":"provider","protocol":"open_ai_responses","model":"model"},
                "usage":{"input":1,"output":1,"cache_read":0,"cache_write":0,"reasoning":0,"total_tokens":2},"stop_reason":"tool_use","error_message":null,"provider_code":null,"interrupted":false,"timestamp":"2026-07-21T00:00:00Z"
            }
        }))
        .expect("rejected tool call")
    }

    fn tool_result(id: &str) -> ContextMessage {
        tool_result_with_error(id, false)
    }

    fn tool_result_with_error(id: &str, is_error: bool) -> ContextMessage {
        serde_json::from_value(serde_json::json!({
            "source":"synthetic","message":{"role":"tool_result","tool_call_id":id,"tool_name":"read_file","content":[{"type":"text","text":"done"}],"details":{},"is_error":is_error,"timestamp":"2026-07-21T00:00:01Z"}
        }))
        .expect("tool result")
    }

    fn user_message() -> ContextMessage {
        serde_json::from_value(serde_json::json!({
            "source":"synthetic","message":{"role":"user","content":[{"type":"text","text":"next"}],"timestamp":"2026-07-21T00:00:02Z"}
        }))
        .expect("user")
    }

    #[test]
    fn completed_single_and_multiple_loops_clear_matching_ids() {
        let next = user_message();
        for history in [
            vec![assistant_call("one"), tool_result("one")],
            vec![
                assistant_call("one"),
                tool_result("one"),
                assistant_call("two"),
                tool_result("two"),
            ],
            vec![
                assistant_call("one"),
                tool_result("one"),
                user_message(),
                assistant_call("one"),
                tool_result("one"),
            ],
            vec![
                assistant_call("one"),
                tool_result("one"),
                assistant_call("one"),
                tool_result("one"),
            ],
        ] {
            let boundary = BoundaryContext::from_history(&history, &next, false);
            assert!(boundary.pending_tool_call_ids().next().is_none());
            assert!(boundary.pending_rejected_tool_call_ids().next().is_none());
            assert!(!boundary.unresolved_tool_loop());
            assert_eq!(
                seal_before_next(&batch(L0_BATCH_MIN, 0), &boundary, calibration()),
                Some(SealReason::Normal)
            );
        }
    }

    #[test]
    fn partial_missing_orphan_and_duplicate_loops_fail_closed() {
        let next = user_message();
        let partial = vec![
            assistant_call("one"),
            assistant_call("two"),
            tool_result("one"),
        ];
        let boundary = BoundaryContext::from_history(&partial, &next, false);
        assert_eq!(
            boundary.pending_tool_call_ids().collect::<Vec<_>>(),
            vec!["two"]
        );
        assert!(boundary.unresolved_tool_loop());

        let orphan = vec![tool_result("orphan")];
        assert!(BoundaryContext::from_history(&orphan, &next, false).unresolved_tool_loop());

        let duplicate_result = vec![
            assistant_call("one"),
            tool_result("one"),
            tool_result("one"),
        ];
        assert!(
            BoundaryContext::from_history(&duplicate_result, &next, false).unresolved_tool_loop()
        );

        let duplicate_call = vec![
            assistant_call("one"),
            assistant_call("one"),
            tool_result("one"),
        ];
        assert!(
            BoundaryContext::from_history(&duplicate_call, &next, false).unresolved_tool_loop()
        );

        let overlapping_distinct_calls = vec![
            assistant_call("one"),
            assistant_call("two"),
            tool_result("one"),
            tool_result("two"),
        ];
        assert!(
            BoundaryContext::from_history(&overlapping_distinct_calls, &next, false)
                .unresolved_tool_loop()
        );

        let rejected_non_error = vec![rejected_call("rejected"), tool_result("rejected")];
        assert!(
            BoundaryContext::from_history(&rejected_non_error, &next, false).unresolved_tool_loop()
        );

        let rejected_duplicate = vec![
            rejected_call("rejected"),
            tool_result_with_error("rejected", true),
            tool_result_with_error("rejected", true),
        ];
        assert!(
            BoundaryContext::from_history(&rejected_duplicate, &next, false).unresolved_tool_loop()
        );

        let rejected_late_result = vec![
            rejected_call("rejected"),
            user_message(),
            tool_result_with_error("rejected", true),
        ];
        assert!(
            BoundaryContext::from_history(&rejected_late_result, &next, false)
                .unresolved_tool_loop()
        );

        let cross_kind_duplicate = vec![
            assistant_with_rejected_duplicate("same"),
            tool_result_with_error("same", true),
        ];
        assert!(
            BoundaryContext::from_history(&cross_kind_duplicate, &next, false)
                .unresolved_tool_loop()
        );

        let late_result = vec![assistant_call("one"), user_message(), tool_result("one")];
        assert!(BoundaryContext::from_history(&late_result, &next, false).unresolved_tool_loop());
    }

    #[test]
    fn completed_rejected_pair_is_a_safe_user_boundary() {
        let history = vec![
            rejected_call("rejected"),
            tool_result_with_error("rejected", true),
        ];
        let next = user_message();
        let boundary = BoundaryContext::from_history(&history, &next, false);

        assert!(boundary.pending_tool_call_ids().next().is_none());
        assert!(boundary.pending_rejected_tool_call_ids().next().is_none());
        assert!(!boundary.unresolved_tool_loop());
        assert_eq!(
            seal_before_next(&batch(L0_BATCH_MIN, 0), &boundary, calibration()),
            Some(SealReason::Normal)
        );
    }

    #[test]
    fn tool_result_to_assistant_boundary_never_forces_a_cut() {
        let history = vec![assistant_call("one"), tool_result("one")];
        let next = assistant_call("continuation");
        let boundary = BoundaryContext::from_history(&history, &next, false);

        assert_eq!(
            seal_before_next(
                &batch(L0_FORCED_SEAL_LIMIT + 1, 0),
                &boundary,
                calibration()
            ),
            None
        );
    }
}
