//! Deterministic L0 batch boundary decisions.

use std::collections::HashSet;

use crate::provider::types::{AssistantContent, ContextMessage, Message};

use super::estimate::TokenCalibration;
use super::{BatchState, L0_BATCH_MIN, L0_FORCED_SEAL_LIMIT, L0_LIMIT, L0Batch};

// Every non-empty ID costs at least one public-estimator token. The byte cap
// also covers the estimator's worst case: 1.5 non-ASCII chars/token at four
// UTF-8 bytes each. Both limits therefore derive from the existing L0 ceiling.
const MAX_PENDING_TOOL_IDS: usize = L0_LIMIT as usize;
const MAX_PENDING_TOOL_ID_BYTES: usize = MAX_PENDING_TOOL_IDS * 6;

#[derive(Clone, Copy)]
struct PendingIdLimits {
    count: usize,
    bytes: usize,
}

const PENDING_ID_LIMITS: PendingIdLimits = PendingIdLimits {
    count: MAX_PENDING_TOOL_IDS,
    bytes: MAX_PENDING_TOOL_ID_BYTES,
};

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
    pending_tool_id_bytes: usize,
    pending_tool_tracking_overflowed: bool,
    unsafe_suffix: bool,
}

impl BoundaryContext {
    pub fn from_history(
        history: &[ContextMessage],
        next: &ContextMessage,
        next_user_is_steering: bool,
    ) -> Self {
        let previous = history.last().map(context_message);
        let (
            pending_tool_call_ids,
            pending_rejected_tool_call_ids,
            pending_tool_id_bytes,
            pending_tool_tracking_overflowed,
            unsafe_suffix,
        ) = pending_tool_calls(history);
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
            pending_tool_id_bytes,
            pending_tool_tracking_overflowed,
            unsafe_suffix,
        }
    }

    pub fn next_role(&self) -> MessageRole {
        self.next_role
    }

    pub fn unresolved_tool_loop(&self) -> bool {
        self.unsafe_suffix
            || self.pending_tool_tracking_overflowed
            || !self.pending_tool_call_ids.is_empty()
            || !self.pending_rejected_tool_call_ids.is_empty()
    }

    pub fn pending_tool_call_count(&self) -> usize {
        self.pending_tool_call_ids.len()
    }

    pub fn pending_rejected_tool_call_count(&self) -> usize {
        self.pending_rejected_tool_call_ids.len()
    }

    pub fn pending_tool_id_bytes(&self) -> usize {
        self.pending_tool_id_bytes
    }

    pub fn pending_tool_tracking_overflowed(&self) -> bool {
        self.pending_tool_tracking_overflowed
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

/// Track the unresolved tool suffix needed for cut safety. Call and rejection
/// IDs coalesce into one structural expectation per ID; transcript validity is
/// owned by the canonical transform/assembler, not this boundary projection.
fn pending_tool_calls(
    history: &[ContextMessage],
) -> (HashSet<String>, HashSet<String>, usize, bool, bool) {
    pending_tool_calls_with_limits(history, PENDING_ID_LIMITS)
}

fn pending_tool_calls_with_limits(
    history: &[ContextMessage],
    limits: PendingIdLimits,
) -> (HashSet<String>, HashSet<String>, usize, bool, bool) {
    let mut pending = HashSet::new();
    let mut pending_rejected = HashSet::new();
    let mut pending_id_bytes = 0_usize;
    let mut tracking_overflowed = false;
    let mut unsafe_suffix = false;
    for context in history {
        match context_message(context) {
            Message::Assistant(message) => {
                // An assistant with no open IDs starts a new structural flow;
                // a user or this new flow resets anomalies from an older one.
                if pending.is_empty() && pending_rejected.is_empty() {
                    pending_id_bytes = 0;
                    tracking_overflowed = false;
                    unsafe_suffix = false;
                }
                for content in &message.content {
                    match content {
                        AssistantContent::ToolCall { tool_call, .. } => retain_pending_id(
                            &tool_call.id,
                            false,
                            &mut pending,
                            &mut pending_rejected,
                            &mut pending_id_bytes,
                            &mut tracking_overflowed,
                            limits,
                        ),
                        AssistantContent::RejectedToolCall { rejected, .. } => retain_pending_id(
                            &rejected.id,
                            true,
                            &mut pending,
                            &mut pending_rejected,
                            &mut pending_id_bytes,
                            &mut tracking_overflowed,
                            limits,
                        ),
                        _ => {}
                    }
                }
            }
            Message::ToolResult(message) => {
                if pending.remove(&message.tool_call_id)
                    || pending_rejected.remove(&message.tool_call_id)
                {
                    // Rejected/non-error mismatches are still structurally
                    // closed here; this function only decides cut safety.
                    if let Some(remaining) =
                        pending_id_bytes.checked_sub(message.tool_call_id.len())
                    {
                        pending_id_bytes = remaining;
                    } else {
                        // Unreachable for correctly-maintained state; fail
                        // closed if accounting ever becomes inconsistent.
                        pending_id_bytes = 0;
                        tracking_overflowed = true;
                        unsafe_suffix = true;
                    }
                } else {
                    // An orphan or late result blocks only this immediate
                    // suffix. A later user/new flow clears unsafe_suffix.
                    unsafe_suffix = true;
                }
            }
            Message::User(_) => {
                // A user turn closes the current suffix, including an
                // interrupted call sequence, so future boundaries can seal.
                pending.clear();
                pending_rejected.clear();
                pending_id_bytes = 0;
                tracking_overflowed = false;
                unsafe_suffix = false;
            }
        }
    }
    (
        pending,
        pending_rejected,
        pending_id_bytes,
        tracking_overflowed,
        unsafe_suffix,
    )
}

#[allow(clippy::too_many_arguments)]
fn retain_pending_id(
    id: &str,
    rejected: bool,
    pending: &mut HashSet<String>,
    pending_rejected: &mut HashSet<String>,
    pending_id_bytes: &mut usize,
    tracking_overflowed: &mut bool,
    limits: PendingIdLimits,
) {
    if *tracking_overflowed || pending.contains(id) || pending_rejected.contains(id) {
        return;
    }

    let within_limit = pending
        .len()
        .checked_add(pending_rejected.len())
        .and_then(|count| count.checked_add(1))
        .filter(|count| *count <= limits.count)
        .and_then(|_| pending_id_bytes.checked_add(id.len()))
        .filter(|bytes| *bytes <= limits.bytes);
    let Some(next_bytes) = within_limit else {
        *tracking_overflowed = true;
        return;
    };

    let inserted = if rejected {
        pending_rejected.insert(id.to_owned())
    } else {
        pending.insert(id.to_owned())
    };
    debug_assert!(inserted, "duplicate IDs returned before allocation");
    *pending_id_bytes = next_bytes;
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
        unsafe_suffix: bool,
    ) -> BoundaryContext {
        BoundaryContext {
            previous_role: Some(MessageRole::Assistant),
            previous_assistant_interrupted: steering,
            next_role,
            next_user_is_steering: steering,
            pending_tool_call_ids: pending.iter().map(|id| (*id).to_owned()).collect(),
            pending_rejected_tool_call_ids: HashSet::new(),
            pending_tool_id_bytes: pending.iter().map(|id| id.len()).sum(),
            pending_tool_tracking_overflowed: false,
            unsafe_suffix,
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

    fn assistant_calls(ids: &[&str]) -> ContextMessage {
        let mut context = assistant_call(ids.first().copied().expect("at least one call"));
        let ContextMessage::Persisted {
            message: Message::Assistant(message),
            ..
        } = &mut context
        else {
            unreachable!("assistant call fixture is persisted assistant")
        };
        for (wire_item_index, id) in ids.iter().enumerate().skip(1) {
            message.content.push(
                serde_json::from_value(serde_json::json!({
                    "type":"tool_call",
                    "tool_call":{"id":id,"name":"read_file","arguments":{"path":"x"}},
                    "wire_item_index":wire_item_index
                }))
                .expect("additional assistant call"),
            );
        }
        context
    }

    fn interrupted_assistant_call(id: &str) -> ContextMessage {
        let mut context = assistant_call(id);
        let ContextMessage::Persisted {
            message: Message::Assistant(message),
            ..
        } = &mut context
        else {
            unreachable!("assistant call fixture is persisted assistant")
        };
        message.interrupted = true;
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
    fn pending_id_limits_derive_from_the_l0_estimator_ceiling() {
        assert_eq!(MAX_PENDING_TOOL_IDS, 40_000);
        assert_eq!(MAX_PENDING_TOOL_ID_BYTES, 240_000);
        assert_eq!(MAX_PENDING_TOOL_ID_BYTES, MAX_PENDING_TOOL_IDS * 6);
    }

    #[test]
    fn pending_id_count_limit_accepts_boundary_then_stops_retaining() {
        let limits = PendingIdLimits {
            count: 2,
            bytes: 100,
        };
        let exact = pending_tool_calls_with_limits(&[assistant_calls(&["a", "bb"])], limits);
        assert_eq!(exact.0.len(), 2);
        assert_eq!(exact.2, 3);
        assert!(!exact.3);

        let exceeded =
            pending_tool_calls_with_limits(&[assistant_calls(&["a", "bb", "ccc", "dddd"])], limits);
        assert_eq!(exceeded.0.len(), 2);
        assert_eq!(exceeded.2, 3);
        assert!(exceeded.3);
    }

    #[test]
    fn pending_id_byte_limit_accepts_exact_boundary_and_rejects_overflow() {
        let limits = PendingIdLimits {
            count: 10,
            bytes: 5,
        };
        let exact = pending_tool_calls_with_limits(&[assistant_calls(&["aa", "bbb"])], limits);
        assert_eq!(exact.0.len(), 2);
        assert_eq!(exact.2, 5);
        assert!(!exact.3);

        let exceeded =
            pending_tool_calls_with_limits(&[assistant_calls(&["aa", "bbb", "c"])], limits);
        assert_eq!(exceeded.0.len(), 2);
        assert_eq!(exceeded.2, 5);
        assert!(exceeded.3);
    }

    #[test]
    fn pending_id_overflow_blocks_immediate_cut_and_later_flows_recover() {
        let oversized = "x".repeat(MAX_PENDING_TOOL_ID_BYTES + 1);
        let overflow_history = vec![assistant_calls(&[oversized.as_str(), "not-retained"])];
        let next = user_message();
        let overflow = BoundaryContext::from_history(&overflow_history, &next, false);
        assert_eq!(overflow.pending_tool_call_count(), 0);
        assert_eq!(overflow.pending_tool_id_bytes(), 0);
        assert!(overflow.pending_tool_tracking_overflowed());
        assert_eq!(
            seal_before_next(
                &batch(L0_FORCED_SEAL_LIMIT + 1, 0),
                &overflow,
                calibration()
            ),
            None
        );

        let mut after_user = overflow_history.clone();
        after_user.push(user_message());
        let recovered = BoundaryContext::from_history(&after_user, &next, false);
        assert!(!recovered.unresolved_tool_loop());
        assert_eq!(
            seal_before_next(&batch(L0_BATCH_MIN, 0), &recovered, calibration()),
            Some(SealReason::Normal)
        );
        assert_eq!(
            seal_before_next(
                &batch(L0_FORCED_SEAL_LIMIT + 1, 0),
                &recovered,
                calibration()
            ),
            Some(SealReason::ForcedFootprint)
        );

        let mut after_new_flow = overflow_history;
        after_new_flow.push(assistant_call("new"));
        after_new_flow.push(tool_result("new"));
        let recovered = BoundaryContext::from_history(&after_new_flow, &next, false);
        assert!(!recovered.unresolved_tool_loop());
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
            assert_eq!(boundary.pending_tool_call_count(), 0);
            assert_eq!(boundary.pending_rejected_tool_call_count(), 0);
            assert!(!boundary.unresolved_tool_loop());
            assert_eq!(
                seal_before_next(&batch(L0_BATCH_MIN, 0), &boundary, calibration()),
                Some(SealReason::Normal)
            );
        }
    }

    #[test]
    fn immediate_unresolved_suffixes_remain_uncuttable() {
        let next = user_message();
        let partial = vec![
            assistant_call("one"),
            assistant_call("two"),
            tool_result("one"),
        ];
        let boundary = BoundaryContext::from_history(&partial, &next, false);
        assert_eq!(boundary.pending_tool_call_count(), 1);
        assert_eq!(boundary.pending_tool_id_bytes(), "two".len());
        assert!(boundary.unresolved_tool_loop());
        assert_eq!(
            seal_before_next(
                &batch(L0_FORCED_SEAL_LIMIT + 1, 0),
                &boundary,
                calibration()
            ),
            None
        );

        let orphan = vec![tool_result("orphan")];
        let orphan_boundary = BoundaryContext::from_history(&orphan, &next, false);
        assert!(orphan_boundary.unresolved_tool_loop());
        assert_eq!(
            seal_before_next(
                &batch(L0_FORCED_SEAL_LIMIT + 1, 0),
                &orphan_boundary,
                calibration()
            ),
            None
        );

        let duplicate_result = vec![
            assistant_call("one"),
            tool_result("one"),
            tool_result("one"),
        ];
        let duplicate_result_boundary =
            BoundaryContext::from_history(&duplicate_result, &next, false);
        assert!(duplicate_result_boundary.unresolved_tool_loop());
        assert_eq!(
            seal_before_next(
                &batch(L0_FORCED_SEAL_LIMIT + 1, 0),
                &duplicate_result_boundary,
                calibration()
            ),
            None
        );

        let rejected_duplicate_result = vec![
            rejected_call("rejected"),
            tool_result_with_error("rejected", true),
            tool_result_with_error("rejected", true),
        ];
        let rejected_duplicate_boundary =
            BoundaryContext::from_history(&rejected_duplicate_result, &next, false);
        assert!(rejected_duplicate_boundary.unresolved_tool_loop());
        assert_eq!(
            seal_before_next(
                &batch(L0_FORCED_SEAL_LIMIT + 1, 0),
                &rejected_duplicate_boundary,
                calibration()
            ),
            None
        );

        let late_result = vec![assistant_call("one"), user_message(), tool_result("one")];
        let late_boundary = BoundaryContext::from_history(&late_result, &next, false);
        assert!(late_boundary.unresolved_tool_loop());
        assert_eq!(
            seal_before_next(
                &batch(L0_FORCED_SEAL_LIMIT + 1, 0),
                &late_boundary,
                calibration()
            ),
            None
        );

        let rejected_next_result = rejected_call("rejected");
        let rejected_next_result_boundary =
            BoundaryContext::from_history(&[rejected_next_result], &tool_result("rejected"), false);
        assert_eq!(
            seal_before_next(
                &batch(L0_FORCED_SEAL_LIMIT + 1, 0),
                &rejected_next_result_boundary,
                calibration()
            ),
            None
        );

        let completed_result = vec![assistant_call("one"), tool_result("one")];
        let continuation_boundary =
            BoundaryContext::from_history(&completed_result, &assistant_call("next"), false);
        assert_eq!(
            seal_before_next(
                &batch(L0_FORCED_SEAL_LIMIT + 1, 0),
                &continuation_boundary,
                calibration()
            ),
            None
        );
    }

    #[test]
    fn historical_anomalies_recover_after_user_or_new_flow() {
        let next = user_message();
        let histories = [
            vec![assistant_call("one"), user_message()],
            vec![interrupted_assistant_call("interrupted"), user_message()],
            vec![tool_result("orphan"), user_message()],
            vec![
                assistant_call("one"),
                assistant_call("one"),
                tool_result("one"),
                user_message(),
            ],
            vec![
                assistant_call("one"),
                tool_result("one"),
                tool_result("one"),
                user_message(),
            ],
            vec![
                assistant_call("one"),
                user_message(),
                tool_result("one"),
                user_message(),
            ],
            vec![
                assistant_with_rejected_duplicate("same"),
                tool_result_with_error("same", false),
                user_message(),
            ],
            vec![
                rejected_call("rejected"),
                tool_result("rejected"),
                user_message(),
            ],
            vec![
                tool_result("orphan"),
                assistant_call("new-flow"),
                tool_result("new-flow"),
            ],
        ];

        for history in histories {
            let boundary = BoundaryContext::from_history(&history, &next, false);
            assert_eq!(boundary.pending_tool_call_count(), 0);
            assert_eq!(boundary.pending_rejected_tool_call_count(), 0);
            assert!(!boundary.unresolved_tool_loop());
            assert_eq!(
                seal_before_next(&batch(L0_BATCH_MIN, 0), &boundary, calibration()),
                Some(SealReason::Normal)
            );
            assert_eq!(
                seal_before_next(
                    &batch(L0_FORCED_SEAL_LIMIT + 1, 0),
                    &boundary,
                    calibration()
                ),
                Some(SealReason::ForcedFootprint)
            );
        }
    }

    #[test]
    fn completed_rejected_pair_is_a_safe_user_boundary() {
        let history = vec![
            rejected_call("rejected"),
            tool_result_with_error("rejected", true),
        ];
        let next = user_message();
        let boundary = BoundaryContext::from_history(&history, &next, false);

        assert_eq!(boundary.pending_tool_call_count(), 0);
        assert_eq!(boundary.pending_rejected_tool_call_count(), 0);
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
