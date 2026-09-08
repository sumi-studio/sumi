package agentevents

import "encoding/json"

const (
	maxBrowserVolatileEvents   = 64
	maxBrowserVolatileBytes    = 4 << 20
	maxBrowserMergedDeltaBytes = 16 << 10
)

type browserVolatileBatch struct {
	events []Envelope
	bytes  int
}

type browserMessageDelta struct {
	Type      string `json:"type"`
	MessageID string `json:"message_id"`
	Event     struct {
		Type         string `json:"type"`
		ContentIndex uint64 `json:"content_index"`
		Delta        string `json:"delta"`
	} `json:"event"`
}

// append never merges across block boundaries, messages, or other event kinds.
// Input envelopes have passed validateEnvelope before reaching this queue.
func (b *browserVolatileBatch) append(envelope Envelope) bool {
	if n := len(b.events); n > 0 {
		previous := b.events[n-1]
		if merged, ok := mergeBrowserDelta(previous, envelope); ok {
			size := b.bytes - len(previous.Event) + len(merged.Event)
			if size > maxBrowserVolatileBytes {
				return false
			}
			b.events[n-1] = merged
			b.bytes = size
			return true
		}
	}
	if len(b.events) == maxBrowserVolatileEvents || b.bytes+len(envelope.Event) > maxBrowserVolatileBytes {
		return false
	}
	b.events = append(b.events, envelope)
	b.bytes += len(envelope.Event)
	return true
}

func mergeBrowserDelta(previous, next Envelope) (Envelope, bool) {
	if previous.Seq != nil || next.Seq != nil || previous.PersonalityAgentID != next.PersonalityAgentID ||
		len(previous.Event)+len(next.Event) > maxBrowserMergedDeltaBytes {
		return Envelope{}, false
	}
	var a, b browserMessageDelta
	if json.Unmarshal(previous.Event, &a) != nil || json.Unmarshal(next.Event, &b) != nil ||
		a.Type != "message_update" || b.Type != a.Type || a.MessageID != b.MessageID ||
		a.Event.Type != b.Event.Type || a.Event.ContentIndex != b.Event.ContentIndex {
		return Envelope{}, false
	}
	switch a.Event.Type {
	case "text_delta", "thinking_delta", "tool_call_delta", "reasoning_summary_delta":
	default:
		return Envelope{}, false
	}
	a.Event.Delta += b.Event.Delta
	raw, err := json.Marshal(a)
	if err != nil || len(raw) > maxBrowserMergedDeltaBytes {
		return Envelope{}, false
	}
	previous.Event = raw
	return previous, true
}
