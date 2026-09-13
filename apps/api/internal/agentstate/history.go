// conversation_history: the secretary's own stored-record reader, carried
// over from the old runtime's tool of the same name
// (docs/agent/memory-preparation-and-replacement-2026-09-08). This opens
// recorded history — it is not the model "remembering" internally, and its
// results are stored records, not new instructions.
//
// Search is a literal case-sensitive substring scan over each event's search
// projection (its text field, or the serialized payload for tool records):
// omitted fields are not searchable, so no match does not prove absence.
// Read returns the original stored journal events — after a chunk is
// applied, the originals remain readable through chunk_seq — paged by a
// 16,384-Unicode-character budget over the stable journal_event_v1
// serialization, with content_offset fragments for oversized records.
package agentstate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
)

// maxReadJSONChars bounds one read page: the count of serialized
// journal_event_v1 characters, matching the old runtime's page budget.
const maxReadJSONChars = 16 * 1024

const journalEventFormat = "journal_event_v1"

type historyArgs struct {
	operation     string
	query         *string
	seq           *int64
	chunkSeq      *int64
	fromSeq       *int64
	afterSeq      *int64
	contentOffset *int64
	limit         int
}

// searchProjection is the text a search scans: the record's own text when it
// has one, else the serialized payload (tool calls/results and other
// structured records stay findable by their contents).
func searchProjection(e Event) string {
	if t, ok := e.Payload["text"].(string); ok {
		return t
	}
	raw, err := json.Marshal(e.Payload)
	if err != nil {
		return ""
	}
	return string(raw)
}

func parseHistoryArgs(request map[string]any) (historyArgs, error) {
	var a historyArgs
	a.limit = 5
	getStr := func(k string) (*string, bool) {
		v, present := request[k]
		if !present {
			return nil, true
		}
		s, ok := v.(string)
		if !ok {
			return nil, false
		}
		return &s, true
	}
	getInt := func(k string) (*int64, bool) {
		v, present := request[k]
		if !present {
			return nil, true
		}
		f, ok := v.(float64)
		if !ok || f != float64(int64(f)) {
			return nil, false
		}
		i := int64(f)
		return &i, true
	}
	op, _ := request["operation"].(string)
	a.operation = op
	var ok bool
	if a.query, ok = getStr("query"); !ok {
		return a, fmt.Errorf("%w: query must be a string", ErrBadRequest)
	}
	if a.seq, ok = getInt("seq"); !ok {
		return a, fmt.Errorf("%w: seq must be an integer", ErrBadRequest)
	}
	if a.chunkSeq, ok = getInt("chunk_seq"); !ok {
		return a, fmt.Errorf("%w: chunk_seq must be an integer", ErrBadRequest)
	}
	if a.fromSeq, ok = getInt("from_seq"); !ok {
		return a, fmt.Errorf("%w: from_seq must be an integer", ErrBadRequest)
	}
	if a.afterSeq, ok = getInt("after_seq"); !ok {
		return a, fmt.Errorf("%w: after_seq must be an integer", ErrBadRequest)
	}
	if a.contentOffset, ok = getInt("content_offset"); !ok {
		return a, fmt.Errorf("%w: content_offset must be an integer", ErrBadRequest)
	}
	if lim, present := request["limit"]; present {
		f, isNum := lim.(float64)
		if !isNum || f != float64(int64(f)) || f < 1 || f > 20 {
			return a, fmt.Errorf("%w: limit must be an integer in [1, 20]", ErrBadRequest)
		}
		a.limit = int(f)
	}
	if op != "search" && op != "read" {
		return a, fmt.Errorf("%w: operation must be search or read", ErrBadRequest)
	}
	if (op == "search") != (a.query != nil) {
		return a, fmt.Errorf("%w: search requires query and read must not carry one", ErrBadRequest)
	}
	if a.contentOffset != nil && (op != "read" || a.seq == nil) {
		return a, fmt.Errorf("%w: content_offset is only valid with read + seq", ErrBadRequest)
	}
	// Locators are alternatives: one exact record (seq), one chunk's range
	// (chunk_seq), or a range start (from_seq) continued by after_seq.
	locators := 0
	for _, set := range []bool{a.seq != nil, a.chunkSeq != nil, a.fromSeq != nil} {
		if set {
			locators++
		}
	}
	if locators > 1 {
		return a, fmt.Errorf("%w: seq, chunk_seq and from_seq are alternative locators", ErrBadRequest)
	}
	// chunk_seq + after_seq continues a page within that chunk's range.
	if a.seq != nil && a.afterSeq != nil {
		return a, fmt.Errorf("%w: seq cannot combine with after_seq", ErrBadRequest)
	}
	return a, nil
}

// conversationHistory executes the read-only history tool inside the claim
// transaction so the receipt it records reflects the same committed view.
func (s *Store) conversationHistory(ctx context.Context, tx pgx.Tx, personaID string, request map[string]any) (map[string]any, error) {
	args, err := parseHistoryArgs(request)
	if err != nil {
		return nil, err
	}
	if args.operation == "search" {
		return s.historySearch(ctx, tx, personaID, args)
	}
	return s.historyRead(ctx, tx, personaID, args)
}

func historyDetails(args historyArgs, messages []map[string]any, nextAfterSeq, nextRead any) map[string]any {
	hasMore := nextAfterSeq != nil || nextRead != nil
	return map[string]any{
		"operation":                args.operation,
		"scope":                    "your_conversation_history",
		"messages":                 messages,
		"next_after_seq":           nextAfterSeq,
		"next_read":                nextRead,
		"has_more":                 hasMore,
		"journal_event_format":     journalEventFormat,
		"content_complete_meaning": "entire stored event representation returned in this call; does not imply provider vision support",
		"fragment_continuation":    "follow next_read to the end of this event before resuming the original query with after_seq=resume_after_seq; concatenated fragments form the journal_event_v1 JSON",
		"search_coverage":          "stored text fields and serialized tool records only; no match is not proof that a record is absent",
	}
}

func (s *Store) historySearch(ctx context.Context, tx pgx.Tx, personaID string, args historyArgs) (map[string]any, error) {
	var after int64
	if args.afterSeq != nil {
		after = *args.afterSeq
	}
	// Fetch one extra row to know whether the result set continues.
	rows, err := tx.Query(ctx, `
		SELECT persona_id, seq, turn_id, kind, payload, created_at
		FROM core_events
		WHERE persona_id = $1 AND seq > $2
			AND STRPOS(COALESCE(payload->>'text', payload::text), $3) > 0
		ORDER BY seq LIMIT $4`, personaID, after, *args.query, args.limit+1)
	if err != nil {
		return nil, err
	}
	var events []Event
	for rows.Next() {
		var e Event
		if err := rows.Scan(&e.PersonaID, &e.Seq, &e.TurnID, &e.Kind, &e.Payload, &e.CreatedAt); err != nil {
			rows.Close()
			return nil, err
		}
		events = append(events, e)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var nextAfterSeq any
	if len(events) > args.limit {
		nextAfterSeq = events[args.limit-1].Seq
		events = events[:args.limit]
	}
	messages := make([]map[string]any, 0, len(events))
	for _, e := range events {
		text := searchProjection(e)
		matchStart := indexOf(text, *args.query)
		if matchStart < 0 {
			matchStart = 0
		}
		start := matchStart - 80
		if start < 0 {
			start = 0
		}
		snippetRunes := []rune(text)
		end := start + 500
		if end > len(snippetRunes) {
			end = len(snippetRunes)
		}
		truncated := start > 0 || end < len(snippetRunes)
		messages = append(messages, map[string]any{
			"source":             map[string]any{"seq": e.Seq, "turn_id": e.TurnID, "kind": e.Kind},
			"timestamp":          e.CreatedAt,
			"snippet":            string(snippetRunes[start:end]),
			"snippet_char_start": start,
			"snippet_truncated":  truncated,
		})
	}
	return historyDetails(args, messages, nextAfterSeq, nil), nil
}

// indexOf returns the character (not byte) index of the first occurrence of
// sub in s, or -1 — the same literal substring semantics as the old tool.
func indexOf(s, sub string) int {
	bi := strings.Index(s, sub)
	if bi < 0 {
		return -1
	}
	return utf8.RuneCountInString(s[:bi])
}

// journalEventJSON is the stable serialization a read returns and fragments.
func journalEventJSON(e Event) (string, error) {
	raw, err := json.Marshal(map[string]any{
		"seq":        e.Seq,
		"turn_id":    e.TurnID,
		"kind":       e.Kind,
		"created_at": e.CreatedAt,
		"payload":    e.Payload,
	})
	return string(raw), err
}

func (s *Store) historyRead(ctx context.Context, tx pgx.Tx, personaID string, args historyArgs) (map[string]any, error) {
	var events []Event
	// upper bounds a chunk_seq read to that chunk's range; nil is open-ended.
	var upper *int64
	switch {
	case args.seq != nil:
		e, err := scanEvent(tx.QueryRow(ctx, `
			SELECT persona_id, seq, turn_id, kind, payload, created_at
			FROM core_events WHERE persona_id = $1 AND seq = $2`,
			personaID, *args.seq))
		if err != nil {
			return nil, err
		}
		events = []Event{e}
	default:
		var from int64 = 1
		if args.chunkSeq != nil {
			c, err := s.chunk(ctx, tx, personaID, *args.chunkSeq)
			if errors.Is(err, ErrChunkNotFound) {
				// A wrong locator is the model's bad request, recorded as the
				// tool result — not a state-service outage for the turn.
				return nil, fmt.Errorf("%w: memory chunk %d not found", ErrBadRequest, *args.chunkSeq)
			}
			if err != nil {
				return nil, err
			}
			from, upper = c.FirstSeq, &c.LastSeq
		}
		if args.fromSeq != nil {
			from = *args.fromSeq
		}
		if args.afterSeq != nil && *args.afterSeq+1 > from {
			from = *args.afterSeq + 1
		}
		// One extra row detects whether the range continues past this page.
		rows, err := tx.Query(ctx, `
			SELECT persona_id, seq, turn_id, kind, payload, created_at
			FROM core_events WHERE persona_id = $1 AND seq >= $2
				AND ($3::bigint IS NULL OR seq <= $3)
			ORDER BY seq LIMIT $4`, personaID, from, upper, args.limit+1)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var e Event
			if err := rows.Scan(&e.PersonaID, &e.Seq, &e.TurnID, &e.Kind, &e.Payload, &e.CreatedAt); err != nil {
				rows.Close()
				return nil, err
			}
			events = append(events, e)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}

	messages := []map[string]any{}
	var nextAfterSeq any
	var nextRead any
	readChars := 0
	var lastCompleteSeq int64
	haveComplete := false
	for i, e := range events {
		serialized, err := journalEventJSON(e)
		if err != nil {
			return nil, fmt.Errorf("render journal event: %w", err)
		}
		runes := []rune(serialized)
		totalChars := len(runes)
		offset := 0
		if args.contentOffset != nil {
			offset = int(*args.contentOffset)
			if offset > totalChars {
				return nil, fmt.Errorf("%w: content_offset %d beyond record length %d", ErrBadRequest, offset, totalChars)
			}
		}
		// Stop the page before a record that does not fit — either because
		// the count limit is reached or because the char budget is spent.
		// A page still holding nothing fragments the oversized record
		// instead of stopping.
		if i >= args.limit ||
			(offset == 0 && len(messages) > 0 && readChars+totalChars > maxReadJSONChars) {
			if haveComplete {
				nextAfterSeq = lastCompleteSeq
			}
			break
		}
		if offset == 0 && readChars+totalChars <= maxReadJSONChars {
			readChars += totalChars
			lastCompleteSeq = e.Seq
			haveComplete = true
			messages = append(messages, map[string]any{
				"source":           map[string]any{"seq": e.Seq, "turn_id": e.TurnID, "kind": e.Kind},
				"event":            json.RawMessage(serialized),
				"content_complete": true,
			})
			continue
		}
		// Fragment: an oversized record, or a content_offset continuation.
		end := offset + maxReadJSONChars
		if end > totalChars {
			end = totalChars
		}
		fragment := string(runes[offset:end])
		if end < totalChars {
			nextRead = map[string]any{
				"operation":      "read",
				"seq":            e.Seq,
				"content_offset": end,
			}
		}
		// Whether the original query continues after this record.
		more := i+1 < len(events)
		if !more && args.seq == nil {
			var nxt int64
			err := tx.QueryRow(ctx, `
				SELECT seq FROM core_events
				WHERE persona_id = $1 AND seq > $2 AND ($3::bigint IS NULL OR seq <= $3)
				ORDER BY seq LIMIT 1`,
				personaID, e.Seq, upper).Scan(&nxt)
			more = err == nil
			if err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return nil, err
			}
		}
		if more {
			nextAfterSeq = e.Seq
		}
		messages = append(messages, map[string]any{
			"source":              map[string]any{"seq": e.Seq, "turn_id": e.TurnID, "kind": e.Kind},
			"timestamp":           e.CreatedAt,
			"event_json_fragment": fragment,
			"event_json_range": map[string]any{
				"start_char": offset, "end_char": end,
				"total_chars": totalChars, "ends_event": end == totalChars,
			},
			"content_complete": false,
			"resume_after_seq": e.Seq,
		})
		break
	}
	return historyDetails(args, messages, nextAfterSeq, nextRead), nil
}

func scanEvent(row interface{ Scan(...any) error }) (Event, error) {
	var e Event
	err := row.Scan(&e.PersonaID, &e.Seq, &e.TurnID, &e.Kind, &e.Payload, &e.CreatedAt)
	if err != nil {
		if err == pgx.ErrNoRows {
			return e, fmt.Errorf("%w: journal event not found", ErrBadRequest)
		}
		return e, err
	}
	return e, nil
}
