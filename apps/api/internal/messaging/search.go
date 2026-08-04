package messaging

import (
	"context"
	"fmt"
	"strings"
	"unicode"
)

// MaxSearchLimit bounds one search response.
const MaxSearchLimit = 50

const defaultSearchLimit = 20

// MaxSearchQueryBytes bounds the query string: a search phrase, not a document.
const MaxSearchQueryBytes = 200

// searchSnippetRadius is how many runes of context each side of the first
// match survives into the snippet. The full content (up to 64KB) never crosses
// the wire for search results.
const searchSnippetRadius = 40

// SearchOptions scopes a message search.
type SearchOptions struct {
	// PlaceID, when set, restricts the search to that one place. The viewer
	// must be able to see it; an invisible place is ErrPlaceNotFound, matching
	// every other read path (existence is never revealed).
	PlaceID string
	Limit   int
}

// SearchResult is one hit: the matched message, the place it lives in (so
// callers can build a permalink from place + seq without a second lookup), and
// a content fragment around the first match.
type SearchResult struct {
	Message Message
	Place   Place
	Snippet string
}

// SearchMessages finds live messages whose content contains the query,
// case-insensitively, across every place the viewer can see (their workspaces'
// channels plus their dm/group_dm places) — the same visibility basis as
// UnreadSummaries. Tombstones are excluded. Results are ranked by pg_trgm
// similarity between content and query, then by recency, so short exact
// matches surface above long documents that merely contain the words.
//
// Japanese needs substring matching (no lexeme boundaries for FTS), which
// ILIKE provides exactly; the trigram GIN index from migration 0012
// accelerates it for queries of 3+ characters. This lives in the store so
// REST, WS, and the agent tool path (local control) share one implementation.
func (s *Store) SearchMessages(ctx context.Context, viewer ParticipantRef, query string, opt SearchOptions) ([]SearchResult, error) {
	if err := viewer.Validate(); err != nil {
		return nil, err
	}
	q := strings.TrimSpace(query)
	if q == "" {
		return nil, fmt.Errorf("query must not be empty")
	}
	if len(q) > MaxSearchQueryBytes {
		return nil, fmt.Errorf("query exceeds %d bytes", MaxSearchQueryBytes)
	}
	limit := opt.Limit
	if limit <= 0 {
		limit = defaultSearchLimit
	}
	if limit > MaxSearchLimit {
		limit = MaxSearchLimit
	}
	if opt.PlaceID != "" {
		if _, err := s.PlaceFor(ctx, opt.PlaceID, viewer); err != nil {
			return nil, err
		}
	}

	args := []any{viewer.Kind, viewer.ID, "%" + escapeLikePattern(q) + "%", q, limit}
	placeFilter := ""
	if opt.PlaceID != "" {
		placeFilter = "AND m.place_id = $6"
		args = append(args, opt.PlaceID)
	}
	rows, err := s.pool.Query(ctx, fmt.Sprintf(
		`WITH my_places AS (
		   SELECT p.* FROM places p
		   JOIN workspace_members wm ON wm.workspace_id = p.workspace_id
		    AND wm.member_kind = $1 AND wm.member_id = $2 AND wm.left_at IS NULL
		   WHERE p.kind = 'channel'
		   UNION
		   SELECT p.* FROM places p
		   JOIN place_members pm ON pm.place_id = p.place_id
		    AND pm.member_kind = $1 AND pm.member_id = $2 AND pm.left_at IS NULL
		 )
		 SELECT m.message_id, m.place_id, m.seq, m.author_kind, m.author_id,
		        m.content, m.urgency, m.reply_to, m.client_nonce,
		        m.created_at, m.edited_at,
		        mp.kind, mp.workspace_id, mp.name, mp.topic, mp.visibility, mp.last_seq
		 FROM messages m
		 JOIN my_places mp ON mp.place_id = m.place_id
		 WHERE m.deleted_at IS NULL
		   AND m.content ILIKE $3 %s
		 ORDER BY similarity(m.content, $4) DESC, m.created_at DESC, m.seq DESC
		 LIMIT $5`, placeFilter), args...)
	if err != nil {
		return nil, fmt.Errorf("query message search: %w", err)
	}
	defer rows.Close()
	var out []SearchResult
	for rows.Next() {
		var (
			res         SearchResult
			authorKind  string
			replyTo     *string
			workspaceID *string
			name        *string
		)
		if err := rows.Scan(&res.Message.MessageID, &res.Message.PlaceID, &res.Message.Seq,
			&authorKind, &res.Message.Author.ID, &res.Message.Content, &res.Message.Urgency,
			&replyTo, &res.Message.ClientNonce, &res.Message.CreatedAt, &res.Message.EditedAt,
			&res.Place.Kind, &workspaceID, &name,
			&res.Place.Topic, &res.Place.Visibility, &res.Place.LastSeq); err != nil {
			return nil, fmt.Errorf("scan search result: %w", err)
		}
		res.Message.Author.Kind = ParticipantKind(authorKind)
		if replyTo != nil {
			res.Message.ReplyTo = *replyTo
		}
		res.Place.PlaceID = res.Message.PlaceID
		if workspaceID != nil {
			res.Place.WorkspaceID = *workspaceID
		}
		if name != nil {
			res.Place.Name = *name
		}
		res.Snippet = searchSnippet(res.Message.Content, q)
		out = append(out, res)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate search results: %w", err)
	}
	return out, nil
}

// escapeLikePattern neutralizes LIKE metacharacters so the user query is
// matched literally (backslash is the default LIKE escape character).
func escapeLikePattern(s string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(s)
}

// searchSnippet cuts a rune window around the first case-insensitive match.
// Rune-wise lowercasing keeps indexes aligned between the folded and original
// text (unlike full case folding), which is exact for Japanese and close
// enough to ILIKE's semantics for presentation.
func searchSnippet(content, query string) string {
	runes := []rune(content)
	folded := strings.Map(unicode.ToLower, content)
	byteIdx := strings.Index(folded, strings.Map(unicode.ToLower, query))
	if byteIdx < 0 {
		if len(runes) <= 2*searchSnippetRadius {
			return content
		}
		return string(runes[:2*searchSnippetRadius]) + "…"
	}
	matchStart := len([]rune(folded[:byteIdx]))
	matchEnd := matchStart + len([]rune(query))
	start, prefix := matchStart-searchSnippetRadius, "…"
	if start <= 0 {
		start, prefix = 0, ""
	}
	end, suffix := matchEnd+searchSnippetRadius, "…"
	if end >= len(runes) {
		end, suffix = len(runes), ""
	}
	return prefix + string(runes[start:end]) + suffix
}
