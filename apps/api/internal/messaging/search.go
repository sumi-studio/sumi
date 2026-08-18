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

// MaxSearchQueryBytes bounds a search phrase before it reaches PostgreSQL.
const MaxSearchQueryBytes = 200

const searchSnippetRadius = 40

// SearchOptions narrows an authorized search to one visible place when set.
type SearchOptions struct {
	PlaceID string
	Limit   int
}

// SearchResult carries the identity needed to open a hit and the small text
// fragment that may cross the search boundary. Message.Content is used only
// while building that fragment; callers must project Snippet instead.
type SearchResult struct {
	Message Message
	Place   Place
	Snippet string
}

// SearchMessages finds live messages in the scope's Workspace. It uses the
// same active Workspace membership and private-place tenure projection as
// UnreadSummaries, including visible_from_seq for a re-admitted member.
func (s *ScopedStore) SearchMessages(
	ctx context.Context,
	query string,
	opt SearchOptions,
) ([]SearchResult, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return nil, fmt.Errorf("search query must not be empty")
	}
	if len(query) > MaxSearchQueryBytes {
		return nil, fmt.Errorf("search query exceeds %d bytes", MaxSearchQueryBytes)
	}
	limit := opt.Limit
	if limit <= 0 {
		limit = defaultSearchLimit
	}
	if limit > MaxSearchLimit {
		limit = MaxSearchLimit
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin scoped message search: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	membership, err := s.authorizeInTx(ctx, tx)
	if err != nil {
		return nil, err
	}
	if opt.PlaceID != "" {
		place, err := s.loadScopedPlace(ctx, tx, opt.PlaceID)
		if err != nil {
			return nil, err
		}
		if _, err := s.placeAccessAfterAuthorization(ctx, tx, place, s.Scope.Actor); err != nil {
			return nil, err
		}
	}

	args := []any{
		s.Scope.WorkspaceID, membership.WorkspaceMemberID,
		escapeLikePattern(query), query, limit,
	}
	placeFilter := ""
	if opt.PlaceID != "" {
		placeFilter = "AND m.place_id = $6"
		args = append(args, opt.PlaceID)
	}
	rows, err := tx.Query(ctx, fmt.Sprintf(`
		WITH visible_places AS (
			SELECT p.place_id, p.kind, p.workspace_id, p.name, p.topic,
			       p.visibility, p.last_seq,
			       COALESCE(pm.visible_from_seq, 1) AS visible_from_seq
			FROM places p
			LEFT JOIN place_members pm
			  ON pm.workspace_id = p.workspace_id AND pm.place_id = p.place_id
			 AND pm.workspace_member_id = $2 AND pm.left_at IS NULL
			WHERE p.workspace_id = $1
			  AND (p.kind IN ('channel', 'thread') OR
			       (p.kind IN ('dm', 'group_dm') AND pm.place_member_id IS NOT NULL))
		)
		SELECT m.message_id, m.place_id, m.seq, m.author_kind, m.author_id,
		       m.content, m.created_at,
		       vp.kind, vp.workspace_id, vp.name, vp.topic, vp.visibility, vp.last_seq
		FROM messages m
		JOIN visible_places vp ON vp.place_id = m.place_id
		WHERE m.workspace_id = $1 AND m.deleted_at IS NULL
		  AND m.seq >= vp.visible_from_seq
		  AND m.content ILIKE ('%%' || $3 || '%%') %s
		ORDER BY similarity(m.content, $4) DESC, m.created_at DESC, m.seq DESC,
		         m.message_id DESC
		LIMIT $5`, placeFilter), args...)
	if err != nil {
		return nil, fmt.Errorf("query scoped message search: %w", err)
	}
	defer rows.Close()

	results := []SearchResult{}
	for rows.Next() {
		var (
			result     SearchResult
			authorKind string
			name       *string
		)
		if err := rows.Scan(
			&result.Message.MessageID, &result.Message.PlaceID, &result.Message.Seq,
			&authorKind, &result.Message.Author.ID, &result.Message.Content,
			&result.Message.CreatedAt, &result.Place.Kind, &result.Place.WorkspaceID,
			&name, &result.Place.Topic, &result.Place.Visibility, &result.Place.LastSeq,
		); err != nil {
			return nil, fmt.Errorf("scan scoped search result: %w", err)
		}
		result.Message.Author.Kind = ParticipantKind(authorKind)
		result.Place.PlaceID = result.Message.PlaceID
		if name != nil {
			result.Place.Name = *name
		}
		result.Snippet = searchSnippet(result.Message.Content, query)
		results = append(results, result)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate scoped message search: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit scoped message search: %w", err)
	}
	return results, nil
}

func escapeLikePattern(value string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(value)
}

// searchSnippet returns a rune-bounded window around the first match so a
// legal 64 KB message cannot become a 64 KB search response.
func searchSnippet(content, query string) string {
	runes := []rune(content)
	folded := strings.Map(unicode.ToLower, content)
	matchAt := strings.Index(folded, strings.Map(unicode.ToLower, query))
	if matchAt < 0 {
		if len(runes) <= 2*searchSnippetRadius {
			return content
		}
		return string(runes[:2*searchSnippetRadius]) + "…"
	}
	matchStart := len([]rune(folded[:matchAt]))
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
