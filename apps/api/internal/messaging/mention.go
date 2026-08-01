package messaging

import (
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

// mentionBoundary is the set of runes allowed immediately after "@name" for it
// to count as a mention, mirroring DISPLAY_MENTION_BOUNDARY in
// apps/web/src/messaging/mention.ts so the composer's highlight and the
// server's admission-time binding agree on what is a mention.
const mentionBoundary = ".,!?、。！？:：;；()（）[]{}「」『』"

// resolveMentions binds "@表示名" occurrences in content to the active members
// of the place at admission time. Raw string matching is never used again
// after this point (契約ドラフト: mentionsはmembership lookupで解決済み
// ParticipantRefとして束縛). Matching is longest-name-first and each text
// range binds at most one display name — but every member sharing that exact
// display name is bound, since a caller typing an ambiguous name addressed
// all of them.
func resolveMentions(content string, members []MemberProfile) []ParticipantRef {
	named := make([]MemberProfile, 0, len(members))
	for _, m := range members {
		if m.DisplayName != "" {
			named = append(named, m)
		}
	}
	// Longest first so "@Kuro Prod" is not consumed by a member named "Kuro".
	// The sort is stable so members with equal-length names keep store order.
	sort.SliceStable(named, func(i, j int) bool {
		return utf8.RuneCountInString(named[i].DisplayName) > utf8.RuneCountInString(named[j].DisplayName)
	})

	type claim struct {
		start, end int
		name       string
	}
	type binding struct {
		ref   ParticipantRef
		start int
	}
	var (
		claims  []claim
		bound   []binding
		already = map[string]bool{}
	)
	for _, m := range named {
		needle := "@" + m.DisplayName
		for from := 0; ; {
			i := strings.Index(content[from:], needle)
			if i < 0 {
				break
			}
			start := from + i
			end := start + len(needle)
			from = start + 1
			if !boundaryAfter(content, end) {
				continue
			}
			ok := true
			for _, c := range claims {
				if start >= c.end || end <= c.start {
					continue
				}
				// Same range, same display name: another member shares the
				// name and is also addressed. Anything else is an overlap
				// with a longer (or earlier) name and loses.
				if !(c.start == start && c.end == end && c.name == m.DisplayName) {
					ok = false
				}
				break
			}
			if !ok {
				continue
			}
			claims = append(claims, claim{start: start, end: end, name: m.DisplayName})
			if !already[m.Participant.Key()] {
				already[m.Participant.Key()] = true
				bound = append(bound, binding{ref: m.Participant, start: start})
			}
			break
		}
	}
	// Return mentions in the order they appear in the content, not in
	// name-length order (which is a matching detail, not a meaning).
	sort.SliceStable(bound, func(i, j int) bool { return bound[i].start < bound[j].start })
	out := make([]ParticipantRef, len(bound))
	for i, b := range bound {
		out[i] = b.ref
	}
	return out
}

func boundaryAfter(content string, end int) bool {
	if end >= len(content) {
		return true
	}
	r, _ := utf8.DecodeRuneInString(content[end:])
	return unicode.IsSpace(r) || strings.ContainsRune(mentionBoundary, r)
}
