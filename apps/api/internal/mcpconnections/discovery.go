package mcpconnections

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
)

// Discovery hands the secretary whole tool definitions in bounded pages.
//
// A server chooses its own page size, and one of its pages can carry more
// complete schemas than a durable job result may hold. Shortening a schema
// would invite guessed arguments, so this re-paginates instead: each result
// carries complete definitions for as many tools as fit, and a continuation
// cursor that resumes inside the same upstream page. Nothing is skipped
// silently — a tool whose own definition can never fit a page is named in
// tools_omitted with its size, and the cursor still advances past it so the
// rest of the collection stays reachable.
//
// `names` answers the other half: once the secretary knows a name (from an
// earlier page, or from next_names), it can ask for exactly those complete
// definitions instead of paging to reach them.
//
// Two boundaries hold this together, and both are about the transformation a
// result passes through before it is stored (NUL replacement and redaction of
// declared private values):
//
//   - Every size here is measured on the *transformed* copy, through persist,
//     the same function execute persists with. A definition that fits before
//     redaction and not after would otherwise be packed, then dropped by the
//     durable bound, while the cursor had already moved past it.
//   - The continuation cursor is made only of this package's own integers and
//     a digest. No byte of it comes from the server, which is what makes it
//     safe to be the single value that transformation skips — a configured
//     private value as short as "1" would otherwise rewrite its prefix or its
//     base64 body into something that cannot be read back.
const (
	// toolPageBudget bounds the transformed tool definitions in one result.
	// It sits well under resultBoundBytes so the page's own metadata, a
	// continuation cursor and later notifications still fit underneath; the
	// fit check in fitPage is what actually guarantees the result fits.
	toolPageBudget = 44 << 10
	// discoveryScanPages bounds how many upstream pages discovery walks,
	// matching the call path's lookup. It also bounds a cursor's page number.
	discoveryScanPages = 32
	// maxSelectedNames bounds one by-name discovery request.
	maxSelectedNames = 32
	// maxOmittedPerPage bounds the individually-too-large tools named in
	// one result before the cursor defers the rest to the next page.
	maxOmittedPerPage = 16
	// maxNextNames / maxNextNamesBytes bound the "still waiting on this
	// upstream page" name list. Names are cheap; schemas are not.
	maxNextNames      = 100
	maxNextNamesBytes = 4 << 10
	// notificationHeadroom is what a page leaves for events that arrive after
	// it was built: a discovery session only receives tools/list_changed,
	// which carries no payload, but the room must exist.
	notificationHeadroom = 2 << 10

	discoveryCursorPrefix = "sumi.tools.3:"
	// cursorKey is the one result key whose value this package mints and the
	// transformation skips. Named once, used by both sides of that boundary.
	cursorKey = "next_cursor"
)

// ErrDiscoveryCursor is a deterministic request-shape error: a cursor did not
// come from this package, or came back corrupted, so no page can be addressed.
var ErrDiscoveryCursor = errors.New("MCP discovery cursor is not valid; repeat mcp.list_tools without a cursor")

// discoveryCursor resumes discovery at an exact position: Page is how many
// upstream pages to walk past, Offset how many of that page's tools were
// already delivered, Digest the page's tool-name sequence as observed then, and Prefix the
// chained digests of every preceding page. They are evidence, not authority:
// any detected change restarts at page zero, including backward shifts.
//
// It deliberately holds no server-provided bytes. Carrying the server's own
// cursor here would mean handing remote data back to the model inside a
// reversible encoding, where redaction could not see it.
type discoveryCursor struct {
	Page   int    `json:"p,omitempty"`
	Offset int    `json:"o,omitempty"`
	Digest string `json:"d,omitempty"`
	Prefix string `json:"h,omitempty"`
}

func encodeDiscoveryCursor(c discoveryCursor) (string, error) {
	if c.Page < 0 || c.Page >= discoveryScanPages || c.Offset < 0 {
		return "", ErrDiscoveryCursor
	}
	raw, e := json.Marshal(c)
	if e != nil {
		return "", e
	}
	return discoveryCursorPrefix + base64.RawURLEncoding.EncodeToString(raw), nil
}

// decodeDiscoveryCursor accepts an empty cursor or one this package minted.
// Anything else is refused: a cursor is Sumi's own, not a server's.
func decodeDiscoveryCursor(s string) (discoveryCursor, error) {
	if s == "" {
		return discoveryCursor{}, nil
	}
	rest, ours := strings.CutPrefix(s, discoveryCursorPrefix)
	if !ours {
		return discoveryCursor{}, ErrDiscoveryCursor
	}
	raw, e := base64.RawURLEncoding.DecodeString(rest)
	if e != nil {
		return discoveryCursor{}, ErrDiscoveryCursor
	}
	var c discoveryCursor
	if json.Unmarshal(raw, &c) != nil || c.Page < 0 || c.Page >= discoveryScanPages || c.Offset < 0 || len(c.Digest) > 32 || len(c.Prefix) > 32 || (c.Page > 0 && c.Prefix == "") || (c.Offset > 0 && c.Digest == "") {
		return discoveryCursor{}, ErrDiscoveryCursor
	}
	return c, nil
}

// pageDigest identifies one upstream page by its tool-name sequence.
func pageDigest(tools []*mcp.Tool) string {
	h := sha256.New()
	for _, t := range tools {
		h.Write([]byte(t.Name))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil)[:6])
}

// extendPrefix fingerprints the walked name sequence and page boundaries.
// No upstream cursor or recoverable remote content enters the model's cursor.
func extendPrefix(prefix, digest string) string {
	h := sha256.Sum256([]byte(prefix + ":" + digest))
	return hex.EncodeToString(h[:16])
}

// definitionProtected checks parsed strings and keys, not JSON escape bytes.
// A redacted definition is not the server's complete callable definition.
func definitionProtected(tool *mcp.Tool, secrets []string) bool {
	raw, e := json.Marshal(tool)
	if e != nil {
		return true
	}
	var v any
	if json.Unmarshal(raw, &v) != nil {
		return true
	}
	return containsProtected(v, secrets)
}

func containsProtected(v any, secrets []string) bool {
	switch x := v.(type) {
	case string:
		for _, secret := range secrets {
			if secret != "" && strings.Contains(x, secret) {
				return true
			}
		}
	case map[string]any:
		for key, value := range x {
			if containsProtected(key, secrets) || containsProtected(value, secrets) {
				return true
			}
		}
	case []any:
		for _, value := range x {
			if containsProtected(value, secrets) {
				return true
			}
		}
	}
	return false
}

// selectedNames reads an already validated `names` request field.
func selectedNames(request map[string]any) []string {
	raw, _ := request["names"].([]any)
	out := make([]string, 0, len(raw))
	for _, v := range raw {
		if s, ok := v.(string); ok && s != "" {
			out = append(out, s)
		}
	}
	return out
}

// validateDiscoveryRequest is the deterministic shape check shared by the
// pre-approval validator and execution. It reads no state and has no effect.
func validateDiscoveryRequest(req map[string]any) error {
	cursor, _ := req["cursor"].(string)
	if _, e := decodeDiscoveryCursor(cursor); e != nil {
		return fmt.Errorf("%w: %v", agentstate.ErrBadRequest, e)
	}
	raw, present := req["names"]
	if !present || raw == nil {
		return nil
	}
	list, ok := raw.([]any)
	if !ok || len(list) == 0 || len(list) > maxSelectedNames {
		return fmt.Errorf("%w: names must be an array of 1-%d tool names", agentstate.ErrBadRequest, maxSelectedNames)
	}
	if cursor != "" {
		return fmt.Errorf("%w: use either names or cursor, not both", agentstate.ErrBadRequest)
	}
	seen := map[string]bool{}
	for _, v := range list {
		name, ok := v.(string)
		if !ok || name == "" || len(name) > 256 || strings.ContainsAny(name, "\x00\n\r") {
			return fmt.Errorf("%w: each name must be a 1-256 character tool name", agentstate.ErrBadRequest)
		}
		if seen[name] {
			return fmt.Errorf("%w: names must not repeat %q", agentstate.ErrBadRequest, name)
		}
		seen[name] = true
	}
	return nil
}

// persistedToolSize is what this definition will occupy once stored: the
// transformation is applied to a copy and the result measured. Measuring the
// untransformed definition would let a page carry schemas that no longer fit
// by the time they are written, after the durable bound has dropped them.
func persistedToolSize(tool *mcp.Tool, secrets []string) int {
	raw, e := json.Marshal(tool)
	if e != nil {
		return toolPageBudget + 1
	}
	var copied any
	if json.Unmarshal(raw, &copied) != nil {
		return toolPageBudget + 1
	}
	out, e := json.Marshal(scrub(copied, secrets))
	if e != nil {
		return toolPageBudget + 1
	}
	return len(out)
}

// persistedSize is the durable size of a whole result, measured through the
// same boundary execute stores it through.
func persistedSize(result map[string]any, secrets []string) int {
	raw, e := json.Marshal(persist(normalize(result), secrets))
	if e != nil {
		return 1 << 30
	}
	return len(raw)
}

// pagePlan is one candidate page: what it delivers whole, what it names as
// too large to deliver, and how many input tools that accounts for.
type pagePlan struct {
	packed   []*mcp.Tool
	omitted  []map[string]any
	consumed int
}

// packTools takes complete definitions from the front of tools until the page
// budget is reached, measuring each as it will be stored. A definition larger
// than a whole page can never be delivered: it is named in omitted (with its
// stored size) and consumed, so the cursor moves past it instead of stalling
// the rest of the collection.
func packTools(tools []*mcp.Tool, secrets []string) pagePlan {
	plan := pagePlan{packed: []*mcp.Tool{}}
	used := 0
	for _, tool := range tools {
		size := persistedToolSize(tool, secrets)
		if definitionProtected(tool, secrets) {
			if len(plan.omitted) >= maxOmittedPerPage {
				break
			}
			plan.omitted = append(plan.omitted, map[string]any{
				"name": safeToolName(tool.Name, secrets), "bytes": size,
				"reason": "definition contains a protected configuration value; unavailable until the server definition or private-value configuration is corrected",
			})
			plan.consumed++
			continue
		}
		// The same accounting as the packing test below, so the first tool of a
		// page either fits or is recorded as indivisible. A tool that satisfied
		// neither would consume nothing and hand back the cursor it arrived
		// with, stalling the collection at that tool forever.
		if size+1 > toolPageBudget {
			if len(plan.omitted) >= maxOmittedPerPage {
				break
			}
			plan.omitted = append(plan.omitted, map[string]any{
				"name":  safeToolName(tool.Name, secrets),
				"bytes": size,
				"reason": fmt.Sprintf(
					"this tool's complete definition needs %d bytes once stored, past the %d KiB discovery page; Sumi will not shorten a schema. Call it only with arguments the server documents elsewhere.",
					size, toolPageBudget>>10),
			})
			plan.consumed++
			continue
		}
		if used+size+1 > toolPageBudget {
			break
		}
		plan.packed = append(plan.packed, tool)
		used += size + 1
		plan.consumed++
	}
	return plan
}

// Scrub before truncating: a long name may put a private value across the
// truncation boundary. A shortened credential must not be exposed either.
func safeToolName(name string, secrets []string) string {
	return boundName(scrub(name, secrets).(string))
}

func boundName(name string) string {
	if len(name) > 256 {
		return name[:256]
	}
	return name
}

// nextNames lists what is still waiting on this upstream page, bounded by
// count and bytes. Names let the secretary decide whether to page on or ask
// for exact definitions with `names`; they are never a substitute for a schema.
func nextNames(tools []*mcp.Tool, secrets []string) ([]string, bool) {
	out := []string{}
	bytes := 0
	for _, tool := range tools {
		if definitionProtected(tool, secrets) {
			continue
		}
		if len(out) >= maxNextNames || bytes+len(tool.Name)+3 > maxNextNamesBytes {
			return out, true
		}
		out = append(out, safeToolName(tool.Name, secrets))
		bytes += len(tool.Name) + 3
	}
	return out, false
}

// listPage fetches one upstream page, reporting a problem string on failure.
func listPage(ctx context.Context, session *mcp.ClientSession, cursor string, result map[string]any, secrets []string) (*mcp.ListToolsResult, string) {
	list, e := session.ListTools(ctx, &mcp.ListToolsParams{Cursor: cursor})
	if e != nil {
		result["error_detail"] = safeError(e, secrets)
		return nil, "MCP tools/list failed"
	}
	return list, ""
}

// pageAt re-walks at most 32 upstream pages, retaining only the first and
// target pages. Prefix detects changes anywhere before the target, including
// a tool sliding backward across a consumed page boundary. This is detection
// between observations, not a snapshot of a concurrently changing server.
func pageAt(ctx context.Context, session *mcp.ClientSession, page int, result map[string]any, secrets []string) (list, first *mcp.ListToolsResult, prefix, problem string) {
	cursor := ""
	seen := map[string]bool{}
	for index := 0; index <= page; index++ {
		list, problem = listPage(ctx, session, cursor, result, secrets)
		if problem != "" {
			return
		}
		if index == 0 {
			first = list
		}
		if index == page || list.NextCursor == "" {
			return
		}
		if seen[list.NextCursor] {
			problem = "MCP tools/list returned a repeated cursor"
			return
		}
		prefix = extendPrefix(prefix, pageDigest(list.Tools))
		cursor = list.NextCursor
		seen[cursor] = true
	}
	return
}

// discoverTools fills result with one bounded page of complete definitions.
func discoverTools(ctx context.Context, session *mcp.ClientSession, request map[string]any, result map[string]any, secrets []string) string {
	if names := selectedNames(request); len(names) > 0 {
		return discoverByName(ctx, session, names, result, secrets)
	}
	cursorText, _ := request["cursor"].(string)
	c, e := decodeDiscoveryCursor(cursorText)
	if e != nil {
		return e.Error()
	}
	list, first, prefix, problem := pageAt(ctx, session, c.Page, result, secrets)
	if problem != "" {
		return problem
	}
	digest := pageDigest(list.Tools)
	if c.Prefix != prefix || (c.Digest != "" && c.Digest != digest) {
		// Starting at only the target page could still strand a tool that
		// moved into an earlier consumed page. Re-show from the first page.
		list, c, prefix = first, discoveryCursor{}, ""
		digest = pageDigest(list.Tools)
		result["page_changed"] = true
	}
	offset := c.Offset
	if offset > len(list.Tools) {
		offset = len(list.Tools)
	}
	return fitPage(result, list, c.Page, offset, digest, prefix, secrets)
}

// fitPage settles the page and its continuation together, so the cursor can
// only ever advance past tools this result actually delivered or named. The
// byte budget gets close; this check is what makes the decision correspond to
// the durable bytes.
func fitPage(result map[string]any, list *mcp.ListToolsResult, page, offset int, digest, prefix string, secrets []string) string {
	rest := list.Tools[offset:]
	plan := packTools(rest, secrets)
	withNames := true
	for {
		if problem := applyPage(result, list, page, offset, digest, prefix, plan, withNames, secrets); problem != "" {
			return problem
		}
		if persistedSize(result, secrets) <= resultBoundBytes-notificationHeadroom {
			return ""
		}
		if withNames {
			// The waiting-name list is convenience, not content: it goes first.
			withNames = false
			continue
		}
		if plan.consumed == 0 {
			return "MCP discovery cannot fit this page inside the durable result bound; ask for specific tools with names"
		}
		plan = packTools(rest[:plan.consumed-1], secrets)
	}
}

// applyPage renders one candidate page. Everything it can set it also clears,
// so a smaller candidate never inherits a larger one's metadata.
func applyPage(result map[string]any, list *mcp.ListToolsResult, page, offset int, digest, prefix string, plan pagePlan, withNames bool, secrets []string) string {
	for _, key := range []string{"tools_omitted", "remaining_on_page", "next_names", "next_names_truncated", "scan_truncated"} {
		delete(result, key)
	}
	result["tools"] = plan.packed
	if len(plan.omitted) > 0 {
		result["tools_omitted"] = plan.omitted
	}
	rest := list.Tools[offset+plan.consumed:]
	switch {
	case len(rest) > 0:
		next, e := encodeDiscoveryCursor(discoveryCursor{Page: page, Offset: offset + plan.consumed, Digest: digest, Prefix: prefix})
		if e != nil {
			result[cursorKey] = ""
			return e.Error()
		}
		result[cursorKey] = next
		result["remaining_on_page"] = len(rest)
		if withNames {
			waiting, truncated := nextNames(rest, secrets)
			result["next_names"] = waiting
			if truncated {
				result["next_names_truncated"] = true
			}
		}
	case list.NextCursor == "":
		result[cursorKey] = ""
	case page+1 < discoveryScanPages:
		next, e := encodeDiscoveryCursor(discoveryCursor{Page: page + 1, Prefix: extendPrefix(prefix, digest)})
		if e != nil {
			result[cursorKey] = ""
			return e.Error()
		}
		result[cursorKey] = next
	default:
		// The server offers more pages than discovery walks. Say so instead of
		// implying the collection ended here.
		result[cursorKey] = ""
		result["scan_truncated"] = true
	}
	return ""
}

// discoverByName returns complete definitions for exactly the requested tools,
// scanning the same bounded number of upstream pages the call path uses.
func discoverByName(ctx context.Context, session *mcp.ClientSession, names []string, result map[string]any, secrets []string) string {
	want := map[string]bool{}
	for _, name := range names {
		want[name] = true
	}
	found := map[string]*mcp.Tool{}
	cursor := ""
	seen := map[string]bool{}
	scanned := 0
	for len(found) < len(want) {
		// The scan is bounded exactly as the call path's lookup is. Reaching
		// that bound is reported: "not found" would otherwise claim more than
		// this looked at.
		if scanned == discoveryScanPages {
			result["scan_truncated"] = true
			break
		}
		list, problem := listPage(ctx, session, cursor, result, secrets)
		if problem != "" {
			return problem
		}
		scanned++
		for _, tool := range list.Tools {
			if want[tool.Name] && found[tool.Name] == nil {
				found[tool.Name] = tool
			}
		}
		if list.NextCursor == "" {
			break
		}
		if seen[list.NextCursor] {
			return "MCP tools/list returned a repeated cursor"
		}
		cursor = list.NextCursor
		seen[cursor] = true
	}
	ordered := make([]*mcp.Tool, 0, len(found))
	missing := []string{}
	for _, name := range names {
		if tool := found[name]; tool != nil {
			ordered = append(ordered, tool)
		} else {
			missing = append(missing, safeToolName(name, secrets))
		}
	}
	result[cursorKey] = ""
	if len(missing) > 0 {
		result["names_not_found"] = missing
	}
	// Same settlement as a page: what is returned is what fits once stored,
	// and everything else is named rather than dropped.
	for limit := len(ordered); ; limit-- {
		plan := packTools(ordered[:limit], secrets)
		omitted := plan.omitted
		for _, tool := range ordered[plan.consumed:] {
			if len(omitted) >= maxOmittedPerPage {
				break
			}
			omitted = append(omitted, map[string]any{
				"name":   safeToolName(tool.Name, secrets),
				"reason": "did not fit in this discovery page; request it again with fewer names",
			})
		}
		result["tools"] = plan.packed
		delete(result, "tools_omitted")
		if len(omitted) > 0 {
			result["tools_omitted"] = omitted
		}
		if persistedSize(result, secrets) <= resultBoundBytes-notificationHeadroom {
			return ""
		}
		if limit == 0 {
			return "MCP discovery cannot fit these definitions inside the durable result bound; ask for fewer names"
		}
	}
}
