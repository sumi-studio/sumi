package fileaccess

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
)

// StatInfo mirrors the filesvc stat payload.
type StatInfo struct {
	Kind           string
	Size           int64
	MtimeNS        int64
	Version        int64
	Fingerprint    string
	// ContentSHA is the sha256 the service recorded for the last write it
	// admitted — the durable per-file content receipt. Empty for files
	// that arrived outside the service (external_change says whether the
	// record may be stale).
	ContentSHA     string
	ExternalChange bool
}

func (c *Client) Stat(ctx context.Context, scope, path string) (StatInfo, error) {
	out, _, err := c.doJSON(ctx, scope, "stat", http.MethodGet,
		url.Values{"path": {path}}, nil, nil)
	if err != nil {
		return StatInfo{}, err
	}
	var st StatInfo
	st.Kind, _ = out["kind"].(string)
	st.Fingerprint, _ = out["fingerprint"].(string)
	st.ContentSHA, _ = out["content_sha"].(string)
	st.ExternalChange, _ = out["external_change"].(bool)
	st.Size = jsonInt(out["size"])
	st.MtimeNS = jsonInt(out["mtime_ns"])
	st.Version = jsonInt(out["version"])
	return st, nil
}

// ListResult mirrors the filesvc list payload.
type ListResult struct {
	Entries    []any
	NextCursor string
}

func (c *Client) List(ctx context.Context, scope, path, cursor string, limit int) (ListResult, error) {
	q := url.Values{"path": {path}}
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	out, _, err := c.doJSON(ctx, scope, "list", http.MethodGet, q, nil, nil)
	if err != nil {
		return ListResult{}, err
	}
	var res ListResult
	if e, ok := out["entries"].([]any); ok {
		res.Entries = e
	}
	res.NextCursor, _ = out["next_cursor"].(string)
	return res, nil
}

// ReadResult is one bounded read page.
type ReadResult struct {
	Body           []byte
	Version        int64
	ExternalChange bool
}

// Read fetches up to len bytes at offset; len < 0 means "to EOF".
func (c *Client) Read(ctx context.Context, scope, path string, offset, length int64) (ReadResult, error) {
	q := url.Values{"path": {path}}
	if offset > 0 {
		q.Set("offset", strconv.FormatInt(offset, 10))
	}
	if length >= 0 {
		q.Set("len", strconv.FormatInt(length, 10))
	}
	resp, err := c.ProxyOp(ctx, scope, "read", http.MethodGet, q, nil, nil)
	if err != nil {
		return ReadResult{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return ReadResult{}, decodeServiceError(resp)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return ReadResult{}, fmt.Errorf("fileaccess: read filesvc body: %w", err)
	}
	var res ReadResult
	res.Body = body
	res.Version, _ = strconv.ParseInt(resp.Header.Get("X-File-Version"), 10, 64)
	res.ExternalChange = resp.Header.Get("X-External-Change") == "true"
	return res, nil
}

// Write stores body under ifVersion semantics ("none"|<n>; "any" is never
// sent by the effect layer — see effects.go for why).
func (c *Client) Write(ctx context.Context, scope, path, ifVersion string, body []byte) (int64, error) {
	ver, _, err := c.WriteKeyed(ctx, scope, path, ifVersion, "", body)
	return ver, err
}

// WriteKeyed is Write carrying the caller's durable operation identity as
// X-Idempotency-Key. When the service already committed this exact
// operation, the response is the recorded receipt — replayed=true — and no
// second mutation ran. The receipt answers regardless of what later
// operations did to the path; only a same-key DIFFERENT request is refused
// (409 idempotency_conflict).
func (c *Client) WriteKeyed(ctx context.Context, scope, path, ifVersion, opKey string, body []byte) (int64, bool, error) {
	h := http.Header{"If-Version": {ifVersion}}
	if opKey != "" {
		h.Set("X-Idempotency-Key", opKey)
	}
	out, _, err := c.doJSON(ctx, scope, "write", http.MethodPut,
		url.Values{"path": {path}}, h, bytes.NewReader(body))
	if err != nil {
		return 0, false, err
	}
	replayed, _ := out["replayed"].(bool)
	return jsonInt(out["version"]), replayed, nil
}

// WriteKeyedBody is WriteKeyed for a streamed body: the file service
// accepts a whole-file body up to its service ceiling, so a copier can
// stream large files without holding them in memory. Callers that reuse
// opKey must send identical bytes — a same-key different-content request
// is refused as an idempotency conflict.
func (c *Client) WriteKeyedBody(ctx context.Context, scope, path, ifVersion, opKey string, body io.Reader) (int64, bool, error) {
	h := http.Header{"If-Version": {ifVersion}}
	if opKey != "" {
		h.Set("X-Idempotency-Key", opKey)
	}
	out, _, err := c.doJSON(ctx, scope, "write", http.MethodPut,
		url.Values{"path": {path}}, h, body)
	if err != nil {
		return 0, false, err
	}
	replayed, _ := out["replayed"].(bool)
	return jsonInt(out["version"]), replayed, nil
}

func (c *Client) Mkdir(ctx context.Context, scope, path string) (int64, error) {
	ver, _, err := c.MkdirKeyed(ctx, scope, path, "")
	return ver, err
}

func (c *Client) MkdirKeyed(ctx context.Context, scope, path, opKey string) (int64, bool, error) {
	payload, _ := json.Marshal(map[string]string{"path": path})
	h := http.Header{"Content-Type": {"application/json"}}
	if opKey != "" {
		h.Set("X-Idempotency-Key", opKey)
	}
	out, _, err := c.doJSON(ctx, scope, "mkdir", http.MethodPost, nil, h, bytes.NewReader(payload))
	if err != nil {
		return 0, false, err
	}
	replayed, _ := out["replayed"].(bool)
	return jsonInt(out["version"]), replayed, nil
}

func (c *Client) Remove(ctx context.Context, scope, path, ifVersion string) error {
	_, err := c.RemoveKeyed(ctx, scope, path, ifVersion, "")
	return err
}

func (c *Client) RemoveKeyed(ctx context.Context, scope, path, ifVersion, opKey string) (bool, error) {
	h := http.Header{"If-Version": {ifVersion}}
	if opKey != "" {
		h.Set("X-Idempotency-Key", opKey)
	}
	out, _, err := c.doJSON(ctx, scope, "remove", http.MethodDelete,
		url.Values{"path": {path}}, h, nil)
	if err != nil {
		return false, err
	}
	replayed, _ := out["replayed"].(bool)
	return replayed, nil
}

func jsonInt(v any) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int64:
		return n
	case json.Number:
		i, _ := n.Int64()
		return i
	}
	return 0
}
