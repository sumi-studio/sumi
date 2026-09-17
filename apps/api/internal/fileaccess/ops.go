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
	h := http.Header{"If-Version": {ifVersion}}
	out, _, err := c.doJSON(ctx, scope, "write", http.MethodPut,
		url.Values{"path": {path}}, h, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	return jsonInt(out["version"]), nil
}

func (c *Client) Mkdir(ctx context.Context, scope, path string) (int64, error) {
	payload, _ := json.Marshal(map[string]string{"path": path})
	out, _, err := c.doJSON(ctx, scope, "mkdir", http.MethodPost, nil,
		http.Header{"Content-Type": {"application/json"}}, bytes.NewReader(payload))
	if err != nil {
		return 0, err
	}
	return jsonInt(out["version"]), nil
}

func (c *Client) Remove(ctx context.Context, scope, path, ifVersion string) error {
	h := http.Header{"If-Version": {ifVersion}}
	_, _, err := c.doJSON(ctx, scope, "remove", http.MethodDelete,
		url.Values{"path": {path}}, h, nil)
	return err
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
