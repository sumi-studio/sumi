// capture.go is the fileaccess side of the private immutable-capture
// primitive: the storage service's /v1/files/{scope}/capture and
// /v1/capture/{id}/... endpoints. Every call uses the client's internal
// wildcard credential and carries the caller's return lineage
// (owner, epoch) — the service verifies both against the scope's
// durable file_freeze barrier on every boundary. Local-facing callers
// never reach these paths directly; the return session's grant surface
// binds them to a persisted association.
package fileaccess

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// CaptureMeta mirrors the service's capture metadata object. ScopeID is
// the opaque identity of the volume+anchor the capture resolved — a
// scope renamed out and recreated under the same name produces a
// different ScopeID, which is how a retake detects that the grant no
// longer names the same tree.
type CaptureMeta struct {
	CaptureID   string    `json:"capture_id"`
	Scope       string    `json:"scope"`
	ScopeID     string    `json:"scope_id"`
	Owner       string    `json:"owner"`
	OwnerEpoch  int64     `json:"owner_epoch"`
	Volume      string    `json:"volume"`
	ManifestSHA string    `json:"manifest_sha"`
	Status      string    `json:"status"`
	Entries     int64     `json:"entries"`
	Unsupported int64     `json:"unsupported"`
	CreatedAt   time.Time `json:"created_at"`
	ExpiresAt   time.Time `json:"expires_at"`
}

// CaptureEntry is one manifest row. NameB64/PathB64 carry the raw bytes
// (base64); Name/Path are optional UTF-8 conveniences that may be absent
// or lossy — consumers must decode the base64 fields.
type CaptureEntry struct {
	Seq       int64  `json:"seq"`
	PathB64   string `json:"path_b64"`
	NameB64   string `json:"name_b64"`
	Path      string `json:"path"`
	Name      string `json:"name"`
	Type      string `json:"type"`
	Supported bool   `json:"supported"`
	Mode      uint32 `json:"mode"`
	UID       uint32 `json:"uid"`
	GID       uint32 `json:"gid"`
	Nlink     uint32 `json:"nlink"`
	Length    int64  `json:"length"`
	MtimeNS   int64  `json:"mtime_ns"`
	CtimeNS   int64  `json:"ctime_ns"`
	LinkB64   string `json:"link_b64"`
	Link      string `json:"link"`
	LinkGroup string `json:"link_group"`
	MapSHA    string `json:"map_sha"`
}

// CaptureEntriesPage is one page of manifest rows.
type CaptureEntriesPage struct {
	Entries    []CaptureEntry `json:"entries"`
	HasMore    bool           `json:"has_more"`
	NextCursor int64          `json:"next_cursor"`
}

// captureURL builds a /v1/capture/{id}[/op] URL — the capture endpoints
// are not scope-op paths, so send() cannot reach them. owner/epoch are
// the caller's durable lineage, mandatory on every request.
func (c *Client) captureURL(id, op, owner string, epoch int64, q url.Values) string {
	if q == nil {
		q = url.Values{}
	}
	q.Set("owner", owner)
	q.Set("epoch", strconv.FormatInt(epoch, 10))
	u := *c.base
	u.Path = strings.TrimRight(u.Path, "/") + "/v1/capture/" + id
	if op != "" {
		u.Path += "/" + op
	}
	u.RawQuery = q.Encode()
	return u.String()
}

func (c *Client) sendCapture(ctx context.Context, method, rawURL string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, rawURL, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	return c.hc.Do(req)
}

// CreateCapture records a durable manifest of the scope's complete
// namespace under the caller's lineage. expectedScopeID is empty on the
// first bind and the bound scope_id on a retake — a scope whose anchor
// identity changed since the expectation is refused (422) inside the
// service's own transaction.
func (c *Client) CreateCapture(ctx context.Context, scope, owner string, epoch int64, expectedScopeID string) (*CaptureMeta, error) {
	q := url.Values{"owner": {owner}, "epoch": {strconv.FormatInt(epoch, 10)}}
	if expectedScopeID != "" {
		q.Set("expected_scope_id", expectedScopeID)
	}
	resp, err := c.send(ctx, scope, "capture", http.MethodPost, q, nil, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return nil, decodeServiceError(resp)
	}
	var m CaptureMeta
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&m); err != nil {
		return nil, fmt.Errorf("fileaccess: decode capture response: %w", err)
	}
	return &m, nil
}

// GetCapture returns a capture's durable metadata under the caller's
// lineage. 404 unknown, 410 released/expired, 403 stale lineage.
func (c *Client) GetCapture(ctx context.Context, id, owner string, epoch int64) (*CaptureMeta, error) {
	resp, err := c.sendCapture(ctx, http.MethodGet, c.captureURL(id, "", owner, epoch, nil), nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return nil, decodeServiceError(resp)
	}
	var m CaptureMeta
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&m); err != nil {
		return nil, fmt.Errorf("fileaccess: decode capture response: %w", err)
	}
	return &m, nil
}

// CaptureEntries fetches one manifest page (cursor -1 = from the
// beginning). The caller loops until HasMore is false.
func (c *Client) CaptureEntries(ctx context.Context, id, owner string, epoch, cursor int64, limit int) (*CaptureEntriesPage, error) {
	resp, err := c.CaptureEntriesRaw(ctx, id, owner, epoch, cursor, limit)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return nil, decodeServiceError(resp)
	}
	var p CaptureEntriesPage
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<20)).Decode(&p); err != nil {
		return nil, fmt.Errorf("fileaccess: decode capture entries: %w", err)
	}
	return &p, nil
}

// CaptureEntriesRaw streams one manifest page verbatim — the caller
// forwards status and body without re-encoding. The caller closes the
// response body.
func (c *Client) CaptureEntriesRaw(ctx context.Context, id, owner string, epoch, cursor int64, limit int) (*http.Response, error) {
	q := url.Values{
		"cursor": {strconv.FormatInt(cursor, 10)},
		"limit":  {strconv.Itoa(limit)},
	}
	return c.sendCapture(ctx, http.MethodGet, c.captureURL(id, "entries", owner, epoch, q), nil)
}

// CaptureRead proxies one captured row's bytes. seq identifies the
// manifest row; offset/len bound the range (len 0 = to end of row). The
// raw response is returned for streaming — the caller closes the body
// and must treat a short body or a non-200 status as pending, never as
// content.
func (c *Client) CaptureRead(ctx context.Context, id, owner string, epoch, seq, offset, length int64) (*http.Response, error) {
	q := url.Values{"seq": {strconv.FormatInt(seq, 10)}}
	if offset > 0 {
		q.Set("offset", strconv.FormatInt(offset, 10))
	}
	if length > 0 {
		q.Set("len", strconv.FormatInt(length, 10))
	}
	return c.sendCapture(ctx, http.MethodGet, c.captureURL(id, "read", owner, epoch, q), nil)
}

// ReleaseCapture frees a capture's objects reservation under the
// caller's lineage. Idempotent: released/unknown captures are not an
// error to the releaser.
func (c *Client) ReleaseCapture(ctx context.Context, id, owner string, epoch int64) error {
	resp, err := c.sendCapture(ctx, http.MethodDelete, c.captureURL(id, "", owner, epoch, nil), nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
	if resp.StatusCode/100 != 2 && resp.StatusCode != http.StatusNotFound && resp.StatusCode != http.StatusGone {
		return decodeServiceError(resp)
	}
	return nil
}
