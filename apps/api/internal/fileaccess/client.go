// Package fileaccess is the API-side client for the canonical file service
// (filesvc). filesvc is the single storage authority: the API holds one
// internal wildcard-grant credential and derives every caller's scope from
// server-side identity (the verified session's PAID for browser routes, the
// persona the operation ledger claims for core tools). No scope, persona, or
// credential ever crosses from the client side into a filesvc request.
package fileaccess

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// scopeRe is the only scope shape this client will ever emit: the compact
// (dashless, lower-hex) form of a PersonalityAgentID. Enforcing it here —
// not just at the handlers — guarantees an empty or attacker-influenced
// value can never reach a filesvc wildcard or escape the per-scope subtree.
var scopeRe = regexp.MustCompile(`^[0-9a-f]{32}$`)

// ScopeForPersona maps a persona/PAID to its canonical filesvc scope.
func ScopeForPersona(personaID string) (string, error) {
	scope := strings.ToLower(strings.ReplaceAll(personaID, "-", ""))
	if !scopeRe.MatchString(scope) {
		return "", fmt.Errorf("persona id %q does not map to a canonical file scope", personaID)
	}
	return scope, nil
}

// opsAllowed is the subset of the filesvc surface this slice exposes. It is
// enforced at the client so no caller can mint an arbitrary upstream op.
var opsAllowed = map[string]bool{
	"stat": true, "list": true, "read": true,
	"write": true, "mkdir": true, "remove": true,
}

// Client calls filesvc with the internal service credential.
type Client struct {
	base  *url.URL
	token string
	hc    *http.Client
}

// NewClient builds a client for baseURL ("http://host:8780") authenticated
// with the internal service token.
func NewClient(baseURL, token string) (*Client, error) {
	base, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil || base == nil || (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" {
		return nil, fmt.Errorf("fileaccess: bad filesvc url %q", baseURL)
	}
	if strings.TrimSpace(token) == "" {
		return nil, fmt.Errorf("fileaccess: empty filesvc token")
	}
	return &Client{
		base:  base,
		token: strings.TrimSpace(token),
		hc:    &http.Client{Timeout: 120 * time.Second},
	}, nil
}

// FromEnv wires the client from SUMI_FILESVC_URL plus either
// SUMI_FILESVC_TOKEN or SUMI_FILESVC_TOKEN_FILE (a root-owned file whose
// content is the token). Both-or-neither: a URL without a credential, or a
// credential without a URL, is a startup error rather than a half-enabled
// capability. Neither set returns (nil, nil) — files stay unconfigured.
func FromEnv(getenv func(string) string) (*Client, error) {
	rawURL := strings.TrimSpace(getenv("SUMI_FILESVC_URL"))
	token := strings.TrimSpace(getenv("SUMI_FILESVC_TOKEN"))
	if token == "" {
		if f := strings.TrimSpace(getenv("SUMI_FILESVC_TOKEN_FILE")); f != "" {
			b, err := os.ReadFile(f)
			if err != nil {
				return nil, fmt.Errorf("fileaccess: read SUMI_FILESVC_TOKEN_FILE: %w", err)
			}
			token = strings.TrimSpace(string(b))
		}
	}
	if rawURL == "" && token == "" {
		return nil, nil
	}
	if rawURL == "" || token == "" {
		return nil, fmt.Errorf("fileaccess: SUMI_FILESVC_URL and a token (SUMI_FILESVC_TOKEN or SUMI_FILESVC_TOKEN_FILE) must both be set")
	}
	return NewClient(rawURL, token)
}

// ServiceError is a non-2xx filesvc response, with the service's own error
// code preserved so callers can map it truthfully instead of guessing.
type ServiceError struct {
	Status  int
	Code    string
	Message string
}

func (e *ServiceError) Error() string {
	return fmt.Sprintf("filesvc %d %s: %s", e.Status, e.Code, e.Message)
}

// ProxyOp issues one allowlisted filesvc operation for an already-derived
// scope and returns the raw upstream response for streaming. The caller
// closes resp.Body. headers may carry If-Version; the Authorization header
// is always this client's internal credential, never caller input.
func (c *Client) ProxyOp(ctx context.Context, scope, op, method string, query url.Values, headers http.Header, body io.Reader) (*http.Response, error) {
	if !opsAllowed[op] {
		return nil, fmt.Errorf("fileaccess: op %q is not exposed", op)
	}
	return c.send(ctx, scope, op, method, query, headers, body)
}

// send is the shared request path. The base URL's path prefix is
// preserved: the service normally mounts filesvc at an origin root, but
// the return-file proxy mounts it under /api/secretary-files on the API —
// a discarded prefix would send those ops to routes that do not exist.
func (c *Client) send(ctx context.Context, scope, op, method string, query url.Values, headers http.Header, body io.Reader) (*http.Response, error) {
	if !scopeRe.MatchString(scope) {
		return nil, fmt.Errorf("fileaccess: refusing non-canonical scope")
	}
	u := *c.base
	u.Path = strings.TrimRight(u.Path, "/") + "/v1/files/" + scope + "/" + op
	if query != nil {
		u.RawQuery = query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	for k, vs := range headers {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	return c.hc.Do(req)
}

// SetScopeFrozen persists or clears the scope's mutation barrier on the
// service. It is an administrative op, not part of the delegated-op
// allowlist — only the service's internal credential can carry it, and a
// scoped storage token can never freeze its own scope. owner names the
// lineage that set the barrier (a return session id) and ownerEpoch its
// durable generation (the session's file_epoch): a freeze older than the
// recorded lineage is refused, an unfreeze older than the barrier's
// lineage is a no-op, and a newer lineage legitimately releases an older
// retained barrier. A freeze returns once admitted effects have drained
// and the public tree observed stable — 409 drain_pending means the
// barrier stands but the cut is not yet certified; retry.
func (c *Client) SetScopeFrozen(ctx context.Context, scope, owner string, ownerEpoch int64, reason string, frozen bool) error {
	op := "unfreeze"
	if frozen {
		op = "freeze"
	}
	q := url.Values{}
	if reason != "" {
		q.Set("reason", reason)
	}
	if owner != "" {
		q.Set("owner", owner)
	}
	q.Set("epoch", strconv.FormatInt(ownerEpoch, 10))
	resp, err := c.send(ctx, scope, op, http.MethodPost, q, nil, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return decodeServiceError(resp)
	}
	return nil
}

// CutManifest fetches the scope's verified-stable cut manifest (GET
// /v1/files/{scope}/cut): the complete public-tree inventory — every
// path's kind, size, fingerprint, durable identity and link target —
// observed identical across two walks. The mover's copy verification
// brackets its copy with this call: a manifest equal before and after
// the copy means the copied bytes are a consistent snapshot; a
// drain_pending answer means the tree is still moving — retry, never
// copy a racy tree.
func (c *Client) CutManifest(ctx context.Context, scope string) (*http.Response, error) {
	return c.send(ctx, scope, "cut", http.MethodGet, nil, nil, nil)
}

// doJSON issues an op expecting a JSON object response.
func (c *Client) doJSON(ctx context.Context, scope, op, method string, query url.Values, headers http.Header, body io.Reader) (map[string]any, http.Header, error) {
	resp, err := c.ProxyOp(ctx, scope, op, method, query, headers, body)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return nil, nil, decodeServiceError(resp)
	}
	var out map[string]any
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&out); err != nil {
		return nil, nil, fmt.Errorf("fileaccess: decode filesvc %s response: %w", op, err)
	}
	return out, resp.Header, nil
}

func decodeServiceError(resp *http.Response) error {
	var body struct {
		Message string `json:"error"`
		Code    string `json:"code"`
	}
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	_ = json.Unmarshal(raw, &body)
	code := body.Code
	if code == "" {
		code = "service_error"
	}
	return &ServiceError{Status: resp.StatusCode, Code: code, Message: body.Message}
}
