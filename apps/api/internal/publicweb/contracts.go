// Package publicweb reads bounded unauthenticated public HTTPS documents.
package publicweb

import (
	"net/netip"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"
)

const MaxURLBytes = 8192
const MaxBodyBytes = 1 << 20
const MaxTextBytes = 128 << 10
const maxTitleBytes = 1024
const MaxLinks = 100
const MaxLinkURLBytes = 32 << 10

// JSON may escape one source byte into six bytes (including HTML-sensitive
// characters). Include both source URLs, all link URLs and fixed-field headroom.
const MaxResponseBytes = 6*(MaxTextBytes+MaxLinkURLBytes+2*MaxURLBytes+maxTitleBytes) + (16 << 10)

type Request struct {
	URL string `json:"url"`
}
type Link struct {
	ID  int    `json:"id"`
	URL string `json:"url"`
}

type Result struct {
	Links          []Link    `json:"links"`
	LinksTruncated bool      `json:"links_truncated"`
	RequestedURL   string    `json:"requested_url"`
	FetchedURL     string    `json:"fetched_url"`
	FetchedAt      time.Time `json:"fetched_at"`
	StatusCode     int       `json:"status_code"`
	MediaType      string    `json:"media_type"`
	Title          *string   `json:"title"`
	Text           string    `json:"text"`
	BodyBytes      int       `json:"body_bytes"`
	BodySHA256     string    `json:"body_sha256"`
	TextTruncated  bool      `json:"text_truncated"`
}
type Failure struct {
	Code         string `json:"error"`
	Reason       string `json:"reason,omitempty"`
	RequestedURL string `json:"requested_url,omitempty"`
	StatusCode   int    `json:"status_code,omitempty"`
	RedirectURL  string `json:"redirect_url,omitempty"`
}

func (f *Failure) Error() string  { return f.Code }
func fail(code string) *Failure   { return &Failure{Code: code} }
func (r Request) Validate() error { _, err := parseURL(r.URL); return err }
func networkURL(raw string) string {
	if i := strings.IndexByte(raw, '#'); i >= 0 {
		return raw[:i]
	}
	return raw
}
func parseURL(raw string) (*url.URL, error) {
	if len(raw) == 0 || len(raw) > MaxURLBytes || !utf8.ValidString(raw) {
		return nil, fail("invalid_url")
	}
	for _, c := range raw {
		if c <= 0x20 || c == 0x7f || c == '\\' {
			return nil, fail("invalid_url")
		}
	}
	for i := 0; i < len(raw); i++ {
		if raw[i] == '%' {
			if i+2 >= len(raw) || !hexDigit(raw[i+1]) || !hexDigit(raw[i+2]) {
				return nil, fail("invalid_url")
			}
			i += 2
		}
	}
	u, err := url.Parse(raw)
	if err != nil || !strings.EqualFold(u.Scheme, "https") || u.Opaque != "" || u.Host == "" || u.User != nil {
		return nil, fail("invalid_url")
	}
	if !strings.HasPrefix(strings.ToLower(raw), "https://") || strings.ContainsAny(u.Host, "%\\") || strings.HasSuffix(u.Host, ":") || (u.Port() != "" && u.Port() != "443") {
		return nil, fail("invalid_url")
	}
	host := u.Hostname()
	if host == "" {
		return nil, fail("invalid_url")
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		if ip.Zone() != "" || (ip.Is4() && (ip.String() != host || strings.HasPrefix(u.Host, "["))) {
			return nil, fail("invalid_url")
		}
	} else {
		if strings.ContainsAny(host, ":[]") {
			return nil, fail("invalid_url")
		}
		name := strings.TrimSuffix(host, ".")
		labels := strings.Split(name, ".")
		if len(name) > 253 || len(labels) < 2 {
			return nil, fail("invalid_url")
		}
		for _, label := range labels {
			if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
				return nil, fail("invalid_url")
			}
			for _, c := range label {
				if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-') {
					return nil, fail("invalid_url")
				}
			}
		}
		numeric := true
		for _, c := range labels[len(labels)-1] {
			if c < '0' || c > '9' {
				numeric = false
			}
		}
		if numeric {
			return nil, fail("invalid_url")
		}
	}
	u.Scheme = "https"
	u.Fragment = ""
	u.RawFragment = ""
	return u, nil
}

func hexDigit(b byte) bool {
	return b >= '0' && b <= '9' || b >= 'a' && b <= 'f' || b >= 'A' && b <= 'F'
}
