package publicweb

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"time"
	"unicode/utf8"
)

type resolver interface {
	LookupNetIP(context.Context, string, string) ([]netip.Addr, error)
}
type Fetcher struct {
	resolver resolver
	dial     func(context.Context, string, string) (net.Conn, error)
	roots    *x509.CertPool // test-only trust seam; production uses system roots
	slots    chan struct{}
	timeout  time.Duration
}

func NewFetcher() *Fetcher {
	return &Fetcher{resolver: net.DefaultResolver, dial: (&net.Dialer{Timeout: 5 * time.Second}).DialContext, slots: make(chan struct{}, 4), timeout: 15 * time.Second}
}

// beforeSend is a short authorization check after resolution. It never holds an
// epoch lease during network I/O; revocation cannot undo an already sent GET.
func (f *Fetcher) Read(ctx context.Context, r Request, beforeSend func() bool) (Result, error) {
	u, err := parseURL(r.URL)
	if err != nil {
		return Result{}, err
	}
	select {
	case f.slots <- struct{}{}:
		defer func() { <-f.slots }()
	default:
		return Result{}, fail("busy")
	}
	ctx, cancel := context.WithTimeout(ctx, f.timeout)
	defer cancel()
	host := u.Hostname()
	var addresses []netip.Addr
	if ip, e := netip.ParseAddr(host); e == nil {
		addresses = []netip.Addr{ip}
	} else {
		addresses, err = f.resolver.LookupNetIP(ctx, "ip", host)
		if err != nil {
			return Result{}, networkFailure(ctx, err, "dns_failed")
		}
	}
	if len(addresses) == 0 || len(addresses) > 16 {
		return Result{}, fail("dns_failed")
	}
	for _, ip := range addresses {
		if !publicAddress(ip) {
			return Result{}, fail("destination_not_public")
		}
	}
	// A fresh transport cannot reuse a connection, proxy, authentication or DNS
	// decision from another request. Dial only the already validated addresses.
	transport := &http.Transport{Proxy: nil, DisableKeepAlives: true, DisableCompression: true, MaxResponseHeaderBytes: 32 << 10, TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: 5 * time.Second, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, ServerName: host, RootCAs: f.roots}, DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		if address != net.JoinHostPort(host, "443") {
			return nil, fail("destination_not_public")
		}
		var last error
		for _, ip := range addresses {
			connection, e := f.dial(ctx, "tcp", net.JoinHostPort(ip.String(), "443"))
			if e == nil {
				return connection, nil
			}
			last = e
			if ctx.Err() != nil {
				break
			}
		}
		return nil, last
	}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return Result{}, fail("invalid_url")
	}
	request.Header.Set("Accept", "text/html, text/plain;q=0.9")
	request.Header.Set("Accept-Encoding", "identity")
	request.Header.Set("User-Agent", "Sumi-Public-URL-Reader/1.0")
	if beforeSend != nil && !beforeSend() {
		return Result{}, fail("runtime_epoch_changed")
	}
	response, err := client.Do(request)
	if err != nil {
		return Result{}, networkFailure(ctx, err, "fetch_failed")
	}
	defer response.Body.Close()
	if strings.EqualFold(strings.TrimSpace(response.Header.Get("Cf-Mitigated")), "challenge") {
		return Result{}, &Failure{Code: "access_challenge", StatusCode: response.StatusCode}
	}
	if response.StatusCode >= 300 && response.StatusCode < 400 {
		result := &Failure{Code: "redirect_requires_new_request", StatusCode: response.StatusCode}
		if location, e := response.Location(); e == nil {
			target := location.String()
			if _, e = parseURL(target); e == nil {
				result.RedirectURL = target
			}
		}
		return Result{}, result
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return Result{}, &Failure{Code: "http_status", StatusCode: response.StatusCode}
	}
	if encoding := strings.TrimSpace(response.Header.Get("Content-Encoding")); encoding != "" && !strings.EqualFold(encoding, "identity") {
		return Result{}, &Failure{Code: "unsupported_content", Reason: "content_encoding"}
	}
	media, params, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || (media != "text/html" && media != "text/plain") {
		return Result{}, &Failure{Code: "unsupported_content", Reason: "media_type"}
	}
	if charset := params["charset"]; charset != "" && !strings.EqualFold(charset, "utf-8") {
		return Result{}, &Failure{Code: "unsupported_content", Reason: "charset"}
	}
	if response.ContentLength > MaxBodyBytes {
		return Result{}, fail("response_too_large")
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, MaxBodyBytes+1))
	if err != nil {
		return Result{}, networkFailure(ctx, err, "fetch_failed")
	}
	if len(body) > MaxBodyBytes {
		return Result{}, fail("response_too_large")
	}
	if !utf8.Valid(body) {
		return Result{}, &Failure{Code: "unsupported_content", Reason: "invalid_utf8"}
	}
	links := &linkCollector{base: u, links: []Link{}, ids: map[string]int{}}
	text, title, truncated, err := extractDocument(ctx, body, media, links)
	if err != nil {
		return Result{}, err
	}
	if ctx.Err() != nil {
		return Result{}, networkFailure(ctx, ctx.Err(), "fetch_failed")
	}
	if strings.TrimSpace(text) == "" {
		return Result{}, fail("no_readable_text")
	}
	digest := sha256.Sum256(body)
	return Result{Links: links.links, LinksTruncated: links.truncated, RequestedURL: r.URL, FetchedURL: networkURL(r.URL), FetchedAt: time.Now().UTC(), StatusCode: response.StatusCode, MediaType: media, Title: title, Text: text, BodyBytes: len(body), BodySHA256: hex.EncodeToString(digest[:]), TextTruncated: truncated}, nil
}
func networkFailure(ctx context.Context, err error, fallback string) *Failure {
	if errors.Is(ctx.Err(), context.Canceled) || errors.Is(err, context.Canceled) {
		return fail("cancelled")
	}
	var netError net.Error
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.As(err, &netError) && netError.Timeout() {
		return fail("timeout")
	}
	var certificate *tls.CertificateVerificationError
	var record tls.RecordHeaderError
	if errors.As(err, &certificate) || errors.As(err, &record) {
		return fail("tls_failed")
	}
	return fail(fallback)
}
