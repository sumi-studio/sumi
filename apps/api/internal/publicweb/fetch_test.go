package publicweb

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"
)

type lookupFunc func(context.Context, string, string) ([]netip.Addr, error)

func (f lookupFunc) LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error) {
	return f(ctx, network, host)
}
func testFetcher(t *testing.T, handler http.HandlerFunc) (*Fetcher, *atomic.Int32) {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	f := NewFetcher()
	f.roots = x509.NewCertPool()
	f.roots.AddCert(server.Certificate())
	f.resolver = lookupFunc(func(context.Context, string, string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("8.8.8.8")}, nil
	})
	calls := new(atomic.Int32)
	f.dial = func(ctx context.Context, network, address string) (net.Conn, error) {
		calls.Add(1)
		if network != "tcp" || address != "8.8.8.8:443" {
			return nil, errors.New("unexpected unpinned destination")
		}
		return (&net.Dialer{}).DialContext(ctx, "tcp", server.Listener.Addr().String())
	}
	return f, calls
}
func code(t *testing.T, err error, want string) {
	t.Helper()
	var failure *Failure
	if !errors.As(err, &failure) || failure.Code != want {
		t.Fatalf("got %v, want %s", err, want)
	}
}
func TestURLContractFixture(t *testing.T) {
	raw, err := os.ReadFile("testdata/contract.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		URLCases []struct {
			URL        string `json:"url"`
			Valid      bool   `json:"valid"`
			FetchedURL string `json:"fetched_url"`
		} `json:"url_cases"`
		Success Result  `json:"success"`
		Failure Failure `json:"failure"`
	}
	if err = json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	for _, test := range fixture.URLCases {
		_, err := parseURL(test.URL)
		if (err == nil) != test.Valid {
			t.Errorf("%q valid=%v: %v", test.URL, test.Valid, err)
		}
		if test.Valid && networkURL(test.URL) != test.FetchedURL {
			t.Error("network URL changed")
		}
	}
	if fixture.Success.Title != nil || fixture.Success.MediaType != "text/plain" || fixture.Failure.Code != "redirect_requires_new_request" {
		t.Fatal("fixture decode")
	}
}
func TestSpecialPurposeDestinationsRejected(t *testing.T) {
	for _, address := range []string{"0.2.3.4", "10.1.2.3", "100.64.0.1", "127.0.0.1", "169.254.169.254", "172.31.255.255", "192.0.0.9", "192.0.2.1", "192.31.196.1", "192.52.193.1", "192.88.99.1", "192.168.1.1", "192.175.48.1", "198.18.0.1", "198.51.100.1", "203.0.113.1", "224.0.0.1", "240.1.2.3", "255.255.255.255", "::", "::1", "::ffff:8.8.8.8", "64:ff9b::808:808", "64:ff9b:1::1", "100::1", "100:0:0:1::1", "2001::1", "2001:3::1", "2001:db8::1", "2002:808:808::1", "2620:4f:8000::1", "3fff::1", "5f00::1", "fc00::1", "fe80::1", "ff02::1"} {
		if publicAddress(netip.MustParseAddr(address)) {
			t.Error("allowed special destination", address)
		}
	}
	for _, address := range []string{"8.8.8.8", "1.1.1.1", "2001:4860:4860::8888", "2606:4700:4700::1111"} {
		if !publicAddress(netip.MustParseAddr(address)) {
			t.Error("rejected public destination", address)
		}
	}
}
func TestFetchPinsDNSIgnoresProxyAndPreservesSource(t *testing.T) {
	var proxyCalls atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { proxyCalls.Add(1) }))
	defer proxy.Close()
	t.Setenv("HTTPS_PROXY", proxy.URL)
	body := []byte("<html><head><title>Observed title</title><script>secret-script</script></head><body><p>First <b>paragraph</b>.</p><p>日本語</p><template>hidden</template></body></html>")
	f, dials := testFetcher(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Host != "example.com" || r.RequestURI != "/a%2fb?q=1&q=2" || r.TLS.ServerName != "example.com" {
			t.Error("changed network authority or target", r.Host, r.RequestURI, r.TLS.ServerName)
		}
		if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" || r.Header.Get("Referer") != "" || r.Header.Get("Accept-Encoding") != "identity" {
			t.Error("unexpected request credentials/encoding")
		}
		w.Header().Set("Set-Cookie", "ignored=1")
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(body)
	})
	var lookups atomic.Int32
	f.resolver = lookupFunc(func(context.Context, string, string) ([]netip.Addr, error) {
		if lookups.Add(1) == 1 {
			return []netip.Addr{netip.MustParseAddr("8.8.8.8")}, nil
		}
		return []netip.Addr{netip.MustParseAddr("127.0.0.1")}, nil
	})
	request := Request{URL: "https://example.com/a%2fb?q=1&q=2#章"}
	result, err := f.Read(context.Background(), request, nil)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(body)
	if result.RequestedURL != request.URL || result.FetchedURL != networkURL(request.URL) || result.BodySHA256 != hex.EncodeToString(digest[:]) || result.BodyBytes != len(body) || result.Title == nil || *result.Title != "Observed title" || result.Text != "First paragraph.\n日本語" || result.TextTruncated || result.FetchedAt.IsZero() {
		t.Fatalf("unexpected result: %+v", result)
	}
	_, err = f.Read(context.Background(), request, nil)
	code(t, err, "destination_not_public")
	if lookups.Load() != 2 || dials.Load() != 1 || proxyCalls.Load() != 0 {
		t.Fatal("DNS was re-resolved during fetch or proxy used")
	}
}
func TestFetchRejectsMixedDNSBeforeDial(t *testing.T) {
	f, dials := testFetcher(t, func(http.ResponseWriter, *http.Request) { t.Error("unexpected request") })
	f.resolver = lookupFunc(func(context.Context, string, string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("8.8.8.8"), netip.MustParseAddr("127.0.0.1")}, nil
	})
	_, err := f.Read(context.Background(), Request{URL: "https://example.com"}, nil)
	code(t, err, "destination_not_public")
	if dials.Load() != 0 {
		t.Fatal("dial before whole DNS answer validation")
	}
}
func TestFetchRedirectIsDataNotAnotherRequest(t *testing.T) {
	var requests atomic.Int32
	f, _ := testFetcher(t, func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Location", "https://127.0.0.1/private?added=1#section")
		w.WriteHeader(302)
	})
	_, err := f.Read(context.Background(), Request{URL: "https://example.com"}, nil)
	code(t, err, "redirect_requires_new_request")
	failure := err.(*Failure)
	if failure.RedirectURL != "https://127.0.0.1/private?added=1#section" || requests.Load() != 1 {
		t.Fatal(failure, requests.Load())
	}
}
func TestFetchOrdinaryFailures(t *testing.T) {
	tests := []struct {
		name, contentType, encoding, body, want string
		status                                  int
	}{
		{"status", "text/plain", "", "no", "http_status", 404},
		{"binary", "application/pdf", "", "pdf", "unsupported_content", 200},
		{"charset", "text/plain; charset=iso-8859-1", "", "text", "unsupported_content", 200},
		{"gzip", "text/plain", "gzip", "text", "unsupported_content", 200},
		{"invalidUTF8", "text/plain", "", string([]byte{255}), "unsupported_content", 200},
		{"empty", "text/html", "", "<script>only script</script>", "no_readable_text", 200},
		{"htmlcharset", "text/html", "", "<meta charset=iso-8859-1><p>text</p>", "unsupported_content", 200},
		{"size", "text/plain", "", strings.Repeat("a", MaxBodyBytes+1), "response_too_large", 200},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f, _ := testFetcher(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", test.contentType)
				if test.encoding != "" {
					w.Header().Set("Content-Encoding", test.encoding)
				}
				w.WriteHeader(test.status)
				w.Write([]byte(test.body))
			})
			_, err := f.Read(context.Background(), Request{URL: "https://example.com"}, nil)
			code(t, err, test.want)
		})
	}
	f, _ := testFetcher(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		io.WriteString(w, "ok")
	})
	f.roots = nil
	_, err := f.Read(context.Background(), Request{URL: "https://example.com"}, nil)
	code(t, err, "tls_failed")
}
func TestFetchTimeoutCancellationAndBusy(t *testing.T) {
	for _, bodySlow := range []bool{false, true} {
		t.Run(map[bool]string{false: "header", true: "body"}[bodySlow], func(t *testing.T) {
			f, _ := testFetcher(t, func(w http.ResponseWriter, r *http.Request) {
				if bodySlow {
					w.Header().Set("Content-Type", "text/plain")
					w.(http.Flusher).Flush()
				}
				<-r.Context().Done()
			})
			f.timeout = 30 * time.Millisecond
			_, err := f.Read(context.Background(), Request{URL: "https://example.com"}, nil)
			code(t, err, "timeout")
		})
	}
	f, _ := testFetcher(t, func(w http.ResponseWriter, r *http.Request) { t.Error("cancelled request sent") })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := f.Read(ctx, Request{URL: "https://example.com"}, nil)
	code(t, err, "cancelled")
	entered := make(chan struct{}, 4)
	release := make(chan struct{})
	busyFetcher, _ := testFetcher(t, func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		io.WriteString(w, "ok")
	})
	results := make(chan error, 4)
	for i := 0; i < 4; i++ {
		go func() {
			_, err := busyFetcher.Read(context.Background(), Request{URL: "https://example.com"}, nil)
			results <- err
		}()
	}
	for i := 0; i < 4; i++ {
		select {
		case <-entered:
		case <-time.After(time.Second):
			close(release)
			t.Fatal("concurrent requests not started")
		}
	}
	_, err = busyFetcher.Read(context.Background(), Request{URL: "https://example.com"}, nil)
	code(t, err, "busy")
	close(release)
	for i := 0; i < 4; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
}
func TestExtractTextLimitAndNoExternalResources(t *testing.T) {
	for _, media := range []string{"text/plain", "text/html"} {
		body := strings.Repeat("日", MaxTextBytes)
		if media == "text/html" {
			body = "<p>" + body + "</p><img src=https://private.invalid/a>"
		}
		text, _, truncated, err := extract(context.Background(), []byte(body), media)
		if err != nil || !truncated || len(text) > MaxTextBytes || !utf8.ValidString(text) {
			t.Fatal(media, len(text), truncated, err)
		}
	}
}

func TestExtractPreservesPreformattedCode(t *testing.T) {
	body := []byte("<p>Example:</p><pre><code>def answer():\n    if True:\n        return 42\n</code></pre><p>After   code.</p>")
	text, _, truncated, err := extract(context.Background(), body, "text/html")
	if err != nil || truncated || !strings.Contains(text, "def answer():\n    if True:\n        return 42\n") || !strings.HasSuffix(text, "After code.") {
		t.Fatal(text, truncated, err)
	}
}

func TestFetchLiteralDestinationsAndOversizedHeaders(t *testing.T) {
	f, dials := testFetcher(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Large", strings.Repeat("x", 40<<10))
		w.Header().Set("Content-Type", "text/plain")
		io.WriteString(w, "text")
	})
	for _, address := range []string{"https://127.0.0.1/", "https://[::1]/", "https://[::ffff:127.0.0.1]/", "https://169.254.169.254/"} {
		_, err := f.Read(context.Background(), Request{URL: address}, nil)
		code(t, err, "destination_not_public")
	}
	if dials.Load() != 0 {
		t.Fatal("literal private destination dialed")
	}
	_, err := f.Read(context.Background(), Request{URL: "https://example.com"}, nil)
	code(t, err, "fetch_failed")
}
