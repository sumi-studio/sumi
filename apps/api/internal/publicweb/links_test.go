package publicweb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
)

func TestHTMLLinksStayNavigableWithoutFetchingTargets(t *testing.T) {
	f, calls := testFetcher(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, `<head><base href="https://docs.example.org/guides/"><a href="/hidden">hidden</a></head><body><p><a href="intro#one">Read <strong>this</strong></a> then <a href="intro#one">again</a>.</p><a href="//example.com/other">Elsewhere</a><a href="javascript:alert(1)">bad</a><a href="https://user:pass@example.com/">private</a><template><a href="/ignored">ignore</a></template></body>`)
	})
	got, err := f.Read(context.Background(), Request{URL: "https://example.com/page"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 || len(got.Links) != 2 || got.Links[0].ID != 1 || got.Links[0].URL != "https://docs.example.org/guides/intro#one" || got.Links[1].URL != "https://example.com/other" || got.LinksTruncated {
		t.Fatalf("%+v calls=%d", got, calls.Load())
	}
	if !strings.Contains(got.Text, "[1] Read this") || !strings.Contains(got.Text, "[1] again") || strings.Contains(got.Text, "ignore") {
		t.Fatal(got.Text)
	}
}

func TestLinkCountIsBoundedAndTruncationExplicit(t *testing.T) {
	base, _ := parseURL("https://example.com/")
	links := &linkCollector{base: base, ids: map[string]int{}, links: []Link{}}
	var body strings.Builder
	for i := 0; i < 105; i++ {
		fmt.Fprintf(&body, `<a href="/%d">link</a> `, i)
	}
	text, _, _, err := extractDocument(context.Background(), []byte(body.String()), "text/html", links)
	if err != nil || len(links.links) != 100 || !links.truncated || !strings.Contains(text, "[100]") || strings.Contains(text, "[101]") {
		t.Fatalf("links=%d truncated=%v err=%v", len(links.links), links.truncated, err)
	}
}

func TestFailuresDistinguishEvidenceFromGuesses(t *testing.T) {
	for _, test := range []struct {
		name, media, encoding, body, challenge, code, reason string
		status                                               int
	}{
		{name: "generic denied", status: 403, code: "http_status"},
		{name: "explicit challenge", status: 403, challenge: "challenge", code: "access_challenge"},
		{name: "challenge with successful status", status: 200, challenge: "challenge", code: "access_challenge"},
		{name: "compression", status: 200, encoding: "gzip", code: "unsupported_content", reason: "content_encoding"},
		{name: "pdf", status: 200, media: "application/pdf", code: "unsupported_content", reason: "media_type"},
		{name: "charset", status: 200, media: "text/plain; charset=iso-8859-1", code: "unsupported_content", reason: "charset"},
		{name: "utf8", status: 200, media: "text/plain", body: string([]byte{255}), code: "unsupported_content", reason: "invalid_utf8"},
		{name: "meta charset", status: 200, media: "text/html", body: `<meta charset="iso-8859-1"><p>text</p>`, code: "unsupported_content", reason: "charset"},
	} {
		t.Run(test.name, func(t *testing.T) {
			f, _ := testFetcher(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", test.media)
				w.Header().Set("Content-Encoding", test.encoding)
				w.Header().Set("Cf-Mitigated", test.challenge)
				w.WriteHeader(test.status)
				fmt.Fprint(w, test.body)
			})
			_, err := f.Read(context.Background(), Request{URL: "https://example.com/"}, nil)
			var failure *Failure
			if !errors.As(err, &failure) || failure.Code != test.code || failure.Reason != test.reason {
				t.Fatalf("got %+v want %s/%s", err, test.code, test.reason)
			}
			if test.challenge != "" && failure.StatusCode != test.status {
				t.Fatal("lost challenge status")
			}
		})
	}
}

func TestExtendedContractFixtures(t *testing.T) {
	raw, err := os.ReadFile("testdata/contract.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture map[string]json.RawMessage
	if err = json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	var page Result
	if err = json.Unmarshal(fixture["linked_success"], &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Links) != 1 || page.Links[0].ID != 1 || page.Links[0].URL != "https://example.com/guide#section" {
		t.Fatalf("%+v", page)
	}
	for _, key := range []string{"content_failure", "challenge_failure"} {
		var failure Failure
		if err = json.Unmarshal(fixture[key], &failure); err != nil {
			t.Fatal(err)
		}
		if key == "content_failure" && failure.Reason != "charset" {
			t.Fatal(failure)
		}
		if key == "challenge_failure" && (failure.Code != "access_challenge" || failure.StatusCode != 403) {
			t.Fatal(failure)
		}
	}
}

func TestLinkURLBytesBoundAndInvalidSchemes(t *testing.T) {
	base, _ := parseURL("https://example.com/")
	links := &linkCollector{base: base, ids: map[string]int{}, links: []Link{}}
	for _, raw := range []string{"http://example.com/", "mailto:a@example.com", "https://user@example.com/", "https://example.com/%zz"} {
		if links.add(raw) != 0 {
			t.Fatalf("accepted %q", raw)
		}
	}
	for i := 0; i < 10; i++ {
		links.add(fmt.Sprintf("/%d%s", i, strings.Repeat("x", 8000)))
	}
	if links.bytes > 32<<10 || !links.truncated || len(links.links) != 4 {
		t.Fatalf("%+v", links)
	}
	if id := links.add(links.links[0].URL); id != 1 {
		t.Fatal("duplicate lost its reference")
	}
}

func TestNonHTTPSBaseDoesNotInventHTTPSLinks(t *testing.T) {
	for _, baseValue := range []string{"http://elsewhere.example/", "ftp://elsewhere.example/"} {
		base, _ := parseURL("https://example.com/")
		links := &linkCollector{base: base, ids: map[string]int{}, links: []Link{}}
		_, _, _, err := extractDocument(context.Background(), []byte(`<head><base href="`+baseValue+`"></head><body><a href="relative">not supported</a><a href="https://example.com/actual">supported</a></body>`), "text/html", links)
		if err != nil || len(links.links) != 1 || links.links[0].URL != "https://example.com/actual" {
			t.Fatalf("base=%s links=%+v err=%v", baseValue, links.links, err)
		}
	}
}

func TestJSONResponseBoundIncludesWorstCaseEscaping(t *testing.T) {
	const prefix = "https://example.com/?"
	source := prefix + strings.Repeat("&", MaxURLBytes-len(prefix))
	if _, err := parseURL(source); err != nil {
		t.Fatal(err)
	}
	title := strings.Repeat("&", maxTitleBytes)
	page := Result{RequestedURL: source, FetchedURL: source, Title: &title, Text: strings.Repeat("\x01", MaxTextBytes), BodySHA256: strings.Repeat("a", 64), Links: []Link{}}
	for i := 0; i < 4; i++ {
		page.Links = append(page.Links, Link{ID: i + 1, URL: source[:len(source)-1] + fmt.Sprint(i)})
	}
	// Go's actual Encoder escapes &, < and > as well as control bytes.
	var encoded strings.Builder
	if err := json.NewEncoder(&encoded).Encode(page); err != nil {
		t.Fatal(err)
	}
	if encoded.Len() <= 1<<20 || encoded.Len() > MaxResponseBytes {
		t.Fatalf("encoded=%d limit=%d", encoded.Len(), MaxResponseBytes)
	}
	raw, err := os.ReadFile("testdata/contract.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		MaxResponseBytes int `json:"max_response_bytes"`
	}
	if err = json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.MaxResponseBytes != MaxResponseBytes {
		t.Fatal("Rust/Go bound fixture drift")
	}
}
