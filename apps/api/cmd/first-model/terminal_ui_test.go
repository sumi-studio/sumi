package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLocalTerminalAssetsAreSameOriginAndCredentialFree(t *testing.T) {
	dir := t.TempDir()
	if e := os.WriteFile(filepath.Join(dir, "index.html"), []byte("<div id=root></div>"), 0600); e != nil {
		t.Fatal(e)
	}
	mux := http.NewServeMux()
	registerLocalTerminalUI(mux, dir)
	req := httptest.NewRequest("GET", "/local-terminal/", nil)
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, req)
	if recorder.Code != 200 || recorder.Body.String() != "<div id=root></div>" || recorder.Header().Get("Referrer-Policy") != "no-referrer" {
		t.Fatal(recorder)
	}
	if !strings.Contains(uiHTML, `href="/local-terminal/"`) || !strings.Contains(uiHTML, "history.replaceState") {
		t.Fatal("Local page does not expose credential-free entry or clear URL capability")
	}
	missing := http.NewServeMux()
	registerLocalTerminalUI(missing, "")
	recorder = httptest.NewRecorder()
	missing.ServeHTTP(recorder, req)
	if recorder.Code != 503 {
		t.Fatal("missing assets disguised as a working entry", recorder.Code)
	}
}
