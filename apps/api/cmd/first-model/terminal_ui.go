package main

import (
	"net/http"
	"os"
	"path/filepath"
)

// The Local pack carries this existing shared Web renderer as static assets.
// The entry has no embedded capability: it bootstraps with the current fm
// capability already held by the same-origin Local chat/files page.
func registerLocalTerminalUI(mux *http.ServeMux, directory string) {
	var files http.Handler
	if filepath.IsAbs(directory) {
		if info, err := os.Stat(filepath.Join(directory, "index.html")); err == nil && !info.IsDir() {
			files = http.StripPrefix("/local-terminal/", http.FileServer(http.Dir(directory)))
		}
	}
	mux.HandleFunc("GET /local-terminal/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; connect-src 'self' ws:; script-src 'self'; style-src 'self' 'unsafe-inline'; font-src 'self' data:; img-src 'self' data:; frame-ancestors 'none'; base-uri 'none'")
		if files == nil {
			http.Error(w, "Local terminal assets are unavailable; reinstall the current Local pack", http.StatusServiceUnavailable)
			return
		}
		files.ServeHTTP(w, r)
	})
}
