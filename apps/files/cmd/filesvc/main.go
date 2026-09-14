// Command filesvc serves the scoped file API over one canonical namespace
// root — a JuiceFS mount in cloud placement, a plain directory locally.
//
// Env:
//
//	FILESV_LISTEN         listen address (default 127.0.0.1:8780)
//	FILESV_ROOT           namespace root directory (required)
//	FILESV_DB_URL         postgres DSN for version/event state (required)
//	FILESV_TOKENS         "tok1:scopeA,scopeB;tok2:*" scope grants (required)
//	FILESV_REQUIRE_MOUNT  "1" = refuse file ops while root is not a mountpoint
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/sumi-studio/sumi/apps/files/internal/filesvc"
)

func main() {
	listen := envOr("FILESV_LISTEN", "127.0.0.1:8780")
	root := os.Getenv("FILESV_ROOT")
	dsn := os.Getenv("FILESV_DB_URL")
	tokens := parseTokens(os.Getenv("FILESV_TOKENS"))
	if root == "" || dsn == "" || len(tokens) == 0 {
		log.Fatal("filesvc: FILESV_ROOT, FILESV_DB_URL and FILESV_TOKENS are required")
	}

	ctx := context.Background()
	store, err := filesvc.NewStore(ctx, dsn)
	if err != nil {
		log.Fatalf("filesvc: store: %v", err)
	}
	defer store.Close()

	svc, err := filesvc.NewAt(root, store, tokens)
	if err != nil {
		log.Fatalf("filesvc: root: %v", err)
	}
	if os.Getenv("FILESV_REQUIRE_MOUNT") == "1" {
		svc.RequireMount()
	}
	svc.StartReconciler(ctx)
	srv := &http.Server{
		Addr:              listen,
		Handler:           svc,
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Printf("filesvc: serving %s on %s", root, listen)
	log.Fatal(srv.ListenAndServe())
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func parseTokens(spec string) map[string]map[string]bool {
	out := map[string]map[string]bool{}
	for _, grant := range strings.Split(spec, ";") {
		grant = strings.TrimSpace(grant)
		if grant == "" {
			continue
		}
		tok, scopes, ok := strings.Cut(grant, ":")
		if !ok {
			continue
		}
		set := map[string]bool{}
		for _, s := range strings.Split(scopes, ",") {
			if s = strings.TrimSpace(s); s != "" {
				set[s] = true
			}
		}
		out[tok] = set
	}
	return out
}
