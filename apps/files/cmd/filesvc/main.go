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
//
// Private immutable-capture seam (META_URL + OBJ_KIND + the kind's
// identity config required together; absent config leaves capture
// endpoints refusing with capture_unconfigured — there is no live-tree
// fallback):
//
//	FILESV_CAPTURE_META_URL      postgres DSN of the JuiceFS metadata engine
//	FILESV_CAPTURE_OBJ_KIND      object backend kind: "file" or "s3"
//	FILESV_CAPTURE_OBJ_ROOT      expected object root for kind=file (Bucket+Name)
//	FILESV_CAPTURE_S3_ENDPOINT   scheme://host[:port] for kind=s3 (minio/s3)
//	FILESV_CAPTURE_S3_ACCESS_KEY s3 access key (private; never logged)
//	FILESV_CAPTURE_S3_SECRET_KEY s3 secret key (private; never logged)
//	FILESV_CAPTURE_S3_BUCKET     expected bucket the captured format must name
//	FILESV_CAPTURE_S3_ALIASES    optional CSV of trusted endpoint aliases
//	FILESV_CAPTURE_S3_PREFIX     expected object prefix in the bucket ("name/")
//	FILESV_CAPTURE_S3_REGION     SigV4 region (default us-east-1)
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"path/filepath"
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

	// The canonical root path is the storage identity the database binds
	// to: one DB serves one root, and the advisory writer lock makes this
	// process the only live writer on that DB. A second filesvc on the
	// same DB+root waits briefly for a rolling handoff, then refuses;
	// a different root on the same DB is rejected outright.
	rootID, err := filepath.EvalSymlinks(root)
	if err != nil {
		log.Fatalf("filesvc: resolve root: %v", err)
	}
	if rootID, err = filepath.Abs(rootID); err != nil {
		log.Fatalf("filesvc: resolve root: %v", err)
	}

	ctx := context.Background()
	store, err := filesvc.NewStore(ctx, dsn, rootID)
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
	// Capture is opt-in: any capture env set means the operator intends
	// capture — a partial config is a startup failure, not a quietly
	// degraded service.
	if captureEnvSet() {
		cfg := filesvc.CaptureConfig{
			MetaDSN: os.Getenv("FILESV_CAPTURE_META_URL"),
			ObjKind: os.Getenv("FILESV_CAPTURE_OBJ_KIND"),
			ObjRoot: os.Getenv("FILESV_CAPTURE_OBJ_ROOT"),
		}
		if cfg.ObjKind == "s3" {
			cfg.S3 = &filesvc.S3Config{
				Endpoint:  os.Getenv("FILESV_CAPTURE_S3_ENDPOINT"),
				AccessKey: os.Getenv("FILESV_CAPTURE_S3_ACCESS_KEY"),
				SecretKey: os.Getenv("FILESV_CAPTURE_S3_SECRET_KEY"),
				Bucket:    os.Getenv("FILESV_CAPTURE_S3_BUCKET"),
				Prefix:    os.Getenv("FILESV_CAPTURE_S3_PREFIX"),
				Region:    os.Getenv("FILESV_CAPTURE_S3_REGION"),
				Aliases:   splitCSV(os.Getenv("FILESV_CAPTURE_S3_ALIASES")),
			}
		}
		cs, err := filesvc.NewCaptureService(ctx, cfg, store)
		if err != nil {
			log.Fatalf("filesvc: capture: %v", err)
		}
		defer cs.Close()
		svc.SetCapture(cs)
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

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

// captureEnvSet reports whether any capture env is present — partial
// config is a startup failure inside NewCaptureService.
func captureEnvSet() bool {
	for _, k := range []string{
		"FILESV_CAPTURE_META_URL", "FILESV_CAPTURE_OBJ_KIND",
		"FILESV_CAPTURE_OBJ_ROOT", "FILESV_CAPTURE_S3_ENDPOINT",
		"FILESV_CAPTURE_S3_ACCESS_KEY", "FILESV_CAPTURE_S3_SECRET_KEY",
		"FILESV_CAPTURE_S3_BUCKET", "FILESV_CAPTURE_S3_PREFIX",
	} {
		if os.Getenv(k) != "" {
			return true
		}
	}
	return false
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
