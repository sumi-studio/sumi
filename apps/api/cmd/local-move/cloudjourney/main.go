// Command cloudjourney is a test fixture: it serves the REAL
// returnsession service, grant routes and scoped file proxy over real
// Postgres and a real filesvc, so packaged Local installs can run an
// actual Cloud-to-Local return journey end to end.
//
// The one replaced boundary is the owner-session adapter: identity
// comes from returnsessiontest's fixture cookie map (JOURNEY_OWNERS),
// never from a real auth provider. Everything else — session service,
// grant bearer checks, credential mint/lifecycle, file proxy, filesvc —
// is the production code path.
//
// Env:
//
//	JOURNEY_DSN          postgres DSN for the Cloud API state (required)
//	JOURNEY_LISTEN       listen address (default 127.0.0.1:8790)
//	JOURNEY_FILESVC_URL  real filesvc base URL (required)
//	JOURNEY_FILESVC_TOKEN internal (wildcard) filesvc token (required)
//	JOURNEY_OWNERS       "cookie=humanID:personaID;..." fixture sessions
//	JOURNEY_SEED         "humanID:personaID:name" — create human+persona
//	JOURNEY_MODES        "local,cloud" (default both) — explicit enable
//	JOURNEY_FAULT        optional fault endpoint enable ("1")
package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/fileaccess"
	"github.com/sumi-studio/sumi/apps/api/internal/returnsession"
	"github.com/sumi-studio/sumi/apps/api/internal/returnsession/returnsessiontest"
)

func main() {
	dsn := os.Getenv("JOURNEY_DSN")
	furl := os.Getenv("JOURNEY_FILESVC_URL")
	ftok := os.Getenv("JOURNEY_FILESVC_TOKEN")
	if dsn == "" || furl == "" || ftok == "" {
		log.Fatal("cloudjourney: JOURNEY_DSN, JOURNEY_FILESVC_URL and JOURNEY_FILESVC_TOKEN are required")
	}
	listen := os.Getenv("JOURNEY_LISTEN")
	if listen == "" {
		listen = "127.0.0.1:8790"
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("cloudjourney: pool: %v", err)
	}
	defer pool.Close()
	if err := db.Migrate(ctx, pool); err != nil {
		log.Fatalf("cloudjourney: migrate: %v", err)
	}
	if seed := os.Getenv("JOURNEY_SEED"); seed != "" {
		parts := strings.Split(seed, ":")
		if len(parts) != 3 {
			log.Fatalf("cloudjourney: JOURNEY_SEED wants humanID:personaID:name, got %q", seed)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO humans (human_id) VALUES ($1) ON CONFLICT DO NOTHING`, parts[0]); err != nil {
			log.Fatalf("cloudjourney: seed human: %v", err)
		}
		st := agentstate.NewStore(pool)
		if _, _, err := st.EnsurePersona(ctx, parts[1], nil, parts[2]); err != nil {
			log.Fatalf("cloudjourney: seed persona: %v", err)
		}
		if _, err := pool.Exec(ctx, `UPDATE core_personas SET human_id = $1 WHERE persona_id = $2`, parts[0], parts[1]); err != nil {
			log.Fatalf("cloudjourney: bind human: %v", err)
		}
	}
	proof := returnsessiontest.NewProof()
	for _, o := range strings.Split(os.Getenv("JOURNEY_OWNERS"), ";") {
		o = strings.TrimSpace(o)
		if o == "" {
			continue
		}
		kv := strings.SplitN(o, "=", 2)
		hp := strings.SplitN(kv[1], ":", 2)
		if len(kv) != 2 || len(hp) != 2 {
			log.Fatalf("cloudjourney: bad JOURNEY_OWNERS entry %q", o)
		}
		proof.Add(kv[0], hp[0], hp[1])
	}
	modes := []string{"local", "cloud"}
	if m := os.Getenv("JOURNEY_MODES"); m != "" {
		modes = strings.Split(m, ",")
	}
	svc := returnsession.New(pool, returnsession.Config{
		FilePolicy: returnsession.FilePolicySelectable,
		FileModes:  modes,
	})
	svc.SetLogger(func(f string, a ...any) { log.Printf("returnsession: "+f, a...) })
	files, err := fileaccess.NewClient(furl, ftok)
	if err != nil {
		log.Fatalf("cloudjourney: filesvc client: %v", err)
	}
	svc.SetFileStore(files)
	srv, err := returnsession.NewServer(svc, proof, "http://"+listen)
	if err != nil {
		log.Fatalf("cloudjourney: server: %v", err)
	}
	srv.SetFiles(files)
	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)
	srv.RegisterFileProxy(mux)
	var dropMint atomic.Bool
	var cutAfter atomic.Int32
	var cutKeep atomic.Int64
	cutAfter.Store(-1)
	if os.Getenv("JOURNEY_FAULT") == "1" {
		// Fault arm: the next files-credential mint commits server-side,
		// then the connection is hijacked and closed before the response
		// — a real lost-mint-response, not a mocked refusal.
		mux.HandleFunc("POST /_journey/arm-drop-mint", func(w http.ResponseWriter, r *http.Request) {
			dropMint.Store(true)
			w.WriteHeader(http.StatusNoContent)
		})
		// Fault arm: let <after> /files/read requests pass for real, then
		// answer the next with the real handler's status, headers and only
		// the first <keep> bytes of its body before the TCP connection is
		// closed — a genuine mid-copy truncation the mover must treat as
		// retryable transport loss, never as carried bytes.
		mux.HandleFunc("POST /_journey/arm-cut-read", func(w http.ResponseWriter, r *http.Request) {
			after, _ := strconv.Atoi(r.URL.Query().Get("after"))
			keep, _ := strconv.Atoi(r.URL.Query().Get("keep"))
			cutKeep.Store(int64(keep))
			cutAfter.Store(int32(after))
			w.WriteHeader(http.StatusNoContent)
		})
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if dropMint.Load() && r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/files-credential") {
			dropMint.Store(false)
			// Run the real handler against a sink so the mint commits
			// server-side, then close the connection without answering —
			// the credential exists but the response never arrives.
			mux.ServeHTTP(&sinkWriter{h: http.Header{}}, r)
			if hj, ok := w.(http.Hijacker); ok {
				if conn, _, err := hj.Hijack(); err == nil {
					conn.Close()
				}
			}
			return
		}
		if cutAfter.Load() >= 0 && r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/files/read") {
			if cutAfter.Add(-1) < 0 {
				cutAfter.Store(-1) // one shot — the fault is now removed
				// The real handler runs for real (real grant auth, real
				// fileaccess read, real filesvc bytes) into a capture;
				// only the delivered prefix is truncated.
				cw := &captureWriter{h: http.Header{}, code: http.StatusOK}
				mux.ServeHTTP(cw, r)
				for k, vs := range cw.h {
					for _, v := range vs {
						w.Header().Add(k, v)
					}
				}
				// Promise the full body, deliver only <keep> bytes, then
				// close the connection — unexpected EOF on the client.
				w.Header().Set("Content-Length", strconv.Itoa(len(cw.body)))
				w.WriteHeader(cw.code)
				keep := int(cutKeep.Load())
				if keep > len(cw.body) {
					keep = len(cw.body)
				}
				_, _ = w.Write(cw.body[:keep])
				if f, ok := w.(http.Flusher); ok {
					f.Flush()
				}
				if hj, ok := w.(http.Hijacker); ok {
					if conn, _, err := hj.Hijack(); err == nil {
						conn.Close()
					}
				}
				return
			}
		}
		mux.ServeHTTP(w, r)
	})
	httpSrv := &http.Server{Addr: listen, Handler: handler, ReadHeaderTimeout: 10 * time.Second}
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		log.Fatalf("cloudjourney: listen: %v", err)
	}
	log.Printf("cloudjourney: serving real return routes on %s (fixture owner adapter)", ln.Addr())
	log.Fatal(httpSrv.Serve(ln))
}

// sinkWriter discards the handler's response so the real handler can
// run (and commit) without answering the client.
type sinkWriter struct {
	h     http.Header
	wrote int
}

func (s *sinkWriter) Header() http.Header { return s.h }
func (s *sinkWriter) WriteHeader(c int)   { s.wrote = c }
func (s *sinkWriter) Write(p []byte) (int, error) {
	if s.wrote == 0 {
		s.wrote = http.StatusOK
	}
	return len(p), nil
}

// captureWriter buffers the real handler's status, headers and body so a
// fault can deliver only a chosen prefix of the genuine response.
type captureWriter struct {
	h    http.Header
	code int
	body []byte
}

func (c *captureWriter) Header() http.Header  { return c.h }
func (c *captureWriter) WriteHeader(code int) { c.code = code }
func (c *captureWriter) Write(p []byte) (int, error) {
	c.body = append(c.body, p...)
	return len(p), nil
}
