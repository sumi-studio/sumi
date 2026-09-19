package filesvc

// capture_http.go — the private capture seam on the storage service.
//
// Every capture endpoint requires the internal wildcard credential —
// scope tokens never gain capture authority. Every request also
// carries the caller's return lineage (owner, epoch), derived by the
// API from the authenticated return session; filesvc verifies it
// against the durable file_freeze barrier on each boundary:
//
//	POST   /v1/files/{scope}/capture?owner=&epoch=[&expected_scope_id=]
//	GET    /v1/capture/{id}?owner=&epoch=               meta
//	GET    /v1/capture/{id}/entries?owner=&epoch=&cursor=&limit=
//	GET    /v1/capture/{id}/read?owner=&epoch=&seq=&offset=&len=
//	DELETE /v1/capture/{id}?owner=&epoch=               release
//
// Manifest rows carry raw names as base64 (name_b64 / path_b64) so
// delimiter/newline/non-UTF8 names survive unambiguously; a utf-8 `name`
// is included only as a convenience when the bytes are valid UTF-8.
// Slice IDs, object keys, inodes and credentials never leave the
// service: reads are addressed by (capture, entry seq) only, and the
// hardlink signal is the opaque link_group field.

import (
	"encoding/base64"
	"errors"
	"net/http"
	"strconv"
	"unicode/utf8"
)

// routeCapture dispatches /v1/capture/... requests. parts is the path
// after ["v1","capture"]. Called from ServeHTTP.
func (s *Service) routeCapture(w http.ResponseWriter, r *http.Request, parts []string) {
	if s.cap == nil {
		writeErr(w, 503, "capture_unconfigured",
			"capture service is not configured on this deployment")
		return
	}
	if !s.authorizedWildcard(r) {
		writeErr(w, 403, "forbidden", "capture requires the administrative credential")
		return
	}
	owner, epoch, ok := captureLineage(w, r)
	if !ok {
		return
	}
	if len(parts) < 1 || parts[0] == "" {
		writeErr(w, 404, "not_found", "unknown route")
		return
	}
	id := parts[0]
	op := ""
	if len(parts) == 2 {
		op = parts[1]
	} else if len(parts) != 1 {
		writeErr(w, 404, "not_found", "unknown route")
		return
	}
	switch {
	case op == "" && r.Method == "GET":
		s.handleCaptureMeta(w, r, id, owner, epoch)
	case op == "entries" && r.Method == "GET":
		s.handleCaptureEntries(w, r, id, owner, epoch)
	case op == "read" && r.Method == "GET":
		s.handleCaptureRead(w, r, id, owner, epoch)
	case op == "" && r.Method == "DELETE":
		s.handleCaptureRelease(w, r, id, owner, epoch)
	default:
		writeErr(w, 404, "not_found", "unknown op or method")
	}
}

// captureLineage parses the mandatory owner/epoch pair every capture
// request carries. The API derives both from the authenticated return
// session; absent or non-positive values are refused, never defaulted.
func captureLineage(w http.ResponseWriter, r *http.Request) (string, int64, bool) {
	q := r.URL.Query()
	owner := q.Get("owner")
	if owner == "" || len(owner) > 80 {
		writeErr(w, 400, "bad_owner", "non-empty owner is required (<=80 bytes)")
		return "", 0, false
	}
	v, err := strconv.ParseInt(q.Get("epoch"), 10, 64)
	if err != nil || v <= 0 {
		writeErr(w, 400, "bad_epoch", "positive integer epoch is required")
		return "", 0, false
	}
	return owner, v, true
}

func (s *Service) mapCaptureErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrCaptureStale):
		writeErr(w, 403, "capture_stale",
			"lineage is not the scope's current authority")
	case errors.Is(err, ErrCapturePending):
		writeErr(w, 503, "capture_pending",
			"captured objects unavailable; retake a fresh coherent manifest")
	case errors.Is(err, ErrCaptureRefused):
		writeErr(w, 422, "capture_refused", err.Error())
	case errors.Is(err, ErrCaptureNotFound):
		writeErr(w, 404, "capture_not_found", "unknown capture or manifest row")
	case errors.Is(err, ErrCaptureGone):
		writeErr(w, 410, "capture_gone", "capture released or expired")
	default:
		writeErr(w, 503, "capture_error", err.Error())
	}
}

// handleCaptureCreate serves POST /v1/files/{scope}/capture.
// expected_scope_id (required on retake, empty on first capture) is
// validated inside the capture transaction against the resolved anchor.
func (s *Service) handleCaptureCreate(w http.ResponseWriter, r *http.Request, scope string) {
	if s.cap == nil {
		writeErr(w, 503, "capture_unconfigured",
			"capture service is not configured on this deployment")
		return
	}
	if !s.authorizedWildcard(r) {
		writeErr(w, 403, "forbidden", "capture requires the administrative credential")
		return
	}
	owner, epoch, ok := captureLineage(w, r)
	if !ok {
		return
	}
	row, err := s.cap.Capture(r.Context(), scope, owner, epoch,
		r.URL.Query().Get("expected_scope_id"))
	if err != nil {
		s.mapCaptureErr(w, err)
		return
	}
	writeJSON(w, captureMetaJSON(row))
}

func captureMetaJSON(c *captureRow) map[string]any {
	return map[string]any{
		"capture_id":   c.CaptureID,
		"scope":        c.Scope,
		"scope_id":     c.ScopeID,
		"owner":        c.Owner,
		"owner_epoch":  c.OwnerEpoch,
		"volume":       c.Volume,
		"manifest_sha": c.ManifestSHA,
		"status":       c.Status,
		"entries":      c.EntryCount,
		"unsupported":  c.Unsupported,
		"created_at":   c.CreatedAt,
		"expires_at":   c.ExpiresAt,
		"format": map[string]any{
			"meta_version": c.Format.MetaVersion,
			"block_bytes":  c.Format.BlockBytes,
			"hash_prefix":  c.Format.HashPrefix,
			"trash_days":   c.Format.TrashDays,
			"storage":      c.Format.Storage,
		},
	}
}

func (s *Service) handleCaptureMeta(w http.ResponseWriter, r *http.Request, id, owner string, epoch int64) {
	row, err := s.cap.Meta(r.Context(), id, owner, epoch)
	if err != nil {
		s.mapCaptureErr(w, err)
		return
	}
	writeJSON(w, captureMetaJSON(row))
}

// captureEntryJSON renders one manifest row. Byte names are always
// base64; `name`/`path` utf-8 fields are convenience only.
func captureEntryJSON(e captureEntryRow) map[string]any {
	m := map[string]any{
		"seq":       e.Seq,
		"path_b64":  base64.StdEncoding.EncodeToString(e.Path),
		"name_b64":  base64.StdEncoding.EncodeToString(e.Name),
		"type":      nodeTypeName(e.NodeType),
		"supported": e.Supported,
		"mode":      e.Mode,
		"uid":       e.UID,
		"gid":       e.GID,
		"nlink":     e.Nlink,
		"length":    e.Length,
		"mtime_ns":  e.MtimeNS,
		"ctime_ns":  e.CtimeNS,
	}
	if utf8.Valid(e.Path) {
		m["path"] = string(e.Path)
	}
	if utf8.Valid(e.Name) {
		m["name"] = string(e.Name)
	}
	if e.NodeType == jfsTypeSymlink {
		m["link_b64"] = base64.StdEncoding.EncodeToString(e.Link)
		if utf8.Valid(e.Link) {
			m["link"] = string(e.Link)
		}
	}
	if e.LinkGroup != "" {
		m["link_group"] = e.LinkGroup
	}
	if e.MapSHA != "" {
		m["map_sha"] = e.MapSHA
	}
	return m
}

func nodeTypeName(t uint8) string {
	switch t {
	case jfsTypeFile:
		return "file"
	case jfsTypeDir:
		return "dir"
	case jfsTypeSymlink:
		return "symlink"
	case jfsTypeFIFO:
		return "fifo"
	case jfsTypeBlockDev:
		return "blockdev"
	case jfsTypeCharDev:
		return "chardev"
	case jfsTypeSocket:
		return "socket"
	default:
		return "unknown"
	}
}

func (s *Service) handleCaptureEntries(w http.ResponseWriter, r *http.Request, id, owner string, epoch int64) {
	q := r.URL.Query()
	// cursor -1 = from the beginning (seq 0 is the scope anchor row).
	var cursor, limit int64 = -1, 500
	if v := q.Get("cursor"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < -1 {
			writeErr(w, 400, "bad_cursor", "cursor must be an integer >= -1")
			return
		}
		cursor = n
	}
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 && n <= 1000 {
			limit = n
		}
	}
	rows, err := s.cap.Entries(r.Context(), id, owner, epoch, cursor, limit)
	if err != nil {
		s.mapCaptureErr(w, err)
		return
	}
	out := make([]any, 0, len(rows))
	var next int64
	for _, e := range rows {
		out = append(out, captureEntryJSON(e))
		next = e.Seq
	}
	resp := map[string]any{"entries": out, "has_more": int64(len(rows)) == limit}
	if len(rows) > 0 {
		resp["next_cursor"] = next
	}
	writeJSON(w, resp)
}

// handleCaptureRead streams the captured bytes of one manifest row.
// Content-Length is known up front from the manifest; a mid-stream
// object failure after headers can only truncate — the client must
// treat a short body as pending, matching handleRead's existing rule.
func (s *Service) handleCaptureRead(w http.ResponseWriter, r *http.Request, id, owner string, epoch int64) {
	q := r.URL.Query()
	seq, err := strconv.ParseInt(q.Get("seq"), 10, 64)
	if err != nil || seq < 0 {
		writeErr(w, 400, "bad_seq", "seq is required")
		return
	}
	var off, n uint64
	if v := q.Get("offset"); v != "" {
		u, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			writeErr(w, 400, "bad_range", "offset must be a non-negative integer")
			return
		}
		off = u
	}
	if v := q.Get("len"); v != "" {
		u, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			writeErr(w, 400, "bad_range", "len must be a non-negative integer")
			return
		}
		n = u
	}
	// Preflight the entry so a refused/pending verdict precedes headers.
	// Streaming a bounded probe prefix would re-resolve the same
	// manifest rows; a header probe adds nothing — object availability
	// is checked block-by-block during the stream and reported as
	// 503 capture_pending when it fails before the first byte.
	nr, err := s.cap.streamProbe(r.Context(), id, owner, epoch, seq)
	if err != nil {
		s.mapCaptureErr(w, err)
		return
	}
	end := nr
	if off >= end {
		w.Header().Set("content-type", "application/octet-stream")
		w.Header().Set("content-length", "0")
		return
	}
	want := end - off
	if n > 0 && n < want {
		want = n
	}
	// The first-block availability check is folded into Stream: write
	// the stream through a header-guarded writer so an early pending
	// error still maps to 503 rather than a truncated 200.
	gw := &guardWriter{w: w, n: int64(want), off: int64(off)}
	got, err := s.cap.Stream(r.Context(), id, owner, epoch, seq, off, want, gw)
	if err != nil {
		if gw.wrote {
			return // headers sent; truncation is the signal
		}
		s.mapCaptureErr(w, err)
		return
	}
	if !gw.wrote {
		w.Header().Set("content-type", "application/octet-stream")
		w.Header().Set("content-length", "0")
	}
	_ = got
}

// guardWriter delays committing headers until the first byte lands, so
// an object-store failure before byte 0 can still answer 503 pending.
type guardWriter struct {
	w     http.ResponseWriter
	n     int64
	off   int64
	wrote bool
}

func (g *guardWriter) Write(p []byte) (int, error) {
	if !g.wrote {
		g.w.Header().Set("content-type", "application/octet-stream")
		g.w.Header().Set("content-length", strconv.FormatInt(g.n, 10))
		g.wrote = true
	}
	return g.w.Write(p)
}

func (s *Service) handleCaptureRelease(w http.ResponseWriter, r *http.Request, id, owner string, epoch int64) {
	if err := s.cap.Release(r.Context(), id, owner, epoch); err != nil {
		s.mapCaptureErr(w, err)
		return
	}
	writeJSON(w, map[string]any{"capture_id": id, "status": "released"})
}
