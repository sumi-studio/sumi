package filesvc

// capture_http.go — the private capture seam on the storage service.
//
// Routes (all under the existing bearer-token auth):
//
//	POST   /v1/files/{scope}/capture?owner=&epoch=   wildcard: create
//	GET    /v1/capture/{id}                          scope|wildcard: meta
//	GET    /v1/capture/{id}/entries?cursor=&limit=   scope|wildcard: rows
//	GET    /v1/capture/{id}/read?seq=&offset=&len=   scope|wildcard: bytes
//	DELETE /v1/capture/{id}                          wildcard: release
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
		s.handleCaptureMeta(w, r, id)
	case op == "entries" && r.Method == "GET":
		s.handleCaptureEntries(w, r, id)
	case op == "read" && r.Method == "GET":
		s.handleCaptureRead(w, r, id)
	case op == "" && r.Method == "DELETE":
		s.handleCaptureRelease(w, r, id)
	default:
		writeErr(w, 404, "not_found", "unknown op or method")
	}
}

func (s *Service) mapCaptureErr(w http.ResponseWriter, err error) {
	switch {
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

// handleCaptureCreate serves POST /v1/files/{scope}/capture — wildcard
// administrative credential only, like freeze. owner/epoch carry the
// caller's return lineage; the service records them but does not verify
// them (that binding is returnsession's — see the integration contract).
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
	q := r.URL.Query()
	owner := q.Get("owner")
	if len(owner) > 80 {
		writeErr(w, 400, "bad_owner", "owner too long")
		return
	}
	var epoch int64
	if e := q.Get("epoch"); e != "" {
		v, err := strconv.ParseInt(e, 10, 64)
		if err != nil || v < 0 {
			writeErr(w, 400, "bad_epoch", "epoch must be a non-negative integer")
			return
		}
		epoch = v
	}
	row, err := s.cap.Capture(r.Context(), scope, owner, epoch)
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

func (s *Service) handleCaptureMeta(w http.ResponseWriter, r *http.Request, id string) {
	row, err := s.cap.Meta(r.Context(), id)
	if err != nil {
		s.mapCaptureErr(w, err)
		return
	}
	if !s.authorized(r, row.Scope) {
		writeErr(w, 403, "forbidden", "token does not grant this capture's scope")
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

func (s *Service) handleCaptureEntries(w http.ResponseWriter, r *http.Request, id string) {
	row, err := s.cap.Meta(r.Context(), id)
	if err != nil {
		s.mapCaptureErr(w, err)
		return
	}
	if !s.authorized(r, row.Scope) {
		writeErr(w, 403, "forbidden", "token does not grant this capture's scope")
		return
	}
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
	rows, err := s.cap.Entries(r.Context(), id, cursor, limit)
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
func (s *Service) handleCaptureRead(w http.ResponseWriter, r *http.Request, id string) {
	row, err := s.cap.Meta(r.Context(), id)
	if err != nil {
		s.mapCaptureErr(w, err)
		return
	}
	if !s.authorized(r, row.Scope) {
		writeErr(w, 403, "forbidden", "token does not grant this capture's scope")
		return
	}
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
	nr, err := s.cap.streamProbe(r.Context(), id, seq)
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
	got, err := s.cap.Stream(r.Context(), id, seq, off, want, gw)
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

func (s *Service) handleCaptureRelease(w http.ResponseWriter, r *http.Request, id string) {
	if _, err := s.cap.Meta(r.Context(), id); err != nil {
		s.mapCaptureErr(w, err)
		return
	}
	if !s.authorizedWildcard(r) {
		writeErr(w, 403, "forbidden", "release requires the administrative credential")
		return
	}
	if err := s.cap.Release(r.Context(), id); err != nil {
		s.mapCaptureErr(w, err)
		return
	}
	writeJSON(w, map[string]any{"capture_id": id, "status": "released"})
}
