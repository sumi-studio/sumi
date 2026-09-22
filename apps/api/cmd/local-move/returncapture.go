// returncapture.go is the local-mode copy's immutable-capture client and
// staging plan. The mover never enumerates or reads the live workspace:
// it binds the session's durable capture (the service persists
// scope_id + capture_id + manifest_sha), pages the manifest, and streams
// captured rows by sequence number. Names and symlink targets arrive as
// authoritative base64 bytes — convenience UTF-8 fields are never used
// for filesystem work.
//
// A bound capture that loses a required object or expires is retaken as
// one coherent manifest: the old staging identity is abandoned, the
// journal is re-planned against the new rows, and placed files whose
// carried bytes are still at the destination are re-verified against the
// new manifest rather than trusted across the snapshot boundary.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/fileaccess"
)

// errCaptureLost means the bound capture can no longer serve the copy —
// a required object went missing (capture_pending), or the manifest was
// released/expired. The drive loop retakes and re-plans; it is not a
// transport retry of the same dead association.
var errCaptureLost = errors.New("the bound capture needs retaking")

// errCaptureScope means the scope's durable identity changed under the
// binding — the anchor was renamed out and recreated. The copy refuses:
// no new capture can name the same tree.
var errCaptureScope = errors.New("the Cloud workspace's scope identity changed during the copy")

// errCaptureReplaced means the persisted association moved to a
// different capture while the mover was planning or reading — the
// planned manifest is dead; the whole tree must be re-planned against
// the current binding before another byte is accepted.
var errCaptureReplaced = errors.New("the bound capture association changed")

// capBinding mirrors the session's persisted capture association.
type capBinding struct {
	ScopeID     string `json:"scope_id"`
	CaptureID   string `json:"capture_id"`
	ManifestSHA string `json:"manifest_sha"`
	Retaken     bool   `json:"retaken"`
}

// capRow is one manifest entry.
type capRow struct {
	Seq       int64  `json:"seq"`
	PathB64   string `json:"path_b64"`
	NameB64   string `json:"name_b64"`
	Type      string `json:"type"`
	Supported bool   `json:"supported"`
	Length    int64  `json:"length"`
	LinkB64   string `json:"link_b64"`
	LinkGroup string `json:"link_group"`
}

// capPage mirrors the entries response envelope.
type capPage struct {
	Entries    []capRow `json:"entries"`
	HasMore    bool     `json:"has_more"`
	NextCursor int64    `json:"next_cursor"`
}

// capErrBody is the error shape the proxy forwards upstream verbatim.
type capErrBody struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}

// captureCall issues one capture-route request and classifies the
// answer: 401/403 grant problems are permanent, capture_gone /
// capture_not_found / capture_pending mean the association is dead,
// the scope-change refusal is terminal, other 5xx/transport failures
// are retryable unreachability.
func (r *returner) captureCall(ctx context.Context, st *returnState, method, path string, body io.Reader) ([]byte, int, error) {
	rctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(rctx, method, st.SessionURL+path, body)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+st.Grant)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := r.m.client.Do(req)
	r.m.answered.Store(true)
	if err != nil {
		if ctx.Err() != nil {
			return nil, 0, ctx.Err()
		}
		return nil, 0, fmt.Errorf("%w: %v", errUnreachable, err)
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(res.Body, 32<<20))
	if err != nil {
		return nil, res.StatusCode, fmt.Errorf("%w: %v", errUnreachable, err)
	}
	if res.StatusCode < 300 {
		return raw, res.StatusCode, nil
	}
	var eb capErrBody
	_ = json.Unmarshal(raw, &eb)
	msg := strings.TrimSpace(eb.Error)
	if msg == "" {
		msg = fmt.Sprintf("HTTP %d", res.StatusCode)
	}
	switch {
	case res.StatusCode == http.StatusUnauthorized || res.StatusCode == http.StatusForbidden:
		return nil, res.StatusCode, fmt.Errorf("%w: %s", errGrantRejected, msg)
	case res.StatusCode == http.StatusGone || res.StatusCode == http.StatusNotFound,
		eb.Code == "capture_gone", eb.Code == "capture_not_found", eb.Code == "capture_pending":
		return nil, res.StatusCode, fmt.Errorf("%w: %s", errCaptureLost, msg)
	case res.StatusCode == http.StatusConflict &&
		strings.Contains(msg, "scope identity changed"):
		return nil, res.StatusCode, fmt.Errorf("%w: %s", errCaptureScope, msg)
	case res.StatusCode == http.StatusConflict && eb.Code == "capture_replaced":
		return nil, res.StatusCode, fmt.Errorf("%w: %s", errCaptureReplaced, msg)
	case res.StatusCode == http.StatusConflict:
		// A binding conflict the mover cannot name (e.g. the
		// association was released mid-copy) still means the plan it
		// holds is not the association — replan before more bytes.
		return nil, res.StatusCode, fmt.Errorf("%w: %s", errCaptureReplaced, msg)
	case res.StatusCode >= 500:
		return nil, res.StatusCode, fmt.Errorf("%w: %s", errUnreachable, msg)
	default:
		return nil, res.StatusCode, errors.New(msg)
	}
}

// captureEnsure binds (or re-derives) the session's capture association.
func (r *returner) captureEnsure(ctx context.Context, st *returnState) (*capBinding, error) {
	raw, _, err := r.captureCall(ctx, st, http.MethodPost, "/capture", strings.NewReader("{}"))
	if err != nil {
		return nil, err
	}
	var b capBinding
	if err := json.Unmarshal(raw, &b); err != nil {
		return nil, fmt.Errorf("Cloud answered an unreadable capture binding: %v", err)
	}
	return &b, nil
}

// captureRetake asks for a fresh coherent manifest under the same scope
// identity. expectedScopeID stays constant across normal retakes; the
// planned captureID lets the service recognise a retry whose response
// was lost — it answers the current binding instead of minting again.
// The response is the authoritative binding — retaken or not.
func (r *returner) captureRetake(ctx context.Context, st *returnState, expectedScopeID, expectedCaptureID string) (*capBinding, error) {
	raw, _, err := r.captureCall(ctx, st, http.MethodPost, "/capture/retake",
		strings.NewReader(fmt.Sprintf(`{"expected_scope_id":%q,"expected_capture_id":%q}`,
			expectedScopeID, expectedCaptureID)))
	if err != nil {
		return nil, err
	}
	var b capBinding
	if err := json.Unmarshal(raw, &b); err != nil {
		return nil, fmt.Errorf("Cloud answered an unreadable capture binding: %v", err)
	}
	return &b, nil
}

// captureRelease drops the association — best-effort at copy completion.
// The release names the binding it is cleaning up so a delayed answer
// can never clear a newer replacement.
func (r *returner) captureRelease(ctx context.Context, st *returnState, captureID string) {
	q := url.Values{"expected_capture_id": {captureID}}
	_, _, _ = r.captureCall(ctx, st, http.MethodDelete, "/capture?"+q.Encode(), nil)
}

// capturePageLimit bounds one manifest page; the env knob lets journeys
// force mid-page association changes without giant trees.
func capturePageLimit() int {
	if v, err := strconv.Atoi(os.Getenv("SUMI_LOCAL_MOVE_CAPTURE_PAGE")); err == nil && v > 0 {
		return v
	}
	return 1000
}

// captureEntries fetches one manifest page of the PLANNED capture —
// expected pins the association the mover's plan is built on, so a
// concurrent retake is refused before any foreign-manifest rows land.
func (r *returner) captureEntries(ctx context.Context, st *returnState, expected string, cursor int64) (*capPage, error) {
	q := url.Values{"cursor": {fmt.Sprint(cursor)}, "limit": {fmt.Sprint(capturePageLimit())},
		"expected_capture_id": {expected}}
	raw, _, err := r.captureCall(ctx, st, http.MethodGet, "/capture/entries?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	var p capPage
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("Cloud answered an unreadable manifest page: %v", err)
	}
	return &p, nil
}

// captureReadRow streams one manifest row into a staging blob (fsync +
// rename), returning the SHA-256 and byte count actually received. The
// count must equal the manifest length — a truncated or oversized body
// is never carried. Non-200 statuses classify through captureCall's
// rules: capture_pending/gone mean the association is dead.
func (r *returner) captureReadRow(ctx context.Context, st *returnState, seq int64, blob string, wantLen int64, expected string) (string, int64, error) {
	u := fmt.Sprintf("%s/capture/read?seq=%d&len=%d&expected_capture_id=%s",
		st.SessionURL, seq, wantLen, url.QueryEscape(expected))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return "", 0, err
	}
	req.Header.Set("Authorization", "Bearer "+st.Grant)
	res, err := r.m.client.Do(req)
	r.m.answered.Store(true)
	if err != nil {
		if ctx.Err() != nil {
			return "", 0, ctx.Err()
		}
		return "", 0, fmt.Errorf("%w: %v", errUnreachable, err)
	}
	defer res.Body.Close()
	body := &progress{r: res.Body}
	body.last.Store(time.Now().UnixNano())
	var stalled atomic.Bool
	watch := make(chan struct{})
	defer close(watch)
	go func() {
		tick := time.NewTicker(max(r.m.stall/4, 10*time.Millisecond))
		defer tick.Stop()
		for {
			select {
			case <-watch:
				return
			case <-ctx.Done():
				return
			case <-tick.C:
				if body.done.Load() {
					return
				}
				if time.Since(time.Unix(0, body.last.Load())) >= r.m.stall {
					stalled.Store(true)
					_ = res.Body.Close()
					return
				}
			}
		}
	}()
	if res.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(io.LimitReader(body, 1<<16))
		var eb capErrBody
		_ = json.Unmarshal(raw, &eb)
		msg := strings.TrimSpace(eb.Error)
		if msg == "" {
			msg = fmt.Sprintf("HTTP %d", res.StatusCode)
		}
		switch {
		case ctx.Err() != nil:
			return "", 0, ctx.Err()
		case res.StatusCode == http.StatusUnauthorized || res.StatusCode == http.StatusForbidden:
			return "", 0, fmt.Errorf("%w: %s", errGrantRejected, msg)
		case res.StatusCode == http.StatusGone || res.StatusCode == http.StatusNotFound ||
			eb.Code == "capture_gone" || eb.Code == "capture_not_found" || eb.Code == "capture_pending":
			return "", 0, fmt.Errorf("%w: %s", errCaptureLost, msg)
		case res.StatusCode == http.StatusConflict && eb.Code == "capture_replaced":
			return "", 0, fmt.Errorf("%w: %s", errCaptureReplaced, msg)
		case res.StatusCode == http.StatusConflict:
			return "", 0, fmt.Errorf("%w: %s", errCaptureReplaced, msg)
		default:
			return "", 0, fmt.Errorf("%w: %s", errUnreachable, msg)
		}
	}
	if err := os.MkdirAll(filepath.Dir(blob), 0o700); err != nil {
		return "", 0, err
	}
	tmp := blob + ".part"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return "", 0, err
	}
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(f, h), body)
	if err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		switch {
		case ctx.Err() != nil:
			return "", 0, ctx.Err()
		case stalled.Load():
			return "", 0, fmt.Errorf("%w: %v; resume retries the file", errUnreachable, errStalled)
		default:
			return "", 0, fmt.Errorf("%w: %v", errUnreachable, err)
		}
	}
	if n != wantLen {
		_ = f.Close()
		_ = os.Remove(tmp)
		// A short captured stream means the manifest can no longer serve
		// its objects — retake, never carry a partial file.
		return "", 0, fmt.Errorf("%w: captured row %d delivered %d of %d bytes", errCaptureLost, seq, n, wantLen)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return "", 0, err
	}
	if err := f.Close(); err != nil {
		return "", 0, err
	}
	if err := os.Rename(tmp, blob); err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

// --- manifest plan ----------------------------------------------------

// manifestPlan is the decoded, validated manifest: the complete
// workspace tree the copy must reproduce, keyed by raw byte paths.
type manifestPlan struct {
	files       map[string]*planFile // raw rel path -> file
	dirs        []string             // raw rel paths, ordered
	links       map[string][]byte    // raw rel path -> raw symlink target
	groups      map[string][]string  // link_group -> member paths
	unsupported []string
}

type planFile struct {
	seq    int64
	length int64
	group  string
}

// safeCarryPath validates one manifest path as a workspace-relative
// destination path: every segment a real name, never escaping the scope
// root. Raw bytes are preserved — the returned string IS the byte path.
func safeCarryPath(p []byte) (string, error) {
	if len(p) == 0 {
		return "", nil // the scope anchor row itself
	}
	if len(p) > 64<<10 {
		return "", fmt.Errorf("manifest path exceeds 64KiB")
	}
	if p[0] == '/' || strings.IndexByte(string(p), 0) >= 0 {
		return "", fmt.Errorf("manifest path %q is not workspace-relative", p)
	}
	for _, seg := range strings.Split(string(p), "/") {
		if seg == "" || seg == "." || seg == ".." {
			return "", fmt.Errorf("manifest path %q has an unsafe segment", p)
		}
	}
	return string(p), nil
}

// loadManifest pages the whole bound manifest into a validated plan.
// Unsupported entries are collected, not dropped — the caller fails
// visibly on any of them.
func (r *returner) loadManifest(ctx context.Context, st *returnState, expected string) (*manifestPlan, error) {
	pl := &manifestPlan{
		files: map[string]*planFile{}, links: map[string][]byte{},
		groups: map[string][]string{},
	}
	var cursor int64 = -1
	for {
		page, err := r.captureEntries(ctx, st, expected, cursor)
		if err != nil {
			return nil, err
		}
		for _, e := range page.Entries {
			rawPath, err := base64.StdEncoding.DecodeString(e.PathB64)
			if err != nil {
				return nil, fmt.Errorf("manifest row %d has an undecodable path: %v", e.Seq, err)
			}
			p, err := safeCarryPath(rawPath)
			if err != nil {
				return nil, err
			}
			if p == "" {
				continue // scope anchor row
			}
			if !e.Supported {
				pl.unsupported = append(pl.unsupported, fmt.Sprintf("%s (%s)", p, e.Type))
				continue
			}
			switch e.Type {
			case "dir":
				pl.dirs = append(pl.dirs, p)
			case "file":
				pl.files[p] = &planFile{seq: e.Seq, length: e.Length, group: e.LinkGroup}
				if e.LinkGroup != "" {
					pl.groups[e.LinkGroup] = append(pl.groups[e.LinkGroup], p)
				}
			case "symlink":
				target, err := base64.StdEncoding.DecodeString(e.LinkB64)
				if err != nil {
					return nil, fmt.Errorf("manifest row %d has an undecodable link target: %v", e.Seq, err)
				}
				if strings.IndexByte(string(target), 0) >= 0 {
					return nil, fmt.Errorf("manifest row %d link target contains NUL", e.Seq)
				}
				pl.links[p] = target
			default:
				pl.unsupported = append(pl.unsupported, fmt.Sprintf("%s (%s)", p, e.Type))
			}
			if len(pl.files)+len(pl.dirs)+len(pl.links)+len(pl.unsupported) > maxReturnFiles {
				return nil, fmt.Errorf("the Cloud workspace has more than %d entries — refusing to walk an unbounded tree", maxReturnFiles)
			}
		}
		if !page.HasMore {
			break
		}
		cursor = page.NextCursor
	}
	sort.Strings(pl.dirs)
	return pl, nil
}

// --- staging and retake -------------------------------------------------

// stageCapturedFiles binds the session's capture association and streams
// every manifest file row into verified staged blobs. On first run it
// builds the journal plan from the manifest; on resume it compares the
// journaled binding with the service's persisted association — a
// mismatch means a retake happened while this mover was away, so the
// plan is rebuilt rather than two manifests mixed.
func (r *returner) stageCapturedFiles(ctx context.Context, st *returnState, j *filesJournal) error {
	b, err := r.captureEnsure(ctx, st)
	if err != nil {
		return err
	}
	if j.CaptureID != b.CaptureID || j.ManifestSHA != b.ManifestSHA {
		if err := r.replanForBinding(ctx, st, j, b); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(filepath.Join(j.Staging, "blobs"), 0o700); err != nil {
		return err
	}
	var total, count int64
	paths := make([]string, 0, len(j.Files))
	for p := range j.Files {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	// Hardlink groups fetch once: the first member streamed becomes the
	// link source for the rest — staging keeps the linkage, so the
	// promoted tree keeps it too. The member inherits the leader's hash:
	// same inode, same bytes — an empty SHA would force a refetch whose
	// rename breaks the link.
	groupBlob := map[string]string{}
	groupSHA := map[string]string{}
	for _, p := range paths {
		je := j.Files[p]
		total += je.Bytes
		if je.State != "verified" {
			continue
		}
		blob := filepath.Join(j.Staging, "blobs", p)
		if je.SHA256 != "" && blobOK(blob, je) {
			if je.Group != "" && groupBlob[je.Group] == "" {
				groupBlob[je.Group] = blob
				groupSHA[je.Group] = je.SHA256
			}
			count++
			continue
		}
		if err := checkCapacity(r.m.wsRoot, total); err != nil {
			return err
		}
		if je.Group != "" && groupBlob[je.Group] != "" {
			// Same captured inode — link the leader's blob rather than
			// stream identical bytes again.
			if err := os.MkdirAll(filepath.Dir(blob), 0o700); err != nil {
				return err
			}
			_ = os.Remove(blob)
			if err := os.Link(groupBlob[je.Group], blob); err != nil {
				return err
			}
			je.SHA256 = groupSHA[je.Group]
			je.State = "verified"
			count++
			if err := r.saveJournal(st, j); err != nil {
				return err
			}
			continue
		}
		sha, n, err := r.captureReadRow(ctx, st, je.Seq, blob, je.Bytes, j.CaptureID)
		if err != nil {
			return err
		}
		je.SHA256 = sha
		je.Bytes = n
		je.State = "verified"
		count++
		if je.Group != "" && groupBlob[je.Group] == "" {
			groupBlob[je.Group] = blob
			groupSHA[je.Group] = sha
		}
		if err := r.saveJournal(st, j); err != nil {
			return err
		}
	}
	j.Phase = "staged"
	return r.saveJournal(st, j)
}

// stagingDirFor derives the staging identity for one capture — a retake
// moves the copy to a fresh directory so no old-manifest blob can leak
// into the new tree.
func stagingDirFor(wsRoot, sessionID, captureID string) string {
	sess8 := sessionID
	if len(sess8) > 8 {
		sess8 = sess8[:8]
	}
	cap8 := captureID
	if len(cap8) > 8 {
		cap8 = cap8[:8]
	}
	return filepath.Join(wsRoot, ".sumi-return-staging-"+sess8+"-"+cap8)
}

// replanForBinding rebuilds the journal's plan from the binding's
// manifest: new/changed paths become unplaced fetches, vanished paths
// have their copy-product destination quarantined (never deleted —
// an operator edit survives), and unchanged placed content is proven
// against the NEW manifest's bytes, not carried over on faith.
func (r *returner) replanForBinding(ctx context.Context, st *returnState, j *filesJournal, b *capBinding) error {
	if j.CaptureID != "" && j.CaptureID != b.CaptureID && j.Staging != "" {
		// Abandon the old staging identity entirely — its blobs belong
		// to a manifest this copy can no longer see.
		old := j.Staging
		j.Staging = stagingDirFor(r.m.wsRoot, st.SessionID, b.CaptureID)
		if old != j.Staging {
			if err := os.RemoveAll(old); err != nil {
				return fmt.Errorf("could not clear superseded staging %s: %v", old, err)
			}
		}
	}
	if j.Staging == "" {
		j.Staging = stagingDirFor(r.m.wsRoot, st.SessionID, b.CaptureID)
	}
	j.CaptureID, j.ScopeID, j.ManifestSHA = b.CaptureID, b.ScopeID, b.ManifestSHA
	pl, err := r.loadManifest(ctx, st, j.CaptureID)
	if err != nil {
		return err
	}
	if len(pl.unsupported) > 0 {
		sort.Strings(pl.unsupported)
		shown := pl.unsupported
		if len(shown) > 10 {
			shown = shown[:10]
		}
		return fmt.Errorf("the Cloud workspace contains entries this copy cannot carry (not regular files, directories or symlinks): %s%s — "+
			"the return is still sealed; move or remove them on Cloud and run `sumi-local-move return-resume`, "+
			"or `sumi-local-move return-cancel`",
			strings.Join(shown, ", "), moreSuffix(len(pl.unsupported), 10))
	}
	scope, err := fileaccess.ScopeForPersona(st.Persona)
	if err != nil {
		return err
	}
	scopeDir := filepath.Join(r.m.wsRoot, scope)
	// Files already in the journal: keep only what the new manifest
	// still carries; placed leftovers quarantine rather than leak.
	for p, je := range j.Files {
		pf, ok := pl.files[p]
		if !ok {
			if je.State == "placed" {
				if _, lerr := os.Lstat(filepath.Join(scopeDir, p)); lerr == nil {
					if err := r.moveToQuarantine(st, j, scopeDir, p); err != nil {
						return err
					}
				}
			}
			delete(j.Files, p)
			continue
		}
		je.Seq, je.Group = pf.seq, pf.group
		if je.State == "placed" {
			if je.Bytes != pf.length {
				je.State = "verified"
				je.SHA256 = ""
			} else {
				// Same length is not proof — re-read the row and
				// compare hashes before trusting placed bytes across
				// the snapshot boundary.
				blob := filepath.Join(j.Staging, "blobs", p)
				sha, _, rerr := r.captureReadRow(ctx, st, pf.seq, blob, pf.length, j.CaptureID)
				if rerr != nil {
					return rerr
				}
				if sha != je.SHA256 {
					je.State = "verified"
					je.SHA256 = sha // the blob staged IS the new content
				}
			}
		} else {
			je.State, je.SHA256 = "verified", ""
		}
		je.Bytes = pf.length
	}
	for p, pf := range pl.files {
		if _, ok := j.Files[p]; !ok {
			j.Files[p] = &journalEntry{Seq: pf.seq, Bytes: pf.length, Group: pf.group, State: "verified"}
		}
	}
	// Links: same rule — unchanged placed targets keep, changed or new
	// re-place, vanished quarantine their copy-product destination.
	for p, jl := range j.Links {
		t, ok := pl.links[p]
		if !ok {
			if jl.State == "placed" {
				if _, lerr := os.Lstat(filepath.Join(scopeDir, p)); lerr == nil {
					if err := r.moveToQuarantine(st, j, scopeDir, p); err != nil {
						return err
					}
				}
			}
			delete(j.Links, p)
			continue
		}
		if jl.TargetB64 != base64.StdEncoding.EncodeToString(t) {
			jl.TargetB64 = base64.StdEncoding.EncodeToString(t)
			jl.State = ""
		}
	}
	if j.Links == nil {
		j.Links = map[string]*journalLink{}
	}
	for p, t := range pl.links {
		if _, ok := j.Links[p]; !ok {
			j.Links[p] = &journalLink{TargetB64: base64.StdEncoding.EncodeToString(t)}
		}
	}
	// Carried dirs the new manifest dropped are removed when still
	// empty — a dir the operator filled stays.
	oldDirs := j.Dirs
	newDirs := map[string]bool{}
	for _, d := range pl.dirs {
		newDirs[d] = true
	}
	j.Dirs = pl.dirs
	for i := len(oldDirs) - 1; i >= 0; i-- {
		d := oldDirs[i]
		if newDirs[d] {
			continue
		}
		p := filepath.Join(scopeDir, d)
		if fi, lerr := os.Lstat(p); lerr == nil && fi.IsDir() && fi.Mode()&os.ModeSymlink == 0 {
			_ = os.Remove(p) // empty only — a filled dir stays
		}
	}
	// Regress the phase when the replan left work undone — an entry
	// reset to unplaced, a link to re-place, a carried dir missing.
	// "copying" is the safe restart: staging re-proves intact blobs
	// per entry and keeps hardlink groups on one inode, which the
	// promote path's per-entry refetch could not.
	needsWork := false
	for _, je := range j.Files {
		if je.State != "placed" {
			needsWork = true
			break
		}
	}
	if !needsWork {
		for _, jl := range j.Links {
			if jl.State != "placed" {
				needsWork = true
				break
			}
		}
	}
	if !needsWork {
		for _, d := range j.Dirs {
			if fi, serr := os.Stat(filepath.Join(scopeDir, d)); serr != nil || !fi.IsDir() {
				needsWork = true
				break
			}
		}
	}
	if needsWork {
		j.Phase = "copying"
	}
	return r.saveJournal(st, j)
}

// retakeCaptureAndReplan asks the service for a fresh coherent manifest
// under the persisted scope identity, then replans. A scope-identity
// change is terminal for the copy.
func (r *returner) retakeCaptureAndReplan(ctx context.Context, st *returnState, j *filesJournal) error {
	b, err := r.captureRetake(ctx, st, j.ScopeID, j.CaptureID)
	if err != nil {
		return err
	}
	if b.ScopeID != j.ScopeID {
		return errCaptureScope
	}
	if b.CaptureID == j.CaptureID && !b.Retaken {
		// The binding is unchanged — the loss is upstream of the
		// manifest itself (e.g. a still-missing object). A fresh
		// manifest is what a retake must produce; refuse to spin on
		// the same dead association.
		return fmt.Errorf("%w: the capture retake kept the same manifest", errCaptureLost)
	}
	return r.replanForBinding(ctx, st, j, b)
}

// refetchCaptured re-reads one journaled row into its staging blob —
// the promote path's repair for a torn or missing staged copy.
func (r *returner) refetchCaptured(ctx context.Context, st *returnState, p string, je *journalEntry, j *filesJournal) error {
	blob := filepath.Join(j.Staging, "blobs", p)
	sha, n, err := r.captureReadRow(ctx, st, je.Seq, blob, je.Bytes, j.CaptureID)
	if err != nil {
		return err
	}
	je.SHA256, je.Bytes, je.State = sha, n, "verified"
	return nil
}
