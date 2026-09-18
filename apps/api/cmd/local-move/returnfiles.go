// returnfiles.go is the file phase of a return — what the owner's
// explicit file_mode choice does on this install.
//
//	local: while the source is sealed and frozen, every file in the
//	       Cloud workspace is enumerated, streamed into a staging
//	       directory inside the Local workspace filesystem, verified
//	       against a content hash, and promoted into the secretary's
//	       scope directory — all before activation, so a failed or
//	       unfinished copy can never leave a live-but-fileless
//	       secretary. Destination files that collide with carried
//	       content are quarantined, never silently overwritten.
//	cloud: inside the seal window the destination's scoped storage
//	       credential is minted under the session's lineage, written
//	       into the install's configuration, and the running service
//	       retargeted — all before activation, so the secretary's
//	       first file op already reads and writes the same Cloud
//	       store the person sees.
//	"":    a session created before the choice existed moves records
//	       only, exactly as before.
//
// Durability mirrors the rest of the driver: a per-session journal
// (0600, atomic rename) records every verified and placed file, so a
// crash mid-copy or mid-promote resumes from what actually committed —
// never assuming, never re-quarantining, never declaring a copy done
// that is not.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"

	"github.com/sumi-studio/sumi/apps/api/internal/fileaccess"
	"github.com/sumi-studio/sumi/apps/api/internal/returnsession"
)

// remoteEntry is one carried workspace member: a file (with its source
// size for preflight) or a directory (kept so empty directories survive
// the move).
type remoteEntry struct {
	Path string
	Kind string // "file" | "dir"
	Size int64
}

// filesJournal is the durable copy record at <home>/return/files-<session>.json.
// It is evidence as well as resume state: after a finished run it says
// exactly what was carried, verified, and displaced.
type filesJournal struct {
	Phase string `json:"phase"` // copying → staged → promoted | restored
	// Staging is the scratch directory the blobs were streamed into,
	// inside the workspace filesystem so promotion is a rename.
	Staging    string                   `json:"staging_dir"`
	Quarantine string                   `json:"quarantine_dir,omitempty"`
	Files      map[string]*journalEntry `json:"files"`
	Dirs       []string                 `json:"dirs"`
	// Collateral records every object moved aside that is not a
	// journaled carried file — ancestors displaced to make a directory,
	// symlinks, repeat occupants — so cancellation can restore all of
	// them and evidence can list them.
	Collateral map[string]bool `json:"collateral,omitempty"`
}

type journalEntry struct {
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
	State  string `json:"state"` // verified → placed
	// Quarantined records that the destination path held authored
	// content that was moved aside rather than overwritten.
	Quarantined bool `json:"quarantined,omitempty"`
}

// maxReturnFiles bounds one copy's enumeration. A workspace larger than
// this fails honestly instead of walking an unbounded tree.
const maxReturnFiles = 200000

func (r *returner) filesJournalPath(st *returnState) string {
	return filepath.Join(r.rdir, "files-"+st.SessionID+".json")
}

func (r *returner) loadJournal(st *returnState) (*filesJournal, error) {
	raw, err := os.ReadFile(r.filesJournalPath(st))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var j filesJournal
	if err := json.Unmarshal(raw, &j); err != nil {
		return nil, fmt.Errorf("%s is unreadable (%v); it is kept as-is", r.filesJournalPath(st), err)
	}
	if j.Files == nil {
		j.Files = map[string]*journalEntry{}
	}
	return &j, nil
}

// saveJournal commits the copy record atomically, like the return state.
func (r *returner) saveJournal(st *returnState, j *filesJournal) error {
	raw, err := json.MarshalIndent(j, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(r.rdir, ".files-*.json")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err := f.Chmod(0o600); err != nil {
		f.Close()
		return err
	}
	if _, err := f.Write(raw); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), r.filesJournalPath(st))
}

// filesPhase runs whatever the session's file_mode requires before the
// staged import may activate. It is called on every sealed+staged pass,
// so a resume re-enters it until the durable markers say it is done.
func (r *returner) filesPhase(ctx context.Context, st *returnState, v returnsession.View) error {
	if st.FileMode == "" && v.FileMode != "" {
		st.FileMode = v.FileMode
		if err := r.save(st); err != nil {
			return err
		}
	}
	if v.FileMode != "" && st.FileMode != "" && v.FileMode != st.FileMode {
		return fmt.Errorf("Cloud reports file_mode %s but this return recorded %s — refusing rather than mixing file plans",
			v.FileMode, st.FileMode)
	}
	switch st.FileMode {
	case "local":
		if err := r.copyFilesLocal(ctx, st); err != nil {
			return err
		}
		// Store selection lands before activation: stale Cloud keys are
		// removed and the running service retargeted while the seal is
		// still open, so the secretary's first file op already sees the
		// carried workspace — never a post-activation gap.
		return r.filesConfigLocal(ctx, st)
	case "cloud":
		// The scoped credential, config and service retarget all
		// complete inside the seal window — minting is allowed once
		// sealed, and doing it now means an activated secretary's
		// first file op already uses the chosen store.
		return r.filesConfigCloud(ctx, st)
	case "":
		return nil
	default:
		return fmt.Errorf("Cloud selected an unknown file_mode %q", st.FileMode)
	}
}

// copyFilesLocal carries the sealed Cloud workspace into this install's
// workspace store, verifies every copied byte, and promotes the staged
// tree into the secretary's scope directory. It returns only when the
// journal says every carried file is placed — activation is the caller's
// next committed step, so "copied" is always proven before the
// secretary can run.
func (r *returner) copyFilesLocal(ctx context.Context, st *returnState) error {
	if r.m.wsRoot == "" {
		return fmt.Errorf("this return carries the Cloud workspace to Local storage, but SUMI_WORKSPACE_ROOT is not set — " +
			"the install's local file store is not provisioned; provisioning it is an installer step, " +
			"then re-run `sumi-local-move return-resume`")
	}
	info, err := os.Stat(r.m.wsRoot)
	if err != nil || !info.IsDir() {
		return fmt.Errorf("SUMI_WORKSPACE_ROOT %s is not a usable directory (%v)", r.m.wsRoot, err)
	}
	j, err := r.loadJournal(st)
	if err != nil {
		return err
	}
	// "Done" is proven against the destination's actual bytes, never
	// trusted from a marker alone: a crash between journal promote and
	// state save, or an edit to a promoted file before the resume,
	// re-verifies and re-stages rather than claiming the copy.
	if st.FilesDone && st.Outcome != "" {
		return nil // already activated — the workspace is live, not evidence
	}
	if j != nil && (j.Phase == "promoted" || st.FilesDone) {
		return r.verifyPromotedTree(ctx, st, j)
	}
	if st.FilesDone && j == nil {
		// State says done but the journal that proves it is gone —
		// re-copy idempotently rather than trust the flag.
		st.FilesDone = false
	}
	if j == nil {
		sess8 := st.SessionID
		if len(sess8) > 8 {
			sess8 = sess8[:8]
		}
		j = &filesJournal{
			Phase:      "copying",
			Staging:    filepath.Join(r.m.wsRoot, ".sumi-return-staging-"+sess8),
			Quarantine: filepath.Join(r.m.wsRoot, ".sumi-return-quarantine-"+sess8),
			Files:      map[string]*journalEntry{},
			Collateral: map[string]bool{},
		}
	}
	if j.Phase == "copying" {
		if err := r.stageRemoteFiles(ctx, st, j); err != nil {
			return err
		}
	}
	if j.Phase == "staged" {
		if err := r.promoteStagedFiles(st, j); err != nil {
			return err
		}
	}
	return nil
}

// verifyPromotedTree rehashes every carried file at its destination and
// checks the carried directories exist — the proof that the promoted
// tree, not just the journal, holds the complete inventory. Anything
// that fails is reset to "verified" so promotion refetches it from the
// still-sealed source, and the phase falls back to staged; a clean pass
// rebuilds the counters the crash window may have lost.
func (r *returner) verifyPromotedTree(ctx context.Context, st *returnState, j *filesJournal) error {
	scope, err := fileaccess.ScopeForPersona(st.Persona)
	if err != nil {
		return err
	}
	scopeDir := filepath.Join(r.m.wsRoot, scope)
	dirty := false
	for p, je := range j.Files {
		if je.State != "placed" {
			dirty = true
			continue
		}
		sha, herr := fileSHA256(filepath.Join(scopeDir, p))
		if herr != nil || sha != je.SHA256 {
			// Torn, edited or missing — refetch proves it again; a
			// mismatched occupant is quarantined by promote, so an
			// operator's newer edit is preserved, not clobbered.
			je.State = "verified"
			dirty = true
		}
	}
	for _, d := range j.Dirs {
		if fi, serr := os.Stat(filepath.Join(scopeDir, d)); serr != nil || !fi.IsDir() {
			dirty = true
		}
	}
	if dirty {
		j.Phase = "staged"
		if err := r.saveJournal(st, j); err != nil {
			return err
		}
		return r.promoteStagedFiles(st, j)
	}
	st.FilesDone = true
	st.FilesCopied = int64(len(j.Files))
	var bytes, quarantined int64
	for _, je := range j.Files {
		bytes += je.Bytes
		if je.Quarantined {
			quarantined++
		}
	}
	st.FilesBytes = bytes
	st.FilesQuarantined = quarantined
	return r.save(st)
}

// stageRemoteFiles enumerates the frozen source workspace, checks the
// destination has room, and streams every file to a verified staged blob.
// A file already journaled verified survives a resume; anything else is
// fetched fresh — a staged blob that fails its own hash is re-copied,
// never trusted on faith.
func (r *returner) stageRemoteFiles(ctx context.Context, st *returnState, j *filesJournal) error {
	entries, unsupported, err := r.enumerateRemote(ctx, st)
	if err != nil {
		return err
	}
	if len(unsupported) > 0 {
		sort.Strings(unsupported)
		shown := unsupported
		if len(shown) > 10 {
			shown = shown[:10]
		}
		return fmt.Errorf("the Cloud workspace contains entries this copy cannot carry (not regular files or directories): %s%s — "+
			"the return is still sealed; move or remove them on Cloud and run `sumi-local-move return-resume`, "+
			"or `sumi-local-move return-cancel`",
			strings.Join(shown, ", "), moreSuffix(len(unsupported), 10))
	}
	var total, files int64
	for _, e := range entries {
		switch e.Kind {
		case "file":
			files++
			total += e.Size
		case "dir":
			j.Dirs = append(j.Dirs, e.Path)
		}
	}
	if files > maxReturnFiles {
		return fmt.Errorf("the Cloud workspace has more than %d files — too large for this return's copy; "+
			"reduce it on Cloud or choose cloud file storage in a new return", maxReturnFiles)
	}
	if err := checkCapacity(r.m.wsRoot, total); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(j.Staging, "blobs"), 0o700); err != nil {
		return err
	}
	for _, e := range entries {
		if e.Kind != "file" {
			continue
		}
		if je, ok := j.Files[e.Path]; ok && je.State == "verified" {
			// Resume: trust the journal only as far as the blob's own
			// bytes prove — rehash before skipping the fetch.
			if blobOK(filepath.Join(j.Staging, "blobs", e.Path), je) {
				continue
			}
		}
		je, err := r.fetchFile(ctx, st, e.Path, j)
		if err != nil {
			return err
		}
		j.Files[e.Path] = je
		if err := r.saveJournal(st, j); err != nil {
			return err
		}
	}
	j.Phase = "staged"
	return r.saveJournal(st, j)
}

func moreSuffix(n, shown int) string {
	if n > shown {
		return fmt.Sprintf(" and %d more", n-shown)
	}
	return ""
}

// enumerateRemote walks the sealed source workspace through the grant's
// read-only file surface: one directory at a time, every page, until the
// whole tree is seen. Symlinks and special files are collected, not
// followed — the caller decides whether the copy can proceed.
func (r *returner) enumerateRemote(ctx context.Context, st *returnState) (entries []remoteEntry, unsupported []string, err error) {
	var walk func(dir string) error
	walk = func(dir string) error {
		cursor := ""
		for {
			q := url.Values{"path": {dir}, "limit": {"1000"}}
			if cursor != "" {
				q.Set("cursor", cursor)
			}
			var out struct {
				Entries []struct {
					Name string `json:"name"`
					Kind string `json:"kind"`
					Size int64  `json:"size"`
				} `json:"entries"`
				Next string `json:"next_cursor"`
			}
			if err := r.grantFileGet(ctx, st, "list", q, &out); err != nil {
				return err
			}
			for _, e := range out.Entries {
				p := e.Name
				if dir != "" {
					p = dir + "/" + e.Name
				}
				switch e.Kind {
				case "dir":
					entries = append(entries, remoteEntry{Path: p, Kind: "dir"})
					if err := walk(p); err != nil {
						return err
					}
				case "file":
					entries = append(entries, remoteEntry{Path: p, Kind: "file", Size: e.Size})
				default:
					unsupported = append(unsupported, p+" ("+e.Kind+")")
				}
			}
			if len(entries) > maxReturnFiles {
				return fmt.Errorf("the Cloud workspace has more than %d entries — refusing to walk an unbounded tree", maxReturnFiles)
			}
			if out.Next == "" {
				return nil
			}
			cursor = out.Next
		}
	}
	err = walk("")
	return entries, unsupported, err
}

// grantFileGet issues one read op against the return session's file
// surface and decodes the JSON body. Transport failures map to
// errUnreachable so the drive loop treats them as pending, never as a
// permanent verdict.
func (r *returner) grantFileGet(ctx context.Context, st *returnState, op string, q url.Values, out any) error {
	u := st.SessionURL + "/files/" + op
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+st.Grant)
	res, err := r.m.client.Do(req)
	r.m.answered.Store(true)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("%w: %v", errUnreachable, err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 1<<16))
		switch {
		case ctx.Err() != nil:
			return ctx.Err()
		case res.StatusCode == http.StatusUnauthorized || res.StatusCode == http.StatusForbidden:
			return fmt.Errorf("%w: Cloud answered HTTP %d for the workspace copy", errGrantRejected, res.StatusCode)
		default:
			return fmt.Errorf("%w: HTTP %d for file %s", errUnreachable, res.StatusCode, op)
		}
	}
	return json.NewDecoder(io.LimitReader(res.Body, 32<<20)).Decode(out)
}

// fetchFile streams one source file into the staging tree and proves the
// staged bytes: the streamed content hash must match the recorded
// content_sha, or — when the recorded hash lags an external POSIX write —
// a second full read must produce the same bytes. Anything else is a
// concurrent writer, and the copy refuses rather than carry a guess.
func (r *returner) fetchFile(ctx context.Context, st *returnState, path string, j *filesJournal) (*journalEntry, error) {
	blob := filepath.Join(j.Staging, "blobs", path)
	sha, n, err := r.streamFile(ctx, st, path, blob)
	if err != nil {
		return nil, err
	}
	var stt struct {
		SHA     string `json:"content_sha"`
		Version int64  `json:"version"`
		Extern  bool   `json:"external_change"`
	}
	if err := r.grantFileGet(ctx, st, "stat", url.Values{"path": {path}}, &stt); err != nil {
		return nil, err
	}
	if stt.SHA == sha && !stt.Extern {
		return &journalEntry{SHA256: sha, Bytes: n, State: "verified"}, nil
	}
	// The recorded hash lags or the live file drifted from its record —
	// read once more. Identical bytes twice is a stable content version;
	// anything else is a writer racing the frozen scope.
	sha2, _, err := r.streamFile(ctx, st, path, blob)
	if err != nil {
		return nil, err
	}
	if sha2 != sha {
		return nil, fmt.Errorf("%s changed while it was being copied — a writer is still reaching the Cloud workspace; "+
			"the return is still sealed, so it is safe to find and stop the writer and run `sumi-local-move return-resume`", path)
	}
	return &journalEntry{SHA256: sha, Bytes: n, State: "verified"}, nil
}

// streamFile reads one remote file into a staging blob (fsync + rename),
// returning its SHA-256 and size. The same silent-peer bound the bundle
// applies: bytes must keep moving or the attempt is given up, retried by
// resume — never reported as carried.
func (r *returner) streamFile(ctx context.Context, st *returnState, path, blob string) (string, int64, error) {
	u := st.SessionURL + "/files/read?path=" + url.QueryEscape(path)
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
		_, _ = io.Copy(io.Discard, io.LimitReader(body, 1<<16))
		switch {
		case ctx.Err() != nil:
			return "", 0, ctx.Err()
		case res.StatusCode == http.StatusUnauthorized || res.StatusCode == http.StatusForbidden:
			return "", 0, fmt.Errorf("%w: Cloud answered HTTP %d for %s", errGrantRejected, res.StatusCode, path)
		default:
			return "", 0, fmt.Errorf("%w: HTTP %d for %s", errUnreachable, res.StatusCode, path)
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
			return "", 0, fmt.Errorf("%w: %v for %s; resume retries the file",
				errUnreachable, errStalled, r.m.stall)
		default:
			return "", 0, fmt.Errorf("%w: %v", errUnreachable, err)
		}
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

// blobOK rehashes a staged blob against its journal entry — the only
// proof a resume may skip the fetch on.
func blobOK(blob string, je *journalEntry) bool {
	sha, err := fileSHA256(blob)
	return err == nil && sha == je.SHA256
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// checkCapacity refuses the copy when the workspace filesystem cannot
// hold the carried bytes — before a single byte is staged.
func checkCapacity(dir string, need int64) error {
	var st unix.Statfs_t
	if err := unix.Statfs(dir, &st); err != nil {
		return err
	}
	avail := int64(st.Bavail) * int64(st.Bsize)
	if avail < need {
		return fmt.Errorf("this install's workspace has %d bytes free but the Cloud workspace carries %d — "+
			"free space or choose cloud file storage in a new return; the return is still sealed and nothing was changed",
			avail, need)
	}
	return nil
}

// promoteStagedFiles moves the verified tree into the secretary's scope
// directory. A destination path holding different authored content is
// quarantined (moved aside whole, recorded in the journal) — never
// overwritten, never deleted. An identical destination file is simply
// kept: re-running the promote is then idempotent.
func (r *returner) promoteStagedFiles(st *returnState, j *filesJournal) error {
	scope, err := fileaccess.ScopeForPersona(st.Persona)
	if err != nil {
		return err
	}
	scopeDir := filepath.Join(r.m.wsRoot, scope)
	if err := os.MkdirAll(scopeDir, 0o700); err != nil {
		return err
	}
	if j.Collateral == nil {
		j.Collateral = map[string]bool{}
	}
	// Carried directories first — each ancestor chain is made safe one
	// component at a time so a foreign file or symlink standing where a
	// directory must go is quarantined, never followed or overwritten.
	for _, d := range j.Dirs {
		if err := r.ensureScopeDir(st, j, scopeDir, d); err != nil {
			return err
		}
	}
	paths := make([]string, 0, len(j.Files))
	for p := range j.Files {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, p := range paths {
		je := j.Files[p]
		dest := filepath.Join(scopeDir, p)
		if err := r.ensureScopeDir(st, j, scopeDir, filepath.Dir(p)); err != nil {
			return err
		}
		info, lerr := os.Lstat(dest)
		if lerr == nil && info.Mode().IsRegular() {
			if sha, derr := fileSHA256(dest); derr == nil && sha == je.SHA256 {
				// Already the carried content — placed earlier or identical.
				if je.State != "placed" {
					je.State = "placed"
					if err := r.saveJournal(st, j); err != nil {
						return err
					}
				}
				continue
			}
		}
		if lerr == nil {
			// A foreign object stands at the carried path — different
			// bytes, a directory, a symlink, anything. It is moved
			// aside whole and recorded; never overwritten, never
			// deleted, and a symlink is never followed.
			if err := r.quarantineOccupant(st, j, scopeDir, p, je); err != nil {
				return err
			}
		}
		blob := filepath.Join(j.Staging, "blobs", p)
		if !blobOK(blob, je) {
			// The staged copy is gone or torn but the journal says it
			// was carried — the source is still sealed, so refetch and
			// re-verify rather than trust a marker.
			nje, err := r.fetchFile(context.Background(), st, p, j)
			if err != nil {
				return err
			}
			*je = *nje
			if err := r.saveJournal(st, j); err != nil {
				return err
			}
		}
		if err := os.Rename(blob, dest); err != nil {
			return err
		}
		je.State = "placed"
		if err := r.saveJournal(st, j); err != nil {
			return err
		}
		midPromote()
	}
	j.Phase = "promoted"
	if err := r.saveJournal(st, j); err != nil {
		return err
	}
	// The tree is placed; the scratch can go. The journal stays as the
	// durable evidence of what was carried and displaced.
	if err := os.RemoveAll(j.Staging); err != nil {
		return err
	}
	st.FilesDone = true
	st.FilesCopied = int64(len(j.Files))
	var bytes int64
	var quarantined int64
	for _, je := range j.Files {
		bytes += je.Bytes
		if je.Quarantined {
			quarantined++
		}
	}
	st.FilesBytes = bytes
	st.FilesQuarantined = quarantined
	return r.save(st)
}

// ensureScopeDir materializes rel as a real directory chain under
// scopeDir. A foreign object standing where a path component must be a
// directory — a file, or a symlink (which could point outside the
// workspace entirely) — is quarantined into the return's quarantine
// dir and journaled as collateral; it is never followed and never
// deleted.
func (r *returner) ensureScopeDir(st *returnState, j *filesJournal, scopeDir, rel string) error {
	if rel == "" || rel == "." || rel == "/" {
		return nil
	}
	cur := scopeDir
	for _, seg := range strings.Split(filepath.Clean(rel), string(filepath.Separator)) {
		if seg == "" || seg == "." {
			continue
		}
		cur = filepath.Join(cur, seg)
		fi, err := os.Lstat(cur)
		switch {
		case errors.Is(err, os.ErrNotExist):
			if err := os.Mkdir(cur, 0o700); err != nil {
				return err
			}
		case err != nil:
			return err
		case fi.IsDir() && fi.Mode()&os.ModeSymlink == 0:
			// a real directory — the path continues through it
		default:
			drel, rerr := filepath.Rel(scopeDir, cur)
			if rerr != nil || strings.HasPrefix(drel, "..") {
				return fmt.Errorf("refusing workspace path %s", cur)
			}
			if err := r.moveToQuarantine(st, j, scopeDir, drel); err != nil {
				return err
			}
			if err := os.Mkdir(cur, 0o700); err != nil {
				return err
			}
		}
	}
	return nil
}

// quarantineOccupant moves the foreign object at scopeDir/p aside into
// the return's quarantine directory. Journaled carried files mark their
// entry Quarantined; anything else (a dir/symlink ancestor or an object
// appearing after earlier displacement) is recorded as collateral.
func (r *returner) quarantineOccupant(st *returnState, j *filesJournal, scopeDir, p string, je *journalEntry) error {
	if err := r.moveToQuarantine(st, j, scopeDir, p); err != nil {
		return err
	}
	if je != nil {
		je.Quarantined = true
		return r.saveJournal(st, j)
	}
	return nil
}

// moveToQuarantine renames scopeDir/rel into j.Quarantine/rel, picking a
// suffix when that slot is already taken, and records the displacement
// so cancellation can put every moved object back.
func (r *returner) moveToQuarantine(st *returnState, j *filesJournal, scopeDir, rel string) error {
	dstRel := rel
	if j.Collateral == nil {
		j.Collateral = map[string]bool{}
	}
	if j.Collateral[dstRel] {
		// Same source path displaced twice across resumes — keep both.
		for i := 2; ; i++ {
			cand := fmt.Sprintf("%s.%d", rel, i)
			if !j.Collateral[cand] {
				dstRel = cand
				break
			}
		}
	}
	q := filepath.Join(j.Quarantine, dstRel)
	if err := os.MkdirAll(filepath.Dir(q), 0o700); err != nil {
		return err
	}
	if err := os.Rename(filepath.Join(scopeDir, rel), q); err != nil {
		return fmt.Errorf("could not move aside the existing %s (%v) — it was left in place; "+
			"the staged copy is verified and waiting, resolve the path and run `sumi-local-move return-resume`", rel, err)
	}
	j.Collateral[dstRel] = true
	return r.saveJournal(st, j)
}

// restoreWorkspace unwinds a cancelled local-mode copy: placed files
// that still match their carried hash are removed (a file edited since
// the copy is the operator's and is left in place), every displaced
// object moves back from quarantine to its recorded path, and
// directories the copy created are removed when empty. Deepest paths
// first so children clear before their parents. The journal's phase
// becomes "restored" and stays as the evidence of what was undone;
// anything that could not be put back is reported, never hidden.
func (r *returner) restoreWorkspace(st *returnState, j *filesJournal) error {
	scope, err := fileaccess.ScopeForPersona(st.Persona)
	if err != nil {
		return err
	}
	scopeDir := filepath.Join(r.m.wsRoot, scope)
	var kept []string

	paths := make([]string, 0, len(j.Files))
	for p := range j.Files {
		paths = append(paths, p)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(paths)))
	for _, p := range paths {
		je := j.Files[p]
		dest := filepath.Join(scopeDir, p)
		if je.State == "placed" {
			if sha, herr := fileSHA256(dest); herr == nil && sha == je.SHA256 {
				if err := os.Remove(dest); err != nil && !errors.Is(err, os.ErrNotExist) {
					return fmt.Errorf("could not remove carried file %s: %v", p, err)
				}
			} else if _, serr := os.Lstat(dest); serr == nil {
				kept = append(kept, p+" (changed since the copy — left in place)")
			}
		}
		// An object quarantined for this path goes back only if nothing
		// now stands there — the displaced original never clobbers a
		// surviving occupant.
		if je.Quarantined {
			q := filepath.Join(j.Quarantine, p)
			if _, serr := os.Lstat(dest); errors.Is(serr, os.ErrNotExist) {
				if err := os.MkdirAll(filepath.Dir(dest), 0o700); err != nil {
					return err
				}
				if err := os.Rename(q, dest); err != nil {
					return fmt.Errorf("could not put %s back from quarantine: %v", p, err)
				}
				delete(j.Collateral, p)
			} else if _, qerr := os.Lstat(q); qerr == nil {
				kept = append(kept, "the original "+p+" stayed in "+j.Quarantine+" — its path is occupied")
			}
		}
	}
	// Collateral ancestors, deepest first: a displaced file that became
	// a carried directory's ancestor goes back once that directory is
	// empty; a still-occupied path keeps its original in quarantine.
	collateral := make([]string, 0, len(j.Collateral))
	for c := range j.Collateral {
		collateral = append(collateral, c)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(collateral)))
	for _, c := range collateral {
		dest := filepath.Join(scopeDir, c)
		q := filepath.Join(j.Quarantine, c)
		if fi, serr := os.Lstat(dest); serr == nil && fi.IsDir() {
			if err := os.Remove(dest); err != nil {
				kept = append(kept, "the original "+c+" stayed in "+j.Quarantine+" — its path is occupied")
				continue
			}
		}
		if _, serr := os.Lstat(dest); errors.Is(serr, os.ErrNotExist) {
			if _, qerr := os.Lstat(q); qerr != nil {
				delete(j.Collateral, c) // already restored or never landed
				continue
			}
			if err := os.MkdirAll(filepath.Dir(dest), 0o700); err != nil {
				return err
			}
			if err := os.Rename(q, dest); err != nil {
				return fmt.Errorf("could not put %s back from quarantine: %v", c, err)
			}
			delete(j.Collateral, c)
		} else {
			kept = append(kept, "the original "+c+" stayed in "+j.Quarantine+" — its path is occupied")
		}
	}
	// Carried directories go last, deepest first — each is removed only
	// when it is still a real directory and empty, so a directory holding
	// survivor files stays and a restored ancestor (a symlink or file the
	// collateral pass just put back at a carried dir's path) is never
	// unlinked again.
	for i := len(j.Dirs) - 1; i >= 0; i-- {
		p := filepath.Join(scopeDir, j.Dirs[i])
		if fi, lerr := os.Lstat(p); lerr == nil && fi.IsDir() && fi.Mode()&os.ModeSymlink == 0 {
			_ = os.Remove(p)
		}
	}
	_ = os.RemoveAll(j.Staging)
	_ = os.Remove(j.Quarantine) // succeeds only when fully restored
	j.Phase = "restored"
	if err := r.saveJournal(st, j); err != nil {
		return err
	}
	if len(kept) > 0 {
		r.m.say("The cancelled copy was unwound; %d item(s) stayed where they are:", len(kept))
		for _, k := range kept {
			r.m.say("  %s", k)
		}
	}
	return nil
}

// filesConfigCloud mints (or re-mints) this destination's scoped storage
// credential, writes the install's file configuration, and retargets the
// running service — all before activation reports done, so the
// secretary's first file op already uses the Cloud store. CredDone is
// saved only after every step landed: a lost response or a resume
// re-enters and a re-mint supersedes the undelivered token rather than
// stranding access.
func (r *returner) filesConfigCloud(ctx context.Context, st *returnState) error {
	if st.CredDone {
		return nil
	}
	raw, code, err := r.m.callSession(ctx, http.MethodPost, st.SessionURL+"/files-credential",
		st.Grant, nil, "")
	if err != nil {
		return err
	}
	if code != http.StatusCreated {
		// 5xx and transport failures already came back wrapped as
		// unreachable; a surviving non-201 is a refusal, and a refused
		// mint never becomes valid by retrying.
		return fmt.Errorf("Cloud answered HTTP %d minting the file credential", code)
	}
	var cred struct {
		Token string `json:"file_token"`
		Scope string `json:"scope"`
	}
	if err := json.Unmarshal(raw, &cred); err != nil || cred.Token == "" || cred.Scope == "" {
		return fmt.Errorf("Cloud answered an unreadable file credential")
	}
	// The storage URL is the session host's scoped proxy — the Local
	// service's file tools read it through fileaccess.FromEnv.
	u, err := url.Parse(st.SessionURL)
	if err != nil {
		return err
	}
	filesURL := u.Scheme + "://" + u.Host + returnsession.FileRoutePrefix
	if r.config != "" {
		if err := upsertConfigKey(r.config, "SUMI_FILESVC_URL", filesURL); err != nil {
			return err
		}
		if err := upsertConfigKey(r.config, "SUMI_FILESVC_TOKEN", cred.Token); err != nil {
			return err
		}
	}
	// The 0600 evidence file always lands: with no config path the
	// operator can still see (and copy) the credential material exactly
	// once.
	if err := r.writeFilesEvidence(st, map[string]any{
		"file_mode":  "cloud",
		"files_url":  filesURL,
		"scope":      cred.Scope,
		"file_token": cred.Token,
	}); err != nil {
		return err
	}
	// Retarget before CredDone: a service still bound to the local
	// store while the evidence claims cloud would read the wrong
	// workspace on the secretary's first op. The persona is only staged
	// here — the service is stopped now and restarted once activation
	// commits (convergeFileService), never restarted into a staged
	// persona that cannot take the writer lease.
	if err := r.prepareServiceRetarget(ctx, st, "cloud", filesURL, cred.Token); err != nil {
		return err
	}
	st.CredDone = true
	return r.save(st)
}

// prepareServiceRetarget makes the running Local service safe for the
// file store this return selected, while the seal is still open and the
// persona is only staged. It cannot restart the service here: a core
// host started now would wait on the writer lease of a persona that is
// not active yet and die trying — the restart that adopts the config is
// owed (st.SvcRestart) and converged once activation commits. What this
// does inside the seal window is stop the service: the old store stops
// answering file ops before the cut, and no half-configured core can
// run. A service already on the wanted store, or not running, is left
// alone — the config is adopted at its next start either way.
func (r *returner) prepareServiceRetarget(ctx context.Context, st *returnState, want, wantURL, wantToken string) error {
	runDir := filepath.Join(filepath.Dir(r.rdir), "run") // <state-home>/run
	if _, err := os.Stat(filepath.Join(runDir, "service.pid")); err != nil {
		return nil // no running service — config is adopted at next start
	}
	wantEnv := filesEnvWant(want, wantURL, wantToken)
	marker := filepath.Join(runDir, "files-env")
	cur, _ := os.ReadFile(marker)
	if wantEnv != "" && filesEnvMatches(cur, wantEnv) {
		return nil // already serving the wanted store
	}
	ctl := os.Getenv("SUMI_LOCAL_CTL")
	if ctl == "" {
		if wantEnv != "" && strings.TrimSpace(string(cur)) == "none" {
			return nil // file store never mounted — nothing running to retarget
		}
		r.m.say("The file configuration is written, but the running service still")
		r.m.say("uses its earlier file store and could not be stopped from here.")
		r.m.say("Run `sumi-local stop` then `sumi-local-move return-resume`.")
		return errPending
	}
	cctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	out, err := exec.CommandContext(cctx, ctl, "stop").CombinedOutput()
	if err != nil {
		r.m.say("The file configuration is written, but stopping the service to")
		r.m.say("retarget its file store failed (%v): %s", err, strings.TrimSpace(string(out)))
		return errPending
	}
	st.SvcRestart = wantEnv
	return r.save(st)
}

// convergeFileService performs the restart prepareServiceRetarget owed:
// `sumi-local start` recomputes the wanted environment itself and spawns
// the service against it; afterwards the recorded marker must read
// exactly the owed store identity — a fingerprinted want proves this
// endpoint and credential were adopted, not just the mode. A restart
// that fails leaves the service down but the return pending: the
// session is still open, resume retries, and `sumi-local start` run by
// hand converges the same state.
func (r *returner) convergeFileService(ctx context.Context, st *returnState) error {
	if st.SvcRestart == "" {
		return nil
	}
	ctl := os.Getenv("SUMI_LOCAL_CTL")
	if ctl == "" {
		r.m.say("The secretary is active and its file store is configured, but the")
		r.m.say("service could not be restarted from here. Run `sumi-local start`,")
		r.m.say("then `sumi-local-move return-resume`.")
		return errPending
	}
	cctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	out, err := exec.CommandContext(cctx, ctl, "start").CombinedOutput()
	if err != nil {
		r.m.say("The secretary is active and its file store is configured, but")
		r.m.say("restarting the service onto it failed (%v): %s", err, strings.TrimSpace(string(out)))
		return errPending
	}
	marker := filepath.Join(filepath.Dir(r.rdir), "run", "files-env")
	got, rerr := os.ReadFile(marker)
	if rerr != nil || !filesEnvMatches(got, st.SvcRestart) {
		r.m.say("The service restarted but its file store marker reads %q, not %q.",
			strings.TrimSpace(string(got)), st.SvcRestart)
		return errPending
	}
	st.SvcRestart = ""
	return r.save(st)
}

// restoreServiceAfterCancel converges the service after a cancel: a
// service prepareServiceRetarget stopped is started again on the
// converged (post-cleanup) config, and a still-running one is restarted
// only if its recorded store no longer matches what the config now
// says. Best effort — the session is ending anyway; a failure is said
// plainly and `sumi-local start` recovers the same state by hand.
func (r *returner) restoreServiceAfterCancel(ctx context.Context, st *returnState) {
	runDir := filepath.Join(filepath.Dir(r.rdir), "run")
	stoppedByUs := st.SvcRestart != ""
	if stoppedByUs {
		st.SvcRestart = ""
		if err := r.save(st); err != nil {
			r.m.say("could not clear the owed service restart: %v", err)
		}
	}
	// A restart only makes sense while a secretary can actually take the
	// writer lease. After a cancelled return the slot is the surrendered
	// shell again — the service cannot serve it and a start would die
	// waiting on the lease — so the service stays down and that is said.
	var authority string
	_ = r.pool.QueryRow(ctx,
		`SELECT authority FROM core_personas WHERE persona_id = $1`,
		st.SlotPersona).Scan(&authority)
	if authority != "active" {
		if stoppedByUs {
			r.m.say("The service stays stopped: this install's secretary copy is")
			r.m.say("not active, so there is nothing for it to serve. `sumi-local start`")
			r.m.say("brings it back once a secretary is active here again.")
		}
		return
	}
	if !stoppedByUs {
		if _, err := os.Stat(filepath.Join(runDir, "service.pid")); err != nil {
			return // nothing running, nothing stopped — nothing to restore
		}
	}
	ctl := os.Getenv("SUMI_LOCAL_CTL")
	if ctl == "" {
		if stoppedByUs {
			r.m.say("The service was stopped for the return's file setup; run")
			r.m.say("`sumi-local start` to bring it back on the restored configuration.")
		}
		return
	}
	cctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	out, err := exec.CommandContext(cctx, ctl, "start").CombinedOutput()
	if err != nil {
		r.m.say("The service could not be restarted after the cancel (%v): %s —",
			err, strings.TrimSpace(string(out)))
		r.m.say("run `sumi-local start` to bring it back on the restored configuration.")
	}
}

// filesEnvWant is the marker content `sumi-local` computes for this
// store: the mode word plus a fingerprint of the URL+credential pair —
// the same construction the shell script uses (sha256 of
// url+"\n"+token, first 12 hex chars). With no endpoint known the want
// degrades to the bare mode word.
func filesEnvWant(mode, u, token string) string {
	if mode == "" || u == "" || token == "" {
		return mode
	}
	h := sha256.Sum256([]byte(u + "\n" + token))
	return fmt.Sprintf("%s:%x", mode, h[:6])
}

// filesEnvMatches compares the run/files-env marker to the wanted
// store identity. A fingerprinted want ("cloud:ab12cd…") must match
// exactly — the marker records what the running service was actually
// spawned with, so equality proves adoption of THIS endpoint and
// credential, not just the mode. A bare want word accepts either the
// word or a fingerprint of it, for callers that cannot know the
// credential (cancellation, absent config).
func filesEnvMatches(marker []byte, want string) bool {
	m := strings.TrimSpace(string(marker))
	if strings.Contains(want, ":") {
		return m == want
	}
	return m == want || strings.HasPrefix(m, want+":")
}

// filesConfigLocal records the local-mode outcome: where the working
// store now lives and that Cloud's copy is retained read-only — not
// synced, not a managed backup. A previous cloud-mode return's scoped
// credential config must not survive into a local working store, so the
// client keys it wrote are removed before the evidence lands.
func (r *returner) filesConfigLocal(ctx context.Context, st *returnState) error {
	if r.config != "" {
		for _, k := range []string{"SUMI_FILESVC_URL", "SUMI_FILESVC_TOKEN"} {
			if err := removeConfigKey(r.config, k); err != nil {
				return err
			}
		}
	}
	j, err := r.loadJournal(st)
	if err != nil {
		return err
	}
	scope, err := fileaccess.ScopeForPersona(st.Persona)
	if err != nil {
		return err
	}
	ev := map[string]any{
		"file_mode":     "local",
		"workspace_dir": filepath.Join(r.m.wsRoot, scope),
		"files_copied":  st.FilesCopied,
		"files_bytes":   st.FilesBytes,
		"cloud_copy":    "retained read-only; not synced and not a managed backup",
	}
	if j != nil && j.Quarantine != "" {
		ev["quarantine_dir"] = j.Quarantine
	}
	if err := r.writeFilesEvidence(st, ev); err != nil {
		return err
	}
	// A running service bound to a prior cloud-mode store must be
	// retargeted back to the local filesvc before the workspace it
	// just received is trusted as the working set. The local store's
	// endpoint+credential come from the install's own config; when it
	// cannot be read the check falls back to the mode word.
	var localURL, localTok string
	if r.config != "" {
		if listen, err := configValue(r.config, "SUMI_FILES_LISTEN"); err == nil && listen != "" {
			localURL = "http://" + listen
		}
		localTok, _ = configValue(r.config, "SUMI_FILES_TOKEN")
	}
	return r.prepareServiceRetarget(ctx, st, "local", localURL, localTok)
}

// writeFilesEvidence commits <home>/return/files.json — the install's
// record of where this secretary's working files live after the return.
func (r *returner) writeFilesEvidence(st *returnState, ev map[string]any) error {
	raw, err := json.MarshalIndent(ev, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(r.rdir, ".files-evidence-*.json")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err := f.Chmod(0o600); err != nil {
		f.Close()
		return err
	}
	if _, err := f.Write(raw); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), filepath.Join(r.rdir, "files.json"))
}

// upsertConfigKey sets key in an env file: rewriteConfigKey when the key
// exists, a quoted append when it does not. A credential file that was
// provisioned without file keys gains them instead of failing.
func upsertConfigKey(path, key, value string) error {
	if _, err := configValue(path, key); err == nil {
		return rewriteConfigKey(path, key, value)
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	line := key + "='" + strings.ReplaceAll(value, "'", `'\''`) + "'\n"
	_, err = f.WriteString(line)
	return err
}

// removeConfigKey drops every assignment of key from an env file through
// the same 0600 temp + rename discipline as rewriteConfigKey. An absent
// key is not an error — removal is convergence, not a lookup.
func removeConfigKey(path, key string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	lines := strings.Split(string(raw), "\n")
	found := false
	out := lines[:0]
	for _, l := range lines {
		if strings.HasPrefix(strings.TrimSpace(l), key+"=") {
			found = true
			continue
		}
		out = append(out, l)
	}
	if !found {
		return nil
	}
	tmp := path + ".part"
	if err := os.WriteFile(tmp, []byte(strings.Join(out, "\n")), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
