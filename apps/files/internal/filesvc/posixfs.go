package filesvc

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// posixRoot is the service's view of one canonical namespace: a directory on a
// real filesystem (a JuiceFS mount in cloud placement, a plain dir locally).
// All access is contained beneath root; symlinks that would escape are denied.
type posixRoot struct {
	root         string
	requireMount bool // refuse file ops when root is not a verified mount

}

// mountFSType returns the fstype of the mountpoint exactly at p.root, per
// /proc/self/mountinfo — which lists mountpoints only, so a bare directory
// simply has no entry. A path stacked under several mounts appears once per
// mount, topmost last, so the last match wins.
func (p *posixRoot) mountFSType() (fstype string, ok bool) {
	data, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return "", false
	}
	found := false
	for _, line := range strings.Split(string(data), "\n") {
		f := strings.Fields(line)
		if len(f) < 10 {
			continue
		}
		// fields: id parent dev root mountpoint opts ... "-" fstype ...
		mp := strings.ReplaceAll(f[4], "\\040", " ")
		if mp != p.root {
			continue
		}
		sep := -1
		for i := 5; i < len(f); i++ {
			if f[i] == "-" {
				sep = i
				break
			}
		}
		if sep < 0 || sep+1 >= len(f) {
			continue
		}
		fstype, found = f[sep+1], true
	}
	return fstype, found
}

// checkMount enforces the mount requirements for canonical-namespace mode
// (requireMount): the root must itself be a live mountpoint AND its
// filesystem must not serve stale metadata to this client. Freshness is
// verified on every op — no verdict is cached across mount instances,
// because neither st_dev nor the kernel mount ID is a safe generation key
// (both are recycled; a recycled ID was observed resurrecting a stale
// verdict and wedging a healthy remount into 503s).
//
// A fuse.juicefs mount must prove zero metadata caching through the
// /.config control file (synthesized by the JuiceFS daemon — not a
// regular file in the namespace). Other FUSE types are refused as
// unverifiable, as are network filesystems (nfs/cifs/etc. have their own
// client-side attribute caches we cannot inspect). Only known
// kernel-coherent local filesystems pass. Fail closed by default, because
// the CAS fingerprint gate reads through this mount and a nonzero attr
// cache silently re-opens the B1 clobber window.
func (p *posixRoot) checkMount() error {
	fstype, ok := p.mountFSType()
	if !ok {
		return ErrMountUnavailable
	}
	return p.verifyFreshness(fstype)
}

// localCoherentFS are filesystems whose metadata is coherent across
// processes on this kernel with no client-side cache layer — safe to
// serve canonical-namespace ops on without further proof. Anything not
// listed (including other FUSE types and network filesystems) is refused:
// we cannot verify what we do not know.
var localCoherentFS = map[string]bool{
	"ext2": true, "ext3": true, "ext4": true,
	"xfs": true, "btrfs": true, "f2fs": true,
	"tmpfs": true, "ramfs": true, "overlay": true,
	"zfs": true, "vfat": true, "exfat": true, "ntfs3": true,
	"minix": true, "hfs": true, "hfsplus": true, "reiserfs": true,
	"jfs": true, "nilfs2": true, "udf": true, "bcachefs": true,
	"erofs": true, "squashfs": true, "msdos": true, "iso9660": true,
}

// verifyFreshness confirms the mounted filesystem cannot return stale
// metadata to this client. fstype comes from mountinfo.
func (p *posixRoot) verifyFreshness(fstype string) error {
	if fstype == "fuse.juicefs" {
		return checkZeroMetadataCache(filepath.Join(p.root, ".config"))
	}
	if localCoherentFS[fstype] {
		return nil
	}
	return fmt.Errorf("%w: filesystem %q cannot prove metadata freshness",
		ErrMountPolicy, fstype)
}

// checkZeroMetadataCache reads a JuiceFS /.config control file and requires
// every metadata cache timeout to be zero. The file is synthesized by the
// JuiceFS client at the mount root — it is not namespace user data.
func checkZeroMetadataCache(cfgPath string) error {
	cfg, err := os.ReadFile(cfgPath)
	if err != nil {
		return fmt.Errorf("%w: no verifiable mount config", ErrMountPolicy)
	}
	var c map[string]any
	if err := json.Unmarshal(cfg, &c); err != nil {
		return fmt.Errorf("%w: mount config unparseable", ErrMountPolicy)
	}
	for _, k := range []string{
		"AttrTimeout", "EntryTimeout", "DirEntryTimeout", "NegEntryTimeout",
	} {
		v, ok := c[k].(float64)
		if !ok {
			return fmt.Errorf("%w: mount config lacks %s", ErrMountPolicy, k)
		}
		if v != 0 {
			return fmt.Errorf("%w: %s=%v must be 0 (mount with "+
				"--attr-cache=0 --entry-cache=0 --dir-entry-cache=0)",
				ErrMountPolicy, k, v)
		}
	}
	return nil
}

var (
	ErrEscape           = errors.New("path escapes scope root")
	ErrNotFound         = errors.New("not found")
	ErrIsDir            = errors.New("is a directory")
	ErrNotDir           = errors.New("not a directory")
	ErrReserved         = errors.New("path uses the service staging prefix")
	ErrMountUnavailable = errors.New("canonical namespace root is not mounted")
	ErrMountPolicy      = errors.New("canonical mount violates freshness policy")
)

// stagingPrefix marks service-internal temp siblings. User-facing ops reject
// it so the namespace listing can hide staging files without hiding user data.
const stagingPrefix = ".filesv-tmp-"

func checkReserved(path string) error {
	for _, seg := range strings.Split(path, "/") {
		if strings.HasPrefix(seg, stagingPrefix) {
			return ErrReserved
		}
	}
	return nil
}

func newRoot(root string) (*posixRoot, error) {
	r, err := filepath.EvalSymlinks(root)
	if err != nil {
		return nil, err
	}
	abs, err := filepath.Abs(r)
	if err != nil {
		return nil, err
	}
	return &posixRoot{root: abs}, nil
}

// resolve maps a client (scope, path) to a host path contained beneath
// root/scope. Each scope is an isolated subtree — a workspace's or
// secretary's own root. The final element may not exist yet (write/mkdir);
// its parent must resolve. ".." segments are rejected outright rather than
// silently clamped — callers get a deterministic error, not a rewrite.
func (p *posixRoot) resolve(scope, path string) (string, error) {
	if scope == "" || strings.Contains(scope, "/") || strings.Contains(scope, "..") || strings.HasPrefix(scope, ".") {
		return "", ErrEscape
	}
	if path == "" || path == "/" {
		return filepath.Join(p.root, scope), nil
	}
	for _, seg := range strings.Split(path, "/") {
		if seg == ".." {
			return "", ErrEscape
		}
	}
	clean := filepath.Clean("/" + path) // leading / forces interpretation as scope-relative
	scopeRoot := filepath.Join(p.root, scope)
	joined := filepath.Join(scopeRoot, clean)
	if joined != scopeRoot && !strings.HasPrefix(joined, scopeRoot+string(filepath.Separator)) {
		return "", ErrEscape
	}
	// Resolve the deepest existing ancestor; missing trailing segments are
	// rejoined verbatim (they contain no ".." and cannot yet be symlinks).
	// Any real component that escapes scopeRoot is denied.
	resolved := joined
	if _, err := os.Lstat(joined); err == nil {
		resolved, err = filepath.EvalSymlinks(joined)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return "", ErrNotFound // dangling symlink
			}
			return "", err
		}
	} else if errors.Is(err, fs.ErrNotExist) {
		// Walk up to the first existing ancestor.
		var missing []string
		cur := joined
		for {
			parent := filepath.Dir(cur)
			missing = append([]string{filepath.Base(cur)}, missing...)
			if parent != scopeRoot && !strings.HasPrefix(parent, scopeRoot+string(filepath.Separator)) {
				return "", ErrNotFound
			}
			if _, lerr := os.Lstat(parent); lerr == nil {
				anc, aerr := filepath.EvalSymlinks(parent)
				if aerr != nil {
					return "", aerr
				}
				if anc != scopeRoot && !strings.HasPrefix(anc, scopeRoot+string(filepath.Separator)) {
					return "", ErrEscape
				}
				parts := append([]string{anc}, missing...)
				resolved = filepath.Join(parts...)
				break
			} else if !errors.Is(lerr, fs.ErrNotExist) {
				return "", lerr
			}
			cur = parent
		}
	} else {
		return "", err
	}
	if resolved != scopeRoot && !strings.HasPrefix(resolved, scopeRoot+string(filepath.Separator)) {
		return "", ErrEscape
	}
	return resolved, nil
}

// resolveParent resolves the parent directory of (scope, path) and returns
// the unresolved host path beneath it. Use for ops that must act on the
// final element literally (remove, rename source): an in-scope symlink that
// points outside must itself be removable.
func (p *posixRoot) resolveParent(scope, path string) (string, error) {
	if path == "" || path == "/" {
		return "", ErrNotDir
	}
	parent, err := p.resolve(scope, filepath.Dir("/"+path))
	if err != nil {
		return "", err
	}
	return filepath.Join(parent, filepath.Base(path)), nil
}

type FileInfo struct {
	Kind    string `json:"kind"` // "file" | "dir"
	Size    int64  `json:"size"`
	MtimeNS int64  `json:"mtime_ns"`
	// Fingerprint identifies the observed content generation cheaply:
	// ino+size+mtime_ns+ctime_ns. It detects executor-direct change.
	Fingerprint string `json:"fingerprint"`
}

func fingerprint(st fs.FileInfo) string {
	s, ok := st.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Sprintf("%d:%d", st.Size(), st.ModTime().UnixNano())
	}
	// Dev is deliberately excluded: it changes when the FUSE filesystem is
	// remounted, which would flag every file as externally changed.
	return fmt.Sprintf("%d:%d:%d:%d", s.Ino, s.Size,
		s.Mtim.Nsec+s.Mtim.Sec*1e9, s.Ctim.Nsec+s.Ctim.Sec*1e9)
}

func (p *posixRoot) stat(scope, path string) (FileInfo, error) {
	host, err := p.resolve(scope, path)
	if err != nil {
		return FileInfo{}, err
	}
	st, err := os.Stat(host) // follow in-root symlinks
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return FileInfo{}, ErrNotFound
		}
		return FileInfo{}, err
	}
	kind := "file"
	if st.IsDir() {
		kind = "dir"
	}
	return FileInfo{Kind: kind, Size: st.Size(), MtimeNS: st.ModTime().UnixNano(), Fingerprint: fingerprint(st)}, nil
}

type ListEntry struct {
	Name    string `json:"name"`
	Kind    string `json:"kind"`
	Size    int64  `json:"size"`
	MtimeNS int64  `json:"mtime_ns"`
}

func (p *posixRoot) list(scope, path string, limit int, cursor string) ([]ListEntry, string, error) {
	host, err := p.resolve(scope, path)
	if err != nil {
		return nil, "", err
	}
	st, err := os.Stat(host)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, "", ErrNotFound
		}
		return nil, "", err
	}
	if !st.IsDir() {
		return nil, "", ErrNotDir
	}
	des, err := os.ReadDir(host)
	if err != nil {
		return nil, "", err
	}
	names := make([]string, 0, len(des))
	for _, de := range des {
		if strings.HasPrefix(de.Name(), ".filesv-tmp-") {
			continue // hide service staging files
		}
		names = append(names, de.Name())
	}
	sort.Strings(names)
	start := 0
	if cursor != "" {
		start = sort.SearchStrings(names, cursor)
		for start < len(names) && names[start] <= cursor {
			start++
		}
	}
	out := make([]ListEntry, 0, limit)
	next := ""
	for i := start; i < len(names) && len(out) < limit; i++ {
		// Lstat: a symlink's target may be outside the scope or even outside
		// the filesystem — report the link, not its target's metadata.
		fi, err := os.Lstat(filepath.Join(host, names[i]))
		if err != nil {
			continue // raced delete — listing stays honest for what exists
		}
		kind := "file"
		if fi.IsDir() {
			kind = "dir"
		} else if fi.Mode()&os.ModeSymlink != 0 {
			kind = "symlink"
		}
		out = append(out, ListEntry{Name: names[i], Kind: kind, Size: fi.Size(), MtimeNS: fi.ModTime().UnixNano()})
		next = names[i]
	}
	if start+len(out) >= len(names) {
		next = ""
	}
	return out, next, nil
}

// open returns the file positioned at off plus its metadata. The caller
// streams the body — no request-controlled allocation ever happens here.
func (p *posixRoot) open(scope, path string, off int64) (*os.File, FileInfo, error) {
	host, err := p.resolve(scope, path)
	if err != nil {
		return nil, FileInfo{}, err
	}
	f, err := os.Open(host)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, FileInfo{}, ErrNotFound
		}
		return nil, FileInfo{}, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, FileInfo{}, err
	}
	if st.IsDir() {
		f.Close()
		return nil, FileInfo{}, ErrIsDir
	}
	info := FileInfo{Kind: "file", Size: st.Size(), MtimeNS: st.ModTime().UnixNano(), Fingerprint: fingerprint(st)}
	if off > 0 {
		if _, err := f.Seek(off, io.SeekStart); err != nil {
			f.Close()
			return nil, FileInfo{}, err
		}
	}
	return f, info, nil
}

// atomicWrite stages content to a temp sibling, fsyncs, renames over the
// target, and fsyncs the directory. A name never resolves to torn content.
// Caller holds the version CAS; this is the durable part of the write.
// exclusive=true publishes the staged file with link(2), which fails with
// EEXIST if anything already occupies the name — the create-only check is
// atomic against executor-side creates, not just advisory.
func (p *posixRoot) atomicWrite(scope, path string, content []byte, exclusive bool) (FileInfo, error) {
	if err := checkReserved(path); err != nil {
		return FileInfo{}, err
	}
	if err := p.ensureScope(scope); err != nil {
		return FileInfo{}, err
	}
	host, err := p.resolve(scope, path)
	if err != nil {
		return FileInfo{}, err
	}
	dir := filepath.Dir(host)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return FileInfo{}, err
	}
	rnd := make([]byte, 8)
	if _, err := rand.Read(rnd); err != nil {
		return FileInfo{}, err
	}
	tmp := filepath.Join(dir, stagingPrefix+hex.EncodeToString(rnd))
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return FileInfo{}, err
	}
	if _, err := f.Write(content); err != nil {
		f.Close()
		os.Remove(tmp)
		return FileInfo{}, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return FileInfo{}, err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return FileInfo{}, err
	}
	if exclusive {
		err = os.Link(tmp, host)
	} else {
		err = os.Rename(tmp, host)
	}
	if err != nil {
		os.Remove(tmp)
		if errors.Is(err, fs.ErrExist) {
			return FileInfo{}, ErrConflict
		}
		if errors.Is(err, syscall.EISDIR) || errors.Is(err, syscall.ENOTDIR) {
			return FileInfo{}, ErrNotDir
		}
		return FileInfo{}, err
	}
	os.Remove(tmp) // link() leaves the staging name; drop it
	if d, err := os.Open(dir); err == nil {
		d.Sync()
		d.Close()
	}
	return p.stat(scope, path)
}

func (p *posixRoot) rename(scope, from, to string, noReplace bool) (FileInfo, error) {
	if err := checkReserved(to); err != nil {
		return FileInfo{}, err
	}
	src, err := p.resolveParent(scope, from)
	if err != nil {
		return FileInfo{}, err
	}
	dst, err := p.resolveParent(scope, to)
	if err != nil {
		return FileInfo{}, err
	}
	if _, err := os.Lstat(src); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return FileInfo{}, ErrNotFound
		}
		return FileInfo{}, err
	}
	if strings.HasPrefix(dst, src+string(filepath.Separator)) {
		return FileInfo{}, ErrEscape // cannot move a dir beneath itself
	}
	// Guard: a non-empty directory rename over an existing non-empty dir is not
	// atomic on POSIX; the contract does not promise it. Files and empty dirs
	// rename atomically. noReplace uses renameat2(RENAME_NOREPLACE) so a
	// racing creator cannot be silently overwritten.
	var rerr error
	if noReplace {
		rerr = unix.Renameat2(unix.AT_FDCWD, src, unix.AT_FDCWD, dst, unix.RENAME_NOREPLACE)
	} else {
		rerr = os.Rename(src, dst)
	}
	if rerr != nil {
		if errors.Is(rerr, fs.ErrExist) {
			return FileInfo{}, ErrConflict
		}
		return FileInfo{}, rerr
	}
	if d, err := os.Open(filepath.Dir(dst)); err == nil {
		d.Sync()
		d.Close()
	}
	return p.stat(scope, to)
}

func (p *posixRoot) remove(scope, path string) error {
	host, err := p.resolveParent(scope, path)
	if err != nil {
		return err
	}
	if err := os.Remove(host); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return ErrNotFound
		}
		if errors.Is(err, syscall.ENOTEMPTY) || errors.Is(err, syscall.EEXIST) {
			return ErrNotEmpty
		}
		return err
	}
	if d, err := os.Open(filepath.Dir(host)); err == nil {
		d.Sync()
		d.Close()
	}
	return nil
}

func (p *posixRoot) mkdir(scope, path string) (FileInfo, error) {
	if err := checkReserved(path); err != nil {
		return FileInfo{}, err
	}
	if err := p.ensureScope(scope); err != nil {
		return FileInfo{}, err
	}
	host, err := p.resolve(scope, path)
	if err != nil {
		return FileInfo{}, err
	}
	if err := os.MkdirAll(host, 0o755); err != nil {
		return FileInfo{}, err
	}
	return p.stat(scope, path)
}

// ensureScope creates the scope's root directory lazily on first mutation.
func (p *posixRoot) ensureScope(scope string) error {
	dir, err := p.resolve(scope, "")
	if err != nil {
		return err
	}
	return os.MkdirAll(dir, 0o755)
}

// sweepStaging removes service staging files older than 10 minutes — e.g.
// left behind by a SIGKILL mid-write. Run at startup and periodically.
// Safe while the service is live only because the prefix is reserved.
func (p *posixRoot) sweepStaging() {
	scopes, err := os.ReadDir(p.root)
	if err != nil {
		return
	}
	cutoff := time.Now().Add(-10 * time.Minute)
	for _, sc := range scopes {
		if !sc.IsDir() {
			continue
		}
		filepath.WalkDir(filepath.Join(p.root, sc.Name()), func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasPrefix(d.Name(), stagingPrefix) {
				return nil
			}
			if fi, err := d.Info(); err == nil && fi.ModTime().Before(cutoff) {
				os.Remove(path)
			}
			return nil
		})
	}
}
