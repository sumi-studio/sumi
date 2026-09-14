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
	"strconv"
	"strings"
	"sync"
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

	mcMu       sync.Mutex
	mcInflight *inflightMountCheck // single-flight: one checker serves all requests
}

// inflightMountCheck broadcasts one checkMountInner result to every caller
// waiting on it — a wedged FUSE mount parks exactly one goroutine no matter
// how many requests arrive (operation-review B F5).
type inflightMountCheck struct {
	done chan struct{}
	err  error
}

// mountInfoEntry is one parsed /proc/self/mountinfo line.
type mountInfoEntry struct {
	id         uint64 // field 0: kernel mount ID
	root       string // field 3: root of the mount within its filesystem
	mountpoint string // field 4
	fstype     string // first field after the "-" separator
}

// parseMountInfo parses the kernel mount table; malformed lines are skipped.
func parseMountInfo(data []byte) []mountInfoEntry {
	var out []mountInfoEntry
	for _, line := range strings.Split(string(data), "\n") {
		f := strings.Fields(line)
		if len(f) < 10 {
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
		id, err := strconv.ParseUint(f[0], 10, 64)
		if err != nil {
			continue
		}
		out = append(out, mountInfoEntry{
			id:         id,
			root:       unescapeMountInfo(f[3]),
			mountpoint: unescapeMountInfo(f[4]),
			fstype:     f[sep+1],
		})
	}
	return out
}

// findMount returns the mountinfo entry for the given kernel mount ID.
func findMount(entries []mountInfoEntry, id uint64) (mountInfoEntry, bool) {
	for _, e := range entries {
		if e.id == id {
			return e, true
		}
	}
	return mountInfoEntry{}, false
}

// unescapeMountInfo decodes the \ooo octal escapes the kernel applies to
// mountinfo fields containing space, tab, newline, or backslash.
func unescapeMountInfo(s string) string {
	if !strings.Contains(s, "\\") {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) {
			if v, err := strconv.ParseUint(s[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(v))
				i += 3
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// mountVerdict decides whether the mount the kernel resolves root to may
// serve canonical file operations. visible is the mountinfo entry for the
// mount statx(STATX_MNT_ID) reports at the path — the mount a subsequent
// open would actually use, including mounts stacked or placed beneath
// others. The visible mount's mountpoint must be the service root itself:
// if an ancestor is overmounted the path resolves on a different mount and
// writes would land on the wrong filesystem. Returns nil for a JuiceFS
// mount root, whose cache policy the caller must still verify.
func mountVerdict(visible mountInfoEntry, root string) error {
	if visible.mountpoint != root {
		return fmt.Errorf("%w: %s resolves on mount at %q, not a mountpoint",
			ErrMountUnavailable, root, visible.mountpoint)
	}
	if visible.fstype == "fuse.juicefs" {
		if visible.root != "/" {
			return fmt.Errorf("%w: JuiceFS root is a %q subdirectory bind; the service root must be the mount root",
				ErrMountPolicy, visible.root)
		}
		return nil
	}
	if localCoherentFS[visible.fstype] {
		return nil
	}
	return fmt.Errorf("%w: filesystem %q cannot prove metadata freshness",
		ErrMountPolicy, visible.fstype)
}

// mountCheckTimeout bounds the per-request mount check. A wedged-but-alive
// FUSE daemon can block statx/.config reads indefinitely; on timeout the
// request reports mount_unavailable. The abandoned goroutine holds no
// locks and exits when the daemon answers or dies — the same bounded-work
// shape as Store.runBounded.
const mountCheckTimeout = 5 * time.Second

// checkMount enforces the mount requirements for canonical-namespace mode
// (requireMount): the root must itself be a live mountpoint AND its
// filesystem must not serve stale metadata to this client. Freshness is
// verified on every op — no verdict is cached across mount instances,
// because neither st_dev nor the kernel mount ID is a safe generation key
// (both are recycled; a recycled ID was observed resurrecting a stale
// verdict and wedging a healthy remount into 503s).
//
// The mount serving p.root is identified with statx(STATX_MNT_ID): mountinfo
// order does not always reflect which mount a path lookup reaches (a mount
// placed beneath the top is listed last but hidden), so the ID is matched
// rather than the last mountpoint entry.
//
// A fuse.juicefs mount must be mounted at its root (not a subdirectory
// bind, where a forged regular .config could stand in for daemon metadata)
// and must prove zero metadata caching through the /.config control file.
// /.config is verified to be served by the same mount as the root (equal
// statx MNT_ID), so a file bind-mounted over it cannot substitute forged
// timeouts for the daemon-synthesized values. Other FUSE types are
// refused as unverifiable, as are network filesystems (nfs/cifs/etc.
// have their own client-side attribute caches we cannot inspect). Only
// known kernel-coherent local filesystems pass.
// Fail closed by default, because the CAS fingerprint gate reads through
// this mount and a nonzero attr cache silently re-opens the B1 clobber
// window.
func (p *posixRoot) checkMount() error {
	p.mcMu.Lock()
	ic := p.mcInflight
	if ic == nil {
		ic = &inflightMountCheck{done: make(chan struct{})}
		p.mcInflight = ic
		go func() {
			ic.err = p.checkMountInner()
			close(ic.done)
			p.mcMu.Lock()
			p.mcInflight = nil
			p.mcMu.Unlock()
		}()
	}
	p.mcMu.Unlock()
	select {
	case <-ic.done:
		return ic.err
	case <-time.After(mountCheckTimeout):
		return ErrMountUnavailable
	}
}

func (p *posixRoot) checkMountInner() error {
	var stx unix.Statx_t
	err := unix.Statx(unix.AT_FDCWD, p.root,
		unix.AT_STATX_DONT_SYNC, unix.STATX_MNT_ID, &stx)
	if err != nil || stx.Mask&unix.STATX_MNT_ID == 0 {
		// Includes ENOTCONN — a dead FUSE mount still resolves its root
		// dentry; the action is remount, so report unavailable.
		return ErrMountUnavailable
	}
	data, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return ErrMountUnavailable
	}
	visible, ok := findMount(parseMountInfo(data), stx.Mnt_id)
	if !ok {
		return ErrMountUnavailable
	}
	if err := mountVerdict(visible, p.root); err != nil {
		return err
	}
	if visible.fstype == "fuse.juicefs" {
		// The .config we inspect must be served by the same verified
		// mount — a file bind-mounted over it carries its own mount ID
		// and could present forged zero timeouts (whispering-cardboard
		// cfgbind finding).
		cfgPath := filepath.Join(p.root, ".config")
		var cstx unix.Statx_t
		if err := unix.Statx(unix.AT_FDCWD, cfgPath,
			unix.AT_STATX_DONT_SYNC, unix.STATX_MNT_ID, &cstx); err != nil {
			if errors.Is(err, unix.ENOTCONN) {
				return ErrMountUnavailable
			}
			return fmt.Errorf("%w: no verifiable mount config", ErrMountPolicy)
		}
		if cstx.Mask&unix.STATX_MNT_ID == 0 || cstx.Mnt_id != stx.Mnt_id {
			return fmt.Errorf("%w: .config is not served by the verified mount",
				ErrMountPolicy)
		}
		return checkZeroMetadataCache(cfgPath)
	}
	return nil
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
	ErrWrongKind        = errors.New("wrong kind for this operation")
	ErrAccess           = errors.New("permission denied")
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

// --- Descriptor-relative resolution ------------------------------------
//
// Every file operation resolves beneath a pinned directory file descriptor
// with openat2(RESOLVE_BENEATH): the kernel re-resolves each path
// component against the real mount table at use time, so a directory ↔
// symlink swap racing an operation can never redirect it outside the scope
// (the earlier stat-then-open-by-path check could be beaten — final-review
// B F1 reproduced escaped writes and staging files). A swap can still
// change *which in-scope object* an op targets, but cannot make it escape.

func mapPathErr(err error) error {
	switch {
	case errors.Is(err, unix.EXDEV), errors.Is(err, unix.EAGAIN):
		return ErrEscape
	case errors.Is(err, unix.ENOENT):
		return ErrNotFound
	case errors.Is(err, unix.ENOTDIR):
		return ErrNotDir
	case errors.Is(err, unix.ELOOP):
		return ErrNotFound // unresolvable (symlink loop)
	case errors.Is(err, unix.ENXIO):
		return ErrWrongKind // open of a socket or unopenable device
	case errors.Is(err, unix.ENOTCONN):
		return ErrMountUnavailable // dead FUSE mount
	case errors.Is(err, unix.EIO), errors.Is(err, unix.ECONNRESET), errors.Is(err, unix.ECONNABORTED):
		return ErrUnavailable // transient backend/FUSE hiccup — retryable
	case errors.Is(err, unix.EACCES), errors.Is(err, unix.EPERM):
		return ErrAccess
	default:
		return err
	}
}

// openBeneath resolves rel beneath dfd in-kernel. RESOLVE_BENEATH refuses
// any resolution that escapes dfd — escaping symlinks, magic links, or
// ".." past the root all fail with EXDEV/EAGAIN → ErrEscape.
func openBeneath(dfd *os.File, rel string, flags int, mode uint32) (*os.File, error) {
	how := unix.OpenHow{
		Flags:   uint64(flags) | unix.O_CLOEXEC,
		Mode:    uint64(mode),
		Resolve: unix.RESOLVE_BENEATH,
	}
	fd, err := unix.Openat2(int(dfd.Fd()), rel, &how)
	if err != nil {
		return nil, mapPathErr(err)
	}
	return os.NewFile(uintptr(fd), rel), nil
}

// rootFD anchors all scope resolution. O_NOFOLLOW pins it to the real
// directory — a post-start symlink swap at the root path cannot redirect.
func (p *posixRoot) rootFD() (*os.File, error) {
	fd, err := unix.Open(p.root,
		unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, mapPathErr(err)
	}
	return os.NewFile(uintptr(fd), p.root), nil
}

func validScope(scope string) error {
	if scope == "" || strings.Contains(scope, "/") || strings.Contains(scope, "..") || strings.HasPrefix(scope, ".") {
		return ErrEscape
	}
	return nil
}

// scopeDir opens the scope's root directory. With create, a missing scope
// dir is made first (first mutation of a scope). O_NOFOLLOW pins the scope
// name to a real directory: a scope-name symlink would alias one scope's
// tokens onto another scope's files (operation-review P2b/F7) — refused,
// while ordinary symlinks INSIDE the scope remain supported.
func (p *posixRoot) scopeDir(scope string, create bool) (*os.File, error) {
	if err := validScope(scope); err != nil {
		return nil, err
	}
	rfd, err := p.rootFD()
	if err != nil {
		return nil, err
	}
	defer rfd.Close()
	if create {
		err := unix.Mkdirat(int(rfd.Fd()), scope, 0o755)
		if err != nil && !errors.Is(err, unix.EEXIST) {
			return nil, mapPathErr(err)
		}
	}
	// O_RDONLY (not O_PATH): the scope fd is also used for readdir of the
	// scope root itself.
	return openBeneath(rfd, scope,
		unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW, 0)
}

// relPath normalizes a scope-relative path; ".." is rejected outright —
// callers get a deterministic error, not a clamp.
func relPath(path string) (string, error) {
	if path == "" || path == "/" {
		return "", nil
	}
	for _, seg := range strings.Split(path, "/") {
		if seg == ".." {
			return "", ErrEscape
		}
	}
	return filepath.Clean("/" + path)[1:], nil
}

// splitRel splits a normalized relative path into parent dir + final name.
func splitRel(rel string) (dir, name string) {
	dir, name = filepath.Split(rel)
	return strings.TrimSuffix(dir, "/"), name
}

// openDirBeneath resolves rel beneath sfd to a directory fd. With create,
// missing intermediate directories are made (mkdir -p semantics) — each
// segment is re-resolved in-kernel, so a racing symlink swap still cannot
// escape. The caller owns the returned fd; sfd is not consumed.
func openDirBeneath(sfd *os.File, rel string, create bool) (*os.File, error) {
	cur, err := openBeneath(sfd, ".", unix.O_PATH|unix.O_DIRECTORY, 0)
	if err != nil {
		return nil, err
	}
	for _, seg := range strings.Split(rel, "/") {
		if seg == "" {
			continue
		}
		next, err := openBeneath(cur, seg, unix.O_PATH|unix.O_DIRECTORY, 0)
		if err != nil && create && errors.Is(err, ErrNotFound) {
			merr := unix.Mkdirat(int(cur.Fd()), seg, 0o755)
			if merr != nil && !errors.Is(merr, unix.EEXIST) {
				cur.Close()
				return nil, mapPathErr(merr)
			}
			next, err = openBeneath(cur, seg, unix.O_PATH|unix.O_DIRECTORY, 0)
		}
		if err != nil {
			cur.Close()
			return nil, err
		}
		cur.Close()
		cur = next
	}
	return cur, nil
}

// syncDir fsyncs the directory behind dfd so a published name is durable.
func syncDir(dfd *os.File) {
	d, err := openBeneath(dfd, ".", unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		return
	}
	d.Sync()
	d.Close()
}

// unixToFileMode converts a raw unix mode word (S_IFMT type bits in the low
// range) to fs.FileMode (type bits in the high range). A raw cast leaves
// IsDir/IsRegular wrong — which misclassified every list entry and stopped
// sweepDir from recursing (operation-review B F2).
func unixToFileMode(m uint32) fs.FileMode {
	fm := fs.FileMode(m & 0o7777)
	switch m & unix.S_IFMT {
	case unix.S_IFDIR:
		fm |= fs.ModeDir
	case unix.S_IFLNK:
		fm |= fs.ModeSymlink
	case unix.S_IFIFO:
		fm |= fs.ModeNamedPipe
	case unix.S_IFSOCK:
		fm |= fs.ModeSocket
	case unix.S_IFBLK:
		fm |= fs.ModeDevice
	case unix.S_IFCHR:
		fm |= fs.ModeDevice | fs.ModeCharDevice
	}
	if m&unix.S_ISUID != 0 {
		fm |= fs.ModeSetuid
	}
	if m&unix.S_ISGID != 0 {
		fm |= fs.ModeSetgid
	}
	if m&unix.S_ISVTX != 0 {
		fm |= fs.ModeSticky
	}
	return fm
}

// statAt lstats a single name beneath dfd — no traversal, cannot escape.
func statAt(dfd *os.File, name string) (fs.FileMode, int64, int64, int64, error) {
	var st unix.Stat_t
	if err := unix.Fstatat(int(dfd.Fd()), name, &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return 0, 0, 0, 0, mapPathErr(err)
	}
	return unixToFileMode(st.Mode), st.Size,
		st.Mtim.Sec*1e9 + st.Mtim.Nsec, st.Ctim.Sec*1e9 + st.Ctim.Nsec, nil
}

type FileInfo struct {
	Kind    string `json:"kind"` // "file" | "dir" | "symlink" | "special"
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

func kindOf(mode fs.FileMode) string {
	switch {
	case mode.IsDir():
		return "dir"
	case mode&fs.ModeSymlink != 0:
		return "symlink"
	case !mode.IsRegular():
		return "special" // fifo, socket, device — reported, not served as file
	default:
		return "file"
	}
}

func (p *posixRoot) stat(scope, path string) (FileInfo, error) {
	sfd, err := p.scopeDir(scope, false)
	if err != nil {
		return FileInfo{}, err
	}
	defer sfd.Close()
	rel, err := relPath(path)
	if err != nil {
		return FileInfo{}, err
	}
	f := sfd
	if rel != "" {
		// O_PATH + RESOLVE_BENEATH: follows in-scope symlinks, refuses
		// any that escape — and cannot be raced, unlike the old
		// EvalSymlinks-then-open path.
		f, err = openBeneath(sfd, rel, unix.O_PATH, 0)
		if err != nil {
			return FileInfo{}, err
		}
		defer f.Close()
	}
	st, err := f.Stat()
	if err != nil {
		return FileInfo{}, mapPathErr(err)
	}
	return FileInfo{Kind: kindOf(st.Mode()), Size: st.Size(),
		MtimeNS: st.ModTime().UnixNano(), Fingerprint: fingerprint(st)}, nil
}

type ListEntry struct {
	Name    string `json:"name"`
	Kind    string `json:"kind"`
	Size    int64  `json:"size"`
	MtimeNS int64  `json:"mtime_ns"`
}

func (p *posixRoot) list(scope, path string, limit int, cursor string) ([]ListEntry, string, error) {
	sfd, err := p.scopeDir(scope, false)
	if err != nil {
		return nil, "", err
	}
	defer sfd.Close()
	rel, err := relPath(path)
	if err != nil {
		return nil, "", err
	}
	d := sfd
	if rel != "" {
		d, err = openBeneath(sfd, rel, unix.O_RDONLY|unix.O_DIRECTORY, 0)
		if err != nil {
			return nil, "", err
		}
		defer d.Close()
	}
	names, err := d.Readdirnames(-1)
	if err != nil {
		return nil, "", mapPathErr(err)
	}
	visible := names[:0]
	for _, n := range names {
		if strings.HasPrefix(n, stagingPrefix) {
			continue // hide service staging files
		}
		visible = append(visible, n)
	}
	sort.Strings(visible)
	start := 0
	if cursor != "" {
		start = sort.SearchStrings(visible, cursor)
		for start < len(visible) && visible[start] <= cursor {
			start++
		}
	}
	out := make([]ListEntry, 0, limit)
	next := ""
	for i := start; i < len(visible) && len(out) < limit; i++ {
		// Lstat via the pinned dir fd: a symlink's target may be outside
		// the scope — report the link, not its target's metadata.
		mode, size, mtim, _, err := statAt(d, visible[i])
		if err != nil {
			continue // raced delete — listing stays honest for what exists
		}
		out = append(out, ListEntry{Name: visible[i], Kind: kindOf(mode),
			Size: size, MtimeNS: mtim})
		next = visible[i]
	}
	if start+len(out) >= len(visible) {
		next = ""
	}
	return out, next, nil
}

// open returns the file positioned at off plus its metadata. The caller
// streams the body — no request-controlled allocation ever happens here.
// O_NONBLOCK is set for the open itself so a FIFO or device can never
// wedge the handler goroutine (final-review A F1); it is cleared by the
// fstat type check — only regular files are served.
func (p *posixRoot) open(scope, path string, off int64) (*os.File, FileInfo, error) {
	sfd, err := p.scopeDir(scope, false)
	if err != nil {
		return nil, FileInfo{}, err
	}
	defer sfd.Close()
	rel, err := relPath(path)
	if err != nil || rel == "" {
		return nil, FileInfo{}, ErrNotDir
	}
	f, err := openBeneath(sfd, rel, unix.O_RDONLY|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, FileInfo{}, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, FileInfo{}, mapPathErr(err)
	}
	if st.IsDir() {
		f.Close()
		return nil, FileInfo{}, ErrIsDir
	}
	if !st.Mode().IsRegular() {
		f.Close()
		return nil, FileInfo{}, ErrWrongKind
	}
	// Regular file: O_NONBLOCK has no effect on ordinary reads; the flag
	// only mattered to make the open itself non-wedging.
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
// exclusive=true publishes the staged file with linkat(2), which fails
// with EEXIST if anything already occupies the name — the create-only
// check is atomic against executor-side creates, not just advisory.
// Non-exclusive publish uses renameat2(2), which replaces whatever name
// exists (including a dangling symlink — A F3) atomically and entirely
// within the pinned parent directory.
func (p *posixRoot) atomicWrite(scope, path string, content []byte, exclusive bool) (FileInfo, error) {
	if err := checkReserved(path); err != nil {
		return FileInfo{}, err
	}
	rel, err := relPath(path)
	if err != nil {
		return FileInfo{}, err
	}
	if rel == "" {
		return FileInfo{}, ErrNotDir
	}
	sfd, err := p.scopeDir(scope, true)
	if err != nil {
		return FileInfo{}, err
	}
	defer sfd.Close()
	dirRel, name := splitRel(rel)
	pfd, err := openDirBeneath(sfd, dirRel, true)
	if err != nil {
		return FileInfo{}, err
	}
	defer pfd.Close()
	rnd := make([]byte, 8)
	if _, err := rand.Read(rnd); err != nil {
		return FileInfo{}, err
	}
	tmp := stagingPrefix + hex.EncodeToString(rnd)
	tf, err := openBeneath(pfd, tmp,
		unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW, 0o644)
	if err != nil {
		return FileInfo{}, err
	}
	if _, err := tf.Write(content); err != nil {
		tf.Close()
		unix.Unlinkat(int(pfd.Fd()), tmp, 0)
		return FileInfo{}, mapPathErr(err)
	}
	if err := tf.Sync(); err != nil {
		tf.Close()
		unix.Unlinkat(int(pfd.Fd()), tmp, 0)
		return FileInfo{}, mapPathErr(err)
	}
	if err := tf.Close(); err != nil {
		unix.Unlinkat(int(pfd.Fd()), tmp, 0)
		return FileInfo{}, mapPathErr(err)
	}
	if exclusive {
		err = unix.Linkat(int(pfd.Fd()), tmp, int(pfd.Fd()), name, 0)
	} else {
		err = unix.Renameat2(int(pfd.Fd()), tmp, int(pfd.Fd()), name, 0)
	}
	if err != nil {
		unix.Unlinkat(int(pfd.Fd()), tmp, 0)
		return FileInfo{}, mapPublishErr(err, exclusive)
	}
	unix.Unlinkat(int(pfd.Fd()), tmp, 0) // linkat leaves the staging name
	syncDir(pfd)
	return p.stat(scope, path)
}

// mapPublishErr translates the kernel's rename/link/linkat errors into
// contract errors. Distinct from mapPathErr: EEXIST and the kind-mismatch
// errnos carry API-meaningful semantics.
func mapPublishErr(err error, exclusive bool) error {
	switch {
	case errors.Is(err, unix.EEXIST):
		if exclusive {
			return ErrConflict
		}
		return ErrNotEmpty // rename dir over non-empty dir
	case errors.Is(err, unix.ENOTEMPTY):
		return ErrNotEmpty
	case errors.Is(err, unix.EISDIR), errors.Is(err, unix.ENOTDIR):
		return ErrWrongKind
	case errors.Is(err, unix.ENOENT):
		return ErrNotFound
	case errors.Is(err, unix.EINVAL):
		return ErrEscape // e.g. moving a dir beneath itself
	case errors.Is(err, unix.EACCES), errors.Is(err, unix.EPERM):
		return ErrAccess
	default:
		return err
	}
}

// rename moves (scope, from) to (scope, to) beneath the pinned scope dir;
// both parent directories are resolved in-kernel so neither endpoint can
// be redirected outside the scope mid-op. noReplace uses
// renameat2(RENAME_NOREPLACE); the plain form passes flags=0, which unlike
// os.Rename performs no userspace kind pre-check — the kernel handles
// dir-over-empty-dir, and returns typed errors for real mismatches.
func (p *posixRoot) rename(scope, from, to string, noReplace bool) (FileInfo, error) {
	if err := checkReserved(to); err != nil {
		return FileInfo{}, err
	}
	if err := checkReserved(from); err != nil {
		return FileInfo{}, err
	}
	relFrom, err := relPath(from)
	if err != nil {
		return FileInfo{}, err
	}
	relTo, err := relPath(to)
	if err != nil {
		return FileInfo{}, err
	}
	if relFrom == "" || relTo == "" {
		return FileInfo{}, ErrNotDir
	}
	if relTo == relFrom || strings.HasPrefix(relTo, relFrom+"/") {
		return FileInfo{}, ErrEscape // cannot move a dir beneath itself
	}
	sfd, err := p.scopeDir(scope, false)
	if err != nil {
		return FileInfo{}, err
	}
	defer sfd.Close()
	srcDir, srcName := splitRel(relFrom)
	dstDir, dstName := splitRel(relTo)
	srcPfd, err := openDirBeneath(sfd, srcDir, false)
	if err != nil {
		return FileInfo{}, err
	}
	defer srcPfd.Close()
	dstPfd, err := openDirBeneath(sfd, dstDir, false)
	if err != nil {
		return FileInfo{}, err
	}
	defer dstPfd.Close()
	if _, _, _, _, err := statAt(srcPfd, srcName); err != nil {
		return FileInfo{}, err
	}
	var flags uint
	if noReplace {
		flags = unix.RENAME_NOREPLACE
	}
	err = unix.Renameat2(int(srcPfd.Fd()), srcName, int(dstPfd.Fd()), dstName, flags)
	if err != nil {
		return FileInfo{}, mapPublishErr(err, noReplace)
	}
	syncDir(dstPfd)
	return p.stat(scope, to)
}

func (p *posixRoot) remove(scope, path string) error {
	rel, err := relPath(path)
	if err != nil {
		return err
	}
	if rel == "" {
		return ErrNotDir // removing the scope root itself is not an op
	}
	sfd, err := p.scopeDir(scope, false)
	if err != nil {
		return err
	}
	defer sfd.Close()
	dirRel, name := splitRel(rel)
	pfd, err := openDirBeneath(sfd, dirRel, false)
	if err != nil {
		return err
	}
	defer pfd.Close()
	err = unix.Unlinkat(int(pfd.Fd()), name, 0)
	if errors.Is(err, unix.EISDIR) {
		err = unix.Unlinkat(int(pfd.Fd()), name, unix.AT_REMOVEDIR)
	}
	if err != nil {
		switch {
		case errors.Is(err, unix.ENOENT):
			return ErrNotFound
		case errors.Is(err, unix.ENOTEMPTY), errors.Is(err, unix.EEXIST):
			return ErrNotEmpty
		default:
			return mapPathErr(err)
		}
	}
	syncDir(pfd)
	return nil
}

func (p *posixRoot) mkdir(scope, path string) (FileInfo, error) {
	if err := checkReserved(path); err != nil {
		return FileInfo{}, err
	}
	rel, err := relPath(path)
	if err != nil {
		return FileInfo{}, err
	}
	sfd, err := p.scopeDir(scope, true)
	if err != nil {
		return FileInfo{}, err
	}
	defer sfd.Close()
	pfd, err := openDirBeneath(sfd, rel, true)
	if err != nil {
		return FileInfo{}, err
	}
	pfd.Close()
	syncDir(sfd)
	return p.stat(scope, path)
}

// sweepStaging removes service staging files older than 10 minutes — e.g.
// left behind by a SIGKILL mid-write. Run at startup and periodically.
// Safe while the service is live only because the prefix is reserved.
// The walk stays fd-relative, so even this background cleanup cannot be
// redirected outside the root by a racing symlink swap.
func (p *posixRoot) sweepStaging() {
	rfd, err := p.rootFD()
	if err != nil {
		return
	}
	defer rfd.Close()
	d, err := openBeneath(rfd, ".", unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		return
	}
	names, err := d.Readdirnames(-1)
	d.Close()
	if err != nil {
		return
	}
	cutoff := time.Now().Add(-10 * time.Minute)
	for _, sc := range names {
		sd, err := openBeneath(rfd, sc, unix.O_RDONLY|unix.O_DIRECTORY, 0)
		if err != nil {
			continue
		}
		sweepDir(sd, cutoff)
	}
}

func sweepDir(dfd *os.File, cutoff time.Time) {
	defer dfd.Close()
	names, err := dfd.Readdirnames(-1)
	if err != nil {
		return
	}
	for _, n := range names {
		mode, _, mtim, _, err := statAt(dfd, n)
		if err != nil {
			continue
		}
		if mode.IsDir() {
			sub, err := openBeneath(dfd, n, unix.O_RDONLY|unix.O_DIRECTORY, 0)
			if err != nil {
				continue
			}
			sweepDir(sub, cutoff)
			continue
		}
		if strings.HasPrefix(n, stagingPrefix) &&
			time.Unix(0, mtim).Before(cutoff) {
			unix.Unlinkat(int(dfd.Fd()), n, 0)
		}
	}
}
