package filesvc

import (
	"crypto/rand"
	"crypto/sha256"
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

	// faultHook, when non-nil (tests only), runs between a verified
	// exchange's post-verify and the displaced-object discard — the
	// window a paused actor resumes into after ownership loss.
	faultHook func(tag string)
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
	return checkZeroMetadataCacheData(cfg)
}

func checkZeroMetadataCacheData(cfg []byte) error {
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

// opStagePrefix names per-intent recovery objects: the staged content or a
// displaced foreign object parked while an op's verification decides.
// Deterministic (one per intent id) so the reconciler can find leftovers.
const opStagePrefix = ".filesv-op-"

func checkReserved(path string) error {
	for _, seg := range strings.Split(path, "/") {
		if strings.HasPrefix(seg, stagingPrefix) || strings.HasPrefix(seg, opStagePrefix) {
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
	rfd, err := p.rootFD()
	if err != nil {
		return nil, err
	}
	defer rfd.Close()
	return scopeDirFrom(rfd, scope, create)
}

// scopeDirFrom resolves the scope dir beneath an already-open root fd —
// the pinned-view variant used by the reconciler so a mid-pass unmount
// reports ENOTCONN on the dead mount instead of ENOENT on the bare
// directory left behind (F-RA-1).
func scopeDirFrom(rfd *os.File, scope string, create bool) (*os.File, error) {
	if err := validScope(scope); err != nil {
		return nil, err
	}
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

// fp3 is the fingerprint's identity triple (ino:size:mtime). ctime is
// excluded: every rename relink updates it, so an object that was moved
// by an effect can never match a declare-time fp on all four fields.
func fp3(fp string) string {
	i := strings.LastIndex(fp, ":")
	if i < 0 {
		return fp
	}
	return fp[:i]
}

// fp3at lstats a name beneath dfd and returns its identity triple and
// ctime (the recency tiebreak used when deciding which of two foreign
// objects keeps a contested name).
func fp3at(dfd *os.File, name string) (string, fs.FileMode, int64, error) {
	var st unix.Stat_t
	if err := unix.Fstatat(int(dfd.Fd()), name, &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return "", 0, 0, mapPathErr(err)
	}
	return fmt.Sprintf("%d:%d:%d", st.Ino, st.Size, st.Mtim.Sec*1e9+st.Mtim.Nsec),
		unixToFileMode(st.Mode), st.Ctim.Sec*1e9 + st.Ctim.Nsec, nil
}

// errUndoParked marks a verification undo that could not cleanly restore
// the pre-effect shape because a racing writer occupied the name again:
// the newest foreign object keeps the path and the older foreign object
// stays parked at the intent's staging name for the reconciler. Foreign
// bytes are never unlinked — only objects the op itself created.
var errUndoParked = errors.New("undo parked a foreign object")

// errVerifyUnobserved: a verified effect committed its exchange but the
// displaced object could not be inspected — outcome is committed with
// unknown shape; the reconciler settles it.
var errVerifyUnobserved = errors.New("post-exchange observation failed")

// randHex returns n random bytes hex-encoded (2n chars) — used for
// staging and per-call quarantine names.
func randHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err) // crypto/rand failure is unrecoverable
	}
	return hex.EncodeToString(b)
}

// isSealedName reports whether base is a quarantine name: an
// opStage-prefixed name carrying "-q-". Sealed names are a structural
// invariant — once created, NOTHING may write into them (no exchange,
// no rename target, no link target); they may only be moved out of or
// unlinked. That is what makes verify-then-unlink on a sealed name
// race-free: enumerating the name (every reconcile pass does) grants
// no write authority over it.
func isSealedName(base string) bool {
	return strings.HasPrefix(base, opStagePrefix) && strings.Contains(base, "-q-")
}

// removeStagedPreUnlinkHook is a test-only seam marking the gap between
// a sealed re-verify and the unlink — the position a retired actor's
// delayed syscall would land in. Production never sets it.
var removeStagedPreUnlinkHook func()

// inoAt returns the inode of name beneath dfd (lstat semantics).
func inoAt(dfd *os.File, name string) (uint64, error) {
	var st unix.Stat_t
	if err := unix.Fstatat(int(dfd.Fd()), name, &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return 0, mapPathErr(err)
	}
	return st.Ino, nil
}

// quarantineDeleteMatch deletes the object at name beneath pfd only
// after re-proving its identity at a sealed name. The object is
// captured to qname (created by this call, so no delayed syscall can
// have it as a pending target, and sealed so nothing may write into it
// once it exists), the match predicate re-run there, and the unlink
// issued only on a match. A mismatched capture stays parked at the
// sealed name — preserved bytes for the reconciler — and ErrConflict
// is returned along with the qname so the caller can restore it if it
// wants. ENOENT at name reports success: already-gone is the desired
// end state, and once the name is gone a delayed exchange into it can
// only ENOENT.
func quarantineDeleteMatch(pfd *os.File, name string, match func(qname string) (bool, error)) (string, error) {
	qname := name
	if !isSealedName(name) {
		if strings.HasPrefix(name, opStagePrefix) {
			qname = name + "-q-" + randHex(6)
		} else {
			// A non-intent staging name: quarantine under the
			// never-swept prefix so a parked foreign object can't be
			// collected by the .filesv-tmp- sweep.
			qname = opStagePrefix + "q-" + randHex(8)
		}
		if err := unix.Renameat2(int(pfd.Fd()), name,
			int(pfd.Fd()), qname, unix.RENAME_NOREPLACE); err != nil {
			if errors.Is(err, unix.ENOENT) {
				return qname, nil
			}
			return qname, mapPathErr(err)
		}
	}
	ok, err := match(qname)
	if err != nil {
		return qname, err
	}
	if !ok {
		return qname, ErrConflict
	}
	if removeStagedPreUnlinkHook != nil {
		removeStagedPreUnlinkHook()
	}
	uerr := unix.Unlinkat(int(pfd.Fd()), qname, 0)
	if errors.Is(uerr, unix.EISDIR) {
		uerr = unix.Unlinkat(int(pfd.Fd()), qname, unix.AT_REMOVEDIR)
	}
	if uerr != nil && !errors.Is(uerr, unix.ENOENT) {
		return qname, mapPathErr(uerr)
	}
	return qname, nil
}

// quarantineDelete is quarantineDeleteMatch with an fp3 identity check.
func quarantineDelete(pfd *os.File, name, want3 string) (string, error) {
	return quarantineDeleteMatch(pfd, name, func(qname string) (bool, error) {
		q3, _, _, err := fp3at(pfd, qname)
		if err != nil {
			return false, err
		}
		return q3 == want3, nil
	})
}

// discardStaged removes the staged file at tmp only while it is still
// the inode this call created — a delayed exchange decided earlier could
// land different bytes at the enumerable slot name between the create
// and this cleanup; an inode mismatch parks the foreign object at the
// sealed name for the reconciler instead of deleting it. A non-nil
// return means a residual may remain at the slot or sealed name: callers
// must keep the intent (errUndoParked) so the parked object stays
// enumerable, rather than letting a definitive error drop it.
func discardStaged(pfd *os.File, tmp string, ino uint64) error {
	_, err := quarantineDeleteMatch(pfd, tmp, func(qname string) (bool, error) {
		qino, err := inoAt(pfd, qname)
		if err != nil {
			return false, err
		}
		return qino == ino, nil
	})
	return err
}

// undoDisplaced reverses a committed exchange after verification found
// the displaced object was not the declared expectation. It repeatedly
// exchanges (sname under sfd) with (dname under dfd) until the op's own
// object is back at sname (nil), or gives up with errUndoParked when
// both names hold foreign objects. park, when non-empty, moves the
// foreign object at sname to that recovery name in the same directory
// before returning.
//
// When both endpoints are foreign, ctime picks which keeps the public
// name. That is a convergence hint only — a rename changes ctime and
// the clock can step, so it is NOT an acknowledged-version ordering
// proof. Byte preservation is the guarantee here: both foreign objects
// survive (one at dname, one parked). If the hint leaves a stale object
// on the name, the reconciler's row-fp rule (file_version.fp is the
// acknowledged fingerprint) swaps the recorded content back. Bounded:
// each iteration either restores our object, leaves one foreign object
// at dname, or gives up honestly.
func undoDisplaced(sfd, dfd *os.File, sname, dname, park string, ours func(string) bool) error {
	for i := 0; i < 4; i++ {
		s3, _, _, serr := fp3at(sfd, sname)
		if serr == nil && ours(s3) {
			return nil // our object is back at the staging name
		}
		d3, _, dCtim, derr := fp3at(dfd, dname)
		switch {
		case errors.Is(derr, unix.ENOENT):
			// The name is empty — return whatever is staged to it.
			if rerr := unix.Renameat2(int(sfd.Fd()), sname,
				int(dfd.Fd()), dname, unix.RENAME_NOREPLACE); rerr != nil {
				return errUndoParked
			}
		case derr != nil:
			return derr
		case ours(d3):
			// Our object still occupies the name; swap the staged foreign
			// object back onto it.
			if xerr := unix.Renameat2(int(sfd.Fd()), sname,
				int(dfd.Fd()), dname, unix.RENAME_EXCHANGE); xerr != nil {
				return errUndoParked
			}
		default:
			// Both endpoints hold foreign objects (a racing writer landed
			// after our verify). The later-ctime object keeps the name
			// (hint only — the reconciler's row-fp rule corrects a wrong
			// guess); the other is preserved at the staging name — never
			// unlinked.
			_, _, sCtim, _ := fp3at(sfd, sname)
			if sCtim > dCtim {
				if xerr := unix.Renameat2(int(sfd.Fd()), sname,
					int(dfd.Fd()), dname, unix.RENAME_EXCHANGE); xerr != nil {
					return errUndoParked
				}
				continue
			}
			if park != "" {
				unix.Renameat2(int(sfd.Fd()), sname,
					int(sfd.Fd()), park, unix.RENAME_NOREPLACE)
			}
			return errUndoParked
		}
	}
	return errUndoParked
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
	rfd, err := p.rootFD()
	if err != nil {
		return FileInfo{}, err
	}
	defer rfd.Close()
	return statFrom(rfd, scope, path)
}

// lstat fingerprints the object AT the path — a symlink itself, never its
// target. Intent evidence (pre_fp/dst_fp) must describe the object an
// effect would displace; effects operate on links, not their targets.
func (p *posixRoot) lstat(scope, path string) (FileInfo, error) {
	rfd, err := p.rootFD()
	if err != nil {
		return FileInfo{}, err
	}
	defer rfd.Close()
	sfd, err := scopeDirFrom(rfd, scope, false)
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
		f, err = openBeneath(sfd, rel, unix.O_PATH|unix.O_NOFOLLOW, 0)
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

// statFrom stats beneath a pinned root fd — the reconciler's view so an
// "absent" verdict is bound to the filesystem the pass verified, never
// to a bare directory left behind by a mid-pass unmount.
func statFrom(rfd *os.File, scope, path string) (FileInfo, error) {
	sfd, err := scopeDirFrom(rfd, scope, false)
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
		if strings.HasPrefix(n, stagingPrefix) || strings.HasPrefix(n, opStagePrefix) {
			continue // hide service staging/recovery objects
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
	rfd, err := p.rootFD()
	if err != nil {
		return nil, FileInfo{}, err
	}
	defer rfd.Close()
	return openFrom(rfd, scope, path, off)
}

func openFrom(rfd *os.File, scope, path string, off int64) (*os.File, FileInfo, error) {
	sfd, err := scopeDirFrom(rfd, scope, false)
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

// hash returns the sha256 hex of a scope-relative regular file — the
// reconciler compares it against an intent's recorded expectation to
// detect content that is not what the service wrote.
func (p *posixRoot) hash(scope, path string) (string, error) {
	rfd, err := p.rootFD()
	if err != nil {
		return "", err
	}
	defer rfd.Close()
	return hashFrom(rfd, scope, path)
}

func hashFrom(rfd *os.File, scope, path string) (string, error) {
	f, _, err := openFrom(rfd, scope, path, 0)
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

// rootView is a reconcile-pass view of the filesystem pinned to one open
// root descriptor. Every judgment the pass makes resolves beneath this
// fd: if the canonical mount is unmounted mid-pass, fd-relative ops fail
// ENOTCONN on the dead mount — an honest "unverifiable" — instead of
// silently reading the bare directory the mountpoint leaves behind
// (F-RA-1).
type rootView struct {
	rfd *os.File
}

// pin anchors a reconcile pass to the filesystem mounted at the canonical
// root. With requireMount the OPENED DESCRIPTOR's mount identity is
// verified (statx AT_EMPTY_PATH mount ID → mountinfo → verdict → JuiceFS
// .config resolved beneath the same fd): verifying the path and then
// opening it would leave a race where an intervening unmount pins the
// bare directory, which answers "absent" for everything — exactly the
// F-RA-1 defect. After a successful pin the fd either keeps answering on
// a live mount or fails ENOTCONN on the dead one; it can never observe
// the post-unmount placeholder.
func (p *posixRoot) pin(requireMount bool) (*rootView, error) {
	rfd, err := p.rootFD()
	if err != nil {
		return nil, err
	}
	if requireMount {
		if err := p.verifyPinnedMount(rfd); err != nil {
			rfd.Close()
			return nil, err
		}
	}
	return &rootView{rfd: rfd}, nil
}

// verifyPinnedMount is checkMountInner re-expressed against the pinned
// descriptor itself: the mount ID the fd is actually attached to, looked
// up in mountinfo, must be a mountpoint AT the service root of an
// accepted fstype; a JuiceFS mount must additionally serve its zero-cache
// .config beneath this very fd.
func (p *posixRoot) verifyPinnedMount(rfd *os.File) error {
	var stx unix.Statx_t
	if err := unix.Statx(int(rfd.Fd()), "",
		unix.AT_EMPTY_PATH, unix.STATX_MNT_ID, &stx); err != nil ||
		stx.Mask&unix.STATX_MNT_ID == 0 {
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
		cfg, err := openBeneath(rfd, ".config", unix.O_RDONLY|unix.O_NOFOLLOW, 0)
		if err != nil {
			if errors.Is(err, ErrMountUnavailable) {
				return err
			}
			return fmt.Errorf("%w: no verifiable mount config", ErrMountPolicy)
		}
		defer cfg.Close()
		var cstx unix.Statx_t
		if err := unix.Statx(int(cfg.Fd()), "",
			unix.AT_EMPTY_PATH, unix.STATX_MNT_ID, &cstx); err != nil ||
			cstx.Mask&unix.STATX_MNT_ID == 0 || cstx.Mnt_id != stx.Mnt_id {
			return fmt.Errorf("%w: .config is not served by the verified mount",
				ErrMountPolicy)
		}
		content, err := io.ReadAll(cfg)
		if err != nil {
			return fmt.Errorf("%w: no verifiable mount config", ErrMountPolicy)
		}
		return checkZeroMetadataCacheData(content)
	}
	return nil
}

func (v *rootView) Stat(scope, path string) (FileInfo, error) {
	return statFrom(v.rfd, scope, path)
}

func (v *rootView) Hash(scope, path string) (string, error) {
	return hashFrom(v.rfd, scope, path)
}

// MoveStaged restores a recovery object to an empty name, resolved
// fd-relative beneath the pinned root. NOREPLACE means it can never
// overwrite a name a racing writer claimed between the verdict and here.
// The destination may never be a sealed quarantine name: sealing means
// nothing — not even a delayed reconcile pass — writes into it.
func (v *rootView) MoveStaged(scope, from, to string) error {
	sfd, err := scopeDirFrom(v.rfd, scope, false)
	if err != nil {
		return err
	}
	defer sfd.Close()
	relFrom, err := relPath(from)
	if err != nil {
		return err
	}
	relTo, err := relPath(to)
	if err != nil {
		return err
	}
	fromDir, fromName := splitRel(relFrom)
	toDir, toName := splitRel(relTo)
	if isSealedName(toName) {
		return ErrReserved
	}
	fpfd, err := openDirBeneath(sfd, fromDir, false)
	if err != nil {
		return err
	}
	defer fpfd.Close()
	tpfd, err := openDirBeneath(sfd, toDir, false)
	if err != nil {
		return err
	}
	defer tpfd.Close()
	if err := unix.Renameat2(int(fpfd.Fd()), fromName,
		int(tpfd.Fd()), toName, unix.RENAME_NOREPLACE); err != nil {
		return mapPathErr(err)
	}
	syncDir(tpfd)
	return nil
}

// SwapStaged exchanges the staged recovery object with whatever the name
// holds — the dead op's undone exchange continued under the successor.
// Whatever was at the name lands back at the staging slot, where the
// caller inspects it: own staged bytes are discarded, foreign objects
// stay parked. Nothing is unlinked here.
func (v *rootView) SwapStaged(scope, staged, name string) error {
	sfd, err := scopeDirFrom(v.rfd, scope, false)
	if err != nil {
		return err
	}
	defer sfd.Close()
	relS, err := relPath(staged)
	if err != nil {
		return err
	}
	relN, err := relPath(name)
	if err != nil {
		return err
	}
	sDir, sName := splitRel(relS)
	nDir, nName := splitRel(relN)
	if isSealedName(sName) || isSealedName(nName) {
		// The exchange writes into BOTH names — a sealed quarantine
		// name can never be a write target on either side, or the
		// verify→unlink of a pending delete could land on bytes nobody
		// checked.
		return ErrReserved
	}
	spfd, err := openDirBeneath(sfd, sDir, false)
	if err != nil {
		return err
	}
	defer spfd.Close()
	npfd, err := openDirBeneath(sfd, nDir, false)
	if err != nil {
		return err
	}
	defer npfd.Close()
	if err := unix.Renameat2(int(spfd.Fd()), sName,
		int(npfd.Fd()), nName, unix.RENAME_EXCHANGE); err != nil {
		return mapPathErr(err)
	}
	syncDir(npfd)
	return nil
}

// RemoveStaged deletes a recovery object beneath the pinned root only
// after re-proving its identity at a sealed quarantine name. A live
// retired actor may still exchange into the intent's staging slot
// between the caller's inspection and the unlink, so the object is
// first moved to a per-call sealed name — created by this call, and
// unreachable as a write target by construction — re-verified, and
// unlinked there. A mismatched capture stays parked at the sealed
// name — bytes are preserved for a later pass rather than destroyed.
func (v *rootView) RemoveStaged(scope, path, wantFP3, wantSHA string) error {
	return v.RemoveStagedVeto(scope, path, wantFP3, wantSHA, nil)
}

func (v *rootView) RemoveStagedVeto(scope, path, wantFP3, wantSHA string,
	veto func(captured FileInfo) (bool, error)) error {
	sfd, err := scopeDirFrom(v.rfd, scope, false)
	if err != nil {
		return err
	}
	defer sfd.Close()
	rel, err := relPath(path)
	if err != nil {
		return err
	}
	dirRel, name := splitRel(rel)
	pfd, err := openDirBeneath(sfd, dirRel, false)
	if err != nil {
		return err
	}
	defer pfd.Close()
	qname := name
	if !isSealedName(name) {
		// Two-hop: move to a sealed name created by this call. Whoever
		// occupied `path` at move time is captured — possibly not the
		// object the caller verified, which is why it is re-verified
		// below. Sealing means no delayed syscall can write into qname:
		// it did not exist when any retired effect was issued, and the
		// writer checks reject it as a target once it exists.
		qname = name + "-q-" + randHex(6)
		if err := unix.Renameat2(int(pfd.Fd()), name,
			int(pfd.Fd()), qname, unix.RENAME_NOREPLACE); err != nil {
			if errors.Is(err, unix.ENOENT) {
				return nil // already gone — desired end state
			}
			return mapPathErr(err)
		}
	}
	qrel := qname
	if dirRel != "" {
		qrel = dirRel + "/" + qname
	}
	st, serr := statFrom(v.rfd, scope, qrel)
	if serr != nil {
		return mapPathErr(serr)
	}
	match := false
	if wantFP3 != "" {
		match = fp3(st.Fingerprint) == wantFP3
	} else if wantSHA != "" && st.Kind == "file" {
		h, herr := hashFrom(v.rfd, scope, qrel)
		match = herr == nil && h == wantSHA
	}
	if !match {
		// Captured something other than the verified object — leave it
		// parked at the sealed quarantine name.
		return ErrConflict
	}
	if veto != nil {
		// The object is captured and immobilized at the sealed name —
		// nothing can write into it and a move-out leaves it empty for
		// the unlink below. The veto is evaluated at this effect-time
		// point, on the CAPTURED object's identity (a hash-only match can
		// capture a different inode than the caller verified), so a check
		// consulting durable state cannot be invalidated by a delayed
		// syscall decided before the state changed.
		keep, verr := veto(st)
		if verr != nil {
			return verr
		}
		if keep {
			return ErrConflict
		}
	}
	if removeStagedPreUnlinkHook != nil {
		removeStagedPreUnlinkHook()
	}
	err = unix.Unlinkat(int(pfd.Fd()), qname, 0)
	if errors.Is(err, unix.EISDIR) {
		err = unix.Unlinkat(int(pfd.Fd()), qname, unix.AT_REMOVEDIR)
	}
	if err != nil && !errors.Is(err, unix.ENOENT) {
		return mapPathErr(err)
	}
	syncDir(pfd)
	return nil
}

// ListStaged returns the base names in dir that begin with prefix,
// resolved beneath the pinned root.
func (v *rootView) ListStaged(scope, dir, prefix string) ([]string, error) {
	sfd, err := scopeDirFrom(v.rfd, scope, false)
	if err != nil {
		return nil, err
	}
	defer sfd.Close()
	rel, err := relPath(dir)
	if err != nil {
		return nil, err
	}
	dfd := sfd
	if rel != "" {
		dfd, err = openBeneath(sfd, rel, unix.O_RDONLY|unix.O_DIRECTORY, 0)
		if err != nil {
			return nil, err
		}
		defer dfd.Close()
	}
	names, err := dfd.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, n := range names {
		if strings.HasPrefix(n, prefix) {
			out = append(out, n)
		}
	}
	return out, nil
}

func (v *rootView) Close() error { return v.rfd.Close() }

// atomicWrite stages content to a temp sibling, fsyncs, renames over the
// target, and fsyncs the directory. A name never resolves to torn content.
// Caller holds the version CAS; this is the durable part of the write.
// exclusive=true publishes the staged file with linkat(2), which fails
// with EEXIST if anything already occupies the name — the create-only
// check is atomic against executor-side creates, not just advisory.
// Non-exclusive publish uses renameat2(2), which replaces whatever name
// exists (including a dangling symlink — A F3) atomically and entirely
// within the pinned parent directory.
// The bool result reports whether the commit point was REACHED — the
// publish call (linkat/renameat2) succeeded. A trailing stat error after
// that means "the write landed but we could not observe the result"; the
// caller must preserve the intent for reconciliation rather than drop it
// as never-committed (F-RA-5/f120).
// atomicWrite stages the body at a deterministic per-intent name, then
// publishes. A non-exclusive publish is VERIFIED: RENAME_EXCHANGE moves
// whatever the path held into the staging slot, and the displaced
// object's identity must equal expectFP — the fingerprint observed at
// declare. Only the expected object is discarded; a foreign object is
// restored to the name (or parked at the staging slot if a racing writer
// claimed it). This is what prevents a stale in-flight write — issued
// before ownership was lost, completing after a successor's save — from
// destroying newer acknowledged bytes. An effect interrupted between the
// exchange and the verdict leaves both objects recoverable for the
// reconciler.
// The bool result reports whether the fs effect committed — an error
// after that point means "landed but unobserved", not "never ran"; the
// caller must preserve the intent for reconciliation rather than drop it
// as never-committed (F-RA-5/f120).
func (p *posixRoot) atomicWrite(scope, path string, content []byte, exclusive bool, expectFP, stage string) (FileInfo, bool, error) {
	if err := checkReserved(path); err != nil {
		return FileInfo{}, false, err
	}
	rel, err := relPath(path)
	if err != nil {
		return FileInfo{}, false, err
	}
	if rel == "" {
		return FileInfo{}, false, ErrNotDir
	}
	sfd, err := p.scopeDir(scope, true)
	if err != nil {
		return FileInfo{}, false, err
	}
	defer sfd.Close()
	dirRel, name := splitRel(rel)
	pfd, err := openDirBeneath(sfd, dirRel, true)
	if err != nil {
		return FileInfo{}, false, err
	}
	defer pfd.Close()
	tmp := stage
	park := ""
	if tmp == "" {
		tmp = stagingPrefix + randHex(8)
		// A foreign object the undo cannot restore must not sit at a
		// .filesv-tmp- name — the staging sweep would delete it. Give the
		// park slot the never-swept recovery prefix.
		park = opStagePrefix + "adhoc-" + randHex(8)
	}
	tf, err := openBeneath(pfd, tmp,
		unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW, 0o644)
	if err != nil {
		return FileInfo{}, false, err
	}
	// The inode binds every later cleanup of this name to THIS object:
	// a retired pass's delayed exchange can land foreign bytes at the
	// enumerable slot between create and cleanup, and an inode-mismatched
	// capture is parked rather than deleted.
	ourIno, _ := inoAt(pfd, tmp)
	if p.faultHook != nil {
		p.faultHook("write.postCreate")
	}
	// fail returns a pre-commit error after best-effort cleanup of the
	// staged file. If the cleanup left a residual — a foreign object
	// parked under a sealed name, or the slot still occupied — the intent
	// must be tombstoned, not dropped: only a live intent keeps that
	// evidence enumerable for the reconciler.
	fail := func(perr error) (FileInfo, bool, error) {
		if derr := discardStaged(pfd, tmp, ourIno); derr != nil {
			return FileInfo{}, false, fmt.Errorf("%w: %w", perr, errUndoParked)
		}
		return FileInfo{}, false, perr
	}
	if _, err := tf.Write(content); err != nil {
		tf.Close()
		return fail(mapPathErr(err))
	}
	if err := tf.Sync(); err != nil {
		tf.Close()
		return fail(mapPathErr(err))
	}
	if err := tf.Close(); err != nil {
		return fail(mapPathErr(err))
	}
	// Our object's identity before any exchange — ctime shifts on relink,
	// so identity is the ino:size:mtime triple only.
	our3, _, _, ourErr := fp3at(pfd, tmp)
	// commit deletes the staging name's object, fsyncs, and stats the
	// result. The slot is enumerable by every reconcile pass and
	// reachable by swaps a retired pass decided while the name held
	// these bytes — so the delete goes through quarantineDelete:
	// capture to a sealed name nothing can write into, re-verify
	// identity, unlink only what we meant to. want3 is the fp3 the
	// caller expects at the slot (our staged bytes, or the declared
	// displaced object). A foreign capture stays parked at the sealed
	// name: the fs commit stands but the intent must survive for the
	// reconciler to settle the parked object.
	commit := func(want3 string) (FileInfo, bool, error) {
		if _, derr := quarantineDelete(pfd, tmp, want3); derr != nil {
			if errors.Is(derr, ErrConflict) {
				return FileInfo{}, true, fmt.Errorf("%w: %w", ErrExternalChange, errUndoParked)
			}
			return FileInfo{}, true, derr
		}
		syncDir(pfd)
		info, serr := p.stat(scope, path)
		if serr != nil {
			// The publish committed; only the observation failed.
			return FileInfo{}, true, serr
		}
		return info, true, nil
	}
	if exclusive {
		if err := unix.Linkat(int(pfd.Fd()), tmp, int(pfd.Fd()), name, 0); err != nil {
			return fail(mapPublishErr(err, exclusive))
		}
		return commit(our3)
	}
	err = unix.Renameat2(int(pfd.Fd()), tmp, int(pfd.Fd()), name, unix.RENAME_EXCHANGE)
	switch {
	case errors.Is(err, unix.ENOENT):
		// The name is empty.
		if expectFP != "" {
			// The object the intent expected to displace is gone.
			return fail(ErrExternalChange)
		}
		if perr := unix.Renameat2(int(pfd.Fd()), tmp,
			int(pfd.Fd()), name, unix.RENAME_NOREPLACE); perr != nil {
			if errors.Is(perr, unix.EEXIST) {
				// A foreign object claimed the empty name — do not
				// overwrite it.
				return fail(ErrExternalChange)
			}
			return fail(mapPublishErr(perr, exclusive))
		}
		return commit(our3)
	case err != nil:
		return fail(mapPublishErr(err, exclusive))
	}
	// Exchanged: tmp now holds the object the name used to hold.
	d3, dMode, _, serr := fp3at(pfd, tmp)
	if serr != nil {
		// Displaced object unobservable — the effect committed; let the
		// reconciler settle it rather than guess here.
		return FileInfo{}, true, serr
	}
	if d3 == fp3(expectFP) && kindOf(dMode) != "dir" {
		return commit(fp3(expectFP)) // displaced exactly the expected object
	}
	// The displaced object is not what the intent declared — undo the
	// exchange without ever unlinking foreign bytes.
	if ourErr != nil {
		return FileInfo{}, true, ourErr
	}
	if p.faultHook != nil {
		p.faultHook("write.preUndo")
	}
	uerr := undoDisplaced(pfd, pfd, tmp, name, park,
		func(t3 string) bool { return t3 == our3 })
	if p.faultHook != nil {
		p.faultHook("write.postUndo")
	}
	if uerr == nil {
		// tmp holds our own staged bytes — but the slot name stayed
		// enumerable throughout the undo, so a retired pass's delayed
		// swap may have replaced them since undoDisplaced returned.
		// Capture to a sealed name and re-verify before deleting.
		if _, derr := quarantineDelete(pfd, tmp, our3); derr != nil {
			if errors.Is(derr, ErrConflict) {
				// Whatever landed at the slot is foreign and now
				// parked; the undo itself already restored the name.
				return FileInfo{}, false, fmt.Errorf("%w: %w", ErrExternalChange, errUndoParked)
			}
			return FileInfo{}, true, derr
		}
		if kindOf(dMode) == "dir" {
			return FileInfo{}, false, ErrWrongKind
		}
		return FileInfo{}, false, ErrExternalChange
	}
	if errors.Is(uerr, errUndoParked) {
		return FileInfo{}, false, fmt.Errorf("%w: %w", ErrExternalChange, errUndoParked)
	}
	return FileInfo{}, true, uerr
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
// renameat2(RENAME_NOREPLACE); when dstFP is non-empty (the destination
// was expected to hold a specific object) the move is VERIFIED instead:
// RENAME_EXCHANGE puts the displaced object back at the source name, its
// identity must equal dstFP, and only that expected object is removed —
// a foreign destination is restored and the source returned, so a stale
// rename can never destroy a successor's acknowledged object. stage is
// the deterministic recovery name a parked foreign object is moved to.
// The bool result reports whether renameat2 committed — a trailing stat
// failure after that point means "landed but unobserved", not "never
// ran" (F-RA-5/f120).
func (p *posixRoot) rename(scope, from, to string, noReplace bool, dstFP, srcFP, stage string) (FileInfo, bool, error) {
	if err := checkReserved(to); err != nil {
		return FileInfo{}, false, err
	}
	if err := checkReserved(from); err != nil {
		return FileInfo{}, false, err
	}
	relFrom, err := relPath(from)
	if err != nil {
		return FileInfo{}, false, err
	}
	relTo, err := relPath(to)
	if err != nil {
		return FileInfo{}, false, err
	}
	if relFrom == "" || relTo == "" {
		return FileInfo{}, false, ErrNotDir
	}
	if relTo == relFrom || strings.HasPrefix(relTo, relFrom+"/") {
		return FileInfo{}, false, ErrEscape // cannot move a dir beneath itself
	}
	sfd, err := p.scopeDir(scope, false)
	if err != nil {
		return FileInfo{}, false, err
	}
	defer sfd.Close()
	srcDir, srcName := splitRel(relFrom)
	dstDir, dstName := splitRel(relTo)
	srcPfd, err := openDirBeneath(sfd, srcDir, false)
	if err != nil {
		return FileInfo{}, false, err
	}
	defer srcPfd.Close()
	dstPfd, err := openDirBeneath(sfd, dstDir, false)
	if err != nil {
		return FileInfo{}, false, err
	}
	defer dstPfd.Close()
	src3, srcMode, _, serr := fp3at(srcPfd, srcName)
	if serr != nil {
		return FileInfo{}, false, serr
	}
	if srcFP != "" && src3 != fp3(srcFP) {
		// The source object changed since the intent declared — moving it
		// would relocate foreign content under the op's authority.
		return FileInfo{}, false, ErrExternalChange
	}
	var flags uint
	switch {
	case noReplace || dstFP == "":
		// Destination expected absent at declare (or create-only
		// requested): NOREPLACE makes the move fail rather than
		// overwrite an object that appeared after the declare.
		flags = unix.RENAME_NOREPLACE
	default:
		flags = unix.RENAME_EXCHANGE
	}
	err = unix.Renameat2(int(srcPfd.Fd()), srcName, int(dstPfd.Fd()), dstName, flags)
	if err != nil {
		return FileInfo{}, false, mapPublishErr(err, true)
	}
	if flags&unix.RENAME_NOREPLACE != 0 {
		syncDir(dstPfd)
		info, serr := p.stat(scope, to)
		if serr != nil {
			return FileInfo{}, true, serr
		}
		return info, true, nil
	}
	// Exchanged: srcName now holds the displaced destination object.
	d3, dMode, _, derr := fp3at(srcPfd, srcName)
	m3, _, _, merr := fp3at(dstPfd, dstName)
	if derr != nil || merr != nil {
		// Post-exchange observation failed — the move committed; the
		// reconciler settles by inspection.
		return FileInfo{}, true, errVerifyUnobserved
	}
	var fail error
	switch {
	case d3 != fp3(dstFP) || m3 != src3:
		fail = ErrExternalChange // displaced/moved object isn't the declared one
	case dMode.IsDir() != srcMode.IsDir():
		fail = ErrWrongKind // dir↔non-dir exchange never commits
	case stage == "":
		// No private slot to verify the discard under — never unlink a
		// public name (a paused actor resuming could delete a successor's
		// object). Undo instead.
		fail = ErrExternalChange
	default:
		// The displaced destination is the declared object — but it sits
		// at the PUBLIC source name, so the delete must happen under the
		// intent's private slot: capture it there (NOREPLACE, non-
		// destructive), re-verify identity at the slot, then unlink.
		if p.faultHook != nil {
			p.faultHook("rename.postVerify")
		}
		if merr := unix.Renameat2(int(srcPfd.Fd()), srcName,
			int(srcPfd.Fd()), stage, unix.RENAME_NOREPLACE); merr != nil {
			if errors.Is(merr, unix.EEXIST) {
				fail = ErrConflict // slot occupied by a prior leftover
				break
			}
			return FileInfo{}, true, mapPathErr(merr)
		}
		p3, _, _, perr := fp3at(srcPfd, stage)
		if perr != nil {
			return FileInfo{}, true, perr // captured but unobservable — parked
		}
		if p3 != fp3(dstFP) {
			// A racing writer's object reached srcName between the verify
			// and the capture — it is foreign, not ours to delete. Put it
			// back on its name; if the name is already re-occupied it
			// stays parked for the reconciler. The rename itself
			// committed either way.
			unix.Renameat2(int(srcPfd.Fd()), stage,
				int(srcPfd.Fd()), srcName, unix.RENAME_NOREPLACE)
			syncDir(dstPfd)
			info, serr := p.stat(scope, to)
			if serr != nil {
				return FileInfo{}, true, serr
			}
			return info, true, nil
		}
		// The slot name is enumerable and reachable by delayed swaps a
		// retired reconciler decided while the slot held foreign bytes —
		// the delete must happen under a sealed name: capture stage→-q,
		// re-verify, unlink only the declared object.
		qname, uerr := quarantineDelete(srcPfd, stage, fp3(dstFP))
		if uerr == nil {
			syncDir(dstPfd)
			info, serr := p.stat(scope, to)
			if serr != nil {
				return FileInfo{}, true, serr
			}
			return info, true, nil
		}
		if errors.Is(uerr, ErrConflict) {
			// A foreign object reached the slot between the verify and
			// the sealed capture — it is parked, not destroyed. The
			// rename committed; the intent survives for the reconciler.
			return FileInfo{}, true, fmt.Errorf("%w: %w", ErrExternalChange, errUndoParked)
		}
		if !errors.Is(uerr, unix.ENOTEMPTY) && !errors.Is(uerr, unix.EEXIST) {
			return FileInfo{}, true, uerr
		}
		// The captured dir gained members — it diverged from what the
		// intent was allowed to discard. Move it back to srcName and fall
		// through to the undo, refusing the whole op like a pre-exchange
		// ENOTEMPTY would have.
		if unix.Renameat2(int(srcPfd.Fd()), qname,
			int(srcPfd.Fd()), srcName, unix.RENAME_NOREPLACE) != nil {
			return FileInfo{}, true, fmt.Errorf("%w: %w", ErrNotEmpty, errUndoParked)
		}
		fail = ErrNotEmpty
	}
	// Undo the exchange without ever unlinking foreign bytes. Our
	// object's identity is the source triple captured before the move.
	uerr := undoDisplaced(srcPfd, dstPfd, srcName, dstName, stage,
		func(t3 string) bool { return t3 == src3 })
	if uerr == nil {
		return FileInfo{}, false, fail
	}
	if errors.Is(uerr, errUndoParked) {
		return FileInfo{}, false, fmt.Errorf("%w: %w", fail, errUndoParked)
	}
	return FileInfo{}, true, uerr
}

// remove deletes (scope, path) VERIFIED: the target is first moved aside
// to the intent's staging name — never unlinked outright — and its
// identity is compared to expectFP, the object observed at declare. Only
// the expected object is unlinked; a foreign object is restored to the
// path (or left parked at the staging name if a racing writer claimed
// it). A stale remove therefore cannot destroy a successor's newer save.
// The bool result reports whether the object is gone — an error after
// that point is observation, not non-commit (F-RA-5/f120).
func (p *posixRoot) remove(scope, path, expectFP, stage string) (bool, error) {
	if err := checkReserved(path); err != nil {
		return false, err
	}
	rel, err := relPath(path)
	if err != nil {
		return false, err
	}
	if rel == "" {
		return false, ErrNotDir // removing the scope root itself is not an op
	}
	sfd, err := p.scopeDir(scope, false)
	if err != nil {
		return false, err
	}
	defer sfd.Close()
	dirRel, name := splitRel(rel)
	pfd, err := openDirBeneath(sfd, dirRel, false)
	if err != nil {
		return false, err
	}
	defer pfd.Close()
	if stage == "" {
		// The capture may end up holding foreign content — never a
		// .filesv-tmp- name, which the staging sweep would delete.
		stage = opStagePrefix + "adhoc-" + randHex(8)
	}
	// Non-destructive capture: move the target to the staging name. A
	// leftover parked object occupying it refuses the op rather than
	// clobber recovery bytes.
	err = unix.Renameat2(int(pfd.Fd()), name, int(pfd.Fd()), stage, unix.RENAME_NOREPLACE)
	switch {
	case errors.Is(err, unix.ENOENT):
		return false, ErrNotFound
	case errors.Is(err, unix.EEXIST):
		return false, ErrConflict
	case err != nil:
		return false, mapPathErr(err)
	}
	st3, _, _, serr := fp3at(pfd, stage)
	if serr != nil {
		return false, serr // captured but unobservable — reconciler settles
	}
	if st3 == fp3(expectFP) {
		// Captured the expected object — complete the removal under a
		// sealed name: the slot stayed enumerable through this window,
		// so a delayed swap could have replaced the verified object.
		qname, uerr := quarantineDelete(pfd, stage, st3)
		switch {
		case uerr == nil:
			// If a racing reconcile pass restored the captured object
			// to its name, the removal did not land — the name holding
			// an object again means the intent's purpose is unmet.
			if _, _, _, oerr := fp3at(pfd, name); oerr == nil {
				return false, ErrExternalChange
			}
			syncDir(pfd)
			return true, nil
		case errors.Is(uerr, ErrConflict):
			// Foreign bytes reached the slot between verify and the
			// sealed capture — parked, not destroyed; the reconciler
			// settles them.
			return false, fmt.Errorf("%w: %w", ErrExternalChange, errUndoParked)
		case !errors.Is(uerr, unix.ENOTEMPTY) && !errors.Is(uerr, unix.EEXIST):
			// The post-capture state is uncertain — the object may sit
			// at the slot, at the sealed name, or be gone. Tombstone so
			// the reconciler settles whatever was left rather than
			// dropping the intent over live recovery evidence.
			return false, fmt.Errorf("%w: %w", uerr, errUndoParked)
		}
		// ENOTEMPTY: a dir gained members after capture — it diverged
		// from the declared object; restore it from the sealed name
		// instead of deleting.
		rerr := unix.Renameat2(int(pfd.Fd()), qname, int(pfd.Fd()), name, unix.RENAME_NOREPLACE)
		switch {
		case rerr == nil:
			syncDir(pfd)
			return false, ErrExternalChange
		case errors.Is(rerr, unix.EEXIST):
			return false, fmt.Errorf("%w: %w", ErrExternalChange, errUndoParked)
		default:
			return false, rerr
		}
	}
	// Captured object is not what the intent declared — put it back. A
	// racing writer's object occupying the name keeps it; the captured
	// object stays parked at the staging name for recovery.
	rerr := unix.Renameat2(int(pfd.Fd()), stage, int(pfd.Fd()), name, unix.RENAME_NOREPLACE)
	switch {
	case rerr == nil:
		syncDir(pfd)
		return false, ErrExternalChange
	case errors.Is(rerr, unix.EEXIST):
		return false, fmt.Errorf("%w: %w", ErrExternalChange, errUndoParked)
	default:
		return false, rerr
	}
}

// The bool result reports whether the directory exists after the call —
// mkdir's commit point is inside openDirBeneath; a failure of the
// trailing stat means "exists but unobserved" (F-RA-5/f120).
func (p *posixRoot) mkdir(scope, path string) (FileInfo, bool, error) {
	if err := checkReserved(path); err != nil {
		return FileInfo{}, false, err
	}
	rel, err := relPath(path)
	if err != nil {
		return FileInfo{}, false, err
	}
	sfd, err := p.scopeDir(scope, true)
	if err != nil {
		return FileInfo{}, false, err
	}
	defer sfd.Close()
	pfd, err := openDirBeneath(sfd, rel, true)
	if err != nil {
		return FileInfo{}, false, err
	}
	pfd.Close()
	syncDir(sfd)
	info, serr := p.stat(scope, path)
	if serr != nil {
		return FileInfo{}, true, serr
	}
	return info, true, nil
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
