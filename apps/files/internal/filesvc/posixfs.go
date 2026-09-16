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
	mcIdent    mountIdent          // identity of the last verified mount ("" when never verified)

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

// mountIdent is the verified identity of the mount serving the root,
// recorded each time the mount check succeeds. Durable object identity
// (objectID) consults it: a FUSE object's mount ID must equal the
// verified mount's, and a JuiceFS object's namespace is the volume
// UUID, so identity is bound to the verified mount instance — never
// assumed from a path.
type mountIdent struct {
	mntID  uint64
	fstype string
	// jfsUUID is the mounted JuiceFS volume's identifier, read from the
	// in-mount /.config during verification. A remount of the same
	// volume keeps it (identity continuity across remounts); a
	// replacement volume reports a different UUID, so objects from two
	// metadata histories can never compare equal.
	jfsUUID string
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

// setMountIdent records the identity of the mount that just passed
// verification; objectID compares an object's own mount ID against it
// for FUSE filesystems (identity is only provable while the fd is
// served by the mount that was verified — a replaced mount answers
// with a different ID and binds nothing).
func (p *posixRoot) setMountIdent(mi mountIdent) {
	p.mcMu.Lock()
	p.mcIdent = mi
	p.mcMu.Unlock()
}

func (p *posixRoot) mountIdent() mountIdent {
	p.mcMu.Lock()
	defer p.mcMu.Unlock()
	return p.mcIdent
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
	mi := mountIdent{mntID: stx.Mnt_id, fstype: visible.fstype}
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
		cfg, err := os.ReadFile(cfgPath)
		if err != nil {
			return fmt.Errorf("%w: no verifiable mount config", ErrMountPolicy)
		}
		if err := checkZeroMetadataCacheData(cfg); err != nil {
			return err
		}
		mi.jfsUUID = jfsUUIDFromConfig(cfg)
	}
	p.setMountIdent(mi)
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
// IsDir/IsRegular wrong — which misclassified every list entry (operation-
// review B F2).
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
	// DevIno is a transient traversal identity ("dev:ino") for pass-local
	// loop guards — never persisted. Fingerprint deliberately excludes
	// dev for FUSE remount continuity, but a traversal key must include
	// it: inode numbers are unique only within one filesystem, so two
	// distinct objects on different filesystems sharing an inode is
	// ordinary, while a bind-mounted alias keeps dev+ino and is correctly
	// recognized as the same object.
	DevIno string `json:"-"`
	// Oid is the object's durable filesystem identity, derived at
	// open time from the opened object itself — never inferred from
	// path/fingerprint/hash matching. Empty ("") when the filesystem
	// cannot prove a stable identity: callers then fall back to
	// generation evidence (fp3/sha) and never upgrade a match to
	// old-object identity.
	Oid string `json:"-"`
	// oidCls records HOW Oid was derived so the declaring intent can
	// distinguish "identity unsupported on this filesystem" (unbound —
	// proceed on weaker evidence) from "identity exists but could not
	// be proven right now" (transient — a race-class failure; declare
	// must fail rather than record a blind intent).
	oidCls idClass
	// Nlink is the link count — a shared object (hardlink) must never be
	// silently discarded during recovery cleanup.
	Nlink uint64 `json:"-"`
}

// idClass classifies an objectID attempt.
type idClass int

const (
	idBound     idClass = iota // identity proven for this object
	idUnbound                  // filesystem cannot prove identity — no evidence
	idTransient                // identity exists but proof raced (handle stale, etc.) — retryable
	idMisuse                   // caller asked outside a verified context — no evidence
)

// objectID derives the durable identity of the object behind an OPEN fd.
// The fd is the provenance: the identity describes the object that was
// opened, never a path observed later. Per filesystem:
//
//   - ext-family/tmpfs: fstatfs fsid + name_to_handle_at — the kernel's
//     durable object token, stable across rename and remount, unique
//     within the filesystem instance (fsid changes on mkfs, so a
//     replacement volume can never alias old identities).
//   - fuse.juicefs: the verified mount's volume UUID + inode. Valid
//     only while the object's fd is served by the verified mount ID —
//     a remount mints a new mount ID, so an fd opened after a
//     replacement can never be confused with pre-replacement identity.
//     JuiceFS allocates inodes monotonically within a metadata
//     history (verified on the fixture: no reuse across churn), so
//     UUID+ino identifies one object for the volume's lifetime.
//   - everything else: unbound — the filesystem offers no provable
//     object identity and recovery precision degrades accordingly.
func (p *posixRoot) objectID(f *os.File) (string, idClass) {
	var sfs unix.Statfs_t
	if err := unix.Fstatfs(int(f.Fd()), &sfs); err != nil {
		return "", idUnbound
	}
	if uint64(sfs.Type) == 0xef53 || uint64(sfs.Type) == unix.TMPFS_MAGIC {
		// ext-family/tmpfs: fstatfs fsid + name_to_handle_at.
		h, _, err := unix.NameToHandleAt(int(f.Fd()), "", unix.AT_EMPTY_PATH)
		if err != nil {
			if errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOSYS) {
				return "", idUnbound // kernel/fs lacks handle support
			}
			return "", idTransient // EBADF/ESTALE-class — proof raced
		}
		ns := fmt.Sprintf("%x:%x", uint32(sfs.Fsid.Val[0]), uint32(sfs.Fsid.Val[1]))
		return "h:" + ns + ":" + strconv.Itoa(int(h.Type())) + ":" +
			hex.EncodeToString(h.Bytes()), idBound
	}
	// FUSE and everything else: only JuiceFS beneath a VERIFIED mount
	// carries provable identity — the object's own mount ID must equal
	// the mount ID that passed verification.
	mi := p.mountIdent()
	if mi.fstype != "fuse.juicefs" || mi.jfsUUID == "" {
		return "", idUnbound
	}
	var stx unix.Statx_t
	if err := unix.Statx(int(f.Fd()), "", unix.AT_EMPTY_PATH,
		unix.STATX_MNT_ID, &stx); err != nil || stx.Mask&unix.STATX_MNT_ID == 0 {
		return "", idTransient // mount identity unprovable right now
	}
	if stx.Mnt_id != mi.mntID {
		return "", idMisuse // fd is served by a different mount — not the verified one
	}
	st, err := f.Stat()
	if err != nil {
		return "", idTransient
	}
	sys, ok := st.Sys().(*syscall.Stat_t)
	if !ok {
		return "", idUnbound
	}
	return "j:" + mi.jfsUUID + ":" + strconv.FormatUint(sys.Ino, 10), idBound
}

// fileIdentity populates the durable-identity fields of a FileInfo for
// the object behind f, retrying the transient race once. The class is
// recorded so a declaring intent can fail on transient rather than
// record blind evidence.
func (p *posixRoot) fileIdentity(f *os.File) (oid string, cls idClass, nlink uint64) {
	oid, cls = p.objectID(f)
	if cls == idTransient {
		oid, cls = p.objectID(f)
	}
	if st, err := f.Stat(); err == nil {
		if sys, ok := st.Sys().(*syscall.Stat_t); ok {
			nlink = sys.Nlink
		}
	}
	return oid, cls, nlink
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

// devIno is the traversal-key counterpart of fingerprint: dev+ino through
// the same pinned stat. It is deliberately NOT persisted — only valid
// within one pass, where remounts cannot renumber dev mid-walk.
func devIno(st fs.FileInfo) string {
	s, ok := st.Sys().(*syscall.Stat_t)
	if !ok {
		return ""
	}
	return fmt.Sprintf("%d:%d", s.Dev, s.Ino)
}

// fp3 reduces a fingerprint to its stable ino:size:mtime triple —
// ctime shifts on relink, so it is never part of identity. The function
// is idempotent: journaled observations store fp3 already.
func fp3(fp string) string {
	parts := strings.Split(fp, ":")
	if len(parts) > 3 {
		parts = parts[:3]
	}
	return strings.Join(parts, ":")
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
// the name's occupant keeps the path and the foreign object stays parked
// at the intent's private name for the reconciler. Foreign bytes are
// never unlinked — only objects the op itself created or was authorized
// to displace.
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

// discardOwned unlinks the object at name beneath pfd after re-proving
// its fp3. The name is an owned single-epoch private name: the identity
// recheck plus in-place unlink is definitive because nothing else may
// write the name. ENOTEMPTY/EEXIST (a dir that gained members) maps to
// ErrNotEmpty so callers can treat it as divergence; a foreign capture
// (fp3 mismatch) reports ErrConflict and leaves the object parked.
func discardOwned(pfd *os.File, name, want3 string) error {
	st3, _, _, err := fp3at(pfd, name)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if st3 != want3 {
		return ErrConflict
	}
	uerr := unix.Unlinkat(int(pfd.Fd()), name, 0)
	if errors.Is(uerr, unix.EISDIR) {
		uerr = unix.Unlinkat(int(pfd.Fd()), name, unix.AT_REMOVEDIR)
	}
	switch {
	case uerr == nil || errors.Is(uerr, unix.ENOENT):
		return nil
	case errors.Is(uerr, unix.ENOTEMPTY) || errors.Is(uerr, unix.EEXIST):
		return ErrNotEmpty
	default:
		return mapPathErr(uerr)
	}
}

// undoDisplaced reverses a committed exchange after verification found
// the displaced object was not the declared expectation. (sname under
// sfd) is the op's private name; (dname under dfd) is the public name
// it exchanged with. The loop runs until our object is back at sname
// (nil) or the undo cannot proceed without destroying foreign bytes:
//
//   - sname already holds ours → done.
//   - dname holds ours → swap (sname's foreign object goes back to its
//     public name; ours returns to the private name) or, when sname is
//     empty, move ours back to sname.
//   - dname is empty but sname holds a foreign object → that object
//     came from dname via our exchange: put it back (NOREPLACE).
//   - dname holds a foreign object → it is NEVER evicted; the staged
//     foreign object stays parked for the reconciler.
//
// The name's occupant always wins — no ctime or recency arbitration:
// a heuristic that evicts acknowledged content on a guess is not
// authority. Bounded by a fixed iteration count; giving up reports
// errUndoParked honestly.
func undoDisplaced(sfd, dfd *os.File, sname, dname string, ours func(string) bool) error {
	for i := 0; i < 4; i++ {
		s3, _, _, serr := fp3at(sfd, sname)
		if serr == nil && ours(s3) {
			return nil // our object is back at the private name
		}
		if serr != nil && !errors.Is(serr, ErrNotFound) {
			return serr
		}
		d3, _, _, derr := fp3at(dfd, dname)
		switch {
		case errors.Is(derr, ErrNotFound):
			// The public name is empty. If the private name holds a
			// foreign object it came from there via our exchange —
			// return it. An empty private name means our object is
			// unaccounted for: give up honestly.
			if serr != nil {
				return errUndoParked
			}
			if rerr := unix.Renameat2(int(sfd.Fd()), sname,
				int(dfd.Fd()), dname, unix.RENAME_NOREPLACE); rerr != nil {
				return errUndoParked
			}
		case derr != nil:
			return derr
		case ours(d3):
			// Our object occupies the public name — bring it back
			// under the private name. When the private name holds a
			// foreign object, the swap returns it to the public name.
			var xerr error
			if serr == nil {
				xerr = unix.Renameat2(int(sfd.Fd()), sname,
					int(dfd.Fd()), dname, unix.RENAME_EXCHANGE)
			} else {
				xerr = unix.Renameat2(int(dfd.Fd()), dname,
					int(sfd.Fd()), sname, unix.RENAME_NOREPLACE)
			}
			if xerr != nil {
				return errUndoParked
			}
		default:
			// The name's occupant is foreign — it is never evicted.
			// Whatever the private name holds stays parked for the
			// reconciler.
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
	return p.statFrom(rfd, scope, path)
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
	oid, cls, nlink := p.fileIdentity(f)
	return FileInfo{Kind: kindOf(st.Mode()), Size: st.Size(),
		MtimeNS: st.ModTime().UnixNano(), Fingerprint: fingerprint(st),
		DevIno: devIno(st), Oid: oid, oidCls: cls, Nlink: nlink}, nil
}

// statFrom stats beneath a pinned root fd — the reconciler's view so an
// "absent" verdict is bound to the filesystem the pass verified, never
// to a bare directory left behind by a mid-pass unmount.
func (p *posixRoot) statFrom(rfd *os.File, scope, path string) (FileInfo, error) {
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
	oid, cls, nlink := p.fileIdentity(f)
	return FileInfo{Kind: kindOf(st.Mode()), Size: st.Size(),
		MtimeNS: st.ModTime().UnixNano(), Fingerprint: fingerprint(st),
		DevIno: devIno(st), Oid: oid, oidCls: cls, Nlink: nlink}, nil
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
	return p.openFrom(rfd, scope, path, off)
}

func (p *posixRoot) openFrom(rfd *os.File, scope, path string, off int64) (*os.File, FileInfo, error) {
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
	oid, cls, nlink := p.fileIdentity(f)
	info := FileInfo{Kind: "file", Size: st.Size(), MtimeNS: st.ModTime().UnixNano(), Fingerprint: fingerprint(st), DevIno: devIno(st), Oid: oid, oidCls: cls, Nlink: nlink}
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
	return p.hashFrom(rfd, scope, path)
}

func (p *posixRoot) hashFrom(rfd *os.File, scope, path string) (string, error) {
	f, _, err := p.openFrom(rfd, scope, path, 0)
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
	p   *posixRoot
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
	return &rootView{p: p, rfd: rfd}, nil
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
	mi := mountIdent{mntID: stx.Mnt_id, fstype: visible.fstype}
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
		if err := checkZeroMetadataCacheData(content); err != nil {
			return err
		}
		mi.jfsUUID = jfsUUIDFromConfig(content)
	}
	p.setMountIdent(mi)
	return nil
}

// jfsUUIDFromConfig extracts the JuiceFS volume UUID from a verified
// /.config body — the durable namespace leg for object identity on
// JuiceFS. An absent or malformed field degrades identity to unbound,
// never fails the mount check (freshness verdicts stay independent of
// identity capability).
func jfsUUIDFromConfig(cfg []byte) string {
	var c map[string]any
	if err := json.Unmarshal(cfg, &c); err != nil {
		return ""
	}
	u, _ := c["UUID"].(string)
	return u
}

func (v *rootView) Stat(scope, path string) (FileInfo, error) {
	return v.p.statFrom(v.rfd, scope, path)
}

func (v *rootView) Hash(scope, path string) (string, error) {
	return v.p.hashFrom(v.rfd, scope, path)
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

// ListScopes returns the directory entries directly beneath the pinned
// root — the scope inventory for the orphan sweep, bounded to the owned
// root's immediate children.
func (v *rootView) ListScopes() ([]string, error) {
	// The pinned fd is O_PATH — reopen "." beneath it for readdir so the
	// listing stays descriptor-relative to the owned root.
	dfd, err := openBeneath(v.rfd, ".", unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		return nil, err
	}
	defer dfd.Close()
	ents, err := dfd.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	out := ents[:0]
	for _, e := range ents {
		// Hidden and reserved names are never scope dirs.
		if strings.HasPrefix(e, ".") || validScope(e) != nil {
			continue
		}
		out = append(out, e)
	}
	return out, nil
}

// ListDir returns every entry in dir, unfiltered — the orphan sweep
// uses it to recurse into subdirectories.
func (v *rootView) ListDir(scope, dir string) ([]string, error) {
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
	return dfd.Readdirnames(-1)
}

// EnsureDir creates dir and any missing parents beneath the scope —
// fd-relative mkdir-p under the pinned root, so the walk cannot be
// redirected outside it by a racing rename. Recovery uses it to
// recreate a recorded home's parent chain before restoring a member
// out of a parked container.
func (v *rootView) EnsureDir(scope, dir string) error {
	sfd, err := scopeDirFrom(v.rfd, scope, false)
	if err != nil {
		return err
	}
	defer sfd.Close()
	rel, err := relPath(dir)
	if err != nil {
		return err
	}
	if rel == "" {
		return nil
	}
	dfd, err := openDirBeneath(sfd, rel, true)
	if err != nil {
		return err
	}
	syncDir(dfd)
	dfd.Close()
	return nil
}

func (v *rootView) Close() error { return v.rfd.Close() }

// atomicWrite stages the body at the intent's declared private name
// (it.stageBase — recorded in the journal before this call can populate
// it), fsyncs, and publishes. A name never resolves to torn content.
//
// exclusive=true publishes with linkat(2): EEXIST if anything already
// occupies the name — the create-only check is atomic. Non-exclusive
// publish is VERIFIED: RENAME_EXCHANGE moves whatever the path held
// into the private name, and the displaced object's identity must equal
// it.dstFP — the fingerprint (and oid, when bound) observed at declare.
// Only the expected object is discarded; a foreign object is restored
// to the name, or stays parked at the private name for the reconciler
// when a racing writer claimed the path. The private name is a
// single-epoch owned name: only this process's recorded acts may write
// it, so verify-then-unlink in place is definitive — a delayed syscall
// from a retired epoch can never target it (it was declared with a
// unique ordinal under this intent).
//
// The bool result reports whether the fs effect committed — an error
// after that point means "landed but unobserved", not "never ran"; the
// caller must preserve the intent for reconciliation rather than drop
// it as never-committed (F-RA-5/f120).
func (p *posixRoot) atomicWrite(scope, path string, content []byte, exclusive bool, it intent) (FileInfo, bool, error) {
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
	tmp := it.stageBase()
	if tmp == "" {
		// No journal (unintented caller) — still a private recovery name.
		tmp = opStagePrefix + "adhoc-" + randHex(8)
	}
	expectFP := it.dstFP
	tf, err := openBeneath(pfd, tmp,
		unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW, 0o644)
	if err != nil {
		return FileInfo{}, false, err
	}
	// Our object's inode from the open fd — fd-bound, so it stays
	// correct even if a delayed effect swaps what the name points at.
	var ownIno uint64
	var fst unix.Stat_t
	if unix.Fstat(int(tf.Fd()), &fst) == nil {
		ownIno = fst.Ino
	}
	if p.faultHook != nil {
		p.faultHook("write.postCreate")
	}
	// fail returns a pre-commit error after best-effort cleanup of the
	// staged file. The name is ours, but a delayed effect may have
	// swapped its occupant between create and now — verify the name
	// still holds OUR object (same inode; an in-place scribble on our
	// own object is still ours to delete) before unlinking; a foreign
	// occupant stays parked and the intent must be tombstoned.
	fail := func(perr error) (FileInfo, bool, error) {
		cur3, _, _, cerr := fp3at(pfd, tmp)
		curIno, _, _, _ := fpParts(cur3 + ":0")
		switch {
		case errors.Is(cerr, ErrNotFound):
			// Name already empty — nothing to clean.
		case cerr == nil && curIno != "" && curIno == strconv.FormatUint(ownIno, 10):
			if uerr := unix.Unlinkat(int(pfd.Fd()), tmp, 0); uerr != nil && !errors.Is(uerr, unix.ENOENT) {
				return FileInfo{}, false, fmt.Errorf("%w: %w", perr, errUndoParked)
			}
		case cerr == nil:
			// A foreign object sits at our name — never unlink it.
			return FileInfo{}, false, fmt.Errorf("%w: %w", perr, errUndoParked)
		default:
			return FileInfo{}, false, fmt.Errorf("%w: %w", perr, errUndoParked)
		}
		it.njDone()
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
	// The authored body's durable identity from its open fd — journaled
	// so a later pass can tell our body from a late-exchanged occupant
	// without content matching.
	bodyOid, _, _ := p.fileIdentity(tf)
	if err := tf.Close(); err != nil {
		return fail(mapPathErr(err))
	}
	// Record the populate result — a successor judging this name later
	// sees the slot definitively populated by this epoch.
	it.njRes("ok")
	// Our object's identity before any exchange — ctime shifts on relink,
	// so identity is the ino:size:mtime triple only.
	our3, _, ourCtime, ourErr := fp3at(pfd, tmp)
	if ourErr == nil {
		it.njObs(fmt.Sprintf("%s:%d|%s", our3, ourCtime, bodyOid))
	}
	// commitInfo returns the committed object's observed identity only
	// when the name provably still holds the object this op published
	// (fp3 == our3). A late stat that observes a different writer's
	// object — or fails — yields no identity this commit may journal.
	commitInfo := func() (FileInfo, error) {
		info, serr := p.stat(scope, path)
		if serr != nil {
			return FileInfo{}, serr
		}
		if ourErr != nil || fp3(info.Fingerprint) != our3 {
			return FileInfo{}, ErrExternalChange
		}
		return info, nil
	}
	// commit drops the staging name's object (verified in place — the
	// name is owned), fsyncs, and stats the result. want3 is the fp3
	// expected at the slot: our staged bytes, or the declared displaced
	// object. A foreign object at the slot reports ErrConflict and stays
	// parked for the reconciler.
	commit := func(want3 string) (FileInfo, bool, error) {
		if p.faultHook != nil {
			p.faultHook("write.preSlotDelete")
		}
		if derr := discardOwned(pfd, tmp, want3); derr != nil {
			if errors.Is(derr, ErrConflict) {
				// Foreign bytes parked at the private name; the publish
				// committed. Journal only an identity verified against
				// our own object.
				if info, verr := commitInfo(); verr == nil {
					return info, true, fmt.Errorf("%w: %w", ErrExternalChange, errUndoParked)
				} else {
					return FileInfo{}, true, verr
				}
			}
			return FileInfo{}, true, derr
		}
		it.njDone()
		syncDir(pfd)
		info, verr := commitInfo()
		if verr != nil {
			return FileInfo{}, true, verr
		}
		return info, true, nil
	}
	if exclusive {
		if err := unix.Linkat(int(pfd.Fd()), tmp, int(pfd.Fd()), name, 0); err != nil {
			return fail(mapPublishErr(err, exclusive))
		}
		return commit(our3)
	}
	if expectFP == "" {
		// Declared-empty destination: publish non-destructively. An
		// occupant that arrived after declare is never displaced into
		// our private name — the write fails honest-conflict instead.
		it.njAct("pub", rel)
		err = unix.Renameat2(int(pfd.Fd()), tmp, int(pfd.Fd()), name, unix.RENAME_NOREPLACE)
		if p.faultHook != nil {
			// Kill boundary: the publish may have committed while its
			// result journal has not.
			p.faultHook("write.postPub")
		}
		switch {
		case err == nil:
			it.njRes("ok")
			return commit(our3)
		case errors.Is(err, unix.EEXIST):
			it.njRes("noeff") // provably no effect — nothing moved
			return fail(ErrExternalChange)
		case errors.Is(err, unix.ENOENT):
			it.njRes("noeff") // our staged name is gone — nothing moved
			return fail(mapPublishErr(err, exclusive))
		default:
			// Transport-class failure: the publish outcome is UNKNOWN —
			// leave the result unrecorded so a successor judges by disk.
			return fail(mapPublishErr(err, exclusive))
		}
	}
	// Record the exchange BEFORE it can populate the private name with
	// the displaced object — a crash between syscall and record leaves
	// the act declared-but-unresulted, which the reconciler reads as
	// outcome-unknown, never "definitively absent".
	it.njAct("xch", rel)
	err = unix.Renameat2(int(pfd.Fd()), tmp, int(pfd.Fd()), name, unix.RENAME_EXCHANGE)
	if p.faultHook != nil {
		// Kill boundary: the exchange may have committed while its
		// result journal has not — a successor must read res="" +
		// occupied as outcome-unknown, never "definitively absent".
		p.faultHook("write.postXch")
	}
	switch {
	case errors.Is(err, unix.ENOENT):
		it.njRes("noeff")
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
		// Only ENOENT proves no effect; every other error leaves the
		// exchange's outcome unknown — the result stays unrecorded.
		return fail(mapPublishErr(err, exclusive))
	}
	it.njRes("ok")
	// Exchanged: the private name now holds the object the path used to
	// hold.
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
	// Declare the undo before its exchanges can repopulate the slot:
	// res re-opens first so every crash window reads outcome-unknown.
	it.njRes("")
	it.njAct("und", rel)
	uerr := undoDisplaced(pfd, pfd, tmp, name,
		func(t3 string) bool { return t3 == our3 })
	if uerr == nil {
		it.njRes("ok") // the slot provably holds our staged body again
	}
	if p.faultHook != nil {
		p.faultHook("write.postUndo")
	}
	if uerr == nil {
		// tmp holds our own staged bytes again — discard them in place
		// (the name is owned; identity re-verified by discardOwned).
		if derr := discardOwned(pfd, tmp, our3); derr != nil {
			if errors.Is(derr, ErrConflict) {
				// Whatever landed at the slot is foreign and parked;
				// the undo itself already restored the name.
				return FileInfo{}, false, fmt.Errorf("%w: %w", ErrExternalChange, errUndoParked)
			}
			return FileInfo{}, true, derr
		}
		it.njDone()
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
// be redirected outside the scope mid-op. noReplace (or a destination
// declared absent) uses renameat2(RENAME_NOREPLACE). A rename OVER an
// occupied destination is two-legged and verified:
//
//  1. CAPTURE: the source object moves to the intent's declared private
//     name (recorded in the journal before this call can populate it).
//     The source name is momentarily empty — the accepted gap; a crash
//     here leaves the source parked under owned storage, never on a
//     public path it could be mistaken for a user's move.
//  2. EXCHANGE: the private name swaps with the destination. The
//     displaced destination object lands at the private name — declared
//     private storage — so a crash can never strand it on the public
//     source name indistinguishable from an ordinary `mv`.
//  3. VERIFY + DISCARD: the private name's object must equal the
//     declared destination (oid when bound, fp3 otherwise). Only that
//     object is unlinked — in place, under the single-epoch name. A
//     foreign object undoes the exchange (its content goes back to the
//     destination name, ours back to the private name, then home) or
//     stays parked for the reconciler.
//
// The bool result reports whether the fs effect committed — an error
// after the exchange means "landed but unobserved", not "never ran"
// (F-RA-5/f120).
func (p *posixRoot) rename(scope, from, to string, noReplace bool, it intent) (FileInfo, bool, error) {
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
	if it.preFP != "" && src3 != fp3(it.preFP) {
		// The source object changed since the intent declared — moving it
		// would relocate foreign content under the op's authority.
		return FileInfo{}, false, ErrExternalChange
	}
	// committedInfo returns the moved object's observed identity only
	// when the destination provably still holds the object this rename
	// moved (fp3 == src3, captured before the move).
	committedInfo := func() (FileInfo, error) {
		info, serr := p.stat(scope, to)
		if serr != nil {
			return FileInfo{}, serr
		}
		if fp3(info.Fingerprint) != src3 {
			return FileInfo{}, ErrExternalChange
		}
		return info, nil
	}
	if noReplace || it.dstFP == "" {
		// Destination expected absent at declare (or create-only
		// requested): NOREPLACE makes the move fail rather than
		// overwrite an object that appeared after the declare.
		if err := unix.Renameat2(int(srcPfd.Fd()), srcName,
			int(dstPfd.Fd()), dstName, unix.RENAME_NOREPLACE); err != nil {
			return FileInfo{}, false, mapPublishErr(err, true)
		}
		syncDir(dstPfd)
		info, serr := committedInfo()
		if serr != nil {
			return FileInfo{}, true, serr
		}
		return info, true, nil
	}
	// Rename-over: stage the source beneath the intent's private name
	// first so the exchange can displace the destination object into
	// owned storage rather than onto the public source name.
	stage := it.stageBase()
	if stage == "" {
		stage = opStagePrefix + "adhoc-" + randHex(8)
	}
	// restoreSrc returns the staged source object to its public name —
	// non-destructive (NOREPLACE); a name claimed meanwhile leaves it
	// parked under the private name for the reconciler.
	restoreSrc := func(fail error) (FileInfo, bool, error) {
		rerr := unix.Renameat2(int(srcPfd.Fd()), stage,
			int(srcPfd.Fd()), srcName, unix.RENAME_NOREPLACE)
		syncDir(srcPfd)
		switch {
		case rerr == nil:
			return FileInfo{}, false, fail
		case errors.Is(rerr, unix.EEXIST):
			return FileInfo{}, false, fmt.Errorf("%w: %w", fail, errUndoParked)
		default:
			return FileInfo{}, false, rerr
		}
	}
	// 1. Capture the source beneath the declared private name — the act
	// is recorded before the syscall can populate the name, so a crash
	// leaves a declared-unresulted record, never an unowned object.
	it.njAct("cap", relFrom)
	if err := unix.Renameat2(int(srcPfd.Fd()), srcName,
		int(srcPfd.Fd()), stage, unix.RENAME_NOREPLACE); err != nil {
		switch {
		case errors.Is(err, unix.ENOENT):
			it.njRes("noeff")
			return FileInfo{}, false, ErrNotFound
		case errors.Is(err, unix.EEXIST):
			it.njRes("noeff")
			return FileInfo{}, false, ErrConflict // slot holds leftover residue
		default:
			// Transport-class failure: the capture may have committed —
			// leave res unrecorded so the outcome reads unknown.
			return FileInfo{}, false, mapPathErr(err)
		}
	}
	it.njRes("ok")
	// 2. Re-verify the CAPTURED object is the declared source: a foreign
	// object could have claimed `from` between the pre-check and the
	// capture. A mismatch is restored to its name — never moved onward.
	st3, stMode, _, verr := fp3at(srcPfd, stage)
	if verr != nil {
		return restoreSrc(verr)
	}
	if st3 != src3 {
		return restoreSrc(ErrExternalChange)
	}
	if p.faultHook != nil {
		p.faultHook("rename.postVerify")
	}
	// 3. Exchange the private name with the destination — recorded
	// before the syscall can populate the name with the displaced
	// object.
	it.njAct("xch", relTo)
	err = unix.Renameat2(int(srcPfd.Fd()), stage,
		int(dstPfd.Fd()), dstName, unix.RENAME_EXCHANGE)
	if p.faultHook != nil {
		// Kill boundary: the exchange may have committed while its
		// result journal has not.
		p.faultHook("rename.postXch")
	}
	if err != nil {
		switch {
		case errors.Is(err, unix.ENOENT):
			it.njRes("noeff")
			// The destination vanished since declare — nothing to
			// exchange with; restore the source.
			return restoreSrc(ErrExternalChange)
		default:
			// Any other failure leaves the exchange outcome unknown —
			// the result stays unrecorded and the intent survives.
			return restoreSrc(mapPublishErr(err, true))
		}
	}
	it.njRes("ok")
	// Exchanged: the private name now holds the displaced destination
	// object.
	d3, dMode, _, derr := fp3at(srcPfd, stage)
	m3, _, _, merr := fp3at(dstPfd, dstName)
	if derr != nil || merr != nil {
		return FileInfo{}, true, errVerifyUnobserved
	}
	var fail error
	switch {
	case d3 != fp3(it.dstFP) || m3 != src3:
		fail = ErrExternalChange // displaced/moved object isn't the declared one
	case dMode.IsDir() != srcMode.IsDir() || stMode.IsDir() != srcMode.IsDir():
		fail = ErrWrongKind // dir↔non-dir exchange never commits
	default:
		// The displaced destination is the declared object, parked under
		// our single-epoch private name: verify-and-unlink in place.
		if p.faultHook != nil {
			p.faultHook("rename.preSlotDelete")
		}
		uerr := discardOwned(srcPfd, stage, fp3(it.dstFP))
		switch {
		case uerr == nil:
			it.njDone()
			syncDir(srcPfd)
			info, serr := committedInfo()
			if serr != nil {
				return FileInfo{}, true, serr
			}
			return info, true, nil
		case errors.Is(uerr, ErrConflict):
			// A foreign object reached the slot — it is parked, not
			// destroyed. The rename committed; the intent survives.
			info, serr := committedInfo()
			if serr != nil {
				return FileInfo{}, true, serr
			}
			return info, true, fmt.Errorf("%w: %w", ErrExternalChange, errUndoParked)
		case errors.Is(uerr, ErrNotEmpty):
			// The captured dir gained members — it diverged from what
			// the intent was allowed to discard. Undo like a
			// pre-exchange ENOTEMPTY.
			fail = ErrNotEmpty
		default:
			return FileInfo{}, true, uerr
		}
	}
	// Undo: exchange the private name back with the destination (the
	// foreign object returns to its name, ours to the private name),
	// then restore the source to `from`. Never unlinks foreign bytes.
	// Declared before it can repopulate the slot: res re-opens first so
	// every crash window reads outcome-unknown.
	it.njRes("")
	it.njAct("und", relTo)
	uerr := undoDisplaced(srcPfd, dstPfd, stage, dstName,
		func(t3 string) bool { return t3 == src3 })
	if uerr == nil {
		it.njRes("ok") // the slot provably holds our source again
	}
	if uerr == nil {
		// Our source is back under the private name — return it home.
		return restoreSrc(fail)
	}
	if errors.Is(uerr, errUndoParked) {
		// Committed state unknown — the exchange may stand with a
		// foreign object parked at the private name.
		info, serr := committedInfo()
		if serr == nil {
			return info, true, fmt.Errorf("%w: %w", fail, errUndoParked)
		}
		return FileInfo{}, false, fmt.Errorf("%w: %w", fail, errUndoParked)
	}
	return FileInfo{}, true, uerr
}

// remove deletes (scope, path) VERIFIED: the target is first moved aside
// to the intent's declared private name — never unlinked outright — and
// its identity is compared to it.dstFP, the object observed at declare.
// Only the expected object is unlinked, in place under the single-epoch
// private name; a foreign object is restored to the path (or left parked
// under the private name if a racing writer claimed it). A stale remove
// therefore cannot destroy a successor's newer save.
// The bool result reports whether the object is gone — an error after
// that point is observation, not non-commit (F-RA-5/f120).
func (p *posixRoot) remove(scope, path string, it intent) (bool, error) {
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
	stage := it.stageBase()
	if stage == "" {
		// No journal — still a private recovery name.
		stage = opStagePrefix + "adhoc-" + randHex(8)
	}
	// Non-destructive capture: move the target to the private name —
	// the act is journaled before the syscall can populate it. A
	// leftover parked object occupying the name refuses the op rather
	// than clobber recovery bytes.
	it.njAct("cap", rel)
	err = unix.Renameat2(int(pfd.Fd()), name, int(pfd.Fd()), stage, unix.RENAME_NOREPLACE)
	switch {
	case errors.Is(err, unix.ENOENT):
		it.njRes("noeff")
		return false, ErrNotFound
	case errors.Is(err, unix.EEXIST):
		it.njRes("noeff")
		return false, ErrConflict
	case err != nil:
		it.njRes("noeff")
		return false, mapPathErr(err)
	}
	it.njRes("ok")
	st3, _, _, serr := fp3at(pfd, stage)
	if serr != nil {
		// Captured but unobservable — put it back before reporting.
		rerr := unix.Renameat2(int(pfd.Fd()), stage, int(pfd.Fd()), name, unix.RENAME_NOREPLACE)
		if rerr != nil {
			return false, fmt.Errorf("%w: %w", serr, errUndoParked)
		}
		return false, serr
	}
	if st3 == fp3(it.dstFP) {
		// Captured the declared object — unlink it in place under the
		// owned private name (identity re-verified inside discardOwned).
		uerr := discardOwned(pfd, stage, st3)
		switch {
		case uerr == nil:
			it.njDone()
			// If a racing reconcile pass restored the captured object
			// to its name, the removal did not land — the name holding
			// an object again means the intent's purpose is unmet.
			if _, _, _, oerr := fp3at(pfd, name); oerr == nil {
				return false, ErrExternalChange
			}
			syncDir(pfd)
			return true, nil
		case errors.Is(uerr, ErrConflict):
			// Foreign bytes reached the slot — parked, not destroyed;
			// the reconciler settles them.
			return false, fmt.Errorf("%w: %w", ErrExternalChange, errUndoParked)
		case errors.Is(uerr, ErrNotEmpty):
			// A dir gained members after capture — it diverged from the
			// declared object; restore it instead of deleting.
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
		default:
			// The post-capture state is uncertain — tombstone so the
			// reconciler settles whatever was left.
			return false, fmt.Errorf("%w: %w", uerr, errUndoParked)
		}
	}
	// Captured object is not what the intent declared — put it back. A
	// racing writer's object occupying the name keeps it; the captured
	// object stays parked at the private name for recovery.
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
	if p.faultHook != nil {
		p.faultHook("mkdir.postCreate")
	}
	// The opened descriptor IS the object this mkdir committed — an
	// fd-bound stat observes it even if a racer replaces the name a
	// microsecond later. A post-hoc stat of the public path could
	// observe a different object and journal foreign content as this
	// op's acknowledgement — the same false-identity class as the
	// write/rename commit paths.
	st, serr := pfd.Stat()
	oid, _, nlink := p.fileIdentity(pfd)
	pfd.Close()
	syncDir(sfd)
	if serr != nil {
		return FileInfo{}, true, mapPathErr(serr)
	}
	return FileInfo{Kind: kindOf(st.Mode()), Size: st.Size(),
		MtimeNS: st.ModTime().UnixNano(), Fingerprint: fingerprint(st),
		DevIno: devIno(st), Oid: oid, Nlink: nlink}, true, nil
}

// The .filesv-tmp- prefix remains reserved (checkReserved and the
// listings filter) but nothing mints it and nothing deletes it: a
// vestigial startup sweep that unlinked aged names once lived here —
// it destroyed foreign deposits whose disposal was never authorized
// (F258/B-N3), so it was removed rather than re-gated.
