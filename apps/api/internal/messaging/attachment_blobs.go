package messaging

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

// StagedBlob is a backend-owned handle to bytes that were written and synced
// into a private staging file but not yet published under an attachment id.
type StagedBlob struct {
	ID       string
	tempPath string
	Size     int64
	SHA256   []byte
	// Head holds the first 512 bytes for server-side MIME sniffing.
	Head []byte
}

// AttachmentBlobs stores attachment bytes outside the database. Local disk is
// the production backend; the interface exists for tests and lifecycle tools.
type AttachmentBlobs interface {
	// Stage streams r into a private staging file for id. It refuses more
	// than expected bytes with ErrAttachmentTooLarge and fewer with
	// ErrAttachmentSizeMismatch, syncs the file, and returns its digest.
	Stage(id string, r io.Reader, expected int64) (StagedBlob, error)
	// Commit publishes a staged blob under its id with no-replace semantics
	// against live blobs. The caller must have proven that any existing file
	// at the final path is an ownerless artifact; only then is it replaced.
	Commit(staged StagedBlob) error
	// Discard removes a staging file. Missing is not an error.
	Discard(staged StagedBlob) error
	// Open returns the published bytes for range-capable delivery.
	Open(id string) (io.ReadSeekCloser, error)
	// Remove deletes a published blob. Missing is not an error.
	Remove(id string) error
	// Sweep removes staging files older than cutoff and returns the ids of
	// published blobs older than cutoff so the caller can reconcile them
	// against durable metadata. Blobs newer than cutoff are never reported,
	// so a finalization in flight is never mistaken for an orphan.
	Sweep(cutoff time.Time) ([]string, error)
}

// DiskAttachments keeps attachment bytes under one operator-configured root,
// sharded by the first two byte-pairs of the attachment id:
// <root>/01/90/0190....bin. This exact layout is shared with the backup and
// recovery tooling; do not change one without the other.
//
// The root's canonical target is pinned at construction and every directory
// the backend owns must be private (0700) and non-symlinked. Attachment ids
// are server-minted canonical UUIDv7 strings; filenames and file bytes never
// enter a storage path.
type DiskAttachments struct {
	root          string
	syncDirectory func(string) error
}

const (
	attachmentDirectoryMode fs.FileMode = 0o700
	attachmentFileMode      fs.FileMode = 0o600
	attachmentStagingPrefix             = ".staging-"
	attachmentStagingSuffix             = ".tmp"
)

// NewDiskAttachments returns a disk blob store rooted at the configured path.
// The operator must provision the parent hierarchy; the backend creates at
// most the configured root, resolves its canonical target once, and requires
// mode 0700 on every directory it owns.
func NewDiskAttachments(root string) (*DiskAttachments, error) {
	return newDiskAttachments(root, syncAttachmentDirectory)
}

func newDiskAttachments(root string, syncDirectory func(string) error) (*DiskAttachments, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return nil, errors.New("attachment root must not be empty")
	}
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve absolute attachment root: %w", err)
	}
	if err := os.Mkdir(absoluteRoot, attachmentDirectoryMode); err != nil && !errors.Is(err, os.ErrExist) {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("create attachment root: parent directory must already exist: %w", err)
		}
		return nil, fmt.Errorf("create attachment root: %w", err)
	}
	canonicalRoot, err := filepath.EvalSymlinks(absoluteRoot)
	if err != nil {
		return nil, fmt.Errorf("resolve canonical attachment root: %w", err)
	}
	canonicalRoot = filepath.Clean(canonicalRoot)
	if err := validateAttachmentDirectory(canonicalRoot, "root"); err != nil {
		return nil, err
	}
	if err := syncAttachmentDirectories(syncDirectory, canonicalRoot, filepath.Dir(canonicalRoot)); err != nil {
		return nil, fmt.Errorf("persist attachment root: %w", err)
	}
	return &DiskAttachments{root: canonicalRoot, syncDirectory: syncDirectory}, nil
}

// RootPath returns the canonical root pinned by NewDiskAttachments.
func (d *DiskAttachments) RootPath() string { return d.root }

func validateAttachmentDirectory(path, label string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect attachment %s: %w", label, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("attachment %s must be a real directory", label)
	}
	if info.Mode().Perm() != attachmentDirectoryMode {
		return fmt.Errorf("attachment %s permissions are %04o, want %04o", label, info.Mode().Perm(), attachmentDirectoryMode)
	}
	return nil
}

func syncAttachmentDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	if err := directory.Sync(); err != nil {
		_ = directory.Close()
		return fmt.Errorf("sync %s: %w", path, err)
	}
	if err := directory.Close(); err != nil {
		return fmt.Errorf("close %s: %w", path, err)
	}
	return nil
}

func syncAttachmentDirectories(syncDirectory func(string) error, paths ...string) error {
	for _, path := range paths {
		if err := syncDirectory(path); err != nil {
			return err
		}
	}
	return nil
}

func (d *DiskAttachments) shardDirectory(id string, create bool) (string, error) {
	if !validAttachmentID(id) {
		return "", ErrAttachmentNotFound
	}
	if err := validateAttachmentDirectory(d.root, "root"); err != nil {
		return "", err
	}
	directory := d.root
	for _, component := range []string{id[0:2], id[2:4]} {
		directory = filepath.Join(directory, component)
		if create {
			if err := os.Mkdir(directory, attachmentDirectoryMode); err != nil && !errors.Is(err, os.ErrExist) {
				return "", fmt.Errorf("create attachment shard: %w", err)
			}
		}
		if err := validateAttachmentDirectory(directory, "shard"); err != nil {
			if !create && errors.Is(err, os.ErrNotExist) {
				return "", ErrAttachmentNotFound
			}
			return "", err
		}
	}
	return directory, nil
}

func (d *DiskAttachments) finalPath(id string) (string, string, error) {
	directory, err := d.shardDirectory(id, false)
	if err != nil {
		return "", "", err
	}
	return directory, filepath.Join(directory, id+".bin"), nil
}

func (d *DiskAttachments) Stage(id string, r io.Reader, expected int64) (StagedBlob, error) {
	if expected <= 0 {
		return StagedBlob{}, ErrAttachmentEmpty
	}
	if expected > MaxAttachmentBytes {
		return StagedBlob{}, ErrAttachmentTooLarge
	}
	directory, err := d.shardDirectory(id, true)
	if err != nil {
		return StagedBlob{}, err
	}
	var suffix [8]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return StagedBlob{}, fmt.Errorf("staging name entropy: %w", err)
	}
	tempPath := filepath.Join(directory,
		attachmentStagingPrefix+id+"-"+hex.EncodeToString(suffix[:])+attachmentStagingSuffix)
	temp, err := os.OpenFile(tempPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, attachmentFileMode)
	if err != nil {
		return StagedBlob{}, fmt.Errorf("create attachment staging file: %w", err)
	}
	staged := StagedBlob{ID: id, tempPath: tempPath}
	committed := false
	defer func() {
		_ = temp.Close()
		if !committed {
			_ = os.Remove(tempPath)
		}
	}()
	if err := temp.Chmod(attachmentFileMode); err != nil {
		return StagedBlob{}, fmt.Errorf("chmod attachment staging file: %w", err)
	}
	digest := sha256.New()
	head := make([]byte, 0, 512)
	// One byte past the declared size is enough to know the body overran it.
	limited := io.LimitReader(r, expected+1)
	buffer := make([]byte, 256*1024)
	var size int64
	for {
		read, readErr := limited.Read(buffer)
		if read > 0 {
			chunk := buffer[:read]
			if len(head) < 512 {
				take := 512 - len(head)
				if take > len(chunk) {
					take = len(chunk)
				}
				head = append(head, chunk[:take]...)
			}
			if _, err := temp.Write(chunk); err != nil {
				return StagedBlob{}, fmt.Errorf("write attachment staging file: %w", err)
			}
			_, _ = digest.Write(chunk)
			size += int64(read)
			if size > expected {
				return StagedBlob{}, ErrAttachmentTooLarge
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return StagedBlob{}, readErr
		}
	}
	if size != expected {
		return StagedBlob{}, ErrAttachmentSizeMismatch
	}
	if err := temp.Sync(); err != nil {
		return StagedBlob{}, fmt.Errorf("sync attachment staging file: %w", err)
	}
	if err := temp.Close(); err != nil {
		return StagedBlob{}, fmt.Errorf("close attachment staging file: %w", err)
	}
	staged.Size = size
	staged.SHA256 = digest.Sum(nil)
	staged.Head = head
	committed = true
	return staged, nil
}

func (d *DiskAttachments) Commit(staged StagedBlob) error {
	if staged.tempPath == "" {
		return errors.New("staged attachment has no staging file")
	}
	directory, final, err := d.finalPath(staged.ID)
	if err != nil {
		return err
	}
	if filepath.Dir(staged.tempPath) != directory {
		return errors.New("staged attachment file is outside its shard")
	}
	info, err := os.Lstat(staged.tempPath)
	if err != nil {
		return fmt.Errorf("inspect staged attachment: %w", err)
	}
	if !info.Mode().IsRegular() {
		return errors.New("staged attachment is not a regular file")
	}
	// No-replace rename. EEXIST here can only be an artifact the caller has
	// proven ownerless (the metadata never committed), so it is unlinked once
	// and the rename is retried without replace semantics.
	renameNoReplace := func() error {
		return unix.Renameat2(unix.AT_FDCWD, staged.tempPath, unix.AT_FDCWD, final, unix.RENAME_NOREPLACE)
	}
	if err := renameNoReplace(); err != nil {
		if !errors.Is(err, unix.EEXIST) {
			return fmt.Errorf("publish attachment blob: %w", err)
		}
		if err := d.removeFinal(final); err != nil {
			return fmt.Errorf("replace ownerless attachment artifact: %w", err)
		}
		if err := renameNoReplace(); err != nil {
			return fmt.Errorf("publish attachment blob after artifact removal: %w", err)
		}
	}
	// The file fsync during Stage persisted bytes and inode metadata; the
	// rename is durable only after the directory entries are synced outward.
	if err := syncAttachmentDirectories(d.syncDirectory, directory, filepath.Dir(directory), d.root); err != nil {
		return fmt.Errorf("persist attachment name: %w", err)
	}
	return nil
}

func (d *DiskAttachments) Discard(staged StagedBlob) error {
	if staged.tempPath == "" {
		return nil
	}
	if !strings.HasPrefix(filepath.Base(staged.tempPath), attachmentStagingPrefix) {
		return errors.New("refusing to discard a non-staging path")
	}
	if err := os.Remove(staged.tempPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("discard attachment staging file: %w", err)
	}
	return nil
}

func (d *DiskAttachments) Open(id string) (io.ReadSeekCloser, error) {
	_, path, err := d.finalPath(id)
	if err != nil {
		return nil, err
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrAttachmentNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("inspect attachment: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("attachment blob is not a regular file")
	}
	file, err := os.OpenFile(path, os.O_RDONLY|unix.O_NOFOLLOW, 0)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrAttachmentNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("open attachment: %w", err)
	}
	opened, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("inspect opened attachment: %w", err)
	}
	if !opened.Mode().IsRegular() {
		_ = file.Close()
		return nil, errors.New("opened attachment blob is not a regular file")
	}
	return file, nil
}

func (d *DiskAttachments) removeFinal(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect attachment before remove: %w", err)
	}
	if !info.Mode().IsRegular() {
		return errors.New("attachment blob is not a regular file")
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove attachment: %w", err)
	}
	return nil
}

func (d *DiskAttachments) Remove(id string) error {
	directory, path, err := d.finalPath(id)
	if errors.Is(err, ErrAttachmentNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := d.removeFinal(path); err != nil {
		return err
	}
	// Removal is confirmed only once the directory entry is durably gone;
	// the caller releases quota after this returns.
	return d.syncDirectory(directory)
}

func (d *DiskAttachments) Sweep(cutoff time.Time) ([]string, error) {
	if err := validateAttachmentDirectory(d.root, "root"); err != nil {
		return nil, err
	}
	var older []string
	err := filepath.WalkDir(d.root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		info, err := entry.Info()
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("attachment storage contains a symlink: %s", path)
		}
		if info.IsDir() {
			return validateAttachmentDirectory(path, "directory")
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		if !info.ModTime().Before(cutoff) {
			return nil
		}
		name := entry.Name()
		if strings.HasPrefix(name, attachmentStagingPrefix) && strings.HasSuffix(name, attachmentStagingSuffix) {
			// A staging file this old is a write whose upload never finalized:
			// nothing can name it, so nothing can read it.
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("remove abandoned staging file: %w", err)
			}
			return nil
		}
		id := strings.TrimSuffix(name, ".bin")
		if id == name || !validAttachmentID(id) {
			return nil
		}
		older = append(older, id)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("sweep attachment root: %w", err)
	}
	return older, nil
}
