package messaging

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

// Both backends behind AttachmentBlobs answer the same contract. The disk
// backend is the reference; the PostgreSQL backend serves hosts whose disk is
// discarded on replacement.
func TestAttachmentBlobBackendsShareTheContract(t *testing.T) {
	backends := map[string]func(t *testing.T) (AttachmentBlobs, func(id string, at time.Time)){
		"disk": func(t *testing.T) (AttachmentBlobs, func(string, time.Time)) {
			blobs, err := NewDiskAttachments(filepath.Join(t.TempDir(), "attachments"))
			if err != nil {
				t.Fatal(err)
			}
			return blobs, nil
		},
		"postgres": func(t *testing.T) (AttachmentBlobs, func(string, time.Time)) {
			ctx := context.Background()
			pool := testdb.Create(t)
			if err := db.Migrate(ctx, pool); err != nil {
				t.Fatalf("migrate: %v", err)
			}
			blobs, err := NewPostgresAttachments(pool)
			if err != nil {
				t.Fatal(err)
			}
			age := func(id string, at time.Time) {
				if _, err := pool.Exec(ctx, `UPDATE messaging_attachment_blobs SET changed_at = $2 WHERE attachment_id = $1`, id, at); err != nil {
					t.Fatalf("age blob: %v", err)
				}
			}
			return blobs, age
		},
	}
	for name, open := range backends {
		t.Run(name, func(t *testing.T) {
			blobs, age := open(t)
			content := bytes.Repeat([]byte("0123456789abcdef"), 40_000) // 640000 bytes: several chunks
			id := newUUIDv7()

			if _, err := blobs.Stage(id, bytes.NewReader(nil), 0); !errors.Is(err, ErrAttachmentEmpty) {
				t.Fatalf("empty: %v", err)
			}
			if _, err := blobs.Stage(id, bytes.NewReader(nil), MaxAttachmentBytes+1); !errors.Is(err, ErrAttachmentTooLarge) {
				t.Fatalf("over the limit: %v", err)
			}
			if _, err := blobs.Stage("not-a-uuid", bytes.NewReader(content), int64(len(content))); !errors.Is(err, ErrAttachmentNotFound) {
				t.Fatalf("invalid id: %v", err)
			}
			if _, err := blobs.Stage(id, bytes.NewReader(content), int64(len(content))-1); !errors.Is(err, ErrAttachmentTooLarge) {
				t.Fatalf("body longer than declared: %v", err)
			}
			if _, err := blobs.Stage(id, bytes.NewReader(content[:10]), int64(len(content))); !errors.Is(err, ErrAttachmentSizeMismatch) {
				t.Fatalf("body shorter than declared: %v", err)
			}
			if exists, err := blobs.StagingExists(id); err != nil || exists {
				t.Fatalf("failed stages left staging behind: %v %v", exists, err)
			}

			staged, err := blobs.Stage(id, bytes.NewReader(content), int64(len(content)))
			if err != nil {
				t.Fatalf("stage: %v", err)
			}
			if sum := sha256.Sum256(content); staged.Size != int64(len(content)) || !bytes.Equal(staged.SHA256, sum[:]) || !bytes.Equal(staged.Head, content[:512]) {
				t.Fatalf("staged digest/head/size wrong: %d", staged.Size)
			}
			if exists, err := blobs.StagingExists(id); err != nil || !exists {
				t.Fatalf("staging exists: %v %v", exists, err)
			}
			if _, err := blobs.Stage(id, bytes.NewReader(content), int64(len(content))); !errors.Is(err, ErrAttachmentUploadInProgress) {
				t.Fatalf("second stager: %v", err)
			}
			if _, err := blobs.Open(id); !errors.Is(err, ErrAttachmentNotFound) {
				t.Fatalf("staged bytes readable before commit: %v", err)
			}
			if err := blobs.Commit(staged); err != nil {
				t.Fatalf("commit: %v", err)
			}
			if exists, err := blobs.StagingExists(id); err != nil || exists {
				t.Fatalf("staging after commit: %v %v", exists, err)
			}
			if got := readBlob(t, blobs, id); !bytes.Equal(got, content) {
				t.Fatalf("published bytes differ: %d bytes", len(got))
			}
			// Range delivery seeks across chunk boundaries.
			blob, err := blobs.Open(id)
			if err != nil {
				t.Fatal(err)
			}
			for _, offset := range []int64{262_140, 5, 600_000, 524_288} {
				if _, err := blob.Seek(offset, io.SeekStart); err != nil {
					t.Fatal(err)
				}
				window := make([]byte, 30_000)
				read, err := io.ReadFull(blob, window)
				want := content[offset:min(offset+30_000, int64(len(content)))]
				if (err != nil && !errors.Is(err, io.ErrUnexpectedEOF)) || !bytes.Equal(window[:read], want) {
					t.Fatalf("range at %d: %d bytes, %v", offset, read, err)
				}
			}
			if end, err := blob.Seek(0, io.SeekEnd); err != nil || end != int64(len(content)) {
				t.Fatalf("seek end: %d %v", end, err)
			}
			_ = blob.Close()

			// Publishing again over an ownerless published artifact replaces it.
			replacement := []byte("replacement")
			again, err := blobs.Stage(id, bytes.NewReader(replacement), int64(len(replacement)))
			if err != nil {
				t.Fatal(err)
			}
			if err := blobs.Commit(again); err != nil {
				t.Fatalf("commit over artifact: %v", err)
			}
			if got := readBlob(t, blobs, id); !bytes.Equal(got, replacement) {
				t.Fatalf("replacement bytes: %q", got)
			}

			// Discard and DiscardStaging remove staging only.
			discarded, err := blobs.Stage(id, bytes.NewReader([]byte("x")), 1)
			if err != nil {
				t.Fatal(err)
			}
			if err := blobs.Discard(discarded); err != nil {
				t.Fatal(err)
			}
			if err := blobs.Discard(discarded); err != nil {
				t.Fatalf("second discard: %v", err)
			}
			if _, err := blobs.Stage(id, bytes.NewReader([]byte("y")), 1); err != nil {
				t.Fatal(err)
			}
			if err := blobs.DiscardStaging(id); err != nil {
				t.Fatal(err)
			}
			if exists, err := blobs.StagingExists(id); err != nil || exists {
				t.Fatalf("staging after DiscardStaging: %v %v", exists, err)
			}
			if got := readBlob(t, blobs, id); !bytes.Equal(got, replacement) {
				t.Fatal("discarding staging touched the published blob")
			}

			// Sweep never reports a recent blob; an old one is reported and
			// an old staging blob is dropped.
			stale, err := blobs.Stage(newUUIDv7(), bytes.NewReader([]byte("stale")), 5)
			if err != nil {
				t.Fatal(err)
			}
			if ids, err := blobs.Sweep(time.Now().Add(-time.Minute)); err != nil || len(ids) != 0 {
				t.Fatalf("recent sweep: %v %v", ids, err)
			}
			if before, err := blobs.PublishedBefore(id, time.Now().Add(-time.Minute)); err != nil || before {
				t.Fatalf("recent PublishedBefore: %v %v", before, err)
			}
			old := time.Now().Add(-time.Hour)
			if age != nil {
				age(id, old)
				age(stale.ID, old)
			} else {
				agePath(t, blobs.(*DiskAttachments), id, stale, old)
			}
			ids, err := blobs.Sweep(time.Now().Add(-time.Minute))
			if err != nil || len(ids) != 1 || ids[0] != id {
				t.Fatalf("old sweep: %v %v", ids, err)
			}
			if exists, err := blobs.StagingExists(stale.ID); err != nil || exists {
				t.Fatalf("old staging survived sweep: %v %v", exists, err)
			}
			if before, err := blobs.PublishedBefore(id, time.Now().Add(-time.Minute)); err != nil || !before {
				t.Fatalf("old PublishedBefore: %v %v", before, err)
			}

			if err := blobs.Remove(id); err != nil {
				t.Fatal(err)
			}
			if err := blobs.Remove(id); err != nil {
				t.Fatalf("second remove: %v", err)
			}
			if _, err := blobs.Open(id); !errors.Is(err, ErrAttachmentNotFound) {
				t.Fatalf("open after remove: %v", err)
			}
			if before, err := blobs.PublishedBefore(id, time.Now()); err != nil || before {
				t.Fatalf("PublishedBefore after remove: %v %v", before, err)
			}
		})
	}
}

func agePath(t *testing.T, blobs *DiskAttachments, id string, stale StagedBlob, at time.Time) {
	t.Helper()
	_, final, err := blobs.finalPath(id)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{final, stale.tempPath} {
		if err := os.Chtimes(path, at, at); err != nil {
			t.Fatal(err)
		}
	}
}

// The Messaging store runs its whole attachment lifecycle on the PostgreSQL
// backend: upload, send, read back, and reconciliation of orphans, stale
// staging and missing bytes.
func TestAttachmentLifecycleOnPostgresBlobs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	pool := w.store.core.pool
	blobs, err := NewPostgresAttachments(pool)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.store.core.ConfigureAttachments(blobs, AttachmentPolicy{
		UnboundTTL:            100 * time.Millisecond,
		WorkspaceQuotaBytes:   64 << 20,
		WorkspaceQuotaObjects: 10_000,
		TotalQuotaBytes:       256 << 20,
		TotalQuotaObjects:     50_000,
	}); err != nil {
		t.Fatal(err)
	}
	f := attachmentFixture{world: w}
	workspace, channel := f.workspaceWithChannel(t, ctx)
	sender := f.store.mustScope(t, ctx, workspace.WorkspaceID, f.humanA)

	image := append(append([]byte{}, pngHeader...), bytes.Repeat([]byte{0x7f}, 300_000)...)
	sent := f.mustUpload(t, ctx, sender, channel.PlaceID, "n-image", "shot.png", "application/octet-stream", image)
	if sent.MIME != "image/png" {
		t.Fatalf("MIME sniffed from the staged head: %q", sent.MIME)
	}
	if replay, created, err := f.upload(t, ctx, sender, channel.PlaceID, "n-image", "shot.png", "image/png", image); err != nil || created || replay.AttachmentID != sent.AttachmentID {
		t.Fatalf("nonce replay: %+v %v %v", replay, created, err)
	}
	if _, _, err := sender.AppendMessage(ctx, AppendInput{PlaceID: channel.PlaceID, Content: "look", ClientNonce: "m", AttachmentIDs: []string{sent.AttachmentID}}); err != nil {
		t.Fatal(err)
	}
	if got := readBlob(t, blobs, sent.AttachmentID); !bytes.Equal(got, image) {
		t.Fatal("sent attachment bytes differ")
	}
	draft := f.mustUpload(t, ctx, sender, channel.PlaceID, "d1", "draft.txt", "text/plain", []byte("draft"))
	missing := f.mustUpload(t, ctx, sender, channel.PlaceID, "k1", "kept.txt", "text/plain", []byte("kept-bytes"))
	if _, _, err := sender.AppendMessage(ctx, AppendInput{PlaceID: channel.PlaceID, Content: "keep", ClientNonce: "m2", AttachmentIDs: []string{missing.AttachmentID}}); err != nil {
		t.Fatal(err)
	}
	orphanID := newUUIDv7()
	orphan, err := blobs.Stage(orphanID, bytes.NewReader([]byte("orphan")), 6)
	if err != nil {
		t.Fatal(err)
	}
	if err := blobs.Commit(orphan); err != nil {
		t.Fatal(err)
	}
	stale, err := blobs.Stage(newUUIDv7(), bytes.NewReader([]byte("stale")), 5)
	if err != nil {
		t.Fatal(err)
	}
	if err := blobs.Remove(missing.AttachmentID); err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`UPDATE messaging_attachment_blobs SET changed_at = now() - interval '1 hour'`,
		`UPDATE message_attachments SET created_at = now() - interval '1 hour'`,
		`UPDATE message_attachment_uploads
		 SET created_at = now() - interval '2 hours', expires_at = now() - interval '90 minutes', settled_at = now() - interval '1 hour'
		 WHERE settled_at IS NOT NULL`,
	} {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatalf("age rows: %v", err)
		}
	}
	report, err := f.store.core.ReconcileAttachments(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.ExpiredDrafts != 1 || report.DeletedBlobs != 1 || report.OrphanBlobs != 1 || report.MissingBlobs != 1 {
		t.Fatalf("report %+v", report)
	}
	for _, id := range []string{orphanID, draft.AttachmentID} {
		if _, err := blobs.Open(id); !errors.Is(err, ErrAttachmentNotFound) {
			t.Fatalf("%s should be gone: %v", id, err)
		}
	}
	if exists, err := blobs.StagingExists(stale.ID); err != nil || exists {
		t.Fatalf("stale staging survived: %v %v", exists, err)
	}
	if got := readBlob(t, blobs, sent.AttachmentID); !bytes.Equal(got, image) {
		t.Fatal("reconciliation touched a live attachment")
	}
}
