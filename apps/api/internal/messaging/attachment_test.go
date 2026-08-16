package messaging

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// pngHeader is enough for http.DetectContentType to sniff image/png.
var pngHeader = []byte("\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00")

type attachmentFixture struct {
	world
	root  string
	blobs *DiskAttachments
}

func newAttachmentWorld(t *testing.T, ctx context.Context, policy AttachmentPolicy) attachmentFixture {
	t.Helper()
	w := newWorld(t, ctx)
	root := filepath.Join(t.TempDir(), "attachments")
	blobs, err := NewDiskAttachments(root)
	if err != nil {
		t.Fatalf("new disk attachments: %v", err)
	}
	if policy.WorkspaceQuotaBytes == 0 {
		policy.WorkspaceQuotaBytes = 64 << 20
	}
	if policy.WorkspaceQuotaObjects == 0 {
		policy.WorkspaceQuotaObjects = 10_000
	}
	if policy.TotalQuotaBytes == 0 {
		policy.TotalQuotaBytes = 256 << 20
	}
	if policy.TotalQuotaObjects == 0 {
		policy.TotalQuotaObjects = 50_000
	}
	if err := w.store.core.ConfigureAttachments(blobs, policy); err != nil {
		t.Fatalf("configure attachments: %v", err)
	}
	return attachmentFixture{world: w, root: root, blobs: blobs}
}

func admitAlways(op func() error) (bool, error) { return true, op() }

// upload runs the shared upload state machine as one transport would.
func (f attachmentFixture) upload(t *testing.T, ctx context.Context, scoped *ScopedStore, placeID, nonce, filename, mime string, data []byte) (Attachment, bool, error) {
	t.Helper()
	req := attachmentUploadRequest{
		placeID: placeID, clientNonce: nonce, filename: sanitizeAttachmentFilename(filename),
		declaredMIME: mime, declaredSize: int64(len(data)),
	}
	att, created, admitted, err := uploadAttachment(ctx, scoped, req, admitAlways, nil, bytes.NewReader(data))
	if err != nil {
		return Attachment{}, false, err
	}
	if !admitted {
		t.Fatalf("upload admission unexpectedly refused")
	}
	return att, created, nil
}

func (f attachmentFixture) mustUpload(t *testing.T, ctx context.Context, scoped *ScopedStore, placeID, nonce, filename, mime string, data []byte) Attachment {
	t.Helper()
	att, created, err := f.upload(t, ctx, scoped, placeID, nonce, filename, mime, data)
	if err != nil {
		t.Fatalf("upload %s: %v", filename, err)
	}
	if !created {
		t.Fatalf("upload %s: expected a fresh attachment", filename)
	}
	return att
}

func (f attachmentFixture) usedBytes(t *testing.T, ctx context.Context, workspaceID string) int64 {
	t.Helper()
	var used int64
	err := f.store.core.pool.QueryRow(ctx,
		"SELECT COALESCE((SELECT used_bytes FROM message_attachment_quotas WHERE workspace_id=$1), 0)",
		workspaceID).Scan(&used)
	if err != nil {
		t.Fatalf("read quota: %v", err)
	}
	return used
}

func (f attachmentFixture) usedObjects(t *testing.T, ctx context.Context, workspaceID string) int64 {
	t.Helper()
	var used int64
	err := f.store.core.pool.QueryRow(ctx,
		"SELECT COALESCE((SELECT object_count FROM message_attachment_quotas WHERE workspace_id=$1), 0)",
		workspaceID).Scan(&used)
	if err != nil {
		t.Fatalf("read workspace object quota: %v", err)
	}
	return used
}

func (f attachmentFixture) totalUsage(t *testing.T, ctx context.Context) (int64, int64) {
	t.Helper()
	var bytes, objects int64
	err := f.store.core.pool.QueryRow(ctx,
		"SELECT COALESCE((SELECT used_bytes FROM message_attachment_store_usage WHERE singleton), 0), COALESCE((SELECT object_count FROM message_attachment_store_usage WHERE singleton), 0)").Scan(&bytes, &objects)
	if err != nil {
		t.Fatalf("read attachment store usage: %v", err)
	}
	return bytes, objects
}

func (f attachmentFixture) blobPath(id string) string {
	return filepath.Join(f.root, id[0:2], id[2:4], id+".bin")
}

func readBlob(t *testing.T, blobs AttachmentBlobs, id string) []byte {
	t.Helper()
	blob, err := blobs.Open(id)
	if err != nil {
		t.Fatalf("open blob %s: %v", id, err)
	}
	defer blob.Close()
	data, err := io.ReadAll(blob)
	if err != nil {
		t.Fatalf("read blob: %v", err)
	}
	return data
}

func TestAttachmentUploadBindProjectionAndReplayDigest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	f := newAttachmentWorld(t, ctx, AttachmentPolicy{})
	workspace, channel := f.workspaceWithChannel(t, ctx)
	sender := f.store.mustScope(t, ctx, workspace.WorkspaceID, f.humanA)
	reader := f.store.mustScope(t, ctx, workspace.WorkspaceID, f.humanB)

	text := []byte("hello attachments\n")
	image := append(append([]byte{}, pngHeader...), bytes.Repeat([]byte{0x7f}, 128)...)
	textAtt := f.mustUpload(t, ctx, sender, channel.PlaceID, "n-text", "notes.txt", "text/plain; charset=utf-8", text)
	imageAtt := f.mustUpload(t, ctx, sender, channel.PlaceID, "n-image", "shot.png", "application/octet-stream", image)
	if textAtt.MIME != "text/plain" {
		t.Fatalf("text MIME: got %q", textAtt.MIME)
	}
	if imageAtt.MIME != "image/png" {
		t.Fatalf("image MIME should be sniffed from bytes: got %q", imageAtt.MIME)
	}
	if sum := sha256.Sum256(text); !bytes.Equal(sum[:], textAtt.SHA256) {
		t.Fatal("text digest mismatch")
	}
	// Bytes are durable under the canonical sharded layout, mode 0600.
	info, err := os.Lstat(f.blobPath(textAtt.AttachmentID))
	if err != nil {
		t.Fatalf("blob path: %v", err)
	}
	if info.Mode().Perm() != 0o600 || !info.Mode().IsRegular() {
		t.Fatalf("blob mode %v", info.Mode())
	}
	if used := f.usedBytes(t, ctx, workspace.WorkspaceID); used != int64(len(text)+len(image)) {
		t.Fatalf("ledger used %d, want %d", used, len(text)+len(image))
	}

	// A retry with the same nonce and size returns the first receipt without
	// consuming the body or minting a second row.
	replay, created, err := f.upload(t, ctx, sender, channel.PlaceID, "n-text", "other-name.txt", "text/plain", text)
	if err != nil || created || replay.AttachmentID != textAtt.AttachmentID {
		t.Fatalf("nonce replay: %+v created=%v err=%v", replay, created, err)
	}
	if _, _, err := f.upload(t, ctx, sender, channel.PlaceID, "n-text", "notes.txt", "text/plain", append(text, '!')); !errors.Is(err, ErrAttachmentUploadConflict) {
		t.Fatalf("changed body under same nonce: %v", err)
	}
	// Unbound drafts are visible to their uploader only.
	if _, err := reader.AttachmentForViewer(ctx, textAtt.AttachmentID); !errors.Is(err, ErrAttachmentNotFound) {
		t.Fatalf("other member reading an unbound draft: %v", err)
	}
	if _, err := sender.AttachmentForViewer(ctx, textAtt.AttachmentID); err != nil {
		t.Fatalf("uploader reading own draft: %v", err)
	}

	// Attachment-only message in the sender's (reversed) order.
	msg, created, err := sender.AppendMessage(ctx, AppendInput{
		PlaceID: channel.PlaceID, Content: "", ClientNonce: "m-1",
		AttachmentIDs: []string{imageAtt.AttachmentID, textAtt.AttachmentID},
	})
	if err != nil || !created {
		t.Fatalf("attachment-only send: %v created=%v", err, created)
	}
	if len(msg.Attachments) != 2 || msg.Attachments[0].AttachmentID != imageAtt.AttachmentID || msg.Attachments[1].Position != 1 {
		t.Fatalf("send projection: %+v", msg.Attachments)
	}
	// Same nonce, same request: the original receipt. Same nonce, different
	// order or text: a conflict, never a silent replay.
	again, created, err := sender.AppendMessage(ctx, AppendInput{
		PlaceID: channel.PlaceID, Content: "", ClientNonce: "m-1",
		AttachmentIDs: []string{imageAtt.AttachmentID, textAtt.AttachmentID},
	})
	if err != nil || created || again.MessageID != msg.MessageID || len(again.Attachments) != 2 {
		t.Fatalf("send replay: %v created=%v msg=%+v", err, created, again)
	}
	if _, _, err := sender.AppendMessage(ctx, AppendInput{
		PlaceID: channel.PlaceID, Content: "", ClientNonce: "m-1",
		AttachmentIDs: []string{textAtt.AttachmentID, imageAtt.AttachmentID},
	}); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("reordered replay: %v", err)
	}
	if _, _, err := sender.AppendMessage(ctx, AppendInput{
		PlaceID: channel.PlaceID, Content: "changed", ClientNonce: "m-1",
		AttachmentIDs: []string{imageAtt.AttachmentID, textAtt.AttachmentID},
	}); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed-text replay: %v", err)
	}
	// Plain text messages get the same conflict rule.
	if _, _, err := sender.AppendMessage(ctx, AppendInput{PlaceID: channel.PlaceID, Content: "a", ClientNonce: "m-2"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := sender.AppendMessage(ctx, AppendInput{PlaceID: channel.PlaceID, Content: "b", ClientNonce: "m-2"}); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed text under same nonce: %v", err)
	}

	// History, catch-up, and OpenSnapshot project the same ordered list for
	// another member, who may now download both.
	history, err := reader.History(ctx, channel.PlaceID, HistoryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var found *Message
	for i := range history {
		if history[i].MessageID == msg.MessageID {
			found = &history[i]
		}
	}
	if found == nil || len(found.Attachments) != 2 || found.Attachments[0].Filename != "shot.png" || found.Attachments[1].Filename != "notes.txt" {
		t.Fatalf("history projection: %+v", found)
	}
	since, err := reader.MessagesSince(ctx, channel.PlaceID, msg.Seq-1, 10)
	if err != nil || len(since) == 0 || len(since[0].Attachments) != 2 {
		t.Fatalf("catch-up projection: %v %+v", err, since)
	}
	snapshot, err := reader.OpenSnapshot(ctx, channel.PlaceID, HistoryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Messages) == 0 || len(snapshot.Messages[len(snapshot.Messages)-2].Attachments) != 2 {
		t.Fatalf("open snapshot projection: %+v", snapshot.Messages)
	}
	for _, att := range []Attachment{textAtt, imageAtt} {
		visible, err := reader.AttachmentForViewer(ctx, att.AttachmentID)
		if err != nil {
			t.Fatalf("member reading bound attachment: %v", err)
		}
		if visible.MessageID != msg.MessageID {
			t.Fatalf("bound attachment message: %+v", visible)
		}
	}
	if got := readBlob(t, f.blobs, imageAtt.AttachmentID); !bytes.Equal(got, image) {
		t.Fatal("image bytes differ")
	}
	// A bound attachment cannot be bound again, and the ledger did not move.
	if _, _, err := sender.AppendMessage(ctx, AppendInput{
		PlaceID: channel.PlaceID, Content: "again", ClientNonce: "m-3",
		AttachmentIDs: []string{textAtt.AttachmentID},
	}); !errors.Is(err, ErrAttachmentNotFound) {
		t.Fatalf("rebinding a bound attachment: %v", err)
	}
	if used := f.usedBytes(t, ctx, workspace.WorkspaceID); used != int64(len(text)+len(image)) {
		t.Fatalf("ledger moved on bind: %d", used)
	}
}

func TestAttachmentBindFailureRollsBackTheWholeMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	f := newAttachmentWorld(t, ctx, AttachmentPolicy{})
	workspace, channel := f.workspaceWithChannel(t, ctx)
	sender := f.store.mustScope(t, ctx, workspace.WorkspaceID, f.humanA)
	other := f.store.mustScope(t, ctx, workspace.WorkspaceID, f.humanB)
	// A second channel proves place isolation of drafts.
	otherPlace, err := f.store.CreateChannel(ctx, workspace.WorkspaceID, "random", "", f.humanA)
	if err != nil {
		t.Fatal(err)
	}
	mine := f.mustUpload(t, ctx, sender, channel.PlaceID, "n1", "a.txt", "text/plain", []byte("mine"))
	theirs := f.mustUpload(t, ctx, other, channel.PlaceID, "n2", "b.txt", "text/plain", []byte("theirs"))
	elsewhere := f.mustUpload(t, ctx, sender, otherPlace.PlaceID, "n3", "c.txt", "text/plain", []byte("elsewhere"))

	type durableState struct{ messages, lastSeq, intents, bound int64 }
	snapshot := func() durableState {
		t.Helper()
		var state durableState
		if err := f.store.core.pool.QueryRow(ctx, `
			SELECT (SELECT count(*) FROM messages WHERE place_id=$1),
			       (SELECT last_seq FROM places WHERE place_id=$1),
			       (SELECT count(*) FROM message_notification_intents i JOIN messages m ON m.message_id=i.message_id WHERE m.place_id=$1),
			       (SELECT count(*) FROM message_attachments WHERE message_id IS NOT NULL)`,
			channel.PlaceID).Scan(&state.messages, &state.lastSeq, &state.intents, &state.bound); err != nil {
			t.Fatal(err)
		}
		return state
	}
	before := snapshot()
	cases := map[string][]string{
		"unknown id":          {"0190aaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa"},
		"malformed id":        {"not-a-uuid"},
		"someone else's":      {theirs.AttachmentID},
		"another place":       {elsewhere.AttachmentID},
		"duplicate":           {mine.AttachmentID, mine.AttachmentID},
		"good then bad order": {mine.AttachmentID, theirs.AttachmentID},
	}
	for name, ids := range cases {
		_, _, err := sender.AppendMessage(ctx, AppendInput{
			PlaceID: channel.PlaceID, Content: "@Haru look", ClientNonce: "nonce-" + name,
			AttachmentIDs: ids,
		})
		if !errors.Is(err, ErrAttachmentNotFound) {
			t.Fatalf("%s: got %v", name, err)
		}
		if after := snapshot(); after != before {
			t.Fatalf("%s: durable state changed %+v -> %+v", name, before, after)
		}
	}
	// Even the successful candidate is still unbound after all those rollbacks.
	if att, err := sender.AttachmentForViewer(ctx, mine.AttachmentID); err != nil || att.MessageID != "" {
		t.Fatalf("draft after rollbacks: %+v %v", att, err)
	}
	if _, _, err := sender.AppendMessage(ctx, AppendInput{PlaceID: channel.PlaceID, Content: "", ClientNonce: "empty"}); err == nil {
		t.Fatal("empty content without attachments must be rejected")
	}
	// The database enforces the same rule against a direct insert.
	if _, err := f.store.core.pool.Exec(ctx, `
		INSERT INTO messages (message_id, workspace_id, place_id, seq, author_kind, author_id, content, client_nonce)
		VALUES ($1, $2, $3, 999, 'human', $4, '', 'direct-empty')`,
		newUUIDv7(), workspace.WorkspaceID, channel.PlaceID, f.humanA.ID); err == nil {
		t.Fatal("database accepted an empty message without attachments")
	}
	// Cross-workspace binds are impossible at the database level.
	otherWorkspace, err := f.store.CreateWorkspace(ctx, "elsewhere", f.humanA)
	if err != nil {
		t.Fatal(err)
	}
	otherChannel, err := f.store.CreateChannel(ctx, otherWorkspace.WorkspaceID, "general", "", f.humanA)
	if err != nil {
		t.Fatal(err)
	}
	foreignSender := f.store.mustScope(t, ctx, otherWorkspace.WorkspaceID, f.humanA)
	foreignMsg := f.send(t, ctx, otherChannel.PlaceID, f.humanA, "over there")
	_ = foreignSender
	if _, err := f.store.core.pool.Exec(ctx, `
		UPDATE message_attachments SET message_id=$1, bound_at=now() WHERE attachment_id=$2`,
		foreignMsg.MessageID, mine.AttachmentID); err == nil {
		t.Fatal("database allowed a cross-workspace bind")
	}
}

func TestAttachmentQuotaDraftBudgetAndConcurrentReservations(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	const quota = MaxAttachmentBytes + 3<<20 // 23 MiB: room for one max file plus small ones
	f := newAttachmentWorld(t, ctx, AttachmentPolicy{WorkspaceQuotaBytes: quota, ReservationTTL: 2 * time.Second})
	workspace, channel := f.workspaceWithChannel(t, ctx)
	sender := f.store.mustScope(t, ctx, workspace.WorkspaceID, f.humanA)

	// Reservations count against the ledger before any body is accepted, and
	// changing the nonce does not buy more space.
	one := int64(1 << 20)
	var reserved []AttachmentUploadReservation
	for i := 0; i < 23; i++ {
		receipt, err := sender.ReserveAttachmentUpload(ctx, channel.PlaceID, fmt.Sprintf("r-%d", i), one)
		if i >= 10 {
			// The per-uploader/place draft budget is one full message.
			if !errors.Is(err, ErrAttachmentDraftLimit) {
				t.Fatalf("reservation %d: got %v, want draft limit", i, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("reservation %d: %v", i, err)
		}
		reserved = append(reserved, *receipt.Reservation)
	}
	if used := f.usedBytes(t, ctx, workspace.WorkspaceID); used != 10*one {
		t.Fatalf("ledger after 10 reservations: %d", used)
	}
	// Another uploader in the same Workspace shares the Workspace cap.
	other := f.store.mustScope(t, ctx, workspace.WorkspaceID, f.humanB)
	if _, err := other.ReserveAttachmentUpload(ctx, channel.PlaceID, "big", MaxAttachmentBytes); !errors.Is(err, ErrAttachmentQuotaExceeded) {
		t.Fatalf("workspace cap: %v", err)
	}
	if _, err := other.ReserveAttachmentUpload(ctx, channel.PlaceID, "fits", quota-10*one); err != nil {
		t.Fatalf("exact remaining bytes must fit: %v", err)
	}
	if _, err := other.ReserveAttachmentUpload(ctx, channel.PlaceID, "one-more", 1); !errors.Is(err, ErrAttachmentQuotaExceeded) {
		t.Fatalf("one byte over the cap: %v", err)
	}
	// Resuming a live reservation with a different declared size is a conflict.
	if _, err := sender.ReserveAttachmentUpload(ctx, channel.PlaceID, "r-0", one+1); !errors.Is(err, ErrAttachmentUploadConflict) {
		t.Fatalf("declared size drift: %v", err)
	}
	// Expired reservations release their bytes only through reconciliation.
	time.Sleep(2200 * time.Millisecond)
	report, err := f.store.core.ReconcileAttachments(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.ReleasedReservations != 11 {
		t.Fatalf("released %d reservations", report.ReleasedReservations)
	}
	if used := f.usedBytes(t, ctx, workspace.WorkspaceID); used != 0 {
		t.Fatalf("ledger after release: %d", used)
	}
	// Finalizing against a released reservation fails closed and leaves no
	// staged bytes behind.
	staged, err := f.blobs.Stage(reserved[0].UploadID, bytes.NewReader(bytes.Repeat([]byte{1}, int(one))), one)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = sender.FinalizeAttachmentUpload(ctx, channel.PlaceID, StagedAttachment{
		UploadID: reserved[0].UploadID, Filename: "late.bin", MIME: "application/octet-stream",
		Size: one, SHA256: staged.SHA256, StageToken: reserved[0].StageToken, Handle: staged,
	})
	if !errors.Is(err, ErrAttachmentUploadExpired) || !AttachmentFinalizeDefinitelyNotCommitted(err) {
		t.Fatalf("finalize after expiry: %v", err)
	}
	if entries, _ := filepath.Glob(filepath.Join(f.root, "*", "*", ".staging-*")); len(entries) != 0 {
		t.Fatalf("staging debris left: %v", entries)
	}
	// A re-reservation of a released nonce reuses the identity and re-charges.
	receipt, err := sender.ReserveAttachmentUpload(ctx, channel.PlaceID, "r-0", one)
	if err != nil || receipt.Reservation == nil || receipt.Reservation.UploadID != reserved[0].UploadID {
		t.Fatalf("re-reserve released nonce: %+v %v", receipt, err)
	}
	if used := f.usedBytes(t, ctx, workspace.WorkspaceID); used != one {
		t.Fatalf("ledger after re-reserve: %d", used)
	}

	// Concurrent reservations from many nonces never exceed the cap.
	f2 := newAttachmentWorld(t, ctx, AttachmentPolicy{WorkspaceQuotaBytes: MaxAttachmentBytes})
	ws2, ch2 := f2.workspaceWithChannel(t, ctx)
	scopes := []*ScopedStore{
		f2.store.mustScope(t, ctx, ws2.WorkspaceID, f2.humanA),
		f2.store.mustScope(t, ctx, ws2.WorkspaceID, f2.humanB),
		f2.store.mustScope(t, ctx, ws2.WorkspaceID, f2.agent),
	}
	var wg sync.WaitGroup
	var granted sync.Map
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			receipt, err := scopes[i%3].ReserveAttachmentUpload(ctx, ch2.PlaceID, fmt.Sprintf("c-%d", i), 3<<20)
			if err == nil {
				granted.Store(i, receipt.Reservation.UploadID)
			} else if !errors.Is(err, ErrAttachmentQuotaExceeded) && !errors.Is(err, ErrAttachmentDraftLimit) {
				t.Errorf("concurrent reservation %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()
	count := 0
	granted.Range(func(_, _ any) bool { count++; return true })
	if count != 6 { // 6 * 3 MiB = 18 MiB fits; the seventh would exceed 20 MiB
		t.Fatalf("granted %d concurrent reservations under a 20 MiB cap", count)
	}
	if used := f2.usedBytes(t, ctx, ws2.WorkspaceID); used != 18<<20 {
		t.Fatalf("ledger after concurrent reservations: %d", used)
	}
}

func TestAttachmentWholeStoreByteAndObjectCapsReconcile(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	// Both dimensions are deliberately tiny. The policy still permits a full
	// single attachment per Workspace, but cannot be multiplied by creating
	// more Workspaces.
	f := newAttachmentWorld(t, ctx, AttachmentPolicy{
		WorkspaceQuotaBytes:   MaxAttachmentBytes,
		WorkspaceQuotaObjects: 1,
		TotalQuotaBytes:       MaxAttachmentBytes,
		TotalQuotaObjects:     2,
		ReservationTTL:        30 * time.Millisecond,
	})
	ws1, ch1 := f.workspaceWithChannel(t, ctx)
	s1 := f.store.mustScope(t, ctx, ws1.WorkspaceID, f.humanA)
	if _, err := s1.ReserveAttachmentUpload(ctx, ch1.PlaceID, "one", 12<<20); err != nil {
		t.Fatalf("first reservation: %v", err)
	}
	if _, err := s1.ReserveAttachmentUpload(ctx, ch1.PlaceID, "workspace-object", 1); !errors.Is(err, ErrAttachmentQuotaExceeded) {
		t.Fatalf("workspace object cap: %v", err)
	}
	ws2, err := f.store.CreateWorkspace(ctx, "second", f.humanA)
	if err != nil {
		t.Fatal(err)
	}
	ch2, err := f.store.CreateChannel(ctx, ws2.WorkspaceID, "general", "", f.humanA)
	if err != nil {
		t.Fatal(err)
	}
	s2 := f.store.mustScope(t, ctx, ws2.WorkspaceID, f.humanA)
	if _, err := s2.ReserveAttachmentUpload(ctx, ch2.PlaceID, "two", 8<<20); err != nil {
		t.Fatalf("second workspace exact remaining bytes: %v", err)
	}
	ws3, err := f.store.CreateWorkspace(ctx, "third", f.humanA)
	if err != nil {
		t.Fatal(err)
	}
	ch3, err := f.store.CreateChannel(ctx, ws3.WorkspaceID, "general", "", f.humanA)
	if err != nil {
		t.Fatal(err)
	}
	s3 := f.store.mustScope(t, ctx, ws3.WorkspaceID, f.humanA)
	if _, err := s3.ReserveAttachmentUpload(ctx, ch3.PlaceID, "global-object", 1); !errors.Is(err, ErrAttachmentQuotaExceeded) {
		t.Fatalf("whole-store byte/object cap: %v", err)
	}
	if bytes, objects := f.totalUsage(t, ctx); bytes != MaxAttachmentBytes || objects != 2 {
		t.Fatalf("whole-store usage = %d bytes/%d objects, want %d/2", bytes, objects, MaxAttachmentBytes)
	}
	if got := f.usedObjects(t, ctx, ws1.WorkspaceID); got != 1 {
		t.Fatalf("workspace one objects = %d, want 1", got)
	}
	time.Sleep(40 * time.Millisecond)
	report, err := f.store.core.ReconcileAttachments(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.ReleasedReservations != 2 {
		t.Fatalf("released %d reservations, want 2", report.ReleasedReservations)
	}
	if bytes, objects := f.totalUsage(t, ctx); bytes != 0 || objects != 0 {
		t.Fatalf("whole-store reconciliation left %d bytes/%d objects", bytes, objects)
	}
}

func TestAttachmentDeletedNonceNeverReturnsAReadyReceipt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	f := newAttachmentWorld(t, ctx, AttachmentPolicy{})
	workspace, channel := f.workspaceWithChannel(t, ctx)
	sender := f.store.mustScope(t, ctx, workspace.WorkspaceID, f.humanA)
	att := f.mustUpload(t, ctx, sender, channel.PlaceID, "stable-source-nonce", "doc.txt", "text/plain", []byte("document"))
	msg, _, err := sender.AppendMessage(ctx, AppendInput{PlaceID: channel.PlaceID, Content: "doc", ClientNonce: "message", AttachmentIDs: []string{att.AttachmentID}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sender.DeleteMessage(ctx, channel.PlaceID, msg.MessageID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.core.ReconcileAttachments(ctx); err != nil {
		t.Fatal(err)
	}
	if receipt, err := sender.ReserveAttachmentUpload(ctx, channel.PlaceID, "stable-source-nonce", int64(len("document"))); !errors.Is(err, ErrAttachmentUploadRetired) || receipt.Existing != nil {
		t.Fatalf("tombstoned nonce receipt = %+v, %v; want a retired non-ready receipt", receipt, err)
	}
}

func TestAttachmentDownloadHonorsPrivatePlaceVisibleFromSeq(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	f := newAttachmentWorld(t, ctx, AttachmentPolicy{})
	workspace, _ := f.workspaceWithChannel(t, ctx)
	sender := f.store.mustScope(t, ctx, workspace.WorkspaceID, f.humanA)
	reader := f.store.mustScope(t, ctx, workspace.WorkspaceID, f.humanB)
	group, err := sender.CreateGroupDM(ctx, []ParticipantRef{f.humanB, f.agent})
	if err != nil {
		t.Fatal(err)
	}
	att := f.mustUpload(t, ctx, sender, group.PlaceID, "private-visible", "doc.txt", "text/plain", []byte("private"))
	msg, _, err := sender.AppendMessage(ctx, AppendInput{PlaceID: group.PlaceID, Content: "doc", ClientNonce: "private-message", AttachmentIDs: []string{att.AttachmentID}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reader.AttachmentForViewer(ctx, att.AttachmentID); err != nil {
		t.Fatalf("initial private-place download: %v", err)
	}
	if _, err := f.store.core.pool.Exec(ctx, `
		UPDATE place_members SET visible_from_seq = $3
		WHERE workspace_id = $1 AND place_id = $2 AND member_kind = 'human' AND member_id = $4`,
		workspace.WorkspaceID, group.PlaceID, msg.Seq+1, f.humanB.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.AttachmentForViewer(ctx, att.AttachmentID); !errors.Is(err, ErrAttachmentNotFound) {
		t.Fatalf("pre-tenure private attachment = %v, want not found", err)
	}
}

func TestAttachmentConcurrentReserveAndReclaimHasNoDeadlock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	f := newAttachmentWorld(t, ctx, AttachmentPolicy{ReservationTTL: time.Millisecond})
	workspace, channel := f.workspaceWithChannel(t, ctx)
	sender := f.store.mustScope(t, ctx, workspace.WorkspaceID, f.humanA)
	for i := 0; i < 12; i++ {
		if _, err := sender.ReserveAttachmentUpload(ctx, channel.PlaceID, fmt.Sprintf("expired-%d", i), 1); err != nil {
			t.Fatalf("seed reservation %d: %v", i, err)
		}
		time.Sleep(2 * time.Millisecond)
		start := make(chan struct{})
		errs := make(chan error, 2)
		go func() {
			<-start
			_, err := f.store.core.releaseExpiredReservations(ctx, time.Now())
			errs <- err
		}()
		go func() {
			<-start
			_, err := sender.ReserveAttachmentUpload(ctx, channel.PlaceID, fmt.Sprintf("expired-%d", i), 1)
			errs <- err
		}()
		close(start)
		for range 2 {
			if err := <-errs; err != nil {
				var pgErr *pgconn.PgError
				if errors.As(err, &pgErr) && pgErr.Code == "40P01" {
					t.Fatalf("reserve/reclaim deadlocked on iteration %d: %v", i, err)
				}
				if !errors.Is(err, ErrAttachmentUploadExpired) && !errors.Is(err, ErrAttachmentUploadInProgress) {
					t.Fatalf("reserve/reclaim iteration %d: %v", i, err)
				}
			}
		}
		if _, err := f.store.core.releaseExpiredReservations(ctx, time.Now().Add(time.Hour)); err != nil {
			t.Fatalf("cleanup reservation %d: %v", i, err)
		}
	}
}

func TestAttachmentSameNonceHasOnePhysicalStager(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	f := newAttachmentWorld(t, ctx, AttachmentPolicy{})
	workspace, channel := f.workspaceWithChannel(t, ctx)
	sender := f.store.mustScope(t, ctx, workspace.WorkspaceID, f.humanA)
	req := attachmentUploadRequest{placeID: channel.PlaceID, clientNonce: "same-nonce", filename: "one.txt", declaredMIME: "text/plain", declaredSize: 4}
	reader, writer := io.Pipe()
	type result struct {
		att     Attachment
		created bool
		err     error
	}
	first := make(chan result, 1)
	go func() {
		att, created, _, err := uploadAttachment(ctx, sender, req, admitAlways, nil, reader)
		first <- result{att: att, created: created, err: err}
	}()
	deadline := time.Now().Add(2 * time.Second)
	for {
		entries, _ := filepath.Glob(filepath.Join(f.root, "*", "*", ".staging-*"+attachmentStagingSuffix))
		if len(entries) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first same-nonce upload never acquired a staging file")
		}
		time.Sleep(time.Millisecond)
	}
	for i := 0; i < 3; i++ {
		_, _, _, err := uploadAttachment(ctx, sender, req, admitAlways, nil, bytes.NewReader([]byte("data")))
		if !errors.Is(err, ErrAttachmentUploadInProgress) {
			t.Fatalf("concurrent same-nonce retry %d = %v, want staging in progress", i, err)
		}
	}
	entries, _ := filepath.Glob(filepath.Join(f.root, "*", "*", ".staging-*"+attachmentStagingSuffix))
	if len(entries) != 1 {
		t.Fatalf("physical staging files = %v, want exactly one", entries)
	}
	if _, err := writer.Write([]byte("data")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	got := <-first
	if got.err != nil || !got.created {
		t.Fatalf("first same-nonce upload = %+v", got)
	}
	entries, _ = filepath.Glob(filepath.Join(f.root, "*", "*", ".staging-*"+attachmentStagingSuffix))
	if len(entries) != 0 {
		t.Fatalf("staging remained after finalize: %v", entries)
	}
}

func TestAttachmentVisibilityTombstoneAndDeletionOutbox(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	f := newAttachmentWorld(t, ctx, AttachmentPolicy{})
	workspace, channel := f.workspaceWithChannel(t, ctx)
	sender := f.store.mustScope(t, ctx, workspace.WorkspaceID, f.humanA)
	reader := f.store.mustScope(t, ctx, workspace.WorkspaceID, f.humanB)
	att := f.mustUpload(t, ctx, sender, channel.PlaceID, "n1", "doc.txt", "text/plain", []byte("document"))
	msg, _, err := sender.AppendMessage(ctx, AppendInput{
		PlaceID: channel.PlaceID, Content: "see file", ClientNonce: "m1", AttachmentIDs: []string{att.AttachmentID},
	})
	if err != nil {
		t.Fatal(err)
	}
	// A foreign Workspace scope collapses to not-found even for the uploader.
	elsewhere, err := f.store.CreateWorkspace(ctx, "elsewhere", f.humanA)
	if err != nil {
		t.Fatal(err)
	}
	foreign := f.store.mustScope(t, ctx, elsewhere.WorkspaceID, f.humanA)
	if _, err := foreign.AttachmentForViewer(ctx, att.AttachmentID); !errors.Is(err, ErrAttachmentNotFound) {
		t.Fatalf("foreign scope: %v", err)
	}
	// Channel members receive the full channel history.
	if _, err := reader.AttachmentForViewer(ctx, att.AttachmentID); err != nil {
		t.Fatalf("channel member: %v", err)
	}
	// Membership loss collapses to not-found.
	if err := f.store.RemoveWorkspaceMember(ctx, workspace.WorkspaceID, f.humanB); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.AttachmentForViewer(ctx, att.AttachmentID); !errors.Is(err, ErrPlaceNotFound) && !errors.Is(err, ErrAttachmentNotFound) {
		t.Fatalf("removed member: %v", err)
	}
	// Disable the installation: the stale scope fails closed for reads.
	if _, err := f.apps.SetEnabledByID(ctx, sender.Scope.InstallationID, f.humanA, false); err != nil {
		t.Fatal(err)
	}
	if _, err := sender.AttachmentForViewer(ctx, att.AttachmentID); err == nil {
		t.Fatal("disabled installation must not serve attachments")
	}
	if _, err := f.apps.SetEnabledByID(ctx, sender.Scope.InstallationID, f.humanA, true); err != nil {
		t.Fatal(err)
	}
	if _, err := sender.AttachmentForViewer(ctx, att.AttachmentID); err == nil {
		t.Fatal("pre-disable epoch must not revive after re-enable")
	}
	current := f.store.mustScope(t, ctx, workspace.WorkspaceID, f.humanA)
	if _, err := current.AttachmentForViewer(ctx, att.AttachmentID); err != nil {
		t.Fatalf("current epoch: %v", err)
	}
	// Tombstone: bytes stay until the outbox worker confirms removal; the row
	// stays forever as the record; reads collapse to not-found immediately.
	if _, err := current.DeleteMessage(ctx, channel.PlaceID, msg.MessageID); err != nil {
		t.Fatal(err)
	}
	if _, err := current.AttachmentForViewer(ctx, att.AttachmentID); !errors.Is(err, ErrAttachmentNotFound) {
		t.Fatalf("tombstoned attachment read: %v", err)
	}
	var state string
	if err := f.store.core.pool.QueryRow(ctx, "SELECT blob_state FROM message_attachments WHERE attachment_id=$1", att.AttachmentID).Scan(&state); err != nil || state != AttachmentBlobDeleting {
		t.Fatalf("outbox state %q %v", state, err)
	}
	if _, err := os.Lstat(f.blobPath(att.AttachmentID)); err != nil {
		t.Fatalf("blob must survive the tombstone transaction: %v", err)
	}
	if used := f.usedBytes(t, ctx, workspace.WorkspaceID); used != int64(len("document")) {
		t.Fatalf("ledger before deletion confirm: %d", used)
	}
	history, err := current.History(ctx, channel.PlaceID, HistoryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range history {
		if m.MessageID == msg.MessageID && (!m.Deleted || len(m.Attachments) != 0) {
			t.Fatalf("tombstone still projects attachments: %+v", m)
		}
	}
	// Simulate a crash after unlink but before confirm: replay is idempotent.
	if err := os.Remove(f.blobPath(att.AttachmentID)); err != nil {
		t.Fatal(err)
	}
	report, err := f.store.core.ReconcileAttachments(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.DeletedBlobs != 1 || report.PurgedDraftRows != 0 {
		t.Fatalf("report %+v", report)
	}
	var deletedAt *time.Time
	if err := f.store.core.pool.QueryRow(ctx, "SELECT blob_state, blob_deleted_at FROM message_attachments WHERE attachment_id=$1", att.AttachmentID).Scan(&state, &deletedAt); err != nil || state != AttachmentBlobDeleted || deletedAt == nil {
		t.Fatalf("after confirm: %q %v %v", state, deletedAt, err)
	}
	if used := f.usedBytes(t, ctx, workspace.WorkspaceID); used != 0 {
		t.Fatalf("ledger after deletion confirm: %d", used)
	}
	if report, err := f.store.core.ReconcileAttachments(ctx); err != nil || report != (AttachmentReconciliation{}) {
		t.Fatalf("second pass must be a no-op: %+v %v", report, err)
	}
	// The blob inventory view excludes deleted rows.
	var inventory int
	if err := f.store.core.pool.QueryRow(ctx, "SELECT count(*) FROM message_attachment_blob_inventory").Scan(&inventory); err != nil || inventory != 0 {
		t.Fatalf("inventory %d %v", inventory, err)
	}
}

func TestAttachmentReconcilerReclaimsDraftsOrphansAndReportsMissing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	f := newAttachmentWorld(t, ctx, AttachmentPolicy{ReservationTTL: 50 * time.Millisecond, UnboundTTL: 100 * time.Millisecond})
	workspace, channel := f.workspaceWithChannel(t, ctx)
	sender := f.store.mustScope(t, ctx, workspace.WorkspaceID, f.humanA)
	draft := f.mustUpload(t, ctx, sender, channel.PlaceID, "d1", "draft.txt", "text/plain", []byte("draft"))
	kept := f.mustUpload(t, ctx, sender, channel.PlaceID, "k1", "kept.txt", "text/plain", []byte("kept-bytes"))
	if _, _, err := sender.AppendMessage(ctx, AppendInput{PlaceID: channel.PlaceID, Content: "keep", ClientNonce: "m", AttachmentIDs: []string{kept.AttachmentID}}); err != nil {
		t.Fatal(err)
	}
	// An orphan blob (published, no row) and a stale staging file.
	orphanID := newUUIDv7()
	staged, err := f.blobs.Stage(orphanID, bytes.NewReader([]byte("orphan")), 6)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.blobs.Commit(staged); err != nil {
		t.Fatal(err)
	}
	stale, err := f.blobs.Stage(newUUIDv7(), bytes.NewReader([]byte("stale")), 5)
	if err != nil {
		t.Fatal(err)
	}
	// A bound row whose blob vanished is reported, never repaired.
	if err := os.Remove(f.blobPath(kept.AttachmentID)); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-time.Hour)
	for _, path := range []string{f.blobPath(orphanID), stale.tempPath, f.blobPath(draft.AttachmentID)} {
		if err := os.Chtimes(path, old, old); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(150 * time.Millisecond)
	report, err := f.store.core.ReconcileAttachments(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.ExpiredDrafts != 1 || report.DeletedBlobs != 1 || report.PurgedDraftRows != 1 || report.OrphanBlobs != 1 || report.MissingBlobs != 1 {
		t.Fatalf("report %+v", report)
	}
	for _, path := range []string{f.blobPath(orphanID), stale.tempPath, f.blobPath(draft.AttachmentID)} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s should be gone: %v", path, err)
		}
	}
	var rows int
	if err := f.store.core.pool.QueryRow(ctx, "SELECT count(*) FROM message_attachments WHERE attachment_id=$1", draft.AttachmentID).Scan(&rows); err != nil || rows != 0 {
		t.Fatalf("expired draft row retained: %d %v", rows, err)
	}
	if used := f.usedBytes(t, ctx, workspace.WorkspaceID); used != int64(len("kept-bytes")) {
		t.Fatalf("ledger after draft expiry: %d", used)
	}
	// The draft nonce may be used again afterwards.
	if _, created, err := f.upload(t, ctx, sender, channel.PlaceID, "d1", "draft.txt", "text/plain", []byte("draft")); err != nil || !created {
		t.Fatalf("re-upload after expiry: %v created=%v", err, created)
	}
	// Reads of the missing blob collapse to not-found through the store.
	if _, err := f.blobs.Open(kept.AttachmentID); !errors.Is(err, ErrAttachmentNotFound) {
		t.Fatalf("missing blob open: %v", err)
	}
}

func TestAttachmentUploadPhasesFenceStaleScopeAndDuplicateNonce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	f := newAttachmentWorld(t, ctx, AttachmentPolicy{})
	workspace, channel := f.workspaceWithChannel(t, ctx)
	sender := f.store.mustScope(t, ctx, workspace.WorkspaceID, f.humanA)
	data := []byte("fenced upload body")

	// Reserve under epoch 1, disable + re-enable (epoch 2), then try to
	// finalize with the stale scope and with the current one.
	receipt, err := sender.ReserveAttachmentUpload(ctx, channel.PlaceID, "fence", int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	staged, err := f.blobs.Stage(receipt.Reservation.UploadID, bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.apps.SetEnabledByID(ctx, sender.Scope.InstallationID, f.humanA, false); err != nil {
		t.Fatal(err)
	}
	if _, err := f.apps.SetEnabledByID(ctx, sender.Scope.InstallationID, f.humanA, true); err != nil {
		t.Fatal(err)
	}
	stagedAttachment := StagedAttachment{
		UploadID: receipt.Reservation.UploadID, Filename: "f.txt", MIME: "text/plain",
		Size: int64(len(data)), SHA256: staged.SHA256, StageToken: receipt.Reservation.StageToken, Handle: staged,
	}
	_, _, err = sender.FinalizeAttachmentUpload(ctx, channel.PlaceID, stagedAttachment)
	if err == nil || !AttachmentFinalizeDefinitelyNotCommitted(err) {
		t.Fatalf("stale epoch finalize: %v", err)
	}
	if _, err := os.Lstat(f.blobPath(receipt.Reservation.UploadID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale finalize must not publish: %v", err)
	}
	// The current epoch cannot adopt a reservation fenced to the old one.
	current := f.store.mustScope(t, ctx, workspace.WorkspaceID, f.humanA)
	staged2, err := f.blobs.Stage(receipt.Reservation.UploadID, bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	stagedAttachment.Handle = staged2
	if _, _, err := current.FinalizeAttachmentUpload(ctx, channel.PlaceID, stagedAttachment); !errors.Is(err, ErrAttachmentUploadExpired) {
		t.Fatalf("epoch-fenced reservation under new epoch: %v", err)
	}
	// A fresh preflight under the current epoch resumes the same identity and
	// then finalizes.
	again, err := current.ReserveAttachmentUpload(ctx, channel.PlaceID, "fence", int64(len(data)))
	if err != nil || again.Reservation == nil || again.Reservation.UploadID != receipt.Reservation.UploadID {
		t.Fatalf("re-preflight: %+v %v", again, err)
	}
	staged3, err := f.blobs.Stage(receipt.Reservation.UploadID, bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	stagedAttachment.Handle = staged3
	stagedAttachment.StageToken = again.Reservation.StageToken
	att, created, err := current.FinalizeAttachmentUpload(ctx, channel.PlaceID, stagedAttachment)
	if err != nil || !created {
		t.Fatalf("finalize under current epoch: %v created=%v", err, created)
	}
	if got := readBlob(t, f.blobs, att.AttachmentID); !bytes.Equal(got, data) {
		t.Fatal("published bytes differ")
	}
	if used := f.usedBytes(t, ctx, workspace.WorkspaceID); used != int64(len(data)) {
		t.Fatalf("ledger after fenced upload: %d", used)
	}

	// Two concurrent uploads of the same nonce: exactly one creates, the
	// other returns the same receipt, and only one blob exists.
	body := bytes.Repeat([]byte{9}, 4096)
	var wg sync.WaitGroup
	results := make([]struct {
		att     Attachment
		created bool
		err     error
	}, 4)
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i].att, results[i].created, results[i].err = f.upload(t, ctx, current, channel.PlaceID, "dup", "dup.bin", "application/octet-stream", body)
		}(i)
	}
	wg.Wait()
	createdCount := 0
	inProgressCount := 0
	var id string
	for _, r := range results {
		if r.err != nil {
			if errors.Is(r.err, ErrAttachmentUploadInProgress) {
				inProgressCount++
				continue
			}
			t.Fatalf("duplicate upload: %v", r.err)
		}
		if r.created {
			createdCount++
		}
		if id == "" {
			id = r.att.AttachmentID
		} else if id != r.att.AttachmentID {
			t.Fatalf("duplicate nonce minted two ids")
		}
	}
	if createdCount != 1 {
		t.Fatalf("created=%d for one nonce", createdCount)
	}
	// A live duplicate either observes the durable receipt after the first
	// finalizes or receives the explicit single-stager retry response; neither
	// may create an extra staging file or attachment identity.
	if inProgressCount > 0 {
		replay, created, err := f.upload(t, ctx, current, channel.PlaceID, "dup", "dup.bin", "application/octet-stream", body)
		if err != nil || created || replay.AttachmentID != id {
			t.Fatalf("post-stager replay: %+v created=%v err=%v", replay, created, err)
		}
	}
	if entries, _ := filepath.Glob(filepath.Join(f.root, "*", "*", ".staging-*")); len(entries) != 0 {
		t.Fatalf("staging debris after duplicate uploads: %v", entries)
	}
	if used := f.usedBytes(t, ctx, workspace.WorkspaceID); used != int64(len(data)+len(body)) {
		t.Fatalf("ledger after duplicate uploads: %d", used)
	}
	// Removing the member fences finalization too.
	other := f.store.mustScope(t, ctx, workspace.WorkspaceID, f.humanB)
	r2, err := other.ReserveAttachmentUpload(ctx, channel.PlaceID, "leave", int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	staged4, err := f.blobs.Stage(r2.Reservation.UploadID, bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.store.RemoveWorkspaceMember(ctx, workspace.WorkspaceID, f.humanB); err != nil {
		t.Fatal(err)
	}
	if _, _, err := other.FinalizeAttachmentUpload(ctx, channel.PlaceID, StagedAttachment{
		UploadID: r2.Reservation.UploadID, Filename: "l.txt", MIME: "text/plain",
		Size: int64(len(data)), SHA256: staged4.SHA256, StageToken: r2.Reservation.StageToken, Handle: staged4,
	}); err == nil || !AttachmentFinalizeDefinitelyNotCommitted(err) {
		t.Fatalf("finalize after membership loss: %v", err)
	}
	if _, err := os.Lstat(f.blobPath(r2.Reservation.UploadID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("membership loss must not publish")
	}
}
