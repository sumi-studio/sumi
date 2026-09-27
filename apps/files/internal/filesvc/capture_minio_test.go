package filesvc

// capture_minio_test.go — composed capture/read against a REAL MinIO
// object backend (quay.io/minio/minio container) + native JuiceFS
// volume (storage=minio, 4MiB blocks, MetaVersion=1, TrashDays=1,
// HashPrefix=false, KeyEncrypted=true — the production format shape).
//
// Baseline bytes are read through the live mount (independent oracle);
// captured bytes come only over signed S3 GETs from MinIO.
//
//	CAPTURE_TEST_M_META_DSN    postgres DSN of the minio volume metadata
//	CAPTURE_TEST_M_MOUNT       live mount of that volume (oracle)
//	CAPTURE_TEST_S3_ENDPOINT   e.g. http://172.17.0.14:9000
//	CAPTURE_TEST_S3_BUCKET     expected bucket (capmini)
//	CAPTURE_TEST_S3_PREFIX     expected prefix (capvolmin — trailing / added)
//	CAPTURE_TEST_S3_ACCESS_KEY / CAPTURE_TEST_S3_SECRET_KEY  fixture creds

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type minioFixture struct {
	*capFixture
	s3 *S3Config
}

func minioEnv(t *testing.T) *minioFixture {
	t.Helper()
	mount := os.Getenv("CAPTURE_TEST_M_MOUNT")
	meta := os.Getenv("CAPTURE_TEST_M_META_DSN")
	ep := os.Getenv("CAPTURE_TEST_S3_ENDPOINT")
	bucket := os.Getenv("CAPTURE_TEST_S3_BUCKET")
	prefix := os.Getenv("CAPTURE_TEST_S3_PREFIX")
	ak := os.Getenv("CAPTURE_TEST_S3_ACCESS_KEY")
	sk := os.Getenv("CAPTURE_TEST_S3_SECRET_KEY")
	if mount == "" || meta == "" || ep == "" || bucket == "" || ak == "" || sk == "" {
		t.Skip("CAPTURE_TEST_M_*/S3_* env not set — minio fixture tests skipped")
	}
	if pgDSN(t) == "" {
		t.Skip("FILESV_TEST_DSN not set")
	}
	cfg := &S3Config{
		Endpoint: ep, AccessKey: ak, SecretKey: sk,
		Bucket: bucket, Prefix: prefix, client: http.DefaultClient,
	}
	return &minioFixture{
		capFixture: &capFixture{
			t: t, mount: mount, metaDSN: meta,
			scope: "caps" + randHex(4),
			owner: "sessA", epoch: 7,
			oracle: map[string][]byte{}, links: map[string]string{}, dirs: map[string]bool{},
		},
		s3: cfg,
	}
}

func (m *minioFixture) newService(t *testing.T, root string, s3 *S3Config) (*Service, *Store, *CaptureService) {
	t.Helper()
	dsn := pgDSN(t)
	resetTables(t, dsn)
	st, err := NewStore(context.Background(), dsn, root)
	if err != nil {
		t.Fatal(err)
	}
	// Shrink the deadGrace cut horizon — no predecessor writer exists in
	// the fixture, so the tenure gate would only stall the test.
	st.SetCutHorizon(0)
	st.SetDrainTimeout(2 * time.Second)
	cs, err := NewCaptureService(context.Background(), CaptureConfig{
		MetaDSN: m.metaDSN, ObjKind: "s3", S3: s3,
	}, st)
	if err != nil {
		t.Fatal(err)
	}
	svc, err := NewAt(root, st, map[string]map[string]bool{"adm": {"*": true}})
	if err != nil {
		t.Fatal(err)
	}
	svc.SetCapture(cs)
	return svc, st, cs
}

// s3Request issues a signed request against the fixture bucket — used
// for missing/short-object negatives (DELETE / short PUT). Reuses the
// adapter's own SigV4 so the fixture speaks the same auth dialect.
func (m *minioFixture) s3Request(t *testing.T, method, key string, body []byte) *http.Response {
	t.Helper()
	st := &s3ObjStore{cfg: m.s3}
	req, err := http.NewRequest(method,
		m.s3.Endpoint+"/"+s3Escape(m.s3.Bucket)+"/"+s3EscapePath(m.s3.Prefix+key),
		bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if len(body) == 0 {
		st.sign(req)
	} else {
		sum := sha256.Sum256(body)
		st.signPayload(req, hex.EncodeToString(sum[:]))
	}
	resp, err := m.s3.client.Do(req)
	if err != nil {
		t.Fatalf("s3 %s %s: %v", method, key, err)
	}
	return resp
}

// TestCaptureMinioComposed: real MinIO objects, mount baseline,
// post-capture mutation — captured bytes must equal the pre-mutation
// baseline. Also proves KeyEncrypted=true is NOT refused (the volume
// stores RSA-wrapped credentials, plaintext data — the production
// format shape).
func TestCaptureMinioComposed(t *testing.T) {
	m := minioEnv(t)
	root := t.TempDir()
	svc, st, cs := m.newService(t, root, m.s3)
	defer st.Close()
	defer cs.Close()

	rnd := rand.Reader
	big := make([]byte, 70<<20) // >64MiB: two chunks, 18 blocks @4MiB
	io.ReadFull(rnd, big)
	multi := make([]byte, 6<<20) // multiblock @4MiB
	io.ReadFull(rnd, multi)
	m.wr("f_big", big)
	m.wr("f_multi", multi)
	m.wr("日本語ファイル", []byte("utf8-name"))
	m.mkdir("emptydir")
	m.wr(".filesv-notes", []byte("keep me"))
	syncfs(t, m.capFixture)

	row := m.doCapture(t, svc, st, "")
	if row.Format.Storage != "minio" {
		t.Fatalf("storage %q want minio", row.Format.Storage)
	}
	byPath := m.entriesByPath(t, cs, row.CaptureID)

	// post-capture mutations through the mount
	os.WriteFile(filepath.Join(m.mount, m.scope, "f_multi"), []byte("X"), 0o644)
	os.Remove(filepath.Join(m.mount, m.scope, "日本語ファイル"))
	syncfs(t, m.capFixture)

	for rel, want := range m.oracle {
		e := byPath[rel]
		got, err := m.readAll(t, cs, row.CaptureID, e.Seq)
		if err != nil {
			t.Fatalf("%s: stream: %v", rel, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("%s: captured bytes diverge (len %d want %d)", rel, len(got), len(want))
		}
	}
	t.Log("minio: captured bytes == mount baseline for all files incl >64MiB")
}

// TestCaptureMinioObjectFailures: deleting or truncating an object in
// MinIO (real S3 DELETE/PUT) turns the captured read into bounded
// pending — never corruption, never live-tree fallback.
func TestCaptureMinioObjectFailures(t *testing.T) {
	m := minioEnv(t)
	root := t.TempDir()
	svc, st, cs := m.newService(t, root, m.s3)
	defer st.Close()
	defer cs.Close()

	m.wr("victim", []byte("minio-victim-bytes"))
	syncfs(t, m.capFixture)
	row := m.doCapture(t, svc, st, "")
	e := m.entriesByPath(t, cs, row.CaptureID)["victim"]

	slices, err := st.slicesFor(context.Background(), row.CaptureID, e.Ino)
	if err != nil || len(slices) == 0 {
		t.Fatalf("slices: %v", err)
	}
	sl := slices[0]
	bsize := int(sl.Size)
	if bsize > row.Format.BlockBytes {
		bsize = row.Format.BlockBytes
	}
	key := objectKey(sl.SliceID, 0, bsize, row.Format.BlockBytes, row.Format.HashPrefix)

	// sanity: object exists and serves before sabotage
	if _, err := m.readAll(t, cs, row.CaptureID, e.Seq); err != nil {
		t.Fatalf("pre-sabotage read: %v", err)
	}

	// short object: PUT a truncated body over the same key
	r := m.s3Request(t, http.MethodPut, key, []byte("short"))
	r.Body.Close()
	if r.StatusCode != 200 {
		t.Fatalf("PUT short object: %d", r.StatusCode)
	}
	if _, err := m.readAll(t, cs, row.CaptureID, e.Seq); !errors.Is(err, ErrCapturePending) {
		t.Fatalf("short object: %v want pending", err)
	}

	// missing object: DELETE the key entirely
	r = m.s3Request(t, http.MethodDelete, key, nil)
	r.Body.Close()
	if r.StatusCode != 204 && r.StatusCode != 200 {
		t.Fatalf("DELETE object: %d", r.StatusCode)
	}
	if _, err := m.readAll(t, cs, row.CaptureID, e.Seq); !errors.Is(err, ErrCapturePending) {
		t.Fatalf("deleted object: %v want pending", err)
	}
}

// TestCaptureMinioIdentityMismatch: a service configured for a
// DIFFERENT bucket or prefix refuses the volume at capture time — an
// independently configured object backend cannot silently name
// another volume.
func TestCaptureMinioIdentityMismatch(t *testing.T) {
	m := minioEnv(t)
	root := t.TempDir()
	m.wr("x.txt", []byte("x"))
	syncfs(t, m.capFixture)

	badBucket := *m.s3
	badBucket.Bucket = "someotherbucket"
	svc, st, cs := m.newService(t, root, &badBucket)
	defer st.Close()
	defer cs.Close()
	if err := st.SetScopeFrozen(context.Background(), m.scope, m.owner, m.epoch,
		"capture-test", true); err != nil {
		t.Fatal(err)
	}
	if _, err := cs.Capture(context.Background(), m.scope, m.owner, m.epoch, ""); !errors.Is(err, ErrCaptureRefused) {
		t.Fatalf("wrong-bucket capture: %v want refused", err)
	}
	_ = svc
	// Release the writer lock before binding a second store on the DB.
	cs.Close()
	st.Close()

	badPrefix := *m.s3
	badPrefix.Prefix = "anothervolume/"
	svc2, st2, cs2 := m.newService(t, t.TempDir(), &badPrefix)
	defer st2.Close()
	defer cs2.Close()
	if err := st2.SetScopeFrozen(context.Background(), m.scope, m.owner, m.epoch,
		"capture-test", true); err != nil {
		t.Fatal(err)
	}
	if _, err := cs2.Capture(context.Background(), m.scope, m.owner, m.epoch, ""); !errors.Is(err, ErrCaptureRefused) {
		t.Fatalf("wrong-prefix capture: %v want refused", err)
	}
	_ = svc2
}
