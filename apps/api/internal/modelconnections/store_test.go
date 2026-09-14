package modelconnections

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
	"strings"
	"testing"
)

const owner = "0198f0f4-9b72-7000-8000-000000000201"
const other = "0198f0f4-9b72-7000-8000-000000000202"

func fixture(t *testing.T) *Store {
	t.Helper()
	pool := testdb.Create(t)
	ctx := context.Background()
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{owner, other} {
		if _, err := pool.Exec(ctx, "INSERT INTO humans(human_id) VALUES($1)", id); err != nil {
			t.Fatal(err)
		}
	}
	s, err := New(pool, bytes.Repeat([]byte{8}, 32))
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func input() Input {
	k := "test-secret"
	return Input{Name: "My API", Preset: "openai-chat", BaseURL: "https://provider.example/v1", Model: "model", APIKey: &k}
}
func TestIsolationSelectionRotationAndDelete(t *testing.T) {
	s := fixture(t)
	ctx := context.Background()
	a, err := s.Save(ctx, owner, "", input())
	if err != nil {
		t.Fatal(err)
	}
	if list, err := s.List(ctx, other); err != nil || len(list) != 0 {
		t.Fatal("other list", err)
	}
	if _, err = s.Resolve(ctx, other, a.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal("other resolve", err)
	}
	if err = s.Select(ctx, other, Selection{"api", a.ID}); !errors.Is(err, ErrNotFound) {
		t.Fatal("other select", err)
	}
	if _, err = s.Save(ctx, other, a.ID, input()); !errors.Is(err, ErrNotFound) {
		t.Fatal("other edit", err)
	}
	if err = s.Delete(ctx, other, a.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal("other delete", err)
	}
	if err = s.Select(ctx, owner, Selection{"api", a.ID}); err != nil {
		t.Fatal(err)
	}
	before, err := s.Resolve(ctx, owner, a.ID)
	if err != nil {
		t.Fatal(err)
	}
	in := input()
	in.APIKey = nil
	in.Name = "renamed"
	if _, err = s.Save(ctx, owner, a.ID, in); err != nil {
		t.Fatal(err)
	}
	after, err := s.Resolve(ctx, owner, a.ID)
	if err != nil || after.APIKey != before.APIKey || after.Version != before.Version {
		t.Fatal("retention/version", err)
	}
	in.BaseURL = "https://other.example/v1"
	if _, err = s.Save(ctx, owner, a.ID, in); !errors.Is(err, ErrInvalid) {
		t.Fatal("key sent to changed endpoint", err)
	}
	b, _ := json.Marshal(after)
	if bytes.Contains(b, []byte("secret")) {
		t.Fatal("secret serialized")
	}
	if err = s.Delete(ctx, owner, a.ID); err != nil {
		t.Fatal(err)
	}
	selected, exists, err := s.Selected(ctx, owner)
	if err != nil || !exists || selected.Kind != "none" {
		t.Fatal("delete must not fallback", err)
	}
	if _, err = s.Resolve(ctx, owner, a.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal("deleted key survived", err)
	}
}
func TestCiphertextBoundToOwnerAndConnection(t *testing.T) {
	s := fixture(t)
	ctx := context.Background()
	a, _ := s.Save(ctx, owner, "", input())
	b, _ := s.Save(ctx, other, "", input())
	var raw []byte
	if err := s.pool.QueryRow(ctx, "SELECT credential_ciphertext FROM model_api_connections WHERE human_id=$1 AND connection_id=$2", owner, a.ID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte("test-secret")) {
		t.Fatal("plaintext")
	}
	if _, err := s.pool.Exec(ctx, "UPDATE model_api_connections SET credential_ciphertext=$1 WHERE human_id=$2 AND connection_id=$3", raw, other, b.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Resolve(ctx, other, b.ID); !errors.Is(err, ErrUnavailable) {
		t.Fatal("AAD", err)
	}
}
func TestEndpointAndKeyValidation(t *testing.T) {
	for _, url := range []string{"http://provider.example/v1", "https://localhost/v1", "https://127.0.0.1/v1", "https://169.254.169.254/v1", "https://user:pass@example.com/v1", "https://example.com/v1?key=secret", "https://example.com/#x"} {
		in := input()
		in.BaseURL = url
		if Validate(in) == nil {
			t.Errorf("accepted %s", url)
		}
	}
	if err := Validate(input()); err != nil {
		t.Fatal(err)
	}
}

func TestCanonicalUUIDAndDisplayOnlyChange(t *testing.T) {
	s := fixture(t)
	ctx := context.Background()
	a, err := s.Save(ctx, owner, "", input())
	if err != nil {
		t.Fatal(err)
	}
	in := input()
	if _, err = s.Save(ctx, owner, "urn:uuid:"+strings.ToUpper(a.ID), in); err != nil {
		t.Fatal(err)
	}
	access, err := s.Resolve(ctx, owner, strings.ToUpper(a.ID))
	if err != nil || access.APIKey != *in.APIKey {
		t.Fatal("canonical key resolution", err)
	}
	if err = s.Select(ctx, owner, Selection{"api", a.ID}); err != nil {
		t.Fatal(err)
	}
	before, _ := s.RuntimeFingerprint(ctx, owner)
	in.APIKey = nil
	in.Name = "Display only"
	if _, err = s.Save(ctx, owner, a.ID, in); err != nil {
		t.Fatal(err)
	}
	after, _ := s.RuntimeFingerprint(ctx, owner)
	if before != after {
		t.Fatal("display update changed runtime")
	}
	if _, err = s.Save(ctx, owner, "", input()); err != nil {
		t.Fatal(err)
	}
	after, _ = s.RuntimeFingerprint(ctx, owner)
	if before != after {
		t.Fatal("inactive creation changed runtime")
	}
	in.Model = "different"
	if _, err = s.Save(ctx, owner, a.ID, in); err != nil {
		t.Fatal(err)
	}
	after, _ = s.RuntimeFingerprint(ctx, owner)
	if before == after {
		t.Fatal("model update did not change runtime")
	}
	modelAccess, err := s.Resolve(ctx, owner, a.ID)
	if err != nil || modelAccess.Version != access.Version {
		t.Fatal("model-only edit revoked ongoing credential", err)
	}
	in.APIKey = input().APIKey
	if _, err = s.Save(ctx, owner, a.ID, in); err != nil {
		t.Fatal(err)
	}
	rotated, err := s.Resolve(ctx, owner, a.ID)
	if err != nil || rotated.Version == access.Version {
		t.Fatal("key replacement did not revoke old binding", err)
	}
}

func TestExtraHeadersSealedWithCredential(t *testing.T) {
	s := fixture(t)
	ctx := context.Background()
	in := input()
	in.ExtraHeaders = map[string]string{"X-Gateway-Session": "gw-1", "X-Tenant": "blue"}
	a, err := s.Save(ctx, owner, "", in)
	if err != nil {
		t.Fatal(err)
	}
	// Values live inside the ciphertext: nothing readable at rest.
	var raw []byte
	if err := s.pool.QueryRow(ctx, "SELECT credential_ciphertext FROM model_api_connections WHERE human_id=$1 AND connection_id=$2", owner, a.ID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	for _, leak := range []string{"test-secret", "gw-1", "X-Gateway-Session", "blue"} {
		if bytes.Contains(raw, []byte(leak)) {
			t.Fatalf("ciphertext leaks %q", leak)
		}
	}
	access, err := s.Resolve(ctx, owner, a.ID)
	if err != nil {
		t.Fatal(err)
	}
	if access.APIKey != "test-secret" || access.ExtraHeaders["X-Gateway-Session"] != "gw-1" || access.ExtraHeaders["X-Tenant"] != "blue" {
		t.Fatalf("resolve %+v", access)
	}
	// Headers are credential material: changing them requires the key.
	in.APIKey = nil
	in.ExtraHeaders = map[string]string{"X-Tenant": "red"}
	if _, err = s.Save(ctx, owner, a.ID, in); !errors.Is(err, ErrInvalid) {
		t.Fatal("headers without key accepted", err)
	}
	// Resubmitting the key replaces both key and headers.
	key := "test-secret-2"
	in.APIKey = &key
	if _, err = s.Save(ctx, owner, a.ID, in); err != nil {
		t.Fatal(err)
	}
	access, err = s.Resolve(ctx, owner, a.ID)
	if err != nil || access.APIKey != key || len(access.ExtraHeaders) != 1 || access.ExtraHeaders["X-Tenant"] != "red" {
		t.Fatalf("rotated resolve %+v", access)
	}
	// An update without the headers field keeps them.
	in.ExtraHeaders = nil
	in.APIKey = nil
	in.Name = "renamed"
	if _, err = s.Save(ctx, owner, a.ID, in); err != nil {
		t.Fatal(err)
	}
	access, _ = s.Resolve(ctx, owner, a.ID)
	if access.ExtraHeaders["X-Tenant"] != "red" {
		t.Fatal("keyless edit dropped stored headers")
	}
	// Rotating the key alone — the UI's default rotation flow sends no
	// headers field — must also keep the stored set (F1).
	key3 := "test-secret-3"
	in.APIKey = &key3
	if _, err = s.Save(ctx, owner, a.ID, in); err != nil {
		t.Fatal(err)
	}
	access, _ = s.Resolve(ctx, owner, a.ID)
	if access.APIKey != key3 || access.ExtraHeaders["X-Tenant"] != "red" {
		t.Fatalf("key rotation dropped stored headers: %+v", access)
	}
	// Clearing headers explicitly requires the key.
	in.APIKey = &key
	in.ExtraHeaders = map[string]string{}
	if _, err = s.Save(ctx, owner, a.ID, in); err != nil {
		t.Fatal(err)
	}
	access, _ = s.Resolve(ctx, owner, a.ID)
	if len(access.ExtraHeaders) != 0 {
		t.Fatal("explicit clear did not clear")
	}
}

func TestHeaderValidation(t *testing.T) {
	key := "k"
	ok := input()
	ok.ExtraHeaders = map[string]string{"X-Gateway-Session": "gw-1"}
	if err := Validate(ok); err != nil {
		t.Fatal(err)
	}
	bad := []map[string]string{
		{"Authorization": "x"},
		{"x-api-key": "x"},
		{"Content-Type": "x"},
		{"Host": "x"},
		{"Cookie": "x"},
		{"bad name": "x"},
		{"": "x"},
		{"X-Ok": "line\nbreak"},
		{"X-Ok": strings.Repeat("v", 1025)},
		{strings.Repeat("n", 129): "x"},
		// Beyond Latin-1 fetch cannot put the value on the wire: reject at
		// write instead of deterministically failing every request.
		{"X-Ok": "値"},
		{"X-Ok": "emoji🎉"},
	}
	for _, h := range bad {
		in := input()
		in.ExtraHeaders = h
		if err := Validate(in); err == nil {
			t.Errorf("accepted headers %v", h)
		}
	}
	// Headers without a key cannot be sealed — rejected on create and update.
	in := input()
	in.APIKey = nil
	in.ExtraHeaders = map[string]string{"X-Ok": "v"}
	if err := Validate(in); err == nil {
		t.Error("headers without key accepted")
	}
	many := map[string]string{}
	for i := 0; i < 17; i++ {
		many[string(rune('a'+i))] = "v"
	}
	in = input()
	in.APIKey = &key
	in.ExtraHeaders = many
	if err := Validate(in); err == nil {
		t.Error("17 headers accepted")
	}
}

func TestMissingEncryptionKeyPreservesSelectionsAndRejectsCredentials(t *testing.T) {
	store := fixture(t)
	ctx := context.Background()
	connection, err := store.Save(ctx, owner, "", input())
	if err != nil {
		t.Fatal(err)
	}
	metadata := MetadataOnly(store.pool)
	for _, selection := range []Selection{{Kind: "none"}, {Kind: "api", ConnectionID: connection.ID}} {
		if err := store.Select(ctx, owner, selection); err != nil {
			t.Fatal(err)
		}
		got, exists, err := metadata.Selected(ctx, owner)
		if err != nil || !exists || got != selection {
			t.Fatal("lost persisted choice without key", err)
		}
		if _, err := metadata.Metadata(ctx, owner, connection.ID); !errors.Is(err, ErrUnavailable) {
			t.Fatal("keyless activation admitted", err)
		}
		if _, err := metadata.Resolve(ctx, owner, connection.ID); !errors.Is(err, ErrUnavailable) {
			t.Fatal("keyless credential access admitted", err)
		}
	}
	if _, err := metadata.Save(ctx, owner, connection.ID, input()); !errors.Is(err, ErrUnavailable) {
		t.Fatal("keyless overwrite admitted", err)
	}
	if _, exists, err := metadata.Selected(ctx, other); err != nil || exists {
		t.Fatal("invented default selection", err)
	}
	if _, err := store.Resolve(ctx, owner, connection.ID); err != nil {
		t.Fatal("restored original key no longer decrypts", err)
	}
}

func TestMaxOutputTokensPersistsAsConnectionMetadata(t *testing.T) {
	s := fixture(t)
	ctx := context.Background()
	in := input()
	bound := 8192
	in.MaxOutputTokens = &bound
	c, err := s.Save(ctx, owner, "", in)
	if err != nil {
		t.Fatal(err)
	}
	if c.MaxOutputTokens == nil || *c.MaxOutputTokens != 8192 {
		t.Fatalf("saved connection %+v", c)
	}
	list, err := s.List(ctx, owner)
	if err != nil || len(list) != 1 {
		t.Fatalf("list %v %+v", err, list)
	}
	if list[0].MaxOutputTokens == nil || *list[0].MaxOutputTokens != 8192 {
		t.Fatalf("list omitted the bound: %+v", list[0])
	}
	meta, err := s.Describe(ctx, owner, c.ID)
	if err != nil || meta.Connection.MaxOutputTokens == nil || *meta.Connection.MaxOutputTokens != 8192 {
		t.Fatalf("describe %+v %v", meta, err)
	}
	// Non-secret metadata: present on the readable row, not inside the
	// sealed credential payload.
	var raw []byte
	if err := s.pool.QueryRow(ctx, "SELECT credential_ciphertext FROM model_api_connections WHERE human_id=$1 AND connection_id=$2", owner, c.ID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte("8192")) {
		t.Fatal("output bound leaked into the credential payload")
	}
	// A plain metadata update replaces the bound (unset clears to default).
	in.MaxOutputTokens = nil
	if _, err = s.Save(ctx, owner, c.ID, in); err != nil {
		t.Fatal(err)
	}
	access, err := s.Resolve(ctx, owner, c.ID)
	if err != nil || access.Connection.MaxOutputTokens != nil {
		t.Fatalf("cleared bound still present: %+v", access.Connection)
	}
	for _, v := range []int{0, -5, 1_000_001} {
		in.MaxOutputTokens = &v
		if _, err = s.Save(ctx, owner, c.ID, in); !errors.Is(err, ErrInvalid) {
			t.Fatalf("accepted bound %d: %v", v, err)
		}
	}
}
