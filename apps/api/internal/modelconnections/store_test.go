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
	return Input{"My API", "openai-chat", "https://provider.example/v1", "model", &k}
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
