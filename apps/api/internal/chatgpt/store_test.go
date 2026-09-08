package chatgpt

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

const owner = "0198f0f4-9b72-7000-8000-000000000201"
const other = "0198f0f4-9b72-7000-8000-000000000202"

func testStore(t *testing.T) *Store {
	t.Helper()
	pool := testdb.Create(t)
	if err := db.Migrate(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{owner, other} {
		if _, err := pool.Exec(context.Background(), "INSERT INTO humans(human_id) VALUES($1)", id); err != nil {
			t.Fatal(err)
		}
	}
	s, err := New(pool, bytes.Repeat([]byte{7}, 32))
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func connect(t *testing.T, s *Store, human, account string) Status {
	t.Helper()
	status, err := s.Connect(context.Background(), human, Credentials{AccessToken: "access-secret", RefreshToken: "refresh-secret", AccountID: account, ExpiresAt: time.Now().Add(time.Hour)}, Selection{Model: "gpt-6-astra", Effort: "medium"})
	if err != nil {
		t.Fatal(err)
	}
	return status
}
func TestCiphertextOwnerBindingAndReconnect(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	first := connect(t, s, owner, "account-one")
	var ciphertext []byte
	if err := s.pool.QueryRow(ctx, "SELECT credential_ciphertext FROM chatgpt_connections WHERE human_id=$1", owner).Scan(&ciphertext); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(ciphertext, []byte("secret")) {
		t.Fatal("plaintext credential persisted")
	}
	if _, err := s.ResolveAccess(ctx, other, first.ConnectionID, "", nil); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("wrong owner: %v", err)
	}
	connect(t, s, other, "account-one")
	if _, err := s.pool.Exec(ctx, "UPDATE chatgpt_connections SET credential_ciphertext=$1 WHERE human_id=$2", ciphertext, other); err != nil {
		t.Fatal(err)
	}
	status, _ := s.Status(ctx, other)
	if _, err := s.ResolveAccess(ctx, other, status.ConnectionID, "", nil); !errors.Is(err, ErrCredentialUnavailable) {
		t.Fatalf("owner AAD: %v", err)
	}
	second := connect(t, s, owner, "account-two")
	if second.ConnectionID == first.ConnectionID {
		t.Fatal("reconnect reused runtime identity")
	}
	if _, err := s.ResolveAccess(ctx, owner, first.ConnectionID, "", nil); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("old connection: %v", err)
	}
	access, err := s.ResolveAccess(ctx, owner, second.ConnectionID, "", nil)
	if err != nil || access.AccountID != "account-two" {
		t.Fatalf("reconnected access: %v", err)
	}
	b, _ := json.Marshal(access)
	if bytes.Contains(b, []byte("secret")) {
		t.Fatal("access material serialized")
	}
	if err = s.Disconnect(ctx, owner); err != nil {
		t.Fatal(err)
	}
	status, err = s.Status(ctx, owner)
	if err != nil || status.Connected {
		t.Fatalf("disconnect status: %v", err)
	}
	var count int
	if err = s.pool.QueryRow(ctx, "SELECT count(*) FROM chatgpt_connections WHERE human_id=$1", owner).Scan(&count); err != nil || count != 0 {
		t.Fatal("disconnect retained ciphertext", err)
	}
}
func TestRefreshSingleWriterRotationAndDisconnect(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	status := connect(t, s, owner, "account-one")
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	refresh := func(ctx context.Context, token string) (Credentials, error) {
		if token != "refresh-secret" {
			t.Errorf("unexpected refresh credential")
		}
		calls.Add(1)
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
			return Credentials{}, ctx.Err()
		}
		return Credentials{AccessToken: "new-access", RefreshToken: "rotated-refresh", ExpiresAt: time.Now().Add(time.Hour)}, nil
	}
	done := make(chan error, 2)
	go func() {
		_, err := s.ResolveAccess(ctx, owner, status.ConnectionID, "access-secret", refresh)
		done <- err
	}()
	<-entered
	go func() {
		_, err := s.ResolveAccess(ctx, owner, status.ConnectionID, "access-secret", refresh)
		done <- err
	}()
	close(release)
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("refreshes=%d", calls.Load())
	}
	// Verify actual rotated credential is used next; disconnect queues behind it.
	entered = make(chan struct{})
	release = make(chan struct{})
	go func() {
		_, err := s.ResolveAccess(ctx, owner, status.ConnectionID, "new-access", func(ctx context.Context, token string) (Credentials, error) {
			if token != "rotated-refresh" {
				t.Error("rotation not persisted")
			}
			close(entered)
			<-release
			return Credentials{AccessToken: "last-access", ExpiresAt: time.Now().Add(time.Hour)}, nil
		})
		done <- err
	}()
	<-entered
	go func() { done <- s.Disconnect(ctx, owner) }()
	close(release)
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.ResolveAccess(ctx, owner, status.ConnectionID, "", nil); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("refresh resurrected disconnect: %v", err)
	}
}
func TestRefreshFailuresPreserveConnectionAndAccount(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	status := connect(t, s, owner, "account-one")
	for _, tc := range []struct {
		name    string
		refresh RefreshFunc
		want    error
	}{
		{"transient", func(context.Context, string) (Credentials, error) {
			return Credentials{}, errors.New("secret upstream body")
		}, ErrRefreshFailed},
		{"account switch", func(context.Context, string) (Credentials, error) {
			return Credentials{AccessToken: "wrong", AccountID: "account-two", ExpiresAt: time.Now().Add(time.Hour)}, nil
		}, ErrAccountChanged},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := s.ResolveAccess(ctx, owner, status.ConnectionID, "access-secret", tc.refresh); !errors.Is(err, tc.want) {
				t.Fatal(err)
			}
			access, err := s.ResolveAccess(ctx, owner, status.ConnectionID, "", nil)
			if err != nil || access.Token != "access-secret" {
				t.Fatal("valid credential lost", err)
			}
		})
	}
	_, err := s.ResolveAccess(ctx, owner, status.ConnectionID, "access-secret", func(context.Context, string) (Credentials, error) { return Credentials{}, ErrReconnectRequired })
	if !errors.Is(err, ErrReconnectRequired) {
		t.Fatal(err)
	}
	saved, _ := s.Status(ctx, owner)
	if !saved.ReconnectRequired {
		t.Fatal("invalid grant status missing")
	}
	connect(t, s, owner, "account-one")
	saved, _ = s.Status(ctx, owner)
	if saved.ReconnectRequired {
		t.Fatal("reconnect did not reset failure")
	}
}

func TestRefreshWithoutNewAccessPreservesRotationButDoesNotRepair401(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	status := connect(t, s, owner, "account-one")
	for _, result := range []Credentials{{}, {ExpiresAt: time.Now().Add(24 * time.Hour)}, {AccessToken: "access-secret", ExpiresAt: time.Now().Add(24 * time.Hour)}, {RefreshToken: "rotation-only"}} {
		_, err := s.ResolveAccess(ctx, owner, status.ConnectionID, "access-secret", func(context.Context, string) (Credentials, error) { return result, nil })
		if !errors.Is(err, ErrRefreshFailed) {
			t.Fatalf("unchanged access repaired rejected token: %v", err)
		}
		saved, _ := s.Status(ctx, owner)
		if !saved.ExpiresAt.Equal(status.ExpiresAt.Truncate(time.Microsecond)) {
			t.Fatal("invented token lifetime")
		}
	}
	_, err := s.ResolveAccess(ctx, owner, status.ConnectionID, "access-secret", func(_ context.Context, refresh string) (Credentials, error) {
		if refresh != "rotation-only" {
			t.Fatal("lost rotated refresh token")
		}
		return Credentials{AccessToken: "repaired", ExpiresAt: time.Now().Add(time.Hour)}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
