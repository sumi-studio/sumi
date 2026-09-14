package filesvc

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// f58 regression: a refused/dead DB connection must classify as a store
// fault (503 store_unavailable), never an internal 500.
func TestStoreErrClassification(t *testing.T) {
	// Nothing listens on the owned pgproxy port during unit tests; if a
	// fixture happens to have it up, skip rather than depend on it.
	c, err := net.DialTimeout("tcp", "127.0.0.1:15543", 200*time.Millisecond)
	if err == nil {
		c.Close()
		t.Skip("pgproxy port is live; classification covered by live probes")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, "postgres://sumi:sumi-dev@127.0.0.1:15543/x?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.BeginTx(ctx, pgx.TxOptions{}); !isStoreErr(err) {
		t.Fatalf("refused connection must classify as store err, got %T %v", err, err)
	}
	if isStoreErr(errors.New("some logic bug")) {
		t.Fatal("plain error misclassified as store fault")
	}
	if !isStoreErr(&pgconn.PgError{Code: "08006"}) {
		t.Fatal("SQLSTATE 08 conn failure must classify as store err")
	}
	if !isStoreErr(&pgconn.PgError{Code: "57P01"}) {
		t.Fatal("admin shutdown must classify as store err")
	}
	// pgx v5.7.5 connLockError is unexported; it renders as "conn closed".
	if !isStoreErr(errors.New("conn closed")) {
		t.Fatal("closed pooled conn must classify as store err")
	}
}
