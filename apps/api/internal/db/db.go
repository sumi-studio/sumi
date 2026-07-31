// Package db owns the Sumi control-plane Postgres connection and the embedded
// migration runner. The 戸籍 (identity registry) is the canonical record
// (ADR 0009 §7) and local dev is Postgres-only — there is no SQLite dual path.
package db

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Pool wraps a pgx connection pool for the control-plane database.
type Pool struct {
	*pgxpool.Pool
}

// Open parses databaseURL, opens a connection pool, and pings the database to
// confirm connectivity. The caller must call Close when finished.
func Open(ctx context.Context, databaseURL string) (*Pool, error) {
	if strings.TrimSpace(databaseURL) == "" {
		return nil, errors.New("database url is required")
	}
	if _, err := url.Parse(databaseURL); err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database config: %w", err)
	}
	cfg.MaxConns = 10
	cfg.MinConns = 1
	cfg.MaxConnIdleTime = 30 * time.Minute
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect to database: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return &Pool{Pool: pool}, nil
}

// Close releases the pool's connections. It is safe to call on a nil Pool.
func (p *Pool) Close() {
	if p == nil || p.Pool == nil {
		return
	}
	p.Pool.Close()
}

// Acquire returns a single connection from the pool for ad-hoc use.
func (p *Pool) Acquire(ctx context.Context) (*pgxpool.Conn, error) {
	if p == nil || p.Pool == nil {
		return nil, errors.New("database pool is not open")
	}
	return p.Pool.Acquire(ctx)
}
