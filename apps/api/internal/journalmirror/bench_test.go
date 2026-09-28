package journalmirror

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// BenchmarkRestore measures how long a replaced host takes to write back a
// mirror of the given total size before the API can serve.
func BenchmarkRestore(b *testing.B) {
	for _, mib := range []int{1, 16, 64} {
		b.Run(fmt.Sprintf("%dMiB", mib), func(b *testing.B) {
			pool := migratedPoolB(b)
			ctx := context.Background()
			src := b.TempDir()
			line := append(bytes.Repeat([]byte("x"), 1023), '\n')
			for file := 0; file < 8; file++ {
				content := bytes.Repeat(line, mib*1024/8)
				if err := os.WriteFile(filepath.Join(src, fmt.Sprintf("events-%d.jsonl", file)), content, 0o600); err != nil {
					b.Fatal(err)
				}
			}
			seed, err := Acquire(ctx, pool, "seed", nil)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := seed.Attach(ctx, "browser-events", src); err != nil {
				b.Fatal(err)
			}
			b.SetBytes(int64(mib) << 20)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				m, err := Acquire(ctx, pool, "restore", nil)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := m.Attach(ctx, "browser-events", b.TempDir()); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkAppendSync measures one acknowledged journal append (a ~1 KiB
// event line) including the local fsync and the PostgreSQL commit.
func BenchmarkAppendSync(b *testing.B) {
	pool := migratedPoolB(b)
	ctx := context.Background()
	dir := b.TempDir()
	m, err := Acquire(ctx, pool, "bench", nil)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := m.Attach(ctx, "commands", dir); err != nil {
		b.Fatal(err)
	}
	path := filepath.Join(dir, "log.jsonl")
	raw, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		b.Fatal(err)
	}
	f, err := m.Wrap(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, raw)
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()
	line := append(bytes.Repeat([]byte("y"), 1023), '\n')
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.Write(line); err != nil {
			b.Fatal(err)
		}
		if err := f.Sync(); err != nil {
			b.Fatal(err)
		}
	}
}
