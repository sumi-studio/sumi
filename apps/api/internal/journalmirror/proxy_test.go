package journalmirror

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// netProxy forwards TCP connections to PostgreSQL. It can add latency to
// every server-to-client flight (so a round trip costs delay), count the
// bytes in each direction, and stall every connection without closing it (a
// silent network partition).
type netProxy struct {
	port  uint16
	delay atomic.Int64 // added to each server-to-client flight, in nanoseconds
	rate  atomic.Int64 // server-to-client bytes per second per connection; 0 is unlimited
	up    atomic.Int64
	down  atomic.Int64

	stalled atomic.Bool
	mu      sync.Mutex
	conns   []net.Conn
}

func startProxy(t testing.TB, host string, port uint16, delay time.Duration) *netProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &netProxy{port: uint16(ln.Addr().(*net.TCPAddr).Port)}
	p.delay.Store(int64(delay))
	t.Cleanup(func() {
		ln.Close()
		p.mu.Lock()
		defer p.mu.Unlock()
		for _, c := range p.conns {
			c.Close()
		}
	})
	go func() {
		for {
			client, err := ln.Accept()
			if err != nil {
				return
			}
			server, err := net.Dial("tcp", net.JoinHostPort(host, fmt.Sprint(port)))
			if err != nil {
				client.Close()
				continue
			}
			p.mu.Lock()
			p.conns = append(p.conns, client, server)
			p.mu.Unlock()
			go p.pipe(client, server, &p.up, false)
			go p.pipe(server, client, &p.down, true)
		}
	}()
	return p
}

func (p *netProxy) pipe(from, to net.Conn, counter *atomic.Int64, shaped bool) {
	defer to.Close()
	defer from.Close()
	// Every read is delivered delay after it arrived, without serializing
	// the delays: a pipelined response costs one flight, as on a real link.
	type flight struct {
		data []byte
		due  time.Time
	}
	queue := make(chan flight, 8) // small, so a shaped link pushes back like TCP
	done := make(chan struct{})
	go func() {
		defer close(done)
		for f := range queue {
			if wait := time.Until(f.due); wait > 0 {
				time.Sleep(wait)
			}
			if _, err := to.Write(f.data); err != nil {
				from.Close()
				for range queue {
				}
				return
			}
		}
	}()
	defer func() { close(queue); <-done }()
	buf := make([]byte, 64<<10)
	var free time.Time // when the shaped link finished sending earlier data
	for {
		n, err := from.Read(buf)
		for p.stalled.Load() {
			time.Sleep(10 * time.Millisecond)
		}
		if n > 0 {
			counter.Add(int64(n))
			due := time.Now()
			if shaped {
				if rate := p.rate.Load(); rate > 0 {
					if free.Before(due) {
						free = due
					}
					free = free.Add(time.Duration(int64(n) * int64(time.Second) / rate))
					due = free
				}
				due = due.Add(time.Duration(p.delay.Load()))
			}
			queue <- flight{data: append([]byte(nil), buf[:n]...), due: due}
		}
		if err != nil {
			return
		}
	}
}

// pool returns a pool whose connections, including the mirror's lease
// connection, go through the proxy.
func (p *netProxy) pool(t testing.TB, base *pgxpool.Pool) *pgxpool.Pool {
	t.Helper()
	cfg := base.Config().Copy()
	cfg.ConnConfig.Host = "127.0.0.1"
	cfg.ConnConfig.Port = p.port
	cfg.ConnConfig.Fallbacks = nil
	cfg.MinConns = 0
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func proxyFor(t testing.TB, base *pgxpool.Pool, delay time.Duration) *netProxy {
	t.Helper()
	cfg := base.Config().ConnConfig
	return startProxy(t, cfg.Host, cfg.Port, delay)
}
