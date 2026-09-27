package egressproxy

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// scriptedResolver answers LookupNetIP from a queue/map so tests control
// exactly what each request resolves to — including mid-run DNS changes.
type scriptedResolver struct {
	answers map[string][]netip.Addr
	script  map[string][][]netip.Addr // successive answers per host
	calls   atomic.Int64
	err     error
}

func (r *scriptedResolver) LookupNetIP(_ context.Context, _, host string) ([]netip.Addr, error) {
	r.calls.Add(1)
	if r.err != nil {
		return nil, r.err
	}
	if script, ok := r.script[host]; ok && len(script) > 0 {
		answer := script[0]
		r.script[host] = script[1:]
		return answer, nil
	}
	if addrs, ok := r.answers[host]; ok {
		return addrs, nil
	}
	return nil, errors.New("no such host")
}

// recordedDial asserts the proxy dials the validated IP literal and serves
// as the upstream endpoint over a pipe.
type recordedDial struct {
	t       *testing.T
	seen    chan string
	handler func(net.Conn) // runs on the upstream end
}

func (d *recordedDial) dial(_ context.Context, network, address string) (net.Conn, error) {
	d.seen <- address
	clientEnd, upstreamEnd := net.Pipe()
	go d.handler(upstreamEnd)
	return clientEnd, nil
}

func mustAddr(t *testing.T, s string) netip.Addr {
	t.Helper()
	ip, err := netip.ParseAddr(s)
	if err != nil {
		t.Fatal(err)
	}
	return ip
}

func testProxy(t *testing.T, resolver resolver, dial func(context.Context, string, string) (net.Conn, error)) *Proxy {
	t.Helper()
	return NewProxy(Config{
		Resolver:    resolver,
		Dial:        dial,
		IdleTimeout: 2 * time.Second,
		Logf:        func(string, ...any) {},
	})
}

// serve starts the proxy on an in-process HTTP server bound to a real
// unix socket — the same transport the deployment uses.
func serve(t *testing.T, p *Proxy) string {
	t.Helper()
	dir := t.TempDir()
	socket := dir + "/proxy.sock"
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: p, ReadHeaderTimeout: 5 * time.Second}
	go server.Serve(listener)
	t.Cleanup(func() { server.Close() })
	return socket
}

// dialSocket connects a raw HTTP/1.1 client to the unix socket.
func dialSocket(t *testing.T, socket string) net.Conn {
	t.Helper()
	conn, err := net.Dial("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func readResponse(t *testing.T, conn net.Conn) *http.Response {
	t.Helper()
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func TestConnectPublicDestinationTunnelsBytes(t *testing.T) {
	public := mustAddr(t, "93.184.216.34")
	resolver := &scriptedResolver{answers: map[string][]netip.Addr{"cdn.example": {public}}}
	dial := &recordedDial{
		t:    t,
		seen: make(chan string, 1),
		handler: func(upstream net.Conn) {
			defer upstream.Close()
			buf := make([]byte, 64)
			n, _ := upstream.Read(buf)
			_, _ = upstream.Write([]byte("echo:" + string(buf[:n])))
		},
	}
	socket := serve(t, testProxy(t, resolver, dial.dial))
	conn := dialSocket(t, socket)
	if _, err := conn.Write([]byte("CONNECT cdn.example:443 HTTP/1.1\r\nHost: cdn.example:443\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	// Reuse one buffered reader for the response head and the tunneled
	// bytes — a fresh reader per call could strand pre-read data.
	reader := bufio.NewReader(conn)
	response, err := http.ReadResponse(reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != 200 {
		t.Fatalf("CONNECT status = %d", response.StatusCode)
	}
	select {
	case dialed := <-dial.seen:
		if dialed != "93.184.216.34:443" {
			t.Fatalf("dial target = %q, want pinned validated IP", dialed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("upstream was never dialed")
	}
	if _, err := conn.Write([]byte("tls-bytes")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64)
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := reader.Read(buf)
	if err != nil || string(buf[:n]) != "echo:tls-bytes" {
		t.Fatalf("tunnel did not relay bytes: n=%d err=%v", n, err)
	}
}

func TestConnectDeniedDestinations(t *testing.T) {
	public := mustAddr(t, "93.184.216.34")
	resolver := &scriptedResolver{answers: map[string][]netip.Addr{
		"public.example": {public},
		"mixed.example":  {public, mustAddr(t, "10.9.9.9")},
		"private.example": {mustAddr(t, "192.168.7.7")},
	}}
	dial := &recordedDial{t: t, seen: make(chan string, 8), handler: func(c net.Conn) { c.Close() }}
	socket := serve(t, testProxy(t, resolver, dial.dial))
	for name, target := range map[string]string{
		"loopback-literal":     "127.0.0.1:443",
		"private-literal":      "10.0.0.8:443",
		"link-local-metadata":  "169.254.169.254:443",
		"docker-bridge-gw":     "172.17.0.1:443",
		"ipv6-loopback":        "[::1]:443",
		"ipv6-ula":             "[fd00::1]:443",
		"ipv6-linklocal":       "[fe80::1]:443",
		"private-hostname":     "private.example:443",
		"mixed-answer":         "mixed.example:443",
		"wrong-port":           "public.example:80",
		"high-port":            "public.example:8443",
		"no-port":              "public.example",
		"userinfo":             "u:p@public.example:443",
	} {
		t.Run(name, func(t *testing.T) {
			conn := dialSocket(t, socket)
			if _, err := conn.Write([]byte("CONNECT " + target + " HTTP/1.1\r\nHost: " + target + "\r\n\r\n")); err != nil {
				t.Fatal(err)
			}
			response := readResponse(t, conn)
			if response.StatusCode == 200 {
				t.Fatalf("CONNECT %s was permitted", target)
			}
			select {
			case dialed := <-dial.seen:
				t.Fatalf("denied destination was dialed: %s", dialed)
			case <-time.After(100 * time.Millisecond):
			}
		})
	}
}

// A hostname that resolves public on request 1 and private on request 2 is
// denied on request 2 — the check runs at connection time, every time.
func TestConnectRechecksChangedDNS(t *testing.T) {
	public := mustAddr(t, "93.184.216.34")
	private := mustAddr(t, "10.66.0.1")
	resolver := &scriptedResolver{script: map[string][][]netip.Addr{
		"flapping.example": {{public}, {private}},
	}}
	dial := &recordedDial{t: t, seen: make(chan string, 4), handler: func(c net.Conn) {
		defer c.Close()
		_, _ = io.Copy(io.Discard, c)
	}}
	socket := serve(t, testProxy(t, resolver, dial.dial))
	connect := func() int {
		conn := dialSocket(t, socket)
		defer conn.Close()
		if _, err := conn.Write([]byte("CONNECT flapping.example:443 HTTP/1.1\r\nHost: flapping.example:443\r\n\r\n")); err != nil {
			t.Fatal(err)
		}
		return readResponse(t, conn).StatusCode
	}
	if got := connect(); got != 200 {
		t.Fatalf("first CONNECT = %d, want 200", got)
	}
	if got := connect(); got != 403 {
		t.Fatalf("post-rebind CONNECT = %d, want 403", got)
	}
}

func TestConnectDialFailureIsBadGateway(t *testing.T) {
	resolver := &scriptedResolver{answers: map[string][]netip.Addr{"ok.example": {mustAddr(t, "93.184.216.34")}}}
	dial := func(context.Context, string, string) (net.Conn, error) { return nil, errors.New("no route") }
	socket := serve(t, testProxy(t, resolver, dial))
	conn := dialSocket(t, socket)
	if _, err := conn.Write([]byte("CONNECT ok.example:443 HTTP/1.1\r\nHost: ok.example:443\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	if response := readResponse(t, conn); response.StatusCode != 502 {
		t.Fatalf("failed dial status = %d, want 502", response.StatusCode)
	}
}

// Plain-HTTP forwarding: the request goes to the validated IP with the
// client's Host header, the response (including a redirect) returns
// verbatim, and proxy-control headers never reach the origin.
func TestForwardHTTPToPublicOrigin(t *testing.T) {
	public := mustAddr(t, "93.184.216.34")
	var sawProxyAuth atomic.Bool
	var sawHost atomic.Value
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawHost.Store(r.Host)
		sawProxyAuth.Store(r.Header.Get("Proxy-Authorization") != "" || r.Header.Get("Proxy-Connection") != "")
		w.Header().Set("X-Origin", "yes")
		w.WriteHeader(418)
		_, _ = w.Write([]byte("origin-body"))
	}))
	defer origin.Close()
	var dialed atomic.Value
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		dialed.Store(address)
		return (&net.Dialer{}).DialContext(ctx, "tcp", strings.TrimPrefix(origin.URL, "http://"))
	}
	resolver := &scriptedResolver{answers: map[string][]netip.Addr{"mirror.example": {public}}}
	socket := serve(t, testProxy(t, resolver, dial))
	conn := dialSocket(t, socket)
	request := "GET http://mirror.example/pkg.tar HTTP/1.1\r\nHost: mirror.example\r\nProxy-Authorization: Basic Zm9v\r\nProxy-Connection: keep-alive\r\n\r\n"
	if _, err := conn.Write([]byte(request)); err != nil {
		t.Fatal(err)
	}
	response := readResponse(t, conn)
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != 418 || string(body) != "origin-body" || response.Header.Get("X-Origin") != "yes" {
		t.Fatalf("forwarded response = %d %q", response.StatusCode, body)
	}
	if got := dialed.Load().(string); got != "93.184.216.34:80" {
		t.Fatalf("dial target = %q, want pinned validated IP", got)
	}
	if sawHost.Load().(string) != "mirror.example" {
		t.Fatalf("origin Host = %q", sawHost.Load())
	}
	if sawProxyAuth.Load() {
		t.Fatal("proxy-control headers reached the origin")
	}
}

// A redirect is passed back to the client verbatim — the client's follow-up
// request is itself re-resolved and re-checked.
func TestForwardRedirectReturnedNotFollowed(t *testing.T) {
	public := mustAddr(t, "93.184.216.34")
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://elsewhere.example/next", http.StatusFound)
	}))
	defer origin.Close()
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", strings.TrimPrefix(origin.URL, "http://"))
	}
	resolver := &scriptedResolver{answers: map[string][]netip.Addr{"mirror.example": {public}}}
	socket := serve(t, testProxy(t, resolver, dial))
	conn := dialSocket(t, socket)
	if _, err := conn.Write([]byte("GET http://mirror.example/a HTTP/1.1\r\nHost: mirror.example\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	response := readResponse(t, conn)
	if response.StatusCode != 302 || response.Header.Get("Location") != "http://elsewhere.example/next" {
		t.Fatalf("redirect = %d %q", response.StatusCode, response.Header.Get("Location"))
	}
}

func TestForwardDenied(t *testing.T) {
	resolver := &scriptedResolver{answers: map[string][]netip.Addr{
		"ok.example":      {mustAddr(t, "93.184.216.34")},
		"private.example": {mustAddr(t, "10.1.2.3")},
	}}
	dial := &recordedDial{t: t, seen: make(chan string, 8), handler: func(c net.Conn) { c.Close() }}
	socket := serve(t, testProxy(t, resolver, dial.dial))
	for name, request := range map[string]string{
		"private-name":     "GET http://private.example/ HTTP/1.1\r\nHost: private.example\r\n\r\n",
		"private-literal":  "GET http://192.168.1.1/ HTTP/1.1\r\nHost: 192.168.1.1\r\n\r\n",
		"metadata":         "GET http://169.254.169.254/latest/meta-data HTTP/1.1\r\nHost: 169.254.169.254\r\n\r\n",
		"high-port":        "GET http://ok.example:8080/ HTTP/1.1\r\nHost: ok.example:8080\r\n\r\n",
		"https-absolute":   "GET https://ok.example/ HTTP/1.1\r\nHost: ok.example\r\n\r\n",
		"origin-form":      "GET /local/path HTTP/1.1\r\nHost: ok.example\r\n\r\n",
		"userinfo":         "GET http://u:p@ok.example/ HTTP/1.1\r\nHost: ok.example\r\n\r\n",
	} {
		t.Run(name, func(t *testing.T) {
			conn := dialSocket(t, socket)
			if _, err := conn.Write([]byte(request)); err != nil {
				t.Fatal(err)
			}
			response := readResponse(t, conn)
			if response.StatusCode == 200 || response.StatusCode == 418 {
				t.Fatalf("request %q was forwarded", name)
			}
		})
	}
	select {
	case dialed := <-dial.seen:
		t.Fatalf("denied destination was dialed: %s", dialed)
	case <-time.After(100 * time.Millisecond):
	}
}

// Bridge: a real loopback listener splicing onto the real unix socket —
// the exact in-container shape.
func TestBridgeRelaysLoopbackToUnixSocket(t *testing.T) {
	dir := t.TempDir()
	socket := dir + "/proxy.sock"
	echoListener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer echoListener.Close()
	go func() {
		conn, err := echoListener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = io.Copy(conn, conn) // echo
	}()
	bridge := NewBridge("127.0.0.1:0", socket, 4, func(string, ...any) {})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	// Serve on a chosen port by substituting the listener address.
	bridge.listenAddr = listener.Addr().String()
	listener.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go bridge.Serve(ctx)
	deadline := time.Now().Add(3 * time.Second)
	var conn net.Conn
	for time.Now().Before(deadline) {
		conn, err = net.Dial("tcp", bridge.listenAddr)
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("bridge never listened: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("via-socket")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64)
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := conn.Read(buf)
	if err != nil || string(buf[:n]) != "via-socket" {
		t.Fatalf("bridge relay = %q err=%v", buf[:n], err)
	}
}

// When the socket is absent the client sees a refused/closed connection —
// the honest failure a package tool reports.
func TestBridgeSocketAbsentFailsClean(t *testing.T) {
	dir := t.TempDir()
	bridge := NewBridge("127.0.0.1:0", dir+"/missing.sock", 4, func(string, ...any) {})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	bridge.listenAddr = listener.Addr().String()
	listener.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go bridge.Serve(ctx)
	var conn net.Conn
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		conn, err = net.Dial("tcp", bridge.listenAddr)
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("bridge never listened: %v", err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = conn.Read(make([]byte, 8))
	if err == nil {
		t.Fatal("connection to absent socket stayed open")
	}
}

// f417 regression: the configured dial timeout is the single bound on an
// upstream attempt — both increases above and decreases below the 10s
// default must take effect (the earlier build-order bug kept a hardcoded
// 10s dialer, so an increase never applied).
func TestDialTimeoutConfigApplied(t *testing.T) {
	for _, tc := range []struct {
		name string
		want time.Duration
	}{
		{"increase", 45 * time.Second},
		{"decrease", 50 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var gotDeadline atomic.Int64
			p := NewProxy(Config{
				DialTimeout: tc.want,
				Logf:        func(string, ...any) {},
				Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
					dl, _ := ctx.Deadline()
					gotDeadline.Store(dl.UnixNano())
					return nil, errors.New("stop after deadline observation")
				},
			})
			start := time.Now()
			_, _ = p.dialPinned(context.Background(), []netip.Addr{mustAddr(t, "93.184.216.34")}, "443")
			deadline := time.Unix(0, gotDeadline.Load())
			if deadline.IsZero() {
				t.Fatal("dial ctx carried no deadline")
			}
			got := deadline.Sub(start)
			if got < tc.want-time.Second || got > tc.want+time.Second {
				t.Fatalf("attempt ctx bound = %v, want ≈%v", got, tc.want)
			}
		})
	}
}

// f418 regression: when the client side of a CONNECT tunnel closes its
// write side, the upstream's read side must see EOF promptly — the origin
// can then flush final bytes and the tunnel ends without waiting out the
// idle reaper.
func TestTunnelHalfClosePropagates(t *testing.T) {
	p := NewProxy(Config{IdleTimeout: time.Minute, Logf: func(string, ...any) {}})
	// Upstream stub: echo whatever arrives, and on client EOF send a
	// final reply before closing — only reachable if the half-close is
	// propagated.
	upstreamListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer upstreamListener.Close()
	upstreamSawEOF := make(chan struct{}, 1)
	go func() {
		conn, err := upstreamListener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = io.Copy(io.Discard, conn) // returns when client write side closes
		close(upstreamSawEOF)
		_, _ = conn.Write([]byte("final-bytes"))
	}()
	// Client side over real TCP (half-close needs *net.TCPConn).
	clientListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer clientListener.Close()
	go func() {
		conn, err := clientListener.Accept()
		if err != nil {
			return
		}
		upstream, err := net.Dial("tcp", upstreamListener.Addr().String())
		if err != nil {
			conn.Close()
			return
		}
		p.tunnel(conn, upstream)
	}()
	user, err := net.Dial("tcp", clientListener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer user.Close()
	if _, err := user.Write([]byte("request")); err != nil {
		t.Fatal(err)
	}
	if tcp, ok := user.(*net.TCPConn); ok {
		if err := tcp.CloseWrite(); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-upstreamSawEOF:
	case <-time.After(5 * time.Second):
		t.Fatal("client half-close never reached the upstream read side")
	}
	// The origin's final bytes still make it back to the client.
	_ = user.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf, err := io.ReadAll(user)
	if err != nil || string(buf) != "final-bytes" {
		t.Fatalf("final upstream bytes lost: %q err=%v", buf, err)
	}
}

// O1 regression: in production the tunnel's client side is a
// *net.UnixConn hijacked from the unix-socket listener, not a TCPConn.
// When the upstream ends its write side, that half-close must reach the
// unix client too — otherwise the peer (the in-container bridge) never
// sees EOF and the conn+slot hang until the idle reaper.
func TestTunnelHalfCloseReachesUnixClient(t *testing.T) {
	p := NewProxy(Config{IdleTimeout: time.Minute, Logf: func(string, ...any) {}})
	// Upstream: deliver bytes, then half-close its write side and hold
	// the read side open — the client must still see EOF.
	upstreamListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer upstreamListener.Close()
	go func() {
		conn, err := upstreamListener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = conn.Write([]byte("upstream-done"))
		if tcp, ok := conn.(*net.TCPConn); ok {
			_ = tcp.CloseWrite()
		}
		_, _ = io.Copy(io.Discard, conn) // hold until the client goes away
	}()
	// Client side over a real unix socket — the production transport.
	dir := t.TempDir()
	ulistener, err := net.Listen("unix", dir+"/c.sock")
	if err != nil {
		t.Fatal(err)
	}
	defer ulistener.Close()
	go func() {
		conn, err := ulistener.Accept()
		if err != nil {
			return
		}
		upstream, err := net.Dial("tcp", upstreamListener.Addr().String())
		if err != nil {
			conn.Close()
			return
		}
		p.tunnel(conn, upstream)
	}()
	user, err := net.Dial("unix", dir+"/c.sock")
	if err != nil {
		t.Fatal(err)
	}
	defer user.Close()
	_ = user.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf, err := io.ReadAll(user) // returns at EOF on the upstream half-close
	if err != nil || string(buf) != "upstream-done" {
		t.Fatalf("unix client never saw the upstream half-close: %q err=%v", buf, err)
	}
}
