// Package egressproxy implements the confined public-egress path for
// --network none job containers.
//
// A job container has no NIC, route, or DNS. Its only network surface is a
// bind-mounted unix socket served by Proxy on the deployment side, plus a
// loopback TCP bridge inside the container (Bridge) that splices
// 127.0.0.1 connections onto that socket. Tools in the job use the loopback
// address through ordinary HTTP_PROXY/HTTPS_PROXY environment variables.
//
// Every proxied request is authorized at connection time: the destination
// host is resolved fresh, every resolved address must satisfy
// publicweb.IsPublicAddress, and the proxy then dials only the validated
// IP literals — the checked set and the dialed set are identical, so a DNS
// change between check and dial cannot redirect a connection. Redirects are
// never followed inside the proxy; a redirect answer returns to the client,
// whose next request is re-resolved and re-checked. CONNECT tunnels are
// accepted for port 443 only, after the same check, and forward plain
// HTTP is limited to port 80. Everything else is refused.
package egressproxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/publicweb"
)

const (
	// ConnectPort is the only TCP port reachable through CONNECT — package
	// managers, git-over-HTTPS and fetch tools all use HTTPS.
	ConnectPort = "443"
	// ForwardPort is the only TCP port reachable for plain-HTTP forwarding.
	ForwardPort = "80"
	maxResolved = 16
)

var (
	errNotPublic = errors.New("destination is not a public address")
	errBadTarget = errors.New("invalid proxy target")
	errWrongPort = errors.New("port not permitted")
	errDNS       = errors.New("destination resolution failed")
)

type resolver interface {
	LookupNetIP(context.Context, string, string) ([]netip.Addr, error)
}

// Proxy is an HTTP forward proxy restricted to public TCP destinations.
// It serves on the listener it is given — the deployment binds a unix
// socket shared into job containers — and never terminates TLS.
type Proxy struct {
	resolver       resolver
	dial           func(context.Context, string, string) (net.Conn, error)
	logf           func(string, ...any)
	slots          chan struct{}
	dialTimeout    time.Duration
	idleTimeout    time.Duration
	requestTimeout time.Duration
}

// Config tunes Proxy. Zero values take the documented defaults.
type Config struct {
	// MaxConnections bounds in-flight CONNECT tunnels and forwarded
	// requests together. Default 256.
	MaxConnections int
	// DialTimeout bounds each upstream connect attempt. Default 10s.
	DialTimeout time.Duration
	// IdleTimeout closes an idle CONNECT tunnel after this much silence in
	// both directions. Default 120s.
	IdleTimeout time.Duration
	// RequestTimeout bounds one forwarded plain-HTTP request end to end.
	// Default 10m.
	RequestTimeout time.Duration
	// Logf receives one line per request verdict. Default: log.Printf.
	Logf func(string, ...any)
	// Resolver and Dial are test seams; nil selects net.DefaultResolver and
	// a real net.Dialer.
	Resolver interface {
		LookupNetIP(context.Context, string, string) ([]netip.Addr, error)
	}
	Dial func(context.Context, string, string) (net.Conn, error)
}

func NewProxy(config Config) *Proxy {
	p := &Proxy{
		resolver:       config.Resolver,
		dial:           config.Dial,
		logf:           config.Logf,
		slots:          make(chan struct{}, 256),
		dialTimeout:    10 * time.Second,
		idleTimeout:    120 * time.Second,
		requestTimeout: 10 * time.Minute,
	}
	if p.resolver == nil {
		p.resolver = net.DefaultResolver
	}
	if p.dial == nil {
		dialer := &net.Dialer{Timeout: p.dialTimeout}
		p.dial = dialer.DialContext
	}
	if p.logf == nil {
		p.logf = log.Printf
	}
	if config.MaxConnections > 0 {
		p.slots = make(chan struct{}, config.MaxConnections)
	}
	if config.DialTimeout > 0 {
		p.dialTimeout = config.DialTimeout
	}
	if config.IdleTimeout > 0 {
		p.idleTimeout = config.IdleTimeout
	}
	if config.RequestTimeout > 0 {
		p.requestTimeout = config.RequestTimeout
	}
	return p
}

// ServeHTTP implements http.Handler: CONNECT for HTTPS tunneling,
// absolute-form http:// requests for plain HTTP.
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if p == nil {
		http.Error(w, "sumi-egress: proxy unavailable", http.StatusServiceUnavailable)
		return
	}
	select {
	case p.slots <- struct{}{}:
		defer func() { <-p.slots }()
	default:
		http.Error(w, "sumi-egress: busy", http.StatusTooManyRequests)
		return
	}
	if r.Method == http.MethodConnect {
		p.handleConnect(w, r)
		return
	}
	p.handleForward(w, r)
}

// publicAddrs resolves a destination host and returns the validated
// address set. An IP literal is checked directly; a DNS name is resolved
// fresh and the whole answer must be public — one private address in a
// mixed answer denies the destination, so a hostname can never serve as a
// blended public/private alias.
func (p *Proxy) publicAddrs(ctx context.Context, host string) ([]netip.Addr, error) {
	host = strings.TrimSuffix(host, ".")
	if ip, err := netip.ParseAddr(host); err == nil {
		if !publicweb.IsPublicAddress(ip) {
			return nil, errNotPublic
		}
		return []netip.Addr{ip}, nil
	}
	if !validHostName(host) {
		return nil, errBadTarget
	}
	addrs, err := p.resolver.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errDNS, err)
	}
	if len(addrs) == 0 || len(addrs) > maxResolved {
		return nil, errDNS
	}
	for _, ip := range addrs {
		if !publicweb.IsPublicAddress(ip) {
			return nil, errNotPublic
		}
	}
	return addrs, nil
}

// dialPinned connects to the validated address set — never back through
// the resolver — so the dialed destination is exactly what was checked.
func (p *Proxy) dialPinned(ctx context.Context, addrs []netip.Addr, port string) (net.Conn, error) {
	var last error
	for _, ip := range addrs {
		attempt, cancel := context.WithTimeout(ctx, p.dialTimeout)
		connection, err := p.dial(attempt, "tcp", net.JoinHostPort(ip.String(), port))
		cancel()
		if err == nil {
			return connection, nil
		}
		last = err
		if ctx.Err() != nil {
			break
		}
	}
	if last == nil {
		last = errors.New("no addresses")
	}
	return nil, last
}

func (p *Proxy) deny(w http.ResponseWriter, r *http.Request, status int, err error) {
	p.logf("egress deny %s %s: %v", r.Method, requestTarget(r), err)
	http.Error(w, "sumi-egress: "+err.Error(), status)
}

func requestTarget(r *http.Request) string {
	if r.Method == http.MethodConnect {
		return r.Host
	}
	if r.URL != nil {
		return r.URL.String()
	}
	return ""
}

// handleConnect opens a TCP tunnel to host:443 after the destination
// check, then splices bytes in both directions. TLS stays end-to-end
// between the job and the origin — the proxy never sees plaintext.
func (p *Proxy) handleConnect(w http.ResponseWriter, r *http.Request) {
	host, port, err := splitAuthority(r.Host)
	if err != nil || host == "" {
		p.deny(w, r, http.StatusBadRequest, errBadTarget)
		return
	}
	if port != ConnectPort {
		p.deny(w, r, http.StatusForbidden, errWrongPort)
		return
	}
	addrs, err := p.publicAddrs(r.Context(), host)
	if err != nil {
		status := http.StatusForbidden
		if errors.Is(err, errDNS) {
			status = http.StatusBadGateway
		}
		p.deny(w, r, status, err)
		return
	}
	upstream, err := p.dialPinned(r.Context(), addrs, ConnectPort)
	if err != nil {
		p.logf("egress connect %s: upstream dial failed: %v", r.Host, err)
		http.Error(w, "sumi-egress: upstream connect failed", http.StatusBadGateway)
		return
	}
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		upstream.Close()
		http.Error(w, "sumi-egress: tunneling unsupported", http.StatusInternalServerError)
		return
	}
	client, buffered, err := hijacker.Hijack()
	if err != nil {
		upstream.Close()
		return
	}
	if buffered.Reader.Buffered() > 0 || buffered.Writer.Buffered() > 0 {
		// A CONNECT request carries no body; buffered bytes mean the client
		// pipelined ahead of the tunnel grant, which this proxy does not
		// serve — close rather than misroute bytes.
		client.Close()
		upstream.Close()
		return
	}
	if _, err := client.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n")); err != nil {
		client.Close()
		upstream.Close()
		return
	}
	p.logf("egress connect %s: tunnel open", r.Host)
	p.tunnel(client, upstream)
}

// tunnel relays both directions until EOF, an error, or the idle
// deadline. Activity in either direction extends the deadline, so a live
// download is never cut while a stalled tunnel is reaped.
func (p *Proxy) tunnel(client, upstream net.Conn) {
	defer client.Close()
	defer upstream.Close()
	var activity atomic.Int64
	activity.Store(time.Now().UnixNano())
	idle := p.idleTimeout
	done := make(chan struct{})
	defer close(done)
	go func() {
		ticker := time.NewTicker(idle / 4)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				if time.Since(time.Unix(0, activity.Load())) > idle {
					client.Close()
					upstream.Close()
					return
				}
			}
		}
	}()
	var wg sync.WaitGroup
	splice := func(dst net.Conn, src net.Conn) {
		defer wg.Done()
		buffer := make([]byte, 32<<10)
		for {
			n, err := src.Read(buffer)
			if n > 0 {
				activity.Store(time.Now().UnixNano())
				if _, werr := dst.Write(buffer[:n]); werr != nil {
					return
				}
			}
			if err != nil {
				return
			}
		}
	}
	wg.Add(2)
	go splice(upstream, client)
	go splice(client, upstream)
	wg.Wait()
}

// handleForward proxies a plain-HTTP request: the request line must carry
// an absolute http:// URI on port 80. The destination is resolved and
// checked the same way as CONNECT, and the transport dials only the
// validated addresses. The response — including any redirect — returns
// verbatim; the client issues a new request that is checked again.
func (p *Proxy) handleForward(w http.ResponseWriter, r *http.Request) {
	if r.URL == nil || !r.URL.IsAbs() || !strings.EqualFold(r.URL.Scheme, "http") || r.URL.Host == "" {
		p.deny(w, r, http.StatusBadRequest, errBadTarget)
		return
	}
	if r.URL.User != nil {
		p.deny(w, r, http.StatusBadRequest, errBadTarget)
		return
	}
	host := r.URL.Hostname()
	if port := r.URL.Port(); port != "" && port != ForwardPort {
		p.deny(w, r, http.StatusForbidden, errWrongPort)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), p.requestTimeout)
	defer cancel()
	addrs, err := p.publicAddrs(ctx, host)
	if err != nil {
		status := http.StatusForbidden
		if errors.Is(err, errDNS) {
			status = http.StatusBadGateway
		}
		p.deny(w, r, status, err)
		return
	}
	outbound := r.Clone(ctx)
	outbound.RequestURI = ""
	outbound.URL.Scheme = "http"
	outbound.URL.Host = net.JoinHostPort(host, ForwardPort)
	// The origin sees the authority the client asked for; the dial itself
	// is pinned to the validated addresses below.
	outbound.Host = r.Host
	stripHopHeaders(outbound.Header)
	transport := &http.Transport{
		Proxy:                  nil,
		DisableKeepAlives:      true,
		DisableCompression:     true,
		MaxResponseHeaderBytes: 64 << 10,
		ResponseHeaderTimeout:  30 * time.Second,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			if address != net.JoinHostPort(host, ForwardPort) {
				return nil, errBadTarget
			}
			return p.dialPinned(ctx, addrs, ForwardPort)
		},
	}
	defer transport.CloseIdleConnections()
	response, err := transport.RoundTrip(outbound)
	if err != nil {
		p.logf("egress forward %s: upstream failed: %v", r.URL.String(), err)
		http.Error(w, "sumi-egress: upstream request failed", http.StatusBadGateway)
		return
	}
	defer response.Body.Close()
	for key, values := range response.Header {
		if hopHeader[key] {
			continue
		}
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(response.StatusCode)
	_, _ = io.Copy(flushWriter{w}, response.Body)
}

var hopHeader = map[string]bool{
	"Connection":          true,
	"Proxy-Connection":    true,
	"Proxy-Authorization": true,
	"Proxy-Authenticate":  true,
	"Keep-Alive":          true,
	"Te":                  true,
	"Trailer":             true,
	"Transfer-Encoding":   true,
	"Upgrade":             true,
}

// stripHopHeaders removes hop-by-hop and proxy-control headers, including
// any headers nominated by the Connection token list.
func stripHopHeaders(header http.Header) {
	for _, value := range header.Values("Connection") {
		for _, name := range strings.Split(value, ",") {
			if name = strings.TrimSpace(name); name != "" {
				header.Del(name)
			}
		}
	}
	for name := range hopHeader {
		header.Del(name)
	}
}

// flushWriter makes response streaming immediate for SSE/chunked replies.
type flushWriter struct{ http.ResponseWriter }

func (w flushWriter) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
	return n, err
}

// splitAuthority parses a CONNECT authority (host:port). Bare IP literals,
// bracketed IPv6 and DNS names are all accepted; the port is mandatory.
func splitAuthority(authority string) (host, port string, err error) {
	host, port, err = net.SplitHostPort(authority)
	if err != nil {
		return "", "", err
	}
	if strings.TrimSpace(host) != host || strings.ContainsAny(host, "/@") {
		return "", "", errors.New("bad host")
	}
	return host, port, nil
}

// validHostName accepts DNS-style names. It is deliberately strict — a
// name that is not a hostname cannot be a smuggled URL, auth string, or
// option payload.
func validHostName(host string) bool {
	if host == "" || len(host) > 253 {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if label == "" || len(label) > 63 {
			return false
		}
		for i := 0; i < len(label); i++ {
			c := label[i]
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
				return false
			}
		}
	}
	return true
}

// Bridge serves the in-container end of the egress path: a loopback-only
// TCP listener whose every accepted connection is spliced onto the shared
// unix socket. The proxy on the other end applies all destination checks,
// so the bridge itself has no policy — it cannot reach anything but the
// socket it was given.
type Bridge struct {
	listenAddr string
	socketPath string
	slots      chan struct{}
	dialer     net.Dialer
	logf       func(string, ...any)
}

func NewBridge(listenAddr, socketPath string, maxConns int, logf func(string, ...any)) *Bridge {
	if maxConns <= 0 {
		maxConns = 256
	}
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &Bridge{listenAddr: listenAddr, socketPath: socketPath, slots: make(chan struct{}, maxConns), dialer: net.Dialer{Timeout: 5 * time.Second}, logf: logf}
}

// Serve accepts connections until the listener fails or ctx ends. Each
// connection is forwarded onto the unix socket; when the socket is absent
// the client sees a refused/closed connection — the honest failure the
// job's tool reports.
func (b *Bridge) Serve(ctx context.Context) error {
	listener, err := net.Listen("tcp", b.listenAddr)
	if err != nil {
		return err
	}
	defer listener.Close()
	go func() {
		<-ctx.Done()
		listener.Close()
	}()
	b.logf("egress bridge listening on %s -> %s", b.listenAddr, b.socketPath)
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		select {
		case b.slots <- struct{}{}:
			go func() {
				defer func() { <-b.slots }()
				b.relay(conn)
			}()
		default:
			conn.Close()
		}
	}
}

func (b *Bridge) relay(client net.Conn) {
	defer client.Close()
	upstream, err := b.dialer.DialContext(context.Background(), "unix", b.socketPath)
	if err != nil {
		return
	}
	defer upstream.Close()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = io.Copy(upstream, client)
		if tcp, ok := upstream.(*net.UnixConn); ok {
			_ = tcp.CloseWrite()
		}
	}()
	go func() {
		defer wg.Done()
		_, _ = io.Copy(client, upstream)
		if tcp, ok := client.(*net.TCPConn); ok {
			_ = tcp.CloseWrite()
		}
	}()
	wg.Wait()
}
