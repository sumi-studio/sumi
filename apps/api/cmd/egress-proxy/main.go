// sumi-egress-proxy is the deployment side of the job egress path. It
// listens on a unix socket that is bind-mounted into --network none job
// containers, and forwards only connections whose destination resolved to
// public TCP :443 (CONNECT) or :80 (plain HTTP) addresses. It holds no
// credentials and opens no listener any container could reach over TCP.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/egressproxy"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	var socketPath, socketModeText string
	var maxConns int
	var idleTimeout, requestTimeout, dialTimeout time.Duration
	flag.StringVar(&socketPath, "socket", "/run/sumi/egress/proxy.sock", "unix socket served to job containers")
	flag.StringVar(&socketModeText, "socket-mode", "0622", "permission mode applied to the socket (job uid only needs connect)")
	flag.IntVar(&maxConns, "concurrency", 256, "maximum in-flight tunnels and forwarded requests")
	flag.DurationVar(&idleTimeout, "idle-timeout", 120*time.Second, "close a CONNECT tunnel after this much silence")
	flag.DurationVar(&requestTimeout, "request-timeout", 10*time.Minute, "bound on one forwarded HTTP request")
	flag.DurationVar(&dialTimeout, "dial-timeout", 10*time.Second, "bound on one upstream connect attempt")
	flag.Parse()
	mode, err := strconv.ParseUint(socketModeText, 8, 32)
	if err != nil {
		return fmt.Errorf("parse socket mode: %w", err)
	}

	// A stale socket file from a dead proxy blocks Listen; a live one must
	// never be replaced — probe before removing so a second instance fails
	// instead of stealing the path.
	if _, err := os.Stat(socketPath); err == nil {
		probe, dialErr := (&net.Dialer{Timeout: time.Second}).Dial("unix", socketPath)
		if dialErr == nil {
			probe.Close()
			return fmt.Errorf("egress proxy socket %s is already serving", socketPath)
		}
		if err := os.Remove(socketPath); err != nil {
			return fmt.Errorf("remove stale egress socket: %w", err)
		}
	}
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return fmt.Errorf("listen %s: %w", socketPath, err)
	}
	defer listener.Close()
	if err := os.Chmod(socketPath, os.FileMode(mode)); err != nil {
		return fmt.Errorf("chmod egress socket: %w", err)
	}
	proxy := egressproxy.NewProxy(egressproxy.Config{
		MaxConnections: maxConns,
		DialTimeout:    dialTimeout,
		IdleTimeout:    idleTimeout,
		RequestTimeout: requestTimeout,
	})
	server := &http.Server{
		Handler:           proxy,
		ReadHeaderTimeout: 10 * time.Second,
		// No ReadTimeout/WriteTimeout: forwarded bodies and CONNECT tunnels
		// are long-lived; their bounds live in the proxy itself.
		MaxHeaderBytes: 64 << 10,
	}
	log.Printf("sumi-egress-proxy serving %s (mode %04o)", socketPath, mode)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	serveError := make(chan error, 1)
	go func() { serveError <- server.Serve(listener) }()
	select {
	case err := <-serveError:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return server.Shutdown(shutdown)
	}
}
