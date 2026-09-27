// sumi-egress-bridge runs inside a --network none job container. It
// listens on loopback TCP and splices each accepted connection onto the
// bind-mounted egress unix socket, giving ordinary tools a standard
// HTTP_PROXY endpoint while the container itself keeps no NIC at all.
// The destination policy lives entirely in the proxy behind the socket.
package main

import (
	"context"
	"flag"
	"log"
	"os/signal"
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
	var listenAddr, socketPath string
	var maxConns int
	flag.StringVar(&listenAddr, "listen", "127.0.0.1:3128", "loopback listen address inside the job container")
	flag.StringVar(&socketPath, "socket", "/run/sumi/egress/proxy.sock", "egress unix socket bind-mounted from the host")
	flag.IntVar(&maxConns, "concurrency", 256, "maximum in-flight relayed connections")
	flag.Parse()
	bridge := egressproxy.NewBridge(listenAddr, socketPath, maxConns, log.Printf)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- bridge.Serve(ctx) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		// Give Serve a moment to observe the listener close, then exit —
		// the container is about to end anyway.
		timer := time.NewTimer(2 * time.Second)
		defer timer.Stop()
		select {
		case err := <-done:
			return err
		case <-timer.C:
			return nil
		}
	}
}
