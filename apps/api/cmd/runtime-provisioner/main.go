package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/runtimeprovision"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	var socketPath string
	var socketGID int
	var supervisorPath string
	var socketModeText string
	flag.StringVar(&socketPath, "socket", "/run/sumi/runtime-provisioner/control.sock", "root-managed Unix socket")
	flag.IntVar(&socketGID, "socket-gid", 0, "group allowed to connect to the Unix socket")
	flag.StringVar(&socketModeText, "socket-mode", "0660", "Unix socket permission mode")
	flag.StringVar(&supervisorPath, "supervisor", "/usr/local/libexec/sumi-agent-supervisor", "host Docker supervisor")
	flag.Parse()

	parsedMode, err := strconv.ParseUint(socketModeText, 8, 32)
	if err != nil {
		return fmt.Errorf("parse socket mode: %w", err)
	}
	backend, err := runtimeprovision.NewDockerBackend(runtimeprovision.DockerBackendConfig{
		SupervisorPath:  supervisorPath,
		BaseEnvironment: hostEnvironment(),
	})
	if err != nil {
		return err
	}
	service, err := runtimeprovision.NewService(backend)
	if err != nil {
		return err
	}
	handler, err := runtimeprovision.NewHandler(service)
	if err != nil {
		return err
	}
	listener, err := runtimeprovision.ListenUnix(runtimeprovision.UnixListenerConfig{
		SocketPath: socketPath,
		SocketGID:  socketGID,
		SocketMode: os.FileMode(parsedMode),
	})
	if err != nil {
		return err
	}
	defer listener.Close()

	server := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		// Compose builds and synchronous teardown are bounded by the backend's
		// 15-minute operation timeout; do not sever a committed transition and
		// force the API into recovery before that bound expires.
		WriteTimeout: 20 * time.Minute,
		IdleTimeout:  30 * time.Second,
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	serveError := make(chan error, 1)
	go func() { serveError <- server.Serve(listener) }()
	select {
	case err := <-serveError:
		if err != nil && err != http.ErrServerClosed {
			return err
		}
		return nil
	case <-ctx.Done():
		// Exact stop/abort operations are bounded at 90 seconds by the backend.
		// Keep serving active lifecycle calls long enough to join them cleanly.
		shutdownContext, shutdownCancel := context.WithTimeout(context.Background(), 110*time.Second)
		defer shutdownCancel()
		return server.Shutdown(shutdownContext)
	}
}

func hostEnvironment() []string {
	names := []string{"PATH", "HOME", "LANG", "DOCKER_HOST", "DOCKER_CONFIG", "SUMI_CONFIG_FILE", "SUMI_CONTROL_PLANE_NETWORK"}
	environment := make([]string, 0, len(names))
	for _, name := range names {
		if value, ok := os.LookupEnv(name); ok {
			environment = append(environment, name+"="+value)
		}
	}
	return environment
}
