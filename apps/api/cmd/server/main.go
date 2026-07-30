package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/handler"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	app, err := newApplicationFromEnv()
	if err != nil {
		return err
	}
	defer app.Close()

	publicServer := &http.Server{
		Addr:              ":" + port,
		Handler:           app.publicMux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	var localListener net.Listener
	if app.localMux != nil {
		localListener, err = app.localListener.listen()
		if err != nil {
			return fmt.Errorf("listen on local control transport: %w", err)
		}
	}
	publicListener, err := net.Listen("tcp", publicServer.Addr)
	if err != nil {
		if localListener != nil {
			_ = localListener.Close()
		}
		return fmt.Errorf("listen on public API: %w", err)
	}

	log.Printf("sumi api listening on :%s", port)
	if app.localMux == nil {
		return serveHTTPServers(ctx, serverAndListener{server: publicServer, listener: publicListener})
	}

	localServer := &http.Server{
		Handler:           app.localListener.handler(app.localMux),
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      5 * time.Second,
		IdleTimeout:       15 * time.Second,
		MaxHeaderBytes:    16 * 1024,
	}
	log.Printf("sumi local control listening on %s", app.localListener.description())
	return serveHTTPServers(
		ctx,
		serverAndListener{server: publicServer, listener: publicListener},
		serverAndListener{server: localServer, listener: localListener},
	)
}

type serverAndListener struct {
	server   *http.Server
	listener net.Listener
}

func serveHTTPServers(ctx context.Context, servers ...serverAndListener) error {
	if len(servers) == 0 {
		return errors.New("at least one HTTP server is required")
	}
	errs := make(chan error, len(servers))
	for _, item := range servers {
		item := item
		go func() {
			errs <- item.server.Serve(item.listener)
		}()
	}

	var firstErr error
	select {
	case <-ctx.Done():
	case err := <-errs:
		if !errors.Is(err, http.ErrServerClosed) {
			firstErr = err
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, item := range servers {
		if err := item.server.Shutdown(shutdownCtx); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("shut down HTTP server: %w", err)
		}
	}
	return firstErr
}

type application struct {
	publicMux     *http.ServeMux
	localMux      *http.ServeMux
	localListener *localControlListenerConfig
	store         *agentevents.CommandStore
}

func (a *application) Close() error {
	if a == nil || a.store == nil {
		return nil
	}
	return a.store.Close()
}

func newRouter() (*http.ServeMux, error) {
	app, err := newApplicationFromEnv()
	if err != nil {
		return nil, err
	}
	return app.publicMux, nil
}

func newApplicationFromEnv() (*application, error) {
	tv, err := tokenVerifierFromEnv()
	if err != nil && !errors.Is(err, errTokenSecretMissing) {
		return nil, fmt.Errorf("agent token verifier: %w", err)
	}
	sv, err := browserSessionVerifierFromEnv()
	if err != nil && !errors.Is(err, errBrowserSessionSecretMissing) {
		return nil, fmt.Errorf("browser session verifier: %w", err)
	}

	cmdDir := os.Getenv("SUMI_COMMAND_LOG_DIR")
	if cmdDir == "" {
		return nil, errors.New("SUMI_COMMAND_LOG_DIR not set")
	}
	store, err := agentevents.OpenCommandStore(cmdDir)
	if err != nil {
		return nil, fmt.Errorf("open command store: %w", err)
	}
	runtimeDir := os.Getenv("SUMI_AGENT_RUNTIME_STATE_DIR")
	if runtimeDir == "" {
		_ = store.Close()
		return nil, errors.New("SUMI_AGENT_RUNTIME_STATE_DIR not set")
	}
	runtime, err := agentevents.OpenDurableGateway(runtimeDir, store)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("open agent runtime gateway: %w", err)
	}

	mux, _, _, err := agentevents.NewProductionMux(store, runtime, tv, sv, allowedOriginsFromEnv(), browserAllowedOriginsFromEnv())
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	localControl, enabled, err := localControlServerFromEnv(runtime)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("local control fixture: %w", err)
	}
	localListener, err := localControlListenerFromEnv(enabled)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("local control transport: %w", err)
	}
	var localMux *http.ServeMux
	if enabled {
		localMux = http.NewServeMux()
		if err := localControl.RegisterRoutes(localMux); err != nil {
			_ = store.Close()
			return nil, fmt.Errorf("register local control fixture: %w", err)
		}
	}
	mux.HandleFunc("GET /health", handler.Health)
	return &application{
		publicMux:     mux,
		localMux:      localMux,
		localListener: localListener,
		store:         store,
	}, nil
}

const (
	localControlSocketMode = 0o660
	localControlParentMode = 0o750
	maxUnixSocketPathBytes = 100
)

type localControlListenerConfig struct {
	unixSocket     string
	socketGID      int
	loopbackListen string
}

func localControlListenerFromEnv(enabled bool) (*localControlListenerConfig, error) {
	socketPath := os.Getenv("SUMI_LOCAL_CONTROL_UNIX_SOCKET")
	loopback := os.Getenv("SUMI_LOCAL_CONTROL_LOOPBACK_LISTEN")
	gidRaw := os.Getenv("SUMI_LOCAL_CONTROL_SOCKET_GID")
	if !enabled {
		if socketPath != "" || loopback != "" || gidRaw != "" {
			return nil, errors.New("transport settings require SUMI_LOCAL_CONTROL_ENABLED=1")
		}
		return nil, nil
	}
	if (socketPath == "") == (loopback == "") {
		return nil, errors.New("exactly one of SUMI_LOCAL_CONTROL_UNIX_SOCKET or SUMI_LOCAL_CONTROL_LOOPBACK_LISTEN is required")
	}
	if loopback != "" {
		if gidRaw != "" {
			return nil, errors.New("SUMI_LOCAL_CONTROL_SOCKET_GID is only valid with SUMI_LOCAL_CONTROL_UNIX_SOCKET")
		}
		if err := validateLoopbackListen(loopback); err != nil {
			return nil, err
		}
		return &localControlListenerConfig{loopbackListen: loopback}, nil
	}
	if gidRaw == "" {
		return nil, errors.New("SUMI_LOCAL_CONTROL_SOCKET_GID is required with SUMI_LOCAL_CONTROL_UNIX_SOCKET")
	}
	gid, err := strconv.ParseUint(gidRaw, 10, 31)
	if err != nil {
		return nil, errors.New("SUMI_LOCAL_CONTROL_SOCKET_GID must be a nonnegative decimal GID")
	}
	if err := validateUnixSocketPath(socketPath); err != nil {
		return nil, err
	}
	return &localControlListenerConfig{unixSocket: socketPath, socketGID: int(gid)}, nil
}

func validateLoopbackListen(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("SUMI_LOCAL_CONTROL_LOOPBACK_LISTEN must be host:port: %w", err)
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return errors.New("SUMI_LOCAL_CONTROL_LOOPBACK_LISTEN host must be a literal loopback IP")
	}
	if _, err := strconv.ParseUint(port, 10, 16); err != nil {
		return errors.New("SUMI_LOCAL_CONTROL_LOOPBACK_LISTEN port must be numeric")
	}
	return nil
}

func validateUnixSocketPath(path string) error {
	if !filepath.IsAbs(path) {
		return errors.New("SUMI_LOCAL_CONTROL_UNIX_SOCKET must be absolute")
	}
	if filepath.Clean(path) != path {
		return errors.New("SUMI_LOCAL_CONTROL_UNIX_SOCKET must be a clean path")
	}
	if len(path) > maxUnixSocketPathBytes {
		return fmt.Errorf("SUMI_LOCAL_CONTROL_UNIX_SOCKET exceeds %d bytes", maxUnixSocketPathBytes)
	}
	if filepath.Base(path) == "." || filepath.Base(path) == string(filepath.Separator) {
		return errors.New("SUMI_LOCAL_CONTROL_UNIX_SOCKET must name a socket file")
	}
	return nil
}

func (c *localControlListenerConfig) description() string {
	if c.unixSocket != "" {
		return "unix socket " + c.unixSocket
	}
	return "loopback " + c.loopbackListen
}

func (c *localControlListenerConfig) listen() (net.Listener, error) {
	if c == nil {
		return nil, errors.New("local control listener config is required")
	}
	if c.loopbackListen != "" {
		return net.Listen("tcp", c.loopbackListen)
	}
	return listenTrustedUnixSocket(c.unixSocket, c.socketGID)
}

func (c *localControlListenerConfig) handler(next http.Handler) http.Handler {
	if c == nil || c.unixSocket == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		localRequest := r.Clone(r.Context())
		localRequest.RemoteAddr = "127.0.0.1:0"
		next.ServeHTTP(w, localRequest)
	})
}

func listenTrustedUnixSocket(path string, gid int) (*net.UnixListener, error) {
	if err := validateUnixSocketPath(path); err != nil {
		return nil, err
	}
	parent := filepath.Dir(path)
	parentInfo, err := os.Lstat(parent)
	if err != nil {
		return nil, fmt.Errorf("inspect local control socket parent: %w", err)
	}
	if !parentInfo.IsDir() || parentInfo.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("local control socket parent must be a real directory")
	}
	resolvedParent, err := filepath.EvalSymlinks(parent)
	if err != nil {
		return nil, fmt.Errorf("resolve local control socket parent: %w", err)
	}
	if resolvedParent != parent {
		return nil, errors.New("local control socket parent path must not contain symlinks")
	}
	parentStat, ok := parentInfo.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, errors.New("local control socket parent metadata is unavailable")
	}
	if int(parentStat.Uid) != os.Geteuid() || int(parentStat.Gid) != gid {
		return nil, errors.New("local control socket parent owner or group is not trusted")
	}
	if parentInfo.Mode().Perm() != localControlParentMode {
		return nil, fmt.Errorf("local control socket parent mode must be %04o", localControlParentMode)
	}

	if err := removeTrustedStaleSocket(path, gid); err != nil {
		return nil, err
	}
	oldUmask := syscall.Umask(0o177)
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	syscall.Umask(oldUmask)
	if err != nil {
		return nil, err
	}
	listener.SetUnlinkOnClose(true)
	fail := func(cause error) (*net.UnixListener, error) {
		_ = listener.Close()
		return nil, cause
	}
	if err := os.Chown(path, os.Geteuid(), gid); err != nil {
		return fail(fmt.Errorf("set local control socket owner: %w", err))
	}
	if err := os.Chmod(path, localControlSocketMode); err != nil {
		return fail(fmt.Errorf("set local control socket mode: %w", err))
	}
	if err := validateSocketMetadata(path, gid); err != nil {
		return fail(err)
	}
	return listener, nil
}

func removeTrustedStaleSocket(path string, gid int) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect existing local control socket: %w", err)
	}
	first, err := trustedSocketStat(info, gid)
	if err != nil {
		return fmt.Errorf("refuse existing local control socket target: %w", err)
	}
	probe, probeErr := net.DialTimeout("unix", path, 100*time.Millisecond)
	if probeErr == nil {
		_ = probe.Close()
		return errors.New("refuse to replace a live local control socket")
	}
	if !errors.Is(probeErr, syscall.ECONNREFUSED) {
		return fmt.Errorf("cannot prove existing local control socket is stale: %w", probeErr)
	}
	current, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("reinspect existing local control socket: %w", err)
	}
	second, err := trustedSocketStat(current, gid)
	if err != nil {
		return fmt.Errorf("refuse replaced local control socket target: %w", err)
	}
	if first.Dev != second.Dev || first.Ino != second.Ino {
		return errors.New("local control socket changed during stale cleanup")
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("remove trusted stale local control socket: %w", err)
	}
	return nil
}

func validateSocketMetadata(path string, gid int) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect local control socket: %w", err)
	}
	_, err = trustedSocketStat(info, gid)
	return err
}

func trustedSocketStat(info os.FileInfo, gid int) (*syscall.Stat_t, error) {
	if info.Mode()&os.ModeSocket == 0 || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("target is not a Unix socket")
	}
	if info.Mode().Perm() != localControlSocketMode {
		return nil, fmt.Errorf("socket mode must be %04o", localControlSocketMode)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, errors.New("socket metadata is unavailable")
	}
	if stat.Nlink != 1 {
		return nil, errors.New("socket link count must be exactly one")
	}
	if int(stat.Uid) != os.Geteuid() || int(stat.Gid) != gid {
		return nil, errors.New("socket owner or group is not trusted")
	}
	return stat, nil
}

func allowedOriginsFromEnv() []string {
	return originsFromEnv("SUMI_AGENT_WS_ALLOWED_ORIGINS")
}

func browserAllowedOriginsFromEnv() []string {
	return originsFromEnv("SUMI_BROWSER_WS_ALLOWED_ORIGINS")
}

func originsFromEnv(name string) []string {
	raw := os.Getenv(name)
	if raw == "" {
		return nil
	}
	var origins []string
	for _, o := range strings.Split(raw, ",") {
		o = strings.TrimSpace(o)
		if o != "" {
			origins = append(origins, o)
		}
	}
	return origins
}

var errTokenSecretMissing = errors.New("SUMI_AGENT_TOKEN_SECRET not set")
var errBrowserSessionSecretMissing = errors.New("SUMI_BROWSER_SESSION_SECRET not set")

func tokenVerifierFromEnv() (agentevents.TokenVerifier, error) {
	secret, err := tokenSecretFromEnv()
	if err != nil {
		return nil, err
	}
	audience := os.Getenv("SUMI_AGENT_TOKEN_AUDIENCE")
	return agentevents.NewHMACTokenVerifier(secret, audience)
}

func tokenSecretFromEnv() ([]byte, error) {
	b64 := os.Getenv("SUMI_AGENT_TOKEN_SECRET")
	if b64 == "" {
		return nil, errTokenSecretMissing
	}
	return base64.StdEncoding.DecodeString(b64)
}

// localControlServerFromEnv constructs one exact runtime authorization binding.
// Enabling it also requires an explicit dedicated listener; these routes are
// never registered on the public API mux.
func localControlServerFromEnv(runtime *agentevents.DurableGateway) (*agentevents.LocalControlServer, bool, error) {
	switch enabled := os.Getenv("SUMI_LOCAL_CONTROL_ENABLED"); enabled {
	case "", "0":
		return nil, false, nil
	case "1":
	default:
		return nil, false, errors.New("SUMI_LOCAL_CONTROL_ENABLED must be 0 or 1")
	}

	required := func(name string) (string, error) {
		value := os.Getenv(name)
		if value == "" {
			return "", fmt.Errorf("%s not set", name)
		}
		return value, nil
	}
	bearer, err := required("SUMI_LOCAL_CONTROL_BEARER")
	if err != nil {
		return nil, false, err
	}
	tenantID, err := required("SUMI_LOCAL_CONTROL_TENANT_ID")
	if err != nil {
		return nil, false, err
	}
	personalityAgentID, err := required("SUMI_LOCAL_CONTROL_PERSONALITY_AGENT_ID")
	if err != nil {
		return nil, false, err
	}
	generationRaw, err := required("SUMI_LOCAL_CONTROL_GENERATION")
	if err != nil {
		return nil, false, err
	}
	generation, err := strconv.ParseUint(generationRaw, 10, 64)
	if err != nil {
		return nil, false, fmt.Errorf("parse SUMI_LOCAL_CONTROL_GENERATION: %w", err)
	}
	rpcBootNonce, err := required("SUMI_LOCAL_CONTROL_RPC_BOOT_NONCE")
	if err != nil {
		return nil, false, err
	}
	deliveryRaw, err := required("SUMI_LOCAL_CONTROL_DELIVERY_AUTHORIZATION")
	if err != nil {
		return nil, false, err
	}
	signingSecret, err := tokenSecretFromEnv()
	if err != nil {
		return nil, false, err
	}
	agentAudience := os.Getenv("SUMI_AGENT_TOKEN_AUDIENCE")
	if agentAudience == "" {
		agentAudience = agentevents.DefaultAgentAudience()
	}
	controlAudience := os.Getenv("SUMI_LOCAL_CONTROL_AUDIENCE")
	if controlAudience == "" {
		controlAudience = agentAudience
	}
	if controlAudience != agentAudience {
		return nil, false, errors.New("SUMI_LOCAL_CONTROL_AUDIENCE must match SUMI_AGENT_TOKEN_AUDIENCE")
	}
	control, err := agentevents.NewLocalControlServer(
		runtime,
		signingSecret,
		[]agentevents.LocalRuntimeAuthorization{{
			BearerToken:           bearer,
			TenantID:              tenantID,
			PersonalityAgentID:    personalityAgentID,
			Generation:            generation,
			RPCBootNonce:          rpcBootNonce,
			Audience:              controlAudience,
			DeliveryAuthorization: agentevents.LocalDeliveryAuthorization(deliveryRaw),
		}},
	)
	if err != nil {
		return nil, false, err
	}
	return control, true, nil
}

// browserSessionVerifierFromEnv is deliberately separate from the agent token
// verifier. Browser sessions are HttpOnly cookies scoped to users and
// conversations; agent bearer tokens never enter this route.
func browserSessionVerifierFromEnv() (agentevents.UserSessionVerifier, error) {
	b64 := os.Getenv("SUMI_BROWSER_SESSION_SECRET")
	if b64 == "" {
		return nil, errBrowserSessionSecretMissing
	}
	secret, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return nil, err
	}
	audience := os.Getenv("SUMI_BROWSER_SESSION_AUDIENCE")
	return agentevents.NewHMACUserSessionVerifier(secret, audience)
}
