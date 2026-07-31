package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/handler"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
	"golang.org/x/sys/unix"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) (runErr error) {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	publicAddress, err := publicListenAddressFromEnv(port)
	if err != nil {
		return err
	}

	app, err := newApplicationFromEnv()
	if err != nil {
		return err
	}
	defer func() {
		runErr = errors.Join(runErr, app.Close())
	}()

	publicServer := &http.Server{
		Addr:              publicAddress,
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

	log.Printf("sumi api listening on %s", publicListener.Addr())
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

func publicListenAddressFromEnv(port string) (string, error) {
	publicAddress := strings.TrimSpace(os.Getenv("SUMI_PUBLIC_LISTEN"))
	loopbackAddress := strings.TrimSpace(os.Getenv("SUMI_PUBLIC_LOOPBACK_LISTEN"))
	if publicAddress != "" && loopbackAddress != "" {
		return "", errors.New("SUMI_PUBLIC_LISTEN and SUMI_PUBLIC_LOOPBACK_LISTEN are mutually exclusive")
	}
	if publicAddress != "" {
		return literalListenAddress("SUMI_PUBLIC_LISTEN", publicAddress, false)
	}
	if loopbackAddress == "" {
		return ":" + port, nil
	}
	return literalListenAddress("SUMI_PUBLIC_LOOPBACK_LISTEN", loopbackAddress, true)
}

func literalListenAddress(name, address string, requireLoopback bool) (string, error) {
	host, configuredPort, err := net.SplitHostPort(address)
	if err != nil {
		return "", fmt.Errorf("%s must be host:port: %w", name, err)
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return "", fmt.Errorf("%s host must be a literal IP", name)
	}
	if requireLoopback && !ip.IsLoopback() {
		return "", fmt.Errorf("%s host must be a literal loopback IP", name)
	}
	if !requireLoopback && (ip.IsUnspecified() || ip.IsMulticast()) {
		return "", fmt.Errorf("%s host must not be unspecified or multicast", name)
	}
	if configuredPort == "" {
		return "", fmt.Errorf("%s port must be an integer from 1 to 65535", name)
	}
	for _, digit := range configuredPort {
		if digit < '0' || digit > '9' {
			return "", fmt.Errorf("%s port must be an integer from 1 to 65535", name)
		}
	}
	numericPort, err := strconv.Atoi(configuredPort)
	if err != nil || numericPort < 1 || numericPort > 65535 {
		return "", fmt.Errorf("%s port must be an integer from 1 to 65535", name)
	}
	return net.JoinHostPort(ip.String(), strconv.Itoa(numericPort)), nil
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
	browser       *agentevents.BrowserServer
	database      *db.Pool
	closeOnce     sync.Once
	closeErr      error
}

func (a *application) Close() error {
	if a == nil {
		return nil
	}
	a.closeOnce.Do(func() {
		if a.browser != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			a.closeErr = errors.Join(a.closeErr, a.browser.ShutdownBrowserConnections(ctx))
			cancel()
		}
		if a.store != nil {
			a.closeErr = errors.Join(a.closeErr, a.store.Close())
		}
		if a.database != nil {
			a.database.Close()
		}
	})
	return a.closeErr
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
	sv, browserOrigins, err := browserSessionConfigFromEnv(runtime)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("browser session configuration: %w", err)
	}
	database, err := databaseFromEnv(context.Background())
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("control-plane database: %w", err)
	}
	closeOnError := func() {
		_ = store.Close()
		database.Close()
	}

	var directChatAuthorizer agentevents.DirectChatAuthorizer
	if database != nil {
		directChatAuthorizer = newKosekiDirectChatAuthorizer(koseki.New(database.Pool))
	}
	mux, browser, _, err := agentevents.NewProductionMux(store, runtime, tv, sv, allowedOriginsFromEnv(), browserOrigins, directChatAuthorizer)
	if err != nil {
		closeOnError()
		return nil, err
	}
	authServer, authEnabled, err := browserAuthServerFromEnvWithDB(context.Background(), sv, browserOrigins, database.Pool)
	if err != nil {
		closeOnError()
		return nil, fmt.Errorf("browser auth: %w", err)
	}
	if authEnabled {
		authServer.Connections = browser
		if database != nil {
			authServer.Consents = koseki.New(database.Pool)
		}
		authServer.RegisterRoutes(mux)
	}
	localControl, enabled, err := localControlServerFromEnv(runtime)
	if err != nil {
		closeOnError()
		return nil, fmt.Errorf("local control fixture: %w", err)
	}
	localListener, err := localControlListenerFromEnv(enabled)
	if err != nil {
		closeOnError()
		return nil, fmt.Errorf("local control transport: %w", err)
	}
	var localMux *http.ServeMux
	if enabled {
		localMux = http.NewServeMux()
		if err := localControl.RegisterRoutes(localMux); err != nil {
			closeOnError()
			return nil, fmt.Errorf("register local control fixture: %w", err)
		}
	}
	mux.HandleFunc("GET /health", handler.Health)
	return &application{
		publicMux:     mux,
		localMux:      localMux,
		localListener: localListener,
		store:         store,
		browser:       browser,
		database:      database,
	}, nil
}

// databaseFromEnv opens and migrates the control-plane Postgres database when
// SUMI_DB_URL is configured. An unset variable yields a nil pool so that
// components that do not yet require the 戸籍 (and unit tests) keep working.
func databaseFromEnv(ctx context.Context) (*db.Pool, error) {
	databaseURL := strings.TrimSpace(os.Getenv("SUMI_DB_URL"))
	if databaseURL == "" {
		return nil, nil
	}
	openCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	pool, err := db.Open(openCtx, databaseURL)
	if err != nil {
		return nil, err
	}
	migrateCtx, migrateCancel := context.WithTimeout(ctx, 30*time.Second)
	defer migrateCancel()
	if err := db.Migrate(migrateCtx, pool.Pool); err != nil {
		pool.Close()
		return nil, fmt.Errorf("apply migrations: %w", err)
	}
	log.Printf("sumi control-plane database ready (migrations applied)")
	return pool, nil
}

const (
	localControlSocketMode = 0o660
	localControlParentMode = 0o750
	localControlLockMode   = 0o600
	maxUnixSocketPathBytes = 100
	maxListenerLockBytes   = 4 * 1024
)

func localControlCrashFailpoint(name string) {
	if os.Getenv("SUMI_TEST_LOCAL_CONTROL_CRASH_FAILPOINT") != name {
		return
	}
	_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
	select {}
}

type localControlListenerTestHooks struct {
	beforeLockPublication  func()
	afterLockPublication   func()
	beforeListenerReturn   func()
	beforeSocketQuarantine func()
	afterSocketQuarantine  func(string)
}

type localControlListenerConfig struct {
	unixSocket         string
	socketGID          int
	personalityAgentID string
	loopbackListen     string
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
	personalityAgentID := os.Getenv("SUMI_LOCAL_CONTROL_PERSONALITY_AGENT_ID")
	if personalityAgentID == "" || strings.ContainsAny(personalityAgentID, "\r\n") {
		return nil, errors.New("SUMI_LOCAL_CONTROL_PERSONALITY_AGENT_ID is invalid for listener ownership")
	}
	return &localControlListenerConfig{
		unixSocket:         socketPath,
		socketGID:          int(gid),
		personalityAgentID: personalityAgentID,
	}, nil
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
	return listenTrustedUnixSocket(c.unixSocket, c.socketGID, c.personalityAgentID)
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

type listenerOwnershipLock struct {
	parentPath       string
	pinnedParentPath string
	parent           *os.File
	parentStat       syscall.Stat_t
	pinnedLockPath   string
	lock             *os.File
	lockStat         syscall.Stat_t
	socketName       string
	pinnedSocketPath string
	socketGID        int
	testHooks        *localControlListenerTestHooks
}

type ownedUnixListener struct {
	listener   *net.UnixListener
	ownership  *listenerOwnershipLock
	socketDev  uint64
	socketIno  uint64
	closeOnce  sync.Once
	closeError error
}

func (l *ownedUnixListener) Accept() (net.Conn, error) {
	return l.listener.Accept()
}

func (l *ownedUnixListener) Addr() net.Addr {
	return l.listener.Addr()
}

func (l *ownedUnixListener) Close() error {
	l.closeOnce.Do(func() {
		listenerErr := l.listener.Close()
		unlinkErr := l.ownership.unlinkOwnedSocket(l.socketDev, l.socketIno, true)
		releaseErr := l.ownership.release()
		l.closeError = errors.Join(listenerErr, unlinkErr, releaseErr)
	})
	return l.closeError
}

func listenTrustedUnixSocket(path string, gid int, personalityAgentID string) (*ownedUnixListener, error) {
	return listenTrustedUnixSocketWithHooks(path, gid, personalityAgentID, nil)
}

func listenTrustedUnixSocketWithHooks(
	path string,
	gid int,
	personalityAgentID string,
	hooks *localControlListenerTestHooks,
) (*ownedUnixListener, error) {
	if err := validateUnixSocketPath(path); err != nil {
		return nil, err
	}
	if personalityAgentID == "" || strings.ContainsAny(personalityAgentID, "\r\n") {
		return nil, errors.New("local control listener ownership requires one valid personality agent ID")
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

	ownership, err := acquireListenerOwnership(
		parent,
		parentInfo,
		path,
		gid,
		personalityAgentID,
		hooks,
	)
	if err != nil {
		return nil, err
	}
	releaseOwnership := true
	defer func() {
		if releaseOwnership {
			_ = ownership.release()
		}
	}()
	if err := ownership.validateConfiguredParentIdentity(); err != nil {
		return nil, err
	}
	if err := removeTrustedStaleSocket(
		int(ownership.parent.Fd()),
		ownership.pinnedParentPath,
		ownership.socketName,
		ownership.pinnedSocketPath,
		gid,
		hooks,
	); err != nil {
		return nil, err
	}
	if err := ownership.validateConfiguredParentIdentity(); err != nil {
		return nil, err
	}
	oldUmask := syscall.Umask(0o177)
	listener, err := net.ListenUnix(
		"unix",
		&net.UnixAddr{Name: ownership.pinnedSocketPath, Net: "unix"},
	)
	syscall.Umask(oldUmask)
	if err != nil {
		return nil, err
	}
	listener.SetUnlinkOnClose(false)
	localControlCrashFailpoint("socket-bind-before-metadata")
	socketInfo, err := os.Lstat(ownership.pinnedSocketPath)
	if err != nil {
		_ = listener.Close()
		return nil, fmt.Errorf("inspect newly bound local control socket: %w", err)
	}
	socketStat, ok := socketInfo.Sys().(*syscall.Stat_t)
	if !ok || socketInfo.Mode()&os.ModeSocket == 0 {
		_ = listener.Close()
		return nil, errors.New("newly bound local control socket metadata is unavailable")
	}
	owned := &ownedUnixListener{
		listener:  listener,
		ownership: ownership,
		socketDev: socketStat.Dev,
		socketIno: socketStat.Ino,
	}
	fail := func(cause error) (*ownedUnixListener, error) {
		listenerErr := listener.Close()
		unlinkErr := ownership.unlinkOwnedSocket(socketStat.Dev, socketStat.Ino, false)
		releaseErr := ownership.release()
		releaseOwnership = false
		cleanupErr := errors.Join(listenerErr, unlinkErr, releaseErr)
		if cleanupErr != nil {
			return nil, errors.Join(cause, fmt.Errorf("clean up failed local control listener: %w", cleanupErr))
		}
		return nil, cause
	}
	if err := syscall.Fchownat(
		int(ownership.parent.Fd()),
		ownership.socketName,
		os.Geteuid(),
		gid,
		0,
	); err != nil {
		return fail(fmt.Errorf("set local control socket owner: %w", err))
	}
	if err := syscall.Fchmodat(
		int(ownership.parent.Fd()),
		ownership.socketName,
		localControlSocketMode,
		0,
	); err != nil {
		return fail(fmt.Errorf("set local control socket mode: %w", err))
	}
	if err := validateSocketMetadata(ownership.pinnedSocketPath, gid); err != nil {
		return fail(err)
	}
	finalInfo, err := os.Lstat(ownership.pinnedSocketPath)
	if err != nil {
		return fail(fmt.Errorf("reinspect configured local control socket: %w", err))
	}
	finalStat, ok := finalInfo.Sys().(*syscall.Stat_t)
	if !ok || finalStat.Dev != socketStat.Dev || finalStat.Ino != socketStat.Ino {
		return fail(errors.New("local control socket changed during listener setup"))
	}
	if hooks != nil && hooks.beforeListenerReturn != nil {
		hooks.beforeListenerReturn()
	}
	if err := ownership.validateConfiguredParentIdentity(); err != nil {
		return fail(err)
	}
	releaseOwnership = false
	return owned, nil
}

func acquireListenerOwnership(
	parentPath string,
	parentInfo os.FileInfo,
	socketPath string,
	gid int,
	personalityAgentID string,
	hooks *localControlListenerTestHooks,
) (*listenerOwnershipLock, error) {
	binding := []byte(fmt.Sprintf(
		"sumi-local-control-listener-v1\nsocket=%s\npersonality_agent_id=%s\n",
		socketPath,
		personalityAgentID,
	))
	if len(binding) > maxListenerLockBytes {
		return nil, errors.New("local control listener ownership binding is too large")
	}
	parentFD, err := openConfiguredParentNoSymlinks(parentPath)
	if err != nil {
		return nil, fmt.Errorf("pin no-symlink local control socket parent: %w", err)
	}
	parent := os.NewFile(uintptr(parentFD), parentPath)
	failParent := func(cause error) (*listenerOwnershipLock, error) {
		_ = parent.Close()
		return nil, cause
	}
	var pinnedParent syscall.Stat_t
	if err := syscall.Fstat(parentFD, &pinnedParent); err != nil {
		return failParent(fmt.Errorf("inspect pinned local control socket parent: %w", err))
	}
	pathParent, ok := parentInfo.Sys().(*syscall.Stat_t)
	if !ok ||
		pinnedParent.Dev != pathParent.Dev ||
		pinnedParent.Ino != pathParent.Ino ||
		pinnedParent.Nlink != pathParent.Nlink ||
		pinnedParent.Nlink == 0 ||
		int(pinnedParent.Uid) != os.Geteuid() ||
		int(pinnedParent.Gid) != gid ||
		os.FileMode(pinnedParent.Mode).Perm() != localControlParentMode ||
		pinnedParent.Mode&syscall.S_IFMT != syscall.S_IFDIR {
		return failParent(errors.New("pinned local control socket parent is not trusted"))
	}
	pinnedParentPath := filepath.Join(
		"/proc/self/fd",
		strconv.Itoa(parentFD),
	)
	pinnedParentInfo, err := os.Stat(pinnedParentPath)
	if err != nil {
		return failParent(fmt.Errorf("resolve pinned local control socket parent handle: %w", err))
	}
	pinnedParentPathStat, ok := pinnedParentInfo.Sys().(*syscall.Stat_t)
	if !ok ||
		!pinnedParentInfo.IsDir() ||
		pinnedParentPathStat.Dev != pinnedParent.Dev ||
		pinnedParentPathStat.Ino != pinnedParent.Ino {
		return failParent(errors.New("pinned local control socket parent handle is not stable"))
	}

	socketName := filepath.Base(socketPath)
	pinnedSocketPath := filepath.Join(pinnedParentPath, socketName)
	if len(pinnedSocketPath) > maxUnixSocketPathBytes {
		return failParent(fmt.Errorf(
			"pinned local control Unix socket path exceeds %d bytes",
			maxUnixSocketPathBytes,
		))
	}
	lockName := filepath.Base(socketPath) + ".owner.lock"
	lockPath := filepath.Join(parentPath, lockName)
	pinnedLockPath := filepath.Join(pinnedParentPath, lockName)
	if err := syscall.Flock(parentFD, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return failParent(errors.New("local control listener bootstrap lock is already held"))
		}
		return failParent(fmt.Errorf("acquire local control listener bootstrap lock: %w", err))
	}
	bootstrapHeld := true
	releaseBootstrap := func() error {
		if !bootstrapHeld {
			return nil
		}
		bootstrapHeld = false
		return syscall.Flock(parentFD, syscall.LOCK_UN)
	}
	failBootstrap := func(cause error) (*listenerOwnershipLock, error) {
		unlockErr := releaseBootstrap()
		if unlockErr != nil {
			cause = errors.Join(cause, fmt.Errorf("release local control listener bootstrap lock: %w", unlockErr))
		}
		return failParent(cause)
	}
	if err := validateConfiguredParentIdentity(
		parentPath,
		pinnedParent,
		gid,
	); err != nil {
		return failBootstrap(err)
	}
	if err := ensurePublishedListenerLock(
		parentFD,
		pinnedParentPath,
		lockName,
		pinnedLockPath,
		binding,
		gid,
		hooks,
	); err != nil {
		return failBootstrap(err)
	}

	flags := syscall.O_RDWR | syscall.O_CLOEXEC | syscall.O_NOFOLLOW
	lockFD, err := syscall.Openat(parentFD, lockName, flags, 0)
	if err != nil {
		return failBootstrap(fmt.Errorf("open local control listener ownership lock: %w", err))
	}
	lock := os.NewFile(uintptr(lockFD), lockPath)
	failLock := func(cause error) (*listenerOwnershipLock, error) {
		_ = lock.Close()
		return failBootstrap(cause)
	}
	lockStat, err := validatePinnedLock(lock, pinnedLockPath, gid)
	if err != nil {
		return failLock(err)
	}
	if err := syscall.Flock(lockFD, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return failLock(errors.New("local control listener ownership lock is already held"))
		}
		return failLock(fmt.Errorf("acquire local control listener ownership lock: %w", err))
	}
	failHeldLock := func(cause error) (*listenerOwnershipLock, error) {
		_ = syscall.Flock(lockFD, syscall.LOCK_UN)
		return failLock(cause)
	}
	if err := releaseBootstrap(); err != nil {
		return failHeldLock(fmt.Errorf("release local control listener bootstrap lock: %w", err))
	}
	revalidatedLock, err := validatePinnedLock(lock, pinnedLockPath, gid)
	if err != nil {
		return failHeldLock(err)
	}
	if lockStat.Dev != revalidatedLock.Dev || lockStat.Ino != revalidatedLock.Ino {
		return failHeldLock(errors.New("local control listener ownership lock changed during acquisition"))
	}
	if _, err := lock.Seek(0, io.SeekStart); err != nil {
		return failHeldLock(fmt.Errorf("seek local control listener ownership lock: %w", err))
	}
	existing, err := io.ReadAll(io.LimitReader(lock, maxListenerLockBytes+1))
	if err != nil {
		return failHeldLock(fmt.Errorf("read local control listener ownership lock: %w", err))
	}
	if len(existing) > maxListenerLockBytes {
		return failHeldLock(errors.New("local control listener ownership lock content is oversized"))
	}
	if !bytes.Equal(existing, binding) {
		return failHeldLock(errors.New("local control listener ownership lock is bound to a different socket or personality agent"))
	}
	if err := validateConfiguredParentIdentity(
		parentPath,
		pinnedParent,
		gid,
	); err != nil {
		return failHeldLock(err)
	}

	return &listenerOwnershipLock{
		parentPath:       parentPath,
		pinnedParentPath: pinnedParentPath,
		parent:           parent,
		parentStat:       pinnedParent,
		pinnedLockPath:   pinnedLockPath,
		lock:             lock,
		lockStat:         revalidatedLock,
		socketName:       socketName,
		pinnedSocketPath: pinnedSocketPath,
		socketGID:        gid,
		testHooks:        hooks,
	}, nil
}

func ensurePublishedListenerLock(
	parentFD int,
	parentPath string,
	lockName string,
	lockPath string,
	binding []byte,
	gid int,
	hooks *localControlListenerTestHooks,
) error {
	if err := cleanupListenerInitializationTemps(
		parentFD,
		parentPath,
		lockName+".init-",
		binding,
		gid,
	); err != nil {
		return err
	}
	_, err := os.Lstat(lockPath)
	switch {
	case err == nil:
		valid, validationErr := publishedListenerLockIsComplete(
			parentFD,
			lockName,
			lockPath,
			binding,
			gid,
		)
		if validationErr != nil {
			return validationErr
		}
		if valid {
			return nil
		}
		recovered, recoveryErr := recoverListenerInitializationResidue(
			parentFD,
			lockName,
			lockPath,
			binding,
			gid,
		)
		if recoveryErr != nil {
			return recoveryErr
		}
		if !recovered {
			return errors.New("existing local control listener ownership lock is neither complete nor authenticated initialization residue")
		}
	case errors.Is(err, os.ErrNotExist):
	default:
		return fmt.Errorf("inspect local control listener ownership lock publication: %w", err)
	}
	return publishInitializedListenerLock(
		parentFD,
		parentPath,
		lockName,
		binding,
		gid,
		hooks,
	)
}

func publishedListenerLockIsComplete(
	parentFD int,
	lockName string,
	lockPath string,
	binding []byte,
	gid int,
) (bool, error) {
	lockFD, err := syscall.Openat(
		parentFD,
		lockName,
		syscall.O_RDWR|syscall.O_CLOEXEC|syscall.O_NOFOLLOW,
		0,
	)
	if err != nil {
		return false, fmt.Errorf("open published local control listener ownership lock: %w", err)
	}
	lock := os.NewFile(uintptr(lockFD), lockPath)
	defer lock.Close()
	if _, err := validatePinnedLock(lock, lockPath, gid); err != nil {
		return false, nil
	}
	content, err := io.ReadAll(io.LimitReader(lock, maxListenerLockBytes+1))
	if err != nil {
		return false, fmt.Errorf("read published local control listener ownership lock: %w", err)
	}
	return bytes.Equal(content, binding), nil
}

func publishInitializedListenerLock(
	parentFD int,
	parentPath string,
	lockName string,
	binding []byte,
	gid int,
	hooks *localControlListenerTestHooks,
) error {
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return fmt.Errorf("generate local control listener lock initializer name: %w", err)
	}
	tempName := lockName + ".init-" + hex.EncodeToString(random)
	tempPath := filepath.Join(parentPath, tempName)
	flags := syscall.O_RDWR | syscall.O_CREAT | syscall.O_EXCL | syscall.O_CLOEXEC | syscall.O_NOFOLLOW
	tempFD, err := syscall.Openat(parentFD, tempName, flags, localControlLockMode)
	if err != nil {
		return fmt.Errorf("create private local control listener lock initializer: %w", err)
	}
	temp := os.NewFile(uintptr(tempFD), tempPath)
	published := false
	defer func() {
		_ = temp.Close()
		if !published {
			_ = syscall.Unlinkat(parentFD, tempName)
		}
	}()
	localControlCrashFailpoint("lock-create-before-metadata")
	if err := syscall.Fchown(tempFD, os.Geteuid(), gid); err != nil {
		return fmt.Errorf("set local control listener lock initializer owner: %w", err)
	}
	if err := syscall.Fchmod(tempFD, localControlLockMode); err != nil {
		return fmt.Errorf("set local control listener lock initializer mode: %w", err)
	}
	if err := validatePrivateListenerInitializer(temp, tempPath, binding, gid, true); err != nil {
		return err
	}
	localControlCrashFailpoint("lock-metadata-before-binding")
	if _, err := temp.WriteAt(binding, 0); err != nil {
		return fmt.Errorf("write local control listener lock initializer: %w", err)
	}
	if err := temp.Truncate(int64(len(binding))); err != nil {
		return fmt.Errorf("truncate local control listener lock initializer: %w", err)
	}
	localControlCrashFailpoint("lock-binding-before-fsync")
	if err := temp.Sync(); err != nil {
		return fmt.Errorf("sync local control listener lock initializer: %w", err)
	}
	if err := validatePrivateListenerInitializer(temp, tempPath, binding, gid, false); err != nil {
		return err
	}
	if hooks != nil && hooks.beforeLockPublication != nil {
		hooks.beforeLockPublication()
	}
	if err := unix.Renameat2(
		parentFD,
		tempName,
		parentFD,
		lockName,
		unix.RENAME_NOREPLACE,
	); err != nil {
		if errors.Is(err, syscall.EEXIST) {
			return errors.New("local control listener ownership lock appeared during atomic publication")
		}
		return fmt.Errorf("atomically publish initialized local control listener ownership lock: %w", err)
	}
	published = true
	if err := syscall.Fsync(parentFD); err != nil {
		return fmt.Errorf("sync local control listener ownership lock directory: %w", err)
	}
	if hooks != nil && hooks.afterLockPublication != nil {
		hooks.afterLockPublication()
	}
	return nil
}

func cleanupListenerInitializationTemps(
	parentFD int,
	parentPath string,
	prefix string,
	binding []byte,
	gid int,
) error {
	entries, err := os.ReadDir(parentPath)
	if err != nil {
		return fmt.Errorf("list local control listener initialization residue: %w", err)
	}
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), prefix) {
			continue
		}
		path := filepath.Join(parentPath, entry.Name())
		recovered, err := recoverListenerInitializationResidue(
			parentFD,
			entry.Name(),
			path,
			binding,
			gid,
		)
		if err != nil {
			return err
		}
		if !recovered {
			return errors.New("untrusted local control listener initialization residue blocks startup")
		}
	}
	return nil
}

func recoverListenerInitializationResidue(
	parentFD int,
	name string,
	path string,
	binding []byte,
	gid int,
) (bool, error) {
	fd, err := syscall.Openat(
		parentFD,
		name,
		syscall.O_RDWR|syscall.O_CLOEXEC|syscall.O_NOFOLLOW,
		0,
	)
	if err != nil {
		return false, nil
	}
	file := os.NewFile(uintptr(fd), path)
	defer file.Close()
	if err := validatePrivateListenerInitializer(file, path, binding, gid, true); err != nil {
		return false, nil
	}
	if err := syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return false, errors.New("local control listener initialization residue is still live")
		}
		return false, fmt.Errorf("lock local control listener initialization residue: %w", err)
	}
	defer syscall.Flock(fd, syscall.LOCK_UN)
	if err := validatePrivateListenerInitializer(file, path, binding, gid, true); err != nil {
		return false, nil
	}
	if err := syscall.Unlinkat(parentFD, name); err != nil {
		return false, fmt.Errorf("remove authenticated local control listener initialization residue: %w", err)
	}
	if err := syscall.Fsync(parentFD); err != nil {
		return false, fmt.Errorf("sync removal of local control listener initialization residue: %w", err)
	}
	return true, nil
}

func validatePrivateListenerInitializer(
	file *os.File,
	path string,
	binding []byte,
	gid int,
	allowPrefix bool,
) error {
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("inspect local control listener lock initializer: %w", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok ||
		!info.Mode().IsRegular() ||
		stat.Nlink != 1 ||
		int(stat.Uid) != os.Geteuid() ||
		(int(stat.Gid) != gid && int(stat.Gid) != os.Getegid()) ||
		info.Mode().Perm()&^os.FileMode(localControlLockMode) != 0 ||
		info.Size() > int64(len(binding)) {
		return errors.New("local control listener lock initializer metadata is not authenticated")
	}
	pathInfo, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect local control listener lock initializer path: %w", err)
	}
	pathStat, ok := pathInfo.Sys().(*syscall.Stat_t)
	if !ok ||
		pathInfo.Mode()&os.ModeSymlink != 0 ||
		pathStat.Dev != stat.Dev ||
		pathStat.Ino != stat.Ino {
		return errors.New("local control listener lock initializer path is not pinned")
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek local control listener lock initializer: %w", err)
	}
	content, err := io.ReadAll(io.LimitReader(file, int64(len(binding))+1))
	if err != nil {
		return fmt.Errorf("read local control listener lock initializer: %w", err)
	}
	if allowPrefix {
		if !bytes.HasPrefix(binding, content) {
			return errors.New("local control listener lock initializer content is not authenticated")
		}
	} else if !bytes.Equal(content, binding) {
		return errors.New("local control listener lock initializer binding is incomplete")
	}
	return nil
}

func validateConfiguredParentIdentity(
	parentPath string,
	expected syscall.Stat_t,
	gid int,
) error {
	parentFD, err := openConfiguredParentNoSymlinks(parentPath)
	if err != nil {
		return fmt.Errorf("reopen configured local control socket parent without symlinks: %w", err)
	}
	defer syscall.Close(parentFD)
	var stat syscall.Stat_t
	if err := syscall.Fstat(parentFD, &stat); err != nil {
		return fmt.Errorf("inspect reopened configured local control socket parent: %w", err)
	}
	if stat.Mode&syscall.S_IFMT != syscall.S_IFDIR ||
		stat.Dev != expected.Dev ||
		stat.Ino != expected.Ino ||
		stat.Nlink != expected.Nlink ||
		stat.Nlink == 0 ||
		int(stat.Uid) != os.Geteuid() ||
		int(stat.Gid) != gid ||
		os.FileMode(stat.Mode).Perm() != localControlParentMode {
		return errors.New("configured local control socket parent no longer names the pinned trusted directory")
	}
	return nil
}

func openConfiguredParentNoSymlinks(parentPath string) (int, error) {
	return unix.Openat2(
		unix.AT_FDCWD,
		parentPath,
		&unix.OpenHow{
			Flags: uint64(
				unix.O_RDONLY |
					unix.O_DIRECTORY |
					unix.O_CLOEXEC |
					unix.O_NOFOLLOW,
			),
			Resolve: unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_MAGICLINKS,
		},
	)
}

func validatePinnedLock(lock *os.File, lockPath string, gid int) (syscall.Stat_t, error) {
	info, err := lock.Stat()
	if err != nil {
		return syscall.Stat_t{}, fmt.Errorf("inspect local control listener ownership lock: %w", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok ||
		!info.Mode().IsRegular() ||
		info.Mode().Perm() != localControlLockMode ||
		stat.Nlink != 1 ||
		int(stat.Uid) != os.Geteuid() ||
		int(stat.Gid) != gid {
		return syscall.Stat_t{}, errors.New("local control listener ownership lock metadata is not trusted")
	}
	pathInfo, err := os.Lstat(lockPath)
	if err != nil {
		return syscall.Stat_t{}, fmt.Errorf("inspect local control listener ownership lock path: %w", err)
	}
	pathStat, ok := pathInfo.Sys().(*syscall.Stat_t)
	if !ok ||
		pathInfo.Mode()&os.ModeSymlink != 0 ||
		pathStat.Dev != stat.Dev ||
		pathStat.Ino != stat.Ino {
		return syscall.Stat_t{}, errors.New("local control listener ownership lock path is not pinned")
	}
	return *stat, nil
}

func (o *listenerOwnershipLock) validatePinnedState() error {
	var parentStat syscall.Stat_t
	if err := syscall.Fstat(int(o.parent.Fd()), &parentStat); err != nil {
		return fmt.Errorf("reinspect pinned local control socket parent: %w", err)
	}
	if parentStat.Dev != o.parentStat.Dev ||
		parentStat.Ino != o.parentStat.Ino ||
		parentStat.Nlink != o.parentStat.Nlink ||
		parentStat.Nlink == 0 ||
		int(parentStat.Uid) != os.Geteuid() ||
		int(parentStat.Gid) != o.socketGID ||
		os.FileMode(parentStat.Mode).Perm() != localControlParentMode ||
		parentStat.Mode&syscall.S_IFMT != syscall.S_IFDIR {
		return errors.New("pinned local control socket parent changed")
	}
	lockStat, err := validatePinnedLock(o.lock, o.pinnedLockPath, o.socketGID)
	if err != nil {
		return err
	}
	if lockStat.Dev != o.lockStat.Dev || lockStat.Ino != o.lockStat.Ino {
		return errors.New("local control listener ownership lock inode changed")
	}
	return nil
}

func (o *listenerOwnershipLock) validateConfiguredParentIdentity() error {
	return validateConfiguredParentIdentity(
		o.parentPath,
		o.parentStat,
		o.socketGID,
	)
}

func (o *listenerOwnershipLock) unlinkOwnedSocket(dev, ino uint64, requireTrusted bool) error {
	if err := o.validatePinnedState(); err != nil {
		return err
	}
	info, err := os.Lstat(o.pinnedSocketPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect owned local control socket during close: %w", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Dev != dev || stat.Ino != ino || info.Mode()&os.ModeSocket == 0 {
		return errors.New("refuse to unlink a local control socket path no longer owned by this listener")
	}
	if requireTrusted {
		if _, err := trustedSocketStat(info, o.socketGID); err != nil {
			return fmt.Errorf("refuse to unlink changed local control socket: %w", err)
		}
	}
	var validateMoved func(os.FileInfo) error
	if requireTrusted {
		validateMoved = func(info os.FileInfo) error {
			_, err := trustedSocketStat(info, o.socketGID)
			return err
		}
	}
	return quarantineAndRemoveSocketCandidate(
		int(o.parent.Fd()),
		o.pinnedParentPath,
		o.socketName,
		info,
		validateMoved,
		o.testHooks,
	)
}

func (o *listenerOwnershipLock) release() error {
	unlockErr := syscall.Flock(int(o.lock.Fd()), syscall.LOCK_UN)
	lockCloseErr := o.lock.Close()
	parentCloseErr := o.parent.Close()
	return errors.Join(unlockErr, lockCloseErr, parentCloseErr)
}

func removeTrustedStaleSocket(
	parentFD int,
	pinnedParentPath string,
	socketName string,
	pinnedSocketPath string,
	gid int,
	hooks *localControlListenerTestHooks,
) error {
	info, err := os.Lstat(pinnedSocketPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect existing local control socket: %w", err)
	}
	first, err := trustedOrInitializationSocketStat(info, gid)
	if err != nil {
		return fmt.Errorf("refuse existing local control socket target: %w", err)
	}
	probe, probeErr := net.DialTimeout("unix", pinnedSocketPath, 100*time.Millisecond)
	if probeErr == nil {
		_ = probe.Close()
		return errors.New("refuse to replace a live local control socket")
	}
	if !errors.Is(probeErr, syscall.ECONNREFUSED) {
		return fmt.Errorf("cannot prove existing local control socket is stale: %w", probeErr)
	}
	current, err := os.Lstat(pinnedSocketPath)
	if err != nil {
		return fmt.Errorf("reinspect existing local control socket: %w", err)
	}
	second, err := trustedOrInitializationSocketStat(current, gid)
	if err != nil {
		return fmt.Errorf("refuse replaced local control socket target: %w", err)
	}
	if first.Dev != second.Dev || first.Ino != second.Ino {
		return errors.New("local control socket changed during stale cleanup")
	}
	return quarantineAndRemoveSocketCandidate(
		parentFD,
		pinnedParentPath,
		socketName,
		current,
		func(info os.FileInfo) error {
			_, err := trustedOrInitializationSocketStat(info, gid)
			return err
		},
		hooks,
	)
}

func quarantineAndRemoveSocketCandidate(
	parentFD int,
	pinnedParentPath string,
	socketName string,
	expectedInfo os.FileInfo,
	validateMoved func(os.FileInfo) error,
	hooks *localControlListenerTestHooks,
) error {
	expectedStat, ok := expectedInfo.Sys().(*syscall.Stat_t)
	if !ok {
		return errors.New("local control socket candidate identity is unavailable")
	}
	candidateFD, err := unix.Openat(
		parentFD,
		socketName,
		unix.O_PATH|unix.O_NOFOLLOW|unix.O_CLOEXEC,
		0,
	)
	if err != nil {
		return fmt.Errorf("pin local control socket candidate before quarantine: %w", err)
	}
	defer unix.Close(candidateFD)
	var pinnedCandidate syscall.Stat_t
	if err := syscall.Fstat(candidateFD, &pinnedCandidate); err != nil {
		return fmt.Errorf("inspect pinned local control socket candidate: %w", err)
	}
	if !sameSocketStatIdentity(*expectedStat, pinnedCandidate) {
		return errors.New("local control socket candidate changed before it could be pinned")
	}
	if hooks != nil && hooks.beforeSocketQuarantine != nil {
		hooks.beforeSocketQuarantine()
	}
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return fmt.Errorf("generate local control socket quarantine name: %w", err)
	}
	quarantineName := socketName + ".quarantine-" + hex.EncodeToString(random)
	quarantinePath := filepath.Join(pinnedParentPath, quarantineName)
	if err := unix.Renameat2(
		parentFD,
		socketName,
		parentFD,
		quarantineName,
		unix.RENAME_NOREPLACE,
	); err != nil {
		return fmt.Errorf("atomically quarantine local control socket candidate: %w", err)
	}
	if hooks != nil && hooks.afterSocketQuarantine != nil {
		hooks.afterSocketQuarantine(quarantineName)
	}

	movedInfo, err := os.Lstat(quarantinePath)
	if err != nil {
		return fmt.Errorf("inspect quarantined local control socket candidate: %w", err)
	}
	movedStat, ok := movedInfo.Sys().(*syscall.Stat_t)
	matches := ok &&
		movedInfo.Mode()&os.ModeSocket != 0 &&
		movedInfo.Mode()&os.ModeSymlink == 0 &&
		movedStat.Dev == pinnedCandidate.Dev &&
		movedStat.Ino == pinnedCandidate.Ino
	if matches && validateMoved != nil {
		matches = validateMoved(movedInfo) == nil
	}
	if !matches {
		cause := errors.New("local control socket candidate changed before atomic quarantine removal")
		restoreErr := restoreQuarantinedSocketCandidate(
			parentFD,
			pinnedParentPath,
			quarantineName,
			socketName,
			movedInfo,
		)
		if restoreErr != nil {
			return errors.Join(cause, restoreErr)
		}
		return fmt.Errorf("%w; replacement restored without overwrite", cause)
	}
	if err := revalidateQuarantinedSocketIdentity(quarantinePath, movedInfo); err != nil {
		return err
	}
	if err := syscall.Unlinkat(parentFD, quarantineName); err != nil {
		return fmt.Errorf("remove exact quarantined local control socket candidate: %w", err)
	}
	if err := syscall.Fsync(parentFD); err != nil {
		return fmt.Errorf("sync quarantined local control socket removal: %w", err)
	}
	return nil
}

func sameSocketStatIdentity(left syscall.Stat_t, right syscall.Stat_t) bool {
	return left.Mode&syscall.S_IFMT == syscall.S_IFSOCK &&
		right.Mode&syscall.S_IFMT == syscall.S_IFSOCK &&
		left.Dev == right.Dev &&
		left.Ino == right.Ino &&
		left.Nlink == right.Nlink &&
		left.Uid == right.Uid &&
		left.Gid == right.Gid &&
		left.Mode == right.Mode &&
		left.Ctim.Sec == right.Ctim.Sec &&
		left.Ctim.Nsec == right.Ctim.Nsec
}

func restoreQuarantinedSocketCandidate(
	parentFD int,
	pinnedParentPath string,
	quarantineName string,
	socketName string,
	movedInfo os.FileInfo,
) error {
	quarantinePath := filepath.Join(pinnedParentPath, quarantineName)
	if err := revalidateQuarantinedSocketIdentity(quarantinePath, movedInfo); err != nil {
		return fmt.Errorf("preserve changed local control socket quarantine: %w", err)
	}
	if err := unix.Renameat2(
		parentFD,
		quarantineName,
		parentFD,
		socketName,
		unix.RENAME_NOREPLACE,
	); err != nil {
		if errors.Is(err, syscall.EEXIST) {
			syncErr := syscall.Fsync(parentFD)
			conflictErr := fmt.Errorf(
				"local control socket restore destination is occupied; original and quarantined replacements were both preserved at %q",
				quarantineName,
			)
			if syncErr != nil {
				return errors.Join(
					conflictErr,
					fmt.Errorf("sync preserved local control socket quarantine conflict: %w", syncErr),
				)
			}
			return conflictErr
		}
		return fmt.Errorf("restore quarantined local control socket without overwrite: %w", err)
	}
	if err := syscall.Fsync(parentFD); err != nil {
		return fmt.Errorf("sync restored local control socket candidate: %w", err)
	}
	return nil
}

func revalidateQuarantinedSocketIdentity(path string, expected os.FileInfo) error {
	current, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("reinspect quarantined local control socket candidate: %w", err)
	}
	currentStat, currentOK := current.Sys().(*syscall.Stat_t)
	expectedStat, expectedOK := expected.Sys().(*syscall.Stat_t)
	if !currentOK ||
		!expectedOK ||
		current.Mode()&os.ModeType != expected.Mode()&os.ModeType ||
		currentStat.Dev != expectedStat.Dev ||
		currentStat.Ino != expectedStat.Ino {
		return errors.New("quarantined local control socket candidate changed")
	}
	return nil
}

func trustedOrInitializationSocketStat(info os.FileInfo, gid int) (*syscall.Stat_t, error) {
	stat, trustedErr := trustedSocketStat(info, gid)
	if trustedErr == nil {
		return stat, nil
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok ||
		info.Mode()&os.ModeSocket == 0 ||
		info.Mode()&os.ModeSymlink != 0 ||
		info.Mode().Perm() != 0o600 ||
		stat.Nlink != 1 ||
		int(stat.Uid) != os.Geteuid() ||
		(int(stat.Gid) != gid && int(stat.Gid) != os.Getegid()) {
		return nil, trustedErr
	}
	return stat, nil
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

func browserSessionConfigFromEnv(
	revocations agentevents.BrowserSessionRevocationStore,
) (*agentevents.HMACUserSessionVerifier, []string, error) {
	secretConfigured := strings.TrimSpace(os.Getenv("SUMI_BROWSER_SESSION_SECRET")) != ""
	audienceConfigured := strings.TrimSpace(os.Getenv("SUMI_BROWSER_SESSION_AUDIENCE")) != ""
	originsConfigured := strings.TrimSpace(os.Getenv("SUMI_BROWSER_WS_ALLOWED_ORIGINS")) != ""
	authConfigured := browserAuthConfiguredFromEnv()
	if !secretConfigured && !audienceConfigured && !originsConfigured && !authConfigured {
		return nil, nil, nil
	}
	if !secretConfigured || !audienceConfigured || !originsConfigured {
		return nil, nil, errors.New("SUMI_BROWSER_SESSION_SECRET, SUMI_BROWSER_SESSION_AUDIENCE, and SUMI_BROWSER_WS_ALLOWED_ORIGINS must be configured together")
	}
	origins := browserAllowedOriginsFromEnv()
	if len(origins) == 0 {
		return nil, nil, errors.New("SUMI_BROWSER_WS_ALLOWED_ORIGINS must contain at least one exact origin")
	}
	sessions, err := browserSessionVerifierFromEnv(revocations)
	if err != nil {
		return nil, nil, err
	}
	return sessions, origins, nil
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

const (
	localControlPreviousSigningSecretsEnv = "SUMI_LOCAL_CONTROL_PREVIOUS_SIGNING_SECRETS"
	maxLocalControlPreviousSigningSecrets = 2
)

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

// localControlPreviousSigningSecretsFromEnv parses a bounded comma-separated
// list of base64 secrets used only to verify and re-sign durable local-control
// state during a coordinated rotation. They never verify agent bearer tokens.
func localControlPreviousSigningSecretsFromEnv() ([][]byte, error) {
	raw := os.Getenv(localControlPreviousSigningSecretsEnv)
	if raw == "" {
		return nil, nil
	}
	encoded := strings.Split(raw, ",")
	if len(encoded) > maxLocalControlPreviousSigningSecrets {
		return nil, fmt.Errorf(
			"%s supports at most %d secrets",
			localControlPreviousSigningSecretsEnv,
			maxLocalControlPreviousSigningSecrets,
		)
	}
	secrets := make([][]byte, 0, len(encoded))
	for index, value := range encoded {
		value = strings.TrimSpace(value)
		if value == "" {
			return nil, fmt.Errorf(
				"%s entry %d is empty",
				localControlPreviousSigningSecretsEnv,
				index+1,
			)
		}
		secret, err := base64.StdEncoding.DecodeString(value)
		if err != nil {
			return nil, fmt.Errorf(
				"decode %s entry %d: %w",
				localControlPreviousSigningSecretsEnv,
				index+1,
				err,
			)
		}
		secrets = append(secrets, secret)
	}
	return secrets, nil
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
	previousSigningSecrets, err := localControlPreviousSigningSecretsFromEnv()
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
	control, err := agentevents.NewLocalControlServerWithPreviousSigningSecrets(
		runtime,
		signingSecret,
		previousSigningSecrets,
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
// verifier. Browser sessions are HttpOnly cookies scoped to users and their
// server-bound personality agents; agent bearer tokens never enter this route.
func browserSessionVerifierFromEnv(
	revocations agentevents.BrowserSessionRevocationStore,
) (*agentevents.HMACUserSessionVerifier, error) {
	b64 := os.Getenv("SUMI_BROWSER_SESSION_SECRET")
	if b64 == "" {
		return nil, errBrowserSessionSecretMissing
	}
	secret, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return nil, err
	}
	audience := os.Getenv("SUMI_BROWSER_SESSION_AUDIENCE")
	return agentevents.NewHMACUserSessionVerifier(secret, audience, revocations)
}
