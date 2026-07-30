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
	"github.com/sumi-studio/sumi/apps/api/internal/handler"
	"golang.org/x/sys/unix"
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
	beforeLockPublication func()
	afterLockPublication  func()
	beforeListenerReturn  func()
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
	parent           *os.File
	parentStat       syscall.Stat_t
	pinnedLockPath   string
	lock             *os.File
	lockStat         syscall.Stat_t
	socketName       string
	pinnedSocketPath string
	socketGID        int
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
		ownership.socketName,
		ownership.pinnedSocketPath,
		gid,
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
	parentFD, err := syscall.Open(
		parentPath,
		syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW,
		0,
	)
	if err != nil {
		return nil, fmt.Errorf("pin local control socket parent: %w", err)
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
		parent:           parent,
		parentStat:       pinnedParent,
		pinnedLockPath:   pinnedLockPath,
		lock:             lock,
		lockStat:         revalidatedLock,
		socketName:       socketName,
		pinnedSocketPath: pinnedSocketPath,
		socketGID:        gid,
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
	info, err := os.Lstat(parentPath)
	if err != nil {
		return fmt.Errorf("reinspect configured local control socket parent: %w", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok ||
		!info.IsDir() ||
		info.Mode()&os.ModeSymlink != 0 ||
		stat.Dev != expected.Dev ||
		stat.Ino != expected.Ino ||
		stat.Nlink != expected.Nlink ||
		stat.Nlink == 0 ||
		int(stat.Uid) != os.Geteuid() ||
		int(stat.Gid) != gid ||
		info.Mode().Perm() != localControlParentMode {
		return errors.New("configured local control socket parent no longer names the pinned trusted directory")
	}
	return nil
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
	if err := syscall.Unlinkat(int(o.parent.Fd()), o.socketName); err != nil {
		return fmt.Errorf("unlink owned local control socket: %w", err)
	}
	return nil
}

func (o *listenerOwnershipLock) release() error {
	unlockErr := syscall.Flock(int(o.lock.Fd()), syscall.LOCK_UN)
	lockCloseErr := o.lock.Close()
	parentCloseErr := o.parent.Close()
	return errors.Join(unlockErr, lockCloseErr, parentCloseErr)
}

func removeTrustedStaleSocket(
	parentFD int,
	socketName string,
	pinnedSocketPath string,
	gid int,
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
	if err := syscall.Unlinkat(parentFD, socketName); err != nil {
		return fmt.Errorf("remove trusted stale local control socket: %w", err)
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
