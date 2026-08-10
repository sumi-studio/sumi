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

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/handler"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
	"github.com/sumi-studio/sumi/apps/api/internal/messaging"
	"github.com/sumi-studio/sumi/apps/api/internal/runtimeprovision"
	"github.com/sumi-studio/sumi/apps/api/internal/spawn"
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
	if app.spawnManager != nil {
		reaperCtx, cancelReaper := context.WithCancel(ctx)
		defer cancelReaper()
		go runIdleReaper(reaperCtx, app.spawnManager)
	}
	// Temporary statuses lapse back to what the participant had said before.
	// Readers resolve that themselves, so this loop only makes the change
	// visible on screens that are already open.
	if app.messaging != nil {
		expiryCtx, cancelExpiry := context.WithCancel(ctx)
		defer cancelExpiry()
		go app.messaging.RunStatusExpiry(expiryCtx, messaging.DefaultStatusExpiryInterval)

		sweepCtx, cancelSweep := context.WithCancel(ctx)
		defer cancelSweep()
		go app.messaging.RunAttachmentSweeper(
			sweepCtx, messaging.AttachmentOrphanGrace, messaging.AttachmentSweepInterval)
	}
	if app.localMux == nil {
		return serveHTTPServers(ctx, serverAndListener{server: publicServer, listener: publicListener})
	}

	localServer := newLocalControlHTTPServer(app.localListener.handler(app.localMux))
	log.Printf("sumi local control listening on %s", app.localListener.description())
	return serveHTTPServers(
		ctx,
		serverAndListener{server: publicServer, listener: publicListener},
		serverAndListener{server: localServer, listener: localListener},
	)
}

// The Agent upload client allows 120 seconds for a bounded 20 MiB multipart
// transfer. The server adds five seconds for request dispatch and response
// delivery instead of invalidating that client contract after five seconds.
const localControlRequestTimeout = 125 * time.Second

func newLocalControlHTTPServer(handler http.Handler) *http.Server {
	return &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       localControlRequestTimeout,
		WriteTimeout:      localControlRequestTimeout,
		IdleTimeout:       15 * time.Second,
		MaxHeaderBytes:    16 * 1024,
	}
}

// runIdleReaper periodically stops cold-mode agents that have been idle longer
// than the configured timeout. Warm-mode agents are never stopped. It exits
// when ctx is done.
func runIdleReaper(ctx context.Context, mgr *spawn.Manager) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if stopped := mgr.StopIdleCold(); len(stopped) > 0 {
				log.Printf("spawn: stopped idle cold agents: %v", stopped)
			}
		}
	}
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
	spawnManager  *spawn.Manager
	localRuntimes *agentevents.LocalControlListenerRegistry
	messaging     *messaging.Server
	closeOnce     sync.Once
	closeErr      error
}

type browserSessionConnectionClosers []agentevents.BrowserSessionConnectionCloser

func (closers browserSessionConnectionClosers) CloseBrowserSession(sessionID string) {
	for _, closer := range closers {
		if closer != nil {
			closer.CloseBrowserSession(sessionID)
		}
	}
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
		if a.spawnManager != nil {
			a.closeErr = errors.Join(a.closeErr, a.spawnManager.StopAll())
		}
		if a.localRuntimes != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			a.closeErr = errors.Join(a.closeErr, a.localRuntimes.Close(ctx))
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
	var databasePool *pgxpool.Pool
	var messagingServer *messaging.Server
	if database != nil {
		databasePool = database.Pool
	}
	closeOnError := func() {
		_ = store.Close()
		if database != nil {
			database.Close()
		}
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
	var authServer *agentevents.BrowserAuthServer
	var authEnabled bool
	if browserAuthConfiguredFromEnv() {
		authServer, authEnabled, err = browserAuthServerFromEnvWithDB(context.Background(), sv, browserOrigins, databasePool)
		if err != nil {
			closeOnError()
			return nil, fmt.Errorf("browser auth: %w", err)
		}
	}
	if authEnabled {
		authServer.RegisterRoutes(mux)
	}
	// The /messaging surface requires the control-plane database. Without a
	// session verifier the routes stay mounted but fail closed (401), matching
	// the direct-chat browser routes. sv is a concrete pointer, so guard the
	// nil before it becomes a non-nil interface.
	var messagingWS *messaging.WSServer
	if database != nil {
		var messagingSessions agentevents.UserSessionAuthorizer
		if sv != nil {
			messagingSessions = sv
		}
		messagingStore := messaging.New(database.Pool)
		messagingHub := messaging.NewHub(messagingStore)
		messagingServer = messaging.NewServer(messagingStore, messagingSessions)
		messagingServer.AllowedOrigins = browserOrigins
		messagingServer.Hub = messagingHub
		// Attachment bytes live on local disk. Without a configured root the
		// attachment routes stay mounted and fail closed (503) rather than
		// accepting uploads nothing can serve.
		if root := strings.TrimSpace(os.Getenv("SUMI_MESSAGING_ATTACHMENT_ROOT")); root != "" {
			blobs, err := messaging.NewDiskAttachments(root)
			if err != nil {
				closeOnError()
				return nil, fmt.Errorf("messaging attachments: %w", err)
			}
			messagingServer.Attachments = blobs
			log.Printf("messaging attachments ready (root=%s)", root)
		}
		// Web Push は「タブを閉じていても呼ばれる」ための出口。VAPID の
		// subject は push service に対する運用連絡先で、設定が無ければ push
		// だけを黙って持たない（routes は 503 で正直に断る）。判定も既存の
		// タブ内通知も、これとは独立に動く。
		if subject := strings.TrimSpace(os.Getenv("SUMI_MESSAGING_PUSH_SUBJECT")); subject != "" {
			dispatcher, err := messaging.NewPushDispatcher(context.Background(), messagingStore, subject)
			if err != nil {
				closeOnError()
				return nil, fmt.Errorf("messaging web push: %w", err)
			}
			messagingServer.Push = dispatcher
			messagingStore.UsePush(dispatcher)
			log.Print("messaging web push ready")
		}
		// 通話 (ADR 0012)。現在値のGETは常設し、SFU未設定でも空の状態を返す。
		// token/webhookだけが503へ縮退するため、Webは404を障害として扱わずに済む。
		livekit := messaging.LiveKitConfig{
			URL:       strings.TrimSpace(os.Getenv("SUMI_LIVEKIT_URL")),
			APIKey:    strings.TrimSpace(os.Getenv("SUMI_LIVEKIT_API_KEY")),
			APISecret: strings.TrimSpace(os.Getenv("SUMI_LIVEKIT_API_SECRET")),
		}
		if registerMessagingCallRoutes(mux, messagingServer, livekit) {
			log.Printf("messaging calls ready (livekit url=%s)", livekit.URL)
		} else {
			log.Print("messaging calls unavailable (LiveKit is not configured)")
		}
		messagingServer.RegisterRoutes(mux)
		messagingWS = messaging.NewWSServer(messagingStore, messagingSessions, messagingHub)
		messagingWS.AllowedOrigins = browserOrigins
		mux.Handle("GET /messaging/ws", messagingWS)
		log.Print("messaging routes ready (REST + WS)")
	}
	if authEnabled && messagingServer != nil {
		newHumanProfileServer(messagingServer, sv, browserOrigins).RegisterRoutes(mux)
	}
	if authEnabled {
		closers := browserSessionConnectionClosers{browser}
		if messagingWS != nil {
			closers = append(closers, messagingWS)
		}
		authServer.Connections = closers
	}
	localControl, enabled, err := localControlServerFromEnvWithDB(runtime, databasePool)
	if err != nil {
		closeOnError()
		return nil, fmt.Errorf("local control fixture: %w", err)
	}
	if localControl != nil && messagingServer != nil {
		if err := messagingServer.RegisterLocalControlRoutes(localControl); err != nil {
			closeOnError()
			return nil, fmt.Errorf("register messaging local control routes: %w", err)
		}
	}
	localListener, err := localControlListenerFromEnv(enabled)
	if err != nil {
		closeOnError()
		return nil, fmt.Errorf("local control transport: %w", err)
	}
	var localMux *http.ServeMux
	if enabled {
		if localListener != nil {
			localMux = http.NewServeMux()
			if err := localControl.RegisterRoutes(localMux); err != nil {
				closeOnError()
				return nil, fmt.Errorf("register local control fixture: %w", err)
			}
		}
	}
	localRuntimes, err := localControlListenerRegistryFromEnv(localControl, enabled)
	if err != nil {
		closeOnError()
		return nil, fmt.Errorf("local control listener registry: %w", err)
	}
	var resolver spawn.AgentResolver
	if database != nil {
		resolver = koseki.New(database.Pool)
	}
	spawnManager, err := spawnManagerFromEnv(resolver, localControl, localRuntimes, runtime)
	if err != nil {
		if localRuntimes != nil {
			_ = localRuntimes.Close(context.Background())
		}
		closeOnError()
		return nil, fmt.Errorf("spawn manager: %w", err)
	}
	if spawnManager != nil {
		browser.SetSpawner(spawnManager)
	}
	mux.HandleFunc("GET /health", handler.Health)
	return &application{
		publicMux:     mux,
		localMux:      localMux,
		localListener: localListener,
		store:         store,
		browser:       browser,
		database:      database,
		spawnManager:  spawnManager,
		localRuntimes: localRuntimes,
		messaging:     messagingServer,
	}, nil
}

// registerMessagingCallRoutes keeps the browser's read-only current-state
// route mounted in every database-backed deployment, while exposing call state
// to the agent's local-control lane only when an SFU can actually produce it.
func registerMessagingCallRoutes(
	mux *http.ServeMux,
	server *messaging.Server,
	livekit messaging.LiveKitConfig,
) bool {
	calls := messaging.NewCallService(server, livekit)
	calls.RegisterRoutes(mux)
	if livekit.APIKey == "" || livekit.APISecret == "" {
		return false
	}
	server.Calls = calls
	return true
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
	if runtimeProvisioningEnabledFromEnv() {
		if socketPath != "" || loopback != "" {
			return nil, errors.New("runtime provisioning uses PAID-bound listeners; singular local-control transport settings are forbidden")
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

func localControlListenerRegistryFromEnv(
	control *agentevents.LocalControlServer,
	enabled bool,
) (*agentevents.LocalControlListenerRegistry, error) {
	if !runtimeProvisioningEnabledFromEnv() {
		if os.Getenv("SUMI_LOCAL_CONTROL_ROOT") != "" {
			return nil, errors.New("SUMI_LOCAL_CONTROL_ROOT requires SUMI_RUNTIME_PROVISIONER_SOCKET")
		}
		return nil, nil
	}
	if !enabled || control == nil {
		return nil, errors.New("runtime provisioning requires SUMI_LOCAL_CONTROL_ENABLED=1")
	}
	root := strings.TrimSpace(os.Getenv("SUMI_LOCAL_CONTROL_ROOT"))
	if root == "" {
		return nil, errors.New("SUMI_LOCAL_CONTROL_ROOT not set")
	}
	gidRaw := strings.TrimSpace(os.Getenv("SUMI_LOCAL_CONTROL_SOCKET_GID"))
	if gidRaw == "" {
		return nil, errors.New("SUMI_LOCAL_CONTROL_SOCKET_GID not set")
	}
	gid, err := strconv.ParseUint(gidRaw, 10, 31)
	if err != nil {
		return nil, errors.New("SUMI_LOCAL_CONTROL_SOCKET_GID must be a nonnegative decimal GID")
	}
	return agentevents.NewLocalControlListenerRegistry(
		control,
		agentevents.LocalControlListenerRegistryConfig{
			RootDir:   root,
			SocketGID: int(gid),
			OpenListener: func(socketPath string, socketGID int, personalityAgentID string) (net.Listener, error) {
				return listenTrustedUnixSocket(socketPath, socketGID, personalityAgentID)
			},
		},
	)
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
// never registered on the public API mux. A nil pool preserves the legacy
// single-agent env contract for tests.
func localControlServerFromEnv(runtime *agentevents.DurableGateway) (*agentevents.LocalControlServer, bool, error) {
	return localControlServerFromEnvWithDB(runtime, nil)
}

// localControlServerFromEnvWithDB dynamically registers runtime authorizations
// for every agent in the 戸籍 when pool is non-nil (ADR 0009 §1, issue #125).
// The env-configured agent (if any) keeps the shared bearer so the legacy
// single-process dev launcher keeps working without changes; additional 戸籍
// agents get per-agent derived bearers so each can connect when spawned. The
// single-agent SUMI_LOCAL_CONTROL_PERSONALITY_AGENT_ID setting is optional in
// this mode.
func localControlServerFromEnvWithDB(runtime *agentevents.DurableGateway, pool *pgxpool.Pool) (*agentevents.LocalControlServer, bool, error) {
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
	dynamicProvisioning := runtimeProvisioningEnabledFromEnv()
	var bearer string
	if !dynamicProvisioning {
		var err error
		bearer, err = required("SUMI_LOCAL_CONTROL_BEARER")
		if err != nil {
			return nil, false, err
		}
	}
	tenantID, err := required("SUMI_LOCAL_CONTROL_TENANT_ID")
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

	var authorizations []agentevents.LocalRuntimeAuthorization
	if dynamicProvisioning {
		if strings.TrimSpace(os.Getenv("SUMI_LOCAL_CONTROL_PERSONALITY_AGENT_ID")) != "" ||
			strings.TrimSpace(os.Getenv("SUMI_AGENT_BINARY")) != "" {
			return nil, false, errors.New("runtime provisioning cannot be combined with legacy env-agent or host ExecSpawner settings")
		}
	} else {
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
		authorizations, err = buildLocalControlAuthorizations(
			bearer, tenantID, rpcBootNonce, generation, controlAudience,
			agentevents.LocalDeliveryAuthorization(deliveryRaw), pool,
		)
		if err != nil {
			return nil, false, err
		}
	}
	control, err := agentevents.NewLocalControlServerWithPreviousSigningSecrets(
		runtime,
		signingSecret,
		previousSigningSecrets,
		authorizations,
	)
	if err != nil {
		return nil, false, err
	}
	return control, true, nil
}

func runtimeProvisioningEnabledFromEnv() bool {
	return strings.TrimSpace(os.Getenv("SUMI_RUNTIME_PROVISIONER_SOCKET")) != ""
}

// buildLocalControlAuthorizations assembles the runtime authorization list. In
// the legacy single-agent mode (pool == nil) the env-configured
// SUMI_LOCAL_CONTROL_PERSONALITY_AGENT_ID is required and uses the shared
// bearer. In koseki mode (pool != nil) every agent in the 戸籍 is registered;
// the env agent (if set) keeps the shared bearer, and the rest get per-agent
// derived bearers.
func buildLocalControlAuthorizations(
	bearer, tenantID, rpcBootNonce string,
	generation uint64,
	audience string,
	delivery agentevents.LocalDeliveryAuthorization,
	pool *pgxpool.Pool,
) ([]agentevents.LocalRuntimeAuthorization, error) {
	envAgentID := strings.TrimSpace(os.Getenv("SUMI_LOCAL_CONTROL_PERSONALITY_AGENT_ID"))
	if pool == nil {
		if envAgentID == "" {
			return nil, errors.New("SUMI_LOCAL_CONTROL_PERSONALITY_AGENT_ID not set")
		}
		return []agentevents.LocalRuntimeAuthorization{{
			BearerToken:           bearer,
			TenantID:              tenantID,
			PersonalityAgentID:    envAgentID,
			Generation:            generation,
			RPCBootNonce:          rpcBootNonce,
			Audience:              audience,
			DeliveryAuthorization: delivery,
		}}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	agentIDs, err := koseki.New(pool).ListAgents(ctx)
	if err != nil {
		return nil, fmt.Errorf("load 戸籍 agents: %w", err)
	}
	seen := make(map[string]bool, len(agentIDs)+1)
	var authorizations []agentevents.LocalRuntimeAuthorization
	// The env-configured agent (if any) keeps the shared bearer so the legacy
	// single-process dev launcher connects without a derived-credential change.
	if envAgentID != "" {
		authorizations = append(authorizations, agentevents.LocalRuntimeAuthorization{
			BearerToken:           bearer,
			TenantID:              tenantID,
			PersonalityAgentID:    envAgentID,
			Generation:            generation,
			RPCBootNonce:          rpcBootNonce,
			Audience:              audience,
			DeliveryAuthorization: delivery,
		})
		seen[envAgentID] = true
	}
	for _, agentID := range agentIDs {
		if seen[agentID] {
			continue
		}
		seen[agentID] = true
		authorizations = append(authorizations, agentevents.LocalRuntimeAuthorization{
			BearerToken:           deriveAgentCredential(bearer, agentID),
			TenantID:              tenantID,
			PersonalityAgentID:    agentID,
			Generation:            generation,
			RPCBootNonce:          deriveAgentCredential(rpcBootNonce, agentID),
			Audience:              audience,
			DeliveryAuthorization: delivery,
		})
	}
	if len(authorizations) == 0 {
		return nil, errors.New("no agents registered in 戸籍 and no SUMI_LOCAL_CONTROL_PERSONALITY_AGENT_ID set")
	}
	return authorizations, nil
}

// deriveAgentCredential produces a per-agent bearer/nonce from a shared secret
// and the agent id. The derivation is a simple, deterministic concatenation
// suitable for the dev control plane; each agent gets a unique credential so
// the LocalControlServer can distinguish them.
func deriveAgentCredential(shared, agentID string) string {
	return shared + "/" + agentID
}

// spawnManagerFromEnv builds the lazy runtime controller around the typed
// privileged provisioner. The API never executes host processes and never
// receives a Docker socket.
func spawnManagerFromEnv(
	resolver spawn.AgentResolver,
	control *agentevents.LocalControlServer,
	listeners *agentevents.LocalControlListenerRegistry,
	readiness runtimeReadinessController,
) (*spawn.Manager, error) {
	socketPath := strings.TrimSpace(os.Getenv("SUMI_RUNTIME_PROVISIONER_SOCKET"))
	if socketPath == "" {
		if strings.TrimSpace(os.Getenv("SUMI_AGENT_BINARY")) != "" {
			return nil, errors.New("SUMI_AGENT_BINARY host ExecSpawner is unsupported; configure SUMI_RUNTIME_PROVISIONER_SOCKET")
		}
		return nil, nil
	}
	if resolver == nil {
		return nil, errors.New("runtime provisioning requires a 戸籍 database (SUMI_DB_URL)")
	}
	if control == nil || listeners == nil {
		return nil, errors.New("runtime provisioning requires dynamic local-control authorization and listeners")
	}
	client, err := runtimeprovision.NewUnixClient(socketPath)
	if err != nil {
		return nil, err
	}
	gatewayURL, err := spawnGatewayURLFromEnv()
	if err != nil {
		return nil, err
	}
	require := func(name string) (string, error) {
		value := strings.TrimSpace(os.Getenv(name))
		if value == "" {
			return "", fmt.Errorf("%s not set", name)
		}
		return value, nil
	}
	tenantID, err := require("SUMI_LOCAL_CONTROL_TENANT_ID")
	if err != nil {
		return nil, err
	}
	audience := strings.TrimSpace(os.Getenv("SUMI_LOCAL_CONTROL_AUDIENCE"))
	if audience == "" {
		audience = strings.TrimSpace(os.Getenv("SUMI_AGENT_TOKEN_AUDIENCE"))
	}
	if audience == "" {
		audience = agentevents.DefaultAgentAudience()
	}
	delivery := agentevents.LocalDeliveryAuthorization(strings.TrimSpace(os.Getenv("SUMI_LOCAL_CONTROL_DELIVERY_AUTHORIZATION")))
	gid, err := requiredUintFromEnv("SUMI_LOCAL_CONTROL_SOCKET_GID")
	if err != nil {
		return nil, err
	}
	if gid == 0 || gid > uint64(^uint32(0)-1) {
		return nil, errors.New("SUMI_LOCAL_CONTROL_SOCKET_GID must be a nonzero Linux GID")
	}
	if os.Geteuid() == 0 {
		return nil, errors.New("API runtime provisioning control plane must run as a non-root UID")
	}
	approvalKey, err := require("SUMI_APPROVAL_SECRET_DIGEST_KEY")
	if err != nil {
		return nil, err
	}
	if err := runtimeprovision.ValidateApprovalSecretDigestKey(approvalKey); err != nil {
		return nil, fmt.Errorf("SUMI_APPROVAL_SECRET_DIGEST_KEY: %w", err)
	}
	providerKey, err := require("SUMI_PROVIDER_API_KEY")
	if err != nil {
		return nil, err
	}
	allowInsecure := false
	if raw := strings.TrimSpace(os.Getenv("SUMI_ALLOW_INSECURE_LOOPBACK_GATEWAY")); raw != "" {
		allowInsecure, err = strconv.ParseBool(raw)
		if err != nil {
			return nil, fmt.Errorf("parse SUMI_ALLOW_INSECURE_LOOPBACK_GATEWAY: %w", err)
		}
	}
	provisionedSpawner, err := newProvisionedRuntimeSpawner(provisionedRuntimeSpawnerConfig{
		Provisioner:    client,
		Authorizations: control,
		Listeners:      listeners,
		Readiness:      readiness,
		TenantID:       tenantID,
		Audience:       audience,
		Delivery:       delivery,
		Activation: runtimeprovision.ActivationConfig{
			LocalControlServerUID:        uint32(os.Geteuid()),
			LocalControlSocketGID:        uint32(gid),
			ApprovalSecretDigestKey:      approvalKey,
			ProviderAPIKey:               providerKey,
			ModelPreset:                  strings.TrimSpace(os.Getenv("SUMI_MODEL_PRESET")),
			ModelID:                      strings.TrimSpace(os.Getenv("SUMI_MODEL_ID")),
			AllowInsecureLoopbackGateway: allowInsecure,
			LogFilter:                    strings.TrimSpace(os.Getenv("SUMI_LOG")),
		},
	})
	if err != nil {
		return nil, err
	}
	idleTimeout := 5 * time.Minute
	if v := os.Getenv("SUMI_SPAWN_IDLE_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("parse SUMI_SPAWN_IDLE_TIMEOUT: %w", err)
		}
		idleTimeout = d
	}
	mgr, err := spawn.New(spawn.Config{
		Spawner:     provisionedSpawner,
		Resolver:    resolver,
		GatewayURL:  gatewayURL,
		IdleTimeout: idleTimeout,
	})
	if err != nil {
		return nil, err
	}
	return mgr, nil
}

// spawnGatewayURLFromEnv resolves the gateway URL passed to spawned agents.
// Explicit SUMI_AGENT_GATEWAY_URL wins, otherwise derive from the public
// loopback listener. Insecure ws:// loopback is expected for the dev plane.
func spawnGatewayURLFromEnv() (string, error) {
	if v := os.Getenv("SUMI_AGENT_GATEWAY_URL"); v != "" {
		return v, nil
	}
	loopback := os.Getenv("SUMI_PUBLIC_LOOPBACK_LISTEN")
	if loopback != "" {
		return "ws://" + loopback + "/agent/ws", nil
	}
	public := os.Getenv("SUMI_PUBLIC_LISTEN")
	if public == "" {
		public = ":" + os.Getenv("PORT")
		if public == ":" {
			public = ":8080"
		}
	}
	host, port, err := net.SplitHostPort(public)
	if err != nil {
		return "", fmt.Errorf("SUMI_PUBLIC_LISTEN must be host:port to derive agent gateway URL: %w", err)
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return "", errors.New("SUMI_PUBLIC_LISTEN host is not an IP")
	}
	if !ip.IsUnspecified() && !ip.IsLoopback() {
		return "", errors.New("SUMI_PUBLIC_LISTEN must be loopback or 0.0.0.0 to derive agent gateway URL; set SUMI_AGENT_GATEWAY_URL explicitly")
	}
	if ip.IsUnspecified() {
		ip = net.IPv4(127, 0, 0, 1)
	}
	return "ws://" + net.JoinHostPort(ip.String(), port) + "/agent/ws", nil
}

// requireDirFromEnv returns the value of name, ensuring the directory exists.
func requireDirFromEnv(name string) (string, error) {
	value := os.Getenv(name)
	if value == "" {
		return "", fmt.Errorf("%s not set", name)
	}
	if err := os.MkdirAll(value, 0o700); err != nil {
		return "", fmt.Errorf("create %s: %w", name, err)
	}
	return value, nil
}

// requiredUintFromEnv parses a required unsigned integer env variable.
func requiredUintFromEnv(name string) (uint64, error) {
	value := os.Getenv(name)
	if value == "" {
		return 0, fmt.Errorf("%s not set", name)
	}
	n, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", name, err)
	}
	return n, nil
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
