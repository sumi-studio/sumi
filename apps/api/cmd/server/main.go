package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	applicationapps "github.com/sumi-studio/sumi/apps/api/internal/apps"
	"github.com/sumi-studio/sumi/apps/api/internal/browsertabs"
	"github.com/sumi-studio/sumi/apps/api/internal/chatgpt"
	"github.com/sumi-studio/sumi/apps/api/internal/cloudbrowser"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/directchat"
	"github.com/sumi-studio/sumi/apps/api/internal/feedback"
	"github.com/sumi-studio/sumi/apps/api/internal/fileaccess"
	"github.com/sumi-studio/sumi/apps/api/internal/handler"
	"github.com/sumi-studio/sumi/apps/api/internal/jobexec"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
	"github.com/sumi-studio/sumi/apps/api/internal/mcpconnections"
	"github.com/sumi-studio/sumi/apps/api/internal/messaging"
	"github.com/sumi-studio/sumi/apps/api/internal/modelconnections"
	"github.com/sumi-studio/sumi/apps/api/internal/participant"
	"github.com/sumi-studio/sumi/apps/api/internal/portable"
	"github.com/sumi-studio/sumi/apps/api/internal/returnsession"
	"github.com/sumi-studio/sumi/apps/api/internal/termexec"
	"github.com/sumi-studio/sumi/apps/api/internal/transfersession"
	"github.com/sumi-studio/sumi/apps/api/internal/usageview"
	workspacecontrol "github.com/sumi-studio/sumi/apps/api/internal/workspace"
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
	if app.messagingServer != nil {
		// Readers resolve temporary status expiry themselves; this worker makes
		// it visible on already-open screens. Start it only after run owns the
		// application so Close can cancel it.
		go app.messagingServer.RunStatusExpiry(
			app.backgroundCtx,
			messaging.DefaultStatusExpiryInterval,
		)
	}

	publicServer := &http.Server{
		Addr:              publicAddress,
		Handler:           app.publicMux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	publicListener, err := net.Listen("tcp", publicServer.Addr)
	if err != nil {
		return fmt.Errorf("listen on public API: %w", err)
	}

	log.Printf("sumi api listening on %s", publicListener.Addr())
	app.startAgentAttention()
	app.startCoreWaker()
	app.startCoreDirectChat()
	app.startFeedbackAttention()
	app.startJobExec()
	app.startMCP()
	app.startBrowserTabs()
	app.startCloudBrowser()
	app.startTermExec()
	app.startEmailDelivery()
	if app.transferSessions != nil {
		// Owes activation after a committed account claim, retires staged
		// copies of closed sessions, and promotes interrupted imports.
		go app.transferSessions.Run(app.backgroundCtx, transferSweepInterval, log.Printf)
	}
	if app.returnSessions != nil {
		// Expires unbound admission and lands ledger outcomes (seal,
		// complete, abort) the request that owed the update lost.
		go app.returnSessions.Run(app.backgroundCtx, transferSweepInterval, log.Printf)
	}
	return serveHTTPServers(ctx, serverAndListener{server: publicServer, listener: publicListener})
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
	chatGPTLogin               *chatgpt.LoginService
	emailDelivery              *emailDeliveryWorker
	publicMux                  *http.ServeMux
	store                      *agentevents.CommandStore
	browser                    *agentevents.BrowserServer
	database                   *db.Pool
	messagingServer            *messaging.Server
	backgroundCtx              context.Context
	deliverAttention           func(context.Context) (messaging.AgentAttentionDeliveryStats, error)
	deliverFeedbackAttention   func(context.Context) error
	cleanupFeedbackAttachments func(context.Context) error
	attentionWorkers           sync.WaitGroup
	coreWaker                  *agentstate.RuntimeWaker
	jobExec                    *jobexec.Driver
	mcpRunner                  *mcpconnections.Runner
	browserTabs                *browsertabs.Store
	cloudBrowser               *cloudbrowser.Service
	termExec                   *termexec.Driver
	transferSessions           *transfersession.Service
	returnSessions             *returnsession.Service
	coreDirectChat             *agentevents.CoreDirectChat
	// stopBackground cancels process-lifetime workers such as the attachment
	// reconciler and status expiry sweep.
	stopBackground context.CancelFunc
	closeOnce      sync.Once
	closeErr       error
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
		if a.stopBackground != nil {
			a.stopBackground()
		}
		a.attentionWorkers.Wait()
		if a.chatGPTLogin != nil {
			a.chatGPTLogin.Close()
		}
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
	cmdDir := os.Getenv("SUMI_COMMAND_LOG_DIR")
	if cmdDir == "" {
		return nil, errors.New("SUMI_COMMAND_LOG_DIR not set")
	}
	store, err := agentevents.OpenCommandStore(cmdDir)
	if err != nil {
		return nil, fmt.Errorf("open command store: %w", err)
	}
	runtimeDir := os.Getenv("SUMI_BROWSER_EVENT_DIR")
	if runtimeDir == "" {
		_ = store.Close()
		return nil, errors.New("SUMI_BROWSER_EVENT_DIR not set")
	}
	runtime, err := agentevents.OpenBrowserJournal(runtimeDir, store)
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
	var workspaceServer *workspacecontrol.Server
	var workspaceStore *workspacecontrol.Store
	var appStore *applicationapps.Store
	directChatLifecycle := directchat.NewLifecycleFence()
	if database != nil {
		databasePool = database.Pool
		workspaceStore = workspacecontrol.New(database.Pool)
		appStore = applicationapps.New(database.Pool, workspaceStore, directChatLifecycle)
	}
	closeOnError := func() {
		_ = store.Close()
		if database != nil {
			database.Close()
		}
	}

	chatGPTConnections, err := chatGPTStoreFromEnv(databasePool)
	if err != nil {
		closeOnError()
		return nil, err
	}
	var chatGPTLogin *chatgpt.LoginService
	if chatGPTConnections != nil {
		chatGPTLogin = chatgpt.NewLoginService(chatGPTConnections, chatgpt.NewOAuthClient(), chatGPTBrowserIdentity(sv, browserOrigins), nil)
	}
	modelConnections, err := modelConnectionStoreFromEnv(databasePool)
	if err != nil {
		closeOnError()
		return nil, err
	}
	modelConnectionService := &modelconnections.Service{Store: modelConnections, Authenticate: chatGPTBrowserIdentity(sv, browserOrigins)}

	var directChatAuthorizer agentevents.DirectChatAuthorizer
	if database != nil {
		directChatAuthorizer = newDirectChatAuthorizer(
			database.Pool,
			koseki.New(database.Pool, directChatLifecycle),
			appStore,
		)
	}
	mux, browser, err := agentevents.NewProductionMux(
		store,
		runtime,
		sv,
		browserOrigins,
		directChatAuthorizer,
		directChatLifecycle,
	)
	if err != nil {
		closeOnError()
		return nil, err
	}
	var authServer *agentevents.BrowserAuthServer
	var secretaryTransfer *secretaryTransferMount
	var secretaryReturn *secretaryReturnMount
	var authEnabled bool
	if browserAuthConfiguredFromEnv() {
		authServer, secretaryTransfer, authEnabled, err = browserAuthServerFromEnvWithDB(
			context.Background(), sv, browserOrigins, databasePool, directChatLifecycle,
		)
		if err != nil {
			closeOnError()
			return nil, fmt.Errorf("browser auth: %w", err)
		}
		secretaryReturn, err = secretaryReturnFromEnv(databasePool, sv, browserOrigins)
		if err != nil {
			closeOnError()
			return nil, fmt.Errorf("secretary return: %w", err)
		}
	}
	if authEnabled {
		authServer.RegisterRoutes(mux)
		if secretaryTransfer != nil {
			secretaryTransfer.server.RegisterRoutes(mux)
		}
		if secretaryReturn != nil {
			secretaryReturn.server.RegisterRoutes(mux)
		}
		if database != nil {
			newHumanProfileServer(koseki.New(database.Pool), sv, browserOrigins).RegisterRoutes(mux)
		}
	}
	// The /messaging surface requires the control-plane database. Without a
	// session verifier the routes stay mounted but fail closed (401), matching
	// the direct-chat browser routes. sv is a concrete pointer, so guard the
	// nil before it becomes a non-nil interface.
	var messagingWS *messaging.WSServer
	var feedbackServer *feedback.Server
	if database != nil {
		var messagingSessions agentevents.UserSessionAuthorizer
		if sv != nil {
			messagingSessions = sv
		}
		workspaceServer = workspacecontrol.NewServer(
			workspaceStore,
			appStore,
			messagingSessions,
			koseki.New(database.Pool),
		)
		if authServer != nil {
			workspaceStore.EnrollmentAdmin = authServer.IsEnrollmentAdmin
			workspaceServer.EnrollmentIssuer = koseki.New(database.Pool)
			workspaceServer.VerifyEnrollmentRecipient = func(ctx context.Context, claims agentevents.UserSessionClaims, token string) (workspacecontrol.EnrollmentRecipientProof, error) {
				identity, err := authServer.Firebase.VerifyIDToken(ctx, token)
				if err != nil {
					return workspacecontrol.EnrollmentRecipientProof{}, err
				}
				email, err := koseki.NormalizeEmail(identity.Email)
				if err != nil {
					return workspacecontrol.EnrollmentRecipientProof{}, err
				}
				return workspacecontrol.EnrollmentRecipientProof{FirebaseUID: identity.UID, Email: email, EmailVerified: identity.EmailVerified}, nil
			}
		}
		feedbackRecipients, feedbackErr := feedback.ParseRecipients(os.Getenv("SUMI_FEEDBACK_RECIPIENTS"))
		if feedbackErr != nil {
			log.Print("feedback destination disabled: SUMI_FEEDBACK_RECIPIENTS contains an invalid participant key")
			feedbackRecipients = nil
		}
		feedbackServer = &feedback.Server{Store: feedback.New(database.Pool, feedbackRecipients), Gateway: runtime, Sessions: messagingSessions, AllowedOrigins: browserOrigins}
		feedbackServer.RegisterRoutes(mux)
		workspaceServer.AllowedOrigins = browserOrigins
		workspaceServer.RegisterRoutes(mux)
		log.Print("workspace and app lifecycle routes ready")

		messagingStore := messaging.New(database.Pool, workspaceStore, appStore)
		if authEnabled {
			authServer.PushDevices = messagingStore
		}
		if err := configureMessagingAttachmentsFromEnv(messagingStore); err != nil {
			closeOnError()
			return nil, fmt.Errorf("messaging attachments: %w", err)
		}
		messagingHub := messaging.NewHub(messagingStore)
		messagingServer = messaging.NewServer(messagingStore, messagingSessions)
		messagingServer.AllowedOrigins = browserOrigins
		messagingServer.Hub = messagingHub
		pushSubject := strings.TrimSpace(os.Getenv("SUMI_MESSAGING_PUSH_SUBJECT"))
		if pushSubject != "" {
			if sv == nil {
				closeOnError()
				return nil, errors.New("SUMI_MESSAGING_PUSH_SUBJECT requires browser session authorization")
			}
			pushDispatcher, pushErr := messaging.NewPushDispatcher(
				context.Background(), messagingStore, pushSubject,
			)
			if pushErr != nil {
				closeOnError()
				return nil, fmt.Errorf("messaging Web Push: %w", pushErr)
			}
			messagingStore.UsePush(pushDispatcher)
			messagingServer.Push = pushDispatcher
			log.Print("messaging Web Push ready (generic payload)")
		} else {
			log.Print("messaging Web Push disabled: SUMI_MESSAGING_PUSH_SUBJECT is unset")
		}
		messagingServer.RegisterRoutes(mux)
		livekit, callsEnabled, configErr := liveKitConfigFromEnv()
		if configErr != nil {
			closeOnError()
			return nil, configErr
		}
		if callsEnabled {
			calls := messaging.NewCallService(messagingServer, livekit)
			messagingServer.Calls = calls
			workspaceServer.MembershipClosed = func(ctx context.Context, workspaceID string, member participant.Ref) {
				// Best-effort cleanup after the committed closure: retry a
				// few times on a detached context so a transient store or
				// LiveKit fault doesn't strand media, and a disconnecting
				// client can't cancel the cleanup. Authority never depends
				// on this succeeding — the call bridge gates re-check place
				// membership — but a failure must stay observable.
				cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				var err error
				for attempt := 1; attempt <= 3; attempt++ {
					if err = calls.RemoveWorkspaceParticipant(cleanupCtx, workspaceID, member); err == nil {
						return
					}
					log.Printf("remove call participant after Workspace membership closure (attempt %d/3): %v", attempt, err)
					if attempt == 3 {
						return
					}
					select {
					case <-cleanupCtx.Done():
						return
					case <-time.After(time.Duration(attempt) * 500 * time.Millisecond):
					}
				}
			}
			calls.RegisterRoutes(mux)
			log.Printf("messaging calls ready (livekit url=%s)", livekit.URL)
		}
		messagingWS = messaging.NewWSServer(messagingStore, messagingSessions, messagingHub)
		messagingWS.AllowedOrigins = browserOrigins
		mux.Handle("GET /messaging/ws", messagingWS)
		log.Print("messaging routes ready (REST + WS)")
	}
	if authEnabled {
		closers := browserSessionConnectionClosers{browser}
		if messagingWS != nil {
			closers = append(closers, messagingWS)
		}
		authServer.Connections = closers
	}
	if chatGPTLogin != nil {
		chatGPTLogin.RegisterRoutes(mux)
	}
	modelConnectionService.RegisterRoutes(mux)
	// Persona-scoped core-state service for the shared TypeScript secretary
	// core. Opt-in: mounted only when a service token is configured and the
	// control-plane database exists. Developer/operator credential scope; see
	// internal/agentstate for the authorization model.
	var coreServer *agentstate.Server
	var coreWaker *agentstate.RuntimeWaker
	if coreToken := strings.TrimSpace(os.Getenv("SUMI_CORE_STATE_TOKEN")); coreToken != "" && database != nil {
		if len(coreToken) < 16 {
			closeOnError()
			return nil, errors.New("SUMI_CORE_STATE_TOKEN must be at least 16 characters")
		}
		coreServer = agentstate.NewServer(database.Pool, coreToken)
		coreServer.SetModelConnections(modelConnections)
		// A Cloud core host (Durable Objects) authenticates with one runtime
		// credential and is woken from here; a Local host needs neither.
		if runtimeToken := strings.TrimSpace(os.Getenv(agentstate.RuntimeTokenEnv)); runtimeToken != "" {
			if err := coreServer.SetRuntimeToken(runtimeToken); err != nil {
				closeOnError()
				return nil, err
			}
			log.Print("core state accepts the runtime credential for persona-scoped routes")
		}
		waker, err := agentstate.RuntimeWakerFromEnv(coreServer.Store(), os.Getenv)
		if err != nil {
			closeOnError()
			return nil, err
		}
		if waker != nil {
			coreWaker = waker
			log.Printf("core wake: sweeping for personas awaiting a runtime; waking %s", waker.Target())
		}
		coreServer.RegisterRoutes(mux)
		portable.NewServer(database.Pool, coreToken).RegisterRoutes(mux)
		// The human-facing usage/budget surface shares the core store: a
		// changed selection or connection reopens the funding question for
		// budget-parked inputs, so chain the resume hook into the existing
		// model-connection change callback.
		usageService := &usageview.Service{
			Store:        coreServer.Store(),
			Authenticate: chatGPTBrowserIdentity(sv, browserOrigins),
		}
		usageService.RegisterRoutes(mux)
		prevChanged := modelConnectionService.Changed
		modelConnectionService.Changed = func(human string) {
			if prevChanged != nil {
				prevChanged(human)
			}
			usageService.FundingChanged(human)
		}
		log.Print("core state routes ready (/internal/core, scoped tokens; transfers admin-only)")
	}
	var coreDirectChat *agentevents.CoreDirectChat
	if coreServer == nil {
		// Core is the required execution and state service.
		closeOnError()
		return nil, errors.New("Sumi requires the core state service (SUMI_CORE_STATE_TOKEN and a database)")
	}
	if coreServer != nil {
		// The accepted TypeScript core serves Direct Chat: admitted commands
		// become durable core inputs, and committed journal events are
		// projected into the same browser-visible event log the existing
		// WebSocket/history surfaces already consume. The durable gateway
		// remains the event-log owner; the adapter only repoints admission
		// and readiness.
		coreDirectChat = &agentevents.CoreDirectChat{
			Core:    coreServer.Store(),
			Gateway: runtime,
			Pool:    database.Pool,
		}
		browser.SetAppender(coreDirectChat)
		log.Print("direct chat commands and replies run through the core state service")
	}
	// Canonical workspace files: one internal filesvc credential held
	// server-side. The person-facing /files/* routes derive their scope from
	// the verified session's PAID; the secretary's file.* effects scope to
	// the persona the core operation ledger claims. Neither face accepts a
	// caller-supplied scope. Unset config disables both faces cleanly.
	filesClient, err := fileaccess.FromEnv(os.Getenv)
	if err != nil {
		closeOnError()
		return nil, err
	}
	if filesClient != nil {
		browser.Files = filesClient
		browser.RegisterFileRoutes(mux)
		log.Print("person file routes ready (/files/*, session-scoped to canonical filesvc)")
		if databasePool != nil {
			// The person file surface marks the retained Cloud copy after
			// a local-mode return: the persona's working store is wherever
			// its latest completed return moved it.
			browser.WorkingStore = func(ctx context.Context, personaID string) (string, error) {
				var mode *string
				err := databasePool.QueryRow(ctx, `SELECT file_mode FROM return_sessions
					WHERE persona_id = $1 AND status = 'completed' AND file_mode IS NOT NULL
					ORDER BY created_at DESC LIMIT 1`, personaID).Scan(&mode)
				if err != nil {
					if errors.Is(err, pgx.ErrNoRows) {
						return "cloud", nil
					}
					return "", err
				}
				if mode == nil {
					return "cloud", nil
				}
				return *mode, nil
			}
		}
		if secretaryReturn != nil {
			// Return file surfaces: the grant's copy-read route (local
			// mode), the scoped-storage proxy (cloud mode) and the seal's
			// mutation fence all reach the same canonical service.
			secretaryReturn.service.SetFileStore(filesClient)
			secretaryReturn.service.SetCaptureStore(filesClient)
			secretaryReturn.server.SetFiles(filesClient)
			secretaryReturn.server.RegisterFileProxy(mux)
			log.Print("secretary-return file routes ready (/api/secretary-files/* scoped-token proxy)")
		}
		if coreServer != nil {
			for tool, effect := range fileaccess.FileEffects(filesClient) {
				if err := coreServer.RegisterToolEffect(tool, effect); err != nil {
					closeOnError()
					return nil, fmt.Errorf("register core file effect %s: %w", tool, err)
				}
			}
			coreServer.SetJobFileService(fileaccess.JobFileService(filesClient))
			log.Print("core file tools ready (file.* effects scoped to the claiming persona; job file capability armed)")
		}
	}
	browserTabs, err := wireBrowserTabs(databasePool, coreServer, mux, chatGPTBrowserIdentity(sv, browserOrigins))
	if err != nil {
		closeOnError()
		return nil, err
	}
	cloudBrowser, err := wireCloudBrowser(databasePool, browserTabs, mux, chatGPTBrowserIdentity(sv, browserOrigins))
	if err != nil {
		closeOnError()
		return nil, err
	}
	mcpRunner, err := wireMCP(databasePool, coreServer, mux, chatGPTBrowserIdentity(sv, browserOrigins))
	if err != nil {
		closeOnError()
		return nil, err
	}
	// Cloud Linux jobs: subprocess-kind core_jobs run on the root
	// provisioner's durable process service, bind-mounted to the persona's
	// canonical files scope. Opt-in; a partial configuration is a startup
	// error, not a silently queued backend.
	var jobExec *jobexec.Driver
	{
		var coreStore *agentstate.Store
		if coreServer != nil {
			coreStore = coreServer.Store()
		}
		var err error
		jobExec, err = jobexecFromEnv(coreStore, filesClient)
		if err != nil {
			closeOnError()
			return nil, err
		}
		if jobExec != nil {
			log.Printf("jobexec: Cloud subprocess jobs run through the runtime provisioner (runner %s, canonical files scope)", jobExec.Runner())
		}
	}
	// Interactive terminal sessions: same Cloud execution environment and
	// canonical files scope as subprocess jobs, but long-lived with a real
	// PTY. Opt-in via SUMI_TERMEXEC_ENABLED (or implicitly with jobexec);
	// a partial configuration is a startup error.
	var termExec *termexec.Driver
	{
		var coreStore *agentstate.Store
		if coreServer != nil {
			coreStore = coreServer.Store()
		}
		var err error
		termExec, err = termexecFromEnv(coreStore, filesClient)
		if err != nil {
			closeOnError()
			return nil, err
		}
		if termExec != nil {
			log.Printf("termexec: Cloud interactive terminal sessions run through the runtime provisioner (runner %s, canonical files scope)", termExec.Runner())
			if secretaryReturn != nil {
				// The return seal's writer-cut gate consumes the same
				// provisioner ops the driver claims: a session is only
				// proven stopped when its op reports Quiesced.
				secretaryReturn.service.SetTerminalProcesses(termExec.Processes())
			}
		}
	}
	// Person-facing terminal routes share the core state store — the
	// verified browser session supplies the persona, so a caller can only
	// ever reach its own secretary's sessions. Terminal requests authorize
	// through the participant-owned 'terminal' AppInstallation, not the
	// direct-chat installation the browser may also carry.
	if coreServer != nil {
		browser.Terminals = coreServer.Store()
		if terminalAuth, ok := directChatAuthorizer.(agentevents.TerminalAuthorizer); ok {
			browser.TerminalAuthorizer = terminalAuth
		}
		if termExec != nil {
			browser.TerminalHealth = termExec
		}
		browser.RegisterTerminalRoutes(mux)
		log.Print("person terminal routes ready (/terminal/*, session-scoped to the persona)")
	}
	mux.HandleFunc("GET /health", handler.Health)
	backgroundCtx, stopBackground := context.WithCancel(context.Background())
	if messagingServer != nil && messagingServer.Store.AttachmentsEnabled() {
		go messagingServer.Store.RunAttachmentReconciler(backgroundCtx, messaging.AttachmentReconcileInterval)
	}
	var deliverAttention func(context.Context) (messaging.AgentAttentionDeliveryStats, error)
	switch {
	case messagingServer != nil && coreServer != nil:
		// Shared-conversation intake into the accepted core: attention events
		// become durable per-persona inputs, and the secretary's replies post
		// back into real Messaging places through the messaging.send effect —
		// atomic with the operation record that authorized it.
		delivery := &messaging.CoreAttentionDelivery{
			Core:      coreServer.Store(),
			Messaging: messagingServer.Store,
			Hub:       messagingServer.Hub,
		}
		for tool, effect := range delivery.CoreToolEffects() {
			if err := coreServer.RegisterToolEffect(tool, effect); err != nil {
				stopBackground()
				closeOnError()
				return nil, fmt.Errorf("register core messaging effect %s: %w", tool, err)
			}
		}
		deliverAttention = func(ctx context.Context) (messaging.AgentAttentionDeliveryStats, error) {
			return messagingServer.Store.DeliverAgentAttention(ctx, delivery, 25)
		}
		// The human-facing approval inbox: the browser decides as the verified
		// session's human, and parking/deciding nudges the human's live
		// Messaging sockets so the inbox reflects durable state promptly.
		coreApprovals := &messaging.CoreApprovalsServer{
			Core:           coreServer.Store(),
			Messaging:      messagingServer.Store,
			Hub:            messagingServer.Hub,
			Sessions:       messagingServer.Sessions,
			AllowedOrigins: browserOrigins,
		}
		coreApprovals.RegisterRoutes(mux)
		coreServer.Store().ApprovalsChanged = coreApprovals.NotifyChanged
		// A directed Messaging request that fails terminally leaves its
		// requester a visible reply in the same place — committed atomically
		// with the failure record and deduplicated on commit replay.
		coreServer.Store().TerminalFailureNotice = delivery.TerminalFailureNotice
		log.Print("messaging attention delivers to core state inputs (messaging.* effects registered)")
		if calls := messagingServer.Calls; calls != nil {
			// The secretary's call surface: delegated effects commit session
			// and utterance intent atomically with the operation record, and
			// the persona-scoped bridge routes let a per-placement media
			// runner claim sessions, mint short tickets, and report status
			// and playback dispositions under its own claim authority.
			calls.Hooks = &messaging.CallHooks{Core: coreServer.Store()}
			for tool, effect := range map[string]agentstate.ToolEffect{
				messaging.CallJoinTool:  calls.CallJoinEffect(),
				messaging.CallLeaveTool: calls.CallLeaveEffect(),
				messaging.CallSayTool:   calls.CallSayEffect(),
				messaging.CallStateTool: calls.CallStateEffect(),
			} {
				if err := coreServer.RegisterToolEffect(tool, effect); err != nil {
					stopBackground()
					closeOnError()
					return nil, fmt.Errorf("register core call effect %s: %w", tool, err)
				}
			}
			coreServer.SetCallBridge(calls)
			log.Print("call sessions ready (call.join/leave/say/state effects + media bridge routes)")
		}
	}
	var transferSessions *transfersession.Service
	if secretaryTransfer != nil {
		transferSessions = secretaryTransfer.service
	}
	var returnSessions *returnsession.Service
	if secretaryReturn != nil {
		returnSessions = secretaryReturn.service
	}
	if workspaceStore != nil && coreServer != nil {
		// The secretary's Workspace invitation list/accept on the core: the
		// delegated effects run inside the operation-claim transaction — the
		// membership change and operation receipt commit together.
		for tool, effect := range workspaceStore.CoreInvitationToolEffects() {
			if err := coreServer.RegisterToolEffect(tool, effect); err != nil {
				stopBackground()
				closeOnError()
				return nil, fmt.Errorf("register core workspace effect %s: %w", tool, err)
			}
		}
	}
	var deliverFeedbackAttention func(context.Context) error
	var cleanupFeedbackAttachments func(context.Context) error
	if feedbackServer != nil {
		cleanupFeedbackAttachments = feedbackServer.Store.CleanupAttachments
	}
	switch {
	case feedbackServer != nil && coreServer != nil:
		// Feedback attention joins Messaging attention on the core intake:
		// events become durable per-persona inputs the wake sweep delivers to
		// the configured Core host.
		delivery := &feedback.CoreAttentionDelivery{Core: coreServer.Store(), Pool: database.Pool}
		deliverFeedbackAttention = func(ctx context.Context) error { return feedbackServer.Store.DeliverAttention(ctx, delivery, 25) }
		log.Print("feedback attention delivers to core state inputs")
	}
	return &application{
		cleanupFeedbackAttachments: cleanupFeedbackAttachments,
		deliverFeedbackAttention:   deliverFeedbackAttention,
		chatGPTLogin:               chatGPTLogin,
		emailDelivery:              emailDeliveryWorkerFor(authServer),
		deliverAttention:           deliverAttention,
		coreWaker:                  coreWaker,
		jobExec:                    jobExec,
		mcpRunner:                  mcpRunner,
		browserTabs:                browserTabs,
		cloudBrowser:               cloudBrowser,
		termExec:                   termExec,
		transferSessions:           transferSessions,
		returnSessions:             returnSessions,
		coreDirectChat:             coreDirectChat,
		publicMux:                  mux,
		store:                      store,
		browser:                    browser,
		database:                   database,
		messagingServer:            messagingServer,
		backgroundCtx:              backgroundCtx,
		stopBackground:             stopBackground,
	}, nil
}

// Messaging attachment storage is opt-in and fails closed. The root and every
// byte/object cap must be set together. Attachments stay disabled (uploads
// answer 503) when all are absent, and startup fails for a partial policy so
// the API never runs with an unbounded Workspace or whole blob store.
const (
	messagingAttachmentRootEnv             = "SUMI_MESSAGING_ATTACHMENT_ROOT"
	messagingAttachmentWorkspaceBytesEnv   = "SUMI_MESSAGING_ATTACHMENT_WORKSPACE_QUOTA_BYTES"
	messagingAttachmentWorkspaceObjectsEnv = "SUMI_MESSAGING_ATTACHMENT_WORKSPACE_QUOTA_OBJECTS"
	messagingAttachmentTotalBytesEnv       = "SUMI_MESSAGING_ATTACHMENT_TOTAL_QUOTA_BYTES"
	messagingAttachmentTotalObjectsEnv     = "SUMI_MESSAGING_ATTACHMENT_TOTAL_QUOTA_OBJECTS"
)

func configureMessagingAttachmentsFromEnv(store *messaging.Store) error {
	root := strings.TrimSpace(os.Getenv(messagingAttachmentRootEnv))
	values := map[string]string{
		messagingAttachmentWorkspaceBytesEnv:   strings.TrimSpace(os.Getenv(messagingAttachmentWorkspaceBytesEnv)),
		messagingAttachmentWorkspaceObjectsEnv: strings.TrimSpace(os.Getenv(messagingAttachmentWorkspaceObjectsEnv)),
		messagingAttachmentTotalBytesEnv:       strings.TrimSpace(os.Getenv(messagingAttachmentTotalBytesEnv)),
		messagingAttachmentTotalObjectsEnv:     strings.TrimSpace(os.Getenv(messagingAttachmentTotalObjectsEnv)),
	}
	allAbsent := root == ""
	for _, value := range values {
		allAbsent = allAbsent && value == ""
	}
	if allAbsent {
		log.Print("messaging attachments disabled: no attachment root configured")
		return nil
	}
	if root == "" {
		return fmt.Errorf("%s and all attachment caps must be set together", messagingAttachmentRootEnv)
	}
	parsed := make(map[string]int64, len(values))
	for name, raw := range values {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || value <= 0 {
			return fmt.Errorf("%s must be a positive integer", name)
		}
		parsed[name] = value
	}
	blobs, err := messaging.NewDiskAttachments(root)
	if err != nil {
		return err
	}
	policy := messaging.AttachmentPolicy{
		WorkspaceQuotaBytes:   parsed[messagingAttachmentWorkspaceBytesEnv],
		WorkspaceQuotaObjects: parsed[messagingAttachmentWorkspaceObjectsEnv],
		TotalQuotaBytes:       parsed[messagingAttachmentTotalBytesEnv],
		TotalQuotaObjects:     parsed[messagingAttachmentTotalObjectsEnv],
	}
	if err := store.ConfigureAttachments(blobs, policy); err != nil {
		return err
	}
	log.Printf("messaging attachments ready at %s (workspace %d bytes/%d objects; total %d bytes/%d objects)",
		blobs.RootPath(), policy.WorkspaceQuotaBytes, policy.WorkspaceQuotaObjects,
		policy.TotalQuotaBytes, policy.TotalQuotaObjects)
	return nil
}

// liveKitConfigFromEnv leaves calls absent when both credentials are empty.
// A partially configured media boundary fails startup instead of mounting a
// route that can mint unusable credentials.
func liveKitConfigFromEnv() (messaging.LiveKitConfig, bool, error) {
	config := messaging.LiveKitConfig{
		URL:       strings.TrimSpace(os.Getenv("SUMI_LIVEKIT_URL")),
		APIURL:    strings.TrimSpace(os.Getenv("SUMI_LIVEKIT_API_URL")),
		APIKey:    strings.TrimSpace(os.Getenv("SUMI_LIVEKIT_API_KEY")),
		APISecret: strings.TrimSpace(os.Getenv("SUMI_LIVEKIT_API_SECRET")),
	}
	if config.APIKey == "" && config.APISecret == "" {
		return messaging.LiveKitConfig{}, false, nil
	}
	if config.APIKey == "" || config.APISecret == "" || config.URL == "" {
		return messaging.LiveKitConfig{}, false, errors.New("SUMI_LIVEKIT_URL, SUMI_LIVEKIT_API_KEY, and SUMI_LIVEKIT_API_SECRET must be configured together")
	}
	parsed, err := url.Parse(config.URL)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "ws" && parsed.Scheme != "wss") {
		return messaging.LiveKitConfig{}, false, errors.New("SUMI_LIVEKIT_URL must be an absolute ws:// or wss:// URL")
	}
	if config.APIURL != "" {
		apiURL, err := url.Parse(config.APIURL)
		if err != nil || apiURL.Host == "" || (apiURL.Scheme != "http" && apiURL.Scheme != "https") {
			return messaging.LiveKitConfig{}, false, errors.New("SUMI_LIVEKIT_API_URL must be an absolute http:// or https:// URL")
		}
	}
	return config, true, nil
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

var errBrowserSessionSecretMissing = errors.New("SUMI_BROWSER_SESSION_SECRET not set")

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
