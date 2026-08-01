package agentevents

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"
)

const (
	localControlRegistryDirectoryMode = 0o750
	localControlRegistrySocketName    = "control.sock"
	localControlRegistryMaxSocketPath = 100
)

// LocalControlListenerOpener binds one trusted Unix listener. Production
// callers should adapt the existing trusted listener implementation; the
// registry supplies only a validated canonical path, GID, and PAID binding.
type LocalControlListenerOpener func(
	socketPath string,
	socketGID int,
	personalityAgentID string,
) (net.Listener, error)

type LocalControlListenerRegistryConfig struct {
	// RootDir is an existing, real, absolute directory owned by the current
	// process user and SocketGID with mode 0750. Each PAID gets one child.
	RootDir      string
	SocketGID    int
	OpenListener LocalControlListenerOpener
}

// LocalControlListenerRegistry owns one independent local-control listener per
// PAID. It never multiplexes multiple personality agents onto a shared socket.
type LocalControlListenerRegistry struct {
	control *LocalControlServer
	config  LocalControlListenerRegistryConfig

	mu        sync.Mutex
	listeners map[string]*localControlListenerEntry
	closed    bool
}

type localControlListenerEntry struct {
	path     string
	listener net.Listener
	server   *http.Server
	done     chan error
}

func NewLocalControlListenerRegistry(
	control *LocalControlServer,
	config LocalControlListenerRegistryConfig,
) (*LocalControlListenerRegistry, error) {
	if control == nil || control.gateway == nil {
		return nil, errors.New("local control server is not initialized")
	}
	if config.OpenListener == nil {
		return nil, errors.New("trusted local control listener opener is required")
	}
	if config.SocketGID < 0 {
		return nil, errors.New("local control socket GID must be nonnegative")
	}
	if err := validateLocalControlRegistryRoot(config.RootDir, config.SocketGID); err != nil {
		return nil, err
	}
	return &LocalControlListenerRegistry{
		control:   control,
		config:    config,
		listeners: make(map[string]*localControlListenerEntry),
	}, nil
}

// LocalControlSocketPath returns the only accepted socket path for a PAID.
func LocalControlSocketPath(rootDir, personalityAgentID string) (string, error) {
	if !filepath.IsAbs(rootDir) || filepath.Clean(rootDir) != rootDir {
		return "", errors.New("local control listener root must be an absolute clean path")
	}
	if err := ValidatePersonalityAgentID(personalityAgentID); err != nil {
		return "", err
	}
	child := filepath.Join(rootDir, personalityAgentID)
	path := filepath.Join(child, localControlRegistrySocketName)
	if filepath.Dir(path) != child || len(path) > localControlRegistryMaxSocketPath {
		return "", errors.New("canonical local control socket path is invalid or too long")
	}
	return path, nil
}

// EnsureLocalRuntime idempotently creates the PAID child and starts its bound
// listener. A listener that terminated unexpectedly is closed and recreated.
func (r *LocalControlListenerRegistry) EnsureLocalRuntime(personalityAgentID string) error {
	if r == nil {
		return errors.New("local control listener registry is not initialized")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return errors.New("local control listener registry is closed")
	}
	return r.ensureLocked(personalityAgentID)
}

func (r *LocalControlListenerRegistry) ensureLocked(personalityAgentID string) error {
	path, err := LocalControlSocketPath(r.config.RootDir, personalityAgentID)
	if err != nil {
		return err
	}
	if current, exists := r.listeners[personalityAgentID]; exists {
		select {
		case serveErr := <-current.done:
			delete(r.listeners, personalityAgentID)
			if closeErr := closeLocalControlListener(context.Background(), current); closeErr != nil {
				return errors.Join(serveErr, closeErr)
			}
		default:
			return nil
		}
	}
	if err := validateLocalControlRegistryRoot(r.config.RootDir, r.config.SocketGID); err != nil {
		return err
	}
	if err := ensureLocalControlRegistryChild(
		r.config.RootDir,
		r.config.SocketGID,
		personalityAgentID,
	); err != nil {
		return err
	}
	handler, err := r.control.HandlerForLocalRuntime(personalityAgentID)
	if err != nil {
		return err
	}
	listener, err := r.config.OpenListener(path, r.config.SocketGID, personalityAgentID)
	if err != nil {
		return fmt.Errorf("open PAID-bound local control listener: %w", err)
	}
	entry := &localControlListenerEntry{
		path:     path,
		listener: listener,
		server:   &http.Server{Handler: handler},
		done:     make(chan error, 1),
	}
	r.listeners[personalityAgentID] = entry
	go func() {
		err := entry.server.Serve(entry.listener)
		if errors.Is(err, http.ErrServerClosed) || errors.Is(err, net.ErrClosed) {
			err = nil
		}
		entry.done <- err
	}()
	return nil
}

// CloseLocalRuntime idempotently closes one PAID listener.
func (r *LocalControlListenerRegistry) CloseLocalRuntime(
	ctx context.Context,
	personalityAgentID string,
) error {
	if r == nil {
		return errors.New("local control listener registry is not initialized")
	}
	if err := ValidatePersonalityAgentID(personalityAgentID); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, exists := r.listeners[personalityAgentID]
	if !exists {
		return nil
	}
	delete(r.listeners, personalityAgentID)
	return closeLocalControlListener(ctx, entry)
}

// ReconcileLocalRuntimes makes the active listeners exactly match the desired
// PAIDs. All additions succeed before obsolete listeners are closed; failed
// additions are rolled back without disturbing previously active listeners.
func (r *LocalControlListenerRegistry) ReconcileLocalRuntimes(
	ctx context.Context,
	personalityAgentIDs []string,
) error {
	if r == nil {
		return errors.New("local control listener registry is not initialized")
	}
	desired := make(map[string]struct{}, len(personalityAgentIDs))
	ordered := make([]string, 0, len(personalityAgentIDs))
	for _, personalityAgentID := range personalityAgentIDs {
		if err := ValidatePersonalityAgentID(personalityAgentID); err != nil {
			return err
		}
		if _, duplicate := desired[personalityAgentID]; duplicate {
			continue
		}
		desired[personalityAgentID] = struct{}{}
		ordered = append(ordered, personalityAgentID)
	}
	sort.Strings(ordered)

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return errors.New("local control listener registry is closed")
	}
	created := make([]string, 0, len(ordered))
	for _, personalityAgentID := range ordered {
		before := r.listeners[personalityAgentID]
		if err := r.ensureLocked(personalityAgentID); err != nil {
			for _, added := range created {
				entry := r.listeners[added]
				delete(r.listeners, added)
				_ = closeLocalControlListener(context.Background(), entry)
			}
			return err
		}
		if r.listeners[personalityAgentID] != before {
			created = append(created, personalityAgentID)
		}
	}
	var closeErr error
	for personalityAgentID, entry := range r.listeners {
		if _, keep := desired[personalityAgentID]; keep {
			continue
		}
		delete(r.listeners, personalityAgentID)
		closeErr = errors.Join(closeErr, closeLocalControlListener(ctx, entry))
	}
	return closeErr
}

// Close idempotently terminates the registry and every active listener.
func (r *LocalControlListenerRegistry) Close(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	var closeErr error
	for personalityAgentID, entry := range r.listeners {
		delete(r.listeners, personalityAgentID)
		closeErr = errors.Join(closeErr, closeLocalControlListener(ctx, entry))
	}
	return closeErr
}

func closeLocalControlListener(ctx context.Context, entry *localControlListenerEntry) error {
	if entry == nil {
		return nil
	}
	shutdownErr := entry.server.Shutdown(ctx)
	if shutdownErr != nil {
		shutdownErr = errors.Join(shutdownErr, entry.server.Close())
	}
	listenerErr := entry.listener.Close()
	if errors.Is(listenerErr, net.ErrClosed) || errors.Is(listenerErr, os.ErrClosed) {
		listenerErr = nil
	}
	var serveErr error
	select {
	case serveErr = <-entry.done:
	default:
	}
	return errors.Join(shutdownErr, listenerErr, serveErr)
}

func validateLocalControlRegistryRoot(rootDir string, socketGID int) error {
	if !filepath.IsAbs(rootDir) || filepath.Clean(rootDir) != rootDir {
		return errors.New("local control listener root must be an absolute clean path")
	}
	info, err := os.Lstat(rootDir)
	if err != nil {
		return fmt.Errorf("inspect local control listener root: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("local control listener root must be a real directory")
	}
	resolved, err := filepath.EvalSymlinks(rootDir)
	if err != nil {
		return fmt.Errorf("resolve local control listener root: %w", err)
	}
	if resolved != rootDir {
		return errors.New("local control listener root path must not contain symlinks")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Geteuid() || int(stat.Gid) != socketGID {
		return errors.New("local control listener root owner or group is not trusted")
	}
	if info.Mode().Perm() != localControlRegistryDirectoryMode {
		return fmt.Errorf(
			"local control listener root mode must be %04o",
			localControlRegistryDirectoryMode,
		)
	}
	return nil
}

func ensureLocalControlRegistryChild(rootDir string, socketGID int, personalityAgentID string) error {
	rootFD, err := unix.Open(rootDir, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return fmt.Errorf("pin local control listener root: %w", err)
	}
	defer unix.Close(rootFD)
	created := false
	if err := unix.Mkdirat(rootFD, personalityAgentID, localControlRegistryDirectoryMode); err != nil {
		if !errors.Is(err, unix.EEXIST) {
			return fmt.Errorf("create PAID local control directory: %w", err)
		}
	} else {
		created = true
	}
	childFD, err := unix.Openat(
		rootFD,
		personalityAgentID,
		unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW,
		0,
	)
	if err != nil {
		return fmt.Errorf("pin PAID local control directory: %w", err)
	}
	defer unix.Close(childFD)
	if created {
		if err := unix.Fchown(childFD, os.Geteuid(), socketGID); err != nil {
			return fmt.Errorf("set PAID local control directory owner: %w", err)
		}
		if err := unix.Fchmod(childFD, localControlRegistryDirectoryMode); err != nil {
			return fmt.Errorf("set PAID local control directory mode: %w", err)
		}
	}
	var stat unix.Stat_t
	if err := unix.Fstat(childFD, &stat); err != nil {
		return fmt.Errorf("inspect PAID local control directory: %w", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR ||
		int(stat.Uid) != os.Geteuid() ||
		int(stat.Gid) != socketGID ||
		os.FileMode(stat.Mode).Perm() != localControlRegistryDirectoryMode {
		return errors.New("PAID local control directory is not trusted")
	}
	childPath := filepath.Join(rootDir, personalityAgentID)
	resolved, err := filepath.EvalSymlinks(childPath)
	if err != nil {
		return fmt.Errorf("resolve PAID local control directory: %w", err)
	}
	if resolved != childPath {
		return errors.New("PAID local control directory path must not contain symlinks")
	}
	return nil
}
