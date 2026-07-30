package agentevents

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

const (
	browserSessionRevocationStateVersion  = uint64(1)
	maxBrowserSessionRevocationStateBytes = 512 << 10
	browserSessionRevocationLockID        = "browser-session-revocations"
)

type browserSessionRevocationState struct {
	Version uint64           `json:"version"`
	Entries map[string]int64 `json:"entries"`
}

// CheckBrowserSession checks the shared durable denylist before a signed
// cookie is accepted. Any storage or integrity failure rejects the cookie.
func (g *DurableGateway) CheckBrowserSession(
	ctx context.Context,
	sessionID string,
	expiresAt time.Time,
	now time.Time,
) error {
	if !validBrowserSessionID(sessionID) {
		return errors.New("invalid browser session revocation identity")
	}
	return g.withBrowserSessionRevocationLock(
		ctx,
		syscall.LOCK_SH,
		func() error {
			state, err := g.readBrowserSessionRevocations()
			if err != nil {
				return err
			}
			if revokedUntil, revoked := state.Entries[sessionID]; revoked {
				if revokedUntil != expiresAt.Unix() {
					return errors.New("browser session revocation expiry changed")
				}
				if now.Unix() < revokedUntil {
					return errBrowserSessionRevoked
				}
			}
			return nil
		},
	)
}

// AuthorizeBrowserSession keeps the shared revocation lock held across a
// security-sensitive operation. A successful logout therefore cannot race a
// command append or browser data write in another API process.
func (g *DurableGateway) AuthorizeBrowserSession(
	ctx context.Context,
	sessionID string,
	expiresAt time.Time,
	now time.Time,
	operation func() error,
) error {
	if operation == nil {
		return errors.New("browser session authorization operation is required")
	}
	if !validBrowserSessionID(sessionID) {
		return errors.New("invalid browser session revocation identity")
	}
	return g.withBrowserSessionRevocationLock(
		ctx,
		syscall.LOCK_SH,
		func() error {
			state, err := g.readBrowserSessionRevocations()
			if err != nil {
				return err
			}
			if revokedUntil, revoked := state.Entries[sessionID]; revoked {
				if revokedUntil != expiresAt.Unix() {
					return errors.New("browser session revocation expiry changed")
				}
				if now.Unix() < revokedUntil {
					return errBrowserSessionRevoked
				}
			}
			return operation()
		},
	)
}

// RevokeBrowserSession durably records one session until its signed expiry.
// Expired entries are reclaimed only while holding the exclusive shared lock;
// both the scan and retained set are bounded by maxRevokedSessions.
func (g *DurableGateway) RevokeBrowserSession(
	ctx context.Context,
	sessionID string,
	expiresAt time.Time,
	now time.Time,
) error {
	return g.persistBrowserSessionRevocation(
		ctx,
		sessionID,
		expiresAt,
		now,
		true,
	)
}

// RetireBrowserSession atomically consumes a live session for replacement.
// Replaying an existing revocation is deliberately rejected here while normal
// logout remains idempotent through RevokeBrowserSession.
func (g *DurableGateway) RetireBrowserSession(
	ctx context.Context,
	sessionID string,
	expiresAt time.Time,
	now time.Time,
) error {
	return g.persistBrowserSessionRevocation(
		ctx,
		sessionID,
		expiresAt,
		now,
		false,
	)
}

func (g *DurableGateway) persistBrowserSessionRevocation(
	ctx context.Context,
	sessionID string,
	expiresAt time.Time,
	now time.Time,
	idempotent bool,
) error {
	if !validBrowserSessionID(sessionID) {
		return errors.New("invalid browser session revocation identity")
	}
	if !now.Before(expiresAt) ||
		expiresAt.Sub(now) > maxBrowserSessionTTL {
		return errors.New("invalid browser session revocation expiry")
	}
	return g.withBrowserSessionRevocationLock(
		ctx,
		syscall.LOCK_EX,
		func() error {
			state, err := g.readBrowserSessionRevocations()
			if err != nil {
				return err
			}
			nowUnix := now.Unix()
			for revokedSessionID, revokedUntil := range state.Entries {
				if nowUnix >= revokedUntil {
					delete(state.Entries, revokedSessionID)
				}
			}
			if revokedUntil, exists := state.Entries[sessionID]; exists {
				if revokedUntil != expiresAt.Unix() {
					return errors.New("browser session revocation expiry changed")
				}
				if idempotent {
					return nil
				}
				return errBrowserSessionRetired
			}
			if len(state.Entries) >= g.maxBrowserSessionRevocations() {
				return errRevocationCapacity
			}
			state.Entries[sessionID] = expiresAt.Unix()
			raw, err := json.Marshal(state)
			if err != nil {
				return fmt.Errorf("marshal browser session revocations: %w", err)
			}
			if len(raw) > maxBrowserSessionRevocationStateBytes {
				return errors.New("browser session revocation state exceeds maximum allowed size")
			}
			if err := g.writeAtomic(g.browserSessionRevocationPath(), raw, 0o600); err != nil {
				return fmt.Errorf("persist browser session revocation: %w", err)
			}
			return nil
		},
	)
}

func (g *DurableGateway) withBrowserSessionRevocationLock(
	ctx context.Context,
	mode int,
	operation func() error,
) error {
	if g == nil {
		return errors.New("browser session revocation store is unavailable")
	}
	if operation == nil {
		return errors.New("browser session revocation operation is required")
	}
	lock, err := g.openRuntimeLock(browserSessionRevocationLockID)
	if err != nil {
		return fmt.Errorf("open browser session revocation lock: %w", err)
	}
	defer lock.Close()
	if err := flockContext(ctx, lock.Fd(), mode); err != nil {
		return fmt.Errorf("lock browser session revocations: %w", err)
	}
	defer unlockDurableFile(lock)
	return operation()
}

func (g *DurableGateway) readBrowserSessionRevocations() (browserSessionRevocationState, error) {
	state := browserSessionRevocationState{
		Version: browserSessionRevocationStateVersion,
		Entries: make(map[string]int64),
	}
	file, err := os.OpenFile(
		g.browserSessionRevocationPath(),
		os.O_RDONLY|syscall.O_NOFOLLOW,
		0,
	)
	if errors.Is(err, os.ErrNotExist) {
		return state, nil
	}
	if err != nil {
		return browserSessionRevocationState{}, fmt.Errorf(
			"open browser session revocation state: %w",
			err,
		)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return browserSessionRevocationState{}, fmt.Errorf(
			"inspect browser session revocation state: %w",
			err,
		)
	}
	_, stat, err := fileIdentity(info)
	if err != nil {
		return browserSessionRevocationState{}, err
	}
	if !info.Mode().IsRegular() ||
		info.Mode().Perm() != 0o600 ||
		stat.Uid != uint32(os.Geteuid()) ||
		stat.Nlink != 1 {
		return browserSessionRevocationState{}, errors.New(
			"browser session revocation state must be a private, singly linked regular file owned by the API user",
		)
	}
	raw, err := io.ReadAll(io.LimitReader(
		file,
		maxBrowserSessionRevocationStateBytes+1,
	))
	if err != nil {
		return browserSessionRevocationState{}, fmt.Errorf(
			"read browser session revocation state: %w",
			err,
		)
	}
	if len(raw) > maxBrowserSessionRevocationStateBytes {
		return browserSessionRevocationState{}, errors.New(
			"browser session revocation state exceeds maximum allowed size",
		)
	}
	if err := checkDuplicateKeys(raw); err != nil {
		return browserSessionRevocationState{}, fmt.Errorf(
			"browser session revocation state: %w",
			err,
		)
	}
	if err := unmarshalStrict(raw, &state); err != nil {
		return browserSessionRevocationState{}, fmt.Errorf(
			"decode browser session revocation state: %w",
			err,
		)
	}
	if state.Version != browserSessionRevocationStateVersion ||
		state.Entries == nil ||
		len(state.Entries) > maxRevokedSessions {
		return browserSessionRevocationState{}, errors.New(
			"invalid browser session revocation state",
		)
	}
	for sessionID, expiresAt := range state.Entries {
		if !validBrowserSessionID(sessionID) || expiresAt <= 0 {
			return browserSessionRevocationState{}, errors.New(
				"invalid browser session revocation entry",
			)
		}
	}
	return state, nil
}

func (g *DurableGateway) maxBrowserSessionRevocations() int {
	if g.MaxBrowserSessionRevocations <= 0 ||
		g.MaxBrowserSessionRevocations > maxRevokedSessions {
		return maxRevokedSessions
	}
	return g.MaxBrowserSessionRevocations
}

func (g *DurableGateway) browserSessionRevocationPath() string {
	return filepath.Join(g.dir, "browser-session-revocations.json")
}
