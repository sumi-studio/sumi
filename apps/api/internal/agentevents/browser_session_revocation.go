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
	legacyBrowserSessionRevocationStateVersion = uint64(1)
	browserSessionRevocationStateVersion       = uint64(2)
	maxBrowserSessionRevocationStateBytes      = 2 << 20
	browserSessionRevocationLockID             = "browser-session-revocations"
)

type browserSessionMutationKind string

const (
	browserSessionMutationRevoke browserSessionMutationKind = "revoke"
	browserSessionMutationRotate browserSessionMutationKind = "rotate"
)

// browserSessionLineageRecord is retained through the latest descendant
// expiry. Entries marks revoked SIDs; lineage records bind a rotated SID to
// its successor so logout carrying any still-valid ancestor revokes every
// live descendant.
type browserSessionLineageRecord struct {
	ExpiresAt   int64  `json:"expires_at"`
	RetainUntil int64  `json:"retain_until"`
	Parent      string `json:"parent,omitempty"`
	Successor   string `json:"successor,omitempty"`
}

type browserSessionRevocationState struct {
	Version  uint64                                 `json:"version"`
	Entries  map[string]int64                       `json:"entries"`
	Lineages map[string]browserSessionLineageRecord `json:"lineages"`
}

func newBrowserSessionRevocationState() browserSessionRevocationState {
	return browserSessionRevocationState{
		Version:  browserSessionRevocationStateVersion,
		Entries:  make(map[string]int64),
		Lineages: make(map[string]browserSessionLineageRecord),
	}
}

// CheckBrowserSession checks the shared durable denylist before a signed
// cookie is accepted. Any storage, lineage, or integrity failure rejects it.
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
			return checkBrowserSessionState(
				state,
				sessionID,
				expiresAt.Unix(),
				now.Unix(),
			)
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
			if err := checkBrowserSessionState(
				state,
				sessionID,
				expiresAt.Unix(),
				now.Unix(),
			); err != nil {
				return err
			}
			return operation()
		},
	)
}

// RevokeBrowserSession takes the exclusive lifecycle lock and durably revokes
// one session plus every live successor in its rotation lineage. The union of
// lineage members and standalone revocations remains store-capacity bounded;
// repeating logout is idempotent.
func (g *DurableGateway) RevokeBrowserSession(
	ctx context.Context,
	sessionID string,
	expiresAt time.Time,
	now time.Time,
) error {
	if err := validateBrowserSessionMutation(
		sessionID,
		expiresAt,
		now,
	); err != nil {
		return err
	}
	return g.withBrowserSessionRevocationLock(
		ctx,
		syscall.LOCK_EX,
		func() error {
			g.runBrowserSessionMutationHook(browserSessionMutationRevoke)
			state, err := g.readBrowserSessionRevocations()
			if err != nil {
				return err
			}
			cleanupBrowserSessionRevocations(&state, now.Unix())
			if err := revokeBrowserSessionState(
				&state,
				sessionID,
				expiresAt.Unix(),
				g.maxBrowserSessionRevocations(),
			); err != nil {
				return err
			}
			return g.writeBrowserSessionRevocations(state)
		},
	)
}

// RotateBrowserSession is the single shared-store linearization point for
// replacement. It revokes currentSessionID, records the live successor, and
// extends every ancestor's retention before the successor can be signed or
// returned. A previously revoked or already-rotated current SID is rejected.
func (g *DurableGateway) RotateBrowserSession(
	ctx context.Context,
	currentSessionID string,
	currentExpiresAt time.Time,
	successorSessionID string,
	successorExpiresAt time.Time,
	now time.Time,
) error {
	if err := validateBrowserSessionMutation(
		currentSessionID,
		currentExpiresAt,
		now,
	); err != nil {
		return err
	}
	if err := validateBrowserSessionMutation(
		successorSessionID,
		successorExpiresAt,
		now,
	); err != nil {
		return fmt.Errorf("successor: %w", err)
	}
	if currentSessionID == successorSessionID {
		return errors.New("browser session successor must be distinct")
	}
	return g.withBrowserSessionRevocationLock(
		ctx,
		syscall.LOCK_EX,
		func() error {
			g.runBrowserSessionMutationHook(browserSessionMutationRotate)
			state, err := g.readBrowserSessionRevocations()
			if err != nil {
				return err
			}
			cleanupBrowserSessionRevocations(&state, now.Unix())
			if err := rotateBrowserSessionState(
				&state,
				currentSessionID,
				currentExpiresAt.Unix(),
				successorSessionID,
				successorExpiresAt.Unix(),
				g.maxBrowserSessionRevocations(),
			); err != nil {
				return err
			}
			return g.writeBrowserSessionRevocations(state)
		},
	)
}

func validateBrowserSessionMutation(
	sessionID string,
	expiresAt time.Time,
	now time.Time,
) error {
	if !validBrowserSessionID(sessionID) {
		return errors.New("invalid browser session revocation identity")
	}
	if !now.Before(expiresAt) ||
		expiresAt.Sub(now) > maxBrowserSessionTTL {
		return errors.New("invalid browser session revocation expiry")
	}
	return nil
}

func checkBrowserSessionState(
	state browserSessionRevocationState,
	sessionID string,
	expiresAt int64,
	now int64,
) error {
	if lineage, exists := state.Lineages[sessionID]; exists &&
		lineage.ExpiresAt != expiresAt {
		return errors.New("browser session lineage expiry changed")
	}
	if revokedUntil, revoked := state.Entries[sessionID]; revoked {
		if revokedUntil != expiresAt {
			return errors.New("browser session revocation expiry changed")
		}
		if now < revokedUntil {
			return errBrowserSessionRevoked
		}
	}
	return nil
}

func revokeBrowserSessionState(
	state *browserSessionRevocationState,
	sessionID string,
	expiresAt int64,
	maxEntries int,
) error {
	if lineage, exists := state.Lineages[sessionID]; exists &&
		lineage.ExpiresAt != expiresAt {
		return errors.New("browser session lineage expiry changed")
	}
	currentID := sessionID
	currentExpiry := expiresAt
	visited := make(map[string]struct{})
	for {
		if _, exists := visited[currentID]; exists {
			return errors.New("browser session lineage contains a cycle")
		}
		visited[currentID] = struct{}{}
		if revokedUntil, exists := state.Entries[currentID]; exists &&
			revokedUntil != currentExpiry {
			return errors.New("browser session revocation expiry changed")
		}
		state.Entries[currentID] = currentExpiry
		lineage, exists := state.Lineages[currentID]
		if !exists || lineage.Successor == "" {
			break
		}
		successor, exists := state.Lineages[lineage.Successor]
		if !exists {
			return errors.New("browser session lineage successor is missing")
		}
		currentID = lineage.Successor
		currentExpiry = successor.ExpiresAt
		if len(visited) > maxEntries {
			return errRevocationCapacity
		}
	}
	if browserSessionStateCount(*state) > maxEntries {
		return errRevocationCapacity
	}
	return nil
}

func rotateBrowserSessionState(
	state *browserSessionRevocationState,
	currentSessionID string,
	currentExpiresAt int64,
	successorSessionID string,
	successorExpiresAt int64,
	maxEntries int,
) error {
	if revokedUntil, revoked := state.Entries[currentSessionID]; revoked {
		if revokedUntil != currentExpiresAt {
			return errors.New("browser session revocation expiry changed")
		}
		return errBrowserSessionRetired
	}
	current, exists := state.Lineages[currentSessionID]
	if exists {
		if current.ExpiresAt != currentExpiresAt {
			return errors.New("browser session lineage expiry changed")
		}
		if current.Successor != "" {
			return errBrowserSessionRetired
		}
	} else {
		current = browserSessionLineageRecord{
			ExpiresAt:   currentExpiresAt,
			RetainUntil: currentExpiresAt,
		}
	}
	if _, exists := state.Entries[successorSessionID]; exists {
		return errors.New("browser session successor identity already exists")
	}
	if _, exists := state.Lineages[successorSessionID]; exists {
		return errors.New("browser session successor identity already exists")
	}

	additional := 1
	if _, exists := state.Lineages[currentSessionID]; !exists {
		additional++
	}
	if browserSessionStateCount(*state)+additional > maxEntries {
		return errRevocationCapacity
	}
	current.Successor = successorSessionID
	if successorExpiresAt > current.RetainUntil {
		current.RetainUntil = successorExpiresAt
	}
	state.Lineages[currentSessionID] = current
	state.Lineages[successorSessionID] = browserSessionLineageRecord{
		ExpiresAt:   successorExpiresAt,
		RetainUntil: successorExpiresAt,
		Parent:      currentSessionID,
	}
	state.Entries[currentSessionID] = currentExpiresAt

	ancestorID := current.Parent
	for traversed := 0; ancestorID != ""; traversed++ {
		if traversed >= maxEntries {
			return errors.New("browser session lineage exceeds maximum depth")
		}
		ancestor, exists := state.Lineages[ancestorID]
		if !exists {
			return errors.New("browser session lineage parent is missing")
		}
		if successorExpiresAt > ancestor.RetainUntil {
			ancestor.RetainUntil = successorExpiresAt
			state.Lineages[ancestorID] = ancestor
		}
		ancestorID = ancestor.Parent
	}
	return nil
}

func cleanupBrowserSessionRevocations(
	state *browserSessionRevocationState,
	now int64,
) {
	removed := make(map[string]struct{})
	for sessionID, lineage := range state.Lineages {
		if now >= lineage.RetainUntil {
			delete(state.Lineages, sessionID)
			removed[sessionID] = struct{}{}
		}
	}
	for sessionID, lineage := range state.Lineages {
		if _, removedSuccessor := removed[lineage.Successor]; removedSuccessor {
			lineage.Successor = ""
			state.Lineages[sessionID] = lineage
		}
	}
	for sessionID, expiresAt := range state.Entries {
		lineage, retainedForSuccessors := state.Lineages[sessionID]
		if now >= expiresAt &&
			(!retainedForSuccessors || now >= lineage.RetainUntil) {
			delete(state.Entries, sessionID)
		}
	}
}

func browserSessionStateCount(state browserSessionRevocationState) int {
	count := len(state.Lineages)
	for sessionID := range state.Entries {
		if _, represented := state.Lineages[sessionID]; !represented {
			count++
		}
	}
	return count
}

func cloneBrowserSessionRevocationState(
	state browserSessionRevocationState,
) browserSessionRevocationState {
	cloned := newBrowserSessionRevocationState()
	for sessionID, expiresAt := range state.Entries {
		cloned.Entries[sessionID] = expiresAt
	}
	for sessionID, lineage := range state.Lineages {
		cloned.Lineages[sessionID] = lineage
	}
	return cloned
}

func (g *DurableGateway) runBrowserSessionMutationHook(
	kind browserSessionMutationKind,
) {
	if g.browserSessionMutationHook != nil {
		g.browserSessionMutationHook(kind)
	}
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
	if g.browserSessionLockAttemptHook != nil {
		g.browserSessionLockAttemptHook()
	}
	if err := flockContext(ctx, lock.Fd(), mode); err != nil {
		return fmt.Errorf("lock browser session revocations: %w", err)
	}
	defer unlockDurableFile(lock)
	return operation()
}

func (g *DurableGateway) readBrowserSessionRevocations() (browserSessionRevocationState, error) {
	state := browserSessionRevocationState{}
	file, err := os.OpenFile(
		g.browserSessionRevocationPath(),
		os.O_RDONLY|syscall.O_NOFOLLOW,
		0,
	)
	if errors.Is(err, os.ErrNotExist) {
		return newBrowserSessionRevocationState(), nil
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
	var encodedFields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &encodedFields); err != nil {
		return browserSessionRevocationState{}, fmt.Errorf(
			"inspect browser session revocation fields: %w",
			err,
		)
	}
	for field := range encodedFields {
		switch field {
		case "version", "entries", "lineages":
		default:
			return browserSessionRevocationState{}, errors.New(
				"invalid browser session revocation field",
			)
		}
	}
	_, hasLineages := encodedFields["lineages"]
	switch state.Version {
	case legacyBrowserSessionRevocationStateVersion:
		if hasLineages {
			return browserSessionRevocationState{}, errors.New(
				"invalid legacy browser session revocation state",
			)
		}
		if err := validateLegacyBrowserSessionRevocationState(state); err != nil {
			return browserSessionRevocationState{}, err
		}
		// The v2 cookie-signing domain rejects every credential represented
		// by this pre-lineage denylist. Start the new credential namespace
		// empty instead of pretending its unknowable successor graph can be
		// reconstructed; the next mutation persists the v2 format.
		state = newBrowserSessionRevocationState()
	case browserSessionRevocationStateVersion:
		if !hasLineages || state.Lineages == nil {
			return browserSessionRevocationState{}, errors.New(
				"invalid browser session revocation lineages",
			)
		}
	default:
		return browserSessionRevocationState{}, errors.New(
			"invalid browser session revocation state version",
		)
	}
	if err := validateBrowserSessionRevocationState(state); err != nil {
		return browserSessionRevocationState{}, err
	}
	return state, nil
}

func validateLegacyBrowserSessionRevocationState(
	state browserSessionRevocationState,
) error {
	if state.Version != legacyBrowserSessionRevocationStateVersion ||
		state.Entries == nil ||
		len(state.Entries) > maxRevokedSessions {
		return errors.New("invalid legacy browser session revocation state")
	}
	for sessionID, expiresAt := range state.Entries {
		if !validBrowserSessionID(sessionID) || expiresAt <= 0 {
			return errors.New("invalid legacy browser session revocation entry")
		}
	}
	return nil
}

func validateBrowserSessionRevocationState(
	state browserSessionRevocationState,
) error {
	if state.Version != browserSessionRevocationStateVersion ||
		state.Entries == nil ||
		state.Lineages == nil ||
		browserSessionStateCount(state) > maxRevokedSessions {
		return errors.New("invalid browser session revocation state")
	}
	for sessionID, expiresAt := range state.Entries {
		if !validBrowserSessionID(sessionID) || expiresAt <= 0 {
			return errors.New("invalid browser session revocation entry")
		}
		if lineage, exists := state.Lineages[sessionID]; exists &&
			lineage.ExpiresAt != expiresAt {
			return errors.New("browser session revocation and lineage expiry mismatch")
		}
	}
	for sessionID, lineage := range state.Lineages {
		if !validBrowserSessionID(sessionID) ||
			lineage.ExpiresAt <= 0 ||
			lineage.RetainUntil < lineage.ExpiresAt {
			return errors.New("invalid browser session lineage entry")
		}
		if lineage.Parent != "" {
			parent, exists := state.Lineages[lineage.Parent]
			if !validBrowserSessionID(lineage.Parent) ||
				!exists ||
				parent.Successor != sessionID ||
				parent.RetainUntil < lineage.RetainUntil {
				return errors.New("invalid browser session lineage parent")
			}
		}
		if lineage.Successor != "" {
			successor, exists := state.Lineages[lineage.Successor]
			_, currentRevoked := state.Entries[sessionID]
			if !validBrowserSessionID(lineage.Successor) ||
				!exists ||
				successor.Parent != sessionID ||
				!currentRevoked {
				return errors.New("invalid browser session lineage successor")
			}
		}
	}
	for sessionID := range state.Lineages {
		visited := make(map[string]struct{})
		currentID := sessionID
		for currentID != "" {
			if _, exists := visited[currentID]; exists {
				return errors.New("browser session lineage contains a cycle")
			}
			visited[currentID] = struct{}{}
			if len(visited) > len(state.Lineages) {
				return errors.New("browser session lineage exceeds bounded state")
			}
			currentID = state.Lineages[currentID].Successor
		}
	}
	return nil
}

func (g *DurableGateway) writeBrowserSessionRevocations(
	state browserSessionRevocationState,
) error {
	if err := validateBrowserSessionRevocationState(state); err != nil {
		return err
	}
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
