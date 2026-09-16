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
	browserSessionRevocationStateVersion  = uint64(3)
	browserSessionRevocationStateVersion2 = uint64(2)
	maxBrowserSessionRevocationStateBytes = 2 << 20
	browserSessionRevocationLockID        = "browser-session-revocations"
	// browserFlowClosureHorizon bounds how long a closed flow or browser epoch
	// must keep refusing session issuance. It exceeds the longest possible
	// issuance path for a flow: the maximum flow TTL plus the completion
	// replay window plus clock skew.
	browserFlowClosureHorizon = 40 * time.Minute
)

type browserSessionMutationKind string

const (
	browserSessionMutationRevoke browserSessionMutationKind = "revoke"
	browserSessionMutationRotate browserSessionMutationKind = "rotate"
)

// browserSessionLineageRecord is retained through the latest descendant
// expiry. Entries marks revoked SIDs; lineage records bind a rotated SID to
// its successor so logout carrying any still-valid ancestor revokes every
// live descendant. Flow, Epoch, and Human record the revocable authority the
// session was admitted under: the issuing auth flow, the browser epoch the
// cookie jar carried, and the Human the session authenticates.
type browserSessionLineageRecord struct {
	ExpiresAt   int64  `json:"expires_at"`
	RetainUntil int64  `json:"retain_until"`
	Parent      string `json:"parent,omitempty"`
	Successor   string `json:"successor,omitempty"`
	Flow        string `json:"flow,omitempty"`
	Epoch       string `json:"epoch,omitempty"`
	Human       string `json:"human,omitempty"`
}

type browserSessionRevocationState struct {
	Version  uint64                                 `json:"version"`
	Entries  map[string]int64                       `json:"entries"`
	Lineages map[string]browserSessionLineageRecord `json:"lineages"`
	// ClosedFlows maps a closed auth flow to the time its refusal may be
	// dropped. A flow closed by logout, discard, or a cross-Human switch can
	// never again issue or keep a session.
	ClosedFlows map[string]int64 `json:"closed_flows,omitempty"`
	// ClosedEpochs maps a closed browser epoch to its refusal horizon. Any
	// session admission carrying or bound to the epoch is refused, which
	// covers flows proved after logout's enumeration of them.
	ClosedEpochs map[string]int64 `json:"closed_epochs,omitempty"`
	// EpochSessions names the latest admitted live session of each browser
	// epoch. It is the commit-time identity witness when a request's own
	// cookie is absent or already stale.
	EpochSessions map[string]string `json:"epoch_sessions,omitempty"`
}

func newBrowserSessionRevocationState() browserSessionRevocationState {
	return browserSessionRevocationState{
		Version:       browserSessionRevocationStateVersion,
		Entries:       make(map[string]int64),
		Lineages:      make(map[string]browserSessionLineageRecord),
		ClosedFlows:   make(map[string]int64),
		ClosedEpochs:  make(map[string]int64),
		EpochSessions: make(map[string]string),
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

// BrowserSessionAdmission describes one session issuance decision. The store
// serializes it: inside one exclusive lock it proves the flow and epoch are
// open, compares the request's authority to the browser's current Human, and
// binds the successor lineage before the token may be signed.
type BrowserSessionAdmission struct {
	// Presented is a locally verified session cookie, or nil when the request
	// carried none or the cookie failed local checks.
	Presented   *BrowserSessionIdentity
	PresentedBy string
	Successor   BrowserSessionIdentity
	SuccessorBy string
	// FlowID binds the successor to the auth flow that issued it; "" marks a
	// non-flow issuance. FlowEpoch is the epoch hash recorded on that flow.
	FlowID    string
	FlowEpoch string
	// Epoch is the hash of the browser epoch the request carried (or the one
	// freshly minted for this response).
	Epoch string
	// SwitchFrom is the Human the person explicitly chose to replace. It must
	// equal the browser's current authority, never just the request's cookie.
	SwitchFrom string
}

// BrowserSessionAdmissionOutcome reports what an admission retired or closed
// so the caller can drop live connections and mirror flow closure.
type BrowserSessionAdmissionOutcome struct {
	RetiredSessionIDs []string
	ClosedFlowIDs     []string
}

var (
	errBrowserFlowClosed  = errors.New("browser auth flow is closed")
	errBrowserEpochClosed = errors.New("browser epoch is closed")
)

// AdmitBrowserSession is the single linearization point for session issuance.
// The successor is registered before it can be signed, so a logout or flow
// closure that commits first always refuses or revokes it. The browser's
// current authority is the live epoch session — the most recently admitted
// session for this jar — falling back to the presented cookie; a different
// successor Human requires SwitchFrom to name that authority.
func (g *DurableGateway) AdmitBrowserSession(
	ctx context.Context,
	admission BrowserSessionAdmission,
	now time.Time,
) (BrowserSessionAdmissionOutcome, error) {
	if err := validateBrowserSessionMutation(
		admission.Successor.ID,
		admission.Successor.ExpiresAt,
		now,
	); err != nil {
		return BrowserSessionAdmissionOutcome{}, fmt.Errorf("successor: %w", err)
	}
	if !provenanceIDRegexp.MatchString(admission.SuccessorBy) {
		return BrowserSessionAdmissionOutcome{}, errors.New("browser session admission has invalid successor human")
	}
	if admission.FlowID != "" && !validAuthFlowID(admission.FlowID) {
		return BrowserSessionAdmissionOutcome{}, errors.New("browser session admission has invalid flow")
	}
	for _, epoch := range []string{admission.Epoch, admission.FlowEpoch} {
		if epoch != "" && !validBrowserEpochHash(epoch) {
			return BrowserSessionAdmissionOutcome{}, errors.New("browser session admission has invalid epoch")
		}
	}
	if admission.SwitchFrom != "" && !provenanceIDRegexp.MatchString(admission.SwitchFrom) {
		admission.SwitchFrom = ""
	}
	var outcome BrowserSessionAdmissionOutcome
	err := g.withBrowserSessionRevocationLock(
		ctx,
		syscall.LOCK_EX,
		func() error {
			g.runBrowserSessionMutationHook(browserSessionMutationRotate)
			state, err := g.readBrowserSessionRevocations()
			if err != nil {
				return err
			}
			cleanupBrowserSessionRevocations(&state, now.Unix())
			outcome, err = admitBrowserSessionState(&state, admission, now, g.maxBrowserSessionRevocations())
			if err != nil {
				return err
			}
			return g.writeBrowserSessionRevocations(state)
		},
	)
	if err != nil {
		return BrowserSessionAdmissionOutcome{}, err
	}
	return outcome, nil
}

func admitBrowserSessionState(
	state *browserSessionRevocationState,
	admission BrowserSessionAdmission,
	now time.Time,
	maxEntries int,
) (BrowserSessionAdmissionOutcome, error) {
	var outcome BrowserSessionAdmissionOutcome
	nowUnix := now.Unix()

	presentedLive := false
	if admission.Presented != nil {
		err := checkBrowserSessionState(
			*state,
			admission.Presented.ID,
			admission.Presented.ExpiresAt.Unix(),
			nowUnix,
		)
		switch {
		case errors.Is(err, errBrowserSessionRevoked):
			// A locally valid but durably retired cookie is consumed
			// authority — a lost logout or rotation response leaves exactly
			// this in the jar. It is non-authoritative, like an absent
			// cookie: it cannot mint, displace, or stand in for the jar's
			// current Human, and the epoch's live tip is still evaluated
			// independently below. Locally invalid cookies never reach here.
		case err != nil:
			// A stored record contradicting the presented credential is an
			// integrity violation and keeps failing closed.
			return outcome, fmt.Errorf("presented session: %w", err)
		default:
			if lineage, exists := state.Lineages[admission.Presented.ID]; !exists || lineage.Successor == "" {
				presentedLive = true
			}
		}
	}

	// The epoch's latest admitted session is the browser's current authority
	// when it is still live; the presented cookie is only a fallback witness.
	authority := ""
	tipID := ""
	if admission.Epoch != "" {
		if tip := liveEpochSessionTip(state, admission.Epoch, nowUnix); tip != "" {
			tipID = tip
			authority = state.Lineages[tip].Human
		}
	}
	if authority == "" && presentedLive {
		authority = admission.PresentedBy
	}

	if admission.FlowID != "" {
		if _, closed := state.ClosedFlows[admission.FlowID]; closed {
			return outcome, errBrowserFlowClosed
		}
	}
	for _, epoch := range []string{admission.FlowEpoch, admission.Epoch} {
		if epoch != "" {
			if _, closed := state.ClosedEpochs[epoch]; closed {
				return outcome, errBrowserFlowClosed
			}
		}
	}

	crossHuman := authority != "" && authority != admission.SuccessorBy
	if crossHuman && admission.SwitchFrom != authority {
		return outcome, ErrBrowserSessionActive
	}

	if _, exists := state.Entries[admission.Successor.ID]; exists {
		return outcome, errors.New("browser session successor identity already exists")
	}
	if _, exists := state.Lineages[admission.Successor.ID]; exists {
		return outcome, errors.New("browser session successor identity already exists")
	}

	// Retire every other live session of this jar: the rotated presented
	// cookie and, when it differs, the epoch's live tip.
	retired := make(map[string]struct{})
	retire := func(sessionID string, expiresAt int64) error {
		if err := revokeBrowserSessionState(
			state,
			sessionID,
			expiresAt,
			maxEntries,
		); err != nil {
			return err
		}
		retired[sessionID] = struct{}{}
		return nil
	}
	// The successor's lineage parent is the live presented cookie, or — when
	// the request's cookie was absent or already consumed — the epoch tip
	// this admission displaces, so a later logout can still walk the jar's
	// real session chain.
	parentID := ""
	if presentedLive {
		if err := retire(admission.Presented.ID, admission.Presented.ExpiresAt.Unix()); err != nil {
			return outcome, err
		}
		parentID = admission.Presented.ID
	}
	if tipID != "" && tipID != admission.PresentedID() {
		if err := retire(tipID, state.Lineages[tipID].ExpiresAt); err != nil {
			return outcome, err
		}
		if parentID == "" {
			parentID = tipID
		}
	}

	if crossHuman {
		// A deliberate account switch cancels the retired Human's pending and
		// completed flow authority so its late completions cannot reissue.
		for _, flowID := range lineageFlowsOf(state, retired) {
			outcome.ClosedFlowIDs = append(outcome.ClosedFlowIDs, flowID)
			closeBrowserFlowState(state, flowID, nowUnix+int64(browserFlowClosureHorizon/time.Second))
		}
	}

	state.Lineages[admission.Successor.ID] = browserSessionLineageRecord{
		ExpiresAt:   admission.Successor.ExpiresAt.Unix(),
		RetainUntil: admission.Successor.ExpiresAt.Unix(),
		Flow:        admission.FlowID,
		Epoch:       admission.Epoch,
		Human:       admission.SuccessorBy,
	}
	if parentID != "" {
		successor := state.Lineages[admission.Successor.ID]
		successor.Parent = parentID
		state.Lineages[admission.Successor.ID] = successor
		current, exists := state.Lineages[parentID]
		if !exists {
			// A session issued before lineage tracking still rotates into a
			// fresh record so logout through its cookie revokes the successor.
			// Only the presented cookie can reach this branch — a live epoch
			// tip always has a lineage record.
			current = browserSessionLineageRecord{
				ExpiresAt:   admission.Presented.ExpiresAt.Unix(),
				RetainUntil: admission.Presented.ExpiresAt.Unix(),
			}
		}
		current.Successor = admission.Successor.ID
		if admission.Successor.ExpiresAt.Unix() > current.RetainUntil {
			current.RetainUntil = admission.Successor.ExpiresAt.Unix()
		}
		state.Lineages[parentID] = current
		for ancestorID := current.Parent; ancestorID != ""; {
			ancestor, exists := state.Lineages[ancestorID]
			if !exists {
				return outcome, errors.New("browser session lineage parent is missing")
			}
			if successor.ExpiresAt > ancestor.RetainUntil {
				ancestor.RetainUntil = successor.ExpiresAt
				state.Lineages[ancestorID] = ancestor
			}
			ancestorID = ancestor.Parent
		}
	}
	if admission.Epoch != "" {
		state.EpochSessions[admission.Epoch] = admission.Successor.ID
	}
	for sessionID := range retired {
		outcome.RetiredSessionIDs = append(outcome.RetiredSessionIDs, sessionID)
	}
	if browserSessionStateCount(*state) > maxEntries {
		return outcome, errRevocationCapacity
	}
	return outcome, nil
}

// PresentedID is the presented session's identifier, or "" when absent.
func (a BrowserSessionAdmission) PresentedID() string {
	if a.Presented == nil {
		return ""
	}
	return a.Presented.ID
}

// liveEpochSessionTip follows an epoch's latest admitted session through any
// successor chain and returns its SID while it is still live, "" otherwise.
func liveEpochSessionTip(
	state *browserSessionRevocationState,
	epoch string,
	now int64,
) string {
	sessionID, exists := state.EpochSessions[epoch]
	if !exists {
		return ""
	}
	visited := make(map[string]struct{})
	for sessionID != "" {
		if _, seen := visited[sessionID]; seen {
			return ""
		}
		visited[sessionID] = struct{}{}
		lineage, exists := state.Lineages[sessionID]
		if !exists {
			return ""
		}
		if lineage.Successor != "" {
			sessionID = lineage.Successor
			continue
		}
		if _, revoked := state.Entries[sessionID]; revoked || now >= lineage.ExpiresAt {
			return ""
		}
		return sessionID
	}
	return ""
}

// lineageFlowsOf collects the issuing flows of every retired session's
// lineage, walking ancestors so an old flow that minted a rotated-away
// session is still closed.
func lineageFlowsOf(
	state *browserSessionRevocationState,
	retired map[string]struct{},
) []string {
	seen := make(map[string]struct{})
	var flows []string
	collect := func(sessionID string) {
		visited := make(map[string]struct{})
		for current := sessionID; current != ""; {
			if _, seen := visited[current]; seen {
				return
			}
			visited[current] = struct{}{}
			lineage, exists := state.Lineages[current]
			if !exists {
				return
			}
			if lineage.Flow != "" {
				if _, dup := seen[lineage.Flow]; !dup {
					seen[lineage.Flow] = struct{}{}
					flows = append(flows, lineage.Flow)
				}
			}
			current = lineage.Parent
		}
	}
	for sessionID := range retired {
		collect(sessionID)
	}
	return flows
}

func closeBrowserFlowState(state *browserSessionRevocationState, flowID string, retainUntil int64) {
	if existing, exists := state.ClosedFlows[flowID]; !exists || existing < retainUntil {
		state.ClosedFlows[flowID] = retainUntil
	}
}

// CloseBrowserSessionsForLogout is the exclusive-lock logout boundary. It
// revokes every presented session's lineage and every epoch's live tip,
// closes every epoch this jar has used — the presented one plus each epoch
// recorded on a retired lineage, which covers a superseded epoch cookie —
// and closes the issuing flows of everything it revokes plus the
// nonce-verified pending flows the browser listed. All of it commits in one
// store write.
func (g *DurableGateway) CloseBrowserSessionsForLogout(
	ctx context.Context,
	presented []BrowserSessionIdentity,
	epochs []string,
	extraFlows map[string]int64,
	now time.Time,
) (closedFlows []string, closedEpochs []string, retiredSessions []string, err error) {
	for _, session := range presented {
		if !validBrowserSessionID(session.ID) {
			return nil, nil, nil, errors.New("invalid browser session revocation identity")
		}
	}
	for _, epoch := range epochs {
		if epoch == "" || !validBrowserEpochHash(epoch) {
			return nil, nil, nil, errors.New("invalid browser epoch")
		}
	}
	for flowID, retain := range extraFlows {
		if !validAuthFlowID(flowID) || retain <= 0 {
			return nil, nil, nil, errors.New("invalid browser logout flow closure")
		}
	}
	err = g.withBrowserSessionRevocationLock(
		ctx,
		syscall.LOCK_EX,
		func() error {
			g.runBrowserSessionMutationHook(browserSessionMutationRevoke)
			state, err := g.readBrowserSessionRevocations()
			if err != nil {
				return err
			}
			cleanupBrowserSessionRevocations(&state, now.Unix())
			closedFlows, closedEpochs, retiredSessions, err =
				closeBrowserSessionsForLogoutState(
					&state, presented, epochs, extraFlows, now, g.maxBrowserSessionRevocations())
			if err != nil {
				return err
			}
			return g.writeBrowserSessionRevocations(state)
		},
	)
	if err != nil {
		return nil, nil, nil, err
	}
	return closedFlows, closedEpochs, retiredSessions, nil
}

func closeBrowserSessionsForLogoutState(
	state *browserSessionRevocationState,
	presented []BrowserSessionIdentity,
	epochs []string,
	extraFlows map[string]int64,
	now time.Time,
	maxEntries int,
) (closedFlows []string, closedEpochs []string, retiredSessions []string, err error) {
	retired := make(map[string]struct{})
	retire := func(sessionID string, expiresAt int64) error {
		if _, done := retired[sessionID]; done {
			return nil
		}
		if err := revokeBrowserSessionState(
			state,
			sessionID,
			expiresAt,
			maxEntries,
		); err != nil {
			return err
		}
		retired[sessionID] = struct{}{}
		return nil
	}
	presentedLive := false
	for _, session := range presented {
		expiresAt := session.ExpiresAt.Unix()
		if lineage, exists := state.Lineages[session.ID]; exists &&
			lineage.ExpiresAt != expiresAt {
			return nil, nil, nil, errors.New("browser session lineage expiry changed")
		}
		if _, retiredAlready := state.Entries[session.ID]; !retiredAlready &&
			now.Unix() < expiresAt {
			presentedLive = true
		}
		if err := retire(session.ID, expiresAt); err != nil {
			return nil, nil, nil, err
		}
	}
	horizon := now.Unix() + int64(browserFlowClosureHorizon/time.Second)
	closing := make(map[string]struct{})
	for _, epoch := range epochs {
		closing[epoch] = struct{}{}
		// Retiring the epoch's live tip requires at least one live presented
		// session: a request carrying only durably retired cookies may still
		// revoke its own lineage above, but a live session it cannot connect
		// to that lineage belongs to a later deliberate choice.
		if !presentedLive {
			continue
		}
		if tip := liveEpochSessionTip(state, epoch, now.Unix()); tip != "" {
			if err := retire(tip, state.Lineages[tip].ExpiresAt); err != nil {
				return nil, nil, nil, err
			}
		}
	}
	// Every epoch that ever named one of this jar's sessions is part of the
	// logout scope, so a flow bound to a superseded cookie value cannot keep
	// issuance authority.
	for sessionID := range retired {
		for epoch := range lineageEpochsOf(state, sessionID) {
			closing[epoch] = struct{}{}
		}
	}
	for epoch, sessionID := range state.EpochSessions {
		if epochReachesRetired(state, sessionID, retired) {
			closing[epoch] = struct{}{}
		}
	}
	for epoch := range closing {
		if existing, exists := state.ClosedEpochs[epoch]; !exists || existing < horizon {
			state.ClosedEpochs[epoch] = horizon
		}
		closedEpochs = append(closedEpochs, epoch)
	}
	closed := make(map[string]struct{})
	for _, flowID := range lineageFlowsOf(state, retired) {
		closeBrowserFlowState(state, flowID, now.Unix()+int64(browserFlowClosureHorizon/time.Second))
		closed[flowID] = struct{}{}
	}
	for flowID, retain := range extraFlows {
		closeBrowserFlowState(state, flowID, retain)
		closed[flowID] = struct{}{}
	}
	for flowID := range closed {
		closedFlows = append(closedFlows, flowID)
	}
	for sessionID := range retired {
		retiredSessions = append(retiredSessions, sessionID)
	}
	return closedFlows, closedEpochs, retiredSessions, nil
}

// lineageEpochsOf collects the epochs recorded on a session's whole lineage,
// walking ancestors and descendants so every cookie value this jar's session
// history used is covered.
func lineageEpochsOf(
	state *browserSessionRevocationState,
	sessionID string,
) map[string]struct{} {
	epochs := make(map[string]struct{})
	visited := make(map[string]struct{})
	var walk func(id string, forward bool)
	walk = func(id string, forward bool) {
		for id != "" {
			if _, seen := visited[id]; seen {
				return
			}
			visited[id] = struct{}{}
			lineage, exists := state.Lineages[id]
			if !exists {
				return
			}
			if lineage.Epoch != "" {
				epochs[lineage.Epoch] = struct{}{}
			}
			if forward {
				id = lineage.Successor
			} else {
				id = lineage.Parent
			}
		}
	}
	walk(sessionID, true)
	walk(sessionID, false)
	return epochs
}

// epochReachesRetired reports whether the session an epoch last named is, or
// rotated into, a session this logout is retiring.
func epochReachesRetired(
	state *browserSessionRevocationState,
	sessionID string,
	retired map[string]struct{},
) bool {
	visited := make(map[string]struct{})
	for sessionID != "" {
		if _, seen := visited[sessionID]; seen {
			return false
		}
		visited[sessionID] = struct{}{}
		if _, hit := retired[sessionID]; hit {
			return true
		}
		lineage, exists := state.Lineages[sessionID]
		if !exists {
			return false
		}
		sessionID = lineage.Successor
	}
	return false
}

// CheckBrowserEpoch reports whether a browser epoch remains usable. A closed
// epoch stays refused for its horizon so a stale cookie cannot name new work.
func (g *DurableGateway) CheckBrowserEpoch(
	ctx context.Context,
	epochHash string,
	now time.Time,
) error {
	if !validBrowserEpochHash(epochHash) {
		return errors.New("invalid browser epoch")
	}
	return g.withBrowserSessionRevocationLock(
		ctx,
		syscall.LOCK_SH,
		func() error {
			state, err := g.readBrowserSessionRevocations()
			if err != nil {
				return err
			}
			cleanupBrowserSessionRevocations(&state, now.Unix())
			if _, closed := state.ClosedEpochs[epochHash]; closed {
				return errBrowserEpochClosed
			}
			return nil
		},
	)
}

// CloseBrowserFlow is the flow-scoped discard boundary. It closes exactly one
// flow and revokes the sessions that flow issued, without touching the
// browser's other sessions or flows.
func (g *DurableGateway) CloseBrowserFlow(
	ctx context.Context,
	flowID string,
	retainUntil time.Time,
	now time.Time,
) (retiredSessions []string, err error) {
	if !validAuthFlowID(flowID) {
		return nil, errors.New("invalid auth flow")
	}
	if !now.Before(retainUntil) {
		// An expired flow can never issue again; discarding it is a no-op.
		return nil, nil
	}
	err = g.withBrowserSessionRevocationLock(
		ctx,
		syscall.LOCK_EX,
		func() error {
			g.runBrowserSessionMutationHook(browserSessionMutationRevoke)
			state, err := g.readBrowserSessionRevocations()
			if err != nil {
				return err
			}
			cleanupBrowserSessionRevocations(&state, now.Unix())
			closeBrowserFlowState(&state, flowID, retainUntil.Unix())
			for sessionID, lineage := range state.Lineages {
				if lineage.Flow != flowID {
					continue
				}
				if _, revoked := state.Entries[sessionID]; revoked {
					continue
				}
				if err := revokeBrowserSessionState(
					&state,
					sessionID,
					lineage.ExpiresAt,
					g.maxBrowserSessionRevocations(),
				); err != nil {
					return err
				}
				retiredSessions = append(retiredSessions, sessionID)
			}
			return g.writeBrowserSessionRevocations(state)
		},
	)
	if err != nil {
		return nil, err
	}
	return retiredSessions, nil
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
	rootHuman := ""
	if lineage, exists := state.Lineages[sessionID]; exists {
		rootHuman = lineage.Human
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
		// A recorded Human change marks a deliberate account switch: the
		// descendant belongs to a later explicit choice that stale cleanup
		// through an ancestor cookie must not consume.
		if successor.Human != rootHuman {
			break
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
	for flowID, retainUntil := range state.ClosedFlows {
		if now >= retainUntil {
			delete(state.ClosedFlows, flowID)
		}
	}
	for epoch, retainUntil := range state.ClosedEpochs {
		if now >= retainUntil {
			delete(state.ClosedEpochs, epoch)
		}
	}
	for epoch, sessionID := range state.EpochSessions {
		if _, exists := state.Lineages[sessionID]; !exists {
			delete(state.EpochSessions, epoch)
		}
	}
}

func browserSessionStateCount(state browserSessionRevocationState) int {
	count := len(state.Lineages) + len(state.ClosedFlows) +
		len(state.ClosedEpochs) + len(state.EpochSessions)
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
	for flowID, retainUntil := range state.ClosedFlows {
		cloned.ClosedFlows[flowID] = retainUntil
	}
	for epoch, retainUntil := range state.ClosedEpochs {
		cloned.ClosedEpochs[epoch] = retainUntil
	}
	for epoch, sessionID := range state.EpochSessions {
		cloned.EpochSessions[epoch] = sessionID
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
		case "version", "entries", "lineages",
			"closed_flows", "closed_epochs", "epoch_sessions":
		default:
			return browserSessionRevocationState{}, errors.New(
				"invalid browser session revocation field",
			)
		}
	}
	_, hasLineages := encodedFields["lineages"]
	switch state.Version {
	case browserSessionRevocationStateVersion, browserSessionRevocationStateVersion2:
		if !hasLineages || state.Lineages == nil {
			return browserSessionRevocationState{}, errors.New(
				"invalid browser session revocation lineages",
			)
		}
	default:
		return browserSessionRevocationState{}, errors.New(
			"unsupported browser session revocation state version; delete the state file to reset it",
		)
	}
	// v2 states upgrade in memory; the next write persists v3. Unknown
	// versions above stay rejected rather than cleared.
	if state.ClosedFlows == nil {
		state.ClosedFlows = make(map[string]int64)
	}
	if state.ClosedEpochs == nil {
		state.ClosedEpochs = make(map[string]int64)
	}
	if state.EpochSessions == nil {
		state.EpochSessions = make(map[string]string)
	}
	state.Version = browserSessionRevocationStateVersion
	if err := validateBrowserSessionRevocationState(state); err != nil {
		return browserSessionRevocationState{}, err
	}
	return state, nil
}

func validateBrowserSessionRevocationState(
	state browserSessionRevocationState,
) error {
	if state.Version != browserSessionRevocationStateVersion ||
		state.Entries == nil ||
		state.Lineages == nil ||
		state.ClosedFlows == nil ||
		state.ClosedEpochs == nil ||
		state.EpochSessions == nil ||
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
			lineage.RetainUntil < lineage.ExpiresAt ||
			(lineage.Flow != "" && !validAuthFlowID(lineage.Flow)) ||
			(lineage.Epoch != "" && !validBrowserEpochHash(lineage.Epoch)) ||
			(lineage.Human != "" && !provenanceIDRegexp.MatchString(lineage.Human)) {
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
	for flowID, retainUntil := range state.ClosedFlows {
		if !validAuthFlowID(flowID) || retainUntil <= 0 {
			return errors.New("invalid browser closed flow entry")
		}
	}
	for epoch, retainUntil := range state.ClosedEpochs {
		if !validBrowserEpochHash(epoch) || retainUntil <= 0 {
			return errors.New("invalid browser closed epoch entry")
		}
	}
	for epoch, sessionID := range state.EpochSessions {
		if !validBrowserEpochHash(epoch) || !validBrowserSessionID(sessionID) {
			return errors.New("invalid browser epoch session entry")
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
