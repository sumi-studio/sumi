package main

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
)

const (
	authFlowTTL     = 15 * time.Minute
	recentReauthAge = 5 * time.Minute
	// providerUnlinkSagaTimeout bounds how long one unlink run waits on its
	// Firebase Admin calls. It bounds our waiting only: Identity Toolkit
	// mutations are applied synchronously inside request handling, but a
	// cancelled or timed-out request does not prove the remote server stopped
	// its work. Late-commit ambiguity is handled by the reconcile path, not
	// by this deadline.
	providerUnlinkSagaTimeout = 30 * time.Second
)

type kosekiAuthFlowController struct {
	store     *koseki.Store
	tenantID  string
	providers firebaseProviderLifecycle
	email     *emailCodeController
	clock     func() time.Time
}

type firebaseProviderAccount struct {
	UID              string
	ProviderSubjects map[string]string
	EmailProvider    bool
	// EmailVerified means Firebase resolves the verified address to this UID,
	// so a Sumi email-code proof can sign in again without a password provider.
	EmailVerified bool
}

type firebaseProviderLifecycle interface {
	ProviderAccount(ctx context.Context, firebaseUID string) (firebaseProviderAccount, error)
	DeleteProvider(ctx context.Context, firebaseUID, provider string) error
}

func newKosekiAuthFlowController(store *koseki.Store, tenantID string, providers firebaseProviderLifecycle) *kosekiAuthFlowController {
	return &kosekiAuthFlowController{store: store, tenantID: tenantID, providers: providers, clock: time.Now}
}

func (c *kosekiAuthFlowController) Start(ctx context.Context, request agentevents.StartBrowserAuthFlowRequest) (agentevents.BrowserAuthFlowResult, error) {
	switch request.Provider {
	case "email_code":
		if c.email == nil {
			return agentevents.BrowserAuthFlowResult{}, agentevents.ErrBrowserEmailUnavailable
		}
		return c.email.start(ctx, request)
	case "google.com", "github.com":
	default:
		// Firebase email-link sending was replaced by Sumi email codes.
		return agentevents.BrowserAuthFlowResult{}, agentevents.ErrBrowserAuthFlowInvalid
	}
	if request.Email != "" {
		return agentevents.BrowserAuthFlowResult{}, agentevents.ErrBrowserAuthFlowInvalid
	}
	flow, err := c.store.StartAuthFlow(ctx, koseki.StartAuthFlowRequest{
		InviteToken: request.InviteToken, Intent: koseki.AuthIntent(request.Intent), Channel: koseki.ChannelProvider,
		ExpectedProvider: request.Provider,
		Continuation:     request.Continuation, Nonce: request.Nonce,
		BrowserEpochHash: request.BrowserEpochHash, TTL: authFlowTTL,
	})
	if err != nil {
		return agentevents.BrowserAuthFlowResult{}, mapFlowError(err)
	}
	return agentevents.BrowserAuthFlowResult{FlowID: flow.FlowID, Outcome: "proof_required", ExpiresAt: flow.ExpiresAt}, nil
}

func (c *kosekiAuthFlowController) Resolve(ctx context.Context, request agentevents.ResolveBrowserAuthFlowRequest, identity agentevents.FirebaseIdentity) (agentevents.BrowserAuthFlowResult, error) {
	verified, err := verifiedKosekiIdentity(identity)
	if err != nil {
		return agentevents.BrowserAuthFlowResult{}, agentevents.ErrBrowserAuthFlowProof
	}
	flow, err := c.store.ResolveAuthProof(ctx, request.FlowID, request.Nonce, verified)
	if err != nil {
		return agentevents.BrowserAuthFlowResult{}, mapFlowError(err)
	}
	return c.flowResult(flow), nil
}

func (c *kosekiAuthFlowController) Confirm(ctx context.Context, request agentevents.ConfirmBrowserAuthFlowRequest) (agentevents.BrowserAuthFlowResult, error) {
	flow, err := c.store.ConfirmAuthFlow(ctx, request.FlowID, request.Nonce, request.Action)
	if err != nil {
		return agentevents.BrowserAuthFlowResult{}, mapFlowError(err)
	}
	return c.flowResult(flow), nil
}

func (c *kosekiAuthFlowController) Status(ctx context.Context, request agentevents.ConfirmBrowserAuthFlowRequest) (agentevents.BrowserAuthFlowResult, error) {
	flow, err := c.store.AuthFlowStatus(ctx, request.FlowID, request.Nonce)
	if err != nil {
		return agentevents.BrowserAuthFlowResult{}, mapFlowError(err)
	}
	return c.flowResult(flow), nil
}

// AuthFlowEpoch exposes the flow's browser-epoch binding to session admission.
func (c *kosekiAuthFlowController) AuthFlowEpoch(ctx context.Context, flowID string) (string, error) {
	epoch, err := c.store.AuthFlowEpochHash(ctx, flowID)
	if err != nil {
		return "", mapFlowError(err)
	}
	return epoch, nil
}

// AuthFlowForNonce authenticates a flow-scoped discard: only the browser that
// holds the nonce may cancel this flow's issuance authority.
func (c *kosekiAuthFlowController) AuthFlowForNonce(ctx context.Context, flowID, nonce string) (agentevents.BrowserFlowRef, error) {
	ref, err := c.store.BrowserFlowRefForNonce(ctx, flowID, nonce)
	if err != nil {
		return agentevents.BrowserFlowRef{}, mapFlowError(err)
	}
	return browserFlowRefFromKoseki(ref), nil
}

// OpenBrowserFlows enumerates the epoch's unclosed flows for logout closure.
func (c *kosekiAuthFlowController) OpenBrowserFlows(ctx context.Context, epochHash string) ([]agentevents.BrowserFlowRef, error) {
	refs, err := c.store.OpenBrowserFlows(ctx, epochHash)
	if err != nil {
		return nil, err
	}
	out := make([]agentevents.BrowserFlowRef, 0, len(refs))
	for _, ref := range refs {
		out = append(out, browserFlowRefFromKoseki(ref))
	}
	return out, nil
}

// CloseFlows mirrors the committed session-store closure. Issuance is fenced
// by the session store, not this column.
func (c *kosekiAuthFlowController) CloseFlows(ctx context.Context, flowIDs []string) error {
	return c.store.CloseAuthFlows(ctx, flowIDs)
}

func browserFlowRefFromKoseki(ref koseki.BrowserFlowRef) agentevents.BrowserFlowRef {
	return agentevents.BrowserFlowRef{
		FlowID: ref.FlowID, HumanID: ref.HumanID, EpochHash: ref.EpochHash,
		ExpiresAt: ref.ExpiresAt, Closed: ref.ClosedAt != nil,
	}
}

func (c *kosekiAuthFlowController) flowResult(flow koseki.AuthFlow) agentevents.BrowserAuthFlowResult {
	result := agentevents.BrowserAuthFlowResult{
		FlowID: flow.FlowID, Continuation: flow.Continuation, ExpiresAt: flow.ExpiresAt,
	}
	if flow.Status == "confirmation_required" {
		result.Outcome, result.NextAction = "confirmation_required", flow.ConfirmationAction
		return result
	}
	result.Outcome = flow.TerminalOutcome
	result.HumanID = flow.HumanID
	result.Claims = agentevents.UserSessionClaims{TenantID: c.tenantID, UserID: flow.HumanID, PersonalityAgentID: flow.AgentID}
	return result
}

func verifiedKosekiIdentity(identity agentevents.FirebaseIdentity) (koseki.VerifiedIdentity, error) {
	verified := koseki.VerifiedIdentity{
		FirebaseUID: identity.UID, EmailVerified: identity.EmailVerified,
		SignInProvider: identity.SignInProvider, DisplayName: identity.DisplayName,
		IssuedAt: identity.IssuedAt,
	}
	if identity.Email != "" {
		email, err := koseki.NormalizeEmail(identity.Email)
		if err != nil {
			return koseki.VerifiedIdentity{}, err
		}
		verified.NormalizedEmail = email
	}
	if subjects := identity.ProviderSubjects[identity.SignInProvider]; len(subjects) == 1 {
		verified.ProviderSubject = subjects[0]
	}
	return verified, nil
}

func (c *kosekiAuthFlowController) StartProviderOperation(ctx context.Context, claims agentevents.UserSessionClaims, request agentevents.StartProviderOperationRequest, identity agentevents.FirebaseIdentity) (agentevents.ProviderOperationResult, error) {
	uid, err := c.store.FirebaseUIDForHuman(ctx, claims.UserID)
	if err != nil || uid != identity.UID {
		return agentevents.ProviderOperationResult{}, agentevents.ErrBrowserAuthFlowProof
	}
	if request.Operation == "unlink" {
		if !validProviderUnlinkReauth(identity, request.Provider, c.clock().UTC()) {
			return agentevents.ProviderOperationResult{}, agentevents.ErrBrowserAuthRecentReauth
		}
		if c.providers == nil {
			return agentevents.ProviderOperationResult{}, agentevents.ErrBrowserAuthProviderUnavailable
		}
	}
	// The saga deadline starts before Begin samples expiry so remote calls
	// cluster near the operation's live window; it bounds our waiting, not
	// remote work — settleProviderUnlink reconciles anything left ambiguous.
	sagaCtx, cancelSaga := context.WithTimeout(ctx, providerUnlinkSagaTimeout)
	defer cancelSaga()
	operation, err := c.store.BeginProviderOperation(sagaCtx, claims.UserID, uid, request.Provider,
		request.Operation, request.DecisionPath, request.Nonce)
	if errors.Is(err, koseki.ErrProviderOperationPending) {
		// A settled orphan releases its fence; the new change then starts
		// normally. An unlink still inside its grace keeps the fence.
		if settled, settleErr := c.settleProviderUnlink(ctx, claims.UserID, uid); settleErr == nil && settled {
			operation, err = c.store.BeginProviderOperation(sagaCtx, claims.UserID, uid, request.Provider,
				request.Operation, request.DecisionPath, request.Nonce)
		}
	}
	if err != nil {
		return agentevents.ProviderOperationResult{}, mapFlowError(err)
	}
	if operation.Status != "pending" {
		return c.recoverStartedProviderOperation(ctx, claims, operation.OperationID, request.Nonce)
	}
	if request.Operation == "unlink" {
		if operation.Expired {
			// An expired unlink is never re-run by its nonce holder; the
			// server reconcile path settles it from live state, issuing its
			// own bounded delete past the grace if the provider is still
			// present. While it stays pending the caller simply learns the
			// change is still settling.
			settled, settleErr := c.settleProviderUnlink(ctx, claims.UserID, uid)
			switch {
			case settleErr == nil && settled:
				// terminalized — recover below
			case settleErr == nil || errors.Is(settleErr, koseki.ErrProviderOperationPending):
				return agentevents.ProviderOperationResult{}, agentevents.ErrBrowserAuthProviderPending
			default:
				return agentevents.ProviderOperationResult{}, agentevents.ErrBrowserAuthProviderUnavailable
			}
			return c.recoverStartedProviderOperation(ctx, claims, operation.OperationID, request.Nonce)
		}
		return c.runProviderUnlink(sagaCtx, claims, request, operation)
	}
	return agentevents.ProviderOperationResult{
		OperationID: operation.OperationID, Outcome: "client_operation_required",
		ClientOperation: "firebase_link_with_credential", CreatedAt: operation.CreatedAt,
		CompletionTokenNotBefore: completionTokenNotBefore(operation.CreatedAt),
		ExpiresAt:                operation.ExpiresAt,
	}, nil
}

func validProviderUnlinkReauth(identity agentevents.FirebaseIdentity, targetProvider string, now time.Time) bool {
	reauthAge := now.Sub(identity.AuthTime)
	if identity.AuthTime.IsZero() || reauthAge < -time.Minute || reauthAge > recentReauthAge ||
		identity.SignInProvider == targetProvider {
		return false
	}
	switch identity.SignInProvider {
	case "google.com", "github.com":
		return len(identity.ProviderSubjects[identity.SignInProvider]) == 1
	case "password":
		return identity.EmailVerified && identity.Email != "" && len(identity.ProviderSubjects["email"]) == 1
	case koseki.EmailCodeSignInProvider:
		// Sumi mints custom tokens only after a mailbox proof, so a recent
		// custom sign-in is a recent email proof for this principal.
		return identity.EmailVerified && identity.Email != ""
	default:
		return false
	}
}

func (c *kosekiAuthFlowController) runProviderUnlink(ctx context.Context, claims agentevents.UserSessionClaims, request agentevents.StartProviderOperationRequest, operation koseki.ProviderOperation) (agentevents.ProviderOperationResult, error) {
	if operation.Status != "pending" {
		return c.recoverStartedProviderOperation(ctx, claims, operation.OperationID, request.Nonce)
	}
	providerSubject, err := c.store.ActiveProviderSubject(ctx, claims.UserID, operation.Provider)
	if err != nil {
		if recovered, recoveryErr := c.recoverStartedProviderOperation(ctx, claims, operation.OperationID, request.Nonce); recoveryErr == nil {
			return recovered, nil
		}
		_, _ = c.store.FailProviderOperation(ctx, operation.OperationID, request.Nonce, "firebase_operation_failed")
		return agentevents.ProviderOperationResult{}, agentevents.ErrBrowserAuthFlowProof
	}

	account, err := c.providers.ProviderAccount(ctx, operation.FirebaseUID)
	if err != nil {
		return agentevents.ProviderOperationResult{}, agentevents.ErrBrowserAuthProviderUnavailable
	}
	if account.UID != operation.FirebaseUID {
		return agentevents.ProviderOperationResult{}, agentevents.ErrBrowserAuthProviderUnavailable
	}
	remoteSubject, providerPresent := account.ProviderSubjects[operation.Provider]
	if !providerPresent {
		return c.finishProviderUnlink(ctx, claims, request.Nonce, operation, providerSubject)
	}
	if remoteSubject != providerSubject {
		_, _ = c.store.FailProviderOperation(ctx, operation.OperationID, request.Nonce, "firebase_operation_failed")
		return agentevents.ProviderOperationResult{}, agentevents.ErrBrowserAuthFlowProof
	}
	emailMethod, err := c.usableEmailMethod(ctx, claims.UserID, account)
	if err != nil {
		return agentevents.ProviderOperationResult{}, agentevents.ErrBrowserAuthProviderUnavailable
	}
	usableMethods := supportedProviderMethodCount(account)
	if emailMethod {
		usableMethods++
	}
	if usableMethods <= 1 {
		if _, err := c.store.FailProviderOperation(ctx, operation.OperationID, request.Nonce, "last_login_method"); err != nil {
			return agentevents.ProviderOperationResult{}, agentevents.ErrBrowserAuthProviderUnavailable
		}
		return agentevents.ProviderOperationResult{}, agentevents.ErrBrowserAuthLastMethod
	}

	deleteErr := c.providers.DeleteProvider(ctx, operation.FirebaseUID, operation.Provider)
	postcheck, postcheckErr := c.providers.ProviderAccount(ctx, operation.FirebaseUID)
	if postcheckErr != nil {
		// The pending row deliberately retains the per-UID fence. A same-nonce
		// retry will repeat the live read and reconcile either remote state.
		return agentevents.ProviderOperationResult{}, agentevents.ErrBrowserAuthProviderUnavailable
	}
	if postcheck.UID != operation.FirebaseUID {
		return agentevents.ProviderOperationResult{}, agentevents.ErrBrowserAuthProviderUnavailable
	}
	if _, stillPresent := postcheck.ProviderSubjects[operation.Provider]; stillPresent {
		if _, err := c.store.FailProviderOperation(ctx, operation.OperationID, request.Nonce, "firebase_operation_failed"); err != nil {
			return agentevents.ProviderOperationResult{}, agentevents.ErrBrowserAuthProviderUnavailable
		}
		return agentevents.ProviderOperationResult{}, agentevents.ErrBrowserAuthProviderUnavailable
	}
	// An Admin error with an absent provider is an ambiguous-success response,
	// not a failure. The live postcheck is authoritative.
	_ = deleteErr
	return c.finishProviderUnlink(ctx, claims, request.Nonce, operation, providerSubject)
}

// usableEmailMethod reports whether this account can sign in by email. A
// verified Firebase address is a sign-in method only while Sumi email-code
// sign-in is enabled; it then resolves that address to this UID. Either family
// also needs Sumi's own completed email proof for the same principal.
func (c *kosekiAuthFlowController) usableEmailMethod(ctx context.Context, humanID string, account firebaseProviderAccount) (bool, error) {
	if !account.EmailProvider && !(account.EmailVerified && c.email != nil) {
		return false, nil
	}
	return c.store.HasCompletedEmailLinkProof(ctx, humanID, account.UID)
}

// settleProviderUnlink reconciles the expired pending unlink fencing this
// principal from live Firebase state, and reports whether an operation was
// terminalized by this call or a concurrent one.
//
// The original unlink run's Admin request may still be executing — context
// cancellation cannot prove the remote server stopped — so this path never
// asserts a final remote outcome. Instead it converges on the operation's
// intent: if the recorded credential subject is gone remotely the unlink
// completes; if the provider is still present past the settle grace the
// server issues one bounded reconcile delete of its own and terminalizes on
// the postcheck. A provider still present after that attempt fails the
// operation as expired — the fence is released because we verified our own
// attempt, not because the original request provably died. When the live
// read itself fails the operation stays pending: the fence honestly reports
// an unverifiable remote until a later trigger can read it.
func (c *kosekiAuthFlowController) settleProviderUnlink(ctx context.Context, humanID, firebaseUID string) (bool, error) {
	if c.providers == nil {
		return false, nil
	}
	operation, found, err := c.store.SettlingProviderUnlink(ctx, humanID, firebaseUID)
	if err != nil || !found {
		return false, err
	}
	providerSubject, err := c.store.ActiveProviderSubject(ctx, humanID, operation.Provider)
	if errors.Is(err, pgx.ErrNoRows) {
		providerSubject = ""
	} else if err != nil {
		return false, err
	}
	readCtx, cancel := context.WithTimeout(ctx, providerUnlinkSagaTimeout)
	defer cancel()
	account, err := c.providers.ProviderAccount(readCtx, firebaseUID)
	if errors.Is(err, errFirebaseUserGone) {
		account = firebaseProviderAccount{UID: firebaseUID, ProviderSubjects: map[string]string{}}
	} else if err != nil {
		return false, err
	}
	if account.UID != firebaseUID {
		return false, agentevents.ErrBrowserAuthProviderUnavailable
	}
	remoteSubject, present := account.ProviderSubjects[operation.Provider]
	removed := !present || (providerSubject != "" && remoteSubject != providerSubject)
	failureOutcome := "expired"
	if !removed && operation.PastSettleGrace && remoteSubject == providerSubject && providerSubject != "" {
		// The user's consent survives the lost browser, but only while the
		// unlink is still safe to apply. Re-run the last-method guard on the
		// live account before driving a delete of our own: consent captured
		// at start does not outlive the methods that made it safe. Failing
		// as last_login_method preserves the account's only usable sign-in
		// method instead of stranding the user.
		emailMethod, emailErr := c.usableEmailMethod(ctx, humanID, account)
		if emailErr != nil {
			return false, emailErr
		}
		usableMethods := supportedProviderMethodCount(account)
		if emailMethod {
			usableMethods++
		}
		if usableMethods <= 1 {
			failureOutcome = "last_login_method"
		} else {
			// The exact identity this unlink was issued against is still
			// linked and other methods remain, so the server drives one
			// bounded delete of its own and settles on the postcheck.
			reconcileCtx, reconcileCancel := context.WithTimeout(ctx, providerUnlinkSagaTimeout)
			_ = c.providers.DeleteProvider(reconcileCtx, firebaseUID, operation.Provider)
			postcheck, postcheckErr := c.providers.ProviderAccount(reconcileCtx, firebaseUID)
			reconcileCancel()
			if errors.Is(postcheckErr, errFirebaseUserGone) {
				postcheck = firebaseProviderAccount{UID: firebaseUID, ProviderSubjects: map[string]string{}}
			} else if postcheckErr != nil {
				// The reconcile attempt cannot be verified: keep the fence
				// pending for a later trigger rather than declaring either
				// way.
				return false, postcheckErr
			}
			if postcheck.UID != firebaseUID {
				return false, agentevents.ErrBrowserAuthProviderUnavailable
			}
			_, stillPresent := postcheck.ProviderSubjects[operation.Provider]
			removed = !stillPresent
		}
	}
	_, err = c.store.SettleProviderUnlink(ctx, operation.OperationID, firebaseUID, providerSubject, removed, failureOutcome)
	if errors.Is(err, koseki.ErrAuthFlowConsumed) {
		return true, nil
	}
	return err == nil, err
}

// ProviderMethods reads the session Human's usable sign-in methods from the
// live Firebase account, so every browser shows the same methods the unlink
// guard counts rather than its own cached provider list. A provider is listed
// only when the remote account carries the exact subject an active credential
// binds: that is the state in which the method can actually sign in and be
// managed. The same read heals drift — an active credential whose remote
// identity vanished (including an unlink that committed after its operation
// was released) is retired, since it can never authenticate again.
func (c *kosekiAuthFlowController) ProviderMethods(ctx context.Context, claims agentevents.UserSessionClaims) (agentevents.ProviderMethodsResult, error) {
	if c.providers == nil {
		return agentevents.ProviderMethodsResult{}, agentevents.ErrBrowserAuthProviderUnavailable
	}
	uid, err := c.store.FirebaseUIDForHuman(ctx, claims.UserID)
	if err != nil {
		return agentevents.ProviderMethodsResult{}, agentevents.ErrBrowserAuthFlowProof
	}
	// Local credential state is read before the remote account: an active row
	// implies its remote mutation already landed, because activation only ever
	// follows a server-verified token or a completed browser-side link. A
	// remote read that omits such a binding is therefore genuinely absent, not
	// a snapshot taken before the commit.
	credentialSubjects := make(map[string]string, 2)
	for _, provider := range []string{"google.com", "github.com"} {
		subject, credErr := c.store.ActiveProviderSubject(ctx, claims.UserID, provider)
		if errors.Is(credErr, pgx.ErrNoRows) {
			subject = ""
		} else if credErr != nil {
			return agentevents.ProviderMethodsResult{}, agentevents.ErrBrowserAuthProviderUnavailable
		}
		credentialSubjects[provider] = subject
	}
	readCtx, cancel := context.WithTimeout(ctx, providerUnlinkSagaTimeout)
	defer cancel()
	account, err := c.providers.ProviderAccount(readCtx, uid)
	if errors.Is(err, errFirebaseUserGone) {
		// A deleted Firebase account carries no providers: report an empty
		// account so the credential heal retires every stale binding.
		account = firebaseProviderAccount{UID: uid, ProviderSubjects: map[string]string{}}
	} else if err != nil || account.UID != uid {
		return agentevents.ProviderMethodsResult{}, agentevents.ErrBrowserAuthProviderUnavailable
	}
	emailMethod, err := c.usableEmailMethod(ctx, claims.UserID, account)
	if err != nil {
		return agentevents.ProviderMethodsResult{}, agentevents.ErrBrowserAuthProviderUnavailable
	}
	result := agentevents.ProviderMethodsResult{Providers: []string{}, Email: emailMethod}
	for _, provider := range []string{"google.com", "github.com"} {
		credentialSubject := credentialSubjects[provider]
		remoteSubject, present := account.ProviderSubjects[provider]
		if present && credentialSubject != "" && remoteSubject == credentialSubject {
			result.Providers = append(result.Providers, provider)
			continue
		}
		if credentialSubject == "" {
			continue
		}
		// Re-validate the stale observation at decision time under the
		// credential row lock: a relink whose remote mutation landed after
		// this read must never be retired by it.
		_ = c.store.DisableStaleProviderCredential(ctx, claims.UserID, uid, provider, credentialSubject,
			func(recheckCtx context.Context) (bool, error) {
				recheckCtx, recheckCancel := context.WithTimeout(recheckCtx, providerUnlinkSagaTimeout)
				defer recheckCancel()
				current, err := c.providers.ProviderAccount(recheckCtx, uid)
				if errors.Is(err, errFirebaseUserGone) {
					return true, nil
				}
				if err != nil {
					return false, err
				}
				if current.UID != uid {
					return false, agentevents.ErrBrowserAuthProviderUnavailable
				}
				subject, present := current.ProviderSubjects[provider]
				return !present || subject != credentialSubject, nil
			})
	}
	return result, nil
}

func supportedProviderMethodCount(account firebaseProviderAccount) int {
	count := 0
	for _, provider := range []string{"google.com", "github.com"} {
		if account.ProviderSubjects[provider] != "" {
			count++
		}
	}
	return count
}

func (c *kosekiAuthFlowController) finishProviderUnlink(ctx context.Context, claims agentevents.UserSessionClaims, nonce string, operation koseki.ProviderOperation, providerSubject string) (agentevents.ProviderOperationResult, error) {
	_, err := c.store.CompleteProviderUnlink(ctx, operation.OperationID, nonce, operation.FirebaseUID, providerSubject)
	if errors.Is(err, koseki.ErrAuthFlowConsumed) {
		return c.recoverStartedProviderOperation(ctx, claims, operation.OperationID, nonce)
	}
	if err != nil {
		return agentevents.ProviderOperationResult{}, agentevents.ErrBrowserAuthProviderUnavailable
	}
	return agentevents.ProviderOperationResult{
		OperationID: operation.OperationID, Outcome: "provider_unlinked",
		CreatedAt: operation.CreatedAt, CompletionTokenNotBefore: completionTokenNotBefore(operation.CreatedAt),
		ExpiresAt: operation.ExpiresAt, NoticeRequired: true,
	}, nil
}

func (c *kosekiAuthFlowController) recoverStartedProviderOperation(ctx context.Context, claims agentevents.UserSessionClaims, operationID, nonce string) (agentevents.ProviderOperationResult, error) {
	status, err := c.StatusProviderOperation(ctx, claims, agentevents.ProviderOperationStatusRequest{OperationID: operationID, Nonce: nonce})
	if err != nil {
		return agentevents.ProviderOperationResult{}, err
	}
	if status.Status == "pending" {
		return agentevents.ProviderOperationResult{}, agentevents.ErrBrowserAuthProviderPending
	}
	if status.Status == "failed" && status.Outcome == "last_login_method" {
		return agentevents.ProviderOperationResult{}, agentevents.ErrBrowserAuthLastMethod
	}
	return agentevents.ProviderOperationResult{
		OperationID: status.OperationID, Outcome: status.Outcome,
		CreatedAt: status.CreatedAt, CompletionTokenNotBefore: status.CompletionTokenNotBefore,
		ExpiresAt: status.ExpiresAt, NoticeRequired: status.NoticeRequired,
	}, nil
}

func (c *kosekiAuthFlowController) CompleteProviderOperation(ctx context.Context, claims agentevents.UserSessionClaims, request agentevents.CompleteProviderOperationRequest, identity agentevents.FirebaseIdentity) (agentevents.ProviderOperationResult, error) {
	operation, err := c.store.PendingProviderOperation(ctx, request.OperationID, request.Nonce)
	if err != nil {
		return agentevents.ProviderOperationResult{}, mapFlowError(err)
	}
	if operation.HumanID != claims.UserID || operation.FirebaseUID != identity.UID {
		return agentevents.ProviderOperationResult{}, agentevents.ErrBrowserAuthFlowProof
	}
	if operation.Operation != "link" {
		return agentevents.ProviderOperationResult{}, agentevents.ErrBrowserAuthFlowInvalid
	}
	if identity.IssuedAt.IsZero() || identity.IssuedAt.Before(completionTokenNotBefore(operation.CreatedAt)) {
		return agentevents.ProviderOperationResult{}, agentevents.ErrBrowserAuthFlowProof
	}
	subjects := identity.ProviderSubjects[operation.Provider]
	if len(subjects) != 1 {
		return agentevents.ProviderOperationResult{}, agentevents.ErrBrowserAuthFlowProof
	}
	var event koseki.SecurityEvent
	event, err = c.store.CompleteProviderLink(ctx, request.OperationID, request.Nonce, identity.UID, subjects[0])
	if errors.Is(err, koseki.ErrCredentialAlreadyBound) {
		_, _ = c.store.FailProviderOperation(ctx, request.OperationID, request.Nonce, "credential_in_use")
	}
	if err == nil && event.TerminalOutcome == "already_linked" {
		return agentevents.ProviderOperationResult{
			OperationID: operation.OperationID, Outcome: "provider_already_linked",
			CreatedAt: operation.CreatedAt, CompletionTokenNotBefore: completionTokenNotBefore(operation.CreatedAt),
			ExpiresAt: operation.ExpiresAt,
		}, nil
	}
	if err != nil {
		return agentevents.ProviderOperationResult{}, mapFlowError(err)
	}
	return agentevents.ProviderOperationResult{
		OperationID: operation.OperationID, Outcome: "provider_linked",
		CreatedAt: operation.CreatedAt, CompletionTokenNotBefore: completionTokenNotBefore(operation.CreatedAt),
		ExpiresAt: operation.ExpiresAt, NoticeRequired: true,
	}, nil
}

// Firebase ID token iat has one-second precision while Postgres created_at has
// sub-second precision. If an operation begins during a second, only a token
// from the next whole second can unambiguously have been issued afterwards.
// An exact whole-second operation timestamp may safely accept the same instant.
func completionTokenNotBefore(createdAt time.Time) time.Time {
	createdAt = createdAt.UTC()
	truncated := createdAt.Truncate(time.Second)
	if createdAt.Equal(truncated) {
		return truncated
	}
	return truncated.Add(time.Second)
}

func (c *kosekiAuthFlowController) FailProviderOperation(ctx context.Context, claims agentevents.UserSessionClaims, request agentevents.FailProviderOperationRequest) (agentevents.ProviderOperationResult, error) {
	operation, err := c.store.PendingProviderOperation(ctx, request.OperationID, request.Nonce)
	if err != nil {
		return agentevents.ProviderOperationResult{}, mapFlowError(err)
	}
	if operation.HumanID != claims.UserID {
		return agentevents.ProviderOperationResult{}, agentevents.ErrBrowserAuthFlowProof
	}
	// Unlink is a backend-owned saga. A browser must never release its durable
	// fence or declare its Firebase Admin mutation failed.
	if operation.Operation != "link" || !validClientProviderFailureOutcome(request.Outcome) {
		return agentevents.ProviderOperationResult{}, agentevents.ErrBrowserAuthFlowInvalid
	}
	_, err = c.store.FailProviderOperation(ctx, request.OperationID, request.Nonce, request.Outcome)
	if err != nil {
		return agentevents.ProviderOperationResult{}, mapFlowError(err)
	}
	return agentevents.ProviderOperationResult{
		OperationID: operation.OperationID, Outcome: request.Outcome,
		CreatedAt: operation.CreatedAt, CompletionTokenNotBefore: completionTokenNotBefore(operation.CreatedAt),
		ExpiresAt: operation.ExpiresAt,
	}, nil
}

func validClientProviderFailureOutcome(outcome string) bool {
	return outcome == "credential_in_use" || outcome == "firebase_operation_failed" || outcome == "cancelled"
}

func (c *kosekiAuthFlowController) StatusProviderOperation(ctx context.Context, claims agentevents.UserSessionClaims, request agentevents.ProviderOperationStatusRequest) (agentevents.ProviderOperationStatusResult, error) {
	operation, err := c.store.ProviderOperationStatus(ctx, claims.UserID, request.OperationID, request.Nonce)
	if err != nil {
		return agentevents.ProviderOperationStatusResult{}, mapFlowError(err)
	}
	result := agentevents.ProviderOperationStatusResult{
		OperationID: operation.OperationID, Provider: operation.Provider,
		Operation: operation.Operation, Status: operation.Status,
		CreatedAt:                operation.CreatedAt,
		CompletionTokenNotBefore: completionTokenNotBefore(operation.CreatedAt),
		ExpiresAt:                operation.ExpiresAt, CompletedAt: operation.CompletedAt,
	}
	switch operation.Status {
	case "pending":
		if operation.Operation == "unlink" {
			result.Outcome = "provider_operation_pending"
		} else {
			result.Outcome = "client_operation_required"
			result.ClientOperation = "firebase_link_with_credential"
		}
	case "completed":
		switch {
		case operation.Operation == "link" && operation.TerminalOutcome == "linked":
			result.Outcome, result.NoticeRequired = "provider_linked", true
		case operation.Operation == "link" && operation.TerminalOutcome == "already_linked":
			result.Outcome = "provider_already_linked"
		case operation.Operation == "unlink" && operation.TerminalOutcome == "unlinked":
			result.Outcome, result.NoticeRequired = "provider_unlinked", true
		default:
			return agentevents.ProviderOperationStatusResult{}, agentevents.ErrBrowserAuthFlowInvalid
		}
	case "failed":
		if !validProviderFailureOutcome(operation.TerminalOutcome) {
			return agentevents.ProviderOperationStatusResult{}, agentevents.ErrBrowserAuthFlowInvalid
		}
		result.Outcome = operation.TerminalOutcome
	default:
		return agentevents.ProviderOperationStatusResult{}, agentevents.ErrBrowserAuthFlowInvalid
	}
	return result, nil
}

func validProviderFailureOutcome(outcome string) bool {
	return outcome == "provider_already_linked" || outcome == "credential_in_use" ||
		outcome == "firebase_operation_failed" || outcome == "cancelled" || outcome == "last_login_method" ||
		outcome == "expired"
}

func mapFlowError(err error) error {
	var mismatch *koseki.EmailCodeMismatchError
	var limited *koseki.EmailSendLimitedError
	switch {
	case errors.As(err, &mismatch):
		return &agentevents.BrowserEmailCodeMismatchError{AttemptsRemaining: mismatch.AttemptsRemaining}
	case errors.As(err, &limited):
		return &agentevents.BrowserEmailSendLimitedError{RetryAt: limited.RetryAt}
	case errors.Is(err, koseki.ErrEmailCodeLocked):
		return agentevents.ErrBrowserEmailCodeLocked
	case errors.Is(err, koseki.ErrEmailChallengeSuperseded):
		return agentevents.ErrBrowserEmailSuperseded
	case errors.Is(err, koseki.ErrEmailChallengeExpired):
		return agentevents.ErrBrowserEmailExpired
	case errors.Is(err, koseki.ErrEmailChallengeConsumed):
		return agentevents.ErrBrowserEmailConsumed
	case errors.Is(err, koseki.ErrEmailLinkInvalid):
		return agentevents.ErrBrowserEmailLinkInvalid
	case errors.Is(err, koseki.ErrEmailLinkAdoptionRequired):
		return agentevents.ErrBrowserEmailAdoptionRequired
	case errors.Is(err, koseki.ErrEmailFlowContinuedElsewhere):
		return agentevents.ErrBrowserEmailContinuedElsewhere
	case errors.Is(err, koseki.ErrEmailChallengeUnavailable):
		return agentevents.ErrBrowserEmailUnavailable
	case errors.Is(err, koseki.ErrEnrollmentInvite):
		return agentevents.ErrBrowserEnrollmentInvite
	case errors.Is(err, koseki.ErrAuthFlowExpired):
		return agentevents.ErrBrowserAuthFlowExpired
	case errors.Is(err, koseki.ErrAuthFlowConsumed):
		return agentevents.ErrBrowserAuthFlowConsumed
	case errors.Is(err, koseki.ErrAuthProofMismatch), errors.Is(err, koseki.ErrCredentialAlreadyBound):
		return agentevents.ErrBrowserAuthFlowProof
	case errors.Is(err, koseki.ErrRecentReauth):
		return agentevents.ErrBrowserAuthRecentReauth
	case errors.Is(err, koseki.ErrLastLoginMethod):
		return agentevents.ErrBrowserAuthLastMethod
	case errors.Is(err, koseki.ErrProviderOperationPending):
		return agentevents.ErrBrowserAuthProviderPending
	default:
		return agentevents.ErrBrowserAuthFlowInvalid
	}
}

var _ agentevents.BrowserAuthFlowController = (*kosekiAuthFlowController)(nil)
