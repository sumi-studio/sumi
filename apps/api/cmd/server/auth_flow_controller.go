package main

import (
	"context"
	"errors"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
)

const (
	authFlowTTL     = 15 * time.Minute
	recentReauthAge = 5 * time.Minute
)

type kosekiAuthFlowController struct {
	store     *koseki.Store
	tenantID  string
	providers firebaseProviderLifecycle
	clock     func() time.Time
}

type firebaseProviderAccount struct {
	UID              string
	ProviderSubjects map[string]string
	EmailProvider    bool
}

type firebaseProviderLifecycle interface {
	ProviderAccount(ctx context.Context, firebaseUID string) (firebaseProviderAccount, error)
	DeleteProvider(ctx context.Context, firebaseUID, provider string) error
}

func newKosekiAuthFlowController(store *koseki.Store, tenantID string, providers firebaseProviderLifecycle) *kosekiAuthFlowController {
	return &kosekiAuthFlowController{store: store, tenantID: tenantID, providers: providers, clock: time.Now}
}

func (c *kosekiAuthFlowController) Start(ctx context.Context, request agentevents.StartBrowserAuthFlowRequest) (agentevents.BrowserAuthFlowResult, error) {
	channel, expectedProvider, normalizedEmail := koseki.ChannelProvider, request.Provider, ""
	if request.Provider == "email_link" {
		channel, expectedProvider = koseki.ChannelEmailLink, "password"
		var err error
		normalizedEmail, err = koseki.NormalizeEmail(request.Email)
		if err != nil {
			return agentevents.BrowserAuthFlowResult{}, agentevents.ErrBrowserAuthFlowInvalid
		}
	}
	flow, err := c.store.StartAuthFlow(ctx, koseki.StartAuthFlowRequest{
		Intent: koseki.AuthIntent(request.Intent), Channel: channel,
		ExpectedProvider: expectedProvider, NormalizedEmail: normalizedEmail,
		Continuation: request.Continuation, Nonce: request.Nonce, TTL: authFlowTTL,
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

func (c *kosekiAuthFlowController) flowResult(flow koseki.AuthFlow) agentevents.BrowserAuthFlowResult {
	result := agentevents.BrowserAuthFlowResult{
		FlowID: flow.FlowID, Continuation: flow.Continuation, ExpiresAt: flow.ExpiresAt,
	}
	if flow.Status == "confirmation_required" {
		result.Outcome, result.NextAction = "confirmation_required", flow.ConfirmationAction
		return result
	}
	result.Outcome = flow.TerminalOutcome
	result.Claims = agentevents.UserSessionClaims{TenantID: c.tenantID, UserID: flow.HumanID, PersonalityAgentID: flow.AgentID}
	return result
}

func verifiedKosekiIdentity(identity agentevents.FirebaseIdentity) (koseki.VerifiedIdentity, error) {
	verified := koseki.VerifiedIdentity{
		FirebaseUID: identity.UID, EmailVerified: identity.EmailVerified,
		SignInProvider: identity.SignInProvider,
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
	operation, err := c.store.BeginProviderOperation(ctx, claims.UserID, uid, request.Provider,
		request.Operation, request.DecisionPath, request.Nonce)
	if err != nil {
		return agentevents.ProviderOperationResult{}, mapFlowError(err)
	}
	if operation.Status != "pending" {
		return c.recoverStartedProviderOperation(ctx, claims, operation.OperationID, request.Nonce)
	}
	if request.Operation == "unlink" {
		return c.runProviderUnlink(ctx, claims, request, operation)
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
	emailLinkProof, err := c.store.HasCompletedEmailLinkProof(ctx, claims.UserID, operation.FirebaseUID)
	if err != nil {
		return agentevents.ProviderOperationResult{}, agentevents.ErrBrowserAuthProviderUnavailable
	}
	usableMethods := supportedProviderMethodCount(account)
	if account.EmailProvider && emailLinkProof {
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
		return agentevents.ProviderOperationResult{}, agentevents.ErrBrowserAuthProviderUnavailable
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
		outcome == "firebase_operation_failed" || outcome == "cancelled" || outcome == "last_login_method"
}

func mapFlowError(err error) error {
	switch {
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
