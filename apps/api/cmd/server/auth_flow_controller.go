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
	store    *koseki.Store
	tenantID string
	clock    func() time.Time
}

func newKosekiAuthFlowController(store *koseki.Store, tenantID string) *kosekiAuthFlowController {
	return &kosekiAuthFlowController{store: store, tenantID: tenantID, clock: time.Now}
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
		reauthAge := c.clock().UTC().Sub(identity.AuthTime)
		if identity.AuthTime.IsZero() || reauthAge < -time.Minute || reauthAge > recentReauthAge || identity.SignInProvider == request.Provider {
			return agentevents.ProviderOperationResult{}, agentevents.ErrBrowserAuthRecentReauth
		}
		if len(identity.ProviderSubjects[request.Provider]) != 1 {
			return agentevents.ProviderOperationResult{}, agentevents.ErrBrowserAuthFlowProof
		}
		if usableProviderCount(identity.ProviderSubjects) < 2 {
			return agentevents.ProviderOperationResult{}, agentevents.ErrBrowserAuthLastMethod
		}
	}
	operation, err := c.store.BeginProviderOperation(ctx, claims.UserID, uid, request.Provider,
		request.Operation, request.DecisionPath, request.Nonce)
	if err != nil {
		return agentevents.ProviderOperationResult{}, mapFlowError(err)
	}
	clientOperation := "firebase_link_with_credential"
	if request.Operation == "unlink" {
		clientOperation = "firebase_unlink_provider"
	}
	return agentevents.ProviderOperationResult{
		OperationID: operation.OperationID, Outcome: "client_operation_required",
		ClientOperation: clientOperation, CreatedAt: operation.CreatedAt,
		CompletionTokenNotBefore: completionTokenNotBefore(operation.CreatedAt),
		ExpiresAt:                operation.ExpiresAt,
	}, nil
}

func usableProviderCount(subjects map[string][]string) int {
	count := 0
	if len(subjects["email"]) > 0 || len(subjects["password"]) > 0 {
		count++
	}
	for _, provider := range []string{"google.com", "github.com", "phone"} {
		if len(subjects[provider]) > 0 {
			count++
		}
	}
	return count
}

func (c *kosekiAuthFlowController) CompleteProviderOperation(ctx context.Context, claims agentevents.UserSessionClaims, request agentevents.CompleteProviderOperationRequest, identity agentevents.FirebaseIdentity) (agentevents.ProviderOperationResult, error) {
	operation, err := c.store.PendingProviderOperation(ctx, request.OperationID, request.Nonce)
	if err != nil {
		return agentevents.ProviderOperationResult{}, mapFlowError(err)
	}
	if operation.HumanID != claims.UserID || operation.FirebaseUID != identity.UID {
		return agentevents.ProviderOperationResult{}, agentevents.ErrBrowserAuthFlowProof
	}
	if identity.IssuedAt.IsZero() || identity.IssuedAt.Before(completionTokenNotBefore(operation.CreatedAt)) {
		return agentevents.ProviderOperationResult{}, agentevents.ErrBrowserAuthFlowProof
	}
	if operation.Operation == "link" {
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
	} else {
		if len(identity.ProviderSubjects[operation.Provider]) != 0 {
			return agentevents.ProviderOperationResult{}, agentevents.ErrBrowserAuthFlowProof
		}
		if usableProviderCount(identity.ProviderSubjects) < 1 {
			return agentevents.ProviderOperationResult{}, agentevents.ErrBrowserAuthLastMethod
		}
		subject, subjectErr := c.store.ActiveProviderSubject(ctx, claims.UserID, operation.Provider)
		if subjectErr != nil {
			return agentevents.ProviderOperationResult{}, agentevents.ErrBrowserAuthFlowProof
		}
		_, err = c.store.CompleteProviderUnlink(ctx, request.OperationID, request.Nonce, identity.UID, subject)
	}
	if err != nil {
		return agentevents.ProviderOperationResult{}, mapFlowError(err)
	}
	outcome := "provider_linked"
	if operation.Operation == "unlink" {
		outcome = "provider_unlinked"
	}
	return agentevents.ProviderOperationResult{
		OperationID: operation.OperationID, Outcome: outcome,
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
	if request.Outcome != "provider_already_linked" && request.Outcome != "credential_in_use" && request.Outcome != "firebase_operation_failed" && request.Outcome != "cancelled" {
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
	default:
		return agentevents.ErrBrowserAuthFlowInvalid
	}
}

var _ agentevents.BrowserAuthFlowController = (*kosekiAuthFlowController)(nil)
