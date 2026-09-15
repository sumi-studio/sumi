package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	firebaseauth "firebase.google.com/go/v4/auth"
	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/authemail"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
)

const (
	emailCodeFlowTTL          = koseki.MaxFlowTTL
	emailDeliveryLease        = time.Minute
	emailDeliveryPollInterval = 5 * time.Second
	emailPrincipalAttempts    = 3
)

// firebaseEmailPrincipalClient is the Admin surface needed to turn a proved
// mailbox into the Firebase principal that owns that address.
type firebaseEmailPrincipalClient interface {
	GetUserByEmail(ctx context.Context, email string) (*firebaseauth.UserRecord, error)
	GetUser(ctx context.Context, uid string) (*firebaseauth.UserRecord, error)
	CreateUser(ctx context.Context, user *firebaseauth.UserToCreate) (*firebaseauth.UserRecord, error)
	UpdateUser(ctx context.Context, uid string, user *firebaseauth.UserToUpdate) (*firebaseauth.UserRecord, error)
	DeleteUser(ctx context.Context, uid string) error
	CustomToken(ctx context.Context, uid string) (string, error)
}

type emailCodeController struct {
	store    *koseki.Store
	firebase firebaseEmailPrincipalClient
	delivery *emailDeliveryWorker
}

var _ agentevents.BrowserEmailAuthController = (*emailCodeController)(nil)

func (c *emailCodeController) start(ctx context.Context, request agentevents.StartBrowserAuthFlowRequest) (agentevents.BrowserAuthFlowResult, error) {
	email, err := koseki.NormalizeEmail(request.Email)
	if err != nil {
		return agentevents.BrowserAuthFlowResult{}, agentevents.ErrBrowserAuthFlowInvalid
	}
	flow, state, err := c.store.StartEmailCodeFlow(ctx, koseki.StartAuthFlowRequest{
		InviteToken: request.InviteToken, Intent: koseki.AuthIntent(request.Intent),
		Channel: koseki.ChannelEmailCode, ExpectedProvider: koseki.EmailCodeSignInProvider,
		NormalizedEmail: email, Continuation: request.Continuation, Nonce: request.Nonce,
		BrowserEpochHash: request.BrowserEpochHash, TTL: emailCodeFlowTTL,
	})
	if err != nil {
		return agentevents.BrowserAuthFlowResult{}, mapFlowError(err)
	}
	c.wakeDelivery()
	challenge := emailChallengeResult(koseki.EmailProof{
		FlowID: flow.FlowID, NormalizedEmail: flow.NormalizedEmail, FlowExpiresAt: flow.ExpiresAt, Status: flow.Status,
	}, state)
	return agentevents.BrowserAuthFlowResult{
		FlowID: flow.FlowID, Outcome: "proof_required", ExpiresAt: flow.ExpiresAt, EmailChallenge: &challenge,
	}, nil
}

func (c *emailCodeController) wakeDelivery() {
	if c.delivery != nil {
		c.delivery.wake()
	}
}

func emailChallengeResult(proof koseki.EmailProof, state koseki.EmailChallengeState) agentevents.EmailChallengeResult {
	flowStatus := proof.Status
	if flowStatus == "pending" && !proof.ProvedAt.IsZero() {
		flowStatus = "email_proved"
	}
	return agentevents.EmailChallengeResult{
		FlowID: proof.FlowID, FlowStatus: flowStatus, Email: proof.NormalizedEmail,
		FlowExpiresAt: proof.FlowExpiresAt, ChallengeExpiresAt: state.ChallengeExpires,
		AttemptsRemaining: state.AttemptsRemaining, Delivery: state.DeliveryStatus,
		ResendAvailableAt: state.ResendAvailableAt, HumanID: proof.HumanID,
	}
}

func (c *emailCodeController) VerifyEmailCode(ctx context.Context, request agentevents.VerifyEmailCodeRequest, session *agentevents.UserSessionClaims) (agentevents.EmailProofResult, error) {
	proof, err := c.store.VerifyEmailCode(ctx, request.FlowID, request.Nonce, request.Code)
	if err != nil {
		return agentevents.EmailProofResult{}, mapFlowError(err)
	}
	return c.finishProof(ctx, proof, request.Nonce, session)
}

func (c *emailCodeController) ResendEmailCode(ctx context.Context, request agentevents.EmailFlowRequest) (agentevents.EmailChallengeResult, error) {
	if _, err := c.store.ResendEmailChallenge(ctx, request.FlowID, request.Nonce); err != nil {
		return agentevents.EmailChallengeResult{}, mapFlowError(err)
	}
	c.wakeDelivery()
	return c.EmailFlowStatus(ctx, request)
}

func (c *emailCodeController) EmailFlowStatus(ctx context.Context, request agentevents.EmailFlowRequest) (agentevents.EmailChallengeResult, error) {
	proof, state, err := c.store.EmailFlowStatus(ctx, request.FlowID, request.Nonce)
	if err != nil {
		return agentevents.EmailChallengeResult{}, mapFlowError(err)
	}
	return emailChallengeResult(proof, state), nil
}

func (c *emailCodeController) InspectEmailLink(ctx context.Context, request agentevents.InspectEmailLinkRequest, session *agentevents.UserSessionClaims) (agentevents.EmailLinkInspectionResult, error) {
	inspection, err := c.store.InspectEmailLink(ctx, request.ChallengeID, request.Token, request.Nonce)
	if err != nil {
		return agentevents.EmailLinkInspectionResult{}, mapFlowError(err)
	}
	result := agentevents.EmailLinkInspectionResult{
		FlowID: inspection.FlowID, Intent: string(inspection.Intent), Email: inspection.NormalizedEmail,
		State: inspection.State, SameBrowser: inspection.SameBrowser, Session: "none", ExpiresAt: inspection.ExpiresAt,
	}
	if session != nil {
		result.Session = c.sessionRelation(ctx, inspection.NormalizedEmail, session.UserID)
	}
	return result, nil
}

// sessionRelation is read-only: it never creates a Firebase user for an
// unproved address. The bound Human — not Firebase's email_verified flag —
// decides "same_account": a signed-in owner of an unverified bound principal
// is the same account, and mailbox proof now enables its verified email.
// Anything uncertain requires the explicit switch path.
func (c *emailCodeController) sessionRelation(ctx context.Context, email, humanID string) string {
	record, err := c.firebase.GetUserByEmail(ctx, email)
	if err != nil || record == nil || record.UID == "" {
		return "other_account"
	}
	boundHuman, ok, err := c.store.HumanForFirebaseUID(ctx, record.UID)
	if err == nil && ok && boundHuman == humanID {
		return "same_account"
	}
	return "other_account"
}

func (c *emailCodeController) CompleteEmailLink(ctx context.Context, request agentevents.CompleteEmailLinkRequest, session *agentevents.UserSessionClaims) (agentevents.EmailProofResult, error) {
	proof, err := c.store.CompleteEmailLink(ctx, request.ChallengeID, request.Token, request.Nonce, request.Adopt, request.BrowserEpochHash)
	if err != nil {
		return agentevents.EmailProofResult{}, mapFlowError(err)
	}
	return c.finishProof(ctx, proof, request.Nonce, session)
}

// finishProof crosses the Firebase boundary after the mailbox proof has
// committed. Every failure here is retryable by the same flow authority: the
// retry skips the code, reuses the bound UID, and mints a fresh custom token.
// The session claims only identify the currently signed-in Human, letting the
// same Human enable verified email on their own bound unverified principal.
func (c *emailCodeController) finishProof(ctx context.Context, proof koseki.EmailProof, nonce string, session *agentevents.UserSessionClaims) (agentevents.EmailProofResult, error) {
	uid := proof.UID
	if uid == "" {
		resolved, err := c.resolvePrincipal(ctx, proof.NormalizedEmail, session)
		if err != nil {
			return agentevents.EmailProofResult{}, err
		}
		bound, err := c.store.BindEmailProofUID(ctx, proof.FlowID, nonce, resolved)
		if err != nil {
			return agentevents.EmailProofResult{}, mapFlowError(err)
		}
		uid = bound.UID
	} else {
		record, err := c.firebase.GetUser(ctx, uid)
		if firebaseauth.IsUserNotFound(err) {
			return agentevents.EmailProofResult{}, agentevents.ErrBrowserAuthFlowProof
		}
		if err != nil {
			return agentevents.EmailProofResult{}, agentevents.ErrBrowserAuthProviderUnavailable
		}
		if !verifiedPrincipalFor(record, proof.NormalizedEmail) {
			return agentevents.EmailProofResult{}, agentevents.ErrBrowserAuthFlowProof
		}
	}
	token, err := c.firebase.CustomToken(ctx, uid)
	if err != nil || token == "" {
		return agentevents.EmailProofResult{}, agentevents.ErrBrowserAuthProviderUnavailable
	}
	humanID := proof.HumanID
	if humanID == "" {
		if bound, ok, err := c.store.HumanForFirebaseUID(ctx, uid); err == nil && ok {
			humanID = bound
		}
	}
	return agentevents.EmailProofResult{
		FlowID: proof.FlowID, Intent: string(proof.Intent), CustomToken: token, HumanID: humanID,
	}, nil
}

func verifiedPrincipalFor(record *firebaseauth.UserRecord, email string) bool {
	if record == nil || record.UserInfo == nil || record.UID == "" || record.Disabled || !record.EmailVerified {
		return false
	}
	normalized, err := koseki.NormalizeEmail(record.Email)
	return err == nil && normalized == email
}

// resolvePrincipal returns the one Firebase user that owns a proved address.
// Firebase keeps one account per email, so concurrent creation converges on
// the same UID through EMAIL_EXISTS. A principal whose email Firebase has not
// verified may belong to someone who never proved this mailbox:
//   - bound to a Human: fail closed; that Human keeps its linked providers.
//     Exception: when the request carries a live session for exactly that
//     bound Human, the mailbox proof enables verified email on the same UID —
//     the owner proving their own mailbox, never a takeover;
//   - never bound: delete it under the credential lock and create a verified
//     principal, so no unverified sign-in method survives the mailbox proof.
func (c *emailCodeController) resolvePrincipal(ctx context.Context, email string, session *agentevents.UserSessionClaims) (string, error) {
	for attempt := 0; attempt < emailPrincipalAttempts; attempt++ {
		record, err := c.firebase.GetUserByEmail(ctx, email)
		if firebaseauth.IsUserNotFound(err) {
			record, err = c.firebase.CreateUser(ctx, (&firebaseauth.UserToCreate{}).Email(email).EmailVerified(true))
			if firebaseauth.IsEmailAlreadyExists(err) {
				continue
			}
		}
		if err != nil || record == nil || record.UserInfo == nil {
			return "", agentevents.ErrBrowserAuthProviderUnavailable
		}
		normalized, normalizeErr := koseki.NormalizeEmail(record.Email)
		if normalizeErr != nil || normalized != email {
			return "", agentevents.ErrBrowserAuthProviderUnavailable
		}
		if record.Disabled {
			return "", agentevents.ErrBrowserAuthFlowProof
		}
		if record.EmailVerified {
			return record.UID, nil
		}
		uid := record.UID
		err = c.store.DeleteUnboundFirebasePrincipal(ctx, uid, func(ctx context.Context) error {
			if err := c.firebase.DeleteUser(ctx, uid); err != nil && !firebaseauth.IsUserNotFound(err) {
				return err
			}
			return nil
		})
		if errors.Is(err, koseki.ErrCredentialAlreadyBound) {
			if session != nil {
				boundHuman, ok, lookupErr := c.store.HumanForFirebaseUID(ctx, uid)
				if lookupErr == nil && ok && boundHuman == session.UserID {
					updated, updateErr := c.firebase.UpdateUser(ctx, uid,
						(&firebaseauth.UserToUpdate{}).EmailVerified(true))
					if updateErr != nil {
						return "", agentevents.ErrBrowserAuthProviderUnavailable
					}
					if updated != nil && updated.EmailVerified {
						return uid, nil
					}
				}
			}
			return "", &agentevents.BrowserEmailUnverifiedAccountError{SignInProviders: browserSignInProviders(record)}
		}
		if err != nil {
			return "", agentevents.ErrBrowserAuthProviderUnavailable
		}
	}
	return "", agentevents.ErrBrowserAuthProviderUnavailable
}

// browserSignInProviders lists the providers linked to the principal that the
// login screen offers. Signing in with one of them yields this same Firebase
// UID, which resolves to the bound Human.
func browserSignInProviders(record *firebaseauth.UserRecord) []string {
	providers := []string{}
	for _, id := range []string{"google.com", "github.com"} {
		for _, info := range record.ProviderUserInfo {
			if info != nil && info.ProviderID == id {
				providers = append(providers, id)
				break
			}
		}
	}
	return providers
}

type emailDeliveryWorker struct {
	store      *koseki.Store
	sender     authemail.Sender
	linkOrigin string
	wakeCh     chan struct{}
	now        func() time.Time
}

func newEmailDeliveryWorker(store *koseki.Store, sender authemail.Sender, linkOrigin string) *emailDeliveryWorker {
	return &emailDeliveryWorker{store: store, sender: sender, linkOrigin: linkOrigin, wakeCh: make(chan struct{}, 1), now: time.Now}
}

func (w *emailDeliveryWorker) wake() {
	select {
	case w.wakeCh <- struct{}{}:
	default:
	}
}

func (w *emailDeliveryWorker) run(ctx context.Context) {
	ticker := time.NewTicker(emailDeliveryPollInterval)
	defer ticker.Stop()
	for {
		w.drain(ctx)
		select {
		case <-ctx.Done():
			return
		case <-w.wakeCh:
		case <-ticker.C:
		}
	}
}

func (w *emailDeliveryWorker) drain(ctx context.Context) {
	for ctx.Err() == nil {
		items, err := w.store.ClaimEmailDeliveries(ctx, 10, emailDeliveryLease)
		if err != nil {
			if ctx.Err() == nil {
				log.Printf("auth email delivery claim failed: %v", err)
			}
			return
		}
		if len(items) == 0 {
			return
		}
		for _, item := range items {
			w.deliver(ctx, item)
		}
	}
}

// deliver never logs the message, code, or link.
func (w *emailDeliveryWorker) deliver(ctx context.Context, item koseki.EmailDelivery) {
	code, token, err := w.store.EmailChallengeKey.EmailChallengeSecrets(item.ChallengeID)
	var sendErr error
	if err != nil {
		sendErr = authemail.Permanent(err)
	} else {
		message := authemail.RenderChallenge(authemail.Challenge{
			To: item.NormalizedEmail, Code: code, LinkURL: emailLinkURL(w.linkOrigin, item.ChallengeID, token),
			ExpiresAt: item.ChallengeExpires, SentAt: item.CreatedAt,
		})
		sendCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		sendErr = w.sender.Send(sendCtx, message)
		cancel()
	}
	permanent := authemail.IsPermanent(sendErr)
	var retryAt time.Time
	outcome := "sent"
	if sendErr != nil {
		outcome = "failed"
		if !permanent && item.Attempts < koseki.EmailDeliveryMaxAttempts {
			retryAt = w.now().Add(time.Duration(15<<(item.Attempts-1)) * time.Second)
			outcome = "retry scheduled"
		}
	}
	if err := w.store.FinishEmailDelivery(ctx, item.DeliveryID, item.Attempts, sendErr, permanent, retryAt); err != nil {
		log.Printf("auth email delivery %s attempt %d: record result: %v", item.DeliveryID, item.Attempts, err)
		return
	}
	log.Printf("auth email delivery %s attempt %d: %s", item.DeliveryID, item.Attempts, outcome)
}

// The link opens an SPA route outside /auth. Secrets stay in the fragment, so
// they are not sent to the server with the page request.
func emailLinkURL(origin, challengeID, token string) string {
	return origin + "/email-sign-in#challenge=" + url.QueryEscape(challengeID) + "&token=" + url.QueryEscape(token)
}

func (a *application) startEmailDelivery() {
	if a.emailDelivery == nil {
		return
	}
	a.attentionWorkers.Add(1)
	go func() { defer a.attentionWorkers.Done(); a.emailDelivery.run(a.backgroundCtx) }()
}

func emailDeliveryWorkerFor(server *agentevents.BrowserAuthServer) *emailDeliveryWorker {
	if server == nil {
		return nil
	}
	controller, ok := server.EmailFlows.(*emailCodeController)
	if !ok || controller == nil {
		return nil
	}
	return controller.delivery
}

type emailAuthConfig struct {
	key        *koseki.EmailChallengeKey
	sender     authemail.Sender
	linkOrigin string
}

var emailChallengeKeyIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

// emailAuthConfigFromEnv enables email sign-in only when the key, key ID,
// sender, and link origin are configured together. The development mailbox is
// accepted only for local insecure-cookie or Auth emulator stacks.
func emailAuthConfigFromEnv(allowedOrigins []string, secureCookies bool) (*emailAuthConfig, error) {
	rawKey := strings.TrimSpace(os.Getenv("SUMI_AUTH_EMAIL_CHALLENGE_KEY"))
	keyID := strings.TrimSpace(os.Getenv("SUMI_AUTH_EMAIL_CHALLENGE_KEY_ID"))
	senderName := strings.TrimSpace(os.Getenv("SUMI_AUTH_EMAIL_SENDER"))
	linkOrigin := strings.TrimSpace(os.Getenv("SUMI_AUTH_EMAIL_LINK_ORIGIN"))
	if rawKey == "" && keyID == "" && senderName == "" && linkOrigin == "" {
		return nil, nil
	}
	if rawKey == "" || keyID == "" || senderName == "" || linkOrigin == "" {
		return nil, errors.New("SUMI_AUTH_EMAIL_CHALLENGE_KEY, SUMI_AUTH_EMAIL_CHALLENGE_KEY_ID, SUMI_AUTH_EMAIL_SENDER, and SUMI_AUTH_EMAIL_LINK_ORIGIN must be configured together")
	}
	key, err := base64.StdEncoding.DecodeString(rawKey)
	if err != nil {
		key, err = base64.RawURLEncoding.DecodeString(rawKey)
	}
	if err != nil || len(key) < 32 {
		return nil, errors.New("SUMI_AUTH_EMAIL_CHALLENGE_KEY must be base64 for at least 32 bytes")
	}
	if !emailChallengeKeyIDPattern.MatchString(keyID) {
		return nil, errors.New("SUMI_AUTH_EMAIL_CHALLENGE_KEY_ID must be 1-64 letters, digits, dot, underscore, or hyphen")
	}
	originAllowed := false
	for _, origin := range allowedOrigins {
		if origin == linkOrigin {
			originAllowed = true
		}
	}
	if !originAllowed {
		return nil, errors.New("SUMI_AUTH_EMAIL_LINK_ORIGIN must be one of the browser origins")
	}
	var sender authemail.Sender
	switch senderName {
	case "dev-mailbox":
		dir := strings.TrimSpace(os.Getenv("SUMI_AUTH_EMAIL_DEV_MAILBOX_DIR"))
		if !filepath.IsAbs(dir) {
			return nil, errors.New("SUMI_AUTH_EMAIL_DEV_MAILBOX_DIR must be an absolute path for the dev-mailbox sender")
		}
		if secureCookies && strings.TrimSpace(os.Getenv("FIREBASE_AUTH_EMULATOR_HOST")) == "" {
			return nil, errors.New("the dev-mailbox sender is local-only; it requires insecure local cookies or the Firebase Auth emulator")
		}
		log.Printf("auth email uses the local development mailbox")
		sender = &authemail.DevMailbox{Dir: dir}
	default:
		return nil, fmt.Errorf("unsupported SUMI_AUTH_EMAIL_SENDER %q", senderName)
	}
	return &emailAuthConfig{key: &koseki.EmailChallengeKey{KeyID: keyID, Key: key}, sender: sender, linkOrigin: linkOrigin}, nil
}
