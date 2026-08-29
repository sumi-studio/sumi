package messaging

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"mime"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	webpush "github.com/SherClockHolmes/webpush-go"
	"github.com/jackc/pgx/v5"
	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	applicationapps "github.com/sumi-studio/sumi/apps/api/internal/apps"
	workspacecontrol "github.com/sumi-studio/sumi/apps/api/internal/workspace"
)

const (
	maxPushEndpointBytes         = 2000
	maxPushP256dhBytes           = 200
	maxPushAuthBytes             = 100
	maxPushSubscriptionsPerHuman = 8
	pushTTL                      = 3600
	pushDeliveryTimeout          = 20 * time.Second
	pushEndpointDeliveryTimeout  = 10 * time.Second
	pushFanoutConcurrency        = 4
	pushSessionCleanupTimeout    = 500 * time.Millisecond
)

type VAPIDKeys struct {
	Public  string
	Private string
}

type PushSubscription struct {
	SubscriptionID  string
	Human           ParticipantRef
	Session         agentevents.BrowserSessionIdentity
	Endpoint        string
	P256dh          string
	Auth            string
	OwnerGeneration int64
	CreatedAt       time.Time
}

var (
	ErrInvalidPushSubscription = errors.New("invalid push subscription")
	ErrPushSubscriptionOwned   = errors.New("push subscription belongs to another browser")
	ErrPushSubscriptionLimit   = errors.New("push subscription limit reached")
)

func (s *Store) EnsureVAPIDKeys(ctx context.Context) (VAPIDKeys, error) {
	var keys VAPIDKeys
	err := s.pool.QueryRow(ctx,
		"SELECT public_key, private_key FROM push_vapid_keys WHERE singleton").
		Scan(&keys.Public, &keys.Private)
	if err == nil {
		return keys, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return VAPIDKeys{}, fmt.Errorf("load VAPID keys: %w", err)
	}
	privateKey, publicKey, err := webpush.GenerateVAPIDKeys()
	if err != nil {
		return VAPIDKeys{}, fmt.Errorf("generate VAPID keys: %w", err)
	}
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO push_vapid_keys (singleton, public_key, private_key)
		VALUES (true, $1, $2)
		ON CONFLICT (singleton) DO NOTHING`, publicKey, privateKey); err != nil {
		return VAPIDKeys{}, fmt.Errorf("insert VAPID keys: %w", err)
	}
	if err := s.pool.QueryRow(ctx,
		"SELECT public_key, private_key FROM push_vapid_keys WHERE singleton").
		Scan(&keys.Public, &keys.Private); err != nil {
		return VAPIDKeys{}, fmt.Errorf("reload VAPID keys: %w", err)
	}
	return keys, nil
}

func (s *ScopedStore) SavePushSubscription(
	ctx context.Context,
	session agentevents.BrowserSessionIdentity,
	endpoint, p256dh, auth string,
) (PushSubscription, error) {
	owner := s.Scope.Actor
	if owner.Kind != KindHuman || session.ID == "" || session.ExpiresAt.IsZero() {
		return PushSubscription{}, ErrInvalidPushSubscription
	}
	endpoint = strings.TrimSpace(endpoint)
	p256dh = strings.TrimSpace(p256dh)
	auth = strings.TrimSpace(auth)
	if err := validatePushSubscriptionFields(endpoint, p256dh, auth); err != nil {
		return PushSubscription{}, err
	}
	if err := s.Store.pushEgressPolicy().allowEndpoint(ctx, endpoint); err != nil {
		return PushSubscription{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return PushSubscription{}, fmt.Errorf("begin push subscription save: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := s.authorizeMutationInTx(ctx, tx); err != nil {
		return PushSubscription{}, err
	}
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtext($1))", owner.ID); err != nil {
		return PushSubscription{}, fmt.Errorf("lock push owner: %w", err)
	}
	if err := lockAndPurgeExpiredPushSubscriptions(ctx, tx, owner.ID, endpoint); err != nil {
		return PushSubscription{}, err
	}
	var otherEndpoints int
	if err := tx.QueryRow(ctx, `
		SELECT count(*) FROM push_subscriptions
		WHERE human_id = $1 AND endpoint <> $2 AND session_expires_at > now()`, owner.ID, endpoint).
		Scan(&otherEndpoints); err != nil {
		return PushSubscription{}, fmt.Errorf("count push subscriptions: %w", err)
	}
	if otherEndpoints >= maxPushSubscriptionsPerHuman {
		return PushSubscription{}, ErrPushSubscriptionLimit
	}
	subscription := PushSubscription{
		SubscriptionID: newUUIDv7(),
		Human:          owner,
		Session:        session,
		Endpoint:       endpoint,
		P256dh:         p256dh,
		Auth:           auth,
	}
	err = tx.QueryRow(ctx, `
		INSERT INTO push_subscriptions
		  (subscription_id, human_id, browser_session_id, session_expires_at,
		   endpoint, p256dh, auth)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (endpoint) DO UPDATE SET
		  human_id = EXCLUDED.human_id,
		  browser_session_id = EXCLUDED.browser_session_id,
		  session_expires_at = EXCLUDED.session_expires_at,
		  p256dh = EXCLUDED.p256dh,
		  auth = EXCLUDED.auth,
		  owner_generation = CASE
		    WHEN push_subscriptions.human_id = EXCLUDED.human_id
		     AND push_subscriptions.browser_session_id = EXCLUDED.browser_session_id
		    THEN push_subscriptions.owner_generation
		    ELSE push_subscriptions.owner_generation + 1
		  END,
		  updated_at = now()
		WHERE (push_subscriptions.human_id = EXCLUDED.human_id
		       AND push_subscriptions.browser_session_id = EXCLUDED.browser_session_id)
		   OR (push_subscriptions.p256dh = EXCLUDED.p256dh
		       AND push_subscriptions.auth = EXCLUDED.auth)
		RETURNING subscription_id, owner_generation, created_at`,
		subscription.SubscriptionID,
		owner.ID,
		session.ID,
		session.ExpiresAt,
		endpoint,
		p256dh,
		auth,
	).Scan(
		&subscription.SubscriptionID,
		&subscription.OwnerGeneration,
		&subscription.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return PushSubscription{}, ErrPushSubscriptionOwned
	}
	if err != nil {
		return PushSubscription{}, fmt.Errorf("save push subscription: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return PushSubscription{}, fmt.Errorf("commit push subscription save: %w", err)
	}
	return subscription, nil
}

// lockAndPurgeExpiredPushSubscriptions lazily bounds durable endpoint state at
// the next save by this Human. Endpoint locks preserve the same transfer/send
// fence as normal ownership changes; a global janitor is unnecessary.
func lockAndPurgeExpiredPushSubscriptions(
	ctx context.Context,
	tx pgx.Tx,
	humanID, currentEndpoint string,
) error {
	rows, err := tx.Query(ctx, `
		SELECT endpoint FROM push_subscriptions
		WHERE human_id = $1 AND session_expires_at <= now()
		ORDER BY endpoint`, humanID)
	if err != nil {
		return fmt.Errorf("list expired push subscriptions: %w", err)
	}
	endpoints := []string{currentEndpoint}
	for rows.Next() {
		var endpoint string
		if err := rows.Scan(&endpoint); err != nil {
			rows.Close()
			return fmt.Errorf("scan expired push subscription: %w", err)
		}
		endpoints = append(endpoints, endpoint)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate expired push subscriptions: %w", err)
	}
	rows.Close()
	sort.Strings(endpoints)
	last := ""
	for _, endpoint := range endpoints {
		if endpoint == last {
			continue
		}
		if _, err := tx.Exec(ctx,
			"SELECT pg_advisory_xact_lock(hashtext($1))", endpoint,
		); err != nil {
			return fmt.Errorf("lock push endpoint: %w", err)
		}
		last = endpoint
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM push_subscriptions
		WHERE session_expires_at <= now()
		  AND (human_id = $1 OR endpoint = $2)`, humanID, currentEndpoint); err != nil {
		return fmt.Errorf("purge expired push subscriptions: %w", err)
	}
	return nil
}

func (s *ScopedStore) DeletePushSubscription(
	ctx context.Context,
	session agentevents.BrowserSessionIdentity,
	endpoint string,
) error {
	owner := s.Scope.Actor
	endpoint = strings.TrimSpace(endpoint)
	if owner.Kind != KindHuman || session.ID == "" || endpoint == "" || len(endpoint) > maxPushEndpointBytes {
		return ErrInvalidPushSubscription
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin push subscription delete: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := s.authorizeMutationInTx(ctx, tx); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtext($1))", endpoint); err != nil {
		return fmt.Errorf("lock push endpoint: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM push_subscriptions
		WHERE human_id = $1 AND browser_session_id = $2 AND endpoint = $3`,
		owner.ID, session.ID, endpoint); err != nil {
		return fmt.Errorf("delete push subscription: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit push subscription delete: %w", err)
	}
	return nil
}

// CloseBrowserSession removes delivery rows for the exact retired session.
// Correctness does not depend on this best-effort cleanup: every send is also
// admitted through the durable session lease. Keep cleanup bounded so it can
// never silently hold logout or session rotation open.
func (s *Store) CloseBrowserSession(sessionID string) {
	if s == nil || sessionID == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), pushSessionCleanupTimeout)
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		log.Printf("messaging push: begin retired-session cleanup: %v", err)
		return
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	rows, err := tx.Query(ctx, `
		SELECT endpoint FROM push_subscriptions
		WHERE browser_session_id = $1 ORDER BY endpoint`, sessionID)
	if err != nil {
		log.Printf("messaging push: list retired-session endpoints: %v", err)
		return
	}
	var endpoints []string
	for rows.Next() {
		var endpoint string
		if err := rows.Scan(&endpoint); err != nil {
			rows.Close()
			log.Printf("messaging push: scan retired-session endpoint: %v", err)
			return
		}
		endpoints = append(endpoints, endpoint)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		log.Printf("messaging push: iterate retired-session endpoints: %v", err)
		return
	}
	rows.Close()
	for _, endpoint := range endpoints {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtext($1))", endpoint); err != nil {
			log.Printf("messaging push: lock retired-session endpoint: %v", err)
			return
		}
	}
	if _, err := tx.Exec(ctx,
		"DELETE FROM push_subscriptions WHERE browser_session_id = $1", sessionID); err != nil {
		log.Printf("messaging push: delete retired-session subscriptions: %v", err)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		log.Printf("messaging push: commit retired-session cleanup: %v", err)
	}
}

func (s *Store) pushSubscriptionsForWith(
	ctx context.Context,
	q querier,
	recipients []ParticipantRef,
) (map[string][]PushSubscription, error) {
	humanIDs := make([]string, 0, len(recipients))
	for _, recipient := range recipients {
		if recipient.Kind == KindHuman {
			humanIDs = append(humanIDs, recipient.ID)
		}
	}
	out := map[string][]PushSubscription{}
	if len(humanIDs) == 0 {
		return out, nil
	}
	rows, err := q.Query(ctx, `
		SELECT subscription_id, human_id, browser_session_id, session_expires_at,
		       endpoint, p256dh, auth, owner_generation, created_at
		FROM push_subscriptions
		WHERE human_id = ANY($1) AND session_expires_at > now()
		ORDER BY created_at`, humanIDs)
	if err != nil {
		return nil, fmt.Errorf("query push subscriptions: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var subscription PushSubscription
		var humanID string
		if err := rows.Scan(
			&subscription.SubscriptionID,
			&humanID,
			&subscription.Session.ID,
			&subscription.Session.ExpiresAt,
			&subscription.Endpoint,
			&subscription.P256dh,
			&subscription.Auth,
			&subscription.OwnerGeneration,
			&subscription.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan push subscription: %w", err)
		}
		subscription.Human = Human(humanID)
		out[subscription.Human.Key()] = append(out[subscription.Human.Key()], subscription)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate push subscriptions: %w", err)
	}
	return out, nil
}

func validatePushSubscriptionFields(endpoint, p256dh, auth string) error {
	switch {
	case endpoint == "" || len(endpoint) > maxPushEndpointBytes:
		return fmt.Errorf("%w: endpoint", ErrInvalidPushSubscription)
	case p256dh == "" || len(p256dh) > maxPushP256dhBytes:
		return fmt.Errorf("%w: p256dh", ErrInvalidPushSubscription)
	case auth == "" || len(auth) > maxPushAuthBytes:
		return fmt.Errorf("%w: auth", ErrInvalidPushSubscription)
	default:
		return nil
	}
}

// PushPayload is deliberately pointer-only. No message text, attachment name,
// participant/place display name, reason, or sequence leaves the API process.
type PushPayload struct {
	WorkspaceID string `json:"workspace_id"`
	PlaceID     string `json:"place_id"`
	PlaceKind   string `json:"place_kind"`
}

type pushHTTPClient interface {
	Do(*http.Request) (*http.Response, error)
}

type PushDispatcher struct {
	store           *Store
	sessions        agentevents.BrowserSessionIdentityAuthorizer
	keys            VAPIDKeys
	subject         string
	client          pushHTTPClient
	endpointTimeout time.Duration
}

func NewPushDispatcher(
	ctx context.Context,
	store *Store,
	sessions agentevents.BrowserSessionIdentityAuthorizer,
	subject string,
) (*PushDispatcher, error) {
	if store == nil || sessions == nil {
		return nil, errors.New("push dispatcher requires store and session authorization")
	}
	normalized, err := normalizeVAPIDSubject(subject)
	if err != nil {
		return nil, err
	}
	if normalized == "" {
		return nil, errors.New("push dispatcher requires a VAPID subject")
	}
	keys, err := store.EnsureVAPIDKeys(ctx)
	if err != nil {
		return nil, err
	}
	return &PushDispatcher{
		store: store, sessions: sessions, keys: keys,
		subject: normalized, client: newPushHTTPClient(),
		endpointTimeout: pushEndpointDeliveryTimeout,
	}, nil
}

func normalizeVAPIDSubject(subject string) (string, error) {
	subject = strings.TrimSpace(subject)
	if subject == "" {
		return "", nil
	}
	if strings.HasPrefix(subject, "https://") {
		parsed, err := url.ParseRequestURI(subject)
		if err != nil || strings.Contains(subject, "#") || !parsed.IsAbs() || parsed.Scheme != "https" ||
			parsed.Host == "" || parsed.Hostname() == "" || parsed.User != nil ||
			parsed.Fragment != "" || parsed.Opaque != "" {
			return "", errors.New("VAPID subject must be a mailto: address or absolute HTTPS URL")
		}
		return subject, nil
	}
	if strings.HasPrefix(subject, "mailto:") {
		subject = strings.TrimPrefix(subject, "mailto:")
	}
	if subject == "" || !strings.Contains(subject, "@") {
		return "", errors.New("VAPID subject must be a mailto: address or HTTPS URL")
	}
	return subject, nil
}

func (d *PushDispatcher) PublicKey() string {
	if d == nil {
		return ""
	}
	return d.keys.Public
}

func (s *Store) UsePush(dispatcher *PushDispatcher) {
	s.push = dispatcher
}

func (s *ScopedStore) deliverPush(
	ctx context.Context,
	place Place,
	decisions []NotificationDecision,
) {
	if s == nil || s.Store == nil || s.Store.push == nil || len(decisions) == 0 {
		return
	}
	dispatcher := s.Store.push
	detached, cancel := context.WithTimeout(context.WithoutCancel(ctx), pushDeliveryTimeout)
	go func() {
		defer cancel()
		dispatcher.deliver(detached, s.Scope, place, decisions)
	}()
}

type pushDelivery struct {
	subscription      PushSubscription
	payload           []byte
	workspaceID       string
	installationID    string
	authorityEpoch    int64
	placeID           string
	placeKind         string
	workspaceMemberID string
	placeMemberID     string
}

func (d *PushDispatcher) deliver(
	ctx context.Context,
	scope Scope,
	place Place,
	decisions []NotificationDecision,
) {
	var deliveries []pushDelivery
	err := d.store.withLiveAudienceInTx(
		ctx,
		scope,
		liveBoundary{placeID: place.PlaceID},
		false,
		func(tx pgx.Tx, audience map[ParticipantRef]struct{}) error {
			humans := make([]NotificationDecision, 0, len(decisions))
			recipients := make([]ParticipantRef, 0, len(decisions))
			for _, decision := range decisions {
				if decision.Participant.Kind == KindHuman {
					if _, allowed := audience[decision.Participant]; allowed {
						humans = append(humans, decision)
						recipients = append(recipients, decision.Participant)
					}
				}
			}
			subscriptions, err := d.store.pushSubscriptionsForWith(ctx, tx, recipients)
			if err != nil {
				return err
			}
			payload, err := json.Marshal(PushPayload{
				WorkspaceID: scope.WorkspaceID,
				PlaceID:     place.PlaceID,
				PlaceKind:   place.Kind,
			})
			if err != nil {
				return fmt.Errorf("encode generic push payload: %w", err)
			}
			deliveries = append(deliveries, pushDeliveriesRoundRobin(
				scope, place, humans, subscriptions, payload,
			)...)
			return nil
		},
	)
	if err != nil {
		log.Printf("messaging push: reauthorize audience: %v", err)
		return
	}
	d.sendDeliveries(ctx, deliveries)
}

// pushDeliveriesRoundRobin prevents one Human's maximum device set from
// occupying every worker for consecutive timeout waves. Each active Human's
// first endpoint is attempted before any Human's second endpoint.
func pushDeliveriesRoundRobin(
	scope Scope,
	place Place,
	humans []NotificationDecision,
	subscriptions map[string][]PushSubscription,
	payload []byte,
) []pushDelivery {
	deliveries := make([]pushDelivery, 0)
	for endpointIndex := 0; ; endpointIndex++ {
		appended := false
		for _, human := range humans {
			owned := subscriptions[human.Participant.Key()]
			if endpointIndex >= len(owned) {
				continue
			}
			deliveries = append(deliveries, pushDelivery{
				subscription:      owned[endpointIndex],
				payload:           payload,
				workspaceID:       scope.WorkspaceID,
				installationID:    scope.InstallationID,
				authorityEpoch:    scope.AuthorityEpoch,
				placeID:           place.PlaceID,
				placeKind:         place.Kind,
				workspaceMemberID: human.workspaceMemberID,
				placeMemberID:     human.placeMemberID,
			})
			appended = true
		}
		if !appended {
			return deliveries
		}
	}
}

func (d *PushDispatcher) sendDeliveries(ctx context.Context, deliveries []pushDelivery) {
	runBoundedPushFanout(ctx, deliveries, d.endpointTimeout, d.send)
}

func runBoundedPushFanout(
	ctx context.Context,
	deliveries []pushDelivery,
	endpointTimeout time.Duration,
	send func(context.Context, pushDelivery),
) {
	if len(deliveries) == 0 {
		return
	}
	workers := min(pushFanoutConcurrency, len(deliveries))
	jobs := make(chan pushDelivery)
	var wait sync.WaitGroup
	wait.Add(workers)
	for range workers {
		go func() {
			defer wait.Done()
			for delivery := range jobs {
				endpointCtx, cancel := context.WithTimeout(ctx, endpointTimeout)
				send(endpointCtx, delivery)
				cancel()
			}
		}()
	}
	for _, delivery := range deliveries {
		select {
		case jobs <- delivery:
		case <-ctx.Done():
			close(jobs)
			wait.Wait()
			return
		}
	}
	close(jobs)
	wait.Wait()
}

type pushEndpointSendLease struct {
	tx pgx.Tx
}

func (l *pushEndpointSendLease) release() {
	if l == nil || l.tx == nil {
		return
	}
	_ = l.tx.Rollback(context.Background())
	l.tx = nil
}

func (d *PushDispatcher) acquirePushSendLease(
	ctx context.Context,
	delivery pushDelivery,
) (*pushEndpointSendLease, bool, error) {
	tx, err := d.store.pool.Begin(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("begin push endpoint lease: %w", err)
	}
	lease := &pushEndpointSendLease{tx: tx}
	address := Scope{
		WorkspaceID:    delivery.workspaceID,
		InstallationID: delivery.installationID,
		AuthorityEpoch: delivery.authorityEpoch,
	}
	if err := address.validateAddress(); err != nil {
		lease.release()
		return nil, false, nil
	}
	if d.store.workspaces == nil || d.store.apps == nil {
		lease.release()
		return nil, false, errors.New("push authority dependencies are unavailable")
	}
	if err := d.store.workspaces.LockSharedInTx(ctx, tx, delivery.workspaceID); err != nil {
		lease.release()
		if errors.Is(err, workspacecontrol.ErrNotFound) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("lock push workspace: %w", err)
	}
	if _, err := d.store.apps.RequireEnabledInstallationEpochInTx(
		ctx,
		tx,
		delivery.installationID,
		delivery.authorityEpoch,
		applicationapps.WorkspaceOwner(delivery.workspaceID),
		MessagingAppID,
	); err != nil {
		lease.release()
		if errors.Is(err, applicationapps.ErrInstallationNotFound) ||
			errors.Is(err, applicationapps.ErrAppDisabled) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("lock push installation: %w", err)
	}
	if ok, err := lockExactPushAudience(ctx, tx, delivery); err != nil {
		lease.release()
		return nil, false, err
	} else if !ok {
		lease.release()
		return nil, false, nil
	}
	if _, err := tx.Exec(ctx,
		"SELECT pg_advisory_xact_lock(hashtext($1))",
		delivery.subscription.Endpoint,
	); err != nil {
		lease.release()
		return nil, false, fmt.Errorf("lock push endpoint: %w", err)
	}
	var present int
	err = tx.QueryRow(ctx, `
		SELECT 1 FROM push_subscriptions
		WHERE endpoint = $1 AND subscription_id = $2 AND human_id = $3
		  AND browser_session_id = $4 AND owner_generation = $5`,
		delivery.subscription.Endpoint,
		delivery.subscription.SubscriptionID,
		delivery.subscription.Human.ID,
		delivery.subscription.Session.ID,
		delivery.subscription.OwnerGeneration,
	).Scan(&present)
	if errors.Is(err, pgx.ErrNoRows) {
		lease.release()
		return nil, false, nil
	}
	if err != nil {
		lease.release()
		return nil, false, fmt.Errorf("recheck push subscription: %w", err)
	}
	return lease, true, nil
}

// lockExactPushAudience fences the network send against removal of the exact
// tenure that created the intent. Remove/leave takes FOR UPDATE on these rows;
// whichever side acquires its lock first defines whether this send may start.
func lockExactPushAudience(ctx context.Context, tx pgx.Tx, delivery pushDelivery) (bool, error) {
	if delivery.workspaceID == "" || delivery.workspaceMemberID == "" ||
		delivery.placeID == "" {
		return false, nil
	}
	var locked string
	err := tx.QueryRow(ctx, `
		SELECT workspace_member_id FROM workspace_members
		WHERE workspace_member_id = $1 AND workspace_id = $2
		  AND member_kind = 'human' AND member_id = $3 AND left_at IS NULL
		FOR SHARE`,
		delivery.workspaceMemberID,
		delivery.workspaceID,
		delivery.subscription.Human.ID,
	).Scan(&locked)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("lock push workspace tenure: %w", err)
	}
	switch delivery.placeKind {
	case PlaceChannel:
		return delivery.placeMemberID == "", nil
	case PlaceDM, PlaceGroupDM:
		if delivery.placeMemberID == "" {
			return false, nil
		}
		err = tx.QueryRow(ctx, `
			SELECT place_member_id FROM place_members
			WHERE place_member_id = $1 AND workspace_id = $2 AND place_id = $3
			  AND workspace_member_id = $4
			  AND member_kind = 'human' AND member_id = $5 AND left_at IS NULL
			FOR SHARE`,
			delivery.placeMemberID,
			delivery.workspaceID,
			delivery.placeID,
			delivery.workspaceMemberID,
			delivery.subscription.Human.ID,
		).Scan(&locked)
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		if err != nil {
			return false, fmt.Errorf("lock push place tenure: %w", err)
		}
		return true, nil
	default:
		return false, nil
	}
}

func pushSendFailureReason(err error) string {
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return "dns"
	}
	var unknownAuthority x509.UnknownAuthorityError
	var hostnameError x509.HostnameError
	var certificateInvalid x509.CertificateInvalidError
	var recordHeader tls.RecordHeaderError
	if errors.As(err, &unknownAuthority) || errors.As(err, &hostnameError) ||
		errors.As(err, &certificateInvalid) || errors.As(err, &recordHeader) {
		return "tls"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	var networkErr net.Error
	if errors.As(err, &networkErr) && networkErr.Timeout() {
		return "timeout"
	}
	return "transport"
}

func (d *PushDispatcher) send(ctx context.Context, delivery pushDelivery) {
	lease, ok, err := d.acquirePushSendLease(ctx, delivery)
	if err != nil {
		log.Printf("messaging push: acquire endpoint lease: %v", err)
		return
	}
	if !ok {
		return
	}
	var response *http.Response
	var sendErr error
	attempted := false
	authorizeErr := d.sessions.AuthorizeBrowserSessionIdentity(
		ctx,
		delivery.subscription.Session,
		func() error {
			attempted = true
			response, sendErr = webpush.SendNotificationWithContext(
				ctx,
				delivery.payload,
				&webpush.Subscription{
					Endpoint: delivery.subscription.Endpoint,
					Keys: webpush.Keys{
						P256dh: delivery.subscription.P256dh,
						Auth:   delivery.subscription.Auth,
					},
				},
				&webpush.Options{
					HTTPClient:      d.client,
					Subscriber:      d.subject,
					VAPIDPublicKey:  d.keys.Public,
					VAPIDPrivateKey: d.keys.Private,
					TTL:             pushTTL,
					Urgency:         webpush.UrgencyHigh,
				},
			)
			return nil
		},
	)
	if authorizeErr != nil {
		lease.release()
		if attempted {
			log.Printf("messaging push: session delivery lease: %v", authorizeErr)
		}
		return
	}
	if sendErr != nil {
		lease.release()
		if pushDialWasRefused(sendErr) {
			log.Print("messaging push: destination refused by egress policy")
			return
		}
		log.Printf("messaging push: send failed (%s)", pushSendFailureReason(sendErr))
		return
	}
	if response == nil {
		lease.release()
		log.Print("messaging push: transport returned no response")
		return
	}
	status := response.StatusCode
	if status == http.StatusNotFound || status == http.StatusGone {
		if _, err := lease.tx.Exec(ctx, `
			DELETE FROM push_subscriptions
			WHERE endpoint = $1 AND subscription_id = $2 AND human_id = $3
			  AND browser_session_id = $4 AND owner_generation = $5`,
			delivery.subscription.Endpoint,
			delivery.subscription.SubscriptionID,
			delivery.subscription.Human.ID,
			delivery.subscription.Session.ID,
			delivery.subscription.OwnerGeneration,
		); err != nil {
			log.Printf("messaging push: delete expired endpoint: %v", err)
			lease.release()
		} else if err := lease.tx.Commit(ctx); err != nil {
			log.Printf("messaging push: commit expired endpoint cleanup: %v", err)
			lease.release()
		} else {
			lease.tx = nil
		}
	} else {
		lease.release()
	}
	if response.Body != nil {
		_ = response.Body.Close()
	}
	if status != http.StatusNotFound && status != http.StatusGone &&
		(status < http.StatusOK || status >= http.StatusMultipleChoices) {
		log.Printf("messaging push: unexpected response status %d", status)
	}
}

func (s *Server) servePushKey(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.viewer(w, r); !ok {
		return
	}
	if s.Push == nil {
		writeError(w, http.StatusServiceUnavailable, "push_unavailable")
		return
	}
	writeJSON(w, http.StatusOK, struct {
		PublicKey string `json:"public_key"`
	}{PublicKey: s.Push.PublicKey()})
}

type pushSubscriptionWire struct {
	Endpoint string `json:"endpoint"`
	Keys     struct {
		P256dh string `json:"p256dh"`
		Auth   string `json:"auth"`
	} `json:"keys"`
}

func requirePushJSONContentType(w http.ResponseWriter, r *http.Request) bool {
	values := r.Header.Values("Content-Type")
	if len(values) != 1 {
		writeError(w, http.StatusUnsupportedMediaType, "application_json_required")
		return false
	}
	mediaType, _, err := mime.ParseMediaType(values[0])
	if err != nil || mediaType != "application/json" {
		writeError(w, http.StatusUnsupportedMediaType, "application_json_required")
		return false
	}
	return true
}

func (s *Server) serveSavePushSubscription(w http.ResponseWriter, r *http.Request) {
	_, claims, ok := s.viewer(w, r)
	if !ok {
		return
	}
	if !requirePushJSONContentType(w, r) {
		return
	}
	if s.Push == nil {
		writeError(w, http.StatusServiceUnavailable, "push_unavailable")
		return
	}
	var request pushSubscriptionWire
	if !decodeJSON(w, r, &request) {
		return
	}
	store := scopedStoreForRequest(r)
	done, err := s.mutate(w, r, claims, func() error {
		_, saveErr := store.SavePushSubscription(
			r.Context(),
			claims.BrowserSessionIdentity(),
			request.Endpoint,
			request.Keys.P256dh,
			request.Keys.Auth,
		)
		return saveErr
	})
	if !done {
		return
	}
	if err != nil {
		writeStoreError(w, err)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) serveDeletePushSubscription(w http.ResponseWriter, r *http.Request) {
	_, claims, ok := s.viewer(w, r)
	if !ok {
		return
	}
	if !requirePushJSONContentType(w, r) {
		return
	}
	var request struct {
		Endpoint string `json:"endpoint"`
	}
	if !decodeJSON(w, r, &request) {
		return
	}
	store := scopedStoreForRequest(r)
	done, err := s.mutate(w, r, claims, func() error {
		return store.DeletePushSubscription(
			r.Context(),
			claims.BrowserSessionIdentity(),
			request.Endpoint,
		)
	})
	if !done {
		return
	}
	if err != nil {
		writeStoreError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
