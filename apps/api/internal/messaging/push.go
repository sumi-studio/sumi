package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	webpush "github.com/SherClockHolmes/webpush-go"
	"github.com/jackc/pgx/v5"
)

// Web Push は 0015 の判定に足を生やす層であって、判定をやり直す層ではない。
// 「呼ぶかどうか」は NotificationDecisionsFor が送信時に一度だけ答えており、
// ここが決めるのは「その答えを、閉じたタブの向こうへどう届けるか」だけである。
// 凍結契約 v1「Push 通知レイヤーとの対応」の左列（人間）にあたる。右列（agent
// の AttentionCandidate）は attention.go が担う。同じ intent、別の adapter。

// maxPushEndpointBytes / maxPushKeyBytes bound what a browser may register.
// These are the shapes RFC 8291 fixes (p256dh は 65 バイトの点、auth は 16
// バイトの secret、いずれも base64url)、余裕を持たせた上限で受ける。
const (
	maxPushEndpointBytes = 2000
	maxPushP256dhBytes   = 200
	maxPushAuthBytes     = 100
)

// pushTTL は push service が端末を待つ秒数。呼びかけは鮮度が命で、半日後に
// 届く「呼ばれました」は嘘に近い。1 時間で捨てる。
const pushTTL = 3600

// pushDeliveryTimeout bounds one fan-out. 送信はリクエストの外側で走るので、
// 呼び出し元の ctx が切れても続くが、無期限にはしない。
const pushDeliveryTimeout = 20 * time.Second

// pushSnippetRunes は本文の抜粋長。通知は本文の代わりではなくポインタである
// （凍結契約 v1: 通知はメッセージ本体ではなくポインタ）。
const pushSnippetRunes = 140

// VAPIDKeys is this deployment's application-server key pair. 購読は鍵に
// 紐づくので、鍵が変わると既存購読はすべて無効になる。生成は一度きり。
type VAPIDKeys struct {
	Public  string
	Private string
}

// PushSubscription is one browser's push endpoint, owned by one Human.
type PushSubscription struct {
	SubscriptionID string
	Human          ParticipantRef
	Endpoint       string
	P256dh         string
	Auth           string
	CreatedAt      time.Time
}

var (
	// ErrInvalidPushSubscription marks a subscription the caller shaped wrongly.
	// It is a request error, so the transport answers 400 instead of 500.
	ErrInvalidPushSubscription = errors.New("invalid push subscription")
	// ErrPushSubscriptionOwned keeps an endpoint bearer from being enough to
	// move another Human's browser subscription to the caller.
	ErrPushSubscriptionOwned = errors.New("push subscription belongs to another human")
)

// EnsureVAPIDKeys returns the deployment's VAPID key pair, minting it on first
// use. 「無ければ作る、あれば絶対に作り直さない」を DB 側の単一行制約に預ける
// ので、同時に起動した複数のプロセスが鍵を奪い合っても既存購読は死なない。
func (s *Store) EnsureVAPIDKeys(ctx context.Context) (VAPIDKeys, error) {
	var keys VAPIDKeys
	err := s.pool.QueryRow(ctx,
		"SELECT public_key, private_key FROM push_vapid_keys WHERE singleton").
		Scan(&keys.Public, &keys.Private)
	if err == nil {
		return keys, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return VAPIDKeys{}, fmt.Errorf("load vapid keys: %w", err)
	}
	private, public, err := webpush.GenerateVAPIDKeys()
	if err != nil {
		return VAPIDKeys{}, fmt.Errorf("generate vapid keys: %w", err)
	}
	// 競合したら先に入った方が正。自分が負けても、直後の SELECT で勝者を読む。
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO push_vapid_keys (singleton, public_key, private_key)
		 VALUES (true, $1, $2) ON CONFLICT (singleton) DO NOTHING`,
		public, private); err != nil {
		return VAPIDKeys{}, fmt.Errorf("insert vapid keys: %w", err)
	}
	if err := s.pool.QueryRow(ctx,
		"SELECT public_key, private_key FROM push_vapid_keys WHERE singleton").
		Scan(&keys.Public, &keys.Private); err != nil {
		return VAPIDKeys{}, fmt.Errorf("reload vapid keys: %w", err)
	}
	return keys, nil
}

// SavePushSubscription registers (or re-registers) one browser endpoint for the
// owner. 所有者は認証済みの呼び出し元であってリクエストの項目ではない。
// endpoint は push service が端末に発行する識別子だが、それだけでは所有権の
// 証明にしない。同じ鍵素材を持つ同じブラウザだけが別のログインへ引き継げる。
func (s *Store) SavePushSubscription(
	ctx context.Context, owner ParticipantRef, endpoint, p256dh, auth string,
) (PushSubscription, error) {
	if owner.Kind != KindHuman {
		// agent はブラウザを持たない。同型性は「同じ判定から、それぞれの身体に
		// 合った出口へ」であって、同じ配送方式を持つことではない（agent 側は
		// attention_candidates）。
		return PushSubscription{}, fmt.Errorf("%w: push subscriptions belong to humans", ErrInvalidPushSubscription)
	}
	if err := owner.Validate(); err != nil {
		return PushSubscription{}, err
	}
	endpoint = strings.TrimSpace(endpoint)
	p256dh = strings.TrimSpace(p256dh)
	auth = strings.TrimSpace(auth)
	if err := validatePushSubscriptionFields(endpoint, p256dh, auth); err != nil {
		return PushSubscription{}, err
	}
	if err := s.participantExists(ctx, owner); err != nil {
		return PushSubscription{}, err
	}
	subscription := PushSubscription{
		SubscriptionID: newUUIDv7(),
		Human:          owner,
		Endpoint:       endpoint,
		P256dh:         p256dh,
		Auth:           auth,
	}
	err := s.pool.QueryRow(ctx,
		`INSERT INTO push_subscriptions (subscription_id, human_id, endpoint, p256dh, auth)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (endpoint) DO UPDATE
		   SET human_id = EXCLUDED.human_id,
		       p256dh = EXCLUDED.p256dh,
		       auth = EXCLUDED.auth
		   WHERE push_subscriptions.human_id = EXCLUDED.human_id
		      OR (push_subscriptions.p256dh = EXCLUDED.p256dh
		          AND push_subscriptions.auth = EXCLUDED.auth)
		 RETURNING subscription_id, created_at`,
		subscription.SubscriptionID, owner.ID, endpoint, p256dh, auth).
		Scan(&subscription.SubscriptionID, &subscription.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return PushSubscription{}, ErrPushSubscriptionOwned
	}
	if err != nil {
		return PushSubscription{}, fmt.Errorf("save push subscription: %w", err)
	}
	return subscription, nil
}

// DeletePushSubscription drops the owner's endpoint. 他人の endpoint を消せて
// しまうと、その人の端末を黙らせる手段になる。所有者一致を条件に入れる。
// 存在しない endpoint は成功として扱う（解除は冪等であるべきで、「無かった」
// ことを教える必要も無い）。
func (s *Store) DeletePushSubscription(ctx context.Context, owner ParticipantRef, endpoint string) error {
	if owner.Kind != KindHuman {
		return fmt.Errorf("%w: push subscriptions belong to humans", ErrInvalidPushSubscription)
	}
	if err := owner.Validate(); err != nil {
		return err
	}
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" || len(endpoint) > maxPushEndpointBytes {
		return fmt.Errorf("%w: endpoint", ErrInvalidPushSubscription)
	}
	if _, err := s.pool.Exec(ctx,
		"DELETE FROM push_subscriptions WHERE human_id = $1 AND endpoint = $2",
		owner.ID, endpoint); err != nil {
		return fmt.Errorf("delete push subscription: %w", err)
	}
	return nil
}

// PushSubscriptionsFor loads every endpoint belonging to the given humans in
// one round trip, keyed by ParticipantRef.Key().
func (s *Store) PushSubscriptionsFor(
	ctx context.Context, recipients []ParticipantRef,
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
	rows, err := s.pool.Query(ctx,
		`SELECT subscription_id, human_id, endpoint, p256dh, auth, created_at
		 FROM push_subscriptions WHERE human_id = ANY($1)
		 ORDER BY created_at`, humanIDs)
	if err != nil {
		return nil, fmt.Errorf("query push subscriptions: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			subscription PushSubscription
			humanID      string
		)
		if err := rows.Scan(&subscription.SubscriptionID, &humanID, &subscription.Endpoint,
			&subscription.P256dh, &subscription.Auth, &subscription.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan push subscription: %w", err)
		}
		subscription.Human = Human(humanID)
		key := subscription.Human.Key()
		out[key] = append(out[key], subscription)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate push subscriptions: %w", err)
	}
	return out, nil
}

// forgetPushEndpoint removes an endpoint the push service declared dead (404 /
// 410). 所有者を問わないのは、これが本人の意思ではなく端末側の事実だから。
func (s *Store) forgetPushEndpoint(ctx context.Context, endpoint string) {
	if _, err := s.pool.Exec(ctx,
		"DELETE FROM push_subscriptions WHERE endpoint = $1", endpoint); err != nil {
		log.Printf("messaging push: drop expired endpoint: %v", err)
	}
}

func validatePushSubscriptionFields(endpoint, p256dh, auth string) error {
	switch {
	case endpoint == "" || len(endpoint) > maxPushEndpointBytes:
		return fmt.Errorf("%w: endpoint", ErrInvalidPushSubscription)
	case !strings.HasPrefix(endpoint, "https://"):
		// push service の endpoint は必ず https。ここを緩めると、任意の URL へ
		// サーバーから POST させる踏み台になる。
		return fmt.Errorf("%w: endpoint must be https", ErrInvalidPushSubscription)
	case p256dh == "" || len(p256dh) > maxPushP256dhBytes:
		return fmt.Errorf("%w: p256dh", ErrInvalidPushSubscription)
	case auth == "" || len(auth) > maxPushAuthBytes:
		return fmt.Errorf("%w: auth", ErrInvalidPushSubscription)
	}
	return nil
}

// --- 配送 ---

// PushPayload is what the Service Worker receives. 本文そのものではなく
// 「どこで、誰が、だいたい何を」に留め、続きは place を開いて読む。
// place の URL 形は web 側（place-route.ts と sw.js）が持つので、ここは
// place_id と place_kind までを運ぶ。
type PushPayload struct {
	PlaceID   string `json:"place_id"`
	PlaceKind string `json:"place_kind"`
	Title     string `json:"title"`
	Body      string `json:"body"`
	Reason    string `json:"reason"`
	Seq       int64  `json:"seq"`
}

// pushHTTPClient is the seam tests replace. 本番では *http.Client。
type pushHTTPClient interface {
	Do(*http.Request) (*http.Response, error)
}

// PushDispatcher sends the already-made notification decision to a Human's
// registered browsers. It is best-effort on top of durable truth: 送信に失敗
// してもメッセージは既に確定している。
type PushDispatcher struct {
	store *Store
	keys  VAPIDKeys
	// subject は VAPID JWT の sub。push service に対する運用連絡先で、
	// mailto: か https: の URL である必要がある。
	subject string
	client  pushHTTPClient
}

// NewPushDispatcher mints the deployment's VAPID keys if needed and returns a
// dispatcher bound to them.
func NewPushDispatcher(ctx context.Context, store *Store, subject string) (*PushDispatcher, error) {
	if store == nil {
		return nil, errors.New("push dispatcher requires a store")
	}
	subject = strings.TrimSpace(subject)
	if subject == "" {
		return nil, errors.New("push dispatcher requires a VAPID subject (mailto: or https: URL)")
	}
	keys, err := store.EnsureVAPIDKeys(ctx)
	if err != nil {
		return nil, err
	}
	return &PushDispatcher{store: store, keys: keys, subject: subject, client: http.DefaultClient}, nil
}

// PublicKey is the application server key a browser must present to
// PushManager.subscribe. 公開鍵なので、認証済みの誰に見せてもよい。
func (d *PushDispatcher) PublicKey() string {
	if d == nil {
		return ""
	}
	return d.keys.Public
}

// UsePush attaches a dispatcher so committed messages also leave the building.
// nil のままなら push は単に起きない（既存のタブ内通知はそのまま動く）。
func (s *Store) UsePush(dispatcher *PushDispatcher) {
	s.push = dispatcher
}

// deliverPush fans one message's decisions out to the recipients' browsers.
// 呼び出し元のリクエストが終わっても送り切りたいので ctx の cancel からは
// 切り離す。値（trace 等）は保つ。
func (s *Store) deliverPush(ctx context.Context, place Place, msg Message, decisions []NotificationDecision) {
	if s == nil || s.push == nil || len(decisions) == 0 {
		return
	}
	dispatcher := s.push
	detached, cancel := context.WithTimeout(context.WithoutCancel(ctx), pushDeliveryTimeout)
	go func() {
		defer cancel()
		dispatcher.deliver(detached, place, msg, decisions)
	}()
}

func (d *PushDispatcher) deliver(ctx context.Context, place Place, msg Message, decisions []NotificationDecision) {
	humans := make([]ParticipantRef, 0, len(decisions))
	for _, decision := range decisions {
		if decision.Participant.Kind == KindHuman {
			humans = append(humans, decision.Participant)
		}
	}
	if len(humans) == 0 {
		return
	}
	subscriptions, err := d.store.PushSubscriptionsFor(ctx, humans)
	if err != nil {
		log.Printf("messaging push: load subscriptions: %v", err)
		return
	}
	if len(subscriptions) == 0 {
		return
	}
	authorName := d.authorName(ctx, place, msg)
	for _, decision := range decisions {
		if decision.Participant.Kind != KindHuman {
			continue
		}
		endpoints := subscriptions[decision.Participant.Key()]
		if len(endpoints) == 0 {
			continue
		}
		payload, err := json.Marshal(PushPayload{
			PlaceID:   place.PlaceID,
			PlaceKind: place.Kind,
			Title:     PushTitle(place, authorName),
			Body:      PushBody(msg),
			Reason:    decision.Reason,
			Seq:       msg.Seq,
		})
		if err != nil {
			log.Printf("messaging push: encode payload: %v", err)
			return
		}
		for _, subscription := range endpoints {
			d.send(ctx, subscription, payload, pushTopic(place.PlaceID))
		}
	}
}

func (d *PushDispatcher) send(ctx context.Context, subscription PushSubscription, payload []byte, topic string) {
	response, err := webpush.SendNotificationWithContext(ctx, payload, &webpush.Subscription{
		Endpoint: subscription.Endpoint,
		Keys:     webpush.Keys{P256dh: subscription.P256dh, Auth: subscription.Auth},
	}, &webpush.Options{
		HTTPClient:      d.client,
		Subscriber:      d.subject,
		VAPIDPublicKey:  d.keys.Public,
		VAPIDPrivateKey: d.keys.Private,
		TTL:             pushTTL,
		Urgency:         webpush.UrgencyHigh,
		// 同じ place の未読を push service の queue の中で積み上げない。
		Topic: topic,
	})
	if err != nil {
		log.Printf("messaging push: send: %v", err)
		return
	}
	defer func() { _ = response.Body.Close() }()
	// 404/410 は「この端末はもう無い」という push service の宣言。次からは
	// 送らないよう、その場で忘れる。
	if response.StatusCode == http.StatusNotFound || response.StatusCode == http.StatusGone {
		d.store.forgetPushEndpoint(ctx, subscription.Endpoint)
		return
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		// endpoint は bearer secret なのでログへ出さない。status があれば設定不良・
		// rate limit・payload rejection の区別には十分である。
		log.Printf("messaging push: unexpected response status %d", response.StatusCode)
	}
}

// pushTopic collapses a place's queued pushes into the latest one. RFC 8030 の
// Topic は base64url 32 文字以内なので、UUID のハイフンを落とした 32 桁を使う
// （16 進は base64url の字集合に収まる）。形が想定外なら topic を諦める：
// collapse は最適化であって、通知の正しさではない。
func pushTopic(placeID string) string {
	topic := strings.ReplaceAll(placeID, "-", "")
	if len(topic) != 32 {
		return ""
	}
	return topic
}

// PushTitle names the place and the speaker: 呼ばれた人が「どこで誰に」を
// ロック画面のまま判断できる最小の情報。
func PushTitle(place Place, authorName string) string {
	if place.Kind == PlaceChannel && place.Name != "" {
		return "#" + place.Name + " — " + authorName
	}
	return authorName
}

// PushBody is the snippet. 削除済みメッセージは通知されないが、念のため本文を
// 出さない。添付だけの発言も「（添付）」で足りる。
func PushBody(msg Message) string {
	if msg.Deleted {
		return ""
	}
	collapsed := strings.Join(strings.Fields(msg.Content), " ")
	if collapsed == "" {
		if len(msg.Attachments) > 0 {
			return "（添付ファイル）"
		}
		return ""
	}
	runes := []rune(collapsed)
	if len(runes) > pushSnippetRunes {
		return string(runes[:pushSnippetRunes-1]) + "…"
	}
	return collapsed
}

// --- REST（人間のブラウザ向け） ---

// servePushKey hands the browser the application server key it must present to
// PushManager.subscribe. 公開鍵なので秘密ではないが、購読の入口はセッションの
// 内側にある。push が構成されていない deployment は 503 で正直に断る。
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
	}{s.Push.PublicKey()})
}

// pushSubscriptionWire matches the browser's PushSubscription.toJSON() shape so
// the client can post what the platform handed it, unedited.
type pushSubscriptionWire struct {
	Endpoint string `json:"endpoint"`
	Keys     struct {
		P256dh string `json:"p256dh"`
		Auth   string `json:"auth"`
	} `json:"keys"`
}

// serveSavePushSubscription registers the caller's browser. 所有者は署名済み
// セッションの人間であって、リクエストの項目ではない。
func (s *Server) serveSavePushSubscription(w http.ResponseWriter, r *http.Request) {
	viewer, claims, ok := s.viewer(w, r)
	if !ok {
		return
	}
	if s.Push == nil {
		writeError(w, http.StatusServiceUnavailable, "push_unavailable")
		return
	}
	var req pushSubscriptionWire
	if !decodeJSON(w, r, &req) {
		return
	}
	done, err := s.mutate(w, r, claims, func() error {
		_, opErr := s.Store.SavePushSubscription(
			r.Context(), viewer, req.Endpoint, req.Keys.P256dh, req.Keys.Auth)
		return opErr
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

// serveDeletePushSubscription forgets one of the caller's own endpoints.
// 冪等: 既に無い endpoint も 204。
func (s *Server) serveDeletePushSubscription(w http.ResponseWriter, r *http.Request) {
	viewer, claims, ok := s.viewer(w, r)
	if !ok {
		return
	}
	var req struct {
		Endpoint string `json:"endpoint"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	done, err := s.mutate(w, r, claims, func() error {
		return s.Store.DeletePushSubscription(r.Context(), viewer, req.Endpoint)
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

// authorName resolves the speaker's display name in this place. ID を名前に
// 使わない（ADR 0008 §1）ので、引けなかったときは名前の無い呼びかけにする。
func (d *PushDispatcher) authorName(ctx context.Context, place Place, msg Message) string {
	profiles, err := d.store.activeMembers(ctx, d.store.pool, place)
	if err != nil {
		log.Printf("messaging push: resolve author name: %v", err)
		return "新しいメッセージ"
	}
	for _, profile := range profiles {
		if profile.Participant == msg.Author {
			if name := profile.ProjectedDisplayName(); name != "" {
				return name
			}
		}
	}
	return "新しいメッセージ"
}
