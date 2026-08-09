package messaging

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// 使い捨てのブラウザ購読鍵。対応する秘密鍵はどこにも無いので、暗号化された
// payload は誰にも読めない——読ませることが目的ではなく、送信経路そのものが
// 本物の鍵で成立することを見るために置いている。
const (
	testPushP256dh = "BDCBECfmPtnXzdwJgF_TdPNsC6ZfDnFm31D8x6HqxOJ7zq3tJamfSlIVx2cDwsSUKwGiiuucupkLMDwJFObrclE"
	testPushAuth   = "lRvaLEgVKoLGOxRND8ZCTA"
)

// recordingPushClient stands in for the push service. 本物の endpoint へ出て
// いかないまま、「どの endpoint に何回出したか」と「相手が死んだと言ったとき
// どうするか」だけを見る。
type recordingPushClient struct {
	mu     sync.Mutex
	sent   []string
	status map[string]int
	err    error
}

func (c *recordingPushClient) Do(request *http.Request) (*http.Response, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return nil, c.err
	}
	c.sent = append(c.sent, request.URL.String())
	status := http.StatusCreated
	if override, ok := c.status[request.URL.String()]; ok {
		status = override
	}
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(bytes.NewReader(nil)),
		Header:     http.Header{},
		Request:    request,
	}, nil
}

func (c *recordingPushClient) endpoints() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.sent...)
}

// testKeys returns a real VAPID pair: webpush-go signs the JWT for every send,
// so a fake string would fail before the transport is exercised at all.
func (w world) dispatcher(t *testing.T, ctx context.Context, client pushHTTPClient) *PushDispatcher {
	t.Helper()
	keys, err := w.store.EnsureVAPIDKeys(ctx)
	if err != nil {
		t.Fatalf("ensure vapid keys: %v", err)
	}
	return &PushDispatcher{store: w.store, keys: keys, subject: "mailto:ops@example.test", client: client}
}

func (w world) subscribe(t *testing.T, ctx context.Context, owner ParticipantRef, endpoint string) {
	t.Helper()
	// 実在する P-256 の点。RFC 8291 の暗号化は本物の鍵でしか通らないので、
	// ここを飾りにすると送信そのものが検証されなくなる（使い捨ての公開鍵で、
	// 対応する秘密鍵はどこにも無い）。
	if _, err := w.store.SavePushSubscription(ctx, owner, endpoint, testPushP256dh, testPushAuth); err != nil {
		t.Fatalf("subscribe %s: %v", endpoint, err)
	}
}

func TestVAPIDKeysAreMintedOnceAndNeverRotatedUnderneathSubscribers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)

	first, err := w.store.EnsureVAPIDKeys(ctx)
	if err != nil {
		t.Fatalf("mint keys: %v", err)
	}
	if first.Public == "" || first.Private == "" {
		t.Fatalf("minted keys = %+v, want both halves", first)
	}
	second, err := w.store.EnsureVAPIDKeys(ctx)
	if err != nil {
		t.Fatalf("reload keys: %v", err)
	}
	if second != first {
		// 鍵が変わると全端末の購読が黙って死ぬ。ここが「一度きり」でなければ
		// 通知は再起動のたびに壊れる。
		t.Fatalf("keys rotated across calls: %+v then %+v", first, second)
	}
}

func TestPushSubscriptionIsOwnedByTheAuthenticatedHumanOnly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)

	w.subscribe(t, ctx, w.humanA, "https://push.example.test/a")
	subscriptions, err := w.store.PushSubscriptionsFor(ctx, []ParticipantRef{w.humanA, w.humanB})
	if err != nil {
		t.Fatalf("load subscriptions: %v", err)
	}
	if len(subscriptions[w.humanA.Key()]) != 1 || len(subscriptions[w.humanB.Key()]) != 0 {
		t.Fatalf("subscriptions = %+v, want one for humanA only", subscriptions)
	}

	// endpoint を知っているだけの別人は、異なる鍵で所有者を奪えない。
	if _, err := w.store.SavePushSubscription(ctx, w.humanB,
		"https://push.example.test/a", "attacker-p256dh", "attacker-auth"); !errors.Is(err, ErrPushSubscriptionOwned) {
		t.Fatalf("cross-owner save err = %v, want ErrPushSubscriptionOwned", err)
	}
	subscriptions, err = w.store.PushSubscriptionsFor(ctx, []ParticipantRef{w.humanA, w.humanB})
	if err != nil {
		t.Fatalf("reload after takeover attempt: %v", err)
	}
	if len(subscriptions[w.humanA.Key()]) != 1 || len(subscriptions[w.humanB.Key()]) != 0 {
		t.Fatalf("takeover changed ownership: %+v", subscriptions)
	}

	// 他人の endpoint は消せない。消せると「その人の端末を黙らせる手段」になる。
	if err := w.store.DeletePushSubscription(ctx, w.humanB, "https://push.example.test/a"); err != nil {
		t.Fatalf("delete someone else's endpoint should be a silent no-op: %v", err)
	}
	subscriptions, err = w.store.PushSubscriptionsFor(ctx, []ParticipantRef{w.humanA})
	if err != nil {
		t.Fatalf("reload subscriptions: %v", err)
	}
	if len(subscriptions[w.humanA.Key()]) != 1 {
		t.Fatalf("humanA's endpoint was removed by humanB: %+v", subscriptions)
	}

	// 本人が消せば消える。二度目も成功する（解除は冪等）。
	for range 2 {
		if err := w.store.DeletePushSubscription(ctx, w.humanA, "https://push.example.test/a"); err != nil {
			t.Fatalf("owner delete: %v", err)
		}
	}
	subscriptions, err = w.store.PushSubscriptionsFor(ctx, []ParticipantRef{w.humanA})
	if err != nil {
		t.Fatalf("reload after delete: %v", err)
	}
	if len(subscriptions[w.humanA.Key()]) != 0 {
		t.Fatalf("endpoint survived its owner's delete: %+v", subscriptions)
	}
}

func TestPushSubscriptionRejectsNonHumansAndNonHTTPSEndpoints(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)

	// agent はブラウザを持たない。同型性は「同じ判定から、それぞれの身体に
	// 合った出口へ」であって、同じ配送方式を持つことではない。
	if _, err := w.store.SavePushSubscription(ctx, w.agent,
		"https://push.example.test/agent", "p256dh", "auth"); !errors.Is(err, ErrInvalidPushSubscription) {
		t.Fatalf("agent push subscription err = %v, want ErrInvalidPushSubscription", err)
	}
	// https 以外を許すと「任意の URL へサーバーから POST させる」踏み台になる。
	if _, err := w.store.SavePushSubscription(ctx, w.humanA,
		"http://push.example.test/a", "p256dh", "auth"); !errors.Is(err, ErrInvalidPushSubscription) {
		t.Fatalf("plaintext endpoint err = %v, want ErrInvalidPushSubscription", err)
	}
}

func TestPushGoesOnlyToTheHumansTheServerDecidedToCall(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	quiet := w.mintHuman(t, ctx, "Shizuka")
	if err := w.store.AddWorkspaceMember(ctx, ch.WorkspaceID, quiet, RoleMember); err != nil {
		t.Fatalf("add quiet member: %v", err)
	}
	// この人はこの place を mute している。決定は 0015 が下しており、push は
	// その決定に足を生やすだけなので、ここに届いてはならない。
	if _, err := w.store.SetNotificationSetting(ctx, quiet, NotifyLevelMute, nil, nil); err != nil {
		t.Fatalf("mute quiet: %v", err)
	}
	w.subscribe(t, ctx, w.humanB, "https://push.example.test/haru")
	w.subscribe(t, ctx, quiet, "https://push.example.test/shizuka")

	client := &recordingPushClient{}
	dispatcher := w.dispatcher(t, ctx, client)
	msg := w.send(t, ctx, ch.PlaceID, w.humanA, "デプロイ、今から始めます")
	decisions, err := w.store.NotificationDecisionsFor(ctx, ch, msg)
	if err != nil {
		t.Fatalf("decisions: %v", err)
	}
	dispatcher.deliver(ctx, ch, msg, decisions)

	sent := client.endpoints()
	if len(sent) != 1 || sent[0] != "https://push.example.test/haru" {
		t.Fatalf("push endpoints = %v, want only Haru's (Shizuka muted the place)", sent)
	}
}

func TestPushForgetsAnEndpointThePushServiceDeclaredGone(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	w.subscribe(t, ctx, w.humanB, "https://push.example.test/dead")

	client := &recordingPushClient{status: map[string]int{
		"https://push.example.test/dead": http.StatusGone,
	}}
	dispatcher := w.dispatcher(t, ctx, client)
	msg := w.send(t, ctx, ch.PlaceID, w.humanA, "おはようございます")
	decisions, err := w.store.NotificationDecisionsFor(ctx, ch, msg)
	if err != nil {
		t.Fatalf("decisions: %v", err)
	}
	dispatcher.deliver(ctx, ch, msg, decisions)

	subscriptions, err := w.store.PushSubscriptionsFor(ctx, []ParticipantRef{w.humanB})
	if err != nil {
		t.Fatalf("reload subscriptions: %v", err)
	}
	if len(subscriptions[w.humanB.Key()]) != 0 {
		t.Fatalf("a 410 endpoint survived: %+v", subscriptions)
	}
}

func TestPushLogsUnexpectedServiceStatus(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	w.subscribe(t, ctx, w.humanB, "https://push.example.test/rejected")

	client := &recordingPushClient{status: map[string]int{
		"https://push.example.test/rejected": http.StatusBadRequest,
	}}
	dispatcher := w.dispatcher(t, ctx, client)
	msg := w.send(t, ctx, ch.PlaceID, w.humanA, "設定を見直してください")
	decisions, err := w.store.NotificationDecisionsFor(ctx, ch, msg)
	if err != nil {
		t.Fatalf("decisions: %v", err)
	}

	var logs bytes.Buffer
	previousWriter, previousFlags := log.Writer(), log.Flags()
	log.SetOutput(&logs)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(previousWriter)
		log.SetFlags(previousFlags)
	})
	dispatcher.deliver(ctx, ch, msg, decisions)

	if !strings.Contains(logs.String(), "unexpected response status 400") {
		t.Fatalf("push log = %q, want rejected status", logs.String())
	}
}

func TestPushTitleNamesThePlaceAndTheSpeaker(t *testing.T) {
	channel := Place{PlaceID: "p", Kind: PlaceChannel, Name: "general"}
	if got := PushTitle(channel, "Yohaku"); got != "#general — Yohaku" {
		t.Fatalf("channel title = %q", got)
	}
	// DM に place 名は無い。呼んでいるのは人であって場所ではない。
	dm := Place{PlaceID: "p", Kind: PlaceDM}
	if got := PushTitle(dm, "Yohaku"); got != "Yohaku" {
		t.Fatalf("dm title = %q", got)
	}
}

func TestPushBodyStaysAPointerNotTheMessage(t *testing.T) {
	long := ""
	for range 400 {
		long += "あ"
	}
	body := PushBody(Message{Content: long})
	if len([]rune(body)) != pushSnippetRunes {
		t.Fatalf("snippet runes = %d, want %d", len([]rune(body)), pushSnippetRunes)
	}
	if got := PushBody(Message{Content: "  改行\nと   空白  "}); got != "改行 と 空白" {
		t.Fatalf("collapsed body = %q", got)
	}
	if got := PushBody(Message{Attachments: []Attachment{{}}}); got != "（添付ファイル）" {
		t.Fatalf("attachment-only body = %q", got)
	}
	if got := PushBody(Message{Content: "消えた", Deleted: true}); got != "" {
		t.Fatalf("deleted body = %q, want empty", got)
	}
}
