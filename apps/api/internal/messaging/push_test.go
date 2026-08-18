package messaging

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
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
	mu             sync.Mutex
	sent           []string
	authorizations []string
	status         map[string]int
	err            error
}

// blockingPushClient makes the boundary between the lease and HTTPS observable:
// started closes only after webpush has built the outbound request, and Do then
// remains blocked until the test releases the remote endpoint.
type blockingPushClient struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *blockingPushClient) Do(request *http.Request) (*http.Response, error) {
	c.once.Do(func() { close(c.started) })
	<-c.release
	return &http.Response{
		StatusCode: http.StatusCreated,
		Body:       io.NopCloser(bytes.NewReader(nil)),
		Header:     http.Header{},
		Request:    request,
	}, nil
}

func (c *recordingPushClient) Do(request *http.Request) (*http.Response, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return nil, c.err
	}
	c.sent = append(c.sent, request.URL.String())
	c.authorizations = append(c.authorizations, request.Header.Get("Authorization"))
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

func (c *recordingPushClient) authorizationHeaders() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.authorizations...)
}

func (c *recordingPushClient) endpoints() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.sent...)
}

// testPushEgress is the test network's DNS. 「どの IP なら出てよいか」は本物の
// 述語のままで、差し替えるのは名前が何に解決されるかだけ——出口の判断そのものを
// テスト用に緩めると、テストは policy ではなく自分の作り話を見ることになる。
//
//	internal.*     → 10.0.0.7（内部アドレスを指す名前）
//	unresolvable.* → 名前が引けない
//	その他          → 8.8.8.8（公開ユニキャスト）
func testPushEgress() *pushEgress {
	return &pushEgress{
		resolve: func(_ context.Context, host string) ([]net.IP, error) {
			switch {
			case strings.HasPrefix(host, "internal."):
				return []net.IP{net.ParseIP("10.0.0.7")}, nil
			case strings.HasPrefix(host, "unresolvable."):
				return nil, errors.New("no such host")
			default:
				return []net.IP{net.ParseIP("8.8.8.8")}, nil
			}
		},
	}
}

// dispatcher returns a dispatcher on a real VAPID pair: webpush-go signs the
// JWT for every send, so a fake string would fail before the transport is
// exercised at all.
func (w world) dispatcher(t *testing.T, ctx context.Context, client pushHTTPClient) *PushDispatcher {
	t.Helper()
	keys, err := w.store.core.EnsureVAPIDKeys(ctx)
	if err != nil {
		t.Fatalf("ensure vapid keys: %v", err)
	}
	return &PushDispatcher{store: w.store.core, keys: keys, subject: "mailto:ops@example.test", client: client}
}

func (w world) subscribe(t *testing.T, ctx context.Context, owner ParticipantRef, endpoint string) {
	t.Helper()
	// 実在する P-256 の点。RFC 8291 の暗号化は本物の鍵でしか通らないので、
	// ここを飾りにすると送信そのものが検証されなくなる（使い捨ての公開鍵で、
	// 対応する秘密鍵はどこにも無い）。
	if _, err := w.store.mustScopeForActor(t, ctx, owner).
		SavePushSubscription(ctx, endpoint, testPushP256dh, testPushAuth); err != nil {
		t.Fatalf("subscribe %s: %v", endpoint, err)
	}
}

func (w world) subscriptionsFor(
	t *testing.T, ctx context.Context, owners ...ParticipantRef,
) map[string][]PushSubscription {
	t.Helper()
	loaded, err := w.store.core.pushSubscriptionsFor(ctx, owners)
	if err != nil {
		t.Fatalf("load subscriptions: %v", err)
	}
	return loaded
}

func TestVAPIDKeysAreMintedOnceAndNeverRotatedUnderneathSubscribers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)

	first, err := w.store.core.EnsureVAPIDKeys(ctx)
	if err != nil {
		t.Fatalf("mint keys: %v", err)
	}
	if first.Public == "" || first.Private == "" {
		t.Fatalf("minted keys = %+v, want both halves", first)
	}
	second, err := w.store.core.EnsureVAPIDKeys(ctx)
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
	w.workspaceWithChannel(t, ctx)

	w.subscribe(t, ctx, w.humanA, "https://push.example.test/a")
	subscriptions := w.subscriptionsFor(t, ctx, w.humanA, w.humanB)
	if len(subscriptions[w.humanA.Key()]) != 1 || len(subscriptions[w.humanB.Key()]) != 0 {
		t.Fatalf("subscriptions = %+v, want one for humanA only", subscriptions)
	}

	// endpoint を知っているだけの別人は、異なる鍵で所有者を奪えない。
	if _, err := w.store.mustScopeForActor(t, ctx, w.humanB).SavePushSubscription(ctx,
		"https://push.example.test/a", "attacker-p256dh", "attacker-auth"); !errors.Is(err, ErrPushSubscriptionOwned) {
		t.Fatalf("cross-owner save err = %v, want ErrPushSubscriptionOwned", err)
	}
	subscriptions = w.subscriptionsFor(t, ctx, w.humanA, w.humanB)
	if len(subscriptions[w.humanA.Key()]) != 1 || len(subscriptions[w.humanB.Key()]) != 0 {
		t.Fatalf("takeover changed ownership: %+v", subscriptions)
	}

	// 他人の endpoint は消せない。消せると「その人の端末を黙らせる手段」になる。
	if err := w.store.mustScopeForActor(t, ctx, w.humanB).
		DeletePushSubscription(ctx, "https://push.example.test/a"); err != nil {
		t.Fatalf("delete someone else's endpoint should be a silent no-op: %v", err)
	}
	if got := len(w.subscriptionsFor(t, ctx, w.humanA)[w.humanA.Key()]); got != 1 {
		t.Fatalf("humanA's endpoint was removed by humanB: %d remaining", got)
	}

	// 本人が消せば消える。二度目も成功する（解除は冪等）。
	for range 2 {
		if err := w.store.mustScopeForActor(t, ctx, w.humanA).
			DeletePushSubscription(ctx, "https://push.example.test/a"); err != nil {
			t.Fatalf("owner delete: %v", err)
		}
	}
	if got := len(w.subscriptionsFor(t, ctx, w.humanA)[w.humanA.Key()]); got != 0 {
		t.Fatalf("endpoint survived its owner's delete: %d remaining", got)
	}
}

func TestPushSubscriptionsAreBoundedPerHumanAndReregistrationDoesNotAddASlot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	w.workspaceWithChannel(t, ctx)
	scoped := w.store.mustScopeForActor(t, ctx, w.humanA)

	for i := 0; i < maxPushSubscriptionsPerHuman; i++ {
		endpoint := fmt.Sprintf("https://push.example.test/slot-%d", i)
		if _, err := scoped.SavePushSubscription(ctx, endpoint, testPushP256dh, testPushAuth); err != nil {
			t.Fatalf("save subscription %d: %v", i, err)
		}
	}
	if got := len(w.subscriptionsFor(t, ctx, w.humanA)[w.humanA.Key()]); got != maxPushSubscriptionsPerHuman {
		t.Fatalf("subscriptions before limit = %d, want %d", got, maxPushSubscriptionsPerHuman)
	}

	// The same endpoint is an upsert, including while every slot is occupied.
	if _, err := scoped.SavePushSubscription(ctx, "https://push.example.test/slot-0", testPushP256dh, testPushAuth); err != nil {
		t.Fatalf("re-register existing endpoint: %v", err)
	}
	if got := len(w.subscriptionsFor(t, ctx, w.humanA)[w.humanA.Key()]); got != maxPushSubscriptionsPerHuman {
		t.Fatalf("re-registration changed subscription count: %d", got)
	}

	if _, err := scoped.SavePushSubscription(ctx, "https://push.example.test/slot-overflow", testPushP256dh, testPushAuth); !errors.Is(err, ErrPushSubscriptionLimit) {
		t.Fatalf("overflow err = %v, want ErrPushSubscriptionLimit", err)
	}
	if got := len(w.subscriptionsFor(t, ctx, w.humanA)[w.humanA.Key()]); got != maxPushSubscriptionsPerHuman {
		t.Fatalf("overflow subscription landed: %d", got)
	}
}

func TestPushSubscriptionRejectsNonHumansAndNonHTTPSEndpoints(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	w.workspaceWithChannel(t, ctx)

	// agent はブラウザを持たない。同型性は「同じ判定から、それぞれの身体に
	// 合った出口へ」であって、同じ配送方式を持つことではない。
	if _, err := w.store.mustScopeForActor(t, ctx, w.agent).SavePushSubscription(ctx,
		"https://push.example.test/agent", testPushP256dh, testPushAuth); !errors.Is(err, ErrInvalidPushSubscription) {
		t.Fatalf("agent push subscription err = %v, want ErrInvalidPushSubscription", err)
	}
	// https 以外を許すと「任意の URL へサーバーから POST させる」踏み台になる。
	if _, err := w.store.mustScopeForActor(t, ctx, w.humanA).SavePushSubscription(ctx,
		"http://push.example.test/a", testPushP256dh, testPushAuth); !errors.Is(err, ErrInvalidPushSubscription) {
		t.Fatalf("plaintext endpoint err = %v, want ErrInvalidPushSubscription", err)
	}
}

func TestPushSubscriptionRequiresALiveMessagingInstallation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, _ := w.workspaceWithChannel(t, ctx)
	scoped := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)

	// 端末の登録も Messaging の入口の内側にある。epoch を一つ進めた宛先は
	// 「今のインストール」ではないので、通らない。
	stale := &ScopedStore{Store: w.store.core, Scope: Scope{
		WorkspaceID:    scoped.Scope.WorkspaceID,
		InstallationID: scoped.Scope.InstallationID,
		AuthorityEpoch: scoped.Scope.AuthorityEpoch + 1,
		Actor:          w.humanA,
	}}
	if _, err := stale.SavePushSubscription(ctx,
		"https://push.example.test/stale", testPushP256dh, testPushAuth); err == nil {
		t.Fatal("a stale authority epoch registered a browser")
	}
	if got := len(w.subscriptionsFor(t, ctx, w.humanA)[w.humanA.Key()]); got != 0 {
		t.Fatalf("stale-epoch subscription landed: %d rows", got)
	}
}

func TestPushEndpointsThatPointInsideTheDeploymentAreRefused(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	w.workspaceWithChannel(t, ctx)
	scoped := w.store.mustScopeForActor(t, ctx, w.humanA)

	// 登録が「サーバーが後で自分から出ていく先」を決めてしまうので、断るのは
	// 送信時ではなく登録時。https は形の条件でしかない。
	for _, endpoint := range []string{
		"https://internal.example.test/hook", // 名前が内部アドレスを指す
		"https://127.0.0.1/hook",             // loopback を直に書く
		"https://[::1]/hook",
		"https://10.0.0.7/hook",              // RFC1918
		"https://169.254.169.254/latest",     // link-local（クラウドの metadata）
		"https://[fd00::1]/hook",             // unique local
		"https://unresolvable.example.test/", // 名前が引けない＝通さない
		"https://user:pass@push.example.test/a",
	} {
		if _, err := scoped.SavePushSubscription(ctx, endpoint, testPushP256dh, testPushAuth); !errors.Is(err, ErrInvalidPushSubscription) {
			t.Fatalf("endpoint %s err = %v, want ErrInvalidPushSubscription", endpoint, err)
		}
	}
	if got := len(w.subscriptionsFor(t, ctx, w.humanA)[w.humanA.Key()]); got != 0 {
		t.Fatalf("a refused endpoint still landed: %d rows", got)
	}

	// 公開ユニキャストへ解決する名前は通る。自前の push service を閉じない。
	if _, err := scoped.SavePushSubscription(ctx,
		"https://push.example.test/ok", testPushP256dh, testPushAuth); err != nil {
		t.Fatalf("public endpoint rejected: %v", err)
	}
}

func TestPushSendingCannotReachAnAddressTheRegistrationWouldHaveRefused(t *testing.T) {
	// 登録時に解決した答えと、送信時に繋ぐ相手は同じとは限らない（rebinding）。
	// だから同じ述語を dialer にも埋め込んである。ここでは本物の client で
	// loopback の TLS server へ出ようとして、繋ぐ前に断られることを見る。
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	client := newPushHTTPClient()
	request, err := http.NewRequest(http.MethodPost, server.URL, strings.NewReader("x"))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	response, err := client.Do(request)
	if err == nil {
		_ = response.Body.Close()
		t.Fatal("the push client connected to a loopback address")
	}
	if !pushDialWasRefused(err) {
		t.Fatalf("dial error = %v, want the egress policy's refusal", err)
	}

	// 述語そのもの：公開ユニキャストだけが通る。
	for address, want := range map[string]bool{
		"8.8.8.8:443":           true,
		"[2606:4700::1]:443":    true,
		"127.0.0.1:443":         false,
		"10.0.0.7:443":          false,
		"192.168.1.5:443":       false,
		"169.254.169.254:80":    false,
		"100.64.0.1:443":        false,
		"[::1]:443":             false,
		"[fd00::1]:443":         false,
		"[::ffff:10.0.0.7]:443": false,
		"224.0.0.1:443":         false,
	} {
		err := guardDialAddress(address)
		if (err == nil) != want {
			t.Fatalf("guardDialAddress(%s) err = %v, want allowed=%v", address, err, want)
		}
	}
}

func TestEverySpecialPurposeRangeIsRefusedAtDial(t *testing.T) {
	for _, prefix := range pushSpecialPurposePrefixes {
		prefix := prefix
		t.Run(prefix.String(), func(t *testing.T) {
			if err := guardDialAddress(net.JoinHostPort(prefix.Addr().String(), "443")); err == nil {
				t.Fatalf("dial to %s was allowed", prefix)
			}
			if !prefix.Addr().Is4() {
				return
			}
			// IPv4-mapped IPv6 has to take the same IPv4 path. A mapped form
			// of each denied IPv4 range must therefore be denied as well.
			mapped := netip.MustParseAddr("::ffff:" + prefix.Addr().String())
			if err := guardDialAddress(net.JoinHostPort(mapped.String(), "443")); err == nil {
				t.Fatalf("dial to mapped %s was allowed", prefix)
			}
		})
	}
}

func TestPushGoesOnlyToTheHumansTheServerDecidedToCall(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, ch := w.workspaceWithChannel(t, ctx)
	quiet := w.mintHuman(t, ctx, "Shizuka")
	if err := w.store.AddWorkspaceMember(ctx, workspace.WorkspaceID, quiet, RoleMember); err != nil {
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
	// 配送は判定の写しではなく、message と同じ transaction で確定した
	// intent の正本から読む。
	decisions, err := w.store.NotificationIntentsForMessage(ctx, msg.MessageID)
	if err != nil {
		t.Fatalf("intents: %v", err)
	}
	dispatcher.deliver(ctx, w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA).Scope, ch, msg, decisions, "Yohaku")

	sent := client.endpoints()
	if len(sent) != 1 || sent[0] != "https://push.example.test/haru" {
		t.Fatalf("push endpoints = %v, want only Haru's (Shizuka muted the place)", sent)
	}
}

func TestPushDoesNotSendToAMemberRemovedAfterMessageCommit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, ch := w.workspaceWithChannel(t, ctx)
	w.subscribe(t, ctx, w.humanB, "https://push.example.test/removed")
	msg := w.send(t, ctx, ch.PlaceID, w.humanA, "削除後には届かない本文")
	decisions, err := w.store.NotificationIntentsForMessage(ctx, msg.MessageID)
	if err != nil {
		t.Fatalf("intents: %v", err)
	}
	if err := w.workspaces.RemoveMember(ctx, workspace.WorkspaceID,
		activeMembershipID(t, ctx, w, workspace.WorkspaceID, w.humanB), w.humanA); err != nil {
		t.Fatalf("remove committed recipient: %v", err)
	}

	client := &recordingPushClient{}
	dispatcher := w.dispatcher(t, ctx, client)
	dispatcher.deliver(ctx, w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA).Scope,
		ch, msg, decisions, "Yohaku")
	if sent := client.endpoints(); len(sent) != 0 {
		t.Fatalf("push sent to member removed after commit: %v", sent)
	}
}

func TestPushDoesNotSendDeletedMessageContent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, ch := w.workspaceWithChannel(t, ctx)
	w.subscribe(t, ctx, w.humanB, "https://push.example.test/deleted")
	msg := w.send(t, ctx, ch.PlaceID, w.humanA, "消した本文は通知しない")
	decisions, err := w.store.NotificationIntentsForMessage(ctx, msg.MessageID)
	if err != nil {
		t.Fatalf("intents: %v", err)
	}
	if _, err := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA).
		DeleteMessage(ctx, ch.PlaceID, msg.MessageID); err != nil {
		t.Fatalf("delete committed before push planning: %v", err)
	}

	client := &recordingPushClient{}
	dispatcher := w.dispatcher(t, ctx, client)
	dispatcher.deliver(ctx, w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA).Scope,
		ch, msg, decisions, "Yohaku")
	if sent := client.endpoints(); len(sent) != 0 {
		t.Fatalf("deleted message sent push endpoints: %v", sent)
	}
}

func TestSlowPushEndpointDoesNotHoldWorkspaceLease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, ch := w.workspaceWithChannel(t, ctx)
	w.subscribe(t, ctx, w.humanB, "https://push.example.test/slow")
	msg := w.send(t, ctx, ch.PlaceID, w.humanA, "遅い push endpoint")
	decisions, err := w.store.NotificationIntentsForMessage(ctx, msg.MessageID)
	if err != nil {
		t.Fatalf("intents: %v", err)
	}
	client := &blockingPushClient{started: make(chan struct{}), release: make(chan struct{})}
	dispatcher := w.dispatcher(t, ctx, client)
	delivered := make(chan struct{})
	go func() {
		defer close(delivered)
		dispatcher.deliver(ctx, w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA).Scope,
			ch, msg, decisions, "Yohaku")
	}()
	select {
	case <-client.started:
	case <-ctx.Done():
		t.Fatal("push never reached the blocked endpoint")
	}

	// This is an exclusive Workspace mutation. It must complete while the
	// endpoint is stalled: the delivery plan's lease was committed before Do.
	mutationCtx, mutationCancel := context.WithTimeout(ctx, 2*time.Second)
	err = w.workspaces.RemoveMember(mutationCtx, workspace.WorkspaceID,
		activeMembershipID(t, ctx, w, workspace.WorkspaceID, w.humanB), w.humanA)
	mutationCancel()
	if err != nil {
		close(client.release)
		<-delivered
		t.Fatalf("workspace mutation waited for slow push: %v", err)
	}
	close(client.release)
	select {
	case <-delivered:
	case <-ctx.Done():
		t.Fatal("push delivery did not finish after release")
	}
}

func TestVAPIDMailtoSubjectIsNormalizedBeforeWebpushBuildsJWT(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, ch := w.workspaceWithChannel(t, ctx)
	w.subscribe(t, ctx, w.humanB, "https://push.example.test/vapid-subject")

	dispatcher, err := NewPushDispatcher(ctx, w.store.core, "mailto:ops@example.test")
	if err != nil {
		t.Fatalf("new dispatcher: %v", err)
	}
	client := &recordingPushClient{}
	dispatcher.client = client
	msg := w.send(t, ctx, ch.PlaceID, w.humanA, "VAPID subject を確認")
	decisions, err := w.store.NotificationIntentsForMessage(ctx, msg.MessageID)
	if err != nil {
		t.Fatalf("intents: %v", err)
	}
	dispatcher.deliver(ctx, w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA).Scope,
		ch, msg, decisions, "Yohaku")

	headers := client.authorizationHeaders()
	if len(headers) != 1 {
		t.Fatalf("VAPID authorization headers = %d, want 1", len(headers))
	}
	if !strings.HasPrefix(headers[0], "vapid t=") {
		t.Fatalf("VAPID authorization = %q", headers[0])
	}
	token, _, found := strings.Cut(strings.TrimPrefix(headers[0], "vapid t="), ", k=")
	if !found {
		t.Fatalf("parse VAPID authorization %q", headers[0])
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("VAPID JWT = %q, want three segments", token)
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("decode VAPID JWT payload: %v", err)
	}
	var claims struct {
		Subject string `json:"sub"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		t.Fatalf("decode VAPID JWT claims: %v", err)
	}
	if claims.Subject != "mailto:ops@example.test" {
		t.Fatalf("VAPID JWT sub = %q, want one mailto prefix", claims.Subject)
	}
}

func TestPushForgetsAnEndpointThePushServiceDeclaredGone(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, ch := w.workspaceWithChannel(t, ctx)
	w.subscribe(t, ctx, w.humanB, "https://push.example.test/dead")

	client := &recordingPushClient{status: map[string]int{
		"https://push.example.test/dead": http.StatusGone,
	}}
	dispatcher := w.dispatcher(t, ctx, client)
	msg := w.send(t, ctx, ch.PlaceID, w.humanA, "おはようございます")
	decisions, err := w.store.NotificationIntentsForMessage(ctx, msg.MessageID)
	if err != nil {
		t.Fatalf("intents: %v", err)
	}
	dispatcher.deliver(ctx, w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA).Scope, ch, msg, decisions, "Yohaku")

	if got := len(w.subscriptionsFor(t, ctx, w.humanB)[w.humanB.Key()]); got != 0 {
		t.Fatalf("a 410 endpoint survived: %d rows", got)
	}
}

func TestPushLogsUnexpectedServiceStatus(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, ch := w.workspaceWithChannel(t, ctx)
	w.subscribe(t, ctx, w.humanB, "https://push.example.test/rejected")

	client := &recordingPushClient{status: map[string]int{
		"https://push.example.test/rejected": http.StatusBadRequest,
	}}
	dispatcher := w.dispatcher(t, ctx, client)
	msg := w.send(t, ctx, ch.PlaceID, w.humanA, "設定を見直してください")
	decisions, err := w.store.NotificationIntentsForMessage(ctx, msg.MessageID)
	if err != nil {
		t.Fatalf("intents: %v", err)
	}

	var logs bytes.Buffer
	previousWriter, previousFlags := log.Writer(), log.Flags()
	log.SetOutput(&logs)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(previousWriter)
		log.SetFlags(previousFlags)
	})
	dispatcher.deliver(ctx, w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA).Scope, ch, msg, decisions, "Yohaku")

	if !strings.Contains(logs.String(), "unexpected response status 400") {
		t.Fatalf("push log = %q, want rejected status", logs.String())
	}
	// endpoint は bearer secret である。status は出しても URL は出さない。
	if strings.Contains(logs.String(), "push.example.test/rejected") {
		t.Fatalf("push log leaked the endpoint: %q", logs.String())
	}
}

func TestPushTitleNamesThePlaceAndTheSpeaker(t *testing.T) {
	channel := Place{PlaceID: "p", Kind: PlaceChannel, Name: "general"}
	if got := PushTitle(channel, "Yohaku"); got != "#general — Yohaku" {
		t.Fatalf("channel title = %q", got)
	}
	dm := Place{PlaceID: "p", Kind: PlaceDM}
	if got := PushTitle(dm, "Yohaku"); got != "Yohaku" {
		t.Fatalf("dm title = %q", got)
	}
}

func TestPushBodyIsAPointerNotTheMessage(t *testing.T) {
	long := strings.Repeat("あ", pushSnippetRunes+40)
	body := PushBody(Message{Content: long})
	if runes := []rune(body); len(runes) != pushSnippetRunes || runes[len(runes)-1] != '…' {
		t.Fatalf("long body = %d runes ending %q", len(runes), string(runes[len(runes)-1]))
	}
	if got := PushBody(Message{Content: "行を\n跨いだ  発言"}); got != "行を 跨いだ 発言" {
		t.Fatalf("collapsed body = %q", got)
	}
	if got := PushBody(Message{Deleted: true, Content: "消えた本文"}); got != "" {
		t.Fatalf("deleted body = %q, want empty", got)
	}
	if got := PushBody(Message{Attachments: []Attachment{{AttachmentID: "a"}}}); got != "（添付ファイル）" {
		t.Fatalf("attachment-only body = %q", got)
	}
}

func TestPushTopicCollapsesOnlyACanonicalPlaceID(t *testing.T) {
	if got := pushTopic("01900000-0000-7000-8000-000000000002"); got != "01900000000070008000000000000002" {
		t.Fatalf("topic = %q", got)
	}
	if got := pushTopic("not-a-uuid"); got != "" {
		t.Fatalf("unexpected topic for a malformed place id: %q", got)
	}
}
