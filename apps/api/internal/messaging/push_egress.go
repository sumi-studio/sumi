package messaging

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"syscall"
	"time"
)

// push endpoint はブラウザが持ってくる **外から与えられた URL** で、サーバーは
// そこへ自分から出ていく。つまりこの層は egress であり、「https で始まる」は
// 出口の制御ではない（内部の TLS 終端は https で始まる）。ここに置くのは
// deployment がどこへ出てよいかの唯一の判断で、登録時の検査と送信時の接続の
// 両方が同じ述語を通る。
//
// 二段構えなのは、名前解決と接続のあいだに答えが変わりうるから（DNS
// rebinding）。登録時に解決して弾き、送信時には **実際に繋ぐ IP** を
// dialer で見る。allowlist を足すこともできるが、それだけにはしない——
// 自前の push service を閉じてしまうと、この deployment の外に出る自由を
// 奪うことになる。

// pushConnectTimeout / pushRequestTimeout bound one outbound push request.
// 相手が黙り込んだときに送信 goroutine を抱え込まない。
const (
	pushConnectTimeout = 5 * time.Second
	pushRequestTimeout = 10 * time.Second
)

// ErrPushEndpointNotAllowed marks an endpoint this deployment refuses to call.
// 呼び出し側から見ると不正な購読なので、ErrInvalidPushSubscription を包む。
var ErrPushEndpointNotAllowed = fmt.Errorf("%w: endpoint is not an allowed push destination", ErrInvalidPushSubscription)

// pushEgress is the single decision about which destinations Web Push may reach.
type pushEgress struct {
	// resolve は host を IP に開く。テストが差し替えられるのは「名前が何に
	// 解決されるか」だけで、「どの IP なら出てよいか」は差し替えられない。
	resolve func(ctx context.Context, host string) ([]net.IP, error)
}

func defaultPushEgress() *pushEgress {
	return &pushEgress{resolve: lookupPushIPs}
}

func lookupPushIPs(ctx context.Context, host string) ([]net.IP, error) {
	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	ips := make([]net.IP, 0, len(addrs))
	for _, addr := range addrs {
		ips = append(ips, addr.IP)
	}
	return ips, nil
}

// pushEgressPolicy returns the store's egress policy, defaulting to real DNS.
func (s *Store) pushEgressPolicy() *pushEgress {
	if s == nil || s.egress == nil {
		return defaultPushEgress()
	}
	return s.egress
}

// allowEndpoint decides whether this deployment may POST to the given endpoint.
// 登録の時点で断るのは、届かない購読を抱えないためだけではなく、押し込まれた
// URL がそのまま将来の送信になるからである。
func (e *pushEgress) allowEndpoint(ctx context.Context, raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("%w: endpoint", ErrInvalidPushSubscription)
	}
	if parsed.Scheme != "https" {
		return fmt.Errorf("%w: endpoint must be https", ErrInvalidPushSubscription)
	}
	// user:password@ を許すと、ログや比較の上で同じ endpoint が別物に見える。
	if parsed.User != nil {
		return ErrPushEndpointNotAllowed
	}
	host := parsed.Hostname()
	if host == "" {
		return fmt.Errorf("%w: endpoint", ErrInvalidPushSubscription)
	}
	if ip := net.ParseIP(host); ip != nil {
		if !pushIPIsPublicUnicast(ip) {
			return ErrPushEndpointNotAllowed
		}
		return nil
	}
	ips, err := e.resolveHost(ctx, host)
	if err != nil {
		// 名前が引けないものは通さない。ここで通すと「あとで内部を指す名前」を
		// 登録できてしまう。
		return ErrPushEndpointNotAllowed
	}
	if len(ips) == 0 {
		return ErrPushEndpointNotAllowed
	}
	for _, ip := range ips {
		if !pushIPIsPublicUnicast(ip) {
			return ErrPushEndpointNotAllowed
		}
	}
	return nil
}

func (e *pushEgress) resolveHost(ctx context.Context, host string) ([]net.IP, error) {
	resolve := lookupPushIPs
	if e != nil && e.resolve != nil {
		resolve = e.resolve
	}
	lookup, cancel := context.WithTimeout(ctx, pushConnectTimeout)
	defer cancel()
	return resolve(lookup, host)
}

// guardDialAddress is the same decision at the moment of connecting. 解決結果が
// 登録時から変わっていても（rebinding）、繋ぐ相手そのものを見ているので効く。
func guardDialAddress(address string) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return ErrPushEndpointNotAllowed
	}
	ip := net.ParseIP(host)
	if ip == nil || !pushIPIsPublicUnicast(ip) {
		return ErrPushEndpointNotAllowed
	}
	return nil
}

// pushIPIsPublicUnicast is the one predicate. 「公開のユニキャストだけ」を
// 許可の側で書く：拒否の列挙は取りこぼすが、許可の側で書けば知らない範囲は
// 自動的に外れる。
func pushIPIsPublicUnicast(ip net.IP) bool {
	if ip == nil {
		return false
	}
	// ::ffff:10.0.0.1 のような写像は IPv4 として判定する。
	if mapped := ip.To4(); mapped != nil {
		ip = mapped
	}
	if ip.IsUnspecified() || ip.IsLoopback() || ip.IsPrivate() ||
		ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsInterfaceLocalMulticast() || ip.IsMulticast() {
		return false
	}
	if v4 := ip.To4(); v4 != nil {
		switch {
		case v4[0] == 0:
			return false
		case v4[0] == 100 && v4[1] >= 64 && v4[1] <= 127: // CGNAT 100.64.0.0/10
			return false
		case v4[0] == 192 && v4[1] == 0 && v4[2] == 0: // IETF protocol assignments
			return false
		case v4[0] == 255 && v4[1] == 255 && v4[2] == 255 && v4[3] == 255:
			return false
		case v4[0] >= 240: // 240.0.0.0/4（将来用）
			return false
		}
		return true
	}
	// IPv6: unique local (fc00::/7) と、IPv4 を包む形（6to4 / Teredo）は通さない。
	if len(ip) != net.IPv6len {
		return false
	}
	switch {
	case ip[0]&0xfe == 0xfc:
		return false
	case ip[0] == 0x20 && ip[1] == 0x02: // 2002::/16 6to4
		return false
	case ip[0] == 0x20 && ip[1] == 0x01 && ip[2] == 0x00 && ip[3] == 0x00: // 2001::/32 Teredo
		return false
	}
	return true
}

// newPushHTTPClient builds the client every push send goes through. policy は
// dialer に埋め込む：送信経路がひとつしかないので、ここを通らない送信が
// 生まれない。
func newPushHTTPClient() *http.Client {
	dialer := &net.Dialer{
		Timeout: pushConnectTimeout,
		Control: func(_, address string, _ syscall.RawConn) error {
			return guardDialAddress(address)
		},
	}
	return &http.Client{
		Timeout: pushRequestTimeout,
		Transport: &http.Transport{
			DialContext:           dialer.DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConnsPerHost:   4,
			IdleConnTimeout:       30 * time.Second,
			TLSHandshakeTimeout:   pushConnectTimeout,
			ResponseHeaderTimeout: pushRequestTimeout,
		},
		// redirect は追わない。追えば、審査を通った endpoint が別の宛先へ
		// 化ける経路になる。3xx はそのまま「予期しない status」として扱う。
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// pushDialWasRefused reports whether a transport error came from this policy.
// ログを読む人に「相手が落ちている」と「出てはいけない先だった」を区別させる。
func pushDialWasRefused(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrPushEndpointNotAllowed) {
		return true
	}
	return strings.Contains(err.Error(), ErrPushEndpointNotAllowed.Error())
}
