package messaging

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
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

// pushSpecialPurposePrefixes is IANA's special-purpose address space that is
// not globally routable unicast. Keep this as data, not a handful of ad-hoc
// switches: registration and the dial-time fence then reject the same complete
// registry set, and the test exercises every prefix below.
var pushSpecialPurposePrefixes = []netip.Prefix{
	// IPv4
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("10.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("127.0.0.0/8"),
	netip.MustParsePrefix("169.254.0.0/16"),
	netip.MustParsePrefix("172.16.0.0/12"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"),
	netip.MustParsePrefix("192.168.0.0/16"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("224.0.0.0/4"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("255.255.255.255/32"),
	// IPv6
	netip.MustParsePrefix("::/128"),
	netip.MustParsePrefix("::1/128"),
	netip.MustParsePrefix("::ffff:0:0/96"),
	netip.MustParsePrefix("64:ff9b::/96"),
	netip.MustParsePrefix("64:ff9b:1::/48"),
	netip.MustParsePrefix("100::/64"),
	netip.MustParsePrefix("2001::/32"),
	netip.MustParsePrefix("2001:2::/48"),
	netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("2002::/16"),
	netip.MustParsePrefix("fc00::/7"),
	netip.MustParsePrefix("fe80::/10"),
	netip.MustParsePrefix("ff00::/8"),
}

// pushIPIsPublicUnicast is the one predicate. IPv4-mapped IPv6 addresses are
// deliberately reduced to IPv4 before the registry lookup: ::ffff:10.0.0.1
// must not bypass the IPv4 private-range entry.
func pushIPIsPublicUnicast(ip net.IP) bool {
	address, ok := netip.AddrFromSlice(ip)
	if !ok {
		return false
	}
	address = address.Unmap()
	for _, prefix := range pushSpecialPurposePrefixes {
		if prefix.Contains(address) {
			return false
		}
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
