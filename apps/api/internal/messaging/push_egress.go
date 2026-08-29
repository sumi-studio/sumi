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

// Push endpoints are browser-authored URLs that the API later calls. Validate
// both registration-time DNS and the actual dial target so HTTPS and DNS
// rebinding cannot turn Web Push into an internal-network request primitive.
const (
	pushConnectTimeout = 5 * time.Second
	pushRequestTimeout = 10 * time.Second
)

var ErrPushEndpointNotAllowed = fmt.Errorf(
	"%w: endpoint is not an allowed push destination",
	ErrInvalidPushSubscription,
)

type pushEgress struct {
	resolve func(context.Context, string) ([]net.IP, error)
}

func defaultPushEgress() *pushEgress {
	return &pushEgress{resolve: lookupPushIPs}
}

func lookupPushIPs(ctx context.Context, host string) ([]net.IP, error) {
	addresses, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	ips := make([]net.IP, 0, len(addresses))
	for _, address := range addresses {
		ips = append(ips, address.IP)
	}
	return ips, nil
}

func (s *Store) pushEgressPolicy() *pushEgress {
	if s == nil || s.egress == nil {
		return defaultPushEgress()
	}
	return s.egress
}

func (e *pushEgress) allowEndpoint(ctx context.Context, raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "https" || parsed.Hostname() == "" {
		return fmt.Errorf("%w: endpoint must be an absolute HTTPS URL", ErrInvalidPushSubscription)
	}
	if parsed.User != nil {
		return ErrPushEndpointNotAllowed
	}
	if ip := net.ParseIP(parsed.Hostname()); ip != nil {
		if !pushIPIsPublicUnicast(ip) {
			return ErrPushEndpointNotAllowed
		}
		return nil
	}
	lookup, cancel := context.WithTimeout(ctx, pushConnectTimeout)
	defer cancel()
	resolve := lookupPushIPs
	if e != nil && e.resolve != nil {
		resolve = e.resolve
	}
	ips, err := resolve(lookup, parsed.Hostname())
	if err != nil || len(ips) == 0 {
		return ErrPushEndpointNotAllowed
	}
	for _, ip := range ips {
		if !pushIPIsPublicUnicast(ip) {
			return ErrPushEndpointNotAllowed
		}
	}
	return nil
}

func guardPushDialAddress(address string) error {
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

var pushSpecialPurposePrefixes = []netip.Prefix{
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

func newPushHTTPClient() *http.Client {
	dialer := &net.Dialer{
		Timeout: pushConnectTimeout,
		Control: func(_, address string, _ syscall.RawConn) error {
			return guardPushDialAddress(address)
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
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func pushDialWasRefused(err error) bool {
	return err != nil && (errors.Is(err, ErrPushEndpointNotAllowed) ||
		strings.Contains(err.Error(), ErrPushEndpointNotAllowed.Error()))
}
