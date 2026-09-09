package publicweb

import "net/netip"

// Conservative initial policy: exclude every listed special-purpose block,
// including globally reachable protocol infrastructure, plus multicast. Sources
// checked 2026-09-09:
// https://www.iana.org/assignments/iana-ipv4-special-registry/
// https://www.iana.org/assignments/iana-ipv6-special-registry/
// This is destination classification, not a claim about arbitrary host routing.
var excludedPrefixes = []netip.Prefix{
	// IPv4 special-purpose and multicast ranges.
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("10.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("127.0.0.0/8"),
	netip.MustParsePrefix("169.254.0.0/16"),
	netip.MustParsePrefix("172.16.0.0/12"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.31.196.0/24"),
	netip.MustParsePrefix("192.52.193.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"),
	netip.MustParsePrefix("192.168.0.0/16"),
	netip.MustParsePrefix("192.175.48.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("224.0.0.0/4"),
	netip.MustParsePrefix("240.0.0.0/4"),

	// IPv6 special-purpose ranges inside the global-unicast allocation.
	netip.MustParsePrefix("2001::/23"),
	netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("2002::/16"),
	netip.MustParsePrefix("2620:4f:8000::/48"),
	netip.MustParsePrefix("3fff::/20"),
}
var ipv6Global = netip.MustParsePrefix("2000::/3")

func publicAddress(ip netip.Addr) bool {
	if !ip.IsValid() || ip.Zone() != "" || ip.Is4In6() || !ip.IsGlobalUnicast() {
		return false
	}
	// Excludes mapped/translation, ULA, link-local, reserved and other non-global
	// IPv6 space without attempting to decode embedded IPv4/tunnel destinations.
	if ip.Is6() && !ipv6Global.Contains(ip) {
		return false
	}
	for _, prefix := range excludedPrefixes {
		if prefix.Contains(ip) {
			return false
		}
	}
	return true
}
