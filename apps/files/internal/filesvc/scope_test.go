package filesvc

import (
	"strings"
	"testing"
)

// F300: validScope must accept exactly the scope grammar
// deploy/files/sumi-files-check accepts —
// [A-Za-z0-9][A-Za-z0-9._-]* without ".." — so a scope the API serves can
// always be verified by the mount check and bound by the executor launch.
// Compact personality-agent ids are a strict subset.
func TestValidScopeGrammar(t *testing.T) {
	accept := []string{
		"0192f3a47b2c7def8abc1234567890ab", // compact PAID
		"a", "Z9",
		"app.v2", "app_v2", "app-v2",
		"a.b-c_d",
		strings.Repeat("s", 255),
	}
	for _, s := range accept {
		if err := validScope(s); err != nil {
			t.Fatalf("scope %q must be accepted: %v", s, err)
		}
	}
	reject := []string{
		"", ".", "..", ".hidden", "_lead", "-lead",
		"a..b", "..x", "x..",
		"a/b", "a\\b", "a b", "a:b",
		"/abs", "trail/",
		strings.Repeat("s", 256),
	}
	for _, s := range reject {
		if err := validScope(s); err == nil {
			t.Fatalf("scope %q must be refused", s)
		}
	}
}
