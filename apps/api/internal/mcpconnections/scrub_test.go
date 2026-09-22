package mcpconnections

import "testing"

func TestScrubRedactsUnionOfOriginalPrivateMatches(t *testing.T) {
	tests := []struct {
		name, input, want string
		private           []string
	}{
		{"prefix", "saved key-prefix-sensitive-suffix", "saved [redacted]", []string{"key-prefix-", "key-prefix-sensitive-suffix"}},
		{"partial overlap", "before abcdef after", "before [redacted] after", []string{"abcd", "cdef"}},
		{"self overlap", "ababa", "[redacted]", []string{"aba"}},
		{"unicode", "前 秘密の鍵です 後", "前 [redacted] 後", []string{"秘密の", "の鍵です"}},
		{"replacement marker", "secret!", "[redacted]!", []string{"secret", "redacted"}},
		{"adjacent", "abcd", "[redacted][redacted]", []string{"ab", "cd"}},
		{"unrelated", "keep this", "keep this", []string{"", "missing"}},
		{"nul without secrets", "before\x00after", "before�after", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, reverse := range []bool{false, true} {
				private := append([]string(nil), tt.private...)
				if reverse {
					for i, j := 0, len(private)-1; i < j; i, j = i+1, j-1 {
						private[i], private[j] = private[j], private[i]
					}
				}
				if got := scrub(tt.input, private); got != tt.want {
					t.Fatalf("reverse=%v: got %q, want %q", reverse, got, tt.want)
				}
			}
		})
	}
}
