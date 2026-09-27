package agentstate

import (
	"errors"
	"testing"
)

func TestEffectJobID(t *testing.T) {
	seen := map[string]bool{}
	for key, want := range map[string]string{
		"request:tool:0":         "op:request:0",
		"request:tool:1":         "op:request:1",
		"other:tool:0":           "op:other:0",
		"request:tool:0:tool:12": "op:request:tool:0:12",
		"colon:request::tool:42": "op:colon:request::42",
	} {
		got, err := EffectJobID(key)
		if err != nil || got != want || seen[got] {
			t.Fatalf("%q: %q, %v", key, got, err)
		}
		seen[got] = true
	}
	for _, key := range []string{"", "request", ":tool:0", "request:tool:", "request:tool:-1", "request:tool:01", "request:tool:1:extra", "request:tool:1.0"} {
		if _, err := EffectJobID(key); !errors.Is(err, ErrBadRequest) {
			t.Fatalf("accepted malformed key %q: %v", key, err)
		}
	}
}
