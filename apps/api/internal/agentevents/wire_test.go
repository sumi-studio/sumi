package agentevents

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestContractFixturesRoundTrip(t *testing.T) {
	repoRoot, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(repoRoot, "contracts", "agent-events-fixtures.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixtures: %v", err)
	}

	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var fixtures map[string]any
	if err := d.Decode(&fixtures); err != nil {
		t.Fatalf("decode fixtures: %v", err)
	}

	passed := 0
	for name, value := range fixtures {
		fixture, ok := value.(map[string]any)
		if !ok {
			t.Fatalf("fixture %q is not an object", name)
		}
		kind, _ := fixture["kind"].(string)
		wireRaw, err := json.Marshal(fixture["wire"])
		if err != nil {
			t.Fatalf("fixture %q: marshal wire: %v", name, err)
		}

		switch kind {
		case "outbound_frame":
			var frame OutboundFrame
			if err := json.Unmarshal(wireRaw, &frame); err != nil {
				t.Fatalf("fixture %q: unmarshal OutboundFrame: %v", name, err)
			}
			if err := frame.Validate(); err != nil {
				t.Fatalf("fixture %q: validate OutboundFrame: %v", name, err)
			}
			roundTripJSON(t, name, wireRaw, &frame)
		case "command_envelope":
			var env CommandEnvelope
			if err := json.Unmarshal(wireRaw, &env); err != nil {
				t.Fatalf("fixture %q: unmarshal CommandEnvelope: %v", name, err)
			}
			if err := ValidateCommand(env.Command); err != nil {
				t.Fatalf("fixture %q: validate command: %v", name, err)
			}
			roundTripJSON(t, name, wireRaw, &env)
		default:
			// For agent_event and public_message fixtures, ensure generic JSON
			// round-trips so the contract shapes remain stable in Go as well.
			roundTripGeneric(t, name, wireRaw)
		}
		passed++
	}

	if passed < 10 {
		t.Fatalf("expected at least 10 fixtures, got %d", passed)
	}
}

func roundTripJSON(t *testing.T, name string, original []byte, v any) {
	t.Helper()
	normalizedOriginal := normalizeJSON(t, original)

	out, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("fixture %q: marshal: %v", name, err)
	}
	normalizedRoundtrip := normalizeJSON(t, out)

	if string(normalizedOriginal) != string(normalizedRoundtrip) {
		t.Fatalf("fixture %q round-trip mismatch\noriginal:  %s\nroundtrip: %s", name, normalizedOriginal, normalizedRoundtrip)
	}
}

func roundTripGeneric(t *testing.T, name string, original []byte) {
	t.Helper()
	normalizedOriginal := normalizeJSON(t, original)

	d := json.NewDecoder(bytes.NewReader(original))
	d.UseNumber()
	var v any
	if err := d.Decode(&v); err != nil {
		t.Fatalf("fixture %q: decode: %v", name, err)
	}
	out, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("fixture %q: marshal: %v", name, err)
	}
	normalizedRoundtrip := normalizeJSON(t, out)

	if string(normalizedOriginal) != string(normalizedRoundtrip) {
		t.Fatalf("fixture %q generic round-trip mismatch", name)
	}
}

func normalizeJSON(t *testing.T, data []byte) []byte {
	t.Helper()
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	var v any
	if err := d.Decode(&v); err != nil {
		t.Fatalf("normalize JSON: %v", err)
	}
	out, err := json.Marshal(normalizeValue(v))
	if err != nil {
		t.Fatalf("marshal normalized: %v", err)
	}
	return out
}

func normalizeValue(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for _, k := range sortedKeys(x) {
			out[k] = normalizeValue(x[k])
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = normalizeValue(e)
		}
		return out
	case json.Number:
		if n, err := x.Int64(); err == nil {
			return n
		}
		f, _ := x.Float64()
		return f
	default:
		return v
	}
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return keys
}
