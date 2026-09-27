package agentstate

import (
	"fmt"
	"strings"
)

// EffectJobID gives a delegated tool effect the same origin-bearing job identity
// as job.start. The key is server-owned: <inputID>:tool:<decimal call index>.
// Split only its final suffix: input IDs may themselves contain colons or :tool:.
func EffectJobID(idempotencyKey string) (string, error) {
	i := strings.LastIndex(idempotencyKey, ":tool:")
	if i <= 0 {
		return "", fmt.Errorf("%w: invalid tool effect identity", ErrBadRequest)
	}
	index := idempotencyKey[i+len(":tool:"):]
	if index == "" || (len(index) > 1 && index[0] == '0') {
		return "", fmt.Errorf("%w: invalid tool effect call index", ErrBadRequest)
	}
	for _, c := range index {
		if c < '0' || c > '9' {
			return "", fmt.Errorf("%w: invalid tool effect call index", ErrBadRequest)
		}
	}
	return jobToolPrefix + idempotencyKey[:i] + ":" + index, nil
}
