package agentevents

import "fmt"

const (
	canonicalUUIDv7Length = 36
	// Kept for callers that slice canonical ids out of longer keys.
	personalityAgentIDLength = canonicalUUIDv7Length
)

// ValidatePersonalityAgentID accepts only the canonical lowercase hyphenated
// RFC UUIDv7 representation. It deliberately does not normalize input because
// this identifier is used as a global runtime and durable-storage key.
func ValidatePersonalityAgentID(value string) error {
	return validateCanonicalUUIDv7("personality_agent_id", value)
}

// ValidateHumanID accepts only the canonical lowercase hyphenated RFC UUIDv7
// representation of a 戸籍 HumanId (ADR 0009 §1). Firebase principals are
// credentials, not identity (ADR 0009 §2), and are never accepted here.
func ValidateHumanID(value string) error {
	return validateCanonicalUUIDv7("human_id", value)
}

func validateCanonicalUUIDv7(field, value string) error {
	if len(value) != canonicalUUIDv7Length {
		return fmt.Errorf("%s must be a 36-byte canonical UUIDv7", field)
	}
	for i := 0; i < len(value); i++ {
		switch i {
		case 8, 13, 18, 23:
			if value[i] != '-' {
				return fmt.Errorf("%s must be hyphenated", field)
			}
		default:
			if !isLowerHex(value[i]) {
				return fmt.Errorf("%s must contain only lowercase hexadecimal digits", field)
			}
		}
	}
	if value[14] != '7' {
		return fmt.Errorf("%s must use UUID version 7, got %q", field, value[14])
	}
	switch value[19] {
	case '8', '9', 'a', 'b':
	default:
		return fmt.Errorf("%s must use the RFC UUID variant, got %q", field, value[19])
	}
	return nil
}

func isLowerHex(value byte) bool {
	return value >= '0' && value <= '9' || value >= 'a' && value <= 'f'
}
