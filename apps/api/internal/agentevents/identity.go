package agentevents

import (
	"errors"
	"fmt"
)

const personalityAgentIDLength = 36

// ValidatePersonalityAgentID accepts only the canonical lowercase hyphenated
// RFC UUIDv7 representation. It deliberately does not normalize input because
// this identifier is used as a global runtime and durable-storage key.
func ValidatePersonalityAgentID(value string) error {
	if len(value) != personalityAgentIDLength {
		return errors.New("personality_agent_id must be a 36-byte canonical UUIDv7")
	}
	for i := 0; i < len(value); i++ {
		switch i {
		case 8, 13, 18, 23:
			if value[i] != '-' {
				return errors.New("personality_agent_id must be hyphenated")
			}
		default:
			if !isLowerHex(value[i]) {
				return errors.New("personality_agent_id must contain only lowercase hexadecimal digits")
			}
		}
	}
	if value[14] != '7' {
		return fmt.Errorf("personality_agent_id must use UUID version 7, got %q", value[14])
	}
	switch value[19] {
	case '8', '9', 'a', 'b':
	default:
		return fmt.Errorf("personality_agent_id must use the RFC UUID variant, got %q", value[19])
	}
	return nil
}

func isLowerHex(value byte) bool {
	return value >= '0' && value <= '9' || value >= 'a' && value <= 'f'
}
