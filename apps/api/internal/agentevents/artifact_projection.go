package agentevents

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
)

const artifactScheme = "artifact://"

type parsedArtifactReference struct {
	end       int
	owner     string
	kind      string
	artifact  string
	isBrowser bool
}

// projectEventArtifactReferences removes the authenticated internal owner only
// from canonical artifact references in system/agent-authored event fields.
// User-role message bodies are deliberately left byte-faithful.
func projectEventArtifactReferences(raw json.RawMessage, owner string) (json.RawMessage, error) {
	return transformEventArtifactReferences(raw, owner, true)
}

// validateInternalEventArtifactReferences prevents a targetless browser
// reference from flowing back into an internal agent event and binds every
// canonical internal reference to the event's authenticated owner. User-role
// messages are exempt because their text is user-authored content.
func validateInternalEventArtifactReferences(raw json.RawMessage, owner string) error {
	_, err := transformEventArtifactReferences(raw, owner, false)
	return err
}

func transformEventArtifactReferences(raw json.RawMessage, owner string, project bool) (json.RawMessage, error) {
	if err := ValidatePersonalityAgentID(owner); err != nil {
		return nil, err
	}
	if err := validateEvent(raw); err != nil {
		return nil, err
	}

	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return nil, err
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return nil, err
	}
	event, ok := decoded.(map[string]any)
	if !ok {
		return nil, errors.New("agent event must be an object")
	}

	var err error
	switch event["type"] {
	case "message_start", "message_end":
		event["message"], err = transformPublicMessageArtifactReferences(event["message"], owner, project)
	case "turn_end":
		if event["message"] != nil {
			event["message"], err = transformPublicMessageArtifactReferences(event["message"], owner, project)
		}
		if err == nil {
			event["tool_results"], err = transformArtifactReferenceValue(event["tool_results"], owner, project)
		}
	case "message_update":
		event["event"], err = transformArtifactReferenceValue(event["event"], owner, project)
	default:
		for key, value := range event {
			if key == "type" {
				continue
			}
			event[key], err = transformArtifactReferenceValue(value, owner, project)
			if err != nil {
				break
			}
		}
	}
	if err != nil {
		return nil, err
	}
	if !project {
		return raw, nil
	}
	projected, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("marshal browser artifact projection: %w", err)
	}
	return projected, nil
}

func transformPublicMessageArtifactReferences(value any, owner string, project bool) (any, error) {
	message, ok := value.(map[string]any)
	if !ok {
		return nil, errors.New("public message must be an object")
	}
	if message["role"] == "user" {
		return value, nil
	}
	return transformArtifactReferenceValue(value, owner, project)
}

func transformArtifactReferenceValue(value any, owner string, project bool) (any, error) {
	switch typed := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		rebuilt := make(map[string]any, len(typed))
		keyOrigins := make(map[string]string, len(typed))
		if project {
			projectedOrigins := make(map[string]string, len(typed))
			for _, key := range keys {
				projectedKey, _ := transformArtifactReferenceStringWithPolicy(key, owner, true, false)
				if previous, exists := projectedOrigins[projectedKey]; exists {
					return nil, fmt.Errorf(
						"artifact projection key collision between %q and %q",
						previous,
						key,
					)
				}
				projectedOrigins[projectedKey] = key
			}
		}
		for _, key := range keys {
			transformedKey, err := transformArtifactReferenceString(key, owner, project)
			if err != nil {
				return nil, fmt.Errorf("artifact reference in object key %q: %w", key, err)
			}
			if previous, exists := keyOrigins[transformedKey]; exists {
				return nil, fmt.Errorf(
					"artifact projection key collision between %q and %q",
					previous,
					key,
				)
			}
			nested := typed[key]
			transformed, err := transformArtifactReferenceValue(nested, owner, project)
			if err != nil {
				return nil, err
			}
			rebuilt[transformedKey] = transformed
			keyOrigins[transformedKey] = key
		}
		return rebuilt, nil
	case []any:
		for index, nested := range typed {
			transformed, err := transformArtifactReferenceValue(nested, owner, project)
			if err != nil {
				return nil, err
			}
			typed[index] = transformed
		}
		return typed, nil
	case string:
		return transformArtifactReferenceString(typed, owner, project)
	default:
		return value, nil
	}
}

func transformArtifactReferenceString(value, owner string, project bool) (string, error) {
	return transformArtifactReferenceStringWithPolicy(value, owner, project, true)
}

func transformArtifactReferenceStringWithPolicy(value, owner string, project, enforceBoundary bool) (string, error) {
	var builder strings.Builder
	searchFrom := 0
	copyFrom := 0
	changed := false
	for {
		relative := strings.Index(value[searchFrom:], artifactScheme)
		if relative < 0 {
			break
		}
		start := searchFrom + relative
		reference, ok := parseArtifactReferenceAt(value, start)
		if !ok {
			searchFrom = start + len(artifactScheme)
			continue
		}
		if reference.isBrowser {
			if enforceBoundary {
				return "", errors.New("targetless browser artifact reference is not valid on the internal event boundary")
			}
			searchFrom = reference.end
			continue
		}
		if reference.owner != owner {
			if enforceBoundary {
				return "", fmt.Errorf("artifact reference owner %q does not match authenticated personality agent", reference.owner)
			}
			searchFrom = reference.end
			continue
		}
		if project {
			if !changed {
				builder.Grow(len(value))
			}
			builder.WriteString(value[copyFrom:start])
			builder.WriteString(artifactScheme)
			builder.WriteString(reference.kind)
			builder.WriteByte('/')
			builder.WriteString(reference.artifact)
			copyFrom = reference.end
			changed = true
		}
		searchFrom = reference.end
	}
	if !changed {
		return value, nil
	}
	builder.WriteString(value[copyFrom:])
	return builder.String(), nil
}

func parseArtifactReferenceAt(value string, start int) (parsedArtifactReference, bool) {
	if start < 0 || !strings.HasPrefix(value[start:], artifactScheme) {
		return parsedArtifactReference{}, false
	}
	if start > 0 && isArtifactComponentByte(value[start-1]) {
		return parsedArtifactReference{}, false
	}
	cursor := start + len(artifactScheme)

	for _, kind := range []string{"attachments", "tool-output"} {
		prefix := kind + "/"
		if strings.HasPrefix(value[cursor:], prefix) {
			artifact, end, ok := parseArtifactID(value, cursor+len(prefix))
			if !ok {
				return parsedArtifactReference{}, false
			}
			return parsedArtifactReference{
				end:       end,
				kind:      kind,
				artifact:  artifact,
				isBrowser: true,
			}, true
		}
	}

	if len(value)-cursor < personalityAgentIDLength+1 {
		return parsedArtifactReference{}, false
	}
	owner := value[cursor : cursor+personalityAgentIDLength]
	if value[cursor+personalityAgentIDLength] != '/' || ValidatePersonalityAgentID(owner) != nil {
		return parsedArtifactReference{}, false
	}
	cursor += personalityAgentIDLength + 1
	for _, kind := range []string{"attachments", "tool-output"} {
		prefix := kind + "/"
		if !strings.HasPrefix(value[cursor:], prefix) {
			continue
		}
		artifact, end, ok := parseArtifactID(value, cursor+len(prefix))
		if !ok {
			return parsedArtifactReference{}, false
		}
		return parsedArtifactReference{
			end:      end,
			owner:    owner,
			kind:     kind,
			artifact: artifact,
		}, true
	}
	return parsedArtifactReference{}, false
}

func parseArtifactID(value string, start int) (string, int, bool) {
	end := start
	for end < len(value) && isArtifactComponentByte(value[end]) {
		end++
	}
	artifact := value[start:end]
	if len(artifact) == 0 || len(artifact) > 200 || artifact == "." || artifact == ".." {
		return "", 0, false
	}
	if end < len(value) && value[end] == '/' {
		return "", 0, false
	}
	return artifact, end, true
}

func isArtifactComponentByte(value byte) bool {
	return value >= 'a' && value <= 'z' ||
		value >= 'A' && value <= 'Z' ||
		value >= '0' && value <= '9' ||
		value == '-' ||
		value == '_' ||
		value == '.'
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("unexpected trailing JSON value")
		}
		return err
	}
	return nil
}
