package agentstate

import (
	"crypto/sha256"
	"embed"
	"fmt"
	"regexp"
)

// These app guides are part of the release, not files in a secretary's
// workspace. Reading one uses the ordinary operation receipt, so retrying a
// saved call returns the version that was actually read, even across deploys.
//
//go:embed skills/*.md
var bundledSkills embed.FS

var skillNamePattern = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)

func readSkill(request map[string]any) (map[string]any, error) {
	name, _ := request["name"].(string)
	if len(request) != 1 || !skillNamePattern.MatchString(name) {
		return nil, fmt.Errorf("%w: skill.read requires one catalog name", ErrBadRequest)
	}
	body, err := bundledSkills.ReadFile("skills/" + name + ".md")
	if err != nil {
		return nil, fmt.Errorf("%w: unknown skill %q; use a name from the skill.read catalog", ErrBadRequest, name)
	}
	return map[string]any{
		"name":       name,
		"content":    string(body),
		"media_type": "text/markdown",
		"revision":   fmt.Sprintf("%x", sha256.Sum256(body)),
	}, nil
}
