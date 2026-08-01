package main

import (
	"slices"
	"testing"
)

func TestHostEnvironmentForwardsPinnedAgentImageTag(t *testing.T) {
	const tag = "a1b2c3d4e5f6"
	t.Setenv("SUMI_AGENT_IMAGE_TAG", tag)

	environment := hostEnvironment()
	if !slices.Contains(environment, "SUMI_AGENT_IMAGE_TAG="+tag) {
		t.Fatalf("hostEnvironment() = %v, want pinned agent image tag", environment)
	}
}
