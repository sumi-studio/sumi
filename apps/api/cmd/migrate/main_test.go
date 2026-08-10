package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestRunManifestNeedsNoDatabaseAndIsMachineReadable(t *testing.T) {
	t.Setenv("SUMI_DB_URL", "")
	var output bytes.Buffer
	if err := run(context.Background(), []string{"manifest"}, &output); err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		ManifestSHA256 string `json:"manifest_sha256"`
		Migrations     []struct {
			Version int    `json:"version"`
			Name    string `json:"name"`
			SHA256  string `json:"sha256"`
		} `json:"migrations"`
	}
	if err := json.Unmarshal(output.Bytes(), &decoded); err != nil {
		t.Fatalf("manifest is not JSON: %v\n%s", err, output.String())
	}
	if len(decoded.ManifestSHA256) != 64 || len(decoded.Migrations) == 0 {
		t.Fatalf("incomplete manifest: %+v", decoded)
	}
}

func TestRunRejectsUnknownModeBeforeDatabaseAccess(t *testing.T) {
	t.Setenv("SUMI_DB_URL", "")
	err := run(context.Background(), []string{"rewrite"}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "unknown mode") {
		t.Fatalf("unexpected error: %v", err)
	}
}
