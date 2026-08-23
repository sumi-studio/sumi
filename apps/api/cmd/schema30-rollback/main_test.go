package main

import (
	"context"
	"io"
	"strings"
	"testing"
)

func TestRunRejectsUnknownOperationBeforeOpeningDatabase(t *testing.T) {
	err := run(context.Background(), []string{"rollback-many"}, func(string) string {
		return "postgres://unused"
	}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "preflight or apply") {
		t.Fatalf("run error = %v, want exact-operation refusal", err)
	}
}

func TestRunDoesNotExposeDatabaseURLOnOpenFailure(t *testing.T) {
	const secret = "do-not-echo-this-password"
	err := run(context.Background(), []string{"preflight"}, func(name string) string {
		if name == "SUMI_DB_URL" {
			return "postgres://operator:" + secret + "@%zz"
		}
		return ""
	}, io.Discard)
	if err == nil {
		t.Fatal("run unexpectedly opened invalid database URL")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("run exposed database secret: %v", err)
	}
}
