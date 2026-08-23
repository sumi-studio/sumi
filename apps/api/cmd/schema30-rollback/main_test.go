package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/sumi-studio/sumi/apps/api/internal/db"
)

func TestRunRejectsUnknownOperationBeforeOpeningDatabase(t *testing.T) {
	err := run(context.Background(), []string{"rollback-many"}, func(string) string {
		return "postgres://unused"
	}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "preflight or apply") {
		t.Fatalf("run error = %v, want exact-operation refusal", err)
	}
}

func TestRollbackCommitOutcomeUnknownRemainsDistinguishable(t *testing.T) {
	err := classifyApplyError(fmt.Errorf("transport boundary: %w", db.ErrSchema30RollbackCommitOutcomeUnknown))
	if !errors.Is(err, db.ErrSchema30RollbackCommitOutcomeUnknown) {
		t.Fatal("commit outcome-unknown classification was lost")
	}
	if strings.Contains(err.Error(), "rollback refused") {
		t.Fatalf("commit outcome-unknown was misreported as a refusal: %v", err)
	}
	if strings.Contains(err.Error(), "postgres://") {
		t.Fatalf("commit outcome-unknown error exposed connection detail: %v", err)
	}

	ordinary := classifyApplyError(errors.New("unsafe data"))
	if !strings.Contains(ordinary.Error(), "rollback refused") {
		t.Fatalf("ordinary rollback error lost refusal classification: %v", ordinary)
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
