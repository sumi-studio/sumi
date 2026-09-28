package runtimeprovision

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestProcessFilesBindingSurvivesRestartAndRefusesRetarget(t *testing.T) {
	root := t.TempDir()
	id, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}
	pa := id.String()
	scope := filepath.Join(root, strings.ReplaceAll(pa, "-", ""))
	if err := os.Mkdir(scope, 0700); err != nil {
		t.Fatal(err)
	}
	check := filepath.Join(root, "check")
	if err := os.WriteFile(check, []byte("#!/bin/sh\n[ \"$1\" != --wait ] || shift 2\nprintf '%s/%s\\n' \"$1\" \"$3\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	config := ServiceConfig{StateDirectory: filepath.Join(root, "state"), Files: FilesEnvironment{Mountpoint: root, VolumeUUID: "original-volume", CheckPath: check}}
	open := func(c ServiceConfig) *Service {
		t.Helper()
		s, err := NewService(&processTestBackend{}, c)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	req := ProcessStartRequest{PersonalityAgentID: pa, OriginatingToolCallID: "first", Executable: "/bin/true", Image: "job", Workspace: "files-scope"}
	first := open(config)
	op, err := first.StartProcess(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	restarted := open(config)
	replay, err := restarted.StartProcess(context.Background(), req)
	if err != nil || replay.OperationID != op.OperationID {
		t.Fatalf("restart replay: %+v %v", replay, err)
	}
	if binding, bound := restarted.files.lookup(pa); !bound || binding.VolumeUUID != "original-volume" {
		t.Fatalf("lost binding: %+v", binding)
	}
	req.OriginatingToolCallID = "new-request"
	for _, volume := range []string{"different-volume", ""} {
		altered := config
		altered.Files.VolumeUUID = volume
		s := open(altered)
		if _, err := s.StartProcess(context.Background(), req); err == nil {
			t.Fatalf("accepted substituted volume %q", volume)
		}
		if len(s.processes.records) != 1 {
			t.Fatal("rejected substitution was journalled")
		}
	}
}
