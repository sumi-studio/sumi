package testfs

import (
	"os"
	"testing"
)

func TestPrivateDirIsOwnerOnly(t *testing.T) {
	dir := PrivateDir(t)
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o700 {
		t.Fatalf("private directory mode = %04o, want 0700", got)
	}
}
