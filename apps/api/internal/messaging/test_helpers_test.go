package messaging

import (
	"testing"

	"github.com/sumi-studio/sumi/apps/api/internal/testfs"
)

func privateRuntimeDir(t testing.TB) string {
	t.Helper()
	return testfs.PrivateDir(t)
}
