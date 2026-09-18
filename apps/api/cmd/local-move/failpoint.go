//go:build movefailpoint

package main

import (
	"os"
	"syscall"
)

// Test-only failpoints: SUMI_LOCAL_MOVE_FAILPOINT=before-complete,
// after-complete or promote-mid kills this process with SIGKILL at that
// point, so nothing after it runs — no deferred unlock, no state-file
// write, no output. promote-mid kills after the third journaled
// placement so a promotion crash lands mid-tree. Release builds do not
// include this file.
func init() {
	stop := func() {
		_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
		select {}
	}
	switch os.Getenv("SUMI_LOCAL_MOVE_FAILPOINT") {
	case "before-complete":
		beforeComplete = stop
	case "after-complete":
		afterComplete = stop
	case "promote-mid":
		placed := 0
		midPromote = func() {
			placed++
			if placed >= 3 {
				stop()
			}
		}
	}
}
