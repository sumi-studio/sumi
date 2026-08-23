//go:build linux

package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"

	"github.com/sumi-studio/sumi/apps/api/internal/composeanchor"
)

func main() {
	if err := composeanchor.Run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "sumi-compose-anchor: %v\n", err)
		var exitError *exec.ExitError
		if errors.As(err, &exitError) && exitError.ExitCode() > 0 {
			os.Exit(exitError.ExitCode())
		}
		os.Exit(125)
	}
}
