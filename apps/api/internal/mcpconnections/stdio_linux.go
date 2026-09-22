//go:build linux

package mcpconnections

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"sync"
	"syscall"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"golang.org/x/sys/unix"
)

// Retain the unreaped group leader until group cleanup. A saved PID or a
// reaped child's number is never used to signal a later/reused process group.
// Descendants that deliberately leave this group are outside this local-user
// lifecycle boundary; this is not a container or sandbox.
func startStdio(ctx context.Context, cfg LocalInput) (mcp.Transport, func(), error) {
	stdinR, stdinW, e := os.Pipe()
	if e != nil {
		return nil, nil, e
	}
	stdoutR, stdoutW, e := os.Pipe()
	if e != nil {
		stdinR.Close()
		stdinW.Close()
		return nil, nil, e
	}
	null, e := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if e != nil {
		stdinR.Close()
		stdinW.Close()
		stdoutR.Close()
		stdoutW.Close()
		return nil, nil, e
	}
	cmd := exec.Command(cfg.Command, cfg.Args...)
	cmd.Dir = cfg.Cwd
	cmd.Env = []string{"PATH=/usr/local/bin:/usr/bin:/bin", "LANG=C.UTF-8"}
	for k, v := range cfg.Env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	cmd.Stdin = stdinR
	cmd.Stdout = stdoutW
	cmd.Stderr = null
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	e = cmd.Start()
	stdinR.Close()
	stdoutW.Close()
	null.Close()
	if e != nil {
		stdinW.Close()
		stdoutR.Close()
		return nil, nil, e
	}
	var stopOnce sync.Once
	stop := func() {
		stopOnce.Do(func() { _ = unix.Kill(-cmd.Process.Pid, unix.SIGKILL); stdinW.Close() })
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		var info unix.Siginfo
		for {
			err := unix.Waitid(unix.P_PID, cmd.Process.Pid, &info, unix.WEXITED|unix.WNOWAIT, nil)
			if err != unix.EINTR {
				break
			}
		}
		stop() // group identity remains pinned by the unreaped leader
		_ = cmd.Wait()
	}()
	go func() {
		select {
		case <-ctx.Done():
			stop()
			stdoutR.Close()
		case <-done:
		}
	}()
	// v1.3.1's SDK IOTransport has no per-line limit. This bounded adapter
	// rejects oversized frames before handing bytes to its JSON-RPC decoder.
	reader := &boundedFrames{source: stdoutR, reader: bufio.NewReaderSize(stdoutR, 2<<20)}
	return &mcp.IOTransport{Reader: reader, Writer: stdinW}, func() { stop(); stdoutR.Close(); <-done }, nil
}

type boundedFrames struct {
	source  io.ReadCloser
	reader  *bufio.Reader
	pending []byte
}

func (r *boundedFrames) Read(p []byte) (int, error) {
	if len(r.pending) == 0 {
		line, e := r.reader.ReadSlice('\n')
		if e == bufio.ErrBufferFull {
			return 0, errors.New("MCP frame exceeds 2 MiB")
		}
		if e != nil {
			return 0, e
		}
		r.pending = line
	}
	n := copy(p, r.pending)
	r.pending = r.pending[n:]
	return n, nil
}
func (r *boundedFrames) Close() error { return r.source.Close() }
