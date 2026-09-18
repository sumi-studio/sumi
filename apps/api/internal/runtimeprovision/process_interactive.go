package runtimeprovision

// Interactive (PTY) process support for the Docker backend.
//
// One interactive operation is ONE long-lived container. Input is
// carried by a supervised `docker attach --no-stdout --no-stderr`
// child — never a sequence of `docker exec` calls, which would make
// each keystroke a new process and break session semantics. Output is
// read from the daemon's own json-file journal at a persisted byte
// offset: resumable across provisioner restarts without replaying
// backlog into duplicate scrollback, and rotation/truncation is
// journaled as an explicit gap — bytes are never silently skipped or
// duplicated.

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// processSink is a supervised child whose stdin reaches the
// container's input stream.
type processSink struct {
	io.WriteCloser
	cmd *exec.Cmd
}

func (s *processSink) Close() error {
	err := s.WriteCloser.Close()
	if s.cmd != nil && s.cmd.Process != nil {
		_ = s.cmd.Process.Kill()
		_ = s.cmd.Wait()
	}
	return err
}

func (s *processSink) Wait() error {
	if s.cmd == nil {
		return nil
	}
	return s.cmd.Wait()
}

// dockerHostSocket locates the daemon's unix socket from the backend
// environment. A non-unix DOCKER_HOST cannot serve the resize call
// below and reports ErrProcessResizeUnsupported rather than guessing.
func (b *DockerBackend) dockerHostSocket() (string, error) {
	host := ""
	for _, v := range b.baseEnvironment {
		if strings.HasPrefix(v, "DOCKER_HOST=") {
			host = strings.TrimPrefix(v, "DOCKER_HOST=")
		}
	}
	if host == "" {
		host = "unix:///var/run/docker.sock"
	}
	u, err := url.Parse(host)
	if err != nil || u.Scheme != "unix" || u.Path == "" {
		return "", fmt.Errorf("%w: DOCKER_HOST is not a unix socket", ErrProcessResizeUnsupported)
	}
	return u.Path, nil
}

// dockerJournalRoot resolves where container json-file journals are
// readable once per backend. SUMI_DOCKER_JOURNAL_ROOT overrides the
// resolution: the daemon's DockerRootDir is a host path, and a
// provisioner running in a container reads it wherever the deployer
// mounted the data root — the override names that mount point rather
// than pretending the host path is valid locally.
func (b *DockerBackend) dockerJournalRoot(ctx context.Context) (string, error) {
	b.journalRootOnce.Do(func() {
		for _, v := range b.baseEnvironment {
			if strings.HasPrefix(v, "SUMI_DOCKER_JOURNAL_ROOT=") {
				b.journalRoot = strings.TrimSpace(strings.TrimPrefix(v, "SUMI_DOCKER_JOURNAL_ROOT="))
				return
			}
		}
		raw, err := b.processDocker(ctx, "info", "--format", "{{.DockerRootDir}}")
		if err == nil {
			b.journalRoot = strings.TrimSpace(string(raw))
		}
		b.journalRootErr = err
	})
	if b.journalRootErr != nil {
		return "", b.journalRootErr
	}
	if b.journalRoot == "" {
		return "", fmt.Errorf("%w: docker journal root unresolved", ErrProcessNotFound)
	}
	return b.journalRoot, nil
}

// ProcessJournalPath resolves the container's json-file journal on the
// host filesystem. The pump reads this file rather than `docker logs
// --follow` because it is the daemon's own durable record of the
// output stream: resumable at a persisted byte offset across
// provisioner restarts (no backlog replay, no duplicated scrollback),
// gap-detectable on rotation (inode change or truncation), and
// readable for a dead container that `logs --follow` cannot attach.
func (b *DockerBackend) ProcessJournalPath(ctx context.Context, o ProcessOperation) (string, error) {
	if !o.Interactive {
		return "", ErrProcessNotInteractive
	}
	root, err := b.dockerJournalRoot(ctx)
	if err != nil {
		return "", err
	}
	raw, err := b.processDocker(ctx, "inspect", "--format", "{{.Id}}", processContainer(o))
	if err != nil {
		return "", errors.Join(fmt.Errorf("%w: interactive journal lookup", ErrProcessNotFound), err)
	}
	id := strings.TrimSpace(string(raw))
	if id == "" {
		return "", fmt.Errorf("%w: interactive journal lookup", ErrProcessNotFound)
	}
	return filepath.Join(root, "containers", id, id+"-json.log"), nil
}

// journalEntry is one docker json-file log record. With a TTY the
// container writes a single merged stream; the stream field is still
// decoded so a malformed record is never silently mixed in.
type journalEntry struct {
	Log    string `json:"log"`
	Stream string `json:"stream"`
}

// OpenProcessInput opens a stdin-only stream to the container through
// the daemon API. The docker CLI's `attach` cannot detach output
// streams on the daemon versions this provisioner supports, and a
// stdout attachment would only duplicate bytes the journal tailer
// already owns — so input is a raw HTTP-upgrade hijack carrying stdin
// and nothing else. Closing the stream detaches; the container's
// stdin stays open (created with --interactive) and a later call
// reattaches. A non-unix DOCKER_HOST cannot be hijacked this way and
// fails honestly.
func (b *DockerBackend) OpenProcessInput(ctx context.Context, o ProcessOperation) (*processSink, error) {
	socket, err := b.dockerHostSocket()
	if err != nil {
		return nil, fmt.Errorf("%w: process input attach", ErrProcessInputUnsupported)
	}
	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", socket)
	if err != nil {
		return nil, fmt.Errorf("attach process input: %w", err)
	}
	fail := func(e error) (*processSink, error) {
		_ = conn.Close()
		return nil, e
	}
	req := fmt.Sprintf("POST /containers/%s/attach?stream=1&stdin=1&stdout=0&stderr=0 HTTP/1.1\r\nHost: docker\r\nUser-Agent: sumi-runtime-provisioner\r\nConnection: Upgrade\r\nUpgrade: tcp\r\nContent-Length: 0\r\n\r\n", url.PathEscape(processContainer(o)))
	if _, err = conn.Write([]byte(req)); err != nil {
		return fail(fmt.Errorf("attach process input: %w", err))
	}
	br := bufio.NewReader(conn)
	status, err := br.ReadString('\n')
	if err != nil {
		return fail(fmt.Errorf("attach process input: %w", err))
	}
	if !strings.Contains(status, "101") {
		body, _ := io.ReadAll(io.LimitReader(br, 8192))
		return fail(fmt.Errorf("attach process input: %s: %s", strings.TrimSpace(status), strings.TrimSpace(string(body))))
	}
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return fail(fmt.Errorf("attach process input: %w", err))
		}
		if line == "\r\n" || line == "\n" {
			break
		}
	}
	// The hijacked connection now carries raw stdin bytes; the daemon
	// copies them to the container's input fifo until we detach.
	return &processSink{WriteCloser: conn}, nil
}

// SignalProcess delivers one allowlisted signal to the interactive
// session's foreground process group (see signalProcessGroup in the
// platform file) — the same target a keystroke signal would hit in
// canonical mode, never indiscriminately PID 1. Delivery is a real
// host-side group signal, so it also reaches foreground jobs that put
// the line discipline in raw mode (where a ^C byte would be data, not
// an interrupt).
func (b *DockerBackend) SignalProcess(ctx context.Context, o ProcessOperation, signal string) error {
	sig := strings.ToUpper(signal)
	if _, ok := processSignalAllowlist[sig]; !ok {
		return fmt.Errorf("%w: signal not permitted", ErrInvalidProcessRequest)
	}
	return b.signalProcessGroup(ctx, o, sig)
}

// ResizeProcess sets the interactive container's TTY winsize through
// the daemon API — there is no `docker` CLI verb for it. Only a unix
// DOCKER_HOST is supported; anything else is a typed
// ErrProcessResizeUnsupported, never a silent no-op.
func (b *DockerBackend) ResizeProcess(ctx context.Context, o ProcessOperation, cols, rows int) error {
	socket, err := b.dockerHostSocket()
	if err != nil {
		return err
	}
	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", socket)
			},
		},
	}
	q := url.Values{"h": {strconv.Itoa(rows)}, "w": {strconv.Itoa(cols)}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"http://docker/v1.41/containers/"+processContainer(o)+"/resize?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("resize request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
		return nil
	}
	var buf bytes.Buffer
	_, _ = io.CopyN(&buf, resp.Body, 4096)
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusNotImplemented {
		return fmt.Errorf("%w: daemon returned %d: %s", ErrProcessResizeUnsupported, resp.StatusCode, strings.TrimSpace(buf.String()))
	}
	return fmt.Errorf("resize failed: daemon returned %d: %s", resp.StatusCode, strings.TrimSpace(buf.String()))
}

// ErrAttachUnavailable reports an input attach that could not be
// established or was lost. Whether any bytes reached the container is
// unknowable at this boundary, so callers map it to the indeterminate
// receipt rather than a clean not-delivered.
var ErrAttachUnavailable = errors.New("process input attach unavailable")

// ErrSignalUnavailable reports a signal that could not be resolved to
// a concrete target — e.g. the session's process group is not
// resolvable on this platform. It is never a silent no-op.
var ErrSignalUnavailable = errors.New("interactive signal target unavailable")
