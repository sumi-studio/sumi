package fileaccess

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
)

// Core file tools. Every effect scopes itself to the claiming persona —
// the persona_id comes from the operation ledger, never from the request —
// so a secretary can only ever reach its own filesvc scope.
//
// Bounded payloads keep tool traffic inside the state service's request/
// response ceilings (a 4 MiB JSON body cannot carry a raw 4 MiB file).
const (
	// ToolWriteMaxBytes caps one file.write call (2 MiB decoded; base64
	// inflates ~1.34x into the claim request body).
	ToolWriteMaxBytes = 2 << 20
	// ToolReadMaxBytes caps one file.read page.
	ToolReadMaxBytes = 1 << 20
	// toolListMaxEntries caps one file.list page.
	toolListMaxEntries = 200
	// toolPathMaxBytes bounds the path argument.
	toolPathMaxBytes = 1024
)

// FileTool names, exported so wiring and docs cannot drift.
const (
	ToolStat   = "file.stat"
	ToolList   = "file.list"
	ToolRead   = "file.read"
	ToolWrite  = "file.write"
	ToolMkdir  = "file.mkdir"
	ToolRemove = "file.remove"
)

var (
	readOnlyAll = func(map[string]any) bool { return true }
	mutating    = func(map[string]any) bool { return false }
)

// FileEffects returns the delegated ToolEffect set for the core file tools.
// A bare state service cannot execute them; they are only claimable — and
// therefore only model-visible — once registered here.
func FileEffects(c *Client) map[string]agentstate.ToolEffect {
	fx := &fileEffects{c: c}
	return map[string]agentstate.ToolEffect{
		ToolStat:   {Apply: fx.stat, ReadOnly: readOnlyAll},
		ToolList:   {Apply: fx.list, ReadOnly: readOnlyAll},
		ToolRead:   {Apply: fx.read, ReadOnly: readOnlyAll},
		ToolWrite:  {Apply: fx.write, ReadOnly: mutating},
		ToolMkdir:  {Apply: fx.mkdir, ReadOnly: mutating},
		ToolRemove: {Apply: fx.remove, ReadOnly: mutating},
	}
}

type fileEffects struct{ c *Client }

func (fx *fileEffects) scope(personaID string) (string, error) {
	s, err := ScopeForPersona(personaID)
	if err != nil {
		return "", fmt.Errorf("fileaccess: %w", err)
	}
	return s, nil
}

func pathArg(request map[string]any) (string, error) {
	p, _ := request["path"].(string)
	if len(p) == 0 || len(p) > toolPathMaxBytes {
		return "", fmt.Errorf("%w: path must be a non-empty string of at most %d bytes", agentstate.ErrBadRequest, toolPathMaxBytes)
	}
	return p, nil
}

// effectErr classifies filesvc outcomes for the operation ledger: a
// deterministic refusal (bad path, not found, conflict, scope denial,
// oversize) is recorded as a failed operation so the turn sees the honest
// answer instead of retrying forever; transport errors and service-side
// 5xx/503 stay transient so a claim replay re-runs the effect.
func effectErr(err error) error {
	var se *ServiceError
	if errors.As(err, &se) && se.Status >= 400 && se.Status < 500 {
		return fmt.Errorf("%w: %s", agentstate.ErrBadRequest, se.Error())
	}
	return err
}

// expectVersion maps the tool's expect_version field to filesvc's
// If-Version token. "any" is refused: an unconditional overwrite cannot be
// replay-verified after a crash (the retry cannot tell its own landed write
// from an interloper's), so mutating tools always carry an exact predicate.
func expectVersion(request map[string]any, def string) (string, error) {
	v, ok := request["expect_version"]
	if !ok {
		return def, nil
	}
	switch t := v.(type) {
	case string:
		if t == "none" {
			return "none", nil
		}
		return "", fmt.Errorf("%w: expect_version must be \"none\" or a file version integer; unconditional \"any\" writes cannot be replay-verified", agentstate.ErrBadRequest)
	case float64:
		if t != float64(int64(t)) || t < 0 {
			return "", fmt.Errorf("%w: expect_version must be a non-negative integer", agentstate.ErrBadRequest)
		}
		return fmt.Sprintf("%d", int64(t)), nil
	}
	return "", fmt.Errorf("%w: expect_version must be \"none\" or a file version integer", agentstate.ErrBadRequest)
}

func (fx *fileEffects) stat(ctx context.Context, _ pgx.Tx, personaID, _ string, request map[string]any) (map[string]any, error) {
	scope, err := fx.scope(personaID)
	if err != nil {
		return nil, err
	}
	p, err := pathArg(request)
	if err != nil {
		return nil, err
	}
	st, err := fx.c.Stat(ctx, scope, p)
	if err != nil {
		return nil, effectErr(err)
	}
	return map[string]any{
		"kind":            st.Kind,
		"size":            st.Size,
		"mtime_ns":        st.MtimeNS,
		"version":         st.Version,
		"external_change": st.ExternalChange,
	}, nil
}

func (fx *fileEffects) list(ctx context.Context, _ pgx.Tx, personaID, _ string, request map[string]any) (map[string]any, error) {
	scope, err := fx.scope(personaID)
	if err != nil {
		return nil, err
	}
	p, _ := request["path"].(string)
	if len(p) > toolPathMaxBytes {
		return nil, fmt.Errorf("%w: path must be at most %d bytes", agentstate.ErrBadRequest, toolPathMaxBytes)
	}
	limit := 0
	if v, ok := request["limit"].(float64); ok && v > 0 {
		limit = int(v)
	}
	if limit > toolListMaxEntries {
		limit = toolListMaxEntries
	}
	cursor, _ := request["cursor"].(string)
	res, err := fx.c.List(ctx, scope, p, cursor, limit)
	if err != nil {
		return nil, effectErr(err)
	}
	return map[string]any{"entries": res.Entries, "next_cursor": res.NextCursor}, nil
}

func (fx *fileEffects) read(ctx context.Context, _ pgx.Tx, personaID, _ string, request map[string]any) (map[string]any, error) {
	scope, err := fx.scope(personaID)
	if err != nil {
		return nil, err
	}
	p, err := pathArg(request)
	if err != nil {
		return nil, err
	}
	var off, length int64
	if v, ok := request["offset"].(float64); ok {
		off = int64(v)
	}
	if v, ok := request["len"].(float64); ok {
		length = int64(v)
	}
	if off < 0 || length < 0 {
		return nil, fmt.Errorf("%w: offset and len must be non-negative", agentstate.ErrBadRequest)
	}
	if length == 0 || length > ToolReadMaxBytes {
		length = ToolReadMaxBytes
	}
	// Stat first: size bounds has_more honestly and the version pairs the
	// page with the same record write/read replies report.
	st, err := fx.c.Stat(ctx, scope, p)
	if err != nil {
		return nil, effectErr(err)
	}
	res, err := fx.c.Read(ctx, scope, p, off, length)
	if err != nil {
		return nil, effectErr(err)
	}
	out := map[string]any{
		"version":         res.Version,
		"size":            st.Size,
		"offset":          off,
		"has_more":        off+int64(len(res.Body)) < st.Size,
		"external_change": res.ExternalChange,
	}
	if utf8.Valid(res.Body) {
		out["content_text"] = string(res.Body)
	} else {
		out["content_base64"] = base64.StdEncoding.EncodeToString(res.Body)
	}
	return out, nil
}

// write applies the create/overwrite with an exact If-Version predicate and
// then reconciles a conflict by content: after a crash between the filesvc
// mutation and the operation-record commit, the retried call gets a 409 —
// at which point bytes-at-path equality is the evidence that this call's
// intended outcome already landed, and the live version becomes the
// receipt. Divergent content means a genuine conflict and records a failed
// operation rather than silently clobbering someone else's bytes.
func (fx *fileEffects) write(ctx context.Context, _ pgx.Tx, personaID, _ string, request map[string]any) (map[string]any, error) {
	scope, err := fx.scope(personaID)
	if err != nil {
		return nil, err
	}
	p, err := pathArg(request)
	if err != nil {
		return nil, err
	}
	var body []byte
	if s, ok := request["content_text"].(string); ok {
		body = []byte(s)
	} else if s, ok := request["content_base64"].(string); ok {
		if body, err = base64.StdEncoding.DecodeString(s); err != nil {
			return nil, fmt.Errorf("%w: content_base64 is not valid base64", agentstate.ErrBadRequest)
		}
	} else {
		return nil, fmt.Errorf("%w: file.write requires content_text or content_base64", agentstate.ErrBadRequest)
	}
	if len(body) > ToolWriteMaxBytes {
		return nil, fmt.Errorf("%w: file.write content exceeds %d bytes; write in pages or use smaller files", agentstate.ErrBadRequest, ToolWriteMaxBytes)
	}
	ifv, err := expectVersion(request, "none")
	if err != nil {
		return nil, err
	}
	ver, err := fx.c.Write(ctx, scope, p, ifv, body)
	if err == nil {
		return map[string]any{"version": ver, "written": true}, nil
	}
	var se *ServiceError
	if !errors.As(err, &se) || se.Status != 409 {
		return nil, effectErr(err)
	}
	// Conflict: decide whether this is a replay of an already-landed write.
	st, serr := fx.c.Stat(ctx, scope, p)
	if serr != nil {
		return nil, effectErr(fmt.Errorf("version_conflict and could not verify replay state: %v", serr))
	}
	if st.Kind != "file" || st.Size != int64(len(body)) {
		return nil, fmt.Errorf("%w: version_conflict at %s: path holds different content (kind=%s size=%d)", agentstate.ErrBadRequest, p, st.Kind, st.Size)
	}
	cur, rerr := fx.c.Read(ctx, scope, p, 0, int64(len(body)))
	if rerr != nil {
		return nil, fmt.Errorf("version_conflict and could not verify replay state: %v", rerr)
	}
	if !bytes.Equal(cur.Body, body) {
		return nil, fmt.Errorf("%w: version_conflict at %s: path holds different content", agentstate.ErrBadRequest, p)
	}
	return map[string]any{"version": cur.Version, "written": true, "replayed": true}, nil
}

func (fx *fileEffects) mkdir(ctx context.Context, _ pgx.Tx, personaID, _ string, request map[string]any) (map[string]any, error) {
	scope, err := fx.scope(personaID)
	if err != nil {
		return nil, err
	}
	p, err := pathArg(request)
	if err != nil {
		return nil, err
	}
	ver, err := fx.c.Mkdir(ctx, scope, p)
	if err != nil {
		return nil, effectErr(err)
	}
	return map[string]any{"version": ver}, nil
}

func (fx *fileEffects) remove(ctx context.Context, _ pgx.Tx, personaID, _ string, request map[string]any) (map[string]any, error) {
	scope, err := fx.scope(personaID)
	if err != nil {
		return nil, err
	}
	p, err := pathArg(request)
	if err != nil {
		return nil, err
	}
	// For removal the idempotent default is "any": the desired end-state is
	// absence, so a replay that finds the file already gone reports
	// already_absent instead of failing.
	ifv, err := expectVersion(request, "any")
	if err != nil {
		return nil, err
	}
	if ifv == "none" {
		return nil, fmt.Errorf("%w: expect_version \"none\" cannot remove a file; use a version integer for CAS or omit for unconditional remove", agentstate.ErrBadRequest)
	}
	err = fx.c.Remove(ctx, scope, p, ifv)
	if err == nil {
		return map[string]any{"removed": true}, nil
	}
	var se *ServiceError
	if errors.As(err, &se) && se.Status == 404 {
		return map[string]any{"removed": true, "already_absent": true}, nil
	}
	return nil, effectErr(err)
}
