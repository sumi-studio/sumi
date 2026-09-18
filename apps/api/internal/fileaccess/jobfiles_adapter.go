package fileaccess

// Adapter from *Client to agentstate.JobFileService — the job-scoped file
// capability's upstream port. The state service defines the interface; this
// file maps the concrete client's typed calls and ServiceError onto it.
// All authority/ledger logic lives in agentstate; this file only moves
// bytes and classifies upstream answers.

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"net/url"
	"strconv"

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
)

// JobFileService adapts c for the job file capability routes.
func JobFileService(c *Client) agentstate.JobFileService {
	return jobFileService{c}
}

type jobFileService struct{ c *Client }

func (a jobFileService) ScopeForPersona(personaID string) (string, error) {
	return ScopeForPersona(personaID)
}

func svcError(err error) error {
	var se *ServiceError
	if errors.As(err, &se) {
		return &agentstate.JobFileSvcError{Status: se.Status, Code: se.Code, Message: se.Message}
	}
	return err
}

func (a jobFileService) ReadOp(ctx context.Context, op, scope, path string, q url.Values) (map[string]any, error) {
	switch op {
	case "stat":
		st, err := a.c.Stat(ctx, scope, path)
		if err != nil {
			return nil, svcError(err)
		}
		return map[string]any{
			"kind": st.Kind, "size": st.Size, "mtime_ns": st.MtimeNS,
			"version": st.Version, "fingerprint": st.Fingerprint,
			"external_change": st.ExternalChange,
		}, nil
	case "list":
		cursor := q.Get("cursor")
		limit, _ := strconv.Atoi(q.Get("limit"))
		res, err := a.c.List(ctx, scope, path, cursor, limit)
		if err != nil {
			return nil, svcError(err)
		}
		return map[string]any{"entries": res.Entries, "next_cursor": res.NextCursor}, nil
	case "read":
		offset, _ := strconv.ParseInt(q.Get("offset"), 10, 64)
		length, _ := strconv.ParseInt(q.Get("len"), 10, 64)
		res, err := a.c.Read(ctx, scope, path, offset, length)
		if err != nil {
			return nil, svcError(err)
		}
		return map[string]any{
			"data_base64":     base64.StdEncoding.EncodeToString(res.Body),
			"version":         res.Version,
			"external_change": res.ExternalChange,
		}, nil
	}
	return nil, &agentstate.JobFileSvcError{Status: http.StatusBadRequest, Code: "bad_op", Message: "unknown read op " + op}
}

func (a jobFileService) MutateOp(ctx context.Context, op, scope, path, ifVersion, opID string, body []byte) (int64, bool, error) {
	var version int64
	var replayed bool
	var err error
	switch op {
	case "write":
		version, replayed, err = a.c.WriteKeyed(ctx, scope, path, ifVersion, opID, body)
	case "mkdir":
		version, replayed, err = a.c.MkdirKeyed(ctx, scope, path, opID)
	case "remove":
		replayed, err = a.c.RemoveKeyed(ctx, scope, path, ifVersion, opID)
	default:
		return 0, false, &agentstate.JobFileSvcError{Status: http.StatusBadRequest, Code: "bad_op", Message: "unknown mutating op " + op}
	}
	return version, replayed, svcError(err)
}
