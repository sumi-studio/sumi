package feedback

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/sumi-studio/sumi/apps/api/internal/participant"
)

// Diagnostics is a bounded, client-observed snapshot. It is evidence supplied by
// the author, never trusted authority or a substitute for server logs.
type Diagnostics struct {
	ServerObservation *ServerObservation   `json:"server_observation,omitempty"`
	ClientEvents      []ClientEvent        `json:"client_events,omitempty"`
	Selection         *DiagnosticSelection `json:"selection,omitempty"`
	TabRelease        string               `json:"tab_release,omitempty"`
	Version           int                  `json:"version"`
	CapturedAt        time.Time            `json:"captured_at"`
	Source            *DiagnosticSource    `json:"source,omitempty"`
	Browser           string               `json:"browser"`
	Language          string               `json:"language"`
	TimeZone          string               `json:"time_zone"`
	UTCOffsetMinutes  int                  `json:"utc_offset_minutes"`
	Online            bool                 `json:"online"`
	Visibility        string               `json:"visibility"`
	Viewport          DiagnosticViewport   `json:"viewport"`
	Assets            []string             `json:"assets"`
	ServedRelease     string               `json:"served_release,omitempty"`
}
type DiagnosticSource struct {
	Path        string            `json:"path"`
	WorkspaceID string            `json:"workspace_id,omitempty"`
	CapturedAt  time.Time         `json:"captured_at"`
	States      map[string]string `json:"states"`
}
type DiagnosticViewport struct {
	ScrollX    float64 `json:"scroll_x,omitempty"`
	ScrollY    float64 `json:"scroll_y,omitempty"`
	Width      int     `json:"width"`
	Height     int     `json:"height"`
	Scale      float64 `json:"scale"`
	PixelRatio float64 `json:"pixel_ratio"`
}

func (d *Diagnostics) valid() bool {
	if d == nil {
		return true
	}
	b, err := json.Marshal(d)
	if err != nil || len(b) > 32<<10 || d.Version != 1 || d.CapturedAt.IsZero() || len(d.Browser) > 1024 || len(d.Language) > 64 || len(d.TimeZone) > 128 || len(d.Visibility) > 32 || d.UTCOffsetMinutes < -1440 || d.UTCOffsetMinutes > 1440 || len(d.ServedRelease) > 64 || len(d.TabRelease) > 64 || len(d.Assets) > 8 {
		return false
	}
	if math.Abs(d.Viewport.ScrollX) > 1e8 || math.Abs(d.Viewport.ScrollY) > 1e8 || len(d.ClientEvents) > 24 {
		return false
	}
	for _, event := range d.ClientEvents {
		if event.At.IsZero() || !diagnosticPath(event.Path) || utf8.RuneCountInString(event.Summary) > 240 {
			return false
		}
		switch event.Kind {
		case "navigation", "click", "error", "unhandledrejection":
		default:
			return false
		}
	}
	if selection := d.Selection; selection != nil {
		if selection.CapturedAt.IsZero() || len(selection.Tag) > 64 || len(selection.Selector) > 512 || utf8.RuneCountInString(selection.Label) > 200 || math.Abs(selection.Rect.X) > 1e8 || math.Abs(selection.Rect.Y) > 1e8 || selection.Rect.Width < 0 || selection.Rect.Width > 1e8 || selection.Rect.Height < 0 || selection.Rect.Height > 1e8 {
			return false
		}
	}
	if observation := d.ServerObservation; observation != nil {
		if observation.CapturedAt.IsZero() || (observation.PersonalityAgentID != "" && !validID(observation.PersonalityAgentID, 7)) || (observation.Status != "available" && observation.Status != "unavailable") {
			return false
		}
		if observation.Generation != "" {
			value, err := strconv.ParseUint(observation.Generation, 10, 63)
			if err != nil || strconv.FormatUint(value, 10) != observation.Generation {
				return false
			}
		}
		switch observation.ReadinessReason {
		case "", "unknown", "ready", "rehydrating", "stopped":
		default:
			return false
		}
	}
	if d.Viewport.Width < 0 || d.Viewport.Width > 100000 || d.Viewport.Height < 0 || d.Viewport.Height > 100000 || d.Viewport.Scale <= 0 || d.Viewport.Scale > 100 || d.Viewport.PixelRatio <= 0 || d.Viewport.PixelRatio > 100 {
		return false
	}
	for _, asset := range d.Assets {
		if len(asset) > 256 || !strings.HasPrefix(asset, "/assets/") || strings.ContainsAny(asset, "?#") {
			return false
		}
	}
	if s := d.Source; s != nil {
		if len(s.Path) > 512 || !strings.HasPrefix(s.Path, "/") || strings.ContainsAny(s.Path, "?#") || len(s.WorkspaceID) > 64 || s.CapturedAt.IsZero() || len(s.States) > 24 {
			return false
		}
		for key, value := range s.States {
			if len(key) > 64 || len(value) > 128 {
				return false
			}
		}
	}
	return true
}

func diagnosticPath(value string) bool {
	return len(value) <= 512 && strings.HasPrefix(value, "/") && !strings.ContainsAny(value, "?#")
}

type ClientEvent struct {
	At      time.Time `json:"at"`
	Kind    string    `json:"kind"`
	Path    string    `json:"path"`
	Summary string    `json:"summary"`
}
type DiagnosticSelection struct {
	Tag        string         `json:"tag"`
	Selector   string         `json:"selector"`
	Label      string         `json:"label"`
	Rect       DiagnosticRect `json:"rect"`
	CapturedAt time.Time      `json:"captured_at"`
}
type DiagnosticRect struct {
	X      float64 `json:"x"`
	Y      float64 `json:"y"`
	Width  float64 `json:"width"`
	Height float64 `json:"height"`
}

// ServerObservation is an allowlisted server response that the client may
// include in its report. Once returned by the client it is author-supplied
// evidence, never trusted server provenance or a permission check.
type ServerObservation struct {
	CapturedAt         time.Time `json:"captured_at"`
	PersonalityAgentID string    `json:"personality_agent_id,omitempty"`
	Status             string    `json:"status"`
	Generation         string    `json:"generation,omitempty"`
	Ready              *bool     `json:"ready,omitempty"`
	ReadinessReason    string    `json:"readiness_reason,omitempty"`
	RunInFlight        *bool     `json:"run_in_flight,omitempty"`
}

func (s *Server) captureDiagnostics(r *http.Request, actor participant.Ref, personalityAgentID string) (any, error) {
	if err := decode(r, &struct{}{}); err != nil {
		return nil, err
	}
	tx, err := s.Store.begin(r.Context(), actor)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(r.Context())
	result := ServerObservation{CapturedAt: time.Now().UTC(), PersonalityAgentID: personalityAgentID, Status: "unavailable"}
	if s.Gateway != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		observation, err := s.Gateway.DiagnosticObservation(ctx, personalityAgentID)
		if err == nil {
			result.Status = "available"
			result.Generation = observation.Generation
			result.Ready = &observation.Ready
			result.ReadinessReason = observation.ReadinessReason
			result.RunInFlight = observation.RunInFlight
		}
	}
	return result, tx.Commit(r.Context())
}
