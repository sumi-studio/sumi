package feedback

import (
	"encoding/json"
	"strings"
	"time"
)

// Diagnostics is a bounded, client-observed snapshot. It is evidence supplied by
// the author, never trusted authority or a substitute for server logs.
type Diagnostics struct {
	TabRelease       string             `json:"tab_release,omitempty"`
	Version          int                `json:"version"`
	CapturedAt       time.Time          `json:"captured_at"`
	Source           *DiagnosticSource  `json:"source,omitempty"`
	Browser          string             `json:"browser"`
	Language         string             `json:"language"`
	TimeZone         string             `json:"time_zone"`
	UTCOffsetMinutes int                `json:"utc_offset_minutes"`
	Online           bool               `json:"online"`
	Visibility       string             `json:"visibility"`
	Viewport         DiagnosticViewport `json:"viewport"`
	Assets           []string           `json:"assets"`
	ServedRelease    string             `json:"served_release,omitempty"`
}
type DiagnosticSource struct {
	Path        string            `json:"path"`
	WorkspaceID string            `json:"workspace_id,omitempty"`
	CapturedAt  time.Time         `json:"captured_at"`
	States      map[string]string `json:"states"`
}
type DiagnosticViewport struct {
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
	if err != nil || len(b) > 12000 || d.Version != 1 || d.CapturedAt.IsZero() || len(d.Browser) > 1024 || len(d.Language) > 64 || len(d.TimeZone) > 128 || len(d.Visibility) > 32 || d.UTCOffsetMinutes < -1440 || d.UTCOffsetMinutes > 1440 || len(d.ServedRelease) > 64 || len(d.TabRelease) > 64 || len(d.Assets) > 8 {
		return false
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
