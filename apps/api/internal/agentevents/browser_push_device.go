package agentevents

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"net/http"
	"time"
)

const (
	BrowserPushDeviceCookie  = "sumi_push_device"
	browserPushDeviceTTL     = 30 * 24 * time.Hour
	browserPushDeviceTimeout = 12 * time.Second
)

// BrowserPushDevices owns notification delivery permission, not HTTP login
// authority. Only an authenticated session exchange may establish a device;
// logging out must durably revoke it before its cookie is discarded.
type BrowserPushDevices interface {
	RefreshBrowserPushDevice(ctx context.Context, existingID, newID, humanID string, expiresAt time.Time) (string, error)
	RevokeBrowserPushDevice(ctx context.Context, deviceID string) error
}

// BrowserPushDeviceID returns only a hash of the opaque device cookie. It is a
// delivery/revocation handle and cannot authenticate an application request.
func BrowserPushDeviceID(r *http.Request) (string, error) {
	cookies := r.CookiesNamed(BrowserPushDeviceCookie)
	if len(cookies) != 1 {
		return "", errors.New("missing or ambiguous push device cookie")
	}
	return browserPushDeviceHash(cookies[0].Value)
}

func browserPushDeviceHash(value string) (string, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(decoded) != 32 || base64.RawURLEncoding.EncodeToString(decoded) != value {
		return "", errors.New("invalid push device cookie")
	}
	hash := sha256.Sum256(decoded)
	return base64.RawURLEncoding.EncodeToString(hash[:]), nil
}

func (s *BrowserAuthServer) refreshPushDevice(w http.ResponseWriter, r *http.Request, humanID string) error {
	if s.PushDevices == nil {
		return nil
	}
	existingID, _ := BrowserPushDeviceID(r)
	var token [32]byte
	if _, err := rand.Read(token[:]); err != nil {
		return err
	}
	value := base64.RawURLEncoding.EncodeToString(token[:])
	newID, _ := browserPushDeviceHash(value)
	ctx, cancel := context.WithTimeout(r.Context(), browserPushDeviceTimeout)
	defer cancel()
	id, err := s.PushDevices.RefreshBrowserPushDevice(ctx, existingID, newID, humanID, time.Now().Add(browserPushDeviceTTL))
	if err != nil {
		return err
	}
	if existingID != "" && id == existingID {
		value = r.CookiesNamed(BrowserPushDeviceCookie)[0].Value
	} else if id != newID {
		return errors.New("unexpected push device identity")
	}
	http.SetCookie(w, s.pushDeviceCookie(value, int(browserPushDeviceTTL/time.Second)))
	return nil
}

func (s *BrowserAuthServer) revokePushDevice(r *http.Request) error {
	if s.PushDevices == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(r.Context(), browserPushDeviceTimeout)
	defer cancel()
	// Revoke each valid cookie on logout even if a legacy/duplicate path cookie
	// is present. Invalid values convey no delivery authority.
	for _, cookie := range r.CookiesNamed(BrowserPushDeviceCookie) {
		id, err := browserPushDeviceHash(cookie.Value)
		if err == nil {
			if err := s.PushDevices.RevokeBrowserPushDevice(ctx, id); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *BrowserAuthServer) pushDeviceCookie(value string, maxAge int) *http.Cookie {
	cookie := &http.Cookie{
		Name: BrowserPushDeviceCookie, Value: value, Path: "/",
		HttpOnly: true, Secure: s.SecureCookies, SameSite: http.SameSiteLaxMode,
		MaxAge: maxAge,
	}
	if maxAge < 0 {
		cookie.Expires = time.Unix(1, 0)
	}
	return cookie
}
