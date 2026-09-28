package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
)

// When Firebase does not prove the invited address, an email-bound invitation
// asks GitHub itself. The browser hands over the OAuth access token Firebase's
// redirect result exposed; it is used for bounded reads against
// the fixed GitHub API origin and is never stored, logged or forwarded.
const (
	githubAPIOrigin         = "https://api.github.com"
	githubEmailProofTimeout = 8 * time.Second
	githubMaxResponseBytes  = 64 * 1024
	githubMaxEmailPages     = 10
)

type githubEmailEvidence int

const (
	// githubEmailAbsent: the proven GitHub account does not list the address.
	githubEmailAbsent githubEmailEvidence = iota
	// githubEmailUnverified: listed, but GitHub has not verified it.
	githubEmailUnverified
	// githubEmailVerified: GitHub verified the address for this account.
	githubEmailVerified
)

var (
	// errGitHubSubjectMismatch is a token for a different GitHub account than
	// the one Firebase verified. It is never evidence for this sign-in.
	errGitHubSubjectMismatch = errors.New("GitHub token belongs to another account")
	// errGitHubProofRejected is a token GitHub refused, or one issued without
	// the user:email scope. Signing in with GitHub again obtains a fresh one.
	errGitHubProofRejected = errors.New("GitHub refused the email proof token")
	// errGitHubUnavailable is an outage, rate limit, timeout or malformed
	// answer: nothing was learned and the same attempt may succeed later.
	errGitHubUnavailable = errors.New("GitHub email proof is unavailable")
)

type githubEmailProver interface {
	// EmailEvidence reports what GitHub says about email for the account the
	// token belongs to, after proving that account is githubUserID.
	EmailEvidence(ctx context.Context, accessToken, githubUserID, email string) (githubEmailEvidence, error)
}

type githubAPIEmailProver struct {
	origin string
	client *http.Client
}

func newGitHubAPIEmailProver() *githubAPIEmailProver {
	return &githubAPIEmailProver{origin: githubAPIOrigin, client: &http.Client{
		Timeout: githubEmailProofTimeout,
		// A redirect is never followed: the bearer token stays on the one
		// origin it was sent to.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
}

func (p *githubAPIEmailProver) EmailEvidence(ctx context.Context, accessToken, githubUserID, email string) (githubEmailEvidence, error) {
	want, err := strconv.ParseInt(githubUserID, 10, 64)
	if err != nil || want <= 0 || strconv.FormatInt(want, 10) != githubUserID {
		return githubEmailAbsent, errGitHubSubjectMismatch
	}
	ctx, cancel := context.WithTimeout(ctx, githubEmailProofTimeout)
	defer cancel()
	var user struct {
		ID int64 `json:"id"`
	}
	if err := p.get(ctx, "/user", accessToken, &user); err != nil {
		return githubEmailAbsent, err
	}
	if user.ID != want {
		return githubEmailAbsent, errGitHubSubjectMismatch
	}
	for page := 1; page <= githubMaxEmailPages; page++ {
		var emails []struct {
			Email    string `json:"email"`
			Verified bool   `json:"verified"`
		}
		// Never follow a provider-supplied pagination URL with the credential.
		path := "/user/emails?per_page=100&page=" + strconv.Itoa(page)
		if err := p.get(ctx, path, accessToken, &emails); err != nil {
			return githubEmailAbsent, err
		}
		for _, entry := range emails {
			normalized, err := koseki.NormalizeEmail(entry.Email)
			if err != nil || normalized != email {
				continue
			}
			if entry.Verified {
				return githubEmailVerified, nil
			}
			return githubEmailUnverified, nil
		}
		if len(emails) < 100 {
			return githubEmailAbsent, nil
		}
	}
	// A truncated search is not evidence that the address is absent.
	return githubEmailAbsent, errGitHubUnavailable
}

func (p *githubAPIEmailProver) get(ctx context.Context, path, accessToken string, target any) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, p.origin+path, nil)
	if err != nil {
		return errGitHubUnavailable
	}
	request.Header.Set("Accept", "application/vnd.github+json")
	request.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	request.Header.Set("User-Agent", "sumi-auth")
	request.Header.Set("Authorization", "Bearer "+accessToken)
	response, err := p.client.Do(request)
	if err != nil {
		// The transport error names the URL only; it carries no header.
		return fmt.Errorf("%w: %v", errGitHubUnavailable, err)
	}
	defer response.Body.Close()
	switch {
	case response.StatusCode == http.StatusOK:
	case response.StatusCode == http.StatusForbidden &&
		(response.Header.Get("X-RateLimit-Remaining") == "0" || response.Header.Get("Retry-After") != ""):
		return errGitHubUnavailable
	case response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden ||
		response.StatusCode == http.StatusNotFound:
		return errGitHubProofRejected
	default:
		return fmt.Errorf("%w: status %d", errGitHubUnavailable, response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, githubMaxResponseBytes+1))
	if err != nil || len(body) > githubMaxResponseBytes || json.Unmarshal(body, target) != nil {
		return errGitHubUnavailable
	}
	return nil
}
