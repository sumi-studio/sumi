//go:build !linux

package localterminal

import "errors"

func New(Config) (ProcessBackend, error) {
	return nil, errors.New("Local shared terminals currently require Linux or WSL")
}
