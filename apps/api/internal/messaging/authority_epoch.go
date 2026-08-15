package messaging

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
)

var errInvalidAuthorityEpoch = errors.New("invalid authority epoch")

// parseCanonicalAuthorityEpoch accepts only the canonical positive signed-int64
// decimal wire form. In particular, sign prefixes, leading zeroes, zero, and
// overflow are not alternative spellings of an epoch.
func parseCanonicalAuthorityEpoch(raw string) (int64, bool) {
	if raw == "" || raw[0] == '0' {
		return 0, false
	}
	for _, char := range raw {
		if char < '0' || char > '9' {
			return 0, false
		}
	}
	epoch, err := strconv.ParseInt(raw, 10, 64)
	return epoch, err == nil && epoch > 0
}

func exactAuthorityEpochQuery(r *http.Request) (int64, bool) {
	raw, ok := exactQueryValue(r, "authority_epoch")
	if !ok {
		return 0, false
	}
	return parseCanonicalAuthorityEpoch(raw)
}

// localAuthorityEpoch preserves the canonical decimal JSON wire while making
// omission and duplication observable. The zero value means omitted; every
// successfully decoded value is positive, so a second UnmarshalJSON call is a
// duplicate object key and fails closed.
type localAuthorityEpoch int64

func (e *localAuthorityEpoch) UnmarshalJSON(data []byte) error {
	if *e != 0 {
		return errInvalidAuthorityEpoch
	}
	var raw string
	if err := json.Unmarshal(data, &raw); err != nil {
		return errInvalidAuthorityEpoch
	}
	epoch, ok := parseCanonicalAuthorityEpoch(raw)
	if !ok {
		return errInvalidAuthorityEpoch
	}
	*e = localAuthorityEpoch(epoch)
	return nil
}
