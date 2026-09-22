package runtimeprovision

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
)

// journalEach decodes complete JSON-file records in data and calls fn
// with the byte offset just past each record and its decoded payload.
// A torn trailing record is not delivered.
func journalEach(data []byte, fn func(end int64, payload []byte) bool) {
	base := int64(0)
	for len(data) > 0 {
		i := bytes.IndexByte(data, '\n')
		if i < 0 {
			return
		}
		line := data[:i]
		end := base + int64(i) + 1
		var e journalEntry
		if json.Unmarshal(bytes.TrimSpace(line), &e) == nil && e.Log != "" {
			if !fn(end, []byte(e.Log)) {
				return
			}
		}
		base = end
		data = data[i+1:]
	}
}

// recoverSource returns the journal position the pump should resume
// from, certified by position rather than content search. Payload
// equality cannot identify a source record — terminals routinely emit
// identical lines (prompts, repeated log entries), so a substring match
// can pick the wrong occurrence and silently replay. Instead the
// scrollback's absolute emitted window [base, total) is compared
// byte-for-byte against the journal's decoded payload stream at the
// same absolute offsets. If the retained bytes are exactly the decoded
// stream's prefix-window, the true resume position is the journal end
// of the record whose payload ends at `total` — recovered regardless of
// what the persisted cursor claims, which absorbs a crash between
// scrollback append and cursor commit, a lost/malformed cursor, and a
// cursor that drifted ahead of what was captured. Anything that cannot
// be certified — divergent bytes, a scrollback ending mid-record, a
// journal shorter than the scrollback — records an explicit gap and the
// committed cursor stands.
func recoverSource(tty *ttyLog, f io.ReaderAt, ino uint64, size int64, pos ttySrc) ttySrc {
	pos.Ino = ino
	retained, base, total, _, err := tty.read(0, math.MaxInt)
	if err != nil {
		return pos
	}
	if len(retained) == 0 {
		// Nothing to verify against. An entirely empty scrollback means
		// any committed coverage was lost with it: resume at 0 and let
		// the journal re-capture — replay into an empty log duplicates
		// nothing. A fully compacted scrollback keeps the committed
		// cursor as its only evidence.
		if total == 0 {
			pos.Off = 0
		}
		return pos
	}
	matched := int64(0) // absolute decoded-stream offset consumed
	recovered := int64(-1)
	diverged := false
	// Decode the journal only up to the scrollback's absolute end; the
	// early exit bounds the scan for large journals.
	journalEach(journalRegion(f, 0, size), func(end int64, payload []byte) bool {
		pStart := matched
		matched += int64(len(payload))
		lo, hi := pStart, matched
		if lo < base {
			lo = base
		}
		if hi > total {
			hi = total
		}
		if lo < hi {
			want := payload[lo-pStart : hi-pStart]
			got := retained[lo-base : hi-base]
			if !bytes.Equal(want, got) {
				diverged = true
				return false
			}
		}
		if matched == total {
			recovered = end
		}
		return matched < total
	})
	if diverged || matched < total || recovered < 0 {
		// Retained bytes are not a clean window of this journal's decoded
		// stream: divergent content, a journal shorter than the
		// scrollback, or a scrollback ending mid-record. The committed
		// cursor stands; the gap marks the uncertified resume.
		_ = tty.recordGap("retained output could not be certified against the journal; a bounded window may be duplicated or lost")
		return pos
	}
	pos.Off = recovered
	_ = tty.saveSrc(pos)
	return pos
}

// journalRegion reads the journal window [from, to), bounded by the
// daemon's JSON journal size cap.
func journalRegion(f io.ReaderAt, from, to int64) []byte {
	if to <= from {
		return nil
	}
	buf := make([]byte, to-from)
	n, err := f.ReadAt(buf, from)
	if err != nil && err != io.EOF {
		return nil
	}
	return buf[:n]
}
