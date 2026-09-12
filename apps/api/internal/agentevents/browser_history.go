package agentevents

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"syscall"
)

// Browser history is a read projection of the durable event log. It never
// changes the secretary's transcript or model context. The offset cache retains
// no message bodies and decodes only newly appended records after its first scan.
// Prefix CRC verification matches durable runtime recovery: it still reads old
// file bytes, because same-inode rewrites can preserve coarse filesystem times.
type browserHistoryIndex struct {
	device, inode  uint64
	size           int64
	crc            uint32
	refs           []historyEventRef
	messages       []int
	ticks          []browserHistoryTick
	activeRun      uint64
	tools          map[string]uint64
	approvals      map[string]uint64
	approvalStarts map[string]uint64
	dispositions   map[string]uint64
}
type historyEventRef struct {
	offset                   int64
	length                   int
	run                      uint64
	toolStart, approvalStart uint64
	kind                     string
}
type browserHistoryTick struct {
	ID      string `json:"id"`
	Seq     uint64 `json:"seq"`
	Title   string `json:"title"`
	Preview string `json:"preview,omitempty"`
}
type browserHistoryPage struct {
	Events              []browserEventEnvelope `json:"events"`
	Context             []browserEventEnvelope `json:"context"`
	LatestSeq           uint64                 `json:"latest_seq"`
	BeforeSeq           *uint64                `json:"before_seq"`
	HasMore             bool                   `json:"has_more"`
	Index               []browserHistoryTick   `json:"index"`
	PendingApprovals    []browserEventEnvelope `json:"pending_approvals"`
	ActiveRun           *browserEventEnvelope  `json:"active_run"`
	CommandDispositions []browserEventEnvelope `json:"command_dispositions"`
}
type browserHistoryQuery struct {
	limit     int
	before    uint64
	around    string
	commands  []string
	omitIndex bool
}

var errHistoryQuery = errors.New("invalid history query")
var errHistoryTarget = errors.New("history target not found")

func historyQuery(r *http.Request) (browserHistoryQuery, error) {
	q := browserHistoryQuery{limit: 100}
	for key, values := range r.URL.Query() {
		switch key {
		case "installation_id", "authority_epoch":
			continue
		case "command_id":
			if len(values) > 100 {
				return q, errHistoryQuery
			}
			seen := map[string]bool{}
			for _, id := range values {
				if !canonicalUUIDRegexp.MatchString(id) || seen[id] {
					return q, errHistoryQuery
				}
				seen[id] = true
				q.commands = append(q.commands, id)
			}
			continue
		case "limit", "before_seq", "around_message_id", "include_index":
		default:
			return q, errHistoryQuery
		}
		if len(values) != 1 || values[0] == "" {
			return q, errHistoryQuery
		}
		switch key {
		case "include_index":
			if values[0] != "true" && values[0] != "false" {
				return q, errHistoryQuery
			}
			q.omitIndex = values[0] == "false"
		case "limit":
			n, e := strconv.Atoi(values[0])
			if e != nil || n < 1 || n > 100 {
				return q, errHistoryQuery
			}
			q.limit = n
		case "before_seq":
			n, e := strconv.ParseUint(values[0], 10, 64)
			if e != nil || n == 0 || n > maxJSONSafeInteger {
				return q, errHistoryQuery
			}
			q.before = n
		case "around_message_id":
			if len(values[0]) > 128 {
				return q, errHistoryQuery
			}
			q.around = values[0]
		}
	}
	if q.before != 0 && q.around != "" {
		return q, errHistoryQuery
	}
	return q, nil
}

// ServeHistory shares the exact session, Employer and AppInstallation boundary
// with the socket. Reading history does not start or stop an agent runtime.
func (s *BrowserServer) ServeHistory(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if s.Sessions == nil || s.Events == nil || s.Authorizer == nil || s.LifecycleFence == nil {
		http.Error(w, "history unavailable", 503)
		return
	}
	if len(r.Header.Values("Origin")) > 0 && !s.checkOrigin(r) {
		http.Error(w, "origin not allowed", 403)
		return
	}
	cookie, err := uniqueBrowserSessionCookie(r)
	if err != nil {
		http.Error(w, "invalid session", 401)
		return
	}
	claims, err := s.Sessions.VerifySession(r.Context(), cookie.Value)
	if err != nil {
		http.Error(w, "invalid session", 401)
		return
	}
	scope, err := directChatScopeFromRequest(r)
	if err != nil {
		writeDirectChatInvalidScope(w)
		return
	}
	query, err := historyQuery(r)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	authorized := false
	err = s.authorizeBrowserOperation(r.Context(), claims, scope, func() error {
		authorized = true
		page, e := s.Events.browserHistory(r.Context(), claims.PersonalityAgentID, query)
		if e != nil {
			return e
		}
		w.Header().Set("Content-Type", "application/json")
		return json.NewEncoder(w).Encode(page)
	})
	if err != nil {
		if errors.Is(err, errHistoryTarget) {
			http.Error(w, "history target not found", 404)
		} else if errors.Is(err, errHistoryQuery) {
			http.Error(w, "invalid history cursor", 400)
		} else if errors.Is(err, ErrDirectChatAuthorizationUnavailable) {
			http.Error(w, "authorization unavailable", 503)
		} else if authorized {
			http.Error(w, "history unavailable", 500)
		} else {
			http.Error(w, "not authorized", 403)
		}
	}
}

func (g *DurableGateway) browserHistory(ctx context.Context, paid string, q browserHistoryQuery) (browserHistoryPage, error) {
	page := browserHistoryPage{Events: []browserEventEnvelope{}, Context: []browserEventEnvelope{}, Index: []browserHistoryTick{}, PendingApprovals: []browserEventEnvelope{}, CommandDispositions: []browserEventEnvelope{}}
	if err := ValidatePersonalityAgentID(paid); err != nil {
		return page, err
	}
	if q.limit < 1 || q.limit > 100 {
		return page, errHistoryQuery
	}
	file, err := g.newFile(g.eventPath(paid), os.O_RDONLY, 0600)
	if os.IsNotExist(err) {
		return page, nil
	}
	if err != nil {
		return page, err
	}
	defer file.Close()
	if err = flockContext(ctx, file.Fd(), syscall.LOCK_SH); err != nil {
		return page, err
	}
	defer func() { _ = unlockDurableFile(file) }()
	var stat syscall.Stat_t
	if err = syscall.Fstat(int(file.Fd()), &stat); err != nil {
		return page, err
	}
	g.historyMu.Lock()
	defer g.historyMu.Unlock()
	if g.history == nil {
		g.history = make(map[string]*browserHistoryIndex)
	}
	index := g.history[paid]
	if index != nil && index.device == uint64(stat.Dev) && index.inode == stat.Ino && index.size <= stat.Size {
		crc, e := crc32OfFilePrefix(file, index.size)
		if e != nil {
			return page, e
		}
		if crc != index.crc {
			index = nil
		}
	}
	if index == nil || index.device != uint64(stat.Dev) || index.inode != stat.Ino || index.size > stat.Size {
		index = &browserHistoryIndex{device: uint64(stat.Dev), inode: stat.Ino, tools: map[string]uint64{}, approvals: map[string]uint64{}, approvalStarts: map[string]uint64{}, dispositions: map[string]uint64{}}
		// Bound the number of independently retained PA indexes; entries are cheap
		// to reconstruct and never authorize access on their own.
		if len(g.history) >= 16 {
			for key := range g.history {
				delete(g.history, key)
				break
			}
		}
		g.history[paid] = index
	}
	if _, err = file.Seek(index.size, io.SeekStart); err != nil {
		return page, err
	}
	reader := bufio.NewReader(file)
	for index.size < stat.Size {
		if err = ctx.Err(); err != nil {
			return page, err
		}
		line, e := reader.ReadBytes('\n')
		if e != nil {
			return page, fmt.Errorf("read history index: %w", e)
		}
		var record durableEventRecord
		if e = json.Unmarshal(bytes.TrimSpace(line), &record); e != nil {
			return page, e
		}
		seq := uint64(len(index.refs) + 1)
		if record.Seq != seq || record.Event.Seq == nil || *record.Event.Seq != seq || record.Event.PersonalityAgentID != paid {
			return page, errors.New("history record identity or sequence mismatch")
		}
		if e = index.append(record.Event, index.size, len(line)); e != nil {
			return page, e
		}
		index.crc = updateCRC(index.crc, line)
		index.size += int64(len(line))
	}
	page.LatestSeq = uint64(len(index.refs))
	if !q.omitIndex {
		page.Index = append(page.Index, index.ticks...)
	}
	if q.before > page.LatestSeq+1 {
		return page, errHistoryQuery
	}
	end := len(index.messages)
	if q.before != 0 {
		end = sort.Search(len(index.messages), func(i int) bool { return uint64(index.messages[i]+1) >= q.before })
	}
	start := max(0, end-q.limit)
	if q.around != "" {
		found := false
		for _, tick := range index.ticks {
			if tick.ID == q.around {
				at := sort.SearchInts(index.messages, int(tick.Seq-1))
				// End at the target so event/byte bounds can never evict it
				// while retaining a later, exceptionally busy exchange.
				end = at + 1
				start = max(0, end-q.limit)
				found = true
				break
			}
		}
		if !found {
			return page, errHistoryTarget
		}
	}
	first, last := 0, len(index.refs)
	if start > 0 {
		first = index.messages[start-1] + 1
	}
	if q.before != 0 {
		// The cursor is an event boundary, including tool-only spans between
		// messages. Rounding down to a message would silently skip those spans.
		last = int(q.before - 1)
	}
	if q.around != "" {
		last = index.messages[end-1] + 1
	}
	// One busy exchange may contain many tools. Bound the event window separately
	// from the 100 user/assistant messages, without expanding to a whole run.
	first = max(first, last-2000)
	var pageBytes int64
	for i := last - 1; i >= first; i-- {
		pageBytes += int64(index.refs[i].length)
		if pageBytes > 8<<20 && i < last-1 {
			first = i + 1
			break
		}
	}
	windowDependencies := func(begin int) (map[uint64]bool, int64) {
		deps := map[uint64]bool{}
		var total int64
		if index.activeRun > 0 {
			total += int64(index.refs[index.activeRun-1].length)
		}
		for _, seq := range index.approvals {
			total += int64(index.refs[seq-1].length)
		}
		for i := begin; i < last; i++ {
			ref := index.refs[i]
			total += int64(ref.length)
			for _, seq := range []uint64{ref.run, ref.toolStart, ref.approvalStart} {
				if seq > 0 && seq <= uint64(begin) {
					deps[seq] = true
				}
			}
		}
		for seq := range deps {
			if run := index.refs[seq-1].run; run > 0 && run <= uint64(begin) {
				deps[run] = true
			}
		}
		for seq := range deps {
			total += int64(index.refs[seq-1].length)
		}
		return deps, total
	}
	dependencies, total := windowDependencies(first)
	for total > 8<<20 && first < last-1 {
		first++
		dependencies, total = windowDependencies(first)
	}
	read := func(seq uint64) (browserEventEnvelope, error) {
		ref := index.refs[seq-1]
		if _, e := file.Seek(ref.offset, io.SeekStart); e != nil {
			return browserEventEnvelope{}, e
		}
		raw := make([]byte, ref.length)
		if _, e := io.ReadFull(file, raw); e != nil {
			return browserEventEnvelope{}, e
		}
		var record durableEventRecord
		if e := json.Unmarshal(raw, &record); e != nil {
			return browserEventEnvelope{}, e
		}
		if record.Seq != seq || record.Event.Seq == nil || *record.Event.Seq != seq || record.Event.PersonalityAgentID != paid {
			return browserEventEnvelope{}, errors.New("history offset identity mismatch")
		}
		return projectBrowserEvent(record.Event)
	}
	depSeq := make([]uint64, 0, len(dependencies))
	for seq := range dependencies {
		depSeq = append(depSeq, seq)
	}
	sort.Slice(depSeq, func(i, j int) bool { return depSeq[i] < depSeq[j] })
	for _, seq := range depSeq {
		event, e := read(seq)
		if e != nil {
			return page, e
		}
		page.Context = append(page.Context, event)
	}
	for i := first; i < last; i++ {
		event, e := read(uint64(i + 1))
		if e != nil {
			return page, e
		}
		page.Events = append(page.Events, event)
	}
	page.HasMore = first > 0
	if first > 0 {
		cursor := uint64(first + 1)
		page.BeforeSeq = &cursor
	}
	if index.activeRun > 0 {
		event, e := read(index.activeRun)
		if e != nil {
			return page, e
		}
		page.ActiveRun = &event
	}
	pending := make([]uint64, 0, len(index.approvals))
	for _, seq := range index.approvals {
		pending = append(pending, seq)
	}
	sort.Slice(pending, func(i, j int) bool { return pending[i] < pending[j] })
	for _, seq := range pending {
		event, e := read(seq)
		if e != nil {
			return page, e
		}
		page.PendingApprovals = append(page.PendingApprovals, event)
	}
	for _, id := range q.commands {
		if seq := index.dispositions[id]; seq > 0 {
			event, e := read(seq)
			if e != nil {
				return page, e
			}
			page.CommandDispositions = append(page.CommandDispositions, event)
		}
	}
	sort.Slice(page.CommandDispositions, func(i, j int) bool { return *page.CommandDispositions[i].Seq < *page.CommandDispositions[j].Seq })
	return page, nil
}

func (index *browserHistoryIndex) append(envelope Envelope, offset int64, length int) error {
	var event struct {
		Type       string `json:"type"`
		MessageID  string `json:"message_id"`
		ToolCallID string `json:"tool_call_id"`
		RequestID  string `json:"request_id"`
		CommandID  string `json:"command_id"`
		Request    struct {
			ID string `json:"id"`
		} `json:"request"`
		Message struct {
			Role       string `json:"role"`
			ToolCallID string `json:"tool_call_id"`
			Content    []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
			IncomingSource struct {
				Source struct {
					Surface string `json:"surface"`
				} `json:"source"`
			} `json:"incoming_source"`
		} `json:"message"`
	}
	if err := json.Unmarshal(envelope.Event, &event); err != nil {
		return err
	}
	seq := *envelope.Seq
	if event.ToolCallID == "" {
		event.ToolCallID = event.Message.ToolCallID
	}
	switch event.Type {
	case "command_disposition":
		index.dispositions[event.CommandID] = seq
	case "agent_start":
		index.activeRun = seq
	case "tool_execution_start":
		index.tools[event.ToolCallID] = seq
	case "approval_requested":
		index.approvals[event.Request.ID] = seq
		index.approvalStarts[event.Request.ID] = seq
	case "approval_resolved":
		delete(index.approvals, event.RequestID)
	}
	index.refs = append(index.refs, historyEventRef{offset: offset, length: length, kind: event.Type, run: index.activeRun, toolStart: index.tools[event.ToolCallID], approvalStart: index.approvalStarts[event.RequestID]})
	if event.Type == "agent_end" {
		index.activeRun = 0
	}
	if event.Type != "message_end" {
		return nil
	}
	if event.Message.Role == "user" || event.Message.Role == "assistant" {
		index.messages = append(index.messages, int(seq-1))
	}
	var text strings.Builder
	for _, part := range event.Message.Content {
		if part.Type == "text" {
			if text.Len() > 0 {
				text.WriteByte(' ')
			}
			text.WriteString(part.Text)
		}
	}
	excerpt := []rune(strings.Join(strings.Fields(text.String()), " "))
	if len(excerpt) > 160 {
		excerpt = excerpt[:160]
	}
	if event.Message.Role == "user" && event.Message.IncomingSource.Source.Surface != "approval_operation" {
		index.ticks = append(index.ticks, browserHistoryTick{ID: event.MessageID, Seq: seq, Title: string(excerpt)})
	} else if event.Message.Role == "assistant" && len(index.ticks) > 0 && index.ticks[len(index.ticks)-1].Preview == "" {
		index.ticks[len(index.ticks)-1].Preview = string(excerpt)
	}
	return nil
}
