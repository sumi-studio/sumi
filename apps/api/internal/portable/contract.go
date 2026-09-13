// Package portable moves one secretary's canonical core state from one
// placement to another — for example from a Local Sumi to Sumi Cloud — so the
// same individual continues there (engineering plan §4.2 and §5, M19).
//
// This is portability for state the new architecture creates. It is not a
// legacy importer and carries no compatibility layer for older formats.
//
// The unit of transfer is a bundle stream (format sumi.portable-secretary,
// version 1): one header line, one line per carried row, and a trailer line
// with row counts and a SHA-256 over every byte before it. A stream without a
// valid trailer is rejected, so a cut connection can never import half a life.
//
// Lifecycle and the authority each step leaves behind:
//
//	source Seal      active → sealed       writer generation bumped past every
//	                                       holder; new inputs refused
//	source Export    (read-only)           deterministic stream of the cut
//	dest   Import    (none) → staged       rows and epoch floor, verified; no
//	                                       writer, no inputs, no effects
//	dest   Activate  staged → active       the destination may run the secretary
//	source Complete  sealed → transferred  records the destination's digest
//
// Abort (source, sealed → active) and Discard (destination, staged → removed)
// undo an unfinished transfer. Every step is idempotent by transfer id: after
// a lost response, repeat the call or read the transfer ledger.
//
// What is deliberately not carried is part of the contract: the writer lease
// itself (only its generation as an epoch floor), placement authority, the
// human binding (the destination binds the persona to its own authenticated
// human), and every kind of state listed in NotIncluded.
package portable

import (
	"encoding/json"
	"time"
)

const (
	FormatName    = "sumi.portable-secretary"
	FormatVersion = 1
	CoreSection   = "core"
	CoreContract  = "core.v1"
	// SecretsNone is the only secrets mode version 1 defines: no credential,
	// token or key material is ever written into a bundle.
	SecretsNone = "none"
)

// Header is the first line of a bundle.
type Header struct {
	Record        string       `json:"record"` // "header"
	Format        string       `json:"format"`
	FormatVersion int          `json:"format_version"`
	TransferID    string       `json:"transfer_id"`
	PersonaID     string       `json:"persona_id"`
	SealedAt      time.Time    `json:"sealed_at"`
	Sections      []SectionRef `json:"sections"`
	Cut           Cut          `json:"cut"`
	Secrets       string       `json:"secrets"`
	NotIncluded   []Exclusion  `json:"not_included"`
}

// SectionRef names a section and the contract its rows follow. A reader must
// refuse a bundle naming a section or contract it does not implement rather
// than skip it: skipping would activate a secretary missing part of its state.
type SectionRef struct {
	Name     string `json:"name"`
	Contract string `json:"contract"`
}

// Cut is the source position the bundle captures.
type Cut struct {
	// GenerationHighWater is the source writer generation after sealing. It
	// exceeds every generation recorded in the bundle; the destination starts
	// its lease epoch here so no carried turn, claim or operation can be
	// mistaken for work of the destination's first writer.
	GenerationHighWater int64 `json:"generation_high_water"`
	LatestEventSeq      int64 `json:"latest_event_seq"`
	LatestOutboxSeq     int64 `json:"latest_outbox_seq"`
}

// Exclusion declares state a version 1 bundle does not carry, with its owner
// and why. It is shown to whoever drives the transfer so nothing reads as
// covered merely because a module exists.
type Exclusion struct {
	Name   string `json:"name"`
	Owner  string `json:"owner"`
	Reason string `json:"reason"`
}

type rowRecord struct {
	Record  string          `json:"record"` // "row"
	Section string          `json:"section"`
	Table   string          `json:"table"`
	Data    json.RawMessage `json:"data"`
}

// Trailer is the last line of a bundle.
type Trailer struct {
	Record        string           `json:"record"` // "trailer"
	Rows          map[string]int64 `json:"rows"`
	ContentSHA256 string           `json:"content_sha256"`
}

// Continuity summarizes what the destination will continue, computed from
// the verified rows. Claimed inputs and running turns are interrupted work of
// a source writer that no longer exists: the destination's first writer
// recovers them through the ordinary recovery path, continuing any recorded
// plan without repeating its completed operations.
type Continuity struct {
	JournalEvents    int64 `json:"journal_events"`
	Notes            int64 `json:"notes"`
	QueuedInputs     int64 `json:"queued_inputs"`
	ClaimedInputs    int64 `json:"claimed_inputs"`
	RunningTurns     int64 `json:"running_turns"`
	UnfinishedPlans  int64 `json:"unfinished_plans"`
	PendingSchedules int64 `json:"pending_schedules"`
	UndeliveredOut   int64 `json:"undelivered_outbox"`
}

// Receipt is the verified result of a transfer step, stored in the ledger
// and returned again on replay.
type Receipt struct {
	Direction     string           `json:"direction"`
	TransferID    string           `json:"transfer_id"`
	PersonaID     string           `json:"persona_id"`
	Status        string           `json:"status"`
	FormatVersion int              `json:"format_version"`
	ContentSHA256 string           `json:"content_sha256,omitempty"`
	SealedAt      time.Time        `json:"sealed_at"`
	Cut           Cut              `json:"cut"`
	Rows          map[string]int64 `json:"rows"`
	Continuity    Continuity       `json:"continuity"`
	NotIncluded   []Exclusion      `json:"not_included"`
	UpdatedAt     time.Time        `json:"updated_at"`
}

// NotIncluded is what version 1 does not carry. Each entry needs its own
// section contract, owned by the module that defines the state, before a
// transfer can claim to preserve it.
var NotIncluded = []Exclusion{
	{Name: "files", Owner: "fabric-cloud (M11)",
		Reason: "file contents, versions and object bytes live in the file service, not core state"},
	{Name: "jobs", Owner: "jobs-results (M09)",
		Reason: "background job records and their completion authority are not core state"},
	{Name: "approvals", Owner: "unassigned (M08)",
		Reason: "pending human approvals do not exist in core state; recorded turn plans are carried but are the model's decisions, not human approvals"},
	{Name: "connections", Owner: "unassigned (M08, D9)",
		Reason: "model and tool connections are not core state; credentials are never written into a bundle"},
	{Name: "memory_projection", Owner: "unassigned (M06)",
		Reason: "search projections and encrypted originals are not core state; the journal, including notes, is carried verbatim"},
	{Name: "account_and_workspace", Owner: "koseki / workspace (M21)",
		Reason: "human account, employer and workspace membership are resolved by the destination's authentication, never imported"},
	{Name: "usage", Owner: "unassigned (M14)",
		Reason: "usage, budget and billing records are not core state"},
}

type colKind int

const (
	colText colKind = iota
	colUUID
	colBigint
	colInt
	colTime
	// colJSON is a NOT NULL jsonb column: a JSON null is the jsonb literal.
	colJSON
	// colJSONNull is a nullable jsonb column: a JSON null is SQL NULL. A
	// source holding the jsonb literal null there cannot be represented and
	// is refused at seal.
	colJSONNull
)

type column struct {
	name string
	kind colKind
}

type table struct {
	name    string
	orderBy string
	cols    []column
}

// personaTable is the persona row as carried: identity and birth time only.
// human_id, authority and transfer_id are placement-local.
var personaTable = table{name: "core_personas", cols: []column{
	{"persona_id", colUUID}, {"display_name", colText}, {"created_at", colTime},
}}

// coreTables is contract core.v1, in insert order (turns and plans reference
// inputs). The column lists are the contract: a schema change to these
// tables must change them deliberately, which the coverage test enforces.
var coreTables = []table{
	{name: "core_inputs", orderBy: `input_id COLLATE "C"`, cols: []column{
		{"persona_id", colUUID}, {"input_id", colText}, {"kind", colText}, {"payload", colJSON},
		{"actor_kind", colText}, {"actor_id", colText}, {"source_surface", colText}, {"thread_id", colText},
		{"occurred_at", colTime}, {"attention", colText}, {"status", colText},
		{"claimed_generation", colBigint}, {"turn_id", colText}, {"created_at", colTime},
		{"done_at", colTime}, {"not_before", colTime},
	}},
	{name: "core_turns", orderBy: `turn_id COLLATE "C"`, cols: []column{
		{"persona_id", colUUID}, {"turn_id", colText}, {"input_id", colText}, {"generation", colBigint},
		{"attempt", colInt}, {"status", colText}, {"started_at", colTime}, {"finished_at", colTime},
		{"output", colJSONNull}, {"usage", colJSONNull}, {"error", colText}, {"commit_request", colJSONNull},
	}},
	{name: "core_turn_plans", orderBy: `input_id COLLATE "C"`, cols: []column{
		{"persona_id", colUUID}, {"input_id", colText}, {"turn_id", colText}, {"generation", colBigint},
		{"plan", colJSON}, {"created_at", colTime},
	}},
	{name: "core_events", orderBy: `seq`, cols: []column{
		{"persona_id", colUUID}, {"seq", colBigint}, {"turn_id", colText}, {"kind", colText},
		{"payload", colJSON}, {"created_at", colTime},
	}},
	{name: "core_operations", orderBy: `operation_id COLLATE "C"`, cols: []column{
		{"persona_id", colUUID}, {"operation_id", colText}, {"turn_id", colText}, {"tool", colText},
		{"idempotency_key", colText}, {"request", colJSON}, {"status", colText}, {"response", colJSONNull},
		{"claimed_generation", colBigint}, {"created_at", colTime}, {"completed_at", colTime},
	}},
	{name: "core_schedules", orderBy: `schedule_id COLLATE "C"`, cols: []column{
		{"persona_id", colUUID}, {"schedule_id", colText}, {"wake_at", colTime}, {"payload", colJSON},
		{"miss_policy", colText}, {"status", colText}, {"claimed_generation", colBigint},
		{"created_at", colTime}, {"fired_at", colTime},
	}},
	{name: "core_outbox", orderBy: `seq`, cols: []column{
		{"persona_id", colUUID}, {"seq", colBigint}, {"kind", colText}, {"payload", colJSON},
		{"created_at", colTime}, {"delivered_at", colTime},
	}},
}

// placementLocalTables reference core_personas but are never carried: the
// lease is live execution authority (only its generation travels, as the
// cut's epoch floor).
var placementLocalTables = map[string]string{
	"core_writer_leases": "live execution authority; the generation travels as cut.generation_high_water",
}
