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
//	                                       holder; new inputs refused. Names
//	                                       the intended destination and mints
//	                                       the transfer's proof key
//	source Export    (read-only)           deterministic stream of the cut;
//	                                       the header is addressed to the one
//	                                       destination and carries the key
//	dest   Import    (none) → staged       rows and epoch floor, verified; no
//	                                       writer, no inputs, no effects.
//	                                       Refused on any other placement
//	dest   Activate  staged → active       the destination may run the
//	                                       secretary; produces activate_proof
//	dest   Retire    staged → retired      this placement will never run this
//	             or  (none) → retired      transfer: the staged copy is deleted
//	                                       or a tombstone is recorded for a
//	                                       bundle that never arrived;
//	                                       produces retire_proof; a bare
//	                                       tombstone stays correctable
//	source Complete  sealed → transferred  requires the destination's
//	                                       activate_proof
//	source Abort     sealed → active       requires the destination's
//	                                       retire_proof
//
// Every step is idempotent by transfer id: after a lost response, repeat the
// call or read the transfer ledger, which returns the recorded proofs.
//
// The proofs are evidence, not authentication: only a party holding the
// bundle can mint them, and the destination produces each honestly only when
// it commits that transition. Each proof names the placement that produced
// it, so evidence minted by the wrong service cannot satisfy the source. The
// coordinator already holds the bundle — and with it the whole life — so the
// gates make the correct order the only easy one: no ordinary lost response,
// retry, wrong-service dispatch or partition can leave two placements able
// to run the secretary, or none.
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

// Header is the first line of a bundle. DestinationID addresses the bundle
// to one placement — an import anywhere else is refused — and TransferKey is
// the per-transfer HMAC key the destination uses to prove it committed a
// transition (activate or retire). The key is private life content like the
// rest of the bundle; it never appears in a receipt.
type Header struct {
	Record        string       `json:"record"` // "header"
	Format        string       `json:"format"`
	FormatVersion int          `json:"format_version"`
	TransferID    string       `json:"transfer_id"`
	PersonaID     string       `json:"persona_id"`
	DestinationID string       `json:"destination_id"`
	TransferKey   string       `json:"transfer_key"`
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
// plan without repeating its completed operations. The memory counts describe
// the carried semantic memory: accepted fragments in context, shelved
// candidates, verdicts, and the still-unprepared ranges the destination will
// prepare itself.
type Continuity struct {
	JournalEvents    int64 `json:"journal_events"`
	Notes            int64 `json:"notes"`
	QueuedInputs     int64 `json:"queued_inputs"`
	ClaimedInputs    int64 `json:"claimed_inputs"`
	RunningTurns     int64 `json:"running_turns"`
	UnfinishedPlans  int64 `json:"unfinished_plans"`
	PendingSchedules int64 `json:"pending_schedules"`
	UndeliveredOut   int64 `json:"undelivered_outbox"`
	MemoryApplied    int64 `json:"memory_applied"`
	MemoryPrepared   int64 `json:"memory_prepared"`
	MemorySealed     int64 `json:"memory_sealed"`
	MemoryKept       int64 `json:"memory_kept"`
	MemoryFailed     int64 `json:"memory_failed"`
}

// Receipt is the verified result of a transfer step, stored in the ledger
// and returned again on replay. ActivateProof and RetireProof are set only
// when the destination committed that transition; they are what the source's
// Complete and Abort require, and each names the destination placement in
// its HMAC input.
type Receipt struct {
	Direction     string           `json:"direction"`
	TransferID    string           `json:"transfer_id"`
	PersonaID     string           `json:"persona_id"`
	Status        string           `json:"status"`
	FormatVersion int              `json:"format_version"`
	DestinationID string           `json:"destination_id,omitempty"`
	HumanID       *string          `json:"human_id,omitempty"`
	ContentSHA256 string           `json:"content_sha256,omitempty"`
	ActivateProof string           `json:"activate_proof,omitempty"`
	RetireProof   string           `json:"retire_proof,omitempty"`
	SealedAt      time.Time        `json:"sealed_at"`
	Cut           Cut              `json:"cut"`
	Rows          map[string]int64 `json:"rows"`
	Continuity    Continuity       `json:"continuity"`
	NotIncluded   []Exclusion      `json:"not_included"`
	UpdatedAt     time.Time        `json:"updated_at"`

	// key is the transfer's HMAC key, loaded from the ledger column for
	// proof verification. It is never serialized into a receipt or stored
	// inside the receipt JSON.
	key string
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
	// colBigintList is a nullable bigint[] column: a JSON null is SQL NULL,
	// a JSON array of integers becomes the Postgres array.
	colBigintList
)

type column struct {
	name string
	kind colKind
}

// identityCols are GENERATED ALWAYS AS IDENTITY columns backed by one
// table-global sequence. Their source values are *not* imported: the column
// is omitted from the insert so the destination's own sequence allocates a
// fresh value per row — work bounded by the number of transferred records,
// never by the size of a numeric gap, and structurally unable to rewind the
// destination's sequence or collide with its in-flight admissions. The
// carried value still matters: the export orders the table by it and the
// import requires the carried values to be strictly increasing, so the
// destination's fresh allocation preserves the source's admission order
// exactly (gaps may collapse; uniqueness and order are the contract, not the
// numeric values).
var identityCols = map[string]string{
	"core_inputs": "admission_seq",
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
	{name: "core_inputs", orderBy: "admission_seq", cols: []column{
		{"persona_id", colUUID}, {"input_id", colText}, {"kind", colText}, {"payload", colJSON},
		{"actor_kind", colText}, {"actor_id", colText}, {"source_surface", colText}, {"thread_id", colText},
		{"occurred_at", colTime}, {"attention", colText}, {"status", colText},
		{"claimed_generation", colBigint}, {"turn_id", colText}, {"created_at", colTime},
		{"done_at", colTime}, {"not_before", colTime}, {"received_seq", colBigint},
		// admission_seq is the claim queue's order: carried so the bundle's
		// row order records the source's admission order; the destination
		// regenerates it (identityCols) so no sequence state crosses.
		{"admission_seq", colBigint},
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
	// The memory layer's durable semantic memory carries verbatim: accepted
	// replacement text, kept/failed verdicts, prepared candidates, the
	// seq-anchored ranges and chunk_seq locators that carried notes and
	// fragment headers reference, and the attempts/interruptions history
	// behind each judgment. What does not cross is a live execution claim:
	// the seal normalizes a 'preparing' row back to 'sealed' and clears its
	// claim fields first, so no bundle can carry work still bound to a
	// fenced source writer (verifyCut refuses a bundle that claims one).
	{name: "core_memory_chunks", orderBy: `chunk_seq`, cols: []column{
		{"persona_id", colUUID}, {"chunk_seq", colBigint}, {"layer", colInt},
		{"sources", colBigintList},
		{"first_seq", colBigint}, {"last_seq", colBigint}, {"est_tokens", colBigint},
		{"status", colText}, {"replacement", colText}, {"replacement_est_tokens", colBigint},
		{"attempts", colInt}, {"interruptions", colInt}, {"last_error", colText},
		{"claimed_generation", colBigint}, {"claimed_at", colTime}, {"not_before", colTime},
		{"created_at", colTime}, {"prepared_at", colTime}, {"applied_at", colTime},
	}},
}

// placementLocalTables reference core_personas but are never carried: the
// lease is live execution authority (only its generation travels, as the
// cut's epoch floor), and a job's claim is runner-owned execution authority
// bound to the placement that queued it (seal refuses while any job is
// non-terminal, so nothing in flight can be left behind or duplicated).
var placementLocalTables = map[string]string{
	"core_writer_leases": "live execution authority; the generation travels as cut.generation_high_water",
	"core_jobs":          "runner-owned execution authority; seal refuses while a job is non-terminal, so job records stay with the placement that ran them",
}
