package portable

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// maxLineBytes bounds one bundle line. The largest carried rows are turn
// outputs and commit requests, which the state service already bounds far
// below this.
const maxLineBytes = 64 << 20

// Export streams the sealed cut. It reads in one repeatable-read snapshot
// with timestamps rendered in UTC and rows in a fixed order, so exporting
// the same transfer again yields the same bytes and digest — which is what
// lets a destination answer a repeated import as a replay.
//
// A failure after the first byte leaves a stream without a trailer, which
// every importer rejects.
func (s *Service) Export(ctx context.Context, personaID, transferID string, w io.Writer) (Receipt, error) {
	if err := validateIDs(personaID, transferID); err != nil {
		return Receipt{}, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return Receipt{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SET LOCAL TimeZone = 'UTC'`); err != nil {
		return Receipt{}, err
	}
	var authority string
	var held *string
	err = tx.QueryRow(ctx,
		`SELECT authority, transfer_id FROM core_personas WHERE persona_id = $1`,
		personaID).Scan(&authority, &held)
	if errors.Is(err, pgx.ErrNoRows) {
		return Receipt{}, ErrPersonaNotFound
	}
	if err != nil {
		return Receipt{}, err
	}
	if !heldBy(held, transferID) || (authority != "sealed" && authority != "transferred") {
		return Receipt{}, fmt.Errorf("%w: persona is not sealed for transfer %s", ErrTransferConflict, transferID)
	}
	rec, err := ledger(ctx, tx, "export", transferID, false)
	if err != nil {
		return Receipt{}, err
	}
	if rec.key == "" {
		return Receipt{}, fmt.Errorf("transfer %s was sealed without a proof key", transferID)
	}
	hdr := Header{
		Record:        "header",
		Format:        FormatName,
		FormatVersion: rec.FormatVersion,
		TransferID:    transferID,
		PersonaID:     personaID,
		DestinationID: rec.DestinationID,
		TransferKey:   rec.key,
		SealedAt:      rec.SealedAt,
		Sections:      []SectionRef{{Name: CoreSection, Contract: CoreContract}},
		Cut:           rec.Cut,
		Secrets:       SecretsNone,
		NotIncluded:   rec.NotIncluded,
	}
	h := sha256.New()
	bw := bufio.NewWriterSize(w, 1<<16)
	body := io.MultiWriter(bw, h)
	line, err := json.Marshal(hdr)
	if err != nil {
		return Receipt{}, err
	}
	if _, err := body.Write(append(line, '\n')); err != nil {
		return Receipt{}, err
	}
	counts, err := writeRows(ctx, tx, personaID, body)
	if err != nil {
		return Receipt{}, err
	}
	if !maps.Equal(counts, rec.Rows) {
		return Receipt{}, fmt.Errorf("export rows %v differ from the sealed cut %v", counts, rec.Rows)
	}
	digest := hex.EncodeToString(h.Sum(nil))
	line, err = json.Marshal(Trailer{Record: "trailer", Rows: counts, ContentSHA256: digest})
	if err != nil {
		return Receipt{}, err
	}
	if _, err := bw.Write(append(line, '\n')); err != nil {
		return Receipt{}, err
	}
	if err := bw.Flush(); err != nil {
		return Receipt{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Receipt{}, err
	}
	// Record the digest the destination must confirm before Complete. A
	// different digest on re-export would mean the cut changed; refuse it.
	tag, err := s.pool.Exec(ctx, `
		UPDATE core_transfers SET content_sha256 = $3, updated_at = now()
		WHERE direction = 'export' AND transfer_id = $1 AND persona_id = $2
		  AND (content_sha256 IS NULL OR content_sha256 = $3)`,
		transferID, personaID, digest)
	if err != nil {
		return Receipt{}, fmt.Errorf("record export digest: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return Receipt{}, fmt.Errorf("%w: re-export digest %s differs from the recorded digest", ErrTransferConflict, digest)
	}
	rec.ContentSHA256 = digest
	return rec, nil
}

// writeRows writes the persona row and every core.v1 row as bundle lines.
func writeRows(ctx context.Context, tx pgx.Tx, personaID string, w io.Writer) (map[string]int64, error) {
	counts := map[string]int64{}
	var persona string
	if err := tx.QueryRow(ctx, `
		SELECT jsonb_build_object('persona_id', persona_id, 'display_name', display_name, 'created_at', created_at)::text
		FROM core_personas WHERE persona_id = $1`, personaID).Scan(&persona); err != nil {
		return nil, err
	}
	if err := writeRow(w, personaTable, []byte(persona), personaID); err != nil {
		return nil, err
	}
	counts[personaTable.name] = 1
	for _, t := range coreTables {
		rows, err := tx.Query(ctx,
			`SELECT to_jsonb(t)::text FROM `+t.name+` t WHERE persona_id = $1 ORDER BY `+t.orderBy, personaID)
		if err != nil {
			return nil, err
		}
		var n int64
		for rows.Next() {
			var data string
			if err := rows.Scan(&data); err != nil {
				rows.Close()
				return nil, err
			}
			if err := writeRow(w, t, []byte(data), personaID); err != nil {
				rows.Close()
				return nil, err
			}
			n++
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
		counts[t.name] = n
	}
	return counts, nil
}

func writeRow(w io.Writer, t table, data []byte, personaID string) error {
	if err := checkRow(t, data, personaID); err != nil {
		return fmt.Errorf("%w: source %v", ErrNotPortable, err)
	}
	var buf bytes.Buffer
	buf.Grow(len(data) + len(t.name) + 64)
	buf.WriteString(`{"record":"row","section":"core","table":"`)
	buf.WriteString(t.name)
	buf.WriteString(`","data":`)
	buf.Write(data)
	buf.WriteString("}\n")
	_, err := w.Write(buf.Bytes())
	return err
}

// checkRow requires exactly the contract's columns, and that the row belongs
// to the bundle's persona.
func checkRow(t table, data []byte, personaID string) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return fmt.Errorf("%s row is not a JSON object: %v", t.name, err)
	}
	if len(fields) != len(t.cols) {
		return fmt.Errorf("%s row has %d columns; %s defines %d", t.name, len(fields), CoreContract, len(t.cols))
	}
	for _, c := range t.cols {
		if _, ok := fields[c.name]; !ok {
			return fmt.Errorf("%s row lacks column %s", t.name, c.name)
		}
	}
	var pid string
	if err := json.Unmarshal(fields["persona_id"], &pid); err != nil || pid != personaID {
		return fmt.Errorf("%s row belongs to another persona", t.name)
	}
	return nil
}

func lookupTable(name string) (table, int, bool) {
	if name == personaTable.name {
		return personaTable, 0, true
	}
	for i, t := range coreTables {
		if t.name == name {
			return t, i + 1, true
		}
	}
	return table{}, 0, false
}

func (t table) insertSQL() string {
	names := make([]string, len(t.cols))
	exprs := make([]string, len(t.cols))
	for i, c := range t.cols {
		names[i] = c.name
		switch c.kind {
		case colText:
			exprs[i] = fmt.Sprintf("d->>'%s'", c.name)
		case colUUID:
			exprs[i] = fmt.Sprintf("(d->>'%s')::uuidv7", c.name)
		case colBigint:
			exprs[i] = fmt.Sprintf("(d->>'%s')::bigint", c.name)
		case colInt:
			exprs[i] = fmt.Sprintf("(d->>'%s')::int", c.name)
		case colTime:
			exprs[i] = fmt.Sprintf("(d->>'%s')::timestamptz", c.name)
		case colJSON:
			exprs[i] = fmt.Sprintf("d->'%s'", c.name)
		case colJSONNull:
			exprs[i] = fmt.Sprintf("NULLIF(d->'%s', 'null'::jsonb)", c.name)
		}
	}
	// A carried identity column keeps the value the source assigned; without
	// OVERRIDING SYSTEM VALUE a GENERATED ALWAYS column rejects the insert.
	override := ""
	if col, ok := identityCols[t.name]; ok {
		for _, c := range t.cols {
			if c.name == col {
				override = " OVERRIDING SYSTEM VALUE"
				break
			}
		}
	}
	return fmt.Sprintf("INSERT INTO %s (%s)%s SELECT %s FROM (SELECT $1::jsonb AS d) r",
		t.name, strings.Join(names, ", "), override, strings.Join(exprs, ", "))
}

// advanceIdentities moves each carried identity sequence past every value
// the destination now holds — including this import's staged rows. The
// sequence is shared by the whole table, so the floor is the table-wide
// maximum, never a per-persona one.
//
// It bumps with nextval rather than setval, deliberately: nextval cannot
// rewind. A stale floor read only makes us bump further, and a concurrent
// admission or another import's bump can interleave freely — every call
// moves the shared sequence forward. A rolled-back import leaves at most a
// legal gap; a setval could instead land below a value already issued to an
// in-flight admission on another persona, and its effect would persist past
// the rollback.
func advanceIdentities(ctx context.Context, tx pgx.Tx) error {
	for name, col := range identityCols {
		var seq string
		if err := tx.QueryRow(ctx,
			`SELECT pg_get_serial_sequence($1, $2)`, name, col).Scan(&seq); err != nil {
			return fmt.Errorf("identity sequence %s.%s: %w", name, col, err)
		}
		var last int64
		var called bool
		// The sequence name comes from pg_get_serial_sequence — the
		// catalog's own qualified, quoted name for this column's sequence.
		if err := tx.QueryRow(ctx, fmt.Sprintf(
			`SELECT last_value, is_called FROM %s`, seq)).Scan(&last, &called); err != nil {
			return fmt.Errorf("identity position %s: %w", seq, err)
		}
		var floor int64
		if err := tx.QueryRow(ctx, fmt.Sprintf(
			`SELECT COALESCE(max(%s), 0) FROM %s`, col, name)).Scan(&floor); err != nil {
			return fmt.Errorf("identity floor %s.%s: %w", name, col, err)
		}
		issued := last
		if !called {
			issued = last - 1 // nothing handed out yet; first nextval returns last_value
		}
		need := floor - issued
		if need <= 0 {
			continue
		}
		if _, err := tx.Exec(ctx,
			`SELECT count(nextval($1::regclass)) FROM generate_series(1, $2::bigint)`,
			seq, need); err != nil {
			return fmt.Errorf("advance %s: %w", seq, err)
		}
	}
	return nil
}

// The destination binds the persona to its own authenticated human; the
// source's human binding is never read from a bundle.
const personaInsertSQL = `
	INSERT INTO core_personas (persona_id, human_id, display_name, created_at, authority, transfer_id)
	SELECT (d->>'persona_id')::uuidv7, $2, d->>'display_name', (d->>'created_at')::timestamptz, 'staged', $3
	FROM (SELECT $1::jsonb AS d) r`

// Import stages a bundle addressed to this placement. Everything happens in
// one transaction: rows are inserted as they stream, then the trailer's
// counts and digest, the cut positions and reference integrity are verified,
// the lease epoch floor is written, and the ledger records the staged
// receipt. Any failure — a cut connection, a changed byte, an unknown section
// or column, a dangling reference — rolls back to nothing.
//
// A bundle addressed to another placement is refused outright, so an ordinary
// "import timed out, try another service" retry can never leave two staged
// copies. A retired transfer is refused forever: cancellation is durable
// against a late or replayed import. A persona already present here is never
// overwritten or duplicated. The same transfer imported again with identical
// content and the same human_id is answered with the recorded receipt
// (created=false); a different human_id is a conflict, not a silent rebind.
func (s *Service) Import(ctx context.Context, r io.Reader, humanID *string) (Receipt, bool, error) {
	if humanID != nil && !uuidv7Re.MatchString(*humanID) {
		return Receipt{}, false, fmt.Errorf("%w: human_id must be a uuidv7", ErrBadRequest)
	}
	br := bufio.NewReaderSize(r, 1<<16)
	h := sha256.New()
	line, err := readLine(br)
	if errors.Is(err, io.EOF) {
		return Receipt{}, false, fmt.Errorf("%w: empty bundle", ErrBadBundle)
	}
	if err != nil {
		return Receipt{}, false, err
	}
	var hdr Header
	if err := strictDecode(line, &hdr); err != nil {
		return Receipt{}, false, err
	}
	if err := checkHeader(hdr); err != nil {
		return Receipt{}, false, err
	}
	own, err := s.PlacementID(ctx)
	if err != nil {
		return Receipt{}, false, err
	}
	if hdr.DestinationID != own {
		return Receipt{}, false, fmt.Errorf("%w: this bundle is addressed to placement %s; this is %s",
			ErrBadBundle, hdr.DestinationID, own)
	}
	h.Write(line)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Receipt{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := transferLock(ctx, tx, hdr.TransferID); err != nil {
		return Receipt{}, false, err
	}
	prior, err := ledger(ctx, tx, "import", hdr.TransferID, true)
	replay := false
	switch {
	case errors.Is(err, ErrTransferNotFound):
	case err != nil:
		return Receipt{}, false, err
	case prior.PersonaID != hdr.PersonaID:
		return Receipt{}, false, fmt.Errorf("%w: transfer %s was imported for another persona", ErrTransferConflict, hdr.TransferID)
	case prior.Status == "retired":
		return Receipt{}, false, fmt.Errorf("%w: transfer %s was retired here; it can never be staged on this placement",
			ErrTransferConflict, hdr.TransferID)
	default:
		replay = true
		if (prior.HumanID == nil) != (humanID == nil) ||
			(prior.HumanID != nil && *prior.HumanID != *humanID) {
			return Receipt{}, false, fmt.Errorf("%w: transfer %s was imported with a different human_id",
				ErrTransferConflict, hdr.TransferID)
		}
	}
	if !replay {
		var authority string
		err := tx.QueryRow(ctx,
			`SELECT authority FROM core_personas WHERE persona_id = $1`, hdr.PersonaID).Scan(&authority)
		if err == nil {
			return Receipt{}, false, fmt.Errorf("%w (authority %s); a transfer never overwrites or duplicates a secretary",
				ErrPersonaExists, authority)
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return Receipt{}, false, err
		}
		if err := checkDestinationSchema(ctx, tx); err != nil {
			return Receipt{}, false, err
		}
	}

	counts := map[string]int64{personaTable.name: 0}
	for _, t := range coreTables {
		counts[t.name] = 0
	}
	next := 0
	var trailer Trailer
	for {
		line, err := readLine(br)
		if errors.Is(err, io.EOF) {
			return Receipt{}, false, fmt.Errorf("%w: truncated: the stream ended before its trailer", ErrBadBundle)
		}
		if err != nil {
			return Receipt{}, false, err
		}
		var kind struct {
			Record string `json:"record"`
		}
		if err := json.Unmarshal(line, &kind); err != nil {
			return Receipt{}, false, fmt.Errorf("%w: malformed line: %v", ErrBadBundle, err)
		}
		if kind.Record == "trailer" {
			if err := strictDecode(line, &trailer); err != nil {
				return Receipt{}, false, err
			}
			break
		}
		if kind.Record != "row" {
			return Receipt{}, false, fmt.Errorf("%w: unexpected record %q", ErrBadBundle, kind.Record)
		}
		var row rowRecord
		if err := strictDecode(line, &row); err != nil {
			return Receipt{}, false, err
		}
		if row.Section != CoreSection {
			return Receipt{}, false, fmt.Errorf("%w: row in undeclared section %q", ErrBadBundle, row.Section)
		}
		t, idx, ok := lookupTable(row.Table)
		if !ok {
			return Receipt{}, false, fmt.Errorf("%w: %s has no table %q", ErrBadBundle, CoreContract, row.Table)
		}
		if idx < next {
			return Receipt{}, false, fmt.Errorf("%w: table %s out of order", ErrBadBundle, row.Table)
		}
		next = idx
		if err := checkRow(t, row.Data, hdr.PersonaID); err != nil {
			return Receipt{}, false, fmt.Errorf("%w: %v", ErrBadBundle, err)
		}
		if idx == 0 && counts[personaTable.name] > 0 {
			return Receipt{}, false, fmt.Errorf("%w: more than one persona row", ErrBadBundle)
		}
		if idx > 0 && counts[personaTable.name] == 0 {
			return Receipt{}, false, fmt.Errorf("%w: rows before the persona row", ErrBadBundle)
		}
		if !replay {
			if idx == 0 {
				_, err = tx.Exec(ctx, personaInsertSQL, json.RawMessage(row.Data), humanID, hdr.TransferID)
				err = personaInsertErr(err)
			} else {
				_, err = tx.Exec(ctx, t.insertSQL(), json.RawMessage(row.Data))
				err = insertErr(t.name, err)
			}
			if err != nil {
				return Receipt{}, false, err
			}
		}
		counts[t.name]++
		h.Write(line)
	}
	if _, err := readLine(br); !errors.Is(err, io.EOF) {
		if err != nil {
			return Receipt{}, false, err
		}
		return Receipt{}, false, fmt.Errorf("%w: data after the trailer", ErrBadBundle)
	}
	digest := hex.EncodeToString(h.Sum(nil))
	if trailer.ContentSHA256 != digest {
		return Receipt{}, false, fmt.Errorf("%w: content digest mismatch (trailer %s, received %s)",
			ErrBadBundle, trailer.ContentSHA256, digest)
	}
	if !maps.Equal(trailer.Rows, counts) {
		return Receipt{}, false, fmt.Errorf("%w: row counts %v differ from the trailer %v", ErrBadBundle, counts, trailer.Rows)
	}
	if counts[personaTable.name] != 1 {
		return Receipt{}, false, fmt.Errorf("%w: bundle has no persona row", ErrBadBundle)
	}
	if replay {
		if prior.ContentSHA256 != digest {
			return Receipt{}, false, fmt.Errorf("%w: transfer %s was imported with different content", ErrTransferConflict, hdr.TransferID)
		}
		return prior, false, tx.Commit(ctx)
	}

	// Epoch floor: the destination's lease starts at the source's sealed
	// generation, already expired. The first writer here acquires the next
	// generation, so every carried turn, claim and operation belongs to an
	// older writer and is recovered rather than resumed as its own. The
	// expiry is a fixed past instant rather than now(): a now()-written
	// "dead" marker can look live to a later transaction after the host
	// clock steps backward, which has been observed on WSL2 hosts.
	if _, err := tx.Exec(ctx, `
		INSERT INTO core_writer_leases (persona_id, generation, holder_id, acquired_at, expires_at)
		VALUES ($1, $2, $3, now(), 'epoch'::timestamptz)`,
		hdr.PersonaID, hdr.Cut.GenerationHighWater, sealHolder(hdr.TransferID)); err != nil {
		return Receipt{}, false, fmt.Errorf("write lease epoch floor: %w", err)
	}
	violations, err := verifyCut(ctx, tx, hdr.PersonaID)
	if err != nil {
		return Receipt{}, false, err
	}
	if len(violations) > 0 {
		return Receipt{}, false, fmt.Errorf("%w: %s", ErrIntegrity, describe(violations))
	}
	rows, cont, cut, err := summarize(ctx, tx, hdr.PersonaID)
	if err != nil {
		return Receipt{}, false, err
	}
	if cut.LatestEventSeq != hdr.Cut.LatestEventSeq || cut.LatestOutboxSeq != hdr.Cut.LatestOutboxSeq {
		return Receipt{}, false, fmt.Errorf("%w: journal/outbox positions %d/%d differ from the declared cut %d/%d",
			ErrBadBundle, cut.LatestEventSeq, cut.LatestOutboxSeq, hdr.Cut.LatestEventSeq, hdr.Cut.LatestOutboxSeq)
	}
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT now()`).Scan(&now); err != nil {
		return Receipt{}, false, err
	}
	rec := Receipt{
		Direction:     "import",
		TransferID:    hdr.TransferID,
		PersonaID:     hdr.PersonaID,
		Status:        "staged",
		FormatVersion: hdr.FormatVersion,
		DestinationID: hdr.DestinationID,
		HumanID:       humanID,
		ContentSHA256: digest,
		SealedAt:      hdr.SealedAt.UTC(),
		Cut:           hdr.Cut,
		Rows:          rows,
		Continuity:    cont,
		// This placement's own statement of what it did not receive, not
		// the bundle's claim.
		NotIncluded: NotIncluded,
		UpdatedAt:   now.UTC(),
	}
	raw, err := json.Marshal(rec)
	if err != nil {
		return Receipt{}, false, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO core_transfers (direction, transfer_id, persona_id, status, format_version, destination_id, content_sha256, proof_key, receipt, created_at, updated_at)
		VALUES ('import', $1, $2, 'staged', $3, $4, $5, $6, $7, $8, $8)`,
		hdr.TransferID, hdr.PersonaID, hdr.FormatVersion, hdr.DestinationID, digest, hdr.TransferKey, raw, now); err != nil {
		return Receipt{}, false, fmt.Errorf("record transfer: %w", err)
	}
	// Last step before commit: a rejected bundle above must not have touched
	// the shared sequence at all. nextval bumps are non-transactional, so a
	// crash after this point can only leave the sequence further ahead.
	if err := advanceIdentities(ctx, tx); err != nil {
		return Receipt{}, false, err
	}
	return rec, true, tx.Commit(ctx)
}

func checkHeader(h Header) error {
	switch {
	case h.Record != "header":
		return fmt.Errorf("%w: the first record must be the header", ErrBadBundle)
	case h.Format != FormatName:
		return fmt.Errorf("%w: format %q is not %s", ErrBadBundle, h.Format, FormatName)
	case h.FormatVersion != FormatVersion:
		return fmt.Errorf("%w: format version %d is not supported; this placement reads version %d",
			ErrBadBundle, h.FormatVersion, FormatVersion)
	case !transferIDRe.MatchString(h.TransferID):
		return fmt.Errorf("%w: invalid transfer_id", ErrBadBundle)
	case !uuidv7Re.MatchString(h.PersonaID):
		return fmt.Errorf("%w: persona_id must be a uuidv7", ErrBadBundle)
	case !uuidv7Re.MatchString(h.DestinationID):
		return fmt.Errorf("%w: destination_id must be a placement uuidv7", ErrBadBundle)
	case !proofRe.MatchString(h.TransferKey):
		return fmt.Errorf("%w: header lacks a valid transfer_key", ErrBadBundle)
	case h.Secrets != SecretsNone:
		return fmt.Errorf("%w: secrets mode %q is not supported", ErrBadBundle, h.Secrets)
	case h.Cut.GenerationHighWater < 1:
		return fmt.Errorf("%w: cut has no generation high-water mark", ErrBadBundle)
	}
	if len(h.Sections) != 1 || h.Sections[0] != (SectionRef{Name: CoreSection, Contract: CoreContract}) {
		return fmt.Errorf("%w: sections %v are not supported; this placement implements only %s/%s",
			ErrBadBundle, h.Sections, CoreSection, CoreContract)
	}
	return nil
}

// checkDestinationSchema refuses to stage into a schema whose carried tables
// differ from core.v1: a missing or extra column would silently drop state
// or fill it with defaults the source never had.
func checkDestinationSchema(ctx context.Context, q querier) error {
	for _, t := range append([]table{personaTable}, coreTables...) {
		have, err := strings_(ctx, q, `
			SELECT column_name FROM information_schema.columns
			WHERE table_schema = current_schema() AND table_name = $1`, t.name)
		if err != nil {
			return err
		}
		present := map[string]bool{}
		for _, c := range have {
			present[c] = true
		}
		for _, c := range t.cols {
			if !present[c.name] {
				return fmt.Errorf("%w: destination %s lacks column %s", ErrNotPortable, t.name, c.name)
			}
		}
		if t.name != personaTable.name && len(have) != len(t.cols) {
			return fmt.Errorf("%w: destination %s has %d columns; %s defines %d",
				ErrNotPortable, t.name, len(have), CoreContract, len(t.cols))
		}
	}
	return nil
}

// insertErr maps insert failures of carried rows. A duplicate key inside a
// bundle is a deterministic bundle defect (422), not a concurrency signal:
// the staged persona's keyspace is fresh, so 23505 here means the bundle
// itself carries the same key twice.
func insertErr(table string, err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && (strings.HasPrefix(pgErr.Code, "22") || strings.HasPrefix(pgErr.Code, "23")) {
		if pgErr.Code == "23505" {
			return fmt.Errorf("%w: %s row duplicates a key already in this bundle", ErrBadBundle, table)
		}
		return fmt.Errorf("%w: %s row: %v", ErrBadBundle, table, err)
	}
	return fmt.Errorf("insert %s row: %w", table, err)
}

// personaInsertErr maps failures of the persona insert itself. A duplicate
// persona means a concurrent different transfer staged it first — a 409, not
// a bundle defect. A missing human is a caller-parameter error naming
// human_id, not a bundle error.
func personaInsertErr(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505":
			return fmt.Errorf("%w (staged by a concurrent transfer); a transfer never overwrites or duplicates a secretary",
				ErrPersonaExists)
		case "23503":
			return fmt.Errorf("%w: human_id does not reference an existing human", ErrBadRequest)
		}
		if strings.HasPrefix(pgErr.Code, "22") || strings.HasPrefix(pgErr.Code, "23") {
			return fmt.Errorf("%w: %s row: %v", ErrBadBundle, personaTable.name, err)
		}
	}
	return fmt.Errorf("insert %s row: %w", personaTable.name, err)
}

func readLine(br *bufio.Reader) ([]byte, error) {
	var buf []byte
	for {
		chunk, err := br.ReadSlice('\n')
		buf = append(buf, chunk...)
		switch {
		case errors.Is(err, bufio.ErrBufferFull):
			if len(buf) > maxLineBytes {
				return nil, fmt.Errorf("%w: a line exceeds %d bytes", ErrBadBundle, maxLineBytes)
			}
		case errors.Is(err, io.EOF):
			if len(buf) == 0 {
				return nil, io.EOF
			}
			return nil, fmt.Errorf("%w: truncated: the last line has no newline", ErrBadBundle)
		case err != nil:
			return nil, err
		default:
			return buf, nil
		}
	}
}

func strictDecode(line []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(line))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("%w: %v", ErrBadBundle, err)
	}
	if dec.More() {
		return fmt.Errorf("%w: more than one JSON value on a line", ErrBadBundle)
	}
	return nil
}
