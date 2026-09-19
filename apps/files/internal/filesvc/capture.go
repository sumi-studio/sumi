package filesvc

// capture.go — the private immutable-capture service primitive.
//
// One scoped PostgreSQL REPEATABLE READ transaction on the JuiceFS
// metadata engine records the explicit scope anchor, the complete
// supported namespace (paths + types), the ordered slice mappings of
// every file, and the gated volume-format identity. The manifest is
// durable in the filesvc database: restart retrieves the same capture,
// reads are bound to manifest rows only, and missing captured objects
// answer bounded pending — never silent divergence.
//
// Privacy boundary: the metadata DSN and object store live only here.
// Callers see manifest rows and byte streams keyed by (capture, entry
// seq) — never slice IDs, object keys, inodes, or credentials.

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CaptureConfig is the private provisioning the service requires. With
// no config the capture endpoints refuse (ErrCaptureUnconfigured) —
// there is deliberately no live-tree fallback path.
type CaptureConfig struct {
	MetaDSN    string        // postgres DSN of the JuiceFS metadata engine
	ObjKind    string        // "file" only; anything else is a config refusal
	ObjRoot    string        // object-store root for ObjKind=="file"
	TTL        time.Duration // manifest lifetime; default 24h
	MaxEntries int64         // namespace bound; default 1_000_000
}

// CaptureService owns capture creation and all manifest/byte reads.
type CaptureService struct {
	cfg     CaptureConfig
	meta    *pgxpool.Pool
	objs    objectStore
	persist capturePersister
	now     func() time.Time
}

// capturePersister is the durable-manifest surface. *Store implements it
// against the filesvc DB; fakes can substitute for handler tests.
type capturePersister interface {
	saveCapture(ctx context.Context, c *captureRow, entries []captureEntryRow, slices []captureSliceRow) error
	getCapture(ctx context.Context, id string) (*captureRow, error)
	entryStream(ctx context.Context, id string, afterSeq, limit int64) ([]captureEntryRow, error)
	getEntry(ctx context.Context, id string, seq int64) (*captureEntryRow, error)
	slicesFor(ctx context.Context, id string, ino uint64) ([]captureSliceRow, error)
	setCaptureStatus(ctx context.Context, id, status string) (bool, error)
}

// captureRow is the durable capture record.
type captureRow struct {
	CaptureID   string
	Scope       string
	Owner       string
	OwnerEpoch  int64
	Volume      string
	AnchorIno   uint64
	ScopeID     string // opaque sha256(volume|anchor) — rename+recreate changes it
	Format      captureFormat
	ManifestSHA string
	Status      string // active|released|expired
	EntryCount  int64
	Unsupported int64
	CreatedAt   time.Time
	ExpiresAt   time.Time
	ReleasedAt  *time.Time
}

// captureEntryRow is one durable manifest row. Path is the raw byte path
// relative to the scope root (” for the anchor itself); names keep raw
// bytes — JSON transport uses base64, never delimiter-joined strings.
type captureEntryRow struct {
	CaptureID string
	Seq       int64
	Path      []byte
	Name      []byte
	ParentIno uint64
	Ino       uint64
	NodeType  uint8
	Mode      uint16
	UID       uint32
	GID       uint32
	Nlink     uint32
	Length    uint64
	MtimeNS   int64
	CtimeNS   int64
	Link      []byte // symlink target, nil otherwise
	LinkGroup string // opaque hardlink group (nlink>1 files only)
	MapSHA    string // sha of resolved ranges for files
	Supported bool
}

// captureSliceRow is one normalized raw slice record, kept in declared
// order per (ino, indx). The resolved view is derived at read time so
// the durable rows stay faithful to what the engine recorded.
type captureSliceRow struct {
	CaptureID string
	Ino       uint64
	Indx      uint32
	Seq       int
	SliceID   uint64
	Pos       uint32
	Size      uint32
	Off       uint32
	Len       uint32
}

const (
	captureStatusActive   = "active"
	captureStatusReleased = "released"
	captureStatusExpired  = "expired"
	maxWalkDepth          = 256
)

// NewCaptureService builds the service or returns a config refusal.
// Startup fails loudly on bad config — a half-configured capture path is
// worse than none.
func NewCaptureService(ctx context.Context, cfg CaptureConfig, persist capturePersister) (*CaptureService, error) {
	if cfg.MetaDSN == "" {
		return nil, fmt.Errorf("%w: capture metadata DSN is required", ErrCaptureUnconfigured)
	}
	if cfg.ObjKind != "file" {
		return nil, fmt.Errorf("%w: object store kind %q unsupported (only \"file\")",
			ErrCaptureUnconfigured, cfg.ObjKind)
	}
	if cfg.ObjRoot == "" {
		return nil, fmt.Errorf("%w: object store root is required", ErrCaptureUnconfigured)
	}
	if cfg.TTL <= 0 {
		cfg.TTL = 24 * time.Hour
	}
	if cfg.MaxEntries <= 0 {
		cfg.MaxEntries = 1_000_000
	}
	pool, err := pgxpool.New(ctx, cfg.MetaDSN)
	if err != nil {
		return nil, fmt.Errorf("capture metadata pool: %w", err)
	}
	return &CaptureService{
		cfg: cfg, meta: pool, persist: persist,
		objs: &fileObjStore{root: cfg.ObjRoot},
		now:  time.Now,
	}, nil
}

func (c *CaptureService) Close() {
	if c.meta != nil {
		c.meta.Close()
	}
}

// scopeID is the opaque anchor identity the mover compares across
// retakes: same volume + same anchor inode ⇒ same id; a renamed-out and
// recreated scope dir is a different inode and a different id. Raw
// inodes never leave the service.
func scopeID(volume string, anchorIno uint64) string {
	s := sha256.Sum256([]byte(fmt.Sprintf("%s|%d", volume, anchorIno)))
	return "si:" + hex.EncodeToString(s[:16])
}

// linkGroup is the opaque hardlink group for one inode inside one
// capture: equal groups mean the names share an inode (byte-equal AND
// linked); distinct files with equal bytes get different groups.
func linkGroup(captureID string, ino uint64) string {
	s := sha256.Sum256([]byte(fmt.Sprintf("%s|%d", captureID, ino)))
	return "lg:" + hex.EncodeToString(s[:16])
}

// requiredColumns is the schema gate: the exact (table, column) pairs
// the capture reads. Missing any pair means an unknown schema/layout —
// refused up front, before trusting a single row.
var requiredColumns = [][2]string{
	{"jfs_edge", "parent"}, {"jfs_edge", "name"}, {"jfs_edge", "inode"}, {"jfs_edge", "type"},
	{"jfs_node", "inode"}, {"jfs_node", "type"}, {"jfs_node", "mode"},
	{"jfs_node", "uid"}, {"jfs_node", "gid"}, {"jfs_node", "mtime"}, {"jfs_node", "mtimensec"},
	{"jfs_node", "ctime"}, {"jfs_node", "ctimensec"}, {"jfs_node", "nlink"}, {"jfs_node", "length"},
	{"jfs_chunk", "inode"}, {"jfs_chunk", "indx"}, {"jfs_chunk", "slices"},
	{"jfs_symlink", "inode"}, {"jfs_symlink", "target"},
	{"jfs_setting", "name"}, {"jfs_setting", "value"},
}

// privateName reports whether a raw edge name is service-private staging
// or operation state. ONLY the two exact reserved predicates are
// excluded — .filesv-notes and every other .filesv-* name is a legal
// user file and belongs in the manifest.
func privateName(name []byte) bool {
	const tmp = ".filesv-tmp-"
	const op = ".filesv-op-"
	return len(name) >= len(tmp) && string(name[:len(tmp)]) == tmp ||
		len(name) >= len(op) && string(name[:len(op)]) == op
}

// jfsNode is the node-attr projection the manifest needs.
type jfsNode struct {
	inode   uint64
	typ     uint8
	mode    uint16
	uid     uint32
	gid     uint32
	mtimeNS int64
	ctimeNS int64
	nlink   uint32
	length  uint64
}

// Capture builds and durably records a manifest for scope under the
// caller's lineage (owner, epoch). The whole namespace read happens in
// one REPEATABLE READ transaction; malformed metadata or a gated
// format fails the capture visibly — never a partial manifest.
func (c *CaptureService) Capture(ctx context.Context, scope, owner string, epoch int64) (*captureRow, error) {
	if scope == "" {
		return nil, fmt.Errorf("%w: empty scope", ErrCaptureRefused)
	}
	if scope == ".trash" {
		// The volume trash tree is never a capturable scope even though
		// it resolves as a real edge at parent 1.
		return nil, fmt.Errorf("%w: .trash is not a capturable scope", ErrCaptureRefused)
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	tx, err := c.meta.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	// --- schema gate -------------------------------------------------
	var missing int
	if err := tx.QueryRow(ctx, `
		SELECT count(*) FROM (
			SELECT table_name, column_name FROM information_schema.columns
			WHERE table_name = ANY($1::text[])) have
		RIGHT JOIN (
			SELECT unnest($2::text[]) AS t, unnest($3::text[]) AS c) want
		ON have.table_name = want.t AND have.column_name = want.c
		WHERE have.table_name IS NULL`,
		tableNames(requiredColumns), colTables(requiredColumns), colNames(requiredColumns),
	).Scan(&missing); err != nil {
		return nil, fmt.Errorf("schema gate: %w", err)
	}
	if missing > 0 {
		return nil, fmt.Errorf("%w: metadata schema missing %d required columns", ErrCaptureRefused, missing)
	}

	// --- format gate -------------------------------------------------
	var formatJSON string
	err = tx.QueryRow(ctx,
		`SELECT value FROM jfs_setting WHERE name='format'`).Scan(&formatJSON)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: no format setting — not a JuiceFS metadata engine", ErrCaptureRefused)
	}
	if err != nil {
		return nil, err
	}
	var raw rawFormat
	if err := json.Unmarshal([]byte(formatJSON), &raw); err != nil {
		return nil, fmt.Errorf("%w: unparsable format: %v", ErrCaptureRefused, err)
	}
	gated, err := gateFormat(raw)
	if err != nil {
		return nil, err
	}

	// --- explicit existing scope anchor ------------------------------
	var anchorIno uint64
	var anchorType uint8
	err = tx.QueryRow(ctx,
		`SELECT e.inode, e.type FROM jfs_edge e
		 WHERE e.parent = 1 AND e.name = $1`, []byte(scope)).Scan(&anchorIno, &anchorType)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: scope %q has no volume-root anchor", ErrCaptureRefused, scope)
	}
	if err != nil {
		return nil, err
	}
	if anchorType != jfsTypeDir {
		return nil, fmt.Errorf("%w: scope anchor is not a directory (type %d)", ErrCaptureRefused, anchorType)
	}

	// --- complete namespace walk (single RR snapshot) ----------------
	entries, err := c.walk(ctx, tx, scope, anchorIno)
	if err != nil {
		return nil, err
	}
	if int64(len(entries)) > c.cfg.MaxEntries {
		return nil, fmt.Errorf("%w: namespace %d entries exceeds bound %d",
			ErrCaptureRefused, len(entries), c.cfg.MaxEntries)
	}

	// --- ordered slice mappings for every file -----------------------
	fileInos := make([]uint64, 0, len(entries))
	for _, e := range entries {
		if e.NodeType == jfsTypeFile && e.Length > 0 {
			fileInos = append(fileInos, e.Ino)
		}
	}
	slices, err := c.chunkRows(ctx, tx, fileInos)
	if err != nil {
		return nil, err
	}
	linkTargets, err := c.symlinkRows(ctx, tx, entries)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("capture tx commit: %w", err)
	}

	// --- assemble + persist ------------------------------------------
	var b [16]byte
	rand.Read(b[:])
	capID := "cap-" + hex.EncodeToString(b[:])
	now := c.now()
	var unsupported int64
	for i := range entries {
		entries[i].CaptureID = capID
		entries[i].Seq = int64(i)
		if t, ok := linkTargets[entries[i].Ino]; ok {
			entries[i].Link = t
		}
		if entries[i].NodeType == jfsTypeFile && entries[i].Nlink > 1 {
			entries[i].LinkGroup = linkGroup(capID, entries[i].Ino)
		}
		if !supportedNodeType(entries[i].NodeType) {
			unsupported++
		}
		entries[i].Supported = supportedNodeType(entries[i].NodeType)
	}
	for i := range slices {
		slices[i].CaptureID = capID
	}
	// Per-file resolved-mapping identity (no object fetch — pure shape).
	for i := range entries {
		if entries[i].NodeType == jfsTypeFile {
			entries[i].MapSHA = resolvedMapSHA(slicesForIno(slices, entries[i].Ino), entries[i].Length)
		}
	}
	row := &captureRow{
		CaptureID: capID, Scope: scope, Owner: owner, OwnerEpoch: epoch,
		Volume: gated.VolumeUUID, AnchorIno: anchorIno,
		ScopeID: scopeID(gated.VolumeUUID, anchorIno),
		Format:  gated, Status: captureStatusActive,
		EntryCount: int64(len(entries)), Unsupported: unsupported,
		CreatedAt: now, ExpiresAt: now.Add(c.cfg.TTL),
	}
	row.ManifestSHA = manifestSHA(row, entries, slices)
	if err := c.persist.saveCapture(ctx, row, entries, slices); err != nil {
		return nil, fmt.Errorf("persist capture: %w", err)
	}
	return row, nil
}

// walk enumerates the scope subtree inside the RR transaction. Every
// supported path lands in the manifest — including empty dirs/files,
// symlink rows, both names of hardlinks, and unsupported-type entries
// flagged (never silently dropped).
func (c *CaptureService) walk(ctx context.Context, tx pgx.Tx, scope string, anchorIno uint64) ([]captureEntryRow, error) {
	// Reserved predicates filtered inside the CTE: excluding the
	// directory row also prunes its whole subtree, and no orphan paths
	// can reach the manifest. Only the two exact prefixes — .filesv-tmp-
	// (12 bytes) and .filesv-op- (11 bytes); .filesv-notes survives.
	rows, err := tx.Query(ctx, `
		WITH RECURSIVE t AS (
			SELECT e.inode, e.parent, e.name, e.type, ''::bytea AS path, 0 AS depth
			  FROM jfs_edge e WHERE e.parent = 1 AND e.name = $1
			UNION ALL
			SELECT c.inode, c.parent, c.name, c.type,
			       CASE WHEN t.path = ''::bytea THEN c.name
			            ELSE t.path || '/'::bytea || c.name END,
			       t.depth + 1
			  FROM jfs_edge c JOIN t ON c.parent = t.inode
			 WHERE t.type = 2 AND t.depth < $2
			   AND substring(c.name from 1 for 12) <> '.filesv-tmp-'::bytea
			   AND substring(c.name from 1 for 11) <> '.filesv-op-'::bytea
		)
		SELECT t.inode, t.parent, t.name, t.path, t.depth,
		       n.type, n.mode, n.uid, n.gid, n.mtime, n.mtimensec,
		       n.ctime, n.ctimensec, n.nlink, n.length
		  FROM t LEFT JOIN jfs_node n ON n.inode = t.inode
		 ORDER BY t.path`, []byte(scope), maxWalkDepth)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []captureEntryRow
	for rows.Next() {
		var e captureEntryRow
		var depth int
		var ntype *uint8
		var mode *uint16
		var uid, gid, nlink *uint32
		var mtime, mtimeNS, ctime, ctimeNS *int64
		var length *uint64
		if err := rows.Scan(&e.Ino, &e.ParentIno, &e.Name, &e.Path, &depth,
			&ntype, &mode, &uid, &gid, &mtime, &mtimeNS,
			&ctime, &ctimeNS, &nlink, &length); err != nil {
			return nil, err
		}
		if ntype == nil || mode == nil || uid == nil || gid == nil ||
			mtime == nil || mtimeNS == nil || ctime == nil || ctimeNS == nil ||
			nlink == nil || length == nil {
			return nil, fmt.Errorf("%w: edge inode %d has no node row", ErrCaptureRefused, e.Ino)
		}
		e.NodeType = *ntype
		e.Mode = *mode
		e.UID, e.GID = *uid, *gid
		e.Nlink = *nlink
		e.Length = *length
		e.MtimeNS = *mtime*1e9 + int64(*mtimeNS)
		e.CtimeNS = *ctime*1e9 + int64(*ctimeNS)
		if privateName(e.Name) {
			continue // belt-and-suspenders; the CTE already prunes these
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(out) == 0 || out[0].Ino != anchorIno {
		return nil, fmt.Errorf("%w: scope anchor vanished during walk", ErrCaptureRefused)
	}
	return out, nil
}

// chunkRows loads and validates the ordered slice mappings of every
// file inode. Validation happens HERE (at capture): a malformed bytea
// refuses the whole capture rather than producing a manifest whose reads
// fail later.
func (c *CaptureService) chunkRows(ctx context.Context, tx pgx.Tx,
	inos []uint64) ([]captureSliceRow, error) {
	if len(inos) == 0 {
		return nil, nil
	}
	rows, err := tx.Query(ctx,
		`SELECT inode, indx, slices FROM jfs_chunk WHERE inode = ANY($1)
		 ORDER BY inode, indx`, inos)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []captureSliceRow
	for rows.Next() {
		var ino uint64
		var indx uint32
		var buf []byte
		if err := rows.Scan(&ino, &indx, &buf); err != nil {
			return nil, err
		}
		recs, err := parseSliceRecs(buf)
		if err != nil {
			return nil, err
		}
		for i, r := range recs {
			// A data record entirely past captured EOF is legal history
			// (truncate) — keep it durable; resolution clips it.
			out = append(out, captureSliceRow{
				Ino: ino, Indx: indx, Seq: i,
				SliceID: r.id, Pos: r.pos, Size: r.size, Off: r.off, Len: r.length,
			})
		}
	}
	return out, rows.Err()
}

func (c *CaptureService) symlinkRows(ctx context.Context, tx pgx.Tx,
	entries []captureEntryRow) (map[uint64][]byte, error) {
	var inos []uint64
	for _, e := range entries {
		if e.NodeType == jfsTypeSymlink {
			inos = append(inos, e.Ino)
		}
	}
	if len(inos) == 0 {
		return nil, nil
	}
	out := map[uint64][]byte{}
	rows, err := tx.Query(ctx,
		`SELECT inode, target FROM jfs_symlink WHERE inode = ANY($1)`, inos)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var ino uint64
		var tgt []byte
		if err := rows.Scan(&ino, &tgt); err != nil {
			return nil, err
		}
		out[ino] = tgt
	}
	// A symlink entry with no target row is corrupt metadata — refuse.
	for _, ino := range inos {
		if _, ok := out[ino]; !ok {
			return nil, fmt.Errorf("%w: symlink inode %d has no target", ErrCaptureRefused, ino)
		}
	}
	return out, rows.Err()
}

func slicesForIno(slices []captureSliceRow, ino uint64) []captureSliceRow {
	var out []captureSliceRow
	for _, s := range slices {
		if s.Ino == ino {
			out = append(out, s)
		}
	}
	return out
}

// resolvedMapSHA hashes the RESOLVED range view of one file — the shape
// identity a mover compares without seeing slice internals.
func resolvedMapSHA(slices []captureSliceRow, fileLen uint64) string {
	type ck struct {
		indx uint32
		recs []sliceRec
	}
	var order []uint32
	byIndx := map[uint32][]sliceRec{}
	for _, s := range slices {
		if _, ok := byIndx[s.Indx]; !ok {
			order = append(order, s.Indx)
		}
		byIndx[s.Indx] = append(byIndx[s.Indx], sliceRec{
			pos: s.Pos, id: s.SliceID, size: s.Size, off: s.Off, length: s.Len,
		})
	}
	sort.Slice(order, func(i, j int) bool { return order[i] < order[j] })
	h := sha256.New()
	for _, indx := range order {
		for _, r := range resolveChunk(byIndx[indx], fileLen, uint64(indx)*chunkSize) {
			fmt.Fprintf(h, "%x|%x|%x|%x|%x\n", indx, r.pos, r.length, r.id, r.off)
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}

// manifestSHA binds the capture's durable identity: gated format,
// anchor, and every manifest row + ordered raw slice record. The hash
// covers the durable rows themselves, so what is verified is exactly
// what was persisted.
func manifestSHA(c *captureRow, entries []captureEntryRow, slices []captureSliceRow) string {
	h := sha256.New()
	fmt.Fprintf(h, "cap|%s|%s|%x\n", c.Volume, c.Scope, c.AnchorIno)
	for _, e := range entries {
		fmt.Fprintf(h, "e|%d|%x|%x|%d|%d|%d|%d|%d|%d|%d|%d|%x\n",
			e.Seq, e.Path, e.Name, e.Ino, e.NodeType, e.Mode, e.UID, e.GID,
			e.Nlink, e.Length, e.MtimeNS, e.Link)
	}
	for _, s := range slices {
		fmt.Fprintf(h, "s|%d|%d|%d|%d|%d|%d|%d|%d\n",
			s.Ino, s.Indx, s.Seq, s.SliceID, s.Pos, s.Size, s.Off, s.Len)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// --- read-side --------------------------------------------------------

// live returns the capture row or maps not-found/released/expired to
// the read-path errors. Expiry is lazy: past-expiry rows are marked and
// answered ErrCaptureGone.
func (c *CaptureService) live(ctx context.Context, id string) (*captureRow, error) {
	row, err := c.persist.getCapture(ctx, id)
	if errors.Is(err, ErrCaptureNotFound) {
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	switch row.Status {
	case captureStatusReleased, captureStatusExpired:
		return nil, ErrCaptureGone
	}
	if c.now().After(row.ExpiresAt) {
		c.persist.setCaptureStatus(ctx, id, captureStatusExpired)
		return nil, ErrCaptureGone
	}
	return row, nil
}

// Meta answers the capture's identity/lifecycle for an authorized
// caller. Opaque ids only — no inodes, slices, keys, or credentials.
func (c *CaptureService) Meta(ctx context.Context, id string) (*captureRow, error) {
	return c.live(ctx, id)
}

// Entries streams manifest rows after cursor. Scope-bound: callers must
// already hold scope authority (checked at the HTTP layer).
func (c *CaptureService) Entries(ctx context.Context, id string, afterSeq, limit int64) ([]captureEntryRow, error) {
	if _, err := c.live(ctx, id); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 1000 {
		limit = 500
	}
	return c.persist.entryStream(ctx, id, afterSeq, limit)
}

// Stream writes the captured bytes of manifest row seq — bounded memory,
// resolved ranges only, captured objects only. Missing/short objects
// answer ErrCapturePending; non-file entries answer ErrCaptureRefused.
func (c *CaptureService) Stream(ctx context.Context, id string, seq int64,
	off, n uint64, w io.Writer) (uint64, error) {
	row, err := c.live(ctx, id)
	if err != nil {
		return 0, err
	}
	e, err := c.persist.getEntry(ctx, id, seq)
	if err != nil {
		return 0, err
	}
	if e.NodeType != jfsTypeFile {
		return 0, fmt.Errorf("%w: entry %d is not a file", ErrCaptureRefused, seq)
	}
	end := e.Length
	if off >= end {
		return 0, nil
	}
	if n == 0 || off+n > end {
		n = end - off
	}
	slices, err := c.persist.slicesFor(ctx, id, e.Ino)
	if err != nil {
		return 0, err
	}
	byIndx := map[uint32][]sliceRec{}
	var order []uint32
	for _, s := range slices {
		if _, ok := byIndx[s.Indx]; !ok {
			order = append(order, s.Indx)
		}
		byIndx[s.Indx] = append(byIndx[s.Indx], sliceRec{
			pos: s.Pos, id: s.SliceID, size: s.Size, off: s.Off, length: s.Len,
		})
	}
	sort.Slice(order, func(i, j int) bool { return order[i] < order[j] })
	// Walk resolved segments; only segments intersecting [off, off+n)
	// cost object fetches — obsolete history is never fetched.
	want := off + n
	cur := off
	for _, indx := range order {
		chunkBase := uint64(indx) * chunkSize
		if chunkBase+chunkSize <= cur {
			continue
		}
		if chunkBase >= want {
			break
		}
		for _, seg := range resolveChunk(byIndx[indx], e.Length, chunkBase) {
			segLo := chunkBase + seg.pos
			segHi := segLo + seg.length
			if segHi <= cur {
				continue
			}
			if segLo > cur {
				// implicit hole between resolved segments
				gapEnd := segLo
				if gapEnd > want {
					gapEnd = want
				}
				if err := writeZeros(w, int64(gapEnd-cur)); err != nil {
					return 0, err
				}
				cur = gapEnd
			}
			if cur >= want {
				break
			}
			readEnd := segHi
			if readEnd > want {
				readEnd = want
			}
			if err := streamRange(ctx, c.objs, row.Format, seg, chunkBase, cur, readEnd-cur, w); err != nil {
				return 0, err
			}
			cur = readEnd
		}
	}
	if cur < want {
		if err := writeZeros(w, int64(want-cur)); err != nil {
			return 0, err
		}
	}
	return n, nil
}

// streamProbe validates a read request before headers: capture live,
// entry exists and is a file. Returns the entry length.
func (c *CaptureService) streamProbe(ctx context.Context, id string, seq int64) (uint64, error) {
	if _, err := c.live(ctx, id); err != nil {
		return 0, err
	}
	e, err := c.persist.getEntry(ctx, id, seq)
	if err != nil {
		return 0, err
	}
	if e.NodeType != jfsTypeFile {
		return 0, fmt.Errorf("%w: entry %d is not a file", ErrCaptureRefused, seq)
	}
	return e.Length, nil
}

// Release marks the capture released. Rows remain as evidence; reads
// close with ErrCaptureGone. Retake is a fresh Capture — a released or
// pending manifest never silently retargets.
func (c *CaptureService) Release(ctx context.Context, id string) error {
	row, err := c.persist.getCapture(ctx, id)
	if err != nil {
		return err
	}
	if row.Status == captureStatusReleased {
		return nil // idempotent
	}
	ok, err := c.persist.setCaptureStatus(ctx, id, captureStatusReleased)
	if err != nil {
		return err
	}
	if !ok {
		return ErrCaptureNotFound
	}
	return nil
}

func tableNames(cols [][2]string) []string {
	seen := map[string]bool{}
	var out []string
	for _, c := range cols {
		if !seen[c[0]] {
			seen[c[0]] = true
			out = append(out, c[0])
		}
	}
	return out
}
func colTables(cols [][2]string) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = c[0]
	}
	return out
}
func colNames(cols [][2]string) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = c[1]
	}
	return out
}
