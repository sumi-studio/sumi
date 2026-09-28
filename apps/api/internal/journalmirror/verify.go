package journalmirror

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// InitializeEmpty declares an empty mirror directory for an installation
// that has no journals yet, so a restore-only host can start. It never
// changes an initialized directory: created is false and lineage is the
// existing one. by is recorded for operators.
//
// Run it only where no host holds journals for this database: a host that
// does must adopt them (AttachAdopt) instead. If one is run by mistake, that
// host's later adoption or restore refuses its unknown local files and
// changes nothing.
func InitializeEmpty(ctx context.Context, pool *pgxpool.Pool, logical, by string) (created bool, lineage string, err error) {
	if !dirNameRe.MatchString(logical) {
		return false, "", fmt.Errorf("invalid journal mirror name %q", logical)
	}
	if by == "" {
		by = "unknown"
	}
	tag, err := pool.Exec(ctx, `
		INSERT INTO api_journal_mirror_dirs (dir, lineage, initialized_how, initialized_by, initialized_files, initialized_bytes)
		VALUES ($1, $2, 'empty', $3, 0, 0)
		ON CONFLICT (dir) DO NOTHING`, logical, uuid.NewString(), by)
	if err != nil {
		return false, "", fmt.Errorf("initialize journal mirror %q: %w", logical, err)
	}
	if err := pool.QueryRow(ctx, `SELECT lineage::text FROM api_journal_mirror_dirs WHERE dir = $1`, logical).Scan(&lineage); err != nil {
		return false, "", err
	}
	return tag.RowsAffected() == 1, lineage, nil
}

// VerifyReport compares a directory with a mirror without changing either.
type VerifyReport struct {
	Dir            string   `json:"dir"`
	Lineage        string   `json:"lineage"`
	InitializedHow string   `json:"initialized_how"`
	LocalLineage   string   `json:"local_lineage,omitempty"` // from the directory marker
	Files          int      `json:"files"`
	Bytes          int64    `json:"bytes"`
	Missing        []string `json:"missing,omitempty"`   // mirrored, absent locally
	Extra          []string `json:"extra,omitempty"`     // local, not mirrored
	Different      []string `json:"different,omitempty"` // present in both, bytes differ
}

// Equal reports whether the directory holds exactly the mirror's files.
func (r VerifyReport) Equal() bool {
	return len(r.Missing) == 0 && len(r.Extra) == 0 && len(r.Different) == 0 &&
		(r.LocalLineage == "" || r.LocalLineage == r.Lineage)
}

// Verify compares dir with the mirror named logical in one consistent
// snapshot, streaming one chunk at a time.
func Verify(ctx context.Context, pool *pgxpool.Pool, logical, dir string) (VerifyReport, error) {
	report := VerifyReport{Dir: logical}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly, IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return report, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	err = tx.QueryRow(ctx, `SELECT lineage::text, initialized_how FROM api_journal_mirror_dirs WHERE dir = $1`, logical).
		Scan(&report.Lineage, &report.InitializedHow)
	if errors.Is(err, pgx.ErrNoRows) {
		return report, fmt.Errorf("%w: %q", ErrNotInitialized, logical)
	}
	if err != nil {
		return report, err
	}
	remote, err := readRemoteMeta(ctx, tx, logical)
	if err != nil {
		return report, err
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return report, err
	}
	if _, err := os.Stat(abs); err != nil {
		return report, err
	}
	local, err := scanLocal(abs, func(string, ...any) {})
	if err != nil {
		return report, err
	}
	report.LocalLineage = local.lineage
	for _, name := range sortedKeys(remote) {
		pg := remote[name]
		report.Files++
		report.Bytes += pg.size
		size, ok := local.files[name]
		if !ok {
			report.Missing = append(report.Missing, name)
			continue
		}
		same := size == pg.size
		if same {
			if same, err = sameContent(ctx, tx, logical, name, pg.size, filepath.Join(abs, name)); err != nil {
				return report, err
			}
		}
		if !same {
			report.Different = append(report.Different, name)
		}
	}
	for _, name := range sortedKeys(local.files) {
		if _, ok := remote[name]; !ok {
			report.Extra = append(report.Extra, name)
		}
	}
	return report, nil
}
