// jfsseed.go is the capture fixture's synthetic JuiceFS volume seeder.
// It writes jfs_edge/jfs_node/jfs_chunk/jfs_symlink/jfs_setting rows and
// the file:// objects they map to — the same tables and object layout
// the immutable-capture service decodes. This is a FIXTURE: it models a
// volume's durable state for owned journey tests; it is not a JuiceFS
// write path and never touches production data.
//
// Usage:
//
//	cloudjourney jfsseed -dsn postgres://... -objroot /path -scope <scope> <op> [args]
//
// ops: init | put <rel> [-size N|-data str|-file f] | mkdir <rel> |
// symlink <rel> <target> | hardlink <rel> <existing-rel> | fifo <rel> |
// rm <rel> | mv <from> <to> | rmobj <rel> | scope-drop | scope-mkdir |
// sha <rel>
package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	jfsFile    = 1
	jfsDir     = 2
	jfsSymlink = 3
	jfsFIFO    = 4
	blockBytes = 4 << 20 // 4 MiB — format BlockSize 4096 (KiB)
)

type seeder struct {
	pool    *pgxpool.Pool
	objroot string
	scope   string
	ino     uint64 // next inode
	slice   uint64 // next slice id
}

func jfsSeedMain(args []string) {
	fs := flag.NewFlagSet("jfsseed", flag.ExitOnError)
	dsn := fs.String("dsn", "", "JuiceFS metadata DSN")
	objroot := fs.String("objroot", "", "file:// object root")
	scope := fs.String("scope", "", "scope (volume-root edge name)")
	_ = fs.Parse(args)
	if *dsn == "" || *objroot == "" || *scope == "" {
		log.Fatal("jfsseed: -dsn, -objroot and -scope are required")
	}
	op := fs.Arg(0)
	// Op-level flags come AFTER the op name — the global FlagSet stops at
	// the first positional, so they need their own parse over the rest.
	opfs := flag.NewFlagSet("jfsseed "+op, flag.ExitOnError)
	size := opfs.Int64("size", -1, "put: deterministic content length")
	data := opfs.String("data", "", "put: literal content")
	file := opfs.String("file", "", "put: content from a local file")
	_ = opfs.Parse(fs.Args()[1:])
	pool, err := pgxpool.New(context.Background(), *dsn)
	if err != nil {
		log.Fatalf("jfsseed: %v", err)
	}
	defer pool.Close()
	s := &seeder{pool: pool, objroot: *objroot, scope: *scope}
	ctx := context.Background()
	switch op {
	case "init":
		err = s.initSchema(ctx)
	case "put":
		var content []byte
		switch {
		case *size >= 0:
			content = make([]byte, *size)
			for i := range content {
				content[i] = byte(i * 31)
			}
		case *file != "":
			content, err = os.ReadFile(*file)
			if err != nil {
				log.Fatalf("jfsseed: %v", err)
			}
		default:
			content = []byte(*data)
		}
		err = s.put(ctx, opfs.Arg(0), content)
	case "mkdir":
		err = s.mkdirAll(ctx, opfs.Arg(0))
	case "symlink":
		err = s.symlink(ctx, opfs.Arg(0), opfs.Arg(1))
	case "hardlink":
		err = s.hardlink(ctx, opfs.Arg(0), opfs.Arg(1))
	case "fifo":
		err = s.special(ctx, opfs.Arg(0), jfsFIFO)
	case "rm":
		err = s.rm(ctx, opfs.Arg(0))
	case "mv":
		err = s.mv(ctx, opfs.Arg(0), opfs.Arg(1))
	case "rmobj":
		err = s.rmobj(ctx, opfs.Arg(0))
	case "scope-drop":
		err = s.scopeEdge(ctx, false)
	case "scope-mkdir":
		err = s.scopeEdge(ctx, true)
	case "sha":
		var sum string
		sum, err = s.sha(ctx, opfs.Arg(0))
		if err == nil {
			fmt.Println(sum)
		}
	default:
		log.Fatalf("jfsseed: unknown op %q", op)
	}
	if err != nil {
		log.Fatalf("jfsseed %s: %v", op, err)
	}
}

func (s *seeder) initSchema(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS jfs_edge (parent bigint NOT NULL, name bytea NOT NULL, inode bigint NOT NULL, type int NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS jfs_edge_p ON jfs_edge (parent, name)`,
		`CREATE TABLE IF NOT EXISTS jfs_node (inode bigint PRIMARY KEY, type int NOT NULL, mode int NOT NULL, uid int NOT NULL, gid int NOT NULL,
			mtime bigint NOT NULL, mtimensec bigint NOT NULL, ctime bigint NOT NULL, ctimensec bigint NOT NULL,
			nlink int NOT NULL, length bigint NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS jfs_chunk (inode bigint NOT NULL, indx int NOT NULL, slices bytea NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS jfs_symlink (inode bigint PRIMARY KEY, target bytea NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS jfs_setting (name text PRIMARY KEY, value text NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS jfs_seed_seq (name text PRIMARY KEY, val bigint NOT NULL)`,
	}
	for _, q := range stmts {
		if _, err := s.pool.Exec(ctx, q); err != nil {
			return err
		}
	}
	format := map[string]any{
		"UUID":    "fixture-vol-0000-0000-000000000001",
		"Name":    filepath.Base(s.objroot),
		"Storage": "file", "Bucket": filepath.Dir(s.objroot), "BlockSize": 4096,
		"Compression": "none", "HashPrefix": false, "TrashDays": 0,
		"MetaVersion": 1,
	}
	raw, _ := json.Marshal(format)
	if _, err := s.pool.Exec(ctx, `INSERT INTO jfs_setting (name, value) VALUES ('format', $1)
		ON CONFLICT (name) DO UPDATE SET value = $1`, string(raw)); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(s.objroot, "chunks"), 0o755); err != nil {
		return err
	}
	// Root node (inode 1) + scope anchor dir.
	if err := s.node(ctx, 1, jfsDir, 0o755, 0); err != nil {
		return err
	}
	return s.scopeEdge(ctx, true)
}

func (s *seeder) alloc(ctx context.Context, name string) (uint64, error) {
	var v int64
	err := s.pool.QueryRow(ctx, `INSERT INTO jfs_seed_seq (name, val) VALUES ($1, 2)
		ON CONFLICT (name) DO UPDATE SET val = jfs_seed_seq.val + 1
		RETURNING val`, name).Scan(&v)
	return uint64(v), err
}

func (s *seeder) node(ctx context.Context, ino uint64, typ int, mode, length uint64) error {
	now := time.Now().Unix()
	nlink := 1
	if typ == jfsDir {
		nlink = 2
	}
	_, err := s.pool.Exec(ctx, `INSERT INTO jfs_node
		(inode, type, mode, uid, gid, mtime, mtimensec, ctime, ctimensec, nlink, length)
		VALUES ($1,$2,$3,0,0,$4,0,$4,0,$5,$6)
		ON CONFLICT (inode) DO NOTHING`, int64(ino), typ, mode, now, nlink, int64(length))
	return err
}

// scopeEdge drops or recreates the scope's volume-root anchor — a
// recreate lands a NEW inode, so a later capture resolves a different
// scope_id (the changed-anchor refusal case).
func (s *seeder) scopeEdge(ctx context.Context, create bool) error {
	if !create {
		_, err := s.pool.Exec(ctx, `DELETE FROM jfs_edge WHERE parent = 1 AND name = $1`, []byte(s.scope))
		return err
	}
	var existing int64
	err := s.pool.QueryRow(ctx, `SELECT inode FROM jfs_edge
		WHERE parent = 1 AND name = $1`, []byte(s.scope)).Scan(&existing)
	if err == nil {
		return nil // anchor already stands — init is idempotent
	}
	if err != pgx.ErrNoRows {
		return err
	}
	ino, err := s.alloc(ctx, "ino")
	if err != nil {
		return err
	}
	if err := s.node(ctx, ino, jfsDir, 0o755, 0); err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `INSERT INTO jfs_edge (parent, name, inode, type)
		VALUES (1, $1, $2, $3)`, []byte(s.scope), int64(ino), jfsDir)
	return err
}

// resolveIno walks the scope tree to rel's inode, creating missing
// intermediate directories when create is set.
func (s *seeder) resolveIno(ctx context.Context, rel string, create bool) (uint64, uint8, error) {
	var anchorIno int64
	err := s.pool.QueryRow(ctx, `SELECT inode FROM jfs_edge WHERE parent = 1 AND name = $1`,
		[]byte(s.scope)).Scan(&anchorIno)
	if err != nil {
		return 0, 0, fmt.Errorf("scope anchor: %w", err)
	}
	cur := uint64(anchorIno)
	parts := strings.Split(strings.Trim(rel, "/"), "/")
	for i, name := range parts {
		var ino int64
		var typ uint8
		err := s.pool.QueryRow(ctx, `SELECT inode, type FROM jfs_edge
			WHERE parent = $1 AND name = $2`, int64(cur), []byte(name)).Scan(&ino, &typ)
		if err == pgx.ErrNoRows {
			if !create {
				return 0, 0, fmt.Errorf("%s: no such entry", rel)
			}
			nino, aerr := s.alloc(ctx, "ino")
			if aerr != nil {
				return 0, 0, aerr
			}
			if err := s.node(ctx, nino, jfsDir, 0o755, 0); err != nil {
				return 0, 0, err
			}
			if _, err := s.pool.Exec(ctx, `INSERT INTO jfs_edge (parent, name, inode, type)
				VALUES ($1,$2,$3,$4)`, int64(cur), []byte(name), int64(nino), jfsDir); err != nil {
				return 0, 0, err
			}
			if i == len(parts)-1 {
				return nino, jfsDir, nil
			}
			cur = nino
			continue
		}
		if err != nil {
			return 0, 0, err
		}
		if i == len(parts)-1 {
			return uint64(ino), typ, nil
		}
		cur = uint64(ino)
	}
	return cur, jfsDir, nil
}

func (s *seeder) leaf(ctx context.Context, rel string) (parent uint64, name string, err error) {
	dir, base := filepath.Split(strings.Trim(rel, "/"))
	if dir == "" {
		var anchorIno int64
		if err := s.pool.QueryRow(ctx, `SELECT inode FROM jfs_edge
			WHERE parent = 1 AND name = $1`, []byte(s.scope)).Scan(&anchorIno); err != nil {
			return 0, "", fmt.Errorf("scope anchor: %w", err)
		}
		return uint64(anchorIno), base, nil
	}
	pino, _, err := s.resolveIno(ctx, strings.TrimSuffix(dir, "/"), true)
	return pino, base, err
}

// sliceRec packs one 24-byte JuiceFS slice record (big-endian
// pos,id,size,off,len).
func sliceRec(pos uint32, id uint64, size, off, length uint32) []byte {
	b := make([]byte, 24)
	binary.BigEndian.PutUint32(b[0:], pos)
	binary.BigEndian.PutUint64(b[4:], id)
	binary.BigEndian.PutUint32(b[12:], size)
	binary.BigEndian.PutUint32(b[16:], off)
	binary.BigEndian.PutUint32(b[20:], length)
	return b
}

func (s *seeder) objectPath(sliceID uint64, indx, blockSize int) string {
	return filepath.Join(s.objroot, "chunks",
		fmt.Sprint(sliceID/1000/1000), fmt.Sprint(sliceID/1000),
		fmt.Sprintf("%d_%d_%d", sliceID, indx, blockSize))
}

// writeObjects splits content into blockBytes objects under the slice id.
func (s *seeder) writeObjects(sliceID uint64, content []byte) error {
	for bi, off := 0, 0; off < len(content) || (bi == 0 && len(content) == 0); bi++ {
		end := off + blockBytes
		if end > len(content) {
			end = len(content)
		}
		bsize := end - off
		p := s.objectPath(sliceID, bi, bsize)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(p, content[off:end], 0o644); err != nil {
			return err
		}
		off = end
		if len(content) == 0 || off >= len(content) {
			break
		}
	}
	return nil
}

// put writes/rewrites a file: a fresh inode+slice per write models a
// real overwrite (the old object stays — a snapshot keeps its own
// pointers).
func (s *seeder) put(ctx context.Context, rel string, content []byte) error {
	parent, name, err := s.leaf(ctx, rel)
	if err != nil {
		return err
	}
	ino, err := s.alloc(ctx, "ino")
	if err != nil {
		return err
	}
	var slices []byte
	if len(content) > 0 {
		sid, err := s.alloc(ctx, "slice")
		if err != nil {
			return err
		}
		if err := s.writeObjects(sid, content); err != nil {
			return err
		}
		slices = sliceRec(0, sid, uint32(len(content)), 0, uint32(len(content)))
	}
	if err := s.node(ctx, ino, jfsFile, 0o644, uint64(len(content))); err != nil {
		return err
	}
	if len(content) > 0 {
		if _, err := s.pool.Exec(ctx, `INSERT INTO jfs_chunk (inode, indx, slices)
			VALUES ($1, 0, $2)`, int64(ino), slices); err != nil {
			return err
		}
	}
	return s.replaceEdge(ctx, parent, name, ino, jfsFile)
}

// replaceEdge deletes any existing (parent,name) edge then inserts the
// new one — pgx prepares each Exec separately, so these stay two
// statements.
func (s *seeder) replaceEdge(ctx context.Context, parent uint64, name string, ino uint64, typ int) error {
	if _, err := s.pool.Exec(ctx, `DELETE FROM jfs_edge WHERE parent = $1 AND name = $2`,
		int64(parent), []byte(name)); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx, `INSERT INTO jfs_edge (parent, name, inode, type)
		VALUES ($1,$2,$3,$4)`, int64(parent), []byte(name), int64(ino), typ)
	return err
}

func (s *seeder) mkdirAll(ctx context.Context, rel string) error {
	parts := strings.Split(strings.Trim(rel, "/"), "/")
	cur := ""
	for _, name := range parts {
		cur = strings.TrimPrefix(cur+"/"+name, "/")
		parent, _, err := s.leaf(ctx, cur)
		if err != nil {
			return err
		}
		var ino int64
		err = s.pool.QueryRow(ctx, `SELECT inode FROM jfs_edge
			WHERE parent = $1 AND name = $2`, int64(parent), []byte(name)).Scan(&ino)
		if err == pgx.ErrNoRows {
			nino, aerr := s.alloc(ctx, "ino")
			if aerr != nil {
				return aerr
			}
			if err := s.node(ctx, nino, jfsDir, 0o755, 0); err != nil {
				return err
			}
			if _, err := s.pool.Exec(ctx, `INSERT INTO jfs_edge (parent, name, inode, type)
				VALUES ($1,$2,$3,$4)`, int64(parent), []byte(name), int64(nino), jfsDir); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *seeder) symlink(ctx context.Context, rel, target string) error {
	parent, name, err := s.leaf(ctx, rel)
	if err != nil {
		return err
	}
	ino, err := s.alloc(ctx, "ino")
	if err != nil {
		return err
	}
	if err := s.node(ctx, ino, jfsSymlink, 0o777, uint64(len(target))); err != nil {
		return err
	}
	if _, err := s.pool.Exec(ctx, `INSERT INTO jfs_symlink (inode, target)
		VALUES ($1, $2)`, int64(ino), []byte(target)); err != nil {
		return err
	}
	return s.replaceEdge(ctx, parent, name, ino, jfsSymlink)
}

// hardlink adds a second name for an existing inode and bumps nlink —
// both names land in the manifest with the same link_group.
func (s *seeder) hardlink(ctx context.Context, rel, existing string) error {
	ino, typ, err := s.resolveIno(ctx, existing, false)
	if err != nil {
		return err
	}
	parent, name, err := s.leaf(ctx, rel)
	if err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `DELETE FROM jfs_edge WHERE parent = $1 AND name = $2`,
		int64(parent), []byte(name)); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO jfs_edge (parent, name, inode, type)
		VALUES ($1,$2,$3,$4)`, int64(parent), []byte(name), int64(ino), int(typ)); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE jfs_node SET nlink = nlink + 1
		WHERE inode = $1`, int64(ino)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *seeder) special(ctx context.Context, rel string, typ int) error {
	parent, name, err := s.leaf(ctx, rel)
	if err != nil {
		return err
	}
	ino, err := s.alloc(ctx, "ino")
	if err != nil {
		return err
	}
	if err := s.node(ctx, ino, typ, 0o644, 0); err != nil {
		return err
	}
	return s.replaceEdge(ctx, parent, name, ino, typ)
}

// rm removes an edge + its node (files/symlinks/specials; a non-empty
// dir refuses — the fixture never deletes a subtree silently).
func (s *seeder) rm(ctx context.Context, rel string) error {
	ino, typ, err := s.resolveIno(ctx, rel, false)
	if err != nil {
		return err
	}
	if typ == jfsDir {
		var n int
		if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM jfs_edge
			WHERE parent = $1`, int64(ino)).Scan(&n); err != nil {
			return err
		}
		if n > 0 {
			return fmt.Errorf("%s: directory not empty", rel)
		}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `DELETE FROM jfs_edge WHERE inode = $1`, int64(ino)); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM jfs_node WHERE inode = $1`, int64(ino)); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM jfs_chunk WHERE inode = $1`, int64(ino)); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM jfs_symlink WHERE inode = $1`, int64(ino)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// mv renames within or across directories — the inode survives, so a
// manifest keeps the same object identity under a new name.
func (s *seeder) mv(ctx context.Context, from, to string) error {
	ino, typ, err := s.resolveIno(ctx, from, false)
	if err != nil {
		return err
	}
	srcParent, srcName, err := s.leaf(ctx, from)
	if err != nil {
		return err
	}
	dstParent, dstName, err := s.leaf(ctx, to)
	if err != nil {
		return err
	}
	if _, err := s.pool.Exec(ctx, `DELETE FROM jfs_edge WHERE parent = $1 AND name = $2`,
		int64(srcParent), []byte(srcName)); err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `INSERT INTO jfs_edge (parent, name, inode, type)
		VALUES ($1,$2,$3,$4)`, int64(dstParent), []byte(dstName), int64(ino), int(typ))
	return err
}

// rmobj deletes the object file(s) a file's slice points to — a real
// "required object went missing" the capture read must answer pending.
func (s *seeder) rmobj(ctx context.Context, rel string) error {
	ino, typ, err := s.resolveIno(ctx, rel, false)
	if err != nil {
		return err
	}
	if typ != jfsFile {
		return fmt.Errorf("%s: not a file", rel)
	}
	rows, err := s.pool.Query(ctx, `SELECT slices FROM jfs_chunk WHERE inode = $1`, int64(ino))
	if err != nil {
		return err
	}
	defer rows.Close()
	var removed int
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return err
		}
		for i := 0; i+24 <= len(raw); i += 24 {
			sid := binary.BigEndian.Uint64(raw[i+4:])
			size := binary.BigEndian.Uint32(raw[i+12:])
			if sid == 0 {
				continue
			}
			for bi, off := 0, 0; off < int(size); bi++ {
				bsize := int(size) - off
				if bsize > blockBytes {
					bsize = blockBytes
				}
				p := s.objectPath(sid, bi, bsize)
				if err := os.Remove(p); err == nil {
					removed++
				}
				off += bsize
			}
		}
	}
	if removed == 0 {
		return fmt.Errorf("%s: no objects found to remove", rel)
	}
	fmt.Printf("removed %d object(s)\n", removed)
	return nil
}

// sha resolves a file's object content hash — fixture verification of
// what the volume ACTUALLY holds, independent of the capture path.
func (s *seeder) sha(ctx context.Context, rel string) (string, error) {
	ino, typ, err := s.resolveIno(ctx, rel, false)
	if err != nil {
		return "", err
	}
	if typ != jfsFile {
		return "", fmt.Errorf("%s: not a file", rel)
	}
	rows, err := s.pool.Query(ctx, `SELECT slices FROM jfs_chunk
		WHERE inode = $1 ORDER BY indx`, int64(ino))
	if err != nil {
		return "", err
	}
	defer rows.Close()
	h := sha256.New()
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return "", err
		}
		for i := 0; i+24 <= len(raw); i += 24 {
			sid := binary.BigEndian.Uint64(raw[i+4:])
			size := binary.BigEndian.Uint32(raw[i+12:])
			slen := binary.BigEndian.Uint32(raw[i+20:])
			if sid == 0 {
				h.Write(make([]byte, slen))
				continue
			}
			got := 0
			for bi, off := 0, 0; off < int(size) && got < int(slen); bi++ {
				bsize := int(size) - off
				if bsize > blockBytes {
					bsize = blockBytes
				}
				b, err := os.ReadFile(s.objectPath(sid, bi, bsize))
				if err != nil {
					return "", fmt.Errorf("%s: %w", rel, err)
				}
				want := int(slen) - got
				if want > len(b) {
					want = len(b)
				}
				h.Write(b[:want])
				got += want
				off += bsize
			}
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
