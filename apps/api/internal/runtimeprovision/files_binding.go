package runtimeprovision

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

const (
	filesBindingsVersion  = 1
	filesBindingsFileName = "files-bindings.json"
	maxFilesBindingsBytes = 1 << 20
)

// filesBinding records that one personality agent's workspace was launched
// bound to the canonical files volume. Once recorded, a later launch may not
// silently substitute a host-local workspace: prepare/activate without the
// matching SUMI_FILES_* configuration refuse until canonical support is
// restored or the record is deliberately retired by an operator. Only the
// volume identity is persisted — the canonical namespace is the volume UUID
// plus the personality-agent scope; the local mountpoint is client topology
// that may legitimately change when the same volume is remounted elsewhere.
type filesBinding struct {
	VolumeUUID string `json:"volume_uuid"`
}

func (binding filesBinding) validate() error {
	if binding.VolumeUUID == "" {
		return errors.New("canonical files binding volume UUID is required")
	}
	return nil
}

type durableFilesBindings struct {
	mu        sync.Mutex
	directory string
	path      string
	entries   map[string]filesBinding
	// pending mirrors reap state: records made visible by rename but whose
	// directory fsync has not completed are held here until confirmed.
	pending       map[string]filesBinding
	syncDirectory func(*os.File) error
}

type filesBindingsDocument struct {
	Version  int                     `json:"version"`
	Bindings map[string]filesBinding `json:"bindings"`
}

func newDurableFilesBindings(directory string) (*durableFilesBindings, error) {
	if directory == "" || !filepath.IsAbs(directory) || filepath.Clean(directory) != directory {
		return nil, errors.New("runtime provision state directory must be canonical and absolute")
	}
	state := &durableFilesBindings{
		directory: directory,
		path:      filepath.Join(directory, filesBindingsFileName),
		entries:   make(map[string]filesBinding),
		pending:   make(map[string]filesBinding),
		syncDirectory: func(directory *os.File) error {
			return directory.Sync()
		},
	}
	if err := state.load(); err != nil {
		return nil, err
	}
	return state, nil
}

func (state *durableFilesBindings) load() error {
	info, err := os.Lstat(state.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect files bindings: %w", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 ||
		stat.Uid != uint32(os.Geteuid()) || info.Mode().Perm() != 0o600 || stat.Nlink != 1 {
		return errors.New("durable files bindings must be an owner-only regular file with one link")
	}
	if info.Size() > maxFilesBindingsBytes {
		return errors.New("durable files bindings exceed the maximum allowed size")
	}
	// Same restart boundary as reap state: sync the directory so a document a
	// previous provisioner renamed without fsync is not silently promoted.
	directory, err := os.Open(state.directory)
	if err != nil {
		return fmt.Errorf("open files bindings directory: %w", err)
	}
	defer directory.Close()
	if err := state.syncDirectory(directory); err != nil {
		return fmt.Errorf("sync files bindings directory: %w", err)
	}
	file, err := os.Open(state.path)
	if err != nil {
		return fmt.Errorf("open files bindings: %w", err)
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, maxFilesBindingsBytes+1))
	decoder.DisallowUnknownFields()
	var document filesBindingsDocument
	if err := decoder.Decode(&document); err != nil {
		return fmt.Errorf("decode files bindings: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return errors.New("decode files bindings: file must contain exactly one JSON value")
	}
	if document.Version != filesBindingsVersion || document.Bindings == nil {
		return errors.New("decode files bindings: unsupported or incomplete state document")
	}
	for personalityAgentID, binding := range document.Bindings {
		if err := ValidatePersonalityAgentID(personalityAgentID); err != nil {
			return fmt.Errorf("decode files bindings personality agent: %w", err)
		}
		if err := binding.validate(); err != nil {
			return fmt.Errorf("decode files binding for %s: %w", personalityAgentID, err)
		}
		state.entries[personalityAgentID] = binding
	}
	return nil
}

func (state *durableFilesBindings) lookup(personalityAgentID string) (filesBinding, bool) {
	state.mu.Lock()
	defer state.mu.Unlock()
	binding, ok := state.entries[personalityAgentID]
	return binding, ok
}

func (state *durableFilesBindings) record(personalityAgentID string, binding filesBinding) error {
	if err := ValidatePersonalityAgentID(personalityAgentID); err != nil {
		return err
	}
	if err := binding.validate(); err != nil {
		return err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if err := state.confirmPending(); err != nil {
		return err
	}
	if previous, existed := state.entries[personalityAgentID]; existed {
		if previous == binding {
			return nil
		}
		// A recorded binding is the established canonical authority: process
		// configuration may not overwrite it. Adopt paths that hold matching
		// configuration converge on the identical record; a differing record
		// here means a launch would have bound a different volume than the
		// workspace already established, which callers must surface rather
		// than persist.
		return fmt.Errorf(
			"%w: canonical files binding already records volume %s; refusing overwrite with %s",
			ErrConflict, previous.VolumeUUID, binding.VolumeUUID,
		)
	}
	candidate := make(map[string]filesBinding, len(state.entries)+1)
	for id, entry := range state.entries {
		candidate[id] = entry
	}
	candidate[personalityAgentID] = binding
	published, err := state.persistEntries(candidate)
	if err != nil {
		if published {
			state.pending[personalityAgentID] = binding
		}
		return err
	}
	state.entries[personalityAgentID] = binding
	return nil
}

func (state *durableFilesBindings) confirmPending() error {
	if len(state.pending) == 0 {
		return nil
	}
	directory, err := os.Open(state.directory)
	if err != nil {
		return fmt.Errorf("open files bindings directory: %w", err)
	}
	defer directory.Close()
	if err := state.syncDirectory(directory); err != nil {
		return fmt.Errorf("sync files bindings directory: %w", err)
	}
	for personalityAgentID, binding := range state.pending {
		state.entries[personalityAgentID] = binding
	}
	clear(state.pending)
	return nil
}

func (state *durableFilesBindings) persistEntries(entries map[string]filesBinding) (published bool, result error) {
	document := filesBindingsDocument{
		Version:  filesBindingsVersion,
		Bindings: entries,
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		return false, fmt.Errorf("encode files bindings: %w", err)
	}
	encoded = append(encoded, '\n')
	if len(encoded) > maxFilesBindingsBytes {
		return false, errors.New("durable files bindings would exceed the maximum allowed size")
	}
	temporary, err := os.CreateTemp(state.directory, ".files-bindings-*")
	if err != nil {
		return false, fmt.Errorf("create temporary files bindings: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryPath)
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return false, fmt.Errorf("protect temporary files bindings: %w", err)
	}
	if _, err := temporary.Write(encoded); err != nil {
		return false, fmt.Errorf("write temporary files bindings: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return false, fmt.Errorf("sync temporary files bindings: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return false, fmt.Errorf("close temporary files bindings: %w", err)
	}
	if err := os.Rename(temporaryPath, state.path); err != nil {
		return false, fmt.Errorf("publish files bindings: %w", err)
	}
	directory, err := os.Open(state.directory)
	if err != nil {
		return true, fmt.Errorf("open files bindings directory: %w", err)
	}
	defer directory.Close()
	if err := state.syncDirectory(directory); err != nil {
		return true, fmt.Errorf("sync files bindings directory: %w", err)
	}
	return true, nil
}
