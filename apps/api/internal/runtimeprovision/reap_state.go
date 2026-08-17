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
	reapStateVersion  = 1
	reapStateFileName = "reap-attestations.json"
	maxReapStateBytes = 1 << 20
)

type durableReapState struct {
	mu        sync.Mutex
	directory string
	path      string
	entries   map[string]uint64
}

type reapStateDocument struct {
	Version           int                             `json:"version"`
	PersonalityAgents map[string]reapStateAgentRecord `json:"personality_agents"`
}

type reapStateAgentRecord struct {
	ReapedThroughGeneration *uint64 `json:"reaped_through_generation"`
}

func newDurableReapState(directory string) (*durableReapState, error) {
	if directory == "" || !filepath.IsAbs(directory) || filepath.Clean(directory) != directory {
		return nil, errors.New("runtime provision state directory must be canonical and absolute")
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, fmt.Errorf("create state directory: %w", err)
	}
	canonical, err := filepath.EvalSymlinks(directory)
	if err != nil || canonical != directory {
		return nil, errors.New("runtime provision state directory must contain no symlink")
	}
	info, err := os.Lstat(directory)
	if err != nil {
		return nil, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.IsDir() || stat.Uid != uint32(os.Geteuid()) || info.Mode().Perm()&0o077 != 0 {
		return nil, errors.New("runtime provision state directory must be owner-only and owned by the provisioner")
	}

	state := &durableReapState{
		directory: directory,
		path:      filepath.Join(directory, reapStateFileName),
		entries:   make(map[string]uint64),
	}
	if err := state.load(); err != nil {
		return nil, err
	}
	return state, nil
}

func (state *durableReapState) load() error {
	info, err := os.Lstat(state.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect reap state: %w", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 ||
		stat.Uid != uint32(os.Geteuid()) || info.Mode().Perm() != 0o600 || stat.Nlink != 1 {
		return errors.New("durable reap state must be an owner-only regular file with one link")
	}
	if info.Size() > maxReapStateBytes {
		return errors.New("durable reap state exceeds the maximum allowed size")
	}
	file, err := os.Open(state.path)
	if err != nil {
		return fmt.Errorf("open reap state: %w", err)
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, maxReapStateBytes+1))
	decoder.DisallowUnknownFields()
	var document reapStateDocument
	if err := decoder.Decode(&document); err != nil {
		return fmt.Errorf("decode reap state: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return errors.New("decode reap state: file must contain exactly one JSON value")
	}
	if document.Version != reapStateVersion || document.PersonalityAgents == nil {
		return errors.New("decode reap state: unsupported or incomplete state document")
	}
	for personalityAgentID, record := range document.PersonalityAgents {
		if err := ValidatePersonalityAgentID(personalityAgentID); err != nil {
			return fmt.Errorf("decode reap state personality agent: %w", err)
		}
		if record.ReapedThroughGeneration == nil {
			return errors.New("decode reap state: reaped_through_generation is required")
		}
		if *record.ReapedThroughGeneration > MaxProcessGeneration {
			return errors.New("decode reap state: generation is outside the process-generation domain")
		}
		state.entries[personalityAgentID] = *record.ReapedThroughGeneration
	}
	return nil
}

func (state *durableReapState) lookup(personalityAgentID string) (uint64, bool) {
	state.mu.Lock()
	defer state.mu.Unlock()
	generation, ok := state.entries[personalityAgentID]
	return generation, ok
}

func (state *durableReapState) record(personalityAgentID string, generation uint64) error {
	if err := ValidatePersonalityAgentID(personalityAgentID); err != nil {
		return err
	}
	if generation > MaxProcessGeneration {
		return errors.New("reaped generation is outside the process-generation domain")
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	previous, existed := state.entries[personalityAgentID]
	if existed && previous >= generation {
		return nil
	}
	state.entries[personalityAgentID] = generation
	if err := state.persist(); err != nil {
		if existed {
			state.entries[personalityAgentID] = previous
		} else {
			delete(state.entries, personalityAgentID)
		}
		return err
	}
	return nil
}

func (state *durableReapState) persist() (result error) {
	document := reapStateDocument{
		Version:           reapStateVersion,
		PersonalityAgents: make(map[string]reapStateAgentRecord, len(state.entries)),
	}
	for personalityAgentID, generation := range state.entries {
		reapedThroughGeneration := generation
		document.PersonalityAgents[personalityAgentID] = reapStateAgentRecord{
			ReapedThroughGeneration: &reapedThroughGeneration,
		}
	}
	encoded, err := encodeReapStateDocument(document)
	if err != nil {
		return err
	}
	if len(encoded) > maxReapStateBytes {
		return errors.New("durable reap state would exceed the maximum allowed size")
	}
	temporary, err := os.CreateTemp(state.directory, ".reap-attestations-*")
	if err != nil {
		return fmt.Errorf("create temporary reap state: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryPath)
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return fmt.Errorf("protect temporary reap state: %w", err)
	}
	if _, err := temporary.Write(encoded); err != nil {
		return fmt.Errorf("write temporary reap state: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("sync temporary reap state: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close temporary reap state: %w", err)
	}
	if err := os.Rename(temporaryPath, state.path); err != nil {
		return fmt.Errorf("publish reap state: %w", err)
	}
	directory, err := os.Open(state.directory)
	if err != nil {
		return fmt.Errorf("open reap state directory: %w", err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync reap state directory: %w", err)
	}
	return nil
}

func encodeReapStateDocument(document reapStateDocument) ([]byte, error) {
	encoded, err := json.Marshal(document)
	if err != nil {
		return nil, fmt.Errorf("encode reap state: %w", err)
	}
	return append(encoded, '\n'), nil
}
