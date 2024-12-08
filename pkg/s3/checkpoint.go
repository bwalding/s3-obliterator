package s3

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
)

type CheckpointMetadata struct {
	filename string
	mutex    sync.Mutex
	// The last key that was started - if it has not finished yet, it will be in the in progres list
	LastKey string `json:"lastKey"`
	// Anything that is currently in-progress
	RunningKeys map[string]bool `json:"inProgress"`
}

func NewCheckpointMetadata(filename string) *CheckpointMetadata {
	return &CheckpointMetadata{
		filename:    filename,
		RunningKeys: make(map[string]bool),
	}
}

func (cpm *CheckpointMetadata) Start(key string) {
	cpm.mutex.Lock()
	defer cpm.mutex.Unlock()

	// May not always be running in order due to how the threads are allocated work
	if key > cpm.LastKey {
		cpm.LastKey = key
	}
	cpm.RunningKeys[key] = true
}

func (cpm *CheckpointMetadata) Finish(key string) {
	cpm.mutex.Lock()
	defer cpm.mutex.Unlock()

	delete(cpm.RunningKeys, key)
}

// NOTE: no mutex - caller's problem.
func (cpm *CheckpointMetadata) isRunning(key string) bool {
	_, ok := cpm.RunningKeys[key]
	return ok
}

func (cpm *CheckpointMetadata) IsComplete(key string) bool {
	cpm.mutex.Lock()
	defer cpm.mutex.Unlock()

	if key > cpm.LastKey {
		return false
	}

	return !cpm.isRunning(key)
}

func (cpm *CheckpointMetadata) Load() error {
	file, errOpen := os.Open(cpm.filename)
	if errOpen != nil {
		if os.IsNotExist(errOpen) {
			cpm.LastKey = ""
			cpm.RunningKeys = make(map[string]bool)
			return nil
		}
		return errOpen
	}
	defer file.Close()

	bytes, errRead := io.ReadAll(file)
	if errRead != nil {
		return errRead
	}

	if len(bytes) == 0 {
		return fmt.Errorf("checkpoint file is empty: %s", cpm.filename)
	}

	var metadata CheckpointMetadata
	errUnmarshal := json.Unmarshal(bytes, &metadata)
	if errUnmarshal != nil {
		return fmt.Errorf("failure unmarshaling %s: %w", cpm.filename, errUnmarshal)
	}

	cpm.LastKey = metadata.LastKey
	cpm.RunningKeys = metadata.RunningKeys
	if cpm.RunningKeys == nil {
		cpm.RunningKeys = make(map[string]bool)
	}
	return nil
}

func (cpm *CheckpointMetadata) Save() error {
	cpm.mutex.Lock()
	defer cpm.mutex.Unlock()

	bytes, errMarshal := json.Marshal(cpm)
	if errMarshal != nil {
		return errMarshal
	}

	tmpFilename := cpm.filename + ".tmp"
	file, errCreate := os.Create(tmpFilename)
	if errCreate != nil {
		return errCreate
	}
	defer file.Close()

	_, errWrite := file.Write(bytes)
	if errWrite != nil {
		return fmt.Errorf("failed to write to %s: %w", tmpFilename, errWrite)
	}
	file.Close()

	errRename := os.Rename(tmpFilename, cpm.filename)
	if errRename != nil {
		return fmt.Errorf("failed to rename %s to %s: %w", tmpFilename, cpm.filename, errRename)
	}

	return nil
}
