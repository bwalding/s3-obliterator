package s3

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

const key1 = "key1"
const key2 = "key2"

func TestIsComplete(t *testing.T) {
	r := require.New(t)

	cpm := NewCheckpointMetadata("test")
	r.False(cpm.IsComplete(key1))

	cpm.Start(key1)
	r.False(cpm.IsComplete(key1))

	cpm.Finish(key1)
	r.True(cpm.IsComplete(key1))
}

func TestPersistence(t *testing.T) {
	r := require.New(t)

	// create a temporary file

	// create a temporary file
	tempFile, errTempFile := os.CreateTemp("", "test.checkpoint")
	if errTempFile != nil {
		r.Failf("Failed to create temporary file", "Failed to create temporary file: %v", errTempFile)
	}
	r.NoError(tempFile.Close())
	r.NoError(os.Remove(tempFile.Name()))

	// We want to cleanup the file since it gets recreated
	defer os.Remove(tempFile.Name())

	cpm := NewCheckpointMetadata(tempFile.Name())
	r.NoError(cpm.Load())

	r.False(cpm.IsComplete(key1))

	cpm.Start(key1)
	r.False(cpm.IsComplete(key1))

	cpm.Finish(key1)
	r.True(cpm.IsComplete(key1))
	r.NoError(cpm.Save())

	// Now create a new cpm to check the consistency
	cpm2 := NewCheckpointMetadata(tempFile.Name())
	r.NoError(cpm2.Load())

	// Check the known state that is exposed
	r.Equal(cpm.LastKey, cpm2.LastKey)
	r.Equal(cpm.RunningKeys, cpm2.RunningKeys)

	// Make sure no internal state is broken
	r.True(cpm2.IsComplete(key1))
	r.False(cpm2.IsComplete(key2))

}
