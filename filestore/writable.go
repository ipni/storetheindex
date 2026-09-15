package filestore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
)

const writeProbePrefix = ".filestore-writetest-"

// checkWritableWithProbe writes and deletes a temporary object to verify that
// store can perform write operations.
func checkWritableWithProbe(ctx context.Context, store Interface) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	path, err := writeProbePath()
	if err != nil {
		return err
	}
	if _, err := store.Put(ctx, path, strings.NewReader("")); err != nil {
		return fmt.Errorf("filestore not writable: %s: %w", store.Location(), err)
	}
	if err := store.Delete(ctx, path); err != nil {
		return fmt.Errorf("filestore not writable: %s: %w", store.Location(), err)
	}
	return nil
}

func writeProbePath() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("filestore write probe: %w", err)
	}
	return writeProbePrefix + hex.EncodeToString(b[:]), nil
}
