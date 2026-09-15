// A wrapper around the internal fsutil/fsutil package,
// kept for go mod backwards compatibility.
package fsutil

import (
	"iter"
	"os"
	"time"

	"github.com/ipni/storetheindex/internal/fsutil"
)

func DirWritable(dir string) error {
	return fsutil.DirWritable(dir)
}

func ExpandHome(path string) (string, error) {
	return fsutil.ExpandHome(path)
}

func FileExists(filename string) bool {
	return fsutil.FileExists(filename)
}

func FileChanged(filePath string, modTime time.Time) (time.Time, bool, error) {
	return fsutil.FileChanged(filePath, modTime)
}

func DirIter(path string, batchSize int) iter.Seq2[os.DirEntry, error] {
	return fsutil.DirIter(path, batchSize)
}
