package filestore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/ipni/storetheindex/fsutil"
)

const (
	ConfigMetadataFileName    = ".filestore-config.json"
	ConfigMetadataFileVersion = "v1"
)

// Local is a file store that stores files in the local file system.
type Local struct {
	basePath  string
	pathSplit []int
}

func NewLocal(basePath string, options ...LocalOption) (*Local, error) {
	if !filepath.IsAbs(basePath) {
		return nil, errors.New("base path must be absolute")
	}

	err := fsutil.DirWritable(basePath)
	if err != nil {
		return nil, err
	}

	opts, err := getLocalOpts(options)
	if err != nil {
		return nil, err
	}

	// BasePath configured directly, not through options
	opts.basePath = basePath

	err = processPersistedMetadata(&opts)
	if err != nil {
		return nil, err
	}

	l := &Local{
		basePath:  opts.basePath,
		pathSplit: opts.pathSplit,
	}

	return l, nil
}

// fsPath returns the filesystem path of a given object path
// based on a given basePath with path splitting criteria applied.
func (l *Local) fsPath(basePath, relPath string) string {
	fsDir, fsName := filepath.Split(filepath.FromSlash(relPath))

	pathSegments := make([]string, 0, len(l.pathSplit)+3)
	pathSegments = append(pathSegments, basePath, fsDir)

	pathSegments = l.appendFnamePathSegments(fsName, pathSegments)

	return filepath.Join(pathSegments...)
}

func (l *Local) appendFnamePathSegments(fileName string, pathSegments []string) []string {
	fsNameNoExt := fileName
	if dotPos := strings.IndexByte(fileName, '.'); dotPos > 0 {
		fsNameNoExt = fileName[:dotPos]
	}

	splitPos := 0
	for _, split := range l.pathSplit {
		splitPos += split
		if splitPos > len(fsNameNoExt) {
			break
		}

		pathSegments = append(pathSegments, fsNameNoExt[splitPos-split:splitPos])
	}

	pathSegments = append(pathSegments, fileName)

	return pathSegments
}

func (l *Local) Delete(ctx context.Context, relPath string) error {
	err := os.Remove(l.fsPath(l.basePath, relPath))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (l *Local) Get(ctx context.Context, relPath string) (*File, io.ReadCloser, error) {
	f, err := os.Open(l.fsPath(l.basePath, relPath))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, fs.ErrNotExist
		}
		return nil, nil, err
	}

	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, err
	}

	if fi.IsDir() {
		f.Close()
		return nil, nil, fs.ErrNotExist
	}

	return &File{
		Modified: fi.ModTime(),
		Path:     relPath,
		Size:     fi.Size(),
	}, f, nil
}

func (l *Local) Head(ctx context.Context, relPath string) (*File, error) {
	fi, err := os.Stat(l.fsPath(l.basePath, relPath))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fs.ErrNotExist
		}
		return nil, err
	}

	if fi.IsDir() {
		return nil, fs.ErrNotExist
	}

	return &File{
		Modified: fi.ModTime(),
		Path:     relPath,
		Size:     fi.Size(),
	}, nil
}

func (l *Local) List(ctx context.Context, relPath string, recursive bool) (<-chan *File, <-chan error) {
	c := make(chan *File, 1)
	e := make(chan error, 1)

	go func() {
		defer close(e)
		defer close(c)

		// The relPath may either be a path to a file or a path prefix,
		// those cases must be handled separately due to pathSplit that only
		// considers the filename as a splittable segment.
		if stat, err := os.Stat(l.fsPath(l.basePath, relPath)); err == nil && stat.Mode().IsRegular() {
			// relPath points to a file - emit that one and exit
			c <- &File{
				Modified: stat.ModTime(),
				Path:     relPath,
				Size:     stat.Size(),
			}

			return
		}

		// relPath must be treated as a directory prefix
		absPath := filepath.Join(l.basePath, relPath)

		e <- filepath.WalkDir(absPath, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					// A resource that is not found does not get listed.
					return nil
				}
				return err
			}

			if d.IsDir() {
				if recursive || len(l.pathSplit) > 0 {
					// For both recursive scan and filepath split, keep descending
					// to find files in sub-directories
					return nil
				}

				if path != absPath {
					// Without path split only a flat structure allowed,
					// skip any sub-directories for faster iteration
					return fs.SkipDir
				}

				return nil
			}

			// Only return results for regular files.
			if !d.Type().IsRegular() {
				return nil
			}

			// Skip metadata files
			if d.Name() == ConfigMetadataFileName {
				return nil
			}

			fi, err := d.Info()
			if err != nil {
				return err
			}

			f := &File{
				Modified: fi.ModTime(),
				Size:     fi.Size(),
				// Note: Path field must be filled in below
			}

			if len(l.pathSplit) > 0 {
				// Before emitting file entry, verify that the path is correct according
				// to path split rules
				fileName := filepath.Base(path)
				expectedFileSubPath := l.fsPath("", fileName)

				relAbsPath, err := filepath.Rel(absPath, path)
				if err != nil {
					return err
				}

				if prefix, found := strings.CutSuffix(relAbsPath, expectedFileSubPath); !found {
					// Skip the file, path structure does not match
					return nil
				} else if !recursive && prefix != "" {
					// Structure matches but is in sub-folder and not recursive mode, skip that one
					return nil
				} else if prefix != "" && prefix[len(prefix)-1] != filepath.Separator {
					// Corner case - the prefix path must align with the path separator boundary
					return nil
				} else {
					// All looks good, adjust the final path by removing sub-folder structure
					f.Path = filepath.ToSlash(filepath.Join(relPath, prefix, fileName))
				}
			} else {
				relFilePath, err := filepath.Rel(l.basePath, path)
				if err != nil {
					return err
				}

				f.Path = filepath.ToSlash(relFilePath)
			}

			select {
			case c <- f:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()

	return c, e
}

func (l *Local) Put(ctx context.Context, relPath string, r io.Reader) (*File, error) {
	absPath := l.fsPath(l.basePath, relPath)

	if dir := filepath.Dir(absPath); dir != "" {
		err := os.MkdirAll(dir, 0755)
		if err != nil {
			return nil, err
		}
	}

	f, err := os.Create(absPath)
	if err != nil {
		return nil, err
	}

	if r != nil {
		if _, err = io.Copy(f, r); err != nil {
			f.Close()
			os.Remove(absPath)
			return nil, err
		}
	}

	fi, err := f.Stat()
	if err != nil {
		f.Close()
		os.Remove(absPath)
		return nil, err
	}

	if err = f.Close(); err != nil {
		os.Remove(absPath)
		return nil, err
	}

	return &File{
		Modified: fi.ModTime(),
		Path:     relPath,
		Size:     fi.Size(),
	}, nil
}

func (l *Local) Type() string {
	return "local"
}

type localFilestoreMetadata struct {
	Version   string
	PathSplit []int
}

// processPersistedMetadata loads the metadata file from disk and updates
// configuration accordingly
func processPersistedMetadata(config *localConfig) error {
	configFilePath := filepath.Join(config.basePath, ConfigMetadataFileName)

	configFile, err := os.Open(configFilePath)
	if errors.Is(err, fs.ErrNotExist) {
		return generatePersistedMetadata(config)
	}
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("failed to open filestore configuration file at %s: %w", configFilePath, err)
	}

	defer configFile.Close()

	metadata := localFilestoreMetadata{}

	err = json.NewDecoder(configFile).Decode(&metadata)
	if err != nil {
		return fmt.Errorf(
			"failed to decode filestore configuration file at %s: %w",
			configFilePath, err,
		)
	}

	if metadata.Version != ConfigMetadataFileVersion {
		return fmt.Errorf(
			"invalid filestore configuration file at %s: unknown version %s",
			configFilePath, ConfigMetadataFileVersion,
		)
	}

	err = WithDefaultPathSplit(metadata.PathSplit...)(config)
	if err != nil {
		return fmt.Errorf(
			"invalid filestore configuration file at %s: %w",
			configFilePath, err,
		)
	}

	return nil
}

// generatePersistedMetadata attempts to build metadata file in case one is not found.
//
// This method uses heuristic approach to guarantee backwards compatibility with filestore
// instances that do not have the metadata file:
//   - if the directory looks empty - i.e. contains no entries -
//     assume we're initializing a new filestore: write down a new metadata file using default settings
//   - if the directory is not empty - assume we're opening existing filestore - do nothing
func generatePersistedMetadata(config *localConfig) error {
	// Check if this is a legacy filestore without metadata file
	for _, err := range fsutil.DirIter(config.basePath, 100) {
		if err != nil {
			return fmt.Errorf("filestore layout detection failed: %w", err)
		}

		// Non-empty directory found - treat it as a legacy filestore, only use basePath
		*config = localConfig{
			basePath: config.basePath,
		}

		return nil
	}

	// Empty directory - generate new metadata file using defaults

	configFilePath := filepath.Join(config.basePath, ConfigMetadataFileName)

	jsonData, _ := json.Marshal(&localFilestoreMetadata{
		Version:   ConfigMetadataFileVersion,
		PathSplit: config.pathSplit,
	})

	err := os.WriteFile(configFilePath, jsonData, 0666)
	if err != nil {
		return fmt.Errorf(
			"failed to create filestore configuration file at %s: %w",
			configFilePath, err,
		)
	}

	return nil
}
