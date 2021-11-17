package config

import (
	"path/filepath"
	"runtime"
	"testing"
)

func TestPath(t *testing.T) {
	const dir = "vstore"

	var absdir string
	if runtime.GOOS == "windows" {
		absdir = "c:\\tmp\vstore"
	} else {
		absdir = "/tmp/vstore"
	}

	path, err := Path("", dir)
	if err != nil {
		t.Fatal(err)
	}

	configRoot, err := PathRoot()
	if err != nil {
		t.Fatal(err)
	}

	if path != filepath.Join(configRoot, dir) {
		t.Fatalf("wrong path %s:", path)
	}

	path, err = Path("altroot", dir)
	if err != nil {
		t.Fatal(err)
	}
	if path != filepath.Join("altroot", dir) {
		t.Fatalf("wrong path %s:", path)
	}

	path, err = Path("altroot", absdir)
	if err != nil {
		t.Fatal(err)
	}
	if path != filepath.Clean(absdir) {
		t.Fatalf("wrong path %s:", path)
	}
}
