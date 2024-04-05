package command

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"reflect"
	"strings"
	"time"

	"github.com/ipni/storetheindex/carstore"
	"github.com/ipni/storetheindex/config"
	"github.com/ipni/storetheindex/filestore"
	"github.com/urfave/cli/v2"
)

var UpdateMirrorCmd = &cli.Command{
	Name:   "update-mirror",
	Usage:  "Update CAR files from read mirror to write mirror",
	Action: updateMirrorAction,
}

func updateMirrorAction(cctx *cli.Context) error {
	cfgMirror, err := getMirrorConfig()
	if err != nil {
		return err
	}

	readStore, writeStore, err := getMirrorStores(cfgMirror)
	if err != nil {
		return err
	}

	carSuffix, err := getCARSuffix(cfgMirror)
	if err != nil {
		return err
	}

	fmt.Println("Updating existing CARs in", writeStore.Type(), "write mirror from CARs in", readStore.Type(), "read mirror")

	ctx := cctx.Context

	var totalSize int64
	var checked, updates, rdErrs, wrErrs int
	start := time.Now()

	files, errs := writeStore.List(ctx, "/", false)
	for wrFile := range files {
		if !strings.HasSuffix(wrFile.Path, carSuffix) {
			continue
		}
		checked++

		rdFile, err := readStore.Head(ctx, wrFile.Path)
		if err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				fmt.Println("Cannot get info for file in read mirror:", err)
				rdErrs++
			}
			// else CAR file not found in read store.
			continue
		}

		if rdFile.Size != wrFile.Size {
			fmt.Println("Updating CAR", wrFile.Path, "to size", rdFile.Size)
			_, rc, err := readStore.Get(ctx, rdFile.Path)
			if err != nil {
				fmt.Println("Cannot read from read mirror:", err)
				rdErrs++
				continue
			}
			_, err = writeStore.Put(ctx, wrFile.Path, rc)
			rc.Close()
			if err != nil {
				fmt.Println("Failed to update CAR in write mirror:", err)
				wrErrs++
				continue
			}
			updates++
			totalSize += rdFile.Size
		}
	}
	elapsed := time.Since(start)

	err = <-errs
	if err != nil {
		if errors.Is(err, context.Canceled) {
			fmt.Println("Mirror update canceled")
			err = nil
		} else {
			fmt.Println("Mirror update ended due to error")
			err = fmt.Errorf("error listing files in read mirror: %w", err)
		}
	} else {
		fmt.Println("Mirror update complete")
	}

	fmt.Println("   Elapsed:     ", elapsed.String())
	fmt.Println("   Checked:     ", checked)
	fmt.Println("   Read errors: ", rdErrs)
	fmt.Println("   Write errors:", wrErrs)
	fmt.Println("   Bytes copied:", totalSize)
	fmt.Println("   Updated CARs:", updates)

	return err
}

func getMirrorStores(cfgMirror config.Mirror) (filestore.Interface, filestore.Interface, error) {
	readStore, err := filestore.MakeFilestore(cfgMirror.Retrieval)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot create car file storage for read mirror: %w", err)
	}
	if readStore == nil {
		return nil, nil, errors.New("read mirror is enabled with no storage backend")
	}

	writeStore, err := filestore.MakeFilestore(cfgMirror.Storage)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot create car file storage for write mirror: %w", err)
	}
	if writeStore == nil {
		return nil, nil, errors.New("write mirror is enabled with no storage backend")
	}

	return readStore, writeStore, nil
}

func getMirrorConfig() (config.Mirror, error) {
	cfg, err := loadConfig("")
	if err != nil {
		return config.Mirror{}, err
	}
	cfgMirror := cfg.Ingest.AdvertisementMirror

	if !cfgMirror.Write {
		return config.Mirror{}, errors.New("write mirror not enabled")
	}
	if !cfgMirror.Read {
		return config.Mirror{}, errors.New("read mirror not enabled")
	}
	if reflect.DeepEqual(cfgMirror.Storage, cfgMirror.Retrieval) {
		return config.Mirror{}, errors.New("read and write mirrors have the same storage")
	}

	return cfgMirror, nil
}

func getCARSuffix(cfgMirror config.Mirror) (string, error) {
	switch cfgMirror.Compress {
	case carstore.Gzip, "gz":
		return carstore.CarFileSuffix + carstore.GzipFileSuffix, nil
	case "", "none", "nil", "null":
		return carstore.CarFileSuffix, nil
	}
	return "", fmt.Errorf("unsupported compression: %s", cfgMirror.Compress)
}
