package command

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"reflect"
	"runtime"
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
	Flags:  updateMirrorFlags,
	Action: updateMirrorAction,
}

var updateMirrorFlags = []cli.Flag{
	&cli.IntFlag{
		Name:    "concurrency",
		Usage:   "Number of concurrent operations. Setting 0 uses number of cores.",
		Aliases: []string{"c"},
		Value:   0,
	},
}

type stats struct {
	totalSize                        int64
	checked, updates, rdErrs, wrErrs int
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

	ctx := cctx.Context
	concurrency := cctx.Int("concurrency")
	if concurrency < 0 {
		return errors.New("concurrency value must be greater than 0")
	}
	if concurrency == 0 {
		concurrency = runtime.NumCPU()
	}

	fmt.Println("Updating existing CARs in", writeStore.Type(), "write mirror from CARs in", readStore.Type(), "read mirror. concurrency =", concurrency)

	statsOut := make(chan stats)
	start := time.Now()

	filesChan, errs := writeStore.List(ctx, "/", false)
	for g := 0; g < concurrency; g++ {
		go func(files <-chan *filestore.File) {
			var st stats
			for wrFile := range files {
				if !strings.HasSuffix(wrFile.Path, carSuffix) {
					continue
				}
				st.checked++

				rdFile, err := readStore.Head(ctx, wrFile.Path)
				if err != nil {
					if !errors.Is(err, fs.ErrNotExist) {
						fmt.Println("Cannot get info for file in read mirror:", err)
						st.rdErrs++
					}
					// else CAR file not found in read store.
					continue
				}

				if rdFile.Size != wrFile.Size {
					fmt.Println("Updating CAR", wrFile.Path, "to size", rdFile.Size)
					_, rc, err := readStore.Get(ctx, rdFile.Path)
					if err != nil {
						fmt.Println("Cannot read from read mirror:", err)
						st.rdErrs++
						continue
					}
					_, err = writeStore.Put(ctx, wrFile.Path, rc)
					rc.Close()
					if err != nil {
						fmt.Println("Failed to update CAR in write mirror:", err)
						st.wrErrs++
						continue
					}
					st.updates++
					st.totalSize += rdFile.Size
				}
			}
			statsOut <- st
		}(filesChan)
	}

	var statsSum stats
	for g := 0; g < concurrency; g++ {
		st := <-statsOut
		statsSum.checked += st.checked
		statsSum.updates += st.updates
		statsSum.rdErrs += st.rdErrs
		statsSum.wrErrs += st.wrErrs
		statsSum.totalSize += st.totalSize
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
	fmt.Println("   Checked:     ", statsSum.checked)
	fmt.Println("   Read errors: ", statsSum.rdErrs)
	fmt.Println("   Write errors:", statsSum.wrErrs)
	fmt.Println("   Bytes copied:", statsSum.totalSize)
	fmt.Println("   Updated CARs:", statsSum.updates)

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
