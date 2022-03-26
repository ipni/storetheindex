package migrate

import (
	"context"
	"fmt"

	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("indexer/migrate")

const (
	// registryVersionKey is where registry version number is stored.
	datastoreVersionKey = "/datastore/version"
	// datastoreVersion is the current version of the datastore data.
	datastoreVersion = "v1"
)

func NeedMigration(ctx context.Context, dstore datastore.Datastore) (bool, error) {
	ver, err := readVersion(ctx, dstore)
	if err != nil {
		return false, err
	}
	return ver != datastoreVersion, nil
}

func Migrate(ctx context.Context, dstore datastore.Datastore) (bool, error) {
	fromVer, err := readVersion(ctx, dstore)
	if err != nil {
		return false, err
	}

	switch fromVer {
	case datastoreVersion:
		// Already at current version.
		return false, nil
	case "v0":
		if err = migrateV0ToV1(ctx, dstore); err != nil {
			return false, fmt.Errorf("failed to migrate from v0 to v1: %w", err)
		}
	default:
		return false, fmt.Errorf("cannot migrate from unsupported datastore version %s", fromVer)
	}

	return true, nil
}

func Revert(ctx context.Context, dstore datastore.Datastore) (bool, error) {
	fromVer, err := readVersion(ctx, dstore)
	if err != nil {
		return false, err
	}

	switch fromVer {
	case "v0":
		// Already at lowest version.
		return false, nil
	case "v1":
		if err = revertV1ToV0(ctx, dstore); err != nil {
			return false, fmt.Errorf("failed to revert from v1 to v0: %w", err)
		}
	default:
		return false, fmt.Errorf("cannot revert from unsupported datastrore version %s", fromVer)
	}

	return true, nil
}

func readVersion(ctx context.Context, dstore datastore.Datastore) (string, error) {
	dsVerKey := datastore.NewKey(datastoreVersionKey)
	dsVerData, err := dstore.Get(ctx, dsVerKey)
	if err != nil && err != datastore.ErrNotFound {
		return "", err
	}

	if len(dsVerData) == 0 {
		return "v0", nil
	}

	return string(dsVerData), nil
}
