package migrate

import (
	"context"
	"fmt"

	"github.com/filecoin-project/storetheindex/internal/registry"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("indexer/migrate")

const (
	datastoreVersion = "1"
)

func NeedMigration(ctx context.Context, dstore datastore.Datastore) (bool, error) {
	ver, err := readVersion(ctx, dstore)
	if err != nil {
		return false, err
	}
	if ver == "" {
		// Empty datastore
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
	case "", datastoreVersion:
		// Empty or already at current version.
		return false, nil
	case "0":
		if err = migrateV0ToV1(ctx, dstore); err != nil {
			return false, fmt.Errorf("failed to migrate version from 0 to 1: %w", err)
		}
	default:
		return false, fmt.Errorf("cannot migrate from unsupported datastore version %q", fromVer)
	}

	return true, nil
}

func Revert(ctx context.Context, dstore datastore.Datastore) (bool, error) {
	fromVer, err := readVersion(ctx, dstore)
	if err != nil {
		return false, err
	}

	switch fromVer {
	case "0", "":
		// Already at lowest version or empty.
		return false, nil
	case "1":
		if err = revertV1ToV0(ctx, dstore); err != nil {
			return false, fmt.Errorf("failed to revert from version 1 to 0: %w", err)
		}
	default:
		return false, fmt.Errorf("cannot revert from unsupported datastrore version %q", fromVer)
	}

	return true, nil
}

func readVersion(ctx context.Context, dstore datastore.Datastore) (string, error) {
	ok, err := hasPrefix(ctx, dstore, v0ProviderKeyPath)
	if err != nil {
		return "", err
	}
	if ok {
		return "0", nil
	}

	ok, err = hasPrefix(ctx, dstore, registry.ProviderKeyPath)
	if err != nil {
		return "", err
	}
	if ok {
		return "1", nil
	}

	return "", nil
}

func hasPrefix(ctx context.Context, dstore datastore.Datastore, prefix string) (bool, error) {
	// Load all providers from the datastore.
	q := query.Query{
		Prefix: prefix,
	}
	results, err := dstore.Query(ctx, q)
	if err != nil {
		return false, err
	}
	defer results.Close()

	for result := range results.Next() {
		if result.Error != nil {
			return false, fmt.Errorf("cannot read provider info: %s", result.Error)
		}
		return true, nil
	}
	return false, nil
}
