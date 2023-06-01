package command

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
)

// updateBatchSize is the number of records to update at a time.
const updateBatchSize = 500000

func updateDatastore(ctx context.Context, ds datastore.Batching) error {
	dsVerKey := datastore.NewKey(dsInfoPrefix + dsVersionKey)
	curVerData, err := ds.Get(ctx, dsVerKey)
	if err != nil && !errors.Is(err, datastore.ErrNotFound) {
		return fmt.Errorf("cannot check datastore: %w", err)
	}
	var curVer string
	if len(curVerData) != 0 {
		curVer = string(curVerData)
	}
	if curVer == dsVersion {
		return nil
	}
	if curVer > dsVersion {
		return fmt.Errorf("unknown datastore verssion: %s", curVer)
	}

	log.Infof("Updating datastore to version %s", dsVersion)
	if err = rmDtFsmRecords(ctx, ds); err != nil {
		return err
	}
	if err = rmOldTempRecords(ctx, ds); err != nil {
		return err
	}

	if err = ds.Put(ctx, dsVerKey, []byte(dsVersion)); err != nil {
		return err
	}
	if err = ds.Sync(ctx, datastore.NewKey("")); err != nil {
		return fmt.Errorf("cannot sync datastore: %w", err)
	}

	log.Infow("Datastore update finished")
	return nil
}

func rmOldTempRecords(ctx context.Context, ds datastore.Batching) error {
	q := query.Query{
		KeysOnly: true,
	}
	results, err := ds.Query(ctx, q)
	if err != nil {
		return fmt.Errorf("cannot query datastore: %w", err)
	}
	defer results.Close()

	batch, err := ds.Batch(ctx)
	if err != nil {
		return fmt.Errorf("cannot create datastore batch: %w", err)
	}

	var cidCount, dtKeyCount, writeCount int
	for result := range results.Next() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if writeCount >= updateBatchSize {
			writeCount = 0
			if err = batch.Commit(ctx); err != nil {
				return fmt.Errorf("cannot commit datastore updates: %w", err)
			}
			log.Infow("Datastore update removed old records", "fsmData", dtKeyCount, "adData", cidCount)
		}
		if result.Error != nil {
			return fmt.Errorf("cannot read query result from datastore: %w", result.Error)
		}
		ent := result.Entry
		if len(ent.Key) == 0 {
			log.Warnf("result entry has empty key")
			continue
		}
		var key string
		if ent.Key[0] == '/' {
			key = ent.Key[1:]
		} else {
			key = ent.Key
		}

		before, after, found := strings.Cut(key, "/")
		if found {
			if before[0] >= '0' && before[0] <= '9' && len(after) > 22 {
				if err = batch.Delete(ctx, datastore.NewKey(key)); err != nil {
					return fmt.Errorf("cannot delete dt state key from datastore: %w", err)
				}
				writeCount++
				dtKeyCount++
			}
			continue
		}

		if _, err := cid.Decode(key); err != nil {
			log.Warnf("Unknown key: %s", key)
			continue
		}
		if err = batch.Delete(ctx, datastore.NewKey(key)); err != nil {
			return fmt.Errorf("cannot delete CID from datastore: %w", err)
		}
		writeCount++
		cidCount++
	}

	if err = batch.Commit(ctx); err != nil {
		return fmt.Errorf("cannot commit datastore updated: %w", err)
	}
	if err = ds.Sync(ctx, datastore.NewKey("")); err != nil {
		return fmt.Errorf("cannot sync datastore: %w", err)
	}

	if dtKeyCount != 0 || cidCount != 0 {
		log.Infow("Datastore update removed old records", "fsmData", dtKeyCount, "adData", cidCount)
	}
	return nil
}

func rmDtFsmRecords(ctx context.Context, ds datastore.Batching) error {
	q := query.Query{
		KeysOnly: true,
		Prefix:   "/data-transfer-v2",
	}
	results, err := ds.Query(ctx, q)
	if err != nil {
		return fmt.Errorf("cannot query datastore: %w", err)
	}
	defer results.Close()

	batch, err := ds.Batch(ctx)
	if err != nil {
		return fmt.Errorf("cannot create datastore batch: %w", err)
	}

	var dtKeyCount, writeCount int
	for result := range results.Next() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if writeCount >= updateBatchSize {
			writeCount = 0
			if err = batch.Commit(ctx); err != nil {
				return fmt.Errorf("cannot commit datastore: %w", err)
			}
			log.Infow("Datastore update removed data-transfer fsm records", "count", dtKeyCount)
		}
		if result.Error != nil {
			return fmt.Errorf("cannot read query result from datastore: %w", result.Error)
		}
		ent := result.Entry
		if len(ent.Key) == 0 {
			log.Warnf("result entry has empty key")
			continue
		}

		if err = batch.Delete(ctx, datastore.NewKey(ent.Key)); err != nil {
			return fmt.Errorf("cannot delete dt state key from datastore: %w", err)
		}
		writeCount++
		dtKeyCount++
	}

	if err = batch.Commit(ctx); err != nil {
		return fmt.Errorf("cannot commit datastore: %w", err)
	}
	if err = ds.Sync(context.Background(), datastore.NewKey(q.Prefix)); err != nil {
		return err
	}

	if dtKeyCount != 0 {
		log.Infow("Datastore update removed data-transfer fsm records", "count", dtKeyCount)
	}
	return nil
}
