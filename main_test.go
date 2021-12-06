package main

import (
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func TestReadLevelDB(t *testing.T) {
	db, _ := leveldb.OpenFile("./testdata/leveldb", nil)
	defer db.Close()
	iter := db.NewIterator(&util.Range{Start: []byte("fact1638772710#key_0#a_0"), Limit: []byte("fact1638772710#key_0#c_0_3")}, nil)
	for iter.Next() {
		// Remember that the contents of the returned slice should not be modified, and
		// only valid until the next call to Next.
		key := iter.Key()
		value := iter.Value()
		t.Logf("key:%s,value:%s", key, value)
	}
	iter.Release()
}

var start = "fact1638772710#key_0#a_0"
var limit = "fact1638772710#key_0#c_0_3"

func TestReadBadgerDB(t *testing.T) {
	dir := "./testdata/badgerdb"

	opts := new(badger.Options)
	*opts = getTestOptions(dir)

	db, _ := badger.Open(*opts)
	defer db.Close()
	db.View(func(txn *badger.Txn) error {
		opt := badger.DefaultIteratorOptions
		// NOTE: Comment opt.Prefix out here to compare the performance
		// difference between providing Prefix as an option, v/s not. I
		// see a 20% improvement when there are ~80 SSTables.
		opt.Prefix = []byte(start)
		opt.AllVersions = true

		itr := txn.NewIterator(opt)
		defer itr.Close()
		key := []byte(start)
		for itr.Seek(key); itr.ValidForPrefix(key); itr.Next() {
			t.Logf(itr.Item().String())
		}

		return nil
	})

}
