package main

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/google/uuid"
	"github.com/studyzy/kvinit/wal"
)

func main() {

	readWal(100, 200)
	return
	runWal(3 * 1024)
	return
	runBadgerTest(nil, func(db *badger.DB) {
		for i := int64(0); i < 1000000; i++ {
			key := GetRandTxId()
			txnSet(db, []byte(key), []byte(fmt.Sprintf("111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111%d", i)), 0x00)
			if i%10000 == 0 {
				fmt.Printf("insert count:%d, current key:%s\n", i, key)
			}
		}
	})
	fmt.Println("all done!")
}
func runWal(sizeG int) {
	walOpt := &wal.Options{
		NoSync: true,
	}

	walOpt.SegmentSize = 100 * 1024 * 1024 //100M

	log, err := wal.Open("./waldata", nil)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	for i := 0; i < sizeG; i++ {
		for j := 0; j < 100; j++ {
			data := make([]byte, 9*1024*1024) //9M
			data[0] = byte(i*100 + j)
			err = log.Write(uint64(i*100+j+1), data)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			fmt.Println("write file index:", i*100+j)
		}
	}
	fmt.Println("write done!")
}
func readWal(h1, h2 uint64) {
	walOpt := &wal.Options{
		NoSync: true,
	}

	walOpt.SegmentSize = 100 * 1024 * 1024 //100M

	log, err := wal.Open("./waldata", nil)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	start := time.Now()
	for i := h1; i <= h2; i++ {
		data, err := log.Read(i)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		fmt.Println("Read wal height:", i, " get data length:", len(data), "cost:", time.Since(start))
	}
}

// Opens a badger db and runs a a test on it.
func runBadgerTest(opts *badger.Options, test func(db *badger.DB)) {
	dir := "./badger_data"
	if opts == nil {
		opts = new(badger.Options)
		*opts = getTestOptions(dir)
	} else {
		opts.Dir = dir
		opts.ValueDir = dir
	}

	if opts.InMemory {
		opts.Dir = ""
		opts.ValueDir = ""
	}
	db, err := badger.Open(*opts)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() {
		db.Close()
	}()
	test(db)
}

func txnSet(kv *badger.DB, key []byte, val []byte, meta byte) {
	txn := kv.NewTransaction(true)
	txn.SetEntry(badger.NewEntry(key, val).WithMeta(meta))
	txn.Commit()
}
func getTestOptions(dir string) badger.Options {
	opt := badger.DefaultOptions(dir).
		WithSyncWrites(false).
		WithLoggingLevel(badger.WARNING)
	return opt
}
func GetUUIDWithSeed(seed int64) string {
	r := rand.New(rand.NewSource(seed))
	uuid, _ := uuid.NewRandomFromReader(r)
	return strings.Replace(uuid.String(), "-", "", -1)
}

func getStandardUUID() string {
	return uuid.New().String()
}

func GetUUID() string {
	return strings.Replace(getStandardUUID(), "-", "", -1)
}

func GetRandTxId() string {
	return GetUUID() + GetUUID()
}
