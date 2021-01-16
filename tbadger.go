package main

import (
	"encoding/binary"
	"log"
	"math"
	"math/rand"
	"time"

	"github.com/pingcap/badger"
)

const (
	dir              = "./data"
	valueDir         = "./data"
	MAX_VALUE int32 = 1000000
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func main() {
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = valueDir
	opts.TableBuilderOptions.BlockSize = 1024
	opts.TableBuilderOptions.MaxTableSize = 8 << 20 * 4
	opts.LevelOneSize = 128 << 20
	opts.TableBuilderOptions.LevelSizeMultiplier = 2
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	BatchInsert(db)

	for n := 0; n < 1000; n++ {
		get(db, uint32(n))
	}
}

func BatchInsert(db *badger.DB) {
	var i uint32 = 1

	for {
		err := db.Update(func(txn *badger.Txn) error {
			n := uint32(rand.Int31n(MAX_VALUE))               // n is in range [0, MAX_VALUE];
			nExp1p1 := uint32(math.Pow(float64(n), 1.2)) // n is in range [0, MAX_VALUE^1.1]; 1_000_000^1.2 = 15_848_931
			key := make([]byte, 4)
			value := make([]byte, 4)
			binary.BigEndian.PutUint32(key, nExp1p1) // making key not continues, [0, 15_848_931]
			binary.BigEndian.PutUint32(value, n)

			// fmt.Printf("%d|%d\n", nExp1p1, n)
			return txn.Set(key, value)
		})
		if err != nil {
			log.Fatal(err)
		}
		i += 1
		if i > 1000000 {
			break
		}
		if i%10000 == 0 {
			log.Printf("%d keys already inserted\n", i)
		}
	}

}

func get(db *badger.DB, key uint32) bool {
	found := false
	var _ []byte
	err := db.View(func(txn *badger.Txn) error {
		bs := make([]byte, 4)
		binary.BigEndian.PutUint32(bs, key)
		item, err := txn.Get(bs)
		if err != nil {
			return err
		}
		_, err = item.Value()
		if err != nil {
			return nil
		}
		found = true
		return nil
	})
	if err != nil {
		// log.Printf("NotFound. key: %d, err: %s", key, err.Error())
	} else {
		// log.Printf("Got key: %d, value %d\n", key, binary.BigEndian.Uint32(value))
	}
	return found
}
