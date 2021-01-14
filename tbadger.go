package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/pingcap/badger"
)

const (
	dir      = "./data"
	valueDir = "./data"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func main() {
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = valueDir
	opts.TableBuilderOptions.BlockSize = 1024
	opts.TableBuilderOptions.MaxTableSize = 8 << 20 * 4
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}

	get(db, 123)
	get(db, 124)
	get(db, 8998)

	BatchInsert(db)

	get(db, 123)
	get(db, 124)
	get(db, 8998)
	get(db,12345)

	defer db.Close()
}

func scan10() {
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = valueDir
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	i := 0
	err = db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			v, err := item.Value()
			if err != nil {
				return err
			}
			fmt.Printf("key=%s, value=%s\n", k, v)
			i += 1
			if i > 10 {
				break
			}
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
}

func BatchInsert(db * badger.DB) {

	i := 0

	for {
		err := db.Update(func(txn *badger.Txn) error {
			key := fmt.Sprintf("%16d", i)
			value := randStringRunes(64)
			return txn.Set([]byte(key), []byte(value))
		})
		if err != nil {
			log.Fatal(err)
		}
		i += 1
		if i > 400000 {
			break
		}
		if i % 10000 == 0{
			log.Printf("%d keys already inserted\n", i)
		}
	}
}

func get(db *badger.DB, key int) {
	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(fmt.Sprintf("%16d", key)))
		if err != nil {
			return err
		}
		val, err := item.Value()
		if err != nil {
			return err
		}
		log.Printf("key: %d, value %s\n", key, val)
		return nil
	})
	if err != nil {
		log.Printf("key: %d, err: %s", key, err.Error())
	}
}
