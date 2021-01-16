package main

import (
	"fmt"
	"github.com/pingcap/badger"
	"log"
	"math"
	"math/rand"
	"testing"
)

func BenchmarkBadger(b *testing.B) {
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = valueDir
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	nFound := 0
	maxValPow1P2 := uint32(math.Pow(float64(MAX_VALUE), 1.2))
	for n := 0; n < b.N; n++ {
		key := rand.Uint32() % maxValPow1P2
		found := get(db, key)
		if found {
			nFound ++
		}
	}
	fmt.Printf("-------> Hit rate: %f\n", float64(nFound) / float64(b.N))
}
