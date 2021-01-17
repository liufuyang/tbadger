package main

import (
	"encoding/binary"
	"fmt"
	"github.com/pingcap/badger"
	"log"
	"math"
	"math/rand"
	"testing"
	"unsafe"
)

var (
	db  *badger.DB
	err error
)

func init() {
	rand.Seed(42)
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = valueDir
	opts.TableBuilderOptions.BlockSize = 1024
	opts.TableBuilderOptions.MaxTableSize = 8 << 20 * 4
	opts.LevelOneSize = 128 << 20
	opts.TableBuilderOptions.LevelSizeMultiplier = 2
	db, err = badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	for n := 0; n < 20; n++ {
		get(db, uint32(n))
	}
}

func BenchmarkBadger(b *testing.B) {
	nFound := 0
	maxValPow1P2 := uint32(math.Pow(float64(MAX_VALUE), 1.2))
	for n := 0; n < b.N; n++ {
		key := rand.Uint32() % maxValPow1P2
		found := get(db, key)
		if found {
			nFound++
		}
	}
	fmt.Printf("-------> Hit rate: %f\n", float64(nFound)/float64(b.N))
}

func Test1(t *testing.T) {
	var x uint32 = 257
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, x)
	fmt.Println(bs)

	p := unsafe.Pointer(&bs[0])
	fmt.Println(p)
	keyAsUint32 := *(*uint32)(p)
	fmt.Println(keyAsUint32)
}

func Test2(t *testing.T) {
	// Trigger get once before
	get(db, 1234)
	get(db, 4321)
}
