package main

import (
	"flag"
	"log"
	"math/rand"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/pborman/uuid"
	mgo "gopkg.in/mgo.v2"
	bson "gopkg.in/mgo.v2/bson"
)

var (
	NumberGoroutine = flag.Int("n", 1, "number of goruntine")
	host            = flag.String("h", "127.0.0.1", "host")
	db              = flag.String("d", "test", "db")
	coll            = flag.String("c", "test", "coll")
	dbCount         = flag.Int("dbs", 1, "db count")
	amount          = flag.Uint64("a", 1, "number of data")
	total           = uint64(0)
	last            = time.Now()
)

func run(done chan<- bool) {
	session, err := mgo.Dial(*host)
	if err != nil {
		log.Fatal(err)
	}
	colls := make([]*mgo.Collection, *dbCount)
	for i := 0; i < *dbCount; i++ {
		colls[i] = session.DB(*db).C(*coll)
	}
	var t uint64
	for {
		if t = atomic.AddUint64(&total, 1); t > *amount {
			break
		}

		err = colls[t%uint64(*dbCount)].Insert(bson.M{
			"_id":      "7x3m1s:" + uuid.New(),
			"hash":     uuid.New(),
			"mimeType": "image/jpeg",
			"fsize":    rand.Int63(),
			"putTime":  rand.Int63(),
			"fh":       bson.Binary{0x00, []byte(uuid.New())},
		})
		if err != nil {
			log.Println(err)
			continue
		}
		if t%100000 == 0 {
			log.Println(t, 100000/time.Since(last).Seconds())
			last = time.Now()
		}
	}
	done <- true
}

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())

	done := make([]chan bool, *NumberGoroutine)
	for i := 0; i < *NumberGoroutine; i++ {
		done[i] = make(chan bool, 1)
	}

	for i := 0; i < *NumberGoroutine-1; i++ {
		go run(done[i])
	}
	run(done[*NumberGoroutine-1])

	for i := 0; i < *NumberGoroutine; i++ {
		<-done[i]
	}
}
