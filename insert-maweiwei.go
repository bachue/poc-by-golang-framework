package main

import (
	"flag"
	"log"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/pborman/uuid"
	"github.com/tuvistavie/securerandom"
	mgo "gopkg.in/mgo.v2"
	bson "gopkg.in/mgo.v2/bson"
)

var (
	NumberGoroutine = flag.Int("n", 1, "number of goruntine")
	host            = flag.String("h", "127.0.0.1", "host")
	db              = flag.String("d", "test", "db")
	coll            = flag.String("c", "test", "coll")
	dbCount         = flag.Int("dbs", 1, "db count")
	amount          = flag.Uint64("a", 0, "number of data")
	total           = uint64(0)
	last            = time.Now()
	collsList       [][]*mgo.Collection
)

func generateSecureRandomHex(n int) string {
	hex, err := securerandom.Hex(n >> 1)
	if err != nil {
		panic(err)
	}
	return hex
}

func write(colls []*mgo.Collection, done chan<- bool) {
	var t uint64
	for {
		if t = atomic.AddUint64(&total, 1); *amount > 0 && t > *amount {
			break
		}

		err := colls[t%uint64(*dbCount)].Insert(bson.M{
			"_id":   uuid.New(),
			"key0":  generateSecureRandomHex(128),
			"key1":  generateSecureRandomHex(128),
			"key2":  generateSecureRandomHex(128),
			"key3":  generateSecureRandomHex(128),
			"key4":  generateSecureRandomHex(128),
			"key5":  generateSecureRandomHex(128),
			"key6":  generateSecureRandomHex(128),
			"key7":  generateSecureRandomHex(128),
			"key8":  generateSecureRandomHex(128),
			"key9":  generateSecureRandomHex(128),
			"key10": generateSecureRandomHex(128),
			"key11": generateSecureRandomHex(128),
			"key12": generateSecureRandomHex(128),
			"key13": generateSecureRandomHex(128),
			"key14": generateSecureRandomHex(128),
			"key15": generateSecureRandomHex(128),
			"key16": generateSecureRandomHex(128),
			"key17": generateSecureRandomHex(128),
			"key18": generateSecureRandomHex(128),
			"key19": generateSecureRandomHex(128),
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

	collsList := make([][]*mgo.Collection, *NumberGoroutine)
	for i := 0; i < *NumberGoroutine; i++ {
		session, err := mgo.Dial(*host)
		if err != nil {
			log.Fatal(err)
		}
		colls := make([]*mgo.Collection, *dbCount)
		for j := 0; j < *dbCount; j++ {
			colls[j] = session.DB(*db).C(*coll)
		}
		collsList[i] = colls
	}

	done := make([]chan bool, *NumberGoroutine)
	for i := 0; i < *NumberGoroutine; i++ {
		done[i] = make(chan bool, 1)
	}

	for i := 0; i < *NumberGoroutine; i++ {
		go write(collsList[i], done[i])
	}

	for i := 0; i < *NumberGoroutine; i++ {
		<-done[i]
	}
}
