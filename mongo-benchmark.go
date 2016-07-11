package main

import (
	"flag"
	"log"
	"math/rand"
	"runtime"
	"strconv"
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
	writeCount      = flag.Uint64("qw", 0, "number of write")
	queryCount      = flag.Uint64("qr", 0, "number of query")
	sampleCount     = flag.Uint64("qs", 2000, "number of samples")
	totalWrite      = uint64(0)
	totalQuery      = uint64(0)
	last            time.Time
	collsList       [][]*mgo.Collection
	samples         []map[string]string
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
	count := *writeCount
	for {
		if t = atomic.AddUint64(&totalWrite, 1); count > 0 && t > count {
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
			log.Println("INSERT", t, 100000/time.Since(last).Seconds())
			last = time.Now()
		}
	}
	done <- true
}

func query(colls []*mgo.Collection, done chan<- bool) {
	var t uint64
	count := *queryCount
	for {
		if t = atomic.AddUint64(&totalQuery, 1); count > 0 && t > count {
			break
		}

		n, err := colls[t%uint64(*dbCount)].Find(samples[rand.Intn(len(samples))]).Count()
		if err != nil {
			log.Println(err)
			continue
		}
		if n != 1 {
			log.Printf("Expected the query will got 1 record, but got %d\n", n)
		}
		if t%100000 == 0 {
			log.Println("QUERY", t, 100000/time.Since(last).Seconds())
			last = time.Now()
		}
	}
	done <- true
}

func readSamples(coll *mgo.Collection) {
	var (
		count   uint64
		rest    uint64 = *sampleCount
		results []map[string]string
		sample  map[string]string
		key     string
	)
	for {
		if rest >= 2000 {
			count = 2000
		} else {
			count = rest
		}

		aggregation := []bson.M{bson.M{"$sample": bson.M{"size": count}}}
		err := coll.Pipe(aggregation).All(&results)
		if err != nil {
			panic(err)
		}
		for _, result := range results {
			sample = make(map[string]string)
			for j := 0; j < rand.Intn(5)+1; j++ {
				key = "key" + strconv.Itoa(rand.Intn(20))
				sample[key] = result[key]
			}
			samples = append(samples, sample)
		}

		if rest > 2000 {
			rest -= 2000
		} else {
			break
		}
	}
}

func ensureIndexes(coll *mgo.Collection) {
	for i := 0; i < 20; i++ {
		err := coll.EnsureIndexKey("key" + strconv.Itoa(i))
		if err != nil {
			panic(err)
		}
	}
}

func main() {
	var (
		session *mgo.Session
		err     error
	)

	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())

	collsList := make([][]*mgo.Collection, *NumberGoroutine)
	for i := 0; i < *NumberGoroutine; i++ {
		session, err = mgo.DialWithTimeout(*host, 1*time.Minute)
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

	last = time.Now()
	if *writeCount > 0 {
		for i := 0; i < *NumberGoroutine; i++ {
			go write(collsList[i], done[i])
		}
		for i := 0; i < *NumberGoroutine; i++ {
			<-done[i]
		}
	}

	if *queryCount > 0 {
		samples = make([]map[string]string, 0, *sampleCount)
		readSamples(collsList[0][0])
		ensureIndexes(collsList[0][0])

		last = time.Now()
		for i := 0; i < *NumberGoroutine; i++ {
			go query(collsList[i], done[i])
		}
		for i := 0; i < *NumberGoroutine; i++ {
			<-done[i]
		}
	}

	for _, channel := range done {
		close(channel)
	}
}