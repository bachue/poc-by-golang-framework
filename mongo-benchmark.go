package main

import (
	"encoding/hex"
	"flag"
	"log"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pborman/uuid"
	murmur3 "github.com/spaolacci/murmur3"
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
	frequency       = flag.Uint64("frequency", 100000, "benchmark frequency")
	verbose         = flag.Bool("verbose", false, "verbose")
	debug           = flag.Bool("debug", false, "debug")
	totalWrite      = uint64(0)
	totalQuery      = uint64(0)
	last            time.Time
	collsList       [][]*mgo.Collection
	samples         []map[string]string
)

func generateMurmur3() []byte {
	var bytesArray [16]byte

	lpointer := unsafe.Pointer(&bytesArray[0])
	hpointer := unsafe.Pointer(&bytesArray[8])
	*(*int64)(lpointer) = time.Now().UnixNano()

	hasher := murmur3.New128()
	hasher.Write(bytesArray[0:8])
	r1, r2 := hasher.Sum128()

	*(*uint64)(lpointer) = r1
	*(*uint64)(hpointer) = r2

	return bytesArray[:]
}

func generateRandomHexes() [20]string {
	var bytes1 []byte = generateMurmur3()
	var bytes2 []byte = generateMurmur3()
	var bytes3 []byte = generateMurmur3()
	var bytes4 []byte = generateMurmur3()
	var hex1 string = hex.EncodeToString(bytes1)
	var hex2 string = hex.EncodeToString(bytes2)
	var hex3 string = hex.EncodeToString(bytes3)
	var hex4 string = hex.EncodeToString(bytes4)
	return [20]string{
		strings.Join([]string{hex1, hex2, hex3, hex4}, ""),
		strings.Join([]string{hex1, hex2, hex4, hex3}, ""),
		strings.Join([]string{hex1, hex3, hex2, hex4}, ""),
		strings.Join([]string{hex1, hex3, hex4, hex2}, ""),
		strings.Join([]string{hex1, hex4, hex2, hex3}, ""),
		strings.Join([]string{hex1, hex4, hex3, hex2}, ""),
		strings.Join([]string{hex2, hex1, hex3, hex4}, ""),
		strings.Join([]string{hex2, hex1, hex4, hex3}, ""),
		strings.Join([]string{hex2, hex3, hex1, hex4}, ""),
		strings.Join([]string{hex2, hex3, hex4, hex1}, ""),
		strings.Join([]string{hex2, hex4, hex3, hex2}, ""),
		strings.Join([]string{hex2, hex4, hex2, hex3}, ""),
		strings.Join([]string{hex3, hex1, hex2, hex4}, ""),
		strings.Join([]string{hex3, hex1, hex4, hex2}, ""),
		strings.Join([]string{hex3, hex2, hex1, hex4}, ""),
		strings.Join([]string{hex3, hex2, hex4, hex1}, ""),
		strings.Join([]string{hex3, hex4, hex1, hex2}, ""),
		strings.Join([]string{hex3, hex4, hex2, hex1}, ""),
		strings.Join([]string{hex4, hex1, hex2, hex3}, ""),
		strings.Join([]string{hex4, hex1, hex3, hex2}, ""),
	}
}

func write(colls []*mgo.Collection, done chan<- bool) {
	var t uint64
	count := *writeCount
	for {
		if t = atomic.AddUint64(&totalWrite, 1); count > 0 && t > count {
			break
		}

		hexes := generateRandomHexes()
		err := colls[t%uint64(*dbCount)].Insert(bson.M{
			"_id":   uuid.New(),
			"key0":  hexes[0],
			"key1":  hexes[1],
			"key2":  hexes[2],
			"key3":  hexes[3],
			"key4":  hexes[4],
			"key5":  hexes[5],
			"key6":  hexes[6],
			"key7":  hexes[7],
			"key8":  hexes[8],
			"key9":  hexes[9],
			"key10": hexes[10],
			"key11": hexes[11],
			"key12": hexes[12],
			"key13": hexes[13],
			"key14": hexes[14],
			"key15": hexes[15],
			"key16": hexes[16],
			"key17": hexes[17],
			"key18": hexes[18],
			"key19": hexes[19],
		})
		if err != nil {
			log.Println(err)
			continue
		}
		if t%(*frequency) == 0 {
			log.Println("INSERT", t, float64(*frequency)/time.Since(last).Seconds())
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
		if t%(*frequency) == 0 {
			log.Println("QUERY", t, float64(*frequency)/time.Since(last).Seconds())
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

	if *verbose {
		logger := log.New(os.Stderr, "INFO", log.LstdFlags)
		mgo.SetLogger(logger)
		mgo.SetDebug(*debug)
	}

	collsList := make([][]*mgo.Collection, *NumberGoroutine)
	for i := 0; i < *NumberGoroutine; i++ {
		session, err = mgo.DialWithTimeout(*host, 1*time.Minute)
		if err != nil {
			log.Fatal(err)
		}
		session.SetSyncTimeout(10 * time.Minute)
		session.SetSocketTimeout(10 * time.Minute)
		session.SetMode(mgo.Eventual, true)
		defer session.Close()
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
		ensureIndexes(collsList[0][0])
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
