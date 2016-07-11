package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"log"
	"math/rand"
	"net"
	"net/http"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/tuvistavie/securerandom"
	mgo "gopkg.in/mgo.v2"
	bson "gopkg.in/mgo.v2/bson"
)

var (
	NumberGoroutine = flag.Int("n", 1, "number of goruntine")
	url             = flag.String("url", "http://127.0.0.1", "url")
	writeCount      = flag.Uint64("qw", 0, "number of write")
	queryCount      = flag.Uint64("qr", 0, "number of query")
	sampleCount     = flag.Uint64("qs", 2000, "number of samples")
	frequency       = flag.Uint64("frequency", 100000, "benchmark frequency")
	mongoHost       = flag.String("mongo", "127.0.0.1", "mongo host")
	mongoDb         = flag.String("d", "test", "mongo db")
	mongoColl       = flag.String("c", "test", "mongo coll")
	totalWrite      = uint64(0)
	totalQuery      = uint64(0)
	last            time.Time
	samples         []map[string]string
)

type Doc map[string]string

func generateSecureRandomHex(n int) string {
	hex, err := securerandom.Hex(n >> 1)
	if err != nil {
		panic(err)
	}
	return hex
}

func write(client *http.Client, done chan<- bool) {
	var t uint64
	count := *writeCount
	doc := Doc{}
	for {
		if t = atomic.AddUint64(&totalWrite, 1); count > 0 && t > count {
			break
		}

		doc["key0"] = generateSecureRandomHex(128)
		doc["key1"] = generateSecureRandomHex(128)
		doc["key2"] = generateSecureRandomHex(128)
		doc["key3"] = generateSecureRandomHex(128)
		doc["key4"] = generateSecureRandomHex(128)
		doc["key5"] = generateSecureRandomHex(128)
		doc["key6"] = generateSecureRandomHex(128)
		doc["key7"] = generateSecureRandomHex(128)
		doc["key8"] = generateSecureRandomHex(128)
		doc["key9"] = generateSecureRandomHex(128)
		doc["key10"] = generateSecureRandomHex(128)
		doc["key11"] = generateSecureRandomHex(128)
		doc["key12"] = generateSecureRandomHex(128)
		doc["key13"] = generateSecureRandomHex(128)
		doc["key14"] = generateSecureRandomHex(128)
		doc["key15"] = generateSecureRandomHex(128)
		doc["key16"] = generateSecureRandomHex(128)
		doc["key17"] = generateSecureRandomHex(128)
		doc["key18"] = generateSecureRandomHex(128)
		doc["key19"] = generateSecureRandomHex(128)

		body, err := json.Marshal(&doc)
		if err != nil {
			panic(err)
		}

		req, err := http.NewRequest("POST", *url+"/", bytes.NewReader(body))
		if err != nil {
			log.Println(err)
			continue
		}
		req.Header["Content-Type"] = []string{"application/json"}

		resp, err := client.Do(req)
		if err != nil {
			log.Println(err)
			continue
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			log.Println("POST Error", resp.StatusCode)
			continue
		}

		if t%(*frequency) == 0 {
			log.Println("POST", t, float64(*frequency)/time.Since(last).Seconds())
			last = time.Now()
		}
	}
	done <- true
}

func query(client *http.Client, done chan<- bool) {
	var t uint64
	count := *queryCount
	for {
		if t = atomic.AddUint64(&totalQuery, 1); count > 0 && t > count {
			break
		}

		sample := samples[rand.Intn(len(samples))]
		body, err := json.Marshal(sample)
		if err != nil {
			panic(err)
		}

		req, err := http.NewRequest("GET", *url+"/", bytes.NewReader(body))
		if err != nil {
			log.Println(err)
			continue
		}
		req.Header["Content-Type"] = []string{"application/json"}

		resp, err := client.Do(req)
		if err != nil {
			log.Println(err)
			continue
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			log.Println("GET Error", resp.StatusCode)
			continue
		}

		if t%(*frequency) == 0 {
			log.Println("GET", t, float64(*frequency)/time.Since(last).Seconds())
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
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())

	transport := &http.Transport{
		Proxy:               nil,
		Dial:                (&net.Dialer{Timeout: 30 * time.Minute, KeepAlive: 30 * time.Minute}).Dial,
		MaxIdleConnsPerHost: 256,
	}
	client := http.Client{Transport: transport, Timeout: time.Duration(1 * time.Hour)}

	session, err := mgo.DialWithTimeout(*mongoHost, 1*time.Minute)
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()
	coll := session.DB(*mongoDb).C(*mongoColl)

	done := make([]chan bool, *NumberGoroutine)
	for i := 0; i < *NumberGoroutine; i++ {
		done[i] = make(chan bool, 1)
	}

	last = time.Now()
	if *writeCount > 0 {
		for i := 0; i < *NumberGoroutine; i++ {
			go write(&client, done[i])
		}
		for i := 0; i < *NumberGoroutine; i++ {
			<-done[i]
		}
	}

	if *queryCount > 0 {
		ensureIndexes(coll)
		readSamples(coll)

		last = time.Now()
		for i := 0; i < *NumberGoroutine; i++ {
			go query(&client, done[i])
		}
		for i := 0; i < *NumberGoroutine; i++ {
			<-done[i]
		}
	}

	for _, channel := range done {
		close(channel)
	}
}
