package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"flag"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	uuid "github.com/pborman/uuid"
	murmur3 "github.com/spaolacci/murmur3"
	mmap "golang.org/x/exp/mmap"
	mgo "gopkg.in/mgo.v2"
)

var (
	NumberGoroutine     = flag.Int("n", 1, "number of goruntine")
	url                 = flag.String("url", "http://127.0.0.1", "url")
	writeCount          = flag.Uint64("qw", 0, "number of write")
	queryCount          = flag.Uint64("qr", 0, "number of query")
	sampleCount         = flag.Uint64("qs", 2000, "number of samples")
	frequency           = flag.Uint64("frequency", 100000, "benchmark frequency")
	mongoHost           = flag.String("mongo", "127.0.0.1", "mongo host")
	mongoDb             = flag.String("d", "test", "mongo db")
	mongoColl           = flag.String("c", "test", "mongo coll")
	samplePath          = flag.String("sample-path", "samplefile.data", "Record all generated sample")
	sampleFile          *os.File
	totalWrite          = uint64(0)
	totalQuery          = uint64(0)
	last                time.Time
	sampleMemoryFile    *mmap.ReaderAt
	sampleMemoryFileLen int
)

type Doc map[string]string

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

func write(client *http.Client, done chan<- bool) {
	var t uint64
	count := *writeCount
	doc := Doc{}
	for {
		if t = atomic.AddUint64(&totalWrite, 1); count > 0 && t > count {
			break
		}

		doc["_id"] = uuid.New()

		hexes := generateRandomHexes()
		doc["key0"] = hexes[0]
		doc["key1"] = hexes[1]
		doc["key2"] = hexes[2]
		doc["key3"] = hexes[3]
		doc["key4"] = hexes[4]
		doc["key5"] = hexes[5]
		doc["key6"] = hexes[6]
		doc["key7"] = hexes[7]
		doc["key8"] = hexes[8]
		doc["key9"] = hexes[9]
		doc["key10"] = hexes[10]
		doc["key11"] = hexes[11]
		doc["key12"] = hexes[12]
		doc["key13"] = hexes[13]
		doc["key14"] = hexes[14]
		doc["key15"] = hexes[15]
		doc["key16"] = hexes[16]
		doc["key17"] = hexes[17]
		doc["key18"] = hexes[18]
		doc["key19"] = hexes[19]

		body, err := json.Marshal(&doc)
		if err != nil {
			log.Fatal(err)
		}

		req, err := http.NewRequest("POST", *url, bytes.NewReader(body))
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

		sampleFile.WriteString(hexes[0])

		if t%(*frequency) == 0 {
			log.Println("POST", t, float64(*frequency)/time.Since(last).Seconds())
			last = time.Now()
		}
	}
	done <- true
}

func getQueryBody() Doc {
	doc := Doc{}
	buf := make([]byte, 128)
	total := int32(sampleMemoryFileLen >> 7)
	if total == 0 {
		log.Fatalf("No data in %s to be read", *samplePath)
	}
	randPos := rand.Int31n(total)
	readLen, err := sampleMemoryFile.ReadAt(buf, int64(randPos)<<7)
	if err != nil {
		log.Fatal(err)
	}
	if readLen != 128 {
		log.Fatalf("Expect to read 128 bytes, but only %d bytes\n", readLen)
	}
	hex1 := string(buf[0:32])
	hex2 := string(buf[32:64])
	hex3 := string(buf[64:96])
	hex4 := string(buf[96:128])
	keyCount := rand.Int31n(5)
	for i := int32(0); i < keyCount; i++ {
		keyNum := rand.Int31n(20)
		switch keyNum {
		case 0:
			doc["key0"] = strings.Join([]string{hex1, hex2, hex3, hex4}, "")
		case 1:
			doc["key1"] = strings.Join([]string{hex1, hex2, hex4, hex3}, "")
		case 2:
			doc["key2"] = strings.Join([]string{hex1, hex3, hex2, hex4}, "")
		case 3:
			doc["key3"] = strings.Join([]string{hex1, hex3, hex4, hex2}, "")
		case 4:
			doc["key4"] = strings.Join([]string{hex1, hex4, hex2, hex3}, "")
		case 5:
			doc["key5"] = strings.Join([]string{hex1, hex4, hex3, hex2}, "")
		case 6:
			doc["key6"] = strings.Join([]string{hex2, hex1, hex3, hex4}, "")
		case 7:
			doc["key7"] = strings.Join([]string{hex2, hex1, hex4, hex3}, "")
		case 8:
			doc["key8"] = strings.Join([]string{hex2, hex3, hex1, hex4}, "")
		case 9:
			doc["key9"] = strings.Join([]string{hex2, hex3, hex4, hex1}, "")
		case 10:
			doc["key10"] = strings.Join([]string{hex2, hex4, hex3, hex2}, "")
		case 11:
			doc["key11"] = strings.Join([]string{hex2, hex4, hex2, hex3}, "")
		case 12:
			doc["key12"] = strings.Join([]string{hex3, hex1, hex2, hex4}, "")
		case 13:
			doc["key13"] = strings.Join([]string{hex3, hex1, hex4, hex2}, "")
		case 14:
			doc["key14"] = strings.Join([]string{hex3, hex2, hex1, hex4}, "")
		case 15:
			doc["key15"] = strings.Join([]string{hex3, hex2, hex4, hex1}, "")
		case 16:
			doc["key16"] = strings.Join([]string{hex3, hex4, hex1, hex2}, "")
		case 17:
			doc["key17"] = strings.Join([]string{hex3, hex4, hex2, hex1}, "")
		case 18:
			doc["key18"] = strings.Join([]string{hex4, hex1, hex2, hex3}, "")
		case 19:
			doc["key19"] = strings.Join([]string{hex4, hex1, hex3, hex2}, "")
		}
	}
	return doc
}

func query(client *http.Client, done chan<- bool) {
	var t uint64
	count := *queryCount
	for {
		if t = atomic.AddUint64(&totalQuery, 1); count > 0 && t > count {
			break
		}

		body, err := json.Marshal(getQueryBody())
		if err != nil {
			log.Fatal(err)
		}

		req, err := http.NewRequest("GET", *url, bytes.NewReader(body))
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

func ensureIndexes(coll *mgo.Collection) {
	for i := 0; i < 20; i++ {
		err := coll.EnsureIndexKey("key" + strconv.Itoa(i))
		if err != nil {
			log.Fatal(err)
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

	sampleFile, err = os.OpenFile(*samplePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		log.Fatal(err)
	}
	defer sampleFile.Close()

	transport := &http.Transport{
		Proxy:               nil,
		Dial:                (&net.Dialer{Timeout: 30 * time.Minute, KeepAlive: 30 * time.Minute}).Dial,
		MaxIdleConnsPerHost: 256,
	}
	client := http.Client{Transport: transport, Timeout: time.Duration(1 * time.Hour)}

	session, err = mgo.DialWithTimeout(*mongoHost, 1*time.Minute)
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
		ensureIndexes(coll)
		for i := 0; i < *NumberGoroutine; i++ {
			go write(&client, done[i])
		}
		for i := 0; i < *NumberGoroutine; i++ {
			<-done[i]
		}
	}

	if *queryCount > 0 {
		sampleMemoryFile, err = mmap.Open(*samplePath)
		defer sampleMemoryFile.Close()
		if err != nil {
			log.Fatal(err)
		}
		sampleMemoryFileLen = sampleMemoryFile.Len()

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
