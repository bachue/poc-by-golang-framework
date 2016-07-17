package main

import (
	"bufio"
	"encoding/hex"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pborman/uuid"
	murmur3 "github.com/spaolacci/murmur3"
	mgo "gopkg.in/mgo.v2"
	bson "gopkg.in/mgo.v2/bson"
)

var (
	NumberGoroutine   = flag.Int("n", 1, "number of goruntine")
	host              = flag.String("h", "127.0.0.1", "host")
	db                = flag.String("d", "test", "db")
	coll              = flag.String("c", "test", "coll")
	dbCount           = flag.Int("dbs", 1, "db count")
	writeCount        = flag.Uint64("qw", 0, "number of write")
	queryCount        = flag.Uint64("qr", 0, "number of query")
	frequency         = flag.Uint64("frequency", 100000, "benchmark frequency")
	verbose           = flag.Bool("verbose", false, "verbose")
	debug             = flag.Bool("debug", false, "debug")
	samplePath        = flag.String("sample-path", "samplefile.data", "Record all generated sample")
	sampleFile        *os.File
	totalWrite        = uint64(0)
	totalQuery        = uint64(0)
	last              time.Time
	collsList         [][]*mgo.Collection
	sampleFileContent []byte
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

func write(colls []*mgo.Collection, wg *sync.WaitGroup) {
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

		sampleFile.WriteString(hexes[0])

		if t%(*frequency) == 0 {
			log.Println("INSERT", t, float64(*frequency)/time.Since(last).Seconds())
			last = time.Now()
		}
	}
	wg.Done()
}

func getQueryBody() map[string]string {
	doc := make(map[string]string)
	total := int32(len(sampleFileContent) >> 7)
	if total == 0 {
		log.Fatalf("No data in %s to be read", *samplePath)
	}
	randPos := rand.Int31n(total)
	buf := sampleFileContent[(int64(randPos) << 7):(int64(randPos)<<7 + 128)]
	hex1 := string(buf[0:32])
	hex2 := string(buf[32:64])
	hex3 := string(buf[64:96])
	hex4 := string(buf[96:128])
	keyCount := rand.Int31n(5) + 1
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

func query(colls []*mgo.Collection, wg *sync.WaitGroup) {
	var t uint64
	var results []map[string]string

	count := *queryCount
	for {
		if t = atomic.AddUint64(&totalQuery, 1); count > 0 && t > count {
			break
		}

		query := getQueryBody()
		err := colls[t%uint64(*dbCount)].Find(query).All(&results)
		if err != nil {
			log.Println(err)
			continue
		}
		if len(results) != 1 {
			log.Printf("Expected the query will got 1 record, but got %d\nQuery Condition: %v\n", len(results), query)
		}
		if t%(*frequency) == 0 {
			log.Println("QUERY", t, float64(*frequency)/time.Since(last).Seconds())
			last = time.Now()
		}
	}
	wg.Done()
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
		wg      sync.WaitGroup
		err     error
	)

	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())

	sampleFile, err = os.OpenFile(*samplePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		log.Fatal(err)
	}
	defer sampleFile.Close()

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

	if *writeCount > 0 {
		ensureIndexes(collsList[0][0])

		wg.Add(*NumberGoroutine)

		last = time.Now()
		for i := 0; i < *NumberGoroutine; i++ {
			go write(collsList[i], &wg)
		}
		wg.Wait()
	}

	if *queryCount > 0 {
		file, err := os.OpenFile(*samplePath, os.O_RDONLY, 0)
		if err != nil {
			log.Fatal(err)
		}

		sampleFileContent, err = ioutil.ReadAll(file)
		file.Close()
		if err != nil {
			log.Fatal(err)
		}

		reader := bufio.NewReader(os.Stdin)
		fmt.Println("Press Enter to continue ...")
		_, _ = reader.ReadString('\n')

		wg.Add(*NumberGoroutine)

		last = time.Now()
		for i := 0; i < *NumberGoroutine; i++ {
			go query(collsList[i], &wg)
		}
		wg.Wait()
	}
}
