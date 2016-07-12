package main

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	mgo "gopkg.in/mgo.v2"
	bson "gopkg.in/mgo.v2/bson"
)

func generateRandomMd5() []byte {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, time.Now().UnixNano())
	if err != nil {
		panic(err)
	}
	array := md5.Sum(buf.Bytes())
	return array[:]
}

func generateRandomHexes() [20]string {
	var bytes1 []byte = generateRandomMd5()
	var bytes2 []byte = generateRandomMd5()
	var bytes3 []byte = generateRandomMd5()
	var bytes4 []byte = generateRandomMd5()
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

type Doc map[string]string

type Server struct {
	debug   bool
	verbose bool
	idx     uint32
	colls   []*mgo.Collection
	samples []Doc
}

func (s *Server) Root(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	switch r.Method {
	case "GET", "HEAD":
		s.find(w, r)
	case "POST":
		s.insert(w, r)
	case "PUT":
		s.createSamples(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
	if s.verbose {
		log.Println(r.Method, r.ContentLength, r.URL.Path, time.Since(start).Seconds()*1000, "ms")
	}
}

func (s *Server) find(w http.ResponseWriter, r *http.Request) {
	if len(s.samples) == 0 {
		log.Println("Call PUT / First")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	sample := s.samples[rand.Intn(len(s.samples))]

	if s.verbose {
		log.Println("Query:", sample)
	}

	n, err := s.getCollection().Find(sample).Count()
	if err != nil {
		log.Println("Find from db failed", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if n > 0 {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

func (s *Server) insert(w http.ResponseWriter, r *http.Request) {
	doc := Doc{}
	hexes := generateRandomHexes()
	for i := 0; i < 20; i++ {
		doc[fmt.Sprintf("key%v", i)] = hexes[i]
	}
	err := s.getCollection().Insert(doc)
	if err != nil {
		log.Println("Insert to db failed", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (s *Server) createSamples(w http.ResponseWriter, r *http.Request) {
	params := r.URL.Query()

	for i := 0; i < 20; i++ {
		err := s.getCollection().EnsureIndexKey("key" + strconv.Itoa(i))
		if err != nil {
			log.Println("Failed to ensure index", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}

	if params["count"] != nil && len(params["count"]) > 0 {
		count, err := strconv.Atoi(params["count"][0])
		if err != nil {
			log.Println("Failed to create samples", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		aggregation := []bson.M{bson.M{"$sample": bson.M{"size": count}}}

		var results []Doc
		err = s.getCollection().Pipe(aggregation).All(&results)
		if err != nil {
			log.Println("Failed to create samples", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		s.samples = make([]Doc, count)
		for i, result := range results {
			sample := make(Doc)
			for j := 0; j < rand.Intn(5); j++ {
				key := "key" + strconv.Itoa(rand.Intn(20))
				sample[key] = result[key]
			}
			s.samples[i] = sample
		}
	}
	w.WriteHeader(http.StatusCreated)
}

func (s *Server) getCollection() *mgo.Collection {
	id := atomic.AddUint32(&s.idx, 1) % uint32(len(s.colls))
	return s.colls[id]
}

func main() {
	mgoAddrs := flag.String("addrs", "127.0.0.1", "mongodb addrs")
	db := flag.String("db", "poc-go", "db")
	coll := flag.String("coll", "coll", "collection")
	listenAddr := flag.String("listen", ":9876", "server listen addr")
	sessionCount := flag.Int("session-count", 10, "Mongodb Session Count for each addr")
	verbose := flag.Bool("verbose", false, "verbose mode")
	debug := flag.Bool("debug", false, "debug mode")
	flag.Parse()
	log.Println("server running at", *listenAddr)

	if *verbose {
		logger := log.New(os.Stdout, "INFO: ", log.LstdFlags)
		mgo.SetLogger(logger)
		mgo.SetDebug(*debug)
	}

	addrs := strings.Split(*mgoAddrs, ",")
	server := &Server{
		verbose: *verbose,
		debug:   *debug,
		colls:   make([]*mgo.Collection, (*sessionCount)*len(addrs)),
	}

	for i := 0; i < (*sessionCount)*len(addrs); i++ {
		s, err := mgo.Dial(addrs[i%len(addrs)])
		if err != nil {
			log.Fatal(err)
		}
		s.SetPoolLimit(1048560)
		defer s.Close()
		server.colls[i] = s.DB(*db).C(*coll)
	}

	http.HandleFunc("/", server.Root)
	log.Fatal(http.ListenAndServe(*listenAddr, nil))
}
