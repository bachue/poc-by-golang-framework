package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/tuvistavie/securerandom"
	mgo "gopkg.in/mgo.v2"
	bson "gopkg.in/mgo.v2/bson"
)

func RandomString(strlen int) string {
	result, err := securerandom.Hex(strlen >> 1)
	if err != nil {
		panic(err)
	}
	return result
}

type Doc map[string]string

type Server struct {
	debug       bool
	verbose     bool
	idx         uint32
	keyCount    int
	valueLength int
	colls       []*mgo.Collection
	samples     []Doc
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
	log.Println(r.Method, r.ContentLength, r.URL.Path, time.Since(start).Seconds()*1000, "ms")
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
	for i := 0; i < s.keyCount; i++ {
		doc[fmt.Sprintf("key%v", i)] = RandomString(s.valueLength)
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
	mgoAddr := flag.String("addr", "127.0.0.1", "mongodb addr")
	db := flag.String("db", "poc-go", "db")
	coll := flag.String("coll", "coll", "collection")
	listenAddr := flag.String("listen", ":9876", "server listen addr")
	keyCount := flag.Int("count", 20, "key count")
	valueLength := flag.Int("value", 128, "value length")
	sessionCount := flag.Int("session-count", 20, "Mongodb Session Count")
	verbose := flag.Bool("verbose", false, "verbose mode")
	debug := flag.Bool("debug", false, "debug mode")
	flag.Parse()
	log.Println("server running at", *listenAddr)

	if *verbose {
		logger := log.New(os.Stdout, "INFO: ", log.LstdFlags)
		mgo.SetLogger(logger)
		mgo.SetDebug(*debug)
	}

	s, err := mgo.Dial(*mgoAddr)
	if err != nil {
		log.Fatal(err)
	}
	s.SetMode(mgo.Nearest, true)
	s.SetPoolLimit(1048560)

	sessions := make([]*mgo.Session, *sessionCount)
	sessions[0] = s
	for i := 1; i < *sessionCount; i++ {
		sessions[i] = s.Copy()
	}

	server := &Server{
		verbose:     *verbose,
		debug:       *debug,
		keyCount:    *keyCount,
		valueLength: *valueLength,
		colls:       make([]*mgo.Collection, *sessionCount),
	}
	for i, session := range sessions {
		defer session.Close()
		server.colls[i] = session.DB(*db).C(*coll)
	}
	http.HandleFunc("/", server.Root)
	log.Fatal(http.ListenAndServe(*listenAddr, nil))
}
