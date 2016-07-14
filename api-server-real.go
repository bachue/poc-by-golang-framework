package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	mgo "gopkg.in/mgo.v2"
)

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
	case "GET":
		s.find(w, r)
	case "POST":
		s.insert(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
	if s.verbose {
		log.Println(r.Method, r.ContentLength, r.URL.Path, time.Since(start).Seconds()*1000, "ms")
	}
}

func (s *Server) find(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	if len(r.Header["Content-Type"]) <= 0 || !strings.Contains(r.Header["Content-Type"][0], "json") {
		log.Printf("Content-Type must be JSON but %d\n", r.Header["Content-Type"])
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println("Read Body Failed", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	r.Body.Close()

	var query map[string]string
	err = json.Unmarshal(body, &query)
	if err != nil {
		log.Println("Parse Body Failed", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	var results []map[string]string
	err = s.getCollection().Find(query).Limit(1).All(&results)
	if err != nil {
		log.Println("Find from db Failed", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if len(results) > 0 {
		respBody, err := json.Marshal(results[0])
		if err != nil {
			log.Println("Marshal JSON Error", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Header().Add("Content-Type", "application/json")
		_, err = w.Write(respBody)
		if err != nil {
			log.Println("Write Response Error", err)
		}
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

func (s *Server) insert(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	if len(r.Header["Content-Type"]) <= 0 || !strings.Contains(r.Header["Content-Type"][0], "json") {
		log.Printf("Content-Type must be JSON but %d\n", r.Header["Content-Type"])
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println("Read Body Failed", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	r.Body.Close()

	var doc map[string]string
	err = json.Unmarshal(body, &doc)
	if err != nil {
		log.Println("Parse Body Failed", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	err = s.getCollection().Insert(doc)
	if err != nil {
		log.Println("Insert to db Failed", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
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
