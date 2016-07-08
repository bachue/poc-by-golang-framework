package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"time"

	mgo "gopkg.in/mgo.v2"
	bson "gopkg.in/mgo.v2/bson"
)

const chars = "abcdefghijklmnopqrstuvwxyz0123456789"

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

func RandomString(strlen int) string {
	result := make([]byte, strlen)
	for i := 0; i < strlen; i++ {
		result[i] = chars[rand.Intn(len(chars))]
	}
	return string(result)
}

type Doc map[string]string

type Server struct {
	debug       bool
	keyCount    int
	valueLength int
	coll        *mgo.Collection
}

var doc = Doc{}

func (s *Server) Root(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	switch r.Method {
	case "GET", "HEAD":
		s.find(w, r)
	case "POST":
		s.insert(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
	log.Println(r.Method, r.ContentLength, r.URL.Path, time.Since(start).Seconds()*1000, "ms")
}

func (s *Server) find(w http.ResponseWriter, r *http.Request) {
	query := bson.M{}
	params := r.URL.Query()
	for key, values := range params {
		if strings.HasPrefix(key, "key") && len(values) > 0 {
			query[key] = values[0]
		}
	}
	n, err := s.coll.Find(query).Count()
	if err != nil {
		log.Println("find from db failed", err)
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
	err := s.coll.Insert(doc)
	if err != nil {
		log.Println("insert to db failed", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func main() {
	mgoAddr := flag.String("addr", "127.0.0.1", "mongodb addr")
	db := flag.String("db", "poc-go", "db")
	coll := flag.String("coll", "coll", "collection")
	listenAddr := flag.String("listen", ":9876", "server listen addr")
	keyCount := flag.Int("count", 20, "key count")
	valueLength := flag.Int("value", 128, "value length")
	debug := flag.Bool("debug", true, "debug mode")
	flag.Parse()
	log.Println("server running at", *listenAddr)
	s, err := mgo.Dial(*mgoAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()
	s.SetMode(mgo.Nearest, true)
	s.SetPoolLimit(1048560)

	for i := 0; i < *keyCount; i++ {
		doc[fmt.Sprintf("key%v", i)] = RandomString(*valueLength)
	}
	server := &Server{
		debug:       *debug,
		keyCount:    *keyCount,
		valueLength: *valueLength,
	}
	server.coll = s.DB(*db).C(*coll)
	http.HandleFunc("/", server.Root)
	log.Fatal(http.ListenAndServe(*listenAddr, nil))
}
