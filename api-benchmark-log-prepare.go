package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"time"

	mgo "gopkg.in/mgo.v2"
	bson "gopkg.in/mgo.v2/bson"
)

var (
	mongoUrl   = flag.String("mongo", "127.0.0.1", "Mongo url")
	mongoDb    = flag.String("d", "test", "Mongo db")
	mongoColl  = flag.String("c", "test", "Mongo coll")
	batch      = flag.Int("batch", 100000, "Load batch at a time")
	prefetch   = flag.Float64("prefetch", 0.25, "Prefetch sets the point at which the next batch of results will be requested.")
	verbose    = flag.Bool("verbose", false, "verbose")
	debug      = flag.Bool("debug", false, "debug")
	samplePath = flag.String("sample-path", "samplefile.data", "Record all generated sample")
	frequency  = flag.Uint64("frequency", 100000, "output frequency")
)

type Sample map[string]string

func writer(source <-chan string, done chan<- bool) {
	defer close(done)
	sampleFile, err := os.OpenFile(*samplePath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0)
	if err != nil {
		log.Fatal(err)
	}
	defer sampleFile.Close()
	for key := range source {
		sampleFile.WriteString(key)
	}
	done <- true
}

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())

	if *verbose {
		logger := log.New(os.Stderr, "INFO", log.LstdFlags)
		mgo.SetLogger(logger)
		mgo.SetDebug(*debug)
	}

	session, err := mgo.DialWithTimeout(*mongoUrl, 1*time.Minute)
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()
	session.SetSyncTimeout(30 * time.Minute)
	session.SetSocketTimeout(30 * time.Minute)
	session.SetMode(mgo.Eventual, true)
	session.SetPrefetch(*prefetch)
	session.SetBatch(*batch)

	docInputChannel := make(chan string, 1024)
	writerDone := make(chan bool, 1)

	go writer(docInputChannel, writerDone)

	collection := session.DB(*mongoDb).C(*mongoColl)

	var sample Sample

	counter := uint64(0)
	iter := collection.Find(nil).Select(bson.M{"key0": 1}).Iter()
	for iter.Next(&sample) {
		docInputChannel <- sample["key0"]
		counter += 1
		if counter%(*frequency) == 0 {
			fmt.Printf("Sync %d records\n", counter)
		}
	}
	if err := iter.Close(); err != nil {
		log.Fatal(err)
	}
	close(docInputChannel)
	<-writerDone
}
