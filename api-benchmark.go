package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"runtime"
	"sync/atomic"
	"time"
)

var (
	NumberGoroutine = flag.Int("n", 1, "number of goruntine")
	url             = flag.String("url", "http://127.0.0.1", "url")
	writeCount      = flag.Uint64("qw", 0, "number of write")
	queryCount      = flag.Uint64("qr", 0, "number of query")
	sampleCount     = flag.Uint64("qs", 2000, "number of samples")
	frequency       = flag.Uint64("frequency", 100000, "benchmark frequency")
	totalWrite      = uint64(0)
	totalQuery      = uint64(0)
	last            time.Time
)

func write(client *http.Client, done chan<- bool) {
	var t uint64
	count := *writeCount
	for {
		if t = atomic.AddUint64(&totalWrite, 1); count > 0 && t > count {
			break
		}

		req, err := http.NewRequest("POST", *url+"/", nil)
		if err != nil {
			log.Println(err)
			continue
		}

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

		req, err := http.NewRequest("GET", *url+"/", nil)
		if err != nil {
			log.Println(err)
			continue
		}

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

func prepareForSearch(client *http.Client) {
	req, err := http.NewRequest("PUT", *url+"/?count=2000", nil)
	if err != nil {
		panic(err)
	}

	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		panic(fmt.Sprintf("PUT Error: %d\n", resp.StatusCode))
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
		prepareForSearch(&client)

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
