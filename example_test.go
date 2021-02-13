// Copyright 2012 Evan Farrer. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package iothrottler_test

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"

	"github.com/efarrer/iothrottler"
)

// Basic usage of a IOThrottlerPool to throttle reading from a file
func ExampleIOThrottlerPool() {
	// Construct a bandwidth throttling pool that's limited to 100 bytes per second
	pool := iothrottler.NewIOThrottlerPool(iothrottler.BytesPerSecond * 100)

	file, err := os.Open("/dev/zero")
	if err != nil {
		// handle error
		return
	}
	defer file.Close()

	throttledFile, err := pool.AddReader(file)
	if err != nil {
		// handle error
		return
	}

	var zeros bytes.Buffer

	copied, err := io.CopyN(&zeros, throttledFile, 200)
	if err != nil {
		// handle error
	}

	fmt.Printf("Copied %v bytes\n", copied)
	// Output: Copied 200 bytes
}

// Throttle web requests using an IOThrottlerPool
func ExampleIOThrottlerPool_AddConn() {
	// Construct a bandwidth throttling pool that's limited to 30 kilobits per
	// second
	pool := iothrottler.NewIOThrottlerPool(iothrottler.Kbps * 30)

	// Create our own Dial function that will be used for the http connection
	throttledDial := func(nt, addr string) (c net.Conn, err error) {
		conn, err := net.Dial(nt, addr)
		if err != nil {
			return nil, err
		}

		return pool.AddConn(conn)
	}

	// Create a transport that will use our throttled Dial function
	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		Dial:  throttledDial,
	}

	// Download the page
	client := &http.Client{Transport: tr}
	resp, err := client.Get("http://www.google.com")
	if err != nil {
		// handle error
		return
	}
	defer resp.Body.Close()

	// Read the entire contents of the body
	_, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		// handle error
		return
	}

	fmt.Println("Downloaded www.google.com")
	// Output: Downloaded www.google.com
}
