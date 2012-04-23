iothrottler
===========

A Go package for throttling application IO (such as bandwidth).

Installation
------------

If you have the Go Language installed type
`go get "github.com/efarrer/iothrottler"`

Example
-------
// Download google's home page at 100 bytes per second and print how many bytes were downloaded

package main

import (
    "github.com/efarrer/iothrottler"
    "io/ioutil"
    "log"
    "net"
    "net/http"
)

func main() {

    // Construct a bandwidth throttling pool that's limited to 100 bytes per second
    pool := iothrottler.NewIOThrottlerPool(iothrottler.BytesPerSecond * 100)
    defer pool.ReleasePool()

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
        DisableCompression: true,
        Dial:               throttledDial,
    }

    //  Download the page
    client := &http.Client{Transport: tr}
    resp, err := client.Get("http://www.google.com")
    if err != nil {
        log.Fatal(err)
    }
    defer resp.Body.Close()

    // Read the entire contents of the body
    body, err := ioutil.ReadAll(resp.Body)
    if err != nil {
        log.Fatal(err)
    }

    // Print the body length
    println(len(body))
}
