// Copyright 2012 Evan Farrer. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package iothrottler_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"testing"
	"time"

	"github.com/efarrer/iothrottler"
)

// Returns the first error or nil if none are errors
func orErrors(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

type Fataler interface {
	Helper()
	Fatalf(format string, args ...interface{})
}

func assertNoError(t Fataler, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("Expecting %v to be nil but it isn't.", err)
	}
}

func assertError(t Fataler, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("Expecting %v to be not nil but it is.", err)
	}
}

/*
 * A simple timer for seeing how long an operation lasts
 */
type timer time.Time

func startTimer() timer {
	return timer(time.Now())
}

type Seconds int64

func (t timer) elapsedSeconds() Seconds {
	milliseconds := time.Now().Sub(time.Time(t)) / time.Millisecond

	// Round to the nearest second
	seconds := ((milliseconds + 500) / 1000)

	return Seconds(seconds)
}

/*
 * Copy 'data' to 'writer' and read it from 'reader' return now long it took
 */
func timePipeTransmittion(data []byte, reader io.Reader, writer io.Writer) (Seconds, error) {

	serverError := make(chan error)
	// Write the data
	go func() {
		count, err := writer.Write(data)
		if count < len(data) {
			serverError <- errors.New("Didn't write full bytes")
		} else {
			serverError <- err
		}
	}()

	buffer := make([]byte, len(data))

	// Read the data
	timer := startTimer()
	count, err := io.ReadAtLeast(reader, buffer, len(data))
	elapsed := timer.elapsedSeconds()
	var countError error = nil
	if count != len(data) {
		countError = errors.New(fmt.Sprintf("Didn't read enough data. Read %v expected %v", count, len(data)))
	}
	return elapsed, orErrors(countError, orErrors(err, <-serverError))
}

/*
 * Copy 'data' from 'reader' to 'writer' and assert the it happens in exactly
 * 'expectedDelay' seconds
 */
func assertTransmitTime(data []byte, reader io.Reader, writer io.Writer, expectedDelay Seconds, t *testing.T) {
	t.Helper()
	elapsedSeconds, err := timePipeTransmittion(data, reader, writer)
	if elapsedSeconds != expectedDelay {
		t.Fatalf("Expecting read to take %v seconds but it took %v instead", expectedDelay, elapsedSeconds)
	}
	assertNoError(t, err)
}

/**
 * Creates a connected TCP pipe (like io.Pipe() but with TCP sockets)
 */
func createTcpPipe(t *testing.T) (net.Conn, net.Conn) {
	addr := "localhost:8080"

	serverConn := make(chan net.Conn)

	ln, err := net.Listen("tcp", addr)
	assertNoError(t, err)
	defer ln.Close()
	go func() {
		server, _ := ln.Accept()
		serverConn <- server
	}()

	// Connect the client socket
	client, err := net.Dial("tcp", addr)
	assertNoError(t, err)

	return client, <-serverConn
}

/*
 * Aggressive writes data to writer and reads data from reader
 */
func dosPipe(reader io.Reader, writer io.Writer) {
	go func() {
		data := []byte("0123456789")
		for {
			_, err := writer.Write(data)
			if nil != err {
				return
			}
		}
	}()

	go func() {
		for {
			buffer := make([]byte, 10)
			_, err := reader.Read(buffer)
			if nil != err {
				return
			}
		}
	}()
}

/*
 * Make sure bandwidth conversions make senct
 */
func TestBandwidthConversion(t *testing.T) {
	for i := iothrottler.Bandwidth(0); i != 1000; i++ {
		if iothrottler.Kbps*i != iothrottler.BytesPerSecond*(i*1024/8) {
			t.Fatalf("Bad conversion from Kbps to Bandwidth %v", i)
		}
		if iothrottler.Mbps*i != iothrottler.BytesPerSecond*i*1024*1024/8 {
			t.Fatalf("Bad conversion from Mbps to Bandwidth %v", i)
		}
		if iothrottler.Gbps*i != iothrottler.BytesPerSecond*i*1024*1024*1024/8 {
			t.Fatalf("Bad conversion from Gbps to Bandwidth %v", i)
		}
	}
}

/*
 * Test unlimited bandwidth value
 */
func TestUnlimitedBandwith(t *testing.T) {
	max := int64(math.MaxInt64)
	if int64(iothrottler.Unlimited) != max {
		t.Fatalf("Unlimited isn't quite as unlimited as it should be %v vs %v", int64(iothrottler.Unlimited), max)
	}
}

/*
 * Make sure we can allocate then release a pool without crashing
 */
func TestCreatingAPool(t *testing.T) {
	iothrottler.NewIOThrottlerPool(iothrottler.Unlimited).ReleasePool()
}

/*
 * Make sure adding to a released pool returns an error
 */
func TestCantAddToAReleasedPool(t *testing.T) {
	pool := iothrottler.NewIOThrottlerPool(iothrottler.Unlimited)
	pool.ReleasePool()

	readEnd, writeEnd := io.Pipe()
	defer readEnd.Close()
	defer writeEnd.Close()

	// Should return an error
	_, err := pool.AddWriter(writeEnd)
	assertError(t, err)

	_, err = pool.AddReader(readEnd)
	assertError(t, err)

	client, server := createTcpPipe(t)
	defer client.Close()
	defer server.Close()

	_, err = pool.AddReadWriter(client)
	assertError(t, err)

	_, err = pool.AddConn(client)
	assertError(t, err)
}

/*
 * Make sure we don't crash or do anything crazy if we release a pool twice
 */
func TestReleasingAPoolTwiceIsNoop(t *testing.T) {
	pool := iothrottler.NewIOThrottlerPool(iothrottler.Unlimited)
	pool.ReleasePool()
	pool.ReleasePool()
}

/*
 * Make sure closing the bandwidth limited reader/writter closes the original
 * reader/writer
 */
func TestCloseThrottledClosesOriginal(t *testing.T) {
	// Test closing throttled writer
	println("\tClose writer")
	{
		// One byte a second
		pool := iothrottler.NewIOThrottlerPool(iothrottler.BytesPerSecond)
		defer pool.ReleasePool()

		readEnd, writeEnd := io.Pipe()

		throttleWriteEnd, err := pool.AddWriter(writeEnd)
		assertNoError(t, err)
		throttleWriteEnd.Close()

		buffer := make([]byte, 10)
		count, err := readEnd.Read(buffer)
		if count != 0 {
			t.Fatalf("Shouldn't read any bytes from the closed connection. Read %v\n", count)
		}
		if nil == err {
			t.Fatalf("Should get error, but we didn't\n")
		}

		// Trying to write to writeEnd would hang
	}

	// Test closing throttled reader
	println("\tClose reader")
	{
		// One byte a second
		pool := iothrottler.NewIOThrottlerPool(iothrottler.BytesPerSecond)
		defer pool.ReleasePool()

		readEnd, writeEnd := io.Pipe()

		throttledReadEnd, err := pool.AddReader(readEnd)
		assertNoError(t, err)
		throttledReadEnd.Close()

		buffer := make([]byte, 10)
		count, err := readEnd.Read(buffer)
		if count != 0 {
			t.Fatalf("Shouldn't read any bytes from the closed connection. Read %v\n", count)
		}
		if nil == err {
			t.Fatalf("Should get error, but we didn't\n")
		}

		count, err = writeEnd.Write(buffer)
		if count != 0 {
			t.Fatalf("Shouldn't write any bytes from the closed connection. Wrote %v\n", count)
		}
		if nil == err {
			t.Fatalf("Should get error, but we didn't\n")
		}
	}

	// Test closing throttled readwriter
	println("\tClose readwriter")
	{
		// One byte a second
		pool := iothrottler.NewIOThrottlerPool(iothrottler.BytesPerSecond)
		defer pool.ReleasePool()

		client, server := createTcpPipe(t)
		defer client.Close()
		defer server.Close()

		throttleClient, err := pool.AddReadWriter(client)
		assertNoError(t, err)
		throttleClient.Close()

		buffer := make([]byte, 10)

		// Client should be closed for writing
		count, err := client.Write(buffer)
		if count != 0 {
			t.Fatalf("Shouldn't write any bytes from the closed connection. Wrote %v\n", count)
		}
		if nil == err {
			t.Fatalf("Should get error, but we didn't\n")
		}

		// Client should be closed for reading
		count, err = client.Read(buffer)
		if count != 0 {
			t.Fatalf("Shouldn't read any bytes from the closed connection. Read %v\n", count)
		}
		if nil == err {
			t.Fatalf("Should get error, but we didn't\n")
		}

		// Server shouldn't be able to read
		count, err = server.Read(buffer)
		if count != 0 {
			t.Fatalf("Shouldn't read any bytes from the closed connection. Read %v\n", count)
		}
		if nil == err {
			t.Fatalf("Should get error, but we didn't\n")
		}

		// Server can still write (even though client is closed)
	}

	// Test closing throttled net.Conn
	println("\tClose net.Conn")
	{
		// One byte a second
		pool := iothrottler.NewIOThrottlerPool(iothrottler.BytesPerSecond)
		defer pool.ReleasePool()

		client, server := createTcpPipe(t)
		defer client.Close()
		defer server.Close()

		throttleClient, err := pool.AddConn(client)
		assertNoError(t, err)
		throttleClient.Close()

		buffer := make([]byte, 10)

		// Client should be closed for writing
		count, err := client.Write(buffer)
		if count != 0 {
			t.Fatalf("Shouldn't write any bytes from the closed connection. Wrote %v\n", count)
		}
		if nil == err {
			t.Fatalf("Should get error, but we didn't\n")
		}

		// Client should be closed for reading
		count, err = client.Read(buffer)
		if count != 0 {
			t.Fatalf("Shouldn't read any bytes from the closed connection. Read %v\n", count)
		}
		if nil == err {
			t.Fatalf("Should get error, but we didn't\n")
		}

		// Server shouldn't be able to read
		count, err = server.Read(buffer)
		if count != 0 {
			t.Fatalf("Shouldn't read any bytes from the closed connection. Read %v\n", count)
		}
		if nil == err {
			t.Fatalf("Should get error, but we didn't\n")
		}

		// Server can still write (even though client is closed)
	}
	println("\tClose done")
}

/*
 * Make sure closing the original reader/writer closes the bandwidth limited
 * reader/writer
 */
func TestCloseOriginalClosesThrottled(t *testing.T) {
	// Test closing original writer
	{
		// One byte a second
		pool := iothrottler.NewIOThrottlerPool(iothrottler.BytesPerSecond)
		defer pool.ReleasePool()

		readEnd, writeEnd := io.Pipe()

		_, err := pool.AddWriter(writeEnd)
		assertNoError(t, err)
		writeEnd.Close()

		buffer := make([]byte, 10)
		count, err := readEnd.Read(buffer)
		if count != 0 {
			t.Fatalf("Shouldn't read any bytes from the closed connection. Read %v\n", count)
		}
		if nil == err {
			t.Fatalf("Should get error, but we didn't\n")
		}

		// Trying to write to writeEnd would hang
	}

	// Test closing original reader
	{
		// One byte a second
		pool := iothrottler.NewIOThrottlerPool(iothrottler.BytesPerSecond)
		defer pool.ReleasePool()

		readEnd, writeEnd := io.Pipe()

		_, err := pool.AddReader(readEnd)
		assertNoError(t, err)
		readEnd.Close()

		buffer := make([]byte, 10)
		count, err := readEnd.Read(buffer)
		if count != 0 {
			t.Fatalf("Shouldn't read any bytes from the closed connection. Read %v\n", count)
		}
		if nil == err {
			t.Fatalf("Should get error, but we didn't\n")
		}

		count, err = writeEnd.Write(buffer)
		if count != 0 {
			t.Fatalf("Shouldn't write any bytes from the closed connection. Wrote %v\n", count)
		}
		if nil == err {
			t.Fatalf("Should get error, but we didn't\n")
		}
	}

	// Test closing original readwriter
	{
		// One byte a second
		pool := iothrottler.NewIOThrottlerPool(iothrottler.BytesPerSecond)
		defer pool.ReleasePool()

		client, server := createTcpPipe(t)
		defer client.Close()
		defer server.Close()

		_, err := pool.AddReadWriter(client)
		assertNoError(t, err)
		client.Close()

		buffer := make([]byte, 10)

		// Client should be closed for writing
		count, err := client.Write(buffer)
		if count != 0 {
			t.Fatalf("Shouldn't write any bytes from the closed connection. Wrote %v\n", count)
		}
		if nil == err {
			t.Fatalf("Should get error, but we didn't\n")
		}

		// Client should be closed for reading
		count, err = client.Read(buffer)
		if count != 0 {
			t.Fatalf("Shouldn't read any bytes from the closed connection. Read %v\n", count)
		}
		if nil == err {
			t.Fatalf("Should get error, but we didn't\n")
		}

		// Server shouldn't be able to read
		count, err = server.Read(buffer)
		if count != 0 {
			t.Fatalf("Shouldn't read any bytes from the closed connection. Read %v\n", count)
		}
		if nil == err {
			t.Fatalf("Should get error, but we didn't\n")
		}

		// Server can still write (even though client is closed)
	}
	// Test closing original net.Conn
	{
		// One byte a second
		pool := iothrottler.NewIOThrottlerPool(iothrottler.BytesPerSecond)
		defer pool.ReleasePool()

		client, server := createTcpPipe(t)
		defer client.Close()
		defer server.Close()

		_, err := pool.AddConn(client)
		assertNoError(t, err)
		client.Close()

		buffer := make([]byte, 10)

		// Client should be closed for writing
		count, err := client.Write(buffer)
		if count != 0 {
			t.Fatalf("Shouldn't write any bytes from the closed connection. Wrote %v\n", count)
		}
		if nil == err {
			t.Fatalf("Should get error, but we didn't\n")
		}

		// Client should be closed for reading
		count, err = client.Read(buffer)
		if count != 0 {
			t.Fatalf("Shouldn't read any bytes from the closed connection. Read %v\n", count)
		}
		if nil == err {
			t.Fatalf("Should get error, but we didn't\n")
		}

		// Server shouldn't be able to read
		count, err = server.Read(buffer)
		if count != 0 {
			t.Fatalf("Shouldn't read any bytes from the closed connection. Read %v\n", count)
		}
		if nil == err {
			t.Fatalf("Should get error, but we didn't\n")
		}

		// Server can still write (even though client is closed)
	}
}

/*
 * Make sure that communicating over throttled connections that transmission
 * takes as long as we'd expect
 */
func TestThrottling(t *testing.T) {

	// Test delay for throttled reader
	println("\tThrottled reader")
	{
		// One byte a second
		pool := iothrottler.NewIOThrottlerPool(iothrottler.BytesPerSecond)
		defer pool.ReleasePool()

		readEnd, writeEnd := io.Pipe()
		throttledReadEnd, err := pool.AddReader(readEnd)
		assertNoError(t, err)
		data := []byte("01234")

		// The pool starts with one second of bandwidth. So time is len(data)-1
		assertTransmitTime(data, throttledReadEnd, writeEnd, Seconds(len(data)-1), t)
	}

	// Test delay for throttled writer
	println("\tThrottled writer")
	{
		// One byte a second
		pool := iothrottler.NewIOThrottlerPool(iothrottler.BytesPerSecond)
		defer pool.ReleasePool()

		readEnd, writeEnd := io.Pipe()
		throttleWriteEnd, err := pool.AddWriter(writeEnd)
		assertNoError(t, err)
		data := []byte("01234")

		// The pool starts with one second of bandwidth. So time is len(data)-1
		assertTransmitTime(data, readEnd, throttleWriteEnd, Seconds(len(data)-1), t)
	}

	// Test delay for throttled read/writer
	println("\tThrottled readwriter")
	{
		client, server := createTcpPipe(t)
		defer client.Close()
		defer server.Close()

		// One byte a second
		pool := iothrottler.NewIOThrottlerPool(iothrottler.BytesPerSecond)
		defer pool.ReleasePool()

		throttleClient, err := pool.AddReadWriter(client)
		assertNoError(t, err)

		// The data to write and a buffer to read into
		data := []byte("01234")

		// The pool starts with one second of bandwidth.
		expectedDelay := Seconds(len(data) - 1)
		assertTransmitTime(data, throttleClient, server, expectedDelay, t)
	}

	// Test delay for throttled net.Conn
	println("\tThrottled conn")
	{
		client, server := createTcpPipe(t)
		defer client.Close()
		defer server.Close()

		// One byte a second
		pool := iothrottler.NewIOThrottlerPool(iothrottler.BytesPerSecond)
		defer pool.ReleasePool()

		throttleClient, err := pool.AddConn(client)
		assertNoError(t, err)

		// The data to write and a buffer to read into
		data := []byte("01234")

		// The pool starts with one second of bandwidth.
		expectedDelay := Seconds(len(data) - 1)
		assertTransmitTime(data, throttleClient, server, expectedDelay, t)
	}
	println("\tThrottle done")
}

func TestUnlimitedBandwidthIsFast(t *testing.T) {
	pool := iothrottler.NewIOThrottlerPool(iothrottler.Unlimited)
	defer pool.ReleasePool()
	readEnd, writeEnd := io.Pipe()
	throttledReadEnd, err := pool.AddReader(readEnd)
	throttledWriteEnd, err := pool.AddWriter(writeEnd)
	assertNoError(t, err)
	data := make([]byte, 10000)

	// Unlimited bandwidth should be fast
	assertTransmitTime(data, throttledReadEnd, throttledWriteEnd, 0, t)
}

func TestAggressiveClientsDontMonopolizeBandwidth(t *testing.T) {
	// Test monopolization for throttled reader
	{
		// One byte a second
		pool := iothrottler.NewIOThrottlerPool(iothrottler.BytesPerSecond)
		defer pool.ReleasePool()

		greedyReadEnd, greedyWriteEnd := io.Pipe()
		readEnd, writeEnd := io.Pipe()

		throttledReadEnd, err := pool.AddReader(readEnd)
		assertNoError(t, err)
		throttledGreedyReadEnd, err := pool.AddReader(greedyReadEnd)
		assertNoError(t, err)

		// Hammer the greedy connection
		dosPipe(throttledGreedyReadEnd, greedyWriteEnd)
		_, err = timePipeTransmittion([]byte("01234"), throttledReadEnd, writeEnd)
		if nil != err {
			t.Fatalf("Couldn't transmit data %v", err)
		}
	}
}

/*
 * Make sure that when a bandwidth limited reader and a bandwidth limited writer
 * that live in different pools will not take longer that if only one side was
 * bandwidth limited (they both have the same bandwidth)
 */
func TestLimitedReadAndWrite(t *testing.T) {
	// One byte a second
	readPool := iothrottler.NewIOThrottlerPool(iothrottler.BytesPerSecond)
	defer readPool.ReleasePool()
	writePool := iothrottler.NewIOThrottlerPool(iothrottler.BytesPerSecond)
	defer writePool.ReleasePool()

	readEnd, writeEnd := io.Pipe()
	throttledReadEnd, err := readPool.AddReader(readEnd)
	assertNoError(t, err)
	throttleWriteEnd, err := writePool.AddWriter(writeEnd)
	assertNoError(t, err)
	data := []byte("01234")

	// The pool starts with one second of bandwidth. So time is len(data)-1
	assertTransmitTime(data, throttledReadEnd, throttleWriteEnd, Seconds(len(data)-1), t)
}

/*
 * Make sure reading and writing to the original connections aren't effected by
 * adding them to a pool.
 */
func TestThrottlerPoolDoesntEffectOriginalReadWriters(t *testing.T) {
	// One byte a second
	readPool := iothrottler.NewIOThrottlerPool(iothrottler.BytesPerSecond)
	defer readPool.ReleasePool()
	writePool := iothrottler.NewIOThrottlerPool(iothrottler.BytesPerSecond)
	defer writePool.ReleasePool()

	readEnd, writeEnd := io.Pipe()
	readPool.AddReader(readEnd)
	writePool.AddWriter(writeEnd)
	data := []byte("01234")

	/*
	 * We use the original Reader and Writer and not the ones that have been
	 * added to the pool, this should work just fine and the original ones
	 * should not be subject to the throttling
	 */
	assertTransmitTime(data, readEnd, writeEnd, Seconds(0), t)
}

/*
 * Make sure that when reading from a bandwidth limited reader that's sharing
 * the bandwidth with another reader, that the bandwidth is shared between the
 * readers.
 */
func TestLimitedReaderSharedPool(t *testing.T) {
	// One byte a second
	pool := iothrottler.NewIOThrottlerPool(iothrottler.BytesPerSecond)
	defer pool.ReleasePool()

	// This reader will not have any data written to it so it just consumes
	// space in the pool
	singleReader, singleWriter := io.Pipe()
	readEnd, writeEnd := io.Pipe()
	throttledReadEnd, err := pool.AddReader(readEnd)
	assertNoError(t, err)
	throttledSingleReader, err := pool.AddReader(singleReader)
	assertNoError(t, err)

	// Try and read a byte over the throttled pipe
	buffer := make([]byte, 1)
	_, err = timePipeTransmittion(buffer, throttledSingleReader, singleWriter)
	assertNoError(t, err)

	data := []byte("01234")

	// The null reader will try to read a single byte but no more so it will
	// consume a single second of bandwidth. This will cause the other read to
	// have to wait one more second for it to get it's allotment. So read will
	// take one second longer
	// Also the pool starts with one second of bandwidth.
	assertTransmitTime(data, throttledReadEnd, writeEnd, Seconds(len(data)-1+1), t)
}

/*
 * Make sure that clients of the pool that don't do any IO don't consume any
 * bandwidth
 */
func TestIdleClientsDontConsumeBandwidthAllocations(t *testing.T) {
	// One byte a second
	pool := iothrottler.NewIOThrottlerPool(iothrottler.BytesPerSecond)
	defer pool.ReleasePool()

	// This reader will not have any data written to it so it just consumes
	// space in the pool
	singleReader, _ := io.Pipe()
	readEnd, writeEnd := io.Pipe()
	throttledReadEnd, err := pool.AddReader(readEnd)
	assertNoError(t, err)

	// Add the client to the pool but don't do any IO with it
	pool.AddReader(singleReader)

	data := []byte("01234")

	// The pool starts with one second of bandwidth. So time is len(data)-1
	assertTransmitTime(data, throttledReadEnd, writeEnd, Seconds(len(data)-1), t)
}

/*
 * Make sure a pool doesn't accumulate bandwidth when there are no items in the
 * pool
 */
func TestEmptyPoolDoesntAccumulateBandwidth(t *testing.T) {
	// One byte a second
	pool := iothrottler.NewIOThrottlerPool(iothrottler.BytesPerSecond)
	defer pool.ReleasePool()

	// Wait a bit to make sure bandwidth isn't accumulated
	time.Sleep(2 * time.Second)

	readEnd, writeEnd := io.Pipe()
	throttleWriteEnd, err := pool.AddWriter(writeEnd)
	assertNoError(t, err)
	data := []byte("01234")

	// The pool starts with one second of bandwidth. So time is len(data)-1
	assertTransmitTime(data, readEnd, throttleWriteEnd, Seconds(len(data)-1), t)
}

/*
 * Make sure we can set a new bandwidth for a pool and the new settings will
 * take effect
 */
func TestSettingBandwidthOnExistingPoolWorks(t *testing.T) {
	// One byte a second
	pool := iothrottler.NewIOThrottlerPool(iothrottler.BytesPerSecond)
	defer pool.ReleasePool()

	readEnd, writeEnd := io.Pipe()
	throttleWriteEnd, err := pool.AddWriter(writeEnd)
	assertNoError(t, err)
	data := []byte("01234")

	// The pool starts with one second of bandwidth. So time is len(data)-1
	assertTransmitTime(data, readEnd, throttleWriteEnd, Seconds(len(data)-1), t)

	// Now switch to unlimited bandwidth
	pool.SetBandwidth(iothrottler.Unlimited)

	// The pool starts with unlimited bandwidth so this should take 0 seconds
	assertTransmitTime(data, readEnd, throttleWriteEnd, Seconds(0), t)

	// Back to a slow connection
	pool.SetBandwidth(iothrottler.BytesPerSecond)

	// The pool starts with one second of bandwidth. So time is len(data)-1
	assertTransmitTime(data, readEnd, throttleWriteEnd, Seconds(len(data)-1), t)
}

/*
 * Make sure that early members of a pool don't get all of the initial bandwidth
 * This can be tricky we want to leave a little bandwidth around in case new
 * members get added but we don't want to unnecessarily limit existing pool
 * members.
 */
func TestFairBandwidthAllocationPoolMembers(t *testing.T) {
	const fileName = "/dev/zero"

	// Test with both a lot of readers and a little data to send and a lot of
	// data to send and a few readers
	for j := 0; j != 2; j++ {
		toCopy := int64(1024 * 10)
		readers := 10
		if j == 0 {
			// Lots of readers only a few bytes to write
			readers *= 10
		} else {
			// Few readers lots of bytes to write
			toCopy *= 10
		}

		// We calculate the bandwidth so that the initial pool should have exactly
		// enough bandwidth for all IO without having to wait any time. If our
		// allocation is perfect this should finish in well under 1/10th of a second
		bandwidth := iothrottler.Bandwidth(iothrottler.BytesPerSecond * iothrottler.Bandwidth(toCopy*int64(readers)))

		pool := iothrottler.NewIOThrottlerPool(bandwidth)
		defer pool.ReleasePool()

		timer := startTimer()
		for i := 0; i != readers; i++ {
			file, err := os.Open(fileName)
			assertNoError(t, err)
			defer file.Close()

			tFile, err := pool.AddReader(file)
			assertNoError(t, err)

			var dst bytes.Buffer
			written, err := io.CopyN(&dst, tFile, toCopy)
			assertNoError(t, err)

			if written != toCopy {
				t.Fatalf("Should have copied %v but only copied %v", toCopy, written)
			}
		}

		// Because we've been given exactly enough bandwidth to copy the data
		// and the pool starts with 1 second of bandwidth we should be able to
		// finish this in under a second
		const expected = 0
		if timer.elapsedSeconds() != expected {
			t.Fatalf("Should have taken %v seconds but it took %v instead", expected, timer.elapsedSeconds())
		}
	}
}

func BenchmarkNewReleasePool(b *testing.B) {
	for _b := 0; _b != b.N; _b++ {
		pool := iothrottler.NewIOThrottlerPool(iothrottler.Unlimited)
		defer pool.ReleasePool()
	}
}

func BenchmarkAdd(b *testing.B) {
	b.StopTimer()

	pool := iothrottler.NewIOThrottlerPool(iothrottler.Unlimited)
	defer pool.ReleasePool()

	const fileName = "/dev/zero"
	file, err := os.Open(fileName)
	assertNoError(b, err)
	defer file.Close()
	b.StartTimer()

	// Keep going until we have enough information
	for _b := 0; _b != b.N; _b++ {
		_, err := pool.AddReader(file)
		assertNoError(b, err)
	}
}

func BenchmarkFull(b *testing.B) {

	copyToReaders := func(bytesToCopy int64, readerCount int) {

		// Don't count creating all of our files and such
		b.StopTimer()

		// We calculate the bandwidth so that the initial pool should have exactly
		// enough bandwidth for all IO without having to wait any time. If our
		// allocation is perfect this should finish in well under 1/10th of a second
		bandwidth := iothrottler.Bandwidth(iothrottler.BytesPerSecond * iothrottler.Bandwidth(bytesToCopy*int64(readerCount)))

		files := make([]*os.File, readerCount)
		for i := 0; i != readerCount; i++ {
			const fileName = "/dev/zero"
			file, err := os.Open(fileName)
			assertNoError(b, err)
			defer file.Close()
			files[i] = file
		}

		var dst bytes.Buffer
		b.StartTimer()

		pool := iothrottler.NewIOThrottlerPool(bandwidth)
		defer pool.ReleasePool()

		timer := startTimer()
		for _, file := range files {
			dst.Reset()

			tFile, err := pool.AddReader(file)
			assertNoError(b, err)

			written, err := io.CopyN(&dst, tFile, bytesToCopy)
			assertNoError(b, err)

			if written != bytesToCopy {
				b.Fatalf("Should have copied %v but only copied %v", bytesToCopy, written)
			}
		}

		// Because we've been given exactly enough bandwidth to copy the data
		// and the pool starts with 1 second of bandwidth we should be able to
		// finish this in under a second
		const expected = 0
		if timer.elapsedSeconds() != expected {
			b.Fatalf("Should have taken %v seconds but it took %v instead", expected, timer.elapsedSeconds())
		}
	}

	// Keep going until we have enough information
	for _b := 0; _b != b.N; _b++ {

		// Test with both a lot of readers and a little data to send and a lot of
		// data to send and a few readers
		for j := 0; j != 2; j++ {

			toCopy := int64(1024 * 10)
			readers := 10
			if j == 0 {
				// Lots of readers only a few bytes to write
				readers *= 10
			} else {
				// Few readers lots of bytes to write
				toCopy *= 10
			}

			copyToReaders(toCopy, readers)
		}
	}
}
