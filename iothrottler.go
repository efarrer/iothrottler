// Copyright 2012 Evan Farrer. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
	Package iothrottler implements application IO throttling.
*/

package iothrottler

import (
	"io"
	"log"
	"math"
	"net"
	"sync"
	"time"
)

// The Bandwidth type represents a bandwidth quantity in bytes per second.
// Sub-byte per seconds values are not supported
type Bandwidth int64

const (
	// Bytes per second
	BytesPerSecond Bandwidth = 1
	// Kilobits per second
	Kbps = BytesPerSecond * (1024 / 8)
	// Megabits per second
	Mbps = Kbps * 1024
	// Gigabits per second
	Gbps = Mbps * 1024
	// Unlimited bandwidth
	Unlimited = math.MaxInt64
)

// A pool for throttling IO
type IOThrottlerPool struct {
	// Locks the structure variables
	bandwidthLock *sync.Mutex

	// The simulated bandwidth
	bandwidth Bandwidth
	// Tracks the number of bytes that were consumed
	bandwidthUsed int64
	// Tracks the time that bandwidth started accumulating
	bandwidthStartTime time.Time
	// Tracks the number of clients
	clientCount int64
}

// Construct a new IO throttling pool
// The bandwidth for this pool will be limited to 'bandwidth'
func NewIOThrottlerPool(bandwidth Bandwidth) *IOThrottlerPool {

	pool := &IOThrottlerPool{
		bandwidthLock: &sync.Mutex{},
		bandwidth:     bandwidth,
	}

	return pool
}

// Release the IOThrottlerPool all bandwidth
// Deprecated: Pools no longer need to be released
func (pool *IOThrottlerPool) ReleasePool() {
}

// Sets the IOThrottlerPool's bandwidth rate
func (pool *IOThrottlerPool) SetBandwidth(bandwidth Bandwidth) {
	// When changing the bandwidth we reset the start time and the bytes sent
	pool.bandwidthLock.Lock()
	pool.bandwidth = bandwidth
	pool.bandwidthUsed = 0
	pool.bandwidthStartTime = time.Time{}
	pool.bandwidthLock.Unlock()

	// Reset the start time (if we have any clients)
	pool.recordStartTime()
}

// useBandwidth consumes up to bytes bandwidth without blocking
// Returns the number of bytes that can be transmitted
func (pool *IOThrottlerPool) useBandwidth(bytes int) int {
	now := time.Now().UTC()

	pool.bandwidthLock.Lock()
	defer pool.bandwidthLock.Unlock()

	secondsElapsed := int64(now.Sub(pool.bandwidthStartTime).Seconds())
	availableBandwidth := int((secondsElapsed * int64(pool.bandwidth)) - pool.bandwidthUsed)
	if bytes > availableBandwidth {
		bytes = availableBandwidth
	}
	pool.bandwidthUsed += int64(bytes)

	return bytes
}

// blockForBandwidth consumes bytes bandwidth
// If that bandwidth isn't immediately available it blocks until it is
func (pool *IOThrottlerPool) blockForBandwidth(min, max int) int {
	gotten := 0
	for {
		// Try and read the maximum
		gotten += pool.useBandwidth(max - gotten)

		// If got at least the minimum
		if gotten >= min {
			return gotten
		}

		pool.bandwidthLock.Lock()
		bandwidth := pool.bandwidth
		pool.bandwidthLock.Unlock()

		// Compute the minimal time we'll need to wait for the bandwidth to accumulate
		waitDuration := time.Duration(bandwidth) * time.Second

		// We want to be fair to other users of the bandwidth. If we block for too long then we could be starved of all
		// bandwidth or starve others of all bandwidth. So limit the time we'll wait.
		// This could probably be tuned a bit to divide it based on the number of clients.
		maxWait := time.Millisecond * 100
		if waitDuration > maxWait {
			waitDuration = maxWait
		}
		time.Sleep(waitDuration)
	}
}

func (pool *IOThrottlerPool) getClientCount() int64 {
	pool.bandwidthLock.Lock()
	defer pool.bandwidthLock.Unlock()
	return pool.clientCount
}

func (pool *IOThrottlerPool) recordStartTime() {
	now := time.Now().UTC()
	pool.bandwidthLock.Lock()
	defer pool.bandwidthLock.Unlock()

	if pool.bandwidthStartTime.IsZero() {
		pool.bandwidthStartTime = now
		// We start with the full allocation of bandwidth. So we simulate this by assuming that negative bandwidth was used.
		pool.bandwidthUsed -= int64(pool.bandwidth)
	}
}

// Returns the first error or nil if neither are errors
func orErrors(er0, er1 error) error {
	if er0 != nil {
		return er0
	}
	return er1
}

/*
 * Updates the client count for a pool return error on failure
 */
func twiddleClientCount(p *IOThrottlerPool, change int64) (err error) {
	p.bandwidthLock.Lock()
	p.clientCount += change
	p.bandwidthLock.Unlock()

	return nil
}

// A ReadCloser that will respect the bandwidth limitations of the IOThrottlerPool
type throttledReadCloser struct {
	origReadCloser io.ReadCloser
	pool           *IOThrottlerPool
}

// A WriteCloser that will respect the bandwidth limitations of the IOThrottlerPool
type throttledWriteCloser struct {
	origWriteCloser io.WriteCloser
	pool            *IOThrottlerPool
}

// A ReadWriteCloser that will respect the bandwidth limitations of the IOThrottlerPool
type throttledReadWriteCloser struct {
	throttledReadCloser
	throttledWriteCloser
}

// Read method for the throttledReadCloser
func (t *throttledReadCloser) Read(b []byte) (int, error) {
	toRead := t.pool.blockForBandwidth(1, len(b))
	// Do the limited read
	return t.origReadCloser.Read(b[:toRead])
}

// Write method for the throttledWriteCloser
func (t *throttledWriteCloser) Write(data []byte) (int, error) {
	bandwidthNeeded := len(data)
	t.pool.blockForBandwidth(bandwidthNeeded, bandwidthNeeded)

	// Do the write
	return t.origWriteCloser.Write(data)
}

// Close method for the throttledReadCloser
func (t *throttledReadCloser) Close() error {
	// Unregister with the pool
	err := twiddleClientCount(t.pool, -1)

	return orErrors(err, t.origReadCloser.Close())
}

// Close method for the throttledWriteCloser
func (t *throttledWriteCloser) Close() error {
	// Unregister with the pool
	err := twiddleClientCount(t.pool, -1)

	return orErrors(err, t.origWriteCloser.Close())
}

// Close method for the throttledReadWriteCloser
func (t *throttledReadWriteCloser) Close() error {
	// In this case we really have two copies of all the data
	// It really doesn't matter which we use as the reader and writer hold the
	// same data

	// Unregister with the pool
	err := twiddleClientCount(t.throttledReadCloser.pool, -1)

	return orErrors(err, t.throttledReadCloser.origReadCloser.Close())
}

// Add a io.ReadCloser to the pool. The returned io.ReadCloser shares the
// IOThrottlerPool's bandwidth with other items in the pool.
func (p *IOThrottlerPool) AddReader(reader io.ReadCloser) (io.ReadCloser, error) {
	// Record the start time once we get our first client
	if p.getClientCount() == 0 {
		p.recordStartTime()
	}
	// Register with the pool
	err := twiddleClientCount(p, 1)
	if err != nil {
		return nil, err
	}

	return &throttledReadCloser{reader, p}, nil
}

// Add a io.WriteCloser to the pool. The returned io.WriteCloser shares the
// IOThrottlerPool's bandwidth with other items in the pool.
func (p *IOThrottlerPool) AddWriter(writer io.WriteCloser) (io.WriteCloser, error) {
	// Record the start time once we get our first client
	if p.getClientCount() == 0 {
		p.recordStartTime()
	}
	// Register with the pool
	err := twiddleClientCount(p, 1)
	if err != nil {
		return nil, err
	}

	return &throttledWriteCloser{writer, p}, nil
}

// Add a io.ReadWriteCloser to the pool. The returned io.ReadWriteCloser shares the
// IOThrottlerPool's bandwidth with other items in the pool.
func (p *IOThrottlerPool) AddReadWriter(readWriter io.ReadWriteCloser) (io.ReadWriteCloser, error) {
	// Record the start time once we get our first client
	if p.getClientCount() == 0 {
		p.recordStartTime()
	}
	// Register with the pool
	err := twiddleClientCount(p, 1)
	if err != nil {
		return nil, err
	}

	return &throttledReadWriteCloser{throttledReadCloser{readWriter, p},
		throttledWriteCloser{readWriter, p}}, err
}

// Add a net.Conn to the pool. The returned net.Conn shares the
// IOThrottlerPool's bandwidth with other items in the pool.
type throttledConn struct {
	throttledReadWriteCloser
	originalConn net.Conn
}

// Implements the net.Conn LocalAddr method
func (c *throttledConn) LocalAddr() net.Addr {
	return c.originalConn.LocalAddr()
}

// Implements the net.Conn RemoteAddr method
func (c *throttledConn) RemoteAddr() net.Addr {
	return c.originalConn.RemoteAddr()
}

// Implements the net.Conn SetDeadline method
func (c *throttledConn) SetDeadline(t time.Time) error {
	return c.originalConn.SetDeadline(t)
}

// Implements the net.Conn SetReadDeadline method
func (c *throttledConn) SetReadDeadline(t time.Time) error {
	return c.originalConn.SetReadDeadline(t)
}

// Implements the net.Conn SetWriteDeadline method
func (c *throttledConn) SetWriteDeadline(t time.Time) error {
	return c.originalConn.SetWriteDeadline(t)
}

// Restrict the network connection to the bandwidth limitations of the IOThrottlerPool
func (p *IOThrottlerPool) AddConn(conn net.Conn) (net.Conn, error) {
	// Record the start time once we get our first client
	if p.getClientCount() == 0 {
		p.recordStartTime()
	}

	rwCloser, err := p.AddReadWriter(conn)
	if err != nil {
		return nil, err
	}
	throttledRWC, ok := rwCloser.(*throttledReadWriteCloser)
	if !ok {
		log.Fatalf("Programming error, expecting *throttledReadWriteCloser but got %v", rwCloser)
	}

	return &throttledConn{*throttledRWC, conn}, nil
}
