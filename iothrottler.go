package iothrottler

import (
	"errors"
	"io"
	"log"
	"math"
	"net"
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
	bandwidth Bandwidth
	// A channel for allocating bandwidth
	bandwidthAllocatorChan chan Bandwidth
	// A channel for returning unused bandwidth to server
	bandwidthFreeChan chan Bandwidth
	// A channel for getting a count of the clients
	// A pool only accumulates bandwidth if the pool is non-empty
	clientCountChan chan int64
	// A channel for getting pool release messages
	releasePoolChan chan bool
}

// Construct a new IO throttling pool
// The bandwidth for this pool will be limited to 'bandwidth'
func NewIOThrottlerPool(bandwidth Bandwidth) *IOThrottlerPool {
	bandwidthAllocatorChan := make(chan Bandwidth)
	bandwidthFreeChan := make(chan Bandwidth)
	clientCountChan := make(chan int64)
	releasePoolChan := make(chan bool)

	// Handle the read
	go func() {

		clientCount := int64(0)
		totalbandwidth := Bandwidth(bandwidth)
		allocationSize := Bandwidth(0)
		var timeout <-chan time.Time = nil
		var thisBandwidthAllocatorChan chan Bandwidth = nil

		recalculateAllocationSize := func() {
			if totalbandwidth == Unlimited {
				allocationSize = Unlimited
			} else {

				// Calculate how much bandwidth each consumer will get
				// We divvy the available bandwidth among the existing
				// clients but leave a bit of room in case more clients
				// connect in the mean time. This greatly improves
				// performance
				allocationSize = totalbandwidth / Bandwidth(clientCount+1)

				// Even if we have a negative totalbandwidth we never want to
				// allocate negative bandwidth to members of our pool
				if allocationSize < 0 {
					allocationSize = 0
				}

				// If we do have some bandwidth make sure we at least allocate 1 byte
				if allocationSize == 0 && totalbandwidth > 0 {
					allocationSize = 1
				}
			}

			if allocationSize > 0 {
				// Since we have bandwidth to allocate we can select on
				// the bandwidth allocator chan
				thisBandwidthAllocatorChan = bandwidthAllocatorChan
			}
		}

		// Start with our initial calculated allocation size
		recalculateAllocationSize()

		for {
			select {
			// Release the pool
			case release := <-releasePoolChan:
				if release {
					close(bandwidthAllocatorChan)
					close(bandwidthFreeChan)
					// Don't close the clientCountChan it's not needed and it
					// complicates the code (two different functions need to recover
					// the panic if it's closed
					releasePoolChan <- true
					close(releasePoolChan)
					return
				}

				// Register a new client
			case increment := <-clientCountChan:
				// We got our first client
				// We start the timer as soon as we get our first client
				if clientCount == 0 {
					timeout = time.Tick(time.Second * 1)
				}
				clientCount += increment
				// Our last client left so stop the timer
				if clientCount == 0 {
					timeout = nil
				}
				recalculateAllocationSize()

			// Allocate some bandwidth
			case thisBandwidthAllocatorChan <- allocationSize:
				if Unlimited != totalbandwidth {
					totalbandwidth -= allocationSize

					if totalbandwidth <= 0 {
						// We've allocate all out bandwidth so we need to wait for
						// more
						thisBandwidthAllocatorChan = nil
					}
				}

			// Get unused bandwidth back from client
			case returnSize := <-bandwidthFreeChan:
				if Unlimited != totalbandwidth {
					totalbandwidth += returnSize
				}
				// We could re-calculate the allocationSize but it may not
				// really matter as we'll do it as soon as we get more
				// bandwidth

				// Get more bandwidth to allocate
			case <-timeout:
				if clientCount > 0 {
					if Unlimited != totalbandwidth {
						// Get a new allotment of bandwidth
						totalbandwidth += bandwidth

						recalculateAllocationSize()
					}
				}
			}
		}
	}()

	return &IOThrottlerPool{bandwidth, bandwidthAllocatorChan, bandwidthFreeChan, clientCountChan, releasePoolChan}
}

// Release the IOThrottlerPool all bandwidth
func (pool *IOThrottlerPool) ReleasePool() {
	// If pool.releasePoolChan is already closed (called ReleasePool more than
	// once) then sending to it will panic so just swallow the panic
	defer func() {
		recover()
	}()
	pool.releasePoolChan <- true
	<-pool.releasePoolChan
}

// Copies bytes from src to dst until an error occurs
// Copying is limited by the bandwidth allocated from the pool over a channel
// passed to the bandwidth channel
func bandwidthLimitedCopy(pool *IOThrottlerPool, src io.ReadCloser, dst io.WriteCloser) (err error) {
	totalBandwidth := Bandwidth(0)

	// When the pool has been released the server closes clientCountChan
	// so our channel send will panic. We want to set the return error
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("Pool has been released")
		}
	}()
	// If the pool has been released this will panic
	pool.releasePoolChan <- false

	go func() {
		defer src.Close()
		defer dst.Close()

		// Register us with the pool so it will start allocating bandwidth
		pool.clientCountChan <- 1
		defer func() { pool.clientCountChan <- -1 }()
		defer func() { pool.bandwidthFreeChan <- totalBandwidth }()

		// Close a closer using CloseWithError if possible
		closeWithError := func(item io.Closer, err error) {
			p, ok := dst.(interface {
				CloseWithError(error) error
			})
			if ok {
				p.CloseWithError(err)
			} else {
				item.Close()
			}
		}

		for {
			// Get our allocation of bandwidth
			allocation, ok := <-pool.bandwidthAllocatorChan
			if !ok {
				return
			}
			totalBandwidth += allocation

			// Use our allocated bandwidth
			for totalBandwidth > 0 {

				// Copy some data
				written, err := io.CopyN(dst, src, int64(totalBandwidth))
				// It's possible for CopyN to copy some bytes and return an
				// error but in this case we don't care
				if nil != err {
					closeWithError(dst, err)
					closeWithError(src, err)
					return
				}
				// Recalculate the bandwidth remaining
				totalBandwidth -= Bandwidth(written)
			}
		}
	}()

	return nil
}

// A ReadWriteCloser that will respect the bandwidth limitations of the IOThrottlerPool
type throttledReadWriteCloser struct {
	readPipe   *io.PipeReader
	writePipe  *io.PipeWriter
	origCloser io.Closer
}

func (q *throttledReadWriteCloser) Read(p []byte) (n int, err error) {
	return q.readPipe.Read(p)
}

func (q *throttledReadWriteCloser) Write(data []byte) (n int, err error) {
	return q.writePipe.Write(data)
}

func (q *throttledReadWriteCloser) Close() error {
	rerr := q.readPipe.Close()
	werr := q.writePipe.Close()
	cerr := q.origCloser.Close()

	orErrors := func(er0, er1 error) error {
		if er0 != nil {
			return er0
		}
		return er1
	}

	return orErrors(rerr, orErrors(werr, cerr))
}

// Add a io.ReadCloser to the pool. The returned io.ReadCloser shares the
// IOThrottlerPool's bandwidth with other items in the pool.
func (p *IOThrottlerPool) AddReader(reader io.ReadCloser) (io.ReadCloser, error) {
	readPipeReadEnd, readPipeWriteEnd := io.Pipe()

	// Do the reading
	err := bandwidthLimitedCopy(p, reader, readPipeWriteEnd)
	if err != nil {
		return nil, err
	}

	return &throttledReadWriteCloser{readPipeReadEnd, readPipeWriteEnd, reader}, nil
}

// Add a io.WriteCloser to the pool. The returned io.WriteCloser shares the
// IOThrottlerPool's bandwidth with other items in the pool.
func (p *IOThrottlerPool) AddWriter(writer io.WriteCloser) (io.WriteCloser, error) {
	writePipeReadEnd, writePipeWriteEnd := io.Pipe()

	// Do the writing
	err := bandwidthLimitedCopy(p, writePipeReadEnd, writer)
	if err != nil {
		return nil, err
	}

	return &throttledReadWriteCloser{writePipeReadEnd, writePipeWriteEnd,
		writer}, nil
}

// Add a io.ReadWriteCloser to the pool. The returned io.ReadWriteCloser shares the
// IOThrottlerPool's bandwidth with other items in the pool.
func (p *IOThrottlerPool) AddReadWriter(readWriter io.ReadWriteCloser) (io.ReadWriteCloser, error) {
	readPipeReadEnd, readPipeWriteEnd := io.Pipe()
	writePipeReadEnd, writePipeWriteEnd := io.Pipe()

	// Do the reading
	err := bandwidthLimitedCopy(p, readWriter, readPipeWriteEnd)
	if err != nil {
		return nil, err
	}

	// Do the writing
	err = bandwidthLimitedCopy(p, writePipeReadEnd, readWriter)
	if err != nil {
		return nil, err
	}

	return &throttledReadWriteCloser{readPipeReadEnd, writePipeWriteEnd,
		readWriter}, nil
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
