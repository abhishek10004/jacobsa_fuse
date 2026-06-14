// Copyright 2015 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package buffer

import (
	"fmt"
	"io"
	"sync"
	"syscall"
	"unsafe"

	"github.com/jacobsa/fuse/internal/fusekernel"
)

// All requests read from the kernel, without data, are shorter than
// this.
var pageSize int

func init() {
	pageSize = syscall.Getpagesize()
}

// Return the hardware page size. Note that this is not always 4KiB! Notably
// it's larger (e.g. 64KiB) on some ARM64 architectures.
func GetPageSize() int {
	return pageSize
}

var BlockPool1M = sync.Pool{
	New: func() interface{} {
		return make([]byte, 1024*1024) // 1 MiB
	},
}

var BlockPool1MPlus = sync.Pool{
	New: func() interface{} {
		return make([]byte, 1024*1024+pageSize)
	},
}

type pooledBuffer struct {
	buf  []byte
	pool *sync.Pool
}

func getBuffer(n int) ([]byte, *sync.Pool) {
	if n <= 1048576 {
		return BlockPool1M.Get().([]byte), &BlockPool1M
	}
	return make([]byte, n), nil
}

// An incoming message from the kernel, including leading fusekernel.InHeader
// struct. Provides storage for messages and convenient access to their
// contents.
type InMessage struct {
	blocks      [][]byte
	size        int
	consumed    int
	tempBuffers [2]pooledBuffer
	numTemps    int
}

// NewInMessage creates a new InMessage.
func NewInMessage(size int) *InMessage {
	return &InMessage{}
}

func (m *InMessage) AllocBlocks(totalSize int) {
	m.FreeBlocks()

	// Always allocate a 1MB+pageSize block first for header & metadata & payload
	m.blocks = append(m.blocks, BlockPool1MPlus.Get().([]byte))

	if totalSize > len(m.blocks[0]) {
		remaining := totalSize - len(m.blocks[0])
		num1MBlocks := (remaining + 1024*1024 - 1) / (1024 * 1024)
		for i := 0; i < num1MBlocks; i++ {
			m.blocks = append(m.blocks, BlockPool1M.Get().([]byte))
		}
	}
	m.consumed = 0
	m.size = 0
}

func (m *InMessage) FreeBlocks() {
	if len(m.blocks) > 0 {
		BlockPool1MPlus.Put(m.blocks[0])
		for i := 1; i < len(m.blocks); i++ {
			BlockPool1M.Put(m.blocks[i])
		}
	}
	m.blocks = nil
	m.size = 0
	m.consumed = 0

	for i := 0; i < m.numTemps; i++ {
		tb := m.tempBuffers[i]
		if tb.pool != nil {
			tb.pool.Put(tb.buf)
		}
		m.tempBuffers[i] = pooledBuffer{}
	}
	m.numTemps = 0
}

func (m *InMessage) ShrinkToFit(n int) {
	m.size = n

	var bytesNeeded = n
	var usedBlocks = 0
	for _, block := range m.blocks {
		usedBlocks++
		if bytesNeeded <= len(block) {
			break
		}
		bytesNeeded -= len(block)
	}

	for i := usedBlocks; i < len(m.blocks); i++ {
		if i == 0 {
			BlockPool1MPlus.Put(m.blocks[i])
		} else {
			BlockPool1M.Put(m.blocks[i])
		}
	}
	m.blocks = m.blocks[:usedBlocks]
}

var readLock sync.Mutex

func (m *InMessage) ReadSingleContiguous(r io.Reader, storage []byte) (int, error) {
	readLock.Lock()
	defer readLock.Unlock()

	// read request length
	if _, err := io.ReadFull(r, storage[0:4]); err != nil {
		return 0, err
	}

	header := (*fusekernel.InHeader)(unsafe.Pointer(&storage[0]))
	l := header.Len
	// read remaining request
	if n, err := io.ReadFull(r, storage[4:l]); err != nil {
		return n, err
	}
	return int(l), nil
}

type fder interface {
	Fd() uintptr
}

// Initialize with the data read by a single call to r.Read or readv. The first call to
// Consume will consume the bytes directly after the fusekernel.InHeader
// struct.
func (m *InMessage) Init(r io.Reader) error {
	var n int
	var err error
	if fusekernel.IsPlatformFuseT {
		var cap int
		for _, b := range m.blocks {
			cap += len(b)
		}
		storage := make([]byte, cap)
		n, err = m.ReadSingleContiguous(r, storage)
		if err == nil {
			var copied int
			for _, b := range m.blocks {
				if copied >= n {
					break
				}
				toCopy := len(b)
				if copied+toCopy > n {
					toCopy = n - copied
				}
				copy(b, storage[copied:copied+toCopy])
				copied += toCopy
			}
		}
	} else {
		if f, ok := r.(fder); ok {
			n, err = readv(int(f.Fd()), m.blocks)
		} else {
			return fmt.Errorf("Reader does not support Fd")
		}
	}

	if err != nil {
		return err
	}

	// Make sure the message is long enough.
	const headerSize = unsafe.Sizeof(fusekernel.InHeader{})
	if uintptr(n) < headerSize {
		return fmt.Errorf("Unexpectedly read only %d bytes.", n)
	}

	m.ShrinkToFit(n)
	m.consumed = int(headerSize)

	// Check the header's length.
	if int(m.Header().Len) != n {
		return fmt.Errorf(
			"Header says %d bytes, but we read %d",
			m.Header().Len,
			n)
	}

	return nil
}

// Return a reference to the header read in the most recent call to Init.
func (m *InMessage) Header() *fusekernel.InHeader {
	return (*fusekernel.InHeader)(unsafe.Pointer(&m.blocks[0][0]))
}

// Return the number of bytes left to consume.
func (m *InMessage) Len() uintptr {
	return uintptr(m.size - m.consumed)
}

// Consume the next n bytes from the message, returning a nil pointer if there
// are fewer than n bytes available.
func (m *InMessage) Consume(n uintptr) unsafe.Pointer {
	if m.Len() == 0 || n > m.Len() {
		return nil
	}

	var blockIdx = 0
	var offset = m.consumed

	for blockIdx < len(m.blocks) {
		bLen := len(m.blocks[blockIdx])
		if offset < bLen {
			break
		}
		offset -= bLen
		blockIdx++
	}

	if offset+int(n) > len(m.blocks[blockIdx]) {
		m.consumed += int(n)
		return nil
	}

	p := unsafe.Pointer(&m.blocks[blockIdx][offset])
	m.consumed += int(n)

	return p
}

// Equivalent to Consume, except returns a slice of bytes. The result will be
// nil if Consume would fail.
func (m *InMessage) ConsumeBytes(n uintptr) []byte {
	if n > m.Len() {
		return nil
	}

	var blockIdx = 0
	var offset = m.consumed

	for blockIdx < len(m.blocks) {
		bLen := len(m.blocks[blockIdx])
		if offset < bLen {
			break
		}
		offset -= bLen
		blockIdx++
	}

	if offset+int(n) <= len(m.blocks[blockIdx]) {
		b := m.blocks[blockIdx][offset : offset+int(n)]
		m.consumed += int(n)
		return b
	}

	buf, pool := getBuffer(int(n))
	if pool != nil && m.numTemps < len(m.tempBuffers) {
		m.tempBuffers[m.numTemps] = pooledBuffer{buf: buf, pool: pool}
		m.numTemps++
	}
	res := buf[:n]
	var bytesCopied = 0
	var remainingToCopy = int(n)

	for remainingToCopy > 0 && blockIdx < len(m.blocks) {
		bLen := len(m.blocks[blockIdx])
		availableInBlock := bLen - offset
		copyLen := availableInBlock
		if copyLen > remainingToCopy {
			copyLen = remainingToCopy
		}

		copy(res[bytesCopied:bytesCopied+copyLen], m.blocks[blockIdx][offset:offset+copyLen])

		bytesCopied += copyLen
		remainingToCopy -= copyLen

		offset = 0
		blockIdx++
	}

	m.consumed += int(n)
	return res
}

// Equivalent to ConsumeBytes, except returns a slice of slices referencing the
// underlying blocks, without allocations/copies.
func (m *InMessage) ConsumeVector(n uintptr) [][]byte {
	if n > m.Len() {
		return nil
	}

	var blockIdx = 0
	var offset = m.consumed

	for blockIdx < len(m.blocks) {
		bLen := len(m.blocks[blockIdx])
		if offset < bLen {
			break
		}
		offset -= bLen
		blockIdx++
	}

	var res [][]byte
	var remainingToCopy = int(n)

	for remainingToCopy > 0 && blockIdx < len(m.blocks) {
		bLen := len(m.blocks[blockIdx])
		availableInBlock := bLen - offset
		copyLen := availableInBlock
		if copyLen > remainingToCopy {
			copyLen = remainingToCopy
		}

		res = append(res, m.blocks[blockIdx][offset:offset+copyLen])

		remainingToCopy -= copyLen
		offset = 0
		blockIdx++
	}

	m.consumed += int(n)
	return res
}

// Get a temporary buffer of n bytes. If it fits in the first block, we slice it
// directly. If it does not fit, we allocate a separate buffer only if allocateDst is true.
func (m *InMessage) GetFree(n int, allocateDst bool) []byte {
	if n <= 0 {
		return nil
	}
	if len(m.blocks) > 0 && m.size+n <= len(m.blocks[0]) {
		return m.blocks[0][m.size : m.size+n]
	}
	if !allocateDst {
		return nil
	}
	buf, pool := getBuffer(n)
	if pool != nil && m.numTemps < len(m.tempBuffers) {
		m.tempBuffers[m.numTemps] = pooledBuffer{buf: buf, pool: pool}
		m.numTemps++
	}
	return buf[:n]
}
