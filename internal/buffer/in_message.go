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
	"golang.org/x/sys/unix"
)

// All requests read from the kernel, without data, are shorter than
// this.
var pageSize int

func init() {
	pageSize = unix.Getpagesize()
}

// Return the hardware page size. Note that this is not always 4KiB! Notably
// it's larger (e.g. 64KiB) on some ARM64 architectures.
func GetPageSize() int {
	return pageSize
}

type blockPool struct {
	mu       sync.Mutex
	list     [][]byte
	limit    int
	alloc    func() []byte
	overflow sync.Pool
}

func newBlockPool(limit int, alloc func() []byte) *blockPool {
	p := &blockPool{
		limit: limit,
		alloc: alloc,
	}
	p.overflow.New = func() interface{} {
		return p.alloc()
	}
	return p
}

func (p *blockPool) Get() []byte {
	p.mu.Lock()
	l := len(p.list)
	if l > 0 {
		buf := p.list[l-1]
		p.list = p.list[:l-1]
		p.mu.Unlock()
		return buf
	}
	p.mu.Unlock()
	return p.overflow.Get().([]byte)
}

func (p *blockPool) Put(buf []byte) {
	buf = buf[:cap(buf)]
	p.mu.Lock()
	if len(p.list) < p.limit {
		p.list = append(p.list, buf)
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()
	p.overflow.Put(buf)
}

var BlockPool1M = newBlockPool(48, func() []byte {
	return make([]byte, 1024*1024) // 1 MiB
})

var BlockPool1MPlus = newBlockPool(8, func() []byte {
	return make([]byte, 1024*1024+pageSize)
})

// An incoming message from the kernel, including leading fusekernel.InHeader
// struct. Provides storage for messages and convenient access to their
// contents.
type InMessage struct {
	blocks         [][]byte
	size           int
	consumed       int
	iovecs         []unix.Iovec
	borrowedBlocks [][]byte
}

// NewInMessage creates a new InMessage.
func NewInMessage(size int) *InMessage {
	return &InMessage{}
}

func (m *InMessage) AllocBlocks(totalSize int) {
	m.FreeBlocks()

	// Always allocate a 1MB+pageSize block first for header & metadata & payload
	m.blocks = append(m.blocks, BlockPool1MPlus.Get())

	if totalSize > len(m.blocks[0]) {
		remaining := totalSize - len(m.blocks[0])
		num1MBlocks := (remaining + 1024*1024 - 1) / (1024 * 1024)
		for i := 0; i < num1MBlocks; i++ {
			m.blocks = append(m.blocks, BlockPool1M.Get())
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
	for _, b := range m.borrowedBlocks {
		BlockPool1M.Put(b)
	}
	m.borrowedBlocks = nil
	m.size = 0
	m.consumed = 0
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
var fuseTContiguousPool sync.Pool


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

type syscallConner interface {
	SyscallConn() (syscall.RawConn, error)
}

// Initialize with the data read by a single call to r.Read or readv. The first call to
// Consume will consume the bytes directly after the fusekernel.InHeader
// struct.
func (m *InMessage) Init(r io.Reader) error {
	var n int
	var err error
	if fusekernel.IsPlatformFuseT {
		if len(m.blocks) == 1 {
			n, err = m.ReadSingleContiguous(r, m.blocks[0])
		} else {
			var cap int
			for _, b := range m.blocks {
				cap += len(b)
			}
			var storage []byte
			if v := fuseTContiguousPool.Get(); v != nil {
				buf := v.([]byte)
				if len(buf) >= cap {
					storage = buf[:cap]
				}
			}
			if storage == nil {
				storage = make([]byte, cap)
			}
			defer func() {
				fuseTContiguousPool.Put(storage)
			}()

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
		}
	} else {
		if sc, ok := r.(syscallConner); ok {
			var rawConn syscall.RawConn
			rawConn, err = sc.SyscallConn()
			if err == nil {
				var readvErr error
				err = rawConn.Control(func(fd uintptr) {
					n, m.iovecs, readvErr = readv(int(fd), m.blocks, m.iovecs)
				})
				if err == nil {
					err = readvErr
				}
			}
		} else if f, ok := r.(fder); ok {
			n, m.iovecs, err = readv(int(f.Fd()), m.blocks, m.iovecs)
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

// getBlockAndOffset returns the block index and the local offset within that block
// corresponding to the currently consumed logical bytes.
func (m *InMessage) getBlockAndOffset() (blockIdx int, localOffset int) {
	localOffset = m.consumed
	for blockIdx < len(m.blocks) {
		bLen := len(m.blocks[blockIdx])
		if localOffset < bLen {
			break
		}
		localOffset -= bLen
		blockIdx++
	}
	return blockIdx, localOffset
}

// Consume the next n bytes from the message, returning a nil pointer if there
// are fewer than n bytes available.
func (m *InMessage) Consume(n uintptr) unsafe.Pointer {
	if m.Len() == 0 || n > m.Len() {
		return nil
	}

	blockIdx, offset := m.getBlockAndOffset()

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

	blockIdx, offset := m.getBlockAndOffset()

	if offset+int(n) <= len(m.blocks[blockIdx]) {
		b := m.blocks[blockIdx][offset : offset+int(n)]
		m.consumed += int(n)
		return b
	}

	// In production, any spanning allocation is larger than 1MB (since block 0
	// is 1MB + pageSize and fits all normal headers/payloads). Thus we always
	// allocate directly from the heap.
	res := make([]byte, n)
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

	blockIdx, offset := m.getBlockAndOffset()

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
// directly. If it does not fit, we allocate a separate buffer.
func (m *InMessage) GetFree(n int) []byte {
	if n <= 0 {
		return nil
	}
	if len(m.blocks) > 0 && m.size+n <= len(m.blocks[0]) {
		return m.blocks[0][m.size : m.size+n]
	}
	// Since n doesn't fit in block 0, and block 0 has size 1MB + pageSize,
	// n is necessarily larger than 1MB (assuming typical small offset like
	// sizeof(ReadIn)). Thus we always allocate directly on the heap.
	return make([]byte, n)
}

// GetFreeVector returns a temporary set of buffers summing to n bytes. If it fits
// in the first block, we return a slice of the first block in a single-element slice.
// If it does not fit, we allocate 1MB buffers from BlockPool1M.
func (m *InMessage) GetFreeVector(n int) [][]byte {
	if n <= 0 {
		return nil
	}
	if len(m.blocks) > 0 && m.size+n <= len(m.blocks[0]) {
		return [][]byte{m.blocks[0][m.size : m.size+n]}
	}

	var res [][]byte
	remaining := n
	for remaining > 0 {
		block := BlockPool1M.Get()
		m.borrowedBlocks = append(m.borrowedBlocks, block)
		allocSize := 1024 * 1024
		if remaining < allocSize {
			allocSize = remaining
		}
		res = append(res, block[:allocSize])
		remaining -= allocSize
	}
	return res
}
