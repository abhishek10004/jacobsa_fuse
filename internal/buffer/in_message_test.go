// Copyright 2026 Google Inc. All Rights Reserved.
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
	"os"
	"syscall"
	"testing"
	"unsafe"

	"github.com/jacobsa/fuse/internal/fusekernel"
)


func TestInMessageAllocAndFree(t *testing.T) {
	m := NewInMessage(0)
	m.AllocBlocks(17 * MiB) // 17 MiB total size

	// 1 (1MB + pageSize) block + 16 1 MiB blocks = 17 blocks
	if len(m.blocks) != 17 {
		t.Errorf("expected 17 blocks, got %d", len(m.blocks))
	}

	// Block 0: 1 MiB + pageSize
	expectedBlock0Size := MiBPlusPageSize
	if len(m.blocks[0]) != expectedBlock0Size {
		t.Errorf("expected block 0 to be %d, got %d", expectedBlock0Size, len(m.blocks[0]))
	}

	// Blocks 1-16: 1 MiB
	for i := 1; i < 17; i++ {
		if len(m.blocks[i]) != MiB {
			t.Errorf("expected block %d to be 1 MiB, got %d", i, len(m.blocks[i]))
		}
	}

	// Shrink to fit for small message (fits within the first block)
	m.ShrinkToFit(100)
	if len(m.blocks) != 1 {
		t.Errorf("expected 1 block after shrinking to 100 bytes, got %d", len(m.blocks))
	}

	m.FreeBlocks()
	if len(m.blocks) != 0 {
		t.Errorf("expected 0 blocks after FreeBlocks, got %d", len(m.blocks))
	}
}

func TestInMessageConsumeAndBytes(t *testing.T) {
	m := NewInMessage(0)
	// Manually allocate small blocks to verify spanning without requiring large pipe transfers.
	m.blocks = [][]byte{
		make([]byte, 50),
		make([]byte, 50),
	}

	msgLen := 100

	// Build a dummy input stream
	data := make([]byte, msgLen)
	// Write InHeader
	header := (*fusekernel.InHeader)(unsafe.Pointer(&data[0]))
	header.Len = uint32(msgLen)
	header.Opcode = 123
	header.Unique = 456

	// Write some bytes spanning across the boundary (offset 50)
	data[49] = 'Y'
	data[50] = 'Z'

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("Pipe failed: %v", err)
	}
	defer r.Close()

	_, err = w.Write(data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	w.Close()

	err = m.Init(r)
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	if m.Header().Unique != 456 {
		t.Errorf("expected Unique = 456, got %d", m.Header().Unique)
	}

	// Consume a dummy struct from Block 0 (offset 40 to 48, size 8)
	p := m.Consume(8)
	if p == nil {
		t.Fatalf("Consume returned nil")
	}

	// Consume remaining bytes of Block 0 up to block boundary (so consumed is 49)
	skip := 49 - 40 - 8
	m.Consume(uintptr(skip))

	// Now we are at the end of block 0 (offset 49). The next bytes are 'Y' and 'Z'.
	// This spans across the boundary.
	yz := m.ConsumeBytes(2)
	if string(yz) != "YZ" {
		t.Errorf("expected 'YZ', got %q", string(yz))
	}

	// Clear m.blocks to nil so FreeBlocks doesn't put our small manual slices into BlockPool1MPlusPage/BlockPool1M.
	m.blocks = nil
	m.FreeBlocks()
}

func TestInMessageInitFuseT(t *testing.T) {
	fusekernel.IsPlatformFuseT = true
	defer func() {
		fusekernel.IsPlatformFuseT = false
	}()

	runTest := func(t *testing.T, blockSizes []int) {
		m := NewInMessage(0)
		for _, sz := range blockSizes {
			m.blocks = append(m.blocks, make([]byte, sz))
		}

		var totalBlockCap int
		for _, b := range m.blocks {
			totalBlockCap += len(b)
		}

		// Prepare dummy FUSE header + message
		data := make([]byte, totalBlockCap)
		header := (*fusekernel.InHeader)(unsafe.Pointer(&data[0]))
		header.Len = uint32(totalBlockCap)
		header.Opcode = 999
		header.Unique = 888

		// Write some test markers
		data[40] = 0xAA
		data[totalBlockCap-1] = 0xBB

		r, w, err := os.Pipe()
		if err != nil {
			t.Fatalf("Pipe failed: %v", err)
		}
		defer r.Close()

		go func() {
			_, _ = w.Write(data)
			w.Close()
		}()

		err = m.Init(r)
		if err != nil {
			t.Fatalf("Init failed: %v", err)
		}

		if m.Header().Opcode != 999 {
			t.Errorf("expected Opcode = 999, got %d", m.Header().Opcode)
		}
		if m.Header().Unique != 888 {
			t.Errorf("expected Unique = 888, got %d", m.Header().Unique)
		}

		// Verify first block byte and last block byte
		if m.blocks[0][40] != 0xAA {
			t.Errorf("expected blocks[0][40] = 0xAA, got %x", m.blocks[0][40])
		}
		lastBlock := m.blocks[len(m.blocks)-1]
		if lastBlock[len(lastBlock)-1] != 0xBB {
			t.Errorf("expected last byte of last block = 0xBB, got %x", lastBlock[len(lastBlock)-1])
		}
	}

	t.Run("single_block", func(t *testing.T) {
		runTest(t, []int{110})
	})

	t.Run("multiple_blocks", func(t *testing.T) {
		runTest(t, []int{50, 30, 30})
	})
}

func TestInMessageGetFree(t *testing.T) {
	m := NewInMessage(0)

	// Case 1: len(m.blocks) == 0
	// should return valid buffer via fallback allocation
	if buf := m.GetFree(10); len(buf) != 10 {
		t.Errorf("expected buffer of size 10 when no blocks allocated, got %v", buf)
	}
	m.FreeBlocks()

	firstBlockSize := MiBPlusPageSize
	m.AllocBlocks(firstBlockSize)
	m.size = 100 // Set message size to 100 bytes

	// Case 2: n <= 0 -> should return nil
	if buf := m.GetFree(0); buf != nil {
		t.Errorf("expected nil for n=0, got %v", buf)
	}
	if buf := m.GetFree(-5); buf != nil {
		t.Errorf("expected nil for n=-5, got %v", buf)
	}

	// Case 3: n is larger than remaining space in blocks[0]
	// remaining is: firstBlockSize - 100
	tooLarge := firstBlockSize - 100 + 1

	// should return fallback buffer
	bufTooLarge := m.GetFree(tooLarge)
	if len(bufTooLarge) != tooLarge {
		t.Errorf("expected buffer of size %d for too large request, got %d", tooLarge, len(bufTooLarge))
	}
	if &bufTooLarge[0] == &m.blocks[0][100] {
		t.Errorf("expected fallback buffer to not be part of block 0")
	}
	m.FreeBlocks()

	// Re-allocate blocks
	m.AllocBlocks(firstBlockSize)
	m.size = 100

	// Case 4: normal allocation within remaining space
	buf1 := m.GetFree(500)
	if len(buf1) != 500 {
		t.Errorf("expected buffer of size 500, got %d", len(buf1))
	}
	expectedStart := &m.blocks[0][100]
	if &buf1[0] != expectedStart {
		t.Errorf("expected buffer to start at index 100 of block 0")
	}

	// Slicing again (m.size is still 100, we don't advance m.size on GetFree)
	buf2 := m.GetFree(500)
	if len(buf2) != 500 {
		t.Errorf("expected buffer of size 500, got %d", len(buf2))
	}
	if &buf2[0] != expectedStart {
		t.Errorf("expected buffer to start at index 100 of block 0")
	}

	m.FreeBlocks()
}

func TestInMessageGetFreeVector(t *testing.T) {
	m := NewInMessage(0)

	// Case 1: len(m.blocks) == 0 -> should return allocated blocks from pool
	bufs := m.GetFreeVector(2*MiB + 100) // 2MB + 100 bytes
	if len(bufs) != 3 {
		t.Errorf("expected 3 buffers, got %d", len(bufs))
	} else {
		if len(bufs[0]) != MiB || len(bufs[1]) != MiB || len(bufs[2]) != 100 {
			t.Errorf("unexpected buffer sizes: %d, %d, %d", len(bufs[0]), len(bufs[1]), len(bufs[2]))
		}
	}
	// Verify that freeing returning buffers to the pool works
	m.FreeBlocks()

	firstBlockSize := MiBPlusPageSize
	m.AllocBlocks(firstBlockSize)
	m.size = 100 // Set message size to 100 bytes

	// Case 2: n <= 0 -> should return nil
	if bufs := m.GetFreeVector(0); bufs != nil {
		t.Errorf("expected nil for n=0, got %v", bufs)
	}
	if bufs := m.GetFreeVector(-5); bufs != nil {
		t.Errorf("expected nil for n=-5, got %v", bufs)
	}

	// Case 3: n fits in blocks[0]
	fitSize := 500
	bufsFit := m.GetFreeVector(fitSize)
	if len(bufsFit) != 1 {
		t.Errorf("expected 1 buffer, got %d", len(bufsFit))
	} else if len(bufsFit[0]) != fitSize {
		t.Errorf("expected buffer of size %d, got %d", fitSize, len(bufsFit[0]))
	} else if &bufsFit[0][0] != &m.blocks[0][100] {
		t.Errorf("expected buffer to start at index 100 of block 0")
	}

	// Case 4: n is larger than remaining space in blocks[0]
	// remaining is: firstBlockSize - 100
	tooLarge := firstBlockSize - 100 + 1
	bufsTooLarge := m.GetFreeVector(tooLarge)
	if len(bufsTooLarge) != 2 {
		t.Errorf("expected 2 buffers, got %d", len(bufsTooLarge))
	} else {
		if len(bufsTooLarge[0]) != MiB || len(bufsTooLarge[1]) != tooLarge-MiB {
			t.Errorf("unexpected sizes: %d, %d", len(bufsTooLarge[0]), len(bufsTooLarge[1]))
		}
	}

	m.FreeBlocks()
}

var benchmarkSink []byte

func BenchmarkConsumeBytesSpanning(b *testing.B) {
	m := NewInMessage(0)
	firstBlockSize := MiBPlusPageSize
	totalSize := firstBlockSize + 2000
	m.AllocBlocks(totalSize)

	m.blocks[0][firstBlockSize-1] = 'Y'
	m.blocks[1][0] = 'Z'

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		m.consumed = firstBlockSize - 1
		m.size = totalSize

		res := m.ConsumeBytes(2)
		if len(res) != 2 || res[0] != 'Y' || res[1] != 'Z' {
			b.Fatalf("unexpected result: %v", res)
		}
		benchmarkSink = res
		m.FreeBlocks()
		m.AllocBlocks(totalSize)
		m.blocks[0][firstBlockSize-1] = 'Y'
		m.blocks[1][0] = 'Z'
	}
	m.FreeBlocks()
}

func BenchmarkGetFree(b *testing.B) {
	m := NewInMessage(0)
	m.AllocBlocks(20000)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		res := m.GetFree(10000)
		if len(res) != 10000 {
			b.Fatalf("expected 10000, got %d", len(res))
		}
		benchmarkSink = res
	}
	m.FreeBlocks()
}

func BenchmarkAllocBlocks(b *testing.B) {
	m := NewInMessage(0)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m.AllocBlocks(4096 + 2000)
		m.FreeBlocks()
	}
}

type fakeRawConn struct {
	fd uintptr
}

func (c fakeRawConn) Control(f func(fd uintptr)) error {
	f(c.fd)
	return nil
}

func (c fakeRawConn) Read(f func(fd uintptr) bool) error {
	return syscall.ENOTSUP
}

func (c fakeRawConn) Write(f func(fd uintptr) bool) error {
	return syscall.ENOTSUP
}

type fakeFdReader struct {
	fd uintptr
}

func (r fakeFdReader) SyscallConn() (syscall.RawConn, error) {
	return fakeRawConn{fd: r.fd}, nil
}

func (r fakeFdReader) Read(p []byte) (int, error) {
	return 0, nil
}

func BenchmarkInMessageInitWithReadv(b *testing.B) {
	m := NewInMessage(0)
	m.AllocBlocks(5 * MiB)
	r := fakeFdReader{fd: ^uintptr(0)} // -1

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = m.Init(r)
	}
	m.FreeBlocks()
}

type fakeFuseTReader struct {
	data []byte
}

func (r *fakeFuseTReader) Read(p []byte) (int, error) {
	return copy(p, r.data), nil
}

func BenchmarkInMessageInitFuseT(b *testing.B) {
	fusekernel.IsPlatformFuseT = true
	defer func() {
		fusekernel.IsPlatformFuseT = false
	}()

	m := NewInMessage(0)
	totalSize := MiBPlusPageSize
	m.AllocBlocks(totalSize)
	defer m.FreeBlocks()

	data := make([]byte, totalSize)
	header := (*fusekernel.InHeader)(unsafe.Pointer(&data[0]))
	header.Len = uint32(totalSize)

	r := &fakeFuseTReader{data: data}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err := m.Init(r)
		if err != nil {
			b.Fatalf("Init failed: %v", err)
		}
	}
}

func BenchmarkInMessageInitFuseTMultiBlock(b *testing.B) {
	fusekernel.IsPlatformFuseT = true
	defer func() {
		fusekernel.IsPlatformFuseT = false
	}()

	m := NewInMessage(0)
	firstBlockSize := MiBPlusPageSize
	totalSize := firstBlockSize + 2*MiB
	m.AllocBlocks(totalSize)
	defer m.FreeBlocks()

	data := make([]byte, totalSize)
	header := (*fusekernel.InHeader)(unsafe.Pointer(&data[0]))
	header.Len = uint32(totalSize)

	r := &fakeFuseTReader{data: data}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err := m.Init(r)
		if err != nil {
			b.Fatalf("Init failed: %v", err)
		}
	}
}

var (
	benchmarkConsumeSink       unsafe.Pointer
	benchmarkConsumeVectorSink [][]byte
	benchmarkGetFreeVectorSink [][]byte
)

func BenchmarkInMessageConsume(b *testing.B) {
	m := NewInMessage(0)
	m.AllocBlocks(4096)
	defer m.FreeBlocks()
	m.size = 4096

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m.consumed = 0
		benchmarkConsumeSink = m.Consume(16)
	}
}

func BenchmarkInMessageConsumeVector_SingleBlock(b *testing.B) {
	m := NewInMessage(0)
	m.AllocBlocks(4096)
	defer m.FreeBlocks()
	m.size = 4096

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m.consumed = 0
		benchmarkConsumeVectorSink = m.ConsumeVector(128)
	}
}

func BenchmarkInMessageConsumeVector_Spanning(b *testing.B) {
	m := NewInMessage(0)
	firstBlockSize := MiBPlusPageSize
	totalSize := firstBlockSize + 2*MiB
	m.AllocBlocks(totalSize)
	defer m.FreeBlocks()
	m.size = totalSize

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m.consumed = firstBlockSize - 128
		benchmarkConsumeVectorSink = m.ConsumeVector(256)
	}
}

func BenchmarkInMessageGetFreeVector_SingleBlock(b *testing.B) {
	m := NewInMessage(0)
	m.AllocBlocks(4096)
	defer m.FreeBlocks()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m.size = 0
		benchmarkGetFreeVectorSink = m.GetFreeVector(128)
	}
}

func BenchmarkInMessageGetFreeVector_MultiBlock(b *testing.B) {
	m := NewInMessage(0)
	m.AllocBlocks(4096)
	defer m.FreeBlocks()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m.size = 0
		benchmarkGetFreeVectorSink = m.GetFreeVector(2 * MiB)

		// Return borrowed blocks to the pool and clear m.borrowedBlocks
		for _, block := range m.borrowedBlocks {
			BlockPool1M.Put(block)
		}
		m.borrowedBlocks = m.borrowedBlocks[:0]
	}
}

func TestInMessageConsumeVector(t *testing.T) {
	m := NewInMessage(0)
	// Manually set up blocks for precise spanning test
	m.blocks = [][]byte{
		make([]byte, 50),
		make([]byte, 50),
		make([]byte, 50),
	}
	m.size = 150
	m.consumed = 0

	// Populate data
	for i := 0; i < 150; i++ {
		m.blocks[i/50][i%50] = byte(i)
	}

	// 1. Request more than available -> should return nil
	if res := m.ConsumeVector(151); res != nil {
		t.Errorf("expected nil when consuming more than size, got %v", res)
	}

	// 2. Consume within the first block (offset 0 to 30)
	v1 := m.ConsumeVector(30)
	if len(v1) != 1 || len(v1[0]) != 30 {
		t.Fatalf("expected 1 slice of length 30, got %v", v1)
	}
	if v1[0][0] != 0 || v1[0][29] != 29 {
		t.Errorf("unexpected content in v1: %v", v1[0])
	}
	if m.consumed != 30 {
		t.Errorf("expected consumed to be 30, got %d", m.consumed)
	}

	// 3. Consume spanning block 0 and block 1 (offset 30 to 70, spanning boundary at 50)
	// Remaining in block 0: 20 bytes (30 to 49)
	// Needed from block 1: 20 bytes (50 to 69)
	v2 := m.ConsumeVector(40)
	if len(v2) != 2 {
		t.Fatalf("expected 2 slices, got %d", len(v2))
	}
	if len(v2[0]) != 20 || len(v2[1]) != 20 {
		t.Errorf("expected slices of size 20 and 20, got %d and %d", len(v2[0]), len(v2[1]))
	}
	if v2[0][0] != 30 || v2[0][19] != 49 || v2[1][0] != 50 || v2[1][19] != 69 {
		t.Errorf("unexpected content in v2: %v, %v", v2[0], v2[1])
	}
	if m.consumed != 70 {
		t.Errorf("expected consumed to be 70, got %d", m.consumed)
	}

	// 4. Consume spanning block 1 and block 2 (offset 70 to 120)
	// Remaining in block 1: 30 bytes (70 to 99)
	// Needed from block 2: 20 bytes (100 to 119)
	v3 := m.ConsumeVector(50)
	if len(v3) != 2 {
		t.Fatalf("expected 2 slices, got %d", len(v3))
	}
	if len(v3[0]) != 30 || len(v3[1]) != 20 {
		t.Errorf("expected slices of size 30 and 20, got %d and %d", len(v3[0]), len(v3[1]))
	}
	if v3[0][0] != 70 || v3[0][29] != 99 || v3[1][0] != 100 || v3[1][19] != 119 {
		t.Errorf("unexpected content in v3: %v, %v", v3[0], v3[1])
	}

	// Clear m.blocks to prevent FreeBlocks from putting them in the global pool
	m.blocks = nil
	m.FreeBlocks()
}

type nonSyscallConnReader struct{}

func (r nonSyscallConnReader) Read(p []byte) (int, error) {
	return 0, nil
}

func TestInMessageInitNonSyscallConnReader(t *testing.T) {
	// Temporarily disable IsPlatformFuseT to trigger the non-FuseT branch
	fusekernel.IsPlatformFuseT = false

	m := NewInMessage(0)
	m.AllocBlocks(4096)
	defer m.FreeBlocks()

	err := m.Init(nonSyscallConnReader{})
	if err == nil {
		t.Fatal("expected error when using a reader that does not implement SyscallConn, got nil")
	}
	expectedErr := "Reader does not support SyscallConn"
	if err.Error() != expectedErr {
		t.Errorf("expected error %q, got %q", expectedErr, err.Error())
	}
}

func TestInMessageInitInvalidHeader(t *testing.T) {
	m := NewInMessage(0)
	m.AllocBlocks(4096)
	defer m.FreeBlocks()

	// 1. Short read (fewer than headerSize = 40 bytes)
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("Pipe failed: %v", err)
	}
	_, _ = w.Write([]byte("too short"))
	w.Close()

	err = m.Init(r)
	r.Close()
	if err == nil {
		t.Fatal("expected error for short read, got nil")
	}

	// 2. Header length mismatch
	r, w, err = os.Pipe()
	if err != nil {
		t.Fatalf("Pipe failed: %v", err)
	}
	header := fusekernel.InHeader{
		Len: 100, // header says 100 bytes, but we only write 40 bytes
	}
	_, _ = w.Write((*[unsafe.Sizeof(header)]byte)(unsafe.Pointer(&header))[:])
	w.Close()

	err = m.Init(r)
	r.Close()
	if err == nil {
		t.Fatal("expected error for header length mismatch, got nil")
	}
}

func TestInMessageShrinkToFitMultiBlock(t *testing.T) {
	m := NewInMessage(0)
	
	m.AllocBlocks(3 * MiB) // Allocates 1 block 0 (1MB+pageSize) + 2 blocks of 1MB = 3 blocks
	if len(m.blocks) != 3 {
		t.Fatalf("expected 3 blocks, got %d", len(m.blocks))
	}

	// Shrink to fit 1.5 MB (should keep block 0 and block 1, release block 2)
	m.ShrinkToFit(1500000)
	if len(m.blocks) != 2 {
		t.Errorf("expected 2 blocks after shrinking to 1.5MB, got %d", len(m.blocks))
	}

	m.FreeBlocks()
}

func TestInMessageBlockPoolRecycling(t *testing.T) {
	m := NewInMessage(0)

	// Measure initial pool size
	BlockPool1M.mu.Lock()
	initialPoolSize := len(BlockPool1M.list)
	BlockPool1M.mu.Unlock()

	// Request a large vector that borrows blocks from BlockPool1M
	bufs := m.GetFreeVector(2*MiB + 100)
	if len(bufs) != 3 {
		t.Fatalf("expected 3 buffers, got %d", len(bufs))
	}

	m.FreeBlocks()

	// Measure pool size after freeing
	BlockPool1M.mu.Lock()
	finalPoolSize := len(BlockPool1M.list)
	BlockPool1M.mu.Unlock()

	if finalPoolSize < initialPoolSize {
		t.Errorf("expected pool size to be at least %d, got %d; blocks were not recycled!", initialPoolSize, finalPoolSize)
	}
}

func TestInMessageConsumeEdgeCases(t *testing.T) {
	m := NewInMessage(0)
	m.blocks = [][]byte{
		make([]byte, 10),
	}
	m.size = 10
	m.consumed = 0

	// 1. Consume 0 bytes -> should return non-nil (start of block)
	p := m.Consume(0)
	if p == nil {
		t.Errorf("expected non-nil pointer for 0-byte Consume")
	}

	// 2. ConsumeBytes 0 bytes -> should return empty slice
	b := m.ConsumeBytes(0)
	if len(b) != 0 {
		t.Errorf("expected empty slice for 0-byte ConsumeBytes, got %v", b)
	}

	// 3. ConsumeVector 0 bytes -> should return empty slice
	v := m.ConsumeVector(0)
	if len(v) != 0 {
		t.Errorf("expected empty slice/vector for 0-byte ConsumeVector, got %v", v)
	}

	// 4. Request more than available
	if p2 := m.Consume(11); p2 != nil {
		t.Errorf("expected nil when consuming more than available, got %v", p2)
	}
	if b2 := m.ConsumeBytes(11); b2 != nil {
		t.Errorf("expected nil when consuming more than available, got %v", b2)
	}
	if v2 := m.ConsumeVector(11); v2 != nil {
		t.Errorf("expected nil when consuming more than available, got %v", v2)
	}

	m.blocks = nil
	m.FreeBlocks()
}
