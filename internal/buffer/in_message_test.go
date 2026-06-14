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
	"testing"
	"unsafe"

	"github.com/jacobsa/fuse/internal/fusekernel"
)

func TestInMessageAllocAndFree(t *testing.T) {
	m := NewInMessage(0)
	m.AllocBlocks(17 * 1024 * 1024) // 17 MiB total size

	// 1 (1MB + pageSize) block + 16 1 MiB blocks = 17 blocks
	if len(m.blocks) != 17 {
		t.Errorf("expected 17 blocks, got %d", len(m.blocks))
	}

	// Block 0: 1 MiB + pageSize
	expectedBlock0Size := 1024*1024 + GetPageSize()
	if len(m.blocks[0]) != expectedBlock0Size {
		t.Errorf("expected block 0 to be %d, got %d", expectedBlock0Size, len(m.blocks[0]))
	}

	// Blocks 1-16: 1 MiB
	for i := 1; i < 17; i++ {
		if len(m.blocks[i]) != 1024*1024 {
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

	// Clear m.blocks to nil so FreeBlocks doesn't put our small manual slices into BlockPool1MPlus/BlockPool1M.
	m.blocks = nil
	m.FreeBlocks()
}

func TestInMessageGetFree(t *testing.T) {
	m := NewInMessage(0)

	// Case 1: len(m.blocks) == 0
	// Subcase A: allocateDst = true -> should return valid buffer via fallback allocation
	if buf := m.GetFree(10, true); len(buf) != 10 {
		t.Errorf("expected buffer of size 10 when no blocks allocated and allocateDst=true, got %v", buf)
	}
	m.FreeBlocks()

	// Subcase B: allocateDst = false -> should return nil
	if buf := m.GetFree(10, false); buf != nil {
		t.Errorf("expected nil when no blocks allocated and allocateDst=false, got %v", buf)
	}

	firstBlockSize := 1024*1024 + GetPageSize()
	m.AllocBlocks(firstBlockSize)
	m.size = 100 // Set message size to 100 bytes

	// Case 2: n <= 0 -> should return nil (regardless of allocateDst)
	if buf := m.GetFree(0, true); buf != nil {
		t.Errorf("expected nil for n=0, got %v", buf)
	}
	if buf := m.GetFree(-5, false); buf != nil {
		t.Errorf("expected nil for n=-5, got %v", buf)
	}

	// Case 3: n is larger than remaining space in blocks[0]
	// remaining is: firstBlockSize - 100
	tooLarge := firstBlockSize - 100 + 1

	// Subcase A: allocateDst = true -> should return fallback buffer
	bufTooLarge := m.GetFree(tooLarge, true)
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

	// Subcase B: allocateDst = false -> should return nil
	if buf := m.GetFree(tooLarge, false); buf != nil {
		t.Errorf("expected nil for too large request when allocateDst=false, got %v", buf)
	}

	// Case 4: normal allocation within remaining space (should work for both true and false)
	buf1 := m.GetFree(500, true)
	if len(buf1) != 500 {
		t.Errorf("expected buffer of size 500, got %d", len(buf1))
	}
	expectedStart := &m.blocks[0][100]
	if &buf1[0] != expectedStart {
		t.Errorf("expected buffer to start at index 100 of block 0")
	}

	// Slicing again (m.size is still 100, we don't advance m.size on GetFree)
	buf2 := m.GetFree(500, false)
	if len(buf2) != 500 {
		t.Errorf("expected buffer of size 500, got %d", len(buf2))
	}
	if &buf2[0] != expectedStart {
		t.Errorf("expected buffer to start at index 100 of block 0")
	}

	m.FreeBlocks()
}

var benchmarkSink []byte

func BenchmarkConsumeBytesSpanning(b *testing.B) {
	m := NewInMessage(0)
	firstBlockSize := 1024*1024 + GetPageSize()
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
		res := m.GetFree(10000, true)
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
