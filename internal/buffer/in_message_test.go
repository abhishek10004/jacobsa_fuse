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
	// Allocate blocks for 1MB + pageSize + 2000 bytes
	firstBlockSize := 1024*1024 + GetPageSize()
	m.AllocBlocks(firstBlockSize + 2000)

	msgLen := firstBlockSize + 2000

	// Build a dummy input stream
	data := make([]byte, msgLen)
	// Write InHeader
	header := (*fusekernel.InHeader)(unsafe.Pointer(&data[0]))
	header.Len = uint32(msgLen)
	header.Opcode = 123
	header.Unique = 456

	// Write some bytes spanning across the block boundary
	data[firstBlockSize-1] = 'Y'
	data[firstBlockSize] = 'Z'

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

	// Consume a dummy struct from Block 0 (offset 40 to 64)
	p := m.Consume(24)
	if p == nil {
		t.Fatalf("Consume returned nil")
	}

	// Consume remaining bytes of Block 0 up to block boundary (so consumed is firstBlockSize - 1)
	skip := firstBlockSize - 1 - 40 - 24
	m.Consume(uintptr(skip))

	// Now we are at the end of block 0. The next bytes are 'Y' and 'Z'.
	// This spans across the boundary.
	yz := m.ConsumeBytes(2)
	if string(yz) != "YZ" {
		t.Errorf("expected 'YZ', got %q", string(yz))
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
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		res := m.GetFree(10000)
		if len(res) != 10000 {
			b.Fatalf("expected 10000, got %d", len(res))
		}
		benchmarkSink = res
		m.FreeBlocks()
	}
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

