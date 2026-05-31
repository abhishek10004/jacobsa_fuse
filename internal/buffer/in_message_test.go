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
	"bytes"
	"testing"
	"unsafe"

	"github.com/jacobsa/fuse/internal/fusekernel"
)

func TestInMessageAllocAndFree(t *testing.T) {
	m := NewInMessage(0)
	m.AllocBlocks(17 * 1024 * 1024) // 17 MiB total size

	// 17 1 MiB blocks
	if len(m.blocks) != 17 {
		t.Errorf("expected 17 blocks, got %d", len(m.blocks))
	}

	// 1 MiB blocks
	for i := 0; i < 17; i++ {
		if len(m.blocks[i]) != 1024*1024 {
			t.Errorf("expected block %d to be 1 MiB, got %d", i, len(m.blocks[i]))
		}
	}

	// Shrink to fit for small message
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
	m.AllocBlocks(3 * 1024 * 1024) // 3 MiB total size

	// Populate mock data across blocks
	// block 0: 1 MiB
	// block 1: 1 MiB
	// block 2: 1 MiB
	msgLen := 3 * 1024 * 1024

	// Build a dummy input stream
	data := make([]byte, msgLen)
	// Write InHeader
	header := (*fusekernel.InHeader)(unsafe.Pointer(&data[0]))
	header.Len = uint32(msgLen)
	header.Opcode = 123
	header.Unique = 456

	// Write some bytes at the beginning of Block 1 (offset: 1 MiB)
	block1Offset := 1024 * 1024
	data[block1Offset] = 'A'
	data[block1Offset+1] = 'B'
	// Write some bytes across Block 1 / Block 2 boundary
	boundaryOffset := 2 * 1024 * 1024
	data[boundaryOffset-1] = 'Y'
	data[boundaryOffset] = 'Z'

	r := bytes.NewReader(data)
	err := m.Init(r)
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	if m.Header().Unique != 456 {
		t.Errorf("expected Unique = 456, got %d", m.Header().Unique)
	}

	// Consume a dummy struct from Block 0
	p := m.Consume(24)
	if p == nil {
		t.Fatalf("Consume returned nil")
	}

	// Consume remaining bytes to get 'A' and 'B' from the start of Block 1
	// consumed is currently 40 + 24 = 64.
	// We need to consume up to 1 MiB.
	skip := block1Offset - 64
	m.Consume(uintptr(skip))

	// Now we are at offset 1 MiB. The next bytes are 'A' and 'B'.
	ab := m.ConsumeBytes(2)
	if string(ab) != "AB" {
		t.Errorf("expected 'AB', got %q", string(ab))
	}

	// Now consume up to the boundary
	// consumed is currently 1 MiB + 2.
	// We need to consume up to 2 MiB - 1.
	skip = 1024*1024 - 3
	m.Consume(uintptr(skip))

	// Now we are 1 byte before the 2 MiB boundary (offset: 2 MiB - 1)
	yz := m.ConsumeBytes(2) // spans across boundary
	if string(yz) != "YZ" {
		t.Errorf("expected 'YZ' spanning across boundary, got %q", string(yz))
	}

	m.FreeBlocks()
}
