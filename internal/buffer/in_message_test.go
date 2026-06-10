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

	// 1 4KB block + 17 1 MiB blocks = 18 blocks
	if len(m.blocks) != 18 {
		t.Errorf("expected 18 blocks, got %d", len(m.blocks))
	}

	// Block 0: 4 KiB
	if len(m.blocks[0]) != 4096 {
		t.Errorf("expected block 0 to be 4 KiB, got %d", len(m.blocks[0]))
	}

	// Blocks 1-17: 1 MiB
	for i := 1; i < 18; i++ {
		if len(m.blocks[i]) != 1024*1024 {
			t.Errorf("expected block %d to be 1 MiB, got %d", i, len(m.blocks[i]))
		}
	}

	// Shrink to fit for small message (fits within the 4KB first block)
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
	// Allocate blocks for 4096 + 2000 bytes
	m.AllocBlocks(4096 + 2000)

	msgLen := 4096 + 2000

	// Build a dummy input stream
	data := make([]byte, msgLen)
	// Write InHeader
	header := (*fusekernel.InHeader)(unsafe.Pointer(&data[0]))
	header.Len = uint32(msgLen)
	header.Opcode = 123
	header.Unique = 456

	// Write some bytes spanning across the 4KB boundary
	data[4095] = 'Y'
	data[4096] = 'Z'

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

	// Consume remaining bytes of Block 0 up to offset 4095 (so consumed is 4095)
	skip := 4095 - 40 - 24
	m.Consume(uintptr(skip))

	// Now we are at offset 4095. The next bytes are 'Y' and 'Z'.
	// This spans across the 4KB boundary (since Block 0 size is 4096, index 4095 is last byte, 4096 is first byte of Block 1).
	yz := m.ConsumeBytes(2)
	if string(yz) != "YZ" {
		t.Errorf("expected 'YZ', got %q", string(yz))
	}

	m.FreeBlocks()
}
