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
	"syscall"
	"unsafe"
)

func readv(fd int, packet [][]byte) (n int, err error) {
	iovecs := make([]syscall.Iovec, 0, len(packet))
	for _, v := range packet {
		if len(v) == 0 {
			continue
		}
		vec := syscall.Iovec{
			Base: &v[0],
		}
		vec.SetLen(len(v))
		iovecs = append(iovecs, vec)
	}
	n1, _, e1 := syscall.Syscall(
		syscall.SYS_READV,
		uintptr(fd), uintptr(unsafe.Pointer(&iovecs[0])), uintptr(len(iovecs)),
	)
	n = int(n1)
	if e1 != 0 {
		err = syscall.Errno(e1)
	}
	return
}
