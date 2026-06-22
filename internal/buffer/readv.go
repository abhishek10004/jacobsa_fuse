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
	"unsafe"

	"golang.org/x/sys/unix"
)

func readv(fd int, packet [][]byte, iovecs []unix.Iovec) (n int, newIovecs []unix.Iovec, err error) {
	iovecs = iovecs[:0]
	for _, v := range packet {
		if len(v) == 0 {
			continue
		}
		vec := unix.Iovec{
			Base: &v[0],
		}
		vec.SetLen(len(v))
		iovecs = append(iovecs, vec)
	}
	if len(iovecs) == 0 {
		return 0, iovecs, nil
	}
	n1, _, e1 := unix.Syscall(
		unix.SYS_READV,
		uintptr(fd), uintptr(unsafe.Pointer(&iovecs[0])), uintptr(len(iovecs)),
	)
	n = int(n1)
	if e1 != 0 {
		err = unix.Errno(e1)
	}
	return n, iovecs, err
}
