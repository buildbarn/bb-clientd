package aliases

import (
	"io"
)

// This file contains aliases for some of the interfaces provided by the
// Go standard library. The only reason this file exists is to allow the
// gomock() Bazel rule to emit mocks for them, as that rule is only
// capable of emitting mocks for interfaces built through a
// go_library().

// ReaderAt is an alias of io.ReaderAt.
type ReaderAt = io.ReaderAt

// WriterAt is an alias of io.WriterAt.
type WriterAt = io.WriterAt
