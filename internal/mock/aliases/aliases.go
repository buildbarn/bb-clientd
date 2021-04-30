package aliases

import (
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/fuse"
)

// This file contains aliases for some of the interfaces provided by the
// Go standard library. The only reason this file exists is to allow the
// gomock() Bazel rule to emit mocks for them, as that rule is only
// capable of emitting mocks for interfaces built through a
// go_library().
//
// It also contains aliases for some of the interfaces provided by the
// FUSE package. These aliases are used to rename them to prevent naming
// collisions with other interface types for which we want to generate
// mocks.

// FUSEDirectory is an alias of fuse.Directory.
type FUSEDirectory = fuse.Directory

// FUSELeaf is an alias of fuse.Leaf.
type FUSELeaf = fuse.Leaf
