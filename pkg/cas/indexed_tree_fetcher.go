package cas

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

// IndexedTree is equivalent to remoteexecution.Tree, except that it
// also stores a map that acts as an index of child directories. The map
// is indexed by the value returned by
// Digest.GetKey(KeyWithoutInstance). This allows callers to efficiently
// traverse the directory hierarchy contained inside a tree.
type IndexedTree struct {
	Tree  *remoteexecution.Tree
	Index map[string]int
}

// IndexedTreeFetcher is responsible for fetching Tree messages from the
// Content Addressable Storage (CAS). These describe the layout of a
// full directory hierarchy created as a build action output.
type IndexedTreeFetcher interface {
	GetIndexedTree(ctx context.Context, digest digest.Digest) (*IndexedTree, error)
}
