package virtual

import (
	"io"

	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

type handleAllocatingCommandFileFactory struct {
	base      CommandFileFactory
	allocator *virtual.ResolvableDigestHandleAllocator
}

// NewHandleAllocatingCommandFileFactory creates a decorator for
// CommandFileFactory that creates shell scripts for launching build
// actions that have a resolvable handle associated with them. This
// gives every shell script its own inode number.
func NewHandleAllocatingCommandFileFactory(base CommandFileFactory, allocation virtual.StatelessHandleAllocation) CommandFileFactory {
	cff := &handleAllocatingCommandFileFactory{
		base: base,
	}
	cff.allocator = virtual.NewResolvableDigestHandleAllocator(allocation, cff.resolve)
	return cff
}

func (cff *handleAllocatingCommandFileFactory) LookupFile(blobDigest digest.Digest) (virtual.Leaf, virtual.Status) {
	leaf, s := cff.base.LookupFile(blobDigest)
	if s != virtual.StatusOK {
		return nil, s
	}
	return cff.allocator.New(blobDigest).AsLeaf(leaf), virtual.StatusOK
}

func (cff *handleAllocatingCommandFileFactory) resolve(blobDigest digest.Digest, remainder io.ByteReader) (virtual.DirectoryChild, virtual.Status) {
	leaf, s := cff.LookupFile(blobDigest)
	return virtual.DirectoryChild{}.FromLeaf(leaf), s
}
