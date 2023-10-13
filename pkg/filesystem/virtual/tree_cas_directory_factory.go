package virtual

import (
	"bytes"
	"context"
	"io"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	re_cas "github.com/buildbarn/bb-remote-execution/pkg/cas"
	re_vfs "github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type treeCASDirectoryFactory struct {
	re_vfs.CASFileFactory

	context          context.Context
	directoryFetcher re_cas.DirectoryFetcher
	handleAllocator  *re_vfs.ResolvableDigestHandleAllocator
	errorLogger      util.ErrorLogger
}

// NewTreeCASDirectoryFactory creates a CASDirectoryFactory that is
// capable of creating directories that correspond to the root directory
// of REv2 Tree messages messages that are stored in a Content
// Addressable Storage (CAS).
func NewTreeCASDirectoryFactory(ctx context.Context, casFileFactory re_vfs.CASFileFactory, directoryFetcher re_cas.DirectoryFetcher, handleAllocation re_vfs.StatelessHandleAllocation, errorLogger util.ErrorLogger) CASDirectoryFactory {
	cdf := &treeCASDirectoryFactory{
		CASFileFactory:   casFileFactory,
		context:          ctx,
		directoryFetcher: directoryFetcher,
		errorLogger:      errorLogger,
	}
	cdf.handleAllocator = re_vfs.NewResolvableDigestHandleAllocator(handleAllocation, cdf.resolve)
	return cdf
}

func (cdf *treeCASDirectoryFactory) LookupDirectory(blobDigest digest.Digest) re_vfs.Directory {
	cdc := cdf.createTreeContext(blobDigest)
	d, _ := cdc.createRootDirectory()
	return d
}

func (cdf *treeCASDirectoryFactory) createTreeContext(blobDigest digest.Digest) *treeCASDirectoryContext {
	cdc := &treeCASDirectoryContext{
		treeCASDirectoryFactory: cdf,
		treeDigest:              blobDigest,
	}
	cdc.handleAllocator = cdf.handleAllocator.New(blobDigest).AsResolvableAllocator(cdc.resolve)
	return cdc
}

func (cdf *treeCASDirectoryFactory) resolve(blobDigest digest.Digest, r io.ByteReader) (re_vfs.DirectoryChild, re_vfs.Status) {
	cdc := cdf.createTreeContext(blobDigest)
	return cdc.resolve(r)
}

// treeContext contains the state associated with one or more
// directories that are all part of the same tree object.
type treeCASDirectoryContext struct {
	*treeCASDirectoryFactory

	treeDigest      digest.Digest
	handleAllocator re_vfs.ResolvableHandleAllocator
}

func (cdc *treeCASDirectoryContext) LookupDirectory(childDigest digest.Digest) re_vfs.Directory {
	d, _ := cdc.createChildDirectory(childDigest)
	return d
}

func (cdc *treeCASDirectoryContext) createRootDirectory() (re_vfs.Directory, re_vfs.HandleResolver) {
	return NewCASDirectory(
		treeRootCASDirectoryContext{
			treeCASDirectoryContext: cdc,
		},
		cdc.treeDigest.GetDigestFunction(),
		cdc.handleAllocator.New(bytes.NewBuffer([]byte{0})),
		uint64(cdc.treeDigest.GetSizeBytes()))
}

func (cdc *treeCASDirectoryContext) createChildDirectory(childDigest digest.Digest) (re_vfs.Directory, re_vfs.HandleResolver) {
	return NewCASDirectory(
		&treeChildCASDirectoryContext{
			treeCASDirectoryContext: cdc,
			childDigest:             childDigest,
		},
		cdc.treeDigest.GetDigestFunction(),
		cdc.handleAllocator.New(bytes.NewBuffer(append([]byte{1}, childDigest.GetCompactBinary()...))),
		uint64(cdc.treeDigest.GetSizeBytes()))
}

func (cdc *treeCASDirectoryContext) resolve(r io.ByteReader) (re_vfs.DirectoryChild, re_vfs.Status) {
	isChild, err := r.ReadByte()
	if err != nil {
		return re_vfs.DirectoryChild{}, re_vfs.StatusErrBadHandle
	}
	switch isChild {
	case 0:
		_, handleResolver := cdc.createRootDirectory()
		return handleResolver(r)
	case 1:
		childDigest, err := cdc.treeDigest.GetInstanceName().NewDigestFromCompactBinary(r)
		if err != nil {
			return re_vfs.DirectoryChild{}, re_vfs.StatusErrBadHandle
		}
		_, handleResolver := cdc.createChildDirectory(childDigest)
		return handleResolver(r)
	default:
		return re_vfs.DirectoryChild{}, re_vfs.StatusErrBadHandle
	}
}

// treeRootCASDirectoryContext contains the state associated with an
// instance of CASDirectory, representing the root directory of a tree.
type treeRootCASDirectoryContext struct {
	*treeCASDirectoryContext
}

func (cdc treeRootCASDirectoryContext) GetDirectoryContents() (*remoteexecution.Directory, re_vfs.Status) {
	directory, err := cdc.directoryFetcher.GetTreeRootDirectory(cdc.context, cdc.treeDigest)
	if err != nil {
		cdc.LogError(err)
		return nil, re_vfs.StatusErrIO
	}
	return directory, re_vfs.StatusOK
}

func (cdc treeRootCASDirectoryContext) LogError(err error) {
	cdc.errorLogger.Log(util.StatusWrapf(err, "Tree %#v root directory", cdc.treeDigest.String()))
}

// treeChildCASDirectoryContext contains the state associated with an
// instance of CASDirectory, representing a child directory of a tree
// (i.e., not the root directory).
type treeChildCASDirectoryContext struct {
	*treeCASDirectoryContext

	childDigest digest.Digest
}

func (cdc *treeChildCASDirectoryContext) GetDirectoryContents() (*remoteexecution.Directory, re_vfs.Status) {
	directory, err := cdc.directoryFetcher.GetTreeChildDirectory(cdc.context, cdc.treeDigest, cdc.childDigest)
	if err != nil {
		cdc.LogError(err)
		return nil, re_vfs.StatusErrIO
	}
	return directory, re_vfs.StatusOK
}

func (cdc *treeChildCASDirectoryContext) LogError(err error) {
	cdc.errorLogger.Log(util.StatusWrapf(err, "Tree %#v child directory %#v", cdc.treeDigest.String(), cdc.childDigest.String()))
}
