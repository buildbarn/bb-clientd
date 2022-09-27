package main

import (
	"bytes"
	"context"
	"io"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	cd_vfs "github.com/buildbarn/bb-clientd/pkg/filesystem/virtual"
	re_cas "github.com/buildbarn/bb-remote-execution/pkg/cas"
	re_vfs "github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
)

// GlobalTreeContext contains all attributes that are needed to
// construct Content Addressable Storage (CAS) backed directories.
// Directory contents are read from remoteexecution.Tree messages.
type GlobalTreeContext struct {
	re_vfs.CASFileFactory

	context          context.Context
	directoryFetcher re_cas.DirectoryFetcher
	handleAllocator  *re_vfs.ResolvableDigestHandleAllocator
	errorLogger      util.ErrorLogger
}

// NewGlobalTreeContext creates a new GlobalTreeContext. Because
// directories created through GlobalTreeContext can contain files, a
// CASFileFactory needs to be provided. remoteexecution.Tree messages
// are read through an IndexedTreeFetcher.
func NewGlobalTreeContext(ctx context.Context, casFileFactory re_vfs.CASFileFactory, directoryFetcher re_cas.DirectoryFetcher, handleAllocation re_vfs.StatelessHandleAllocation, errorLogger util.ErrorLogger) *GlobalTreeContext {
	gtc := &GlobalTreeContext{
		CASFileFactory:   casFileFactory,
		context:          ctx,
		directoryFetcher: directoryFetcher,
		errorLogger:      errorLogger,
	}
	gtc.handleAllocator = re_vfs.NewResolvableDigestHandleAllocator(handleAllocation, gtc.resolve)
	return gtc
}

// LookupTree creates a directory corresponding with the root directory
// of a remoteexecution.Tree message of a given digest.
func (gtc *GlobalTreeContext) LookupTree(blobDigest digest.Digest) re_vfs.Directory {
	tc := gtc.createTreeContext(blobDigest)
	d, _ := tc.createRootDirectory()
	return d
}

func (gtc *GlobalTreeContext) createTreeContext(blobDigest digest.Digest) *treeContext {
	tc := &treeContext{
		GlobalTreeContext: gtc,
		treeDigest:        blobDigest,
	}
	tc.handleAllocator = gtc.handleAllocator.New(blobDigest).AsResolvableAllocator(tc.resolve)
	return tc
}

func (gtc *GlobalTreeContext) resolve(blobDigest digest.Digest, r io.ByteReader) (re_vfs.DirectoryChild, re_vfs.Status) {
	tc := gtc.createTreeContext(blobDigest)
	return tc.resolve(r)
}

// treeContext contains the state associated with one or more
// directories that are all part of the same tree object.
type treeContext struct {
	*GlobalTreeContext

	treeDigest      digest.Digest
	handleAllocator re_vfs.ResolvableHandleAllocator
}

func (tc *treeContext) LookupDirectory(childDigest digest.Digest) re_vfs.Directory {
	d, _ := tc.createChildDirectory(childDigest)
	return d
}

func (tc *treeContext) createRootDirectory() (re_vfs.Directory, re_vfs.HandleResolver) {
	return cd_vfs.NewContentAddressableStorageDirectory(
		treeRootContext{
			treeContext: tc,
		},
		tc.treeDigest.GetInstanceName(),
		tc.handleAllocator.New(bytes.NewBuffer([]byte{0})),
		uint64(tc.treeDigest.GetSizeBytes()))
}

func (tc *treeContext) createChildDirectory(childDigest digest.Digest) (re_vfs.Directory, re_vfs.HandleResolver) {
	return cd_vfs.NewContentAddressableStorageDirectory(
		&treeChildContext{
			treeContext: tc,
			childDigest: childDigest,
		},
		tc.treeDigest.GetInstanceName(),
		tc.handleAllocator.New(bytes.NewBuffer(append([]byte{1}, childDigest.GetCompactBinary()...))),
		uint64(tc.treeDigest.GetSizeBytes()))
}

func (tc *treeContext) resolve(r io.ByteReader) (re_vfs.DirectoryChild, re_vfs.Status) {
	isChild, err := r.ReadByte()
	if err != nil {
		return re_vfs.DirectoryChild{}, re_vfs.StatusErrBadHandle
	}
	switch isChild {
	case 0:
		_, handleResolver := tc.createRootDirectory()
		return handleResolver(r)
	case 1:
		childDigest, err := tc.treeDigest.GetInstanceName().NewDigestFromCompactBinary(r)
		if err != nil {
			return re_vfs.DirectoryChild{}, re_vfs.StatusErrBadHandle
		}
		_, handleResolver := tc.createChildDirectory(childDigest)
		return handleResolver(r)
	default:
		return re_vfs.DirectoryChild{}, re_vfs.StatusErrBadHandle
	}
}

// treeRootContext contains the state associated with an instance of
// ContentAddressableStorageDirectory, representing the root directory
// of a tree.
type treeRootContext struct {
	*treeContext
}

func (trc treeRootContext) GetDirectoryContents() (*remoteexecution.Directory, re_vfs.Status) {
	directory, err := trc.directoryFetcher.GetTreeRootDirectory(trc.context, trc.treeDigest)
	if err != nil {
		trc.LogError(err)
		return nil, re_vfs.StatusErrIO
	}
	return directory, re_vfs.StatusOK
}

func (trc treeRootContext) LogError(err error) {
	trc.errorLogger.Log(util.StatusWrapf(err, "Tree %#v root directory", trc.treeDigest.String()))
}

// treeChildContext contains the state associated with an instance of
// ContentAddressableStorageDirectory, representing a child directory of
// a tree (i.e., not the root directory).
type treeChildContext struct {
	*treeContext

	childDigest digest.Digest
}

func (tcc *treeChildContext) GetDirectoryContents() (*remoteexecution.Directory, re_vfs.Status) {
	directory, err := tcc.directoryFetcher.GetTreeChildDirectory(tcc.context, tcc.treeDigest, tcc.childDigest)
	if err != nil {
		tcc.LogError(err)
		return nil, re_vfs.StatusErrIO
	}
	return directory, re_vfs.StatusOK
}

func (tcc *treeChildContext) LogError(err error) {
	tcc.errorLogger.Log(util.StatusWrapf(err, "Tree %#v child directory %#v", tcc.treeDigest.String(), tcc.childDigest.String()))
}
