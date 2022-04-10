package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	cd_cas "github.com/buildbarn/bb-clientd/pkg/cas"
	cd_vfs "github.com/buildbarn/bb-clientd/pkg/filesystem/virtual"
	re_vfs "github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GlobalTreeContext contains all attributes that are needed to
// construct Content Addressable Storage (CAS) backed directories.
// Directory contents are read from remoteexecution.Tree messages.
type GlobalTreeContext struct {
	re_vfs.CASFileFactory

	context            context.Context
	indexedTreeFetcher cd_cas.IndexedTreeFetcher
	handleAllocator    *re_vfs.ResolvableDigestHandleAllocator
	errorLogger        util.ErrorLogger
}

// NewGlobalTreeContext creates a new GlobalTreeContext. Because
// directories created through GlobalTreeContext can contain files, a
// CASFileFactory needs to be provided. remoteexecution.Tree messages
// are read through an IndexedTreeFetcher.
func NewGlobalTreeContext(ctx context.Context, casFileFactory re_vfs.CASFileFactory, indexedTreeFetcher cd_cas.IndexedTreeFetcher, handleAllocation re_vfs.StatelessHandleAllocation, errorLogger util.ErrorLogger) *GlobalTreeContext {
	gtc := &GlobalTreeContext{
		CASFileFactory:     casFileFactory,
		context:            ctx,
		indexedTreeFetcher: indexedTreeFetcher,
		errorLogger:        errorLogger,
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

func (gtc *GlobalTreeContext) resolve(blobDigest digest.Digest, r io.ByteReader) (re_vfs.Directory, re_vfs.Leaf, re_vfs.Status) {
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

func (tc *treeContext) createRootDirectory() (re_vfs.Directory, re_vfs.HandleResolver) {
	return cd_vfs.NewContentAddressableStorageDirectory(
		treeRootContext{
			treeContext: tc,
		},
		tc.treeDigest.GetInstanceName(),
		tc.handleAllocator.New(bytes.NewBuffer([]byte{0})),
		uint64(tc.treeDigest.GetSizeBytes()))
}

func (tc *treeContext) createChildDirectory(index uint64) (re_vfs.Directory, re_vfs.HandleResolver) {
	var encodedIndex [binary.MaxVarintLen64]byte
	return cd_vfs.NewContentAddressableStorageDirectory(
		&treeChildContext{
			treeContext: tc,
			index:       index,
		},
		tc.treeDigest.GetInstanceName(),
		tc.handleAllocator.New(bytes.NewBuffer(encodedIndex[:binary.PutUvarint(encodedIndex[:], index+1)])),
		uint64(tc.treeDigest.GetSizeBytes()))
}

func (tc *treeContext) resolve(r io.ByteReader) (re_vfs.Directory, re_vfs.Leaf, re_vfs.Status) {
	index, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, nil, re_vfs.StatusErrBadHandle
	}
	if index == 0 {
		_, handleResolver := tc.createRootDirectory()
		return handleResolver(r)
	}
	_, handleResolver := tc.createChildDirectory(index - 1)
	return handleResolver(r)
}

// treeRootContext contains the state associated with an instance of
// ContentAddressableStorageDirectory, representing the root directory
// of a tree.
type treeRootContext struct {
	*treeContext
}

func (trc treeRootContext) GetDirectoryContents() (*remoteexecution.Directory, cd_vfs.DirectoryContentsContext, re_vfs.Status) {
	indexedTree, err := trc.indexedTreeFetcher.GetIndexedTree(trc.context, trc.treeDigest)
	if err != nil {
		trc.LogError(err)
		return nil, nil, re_vfs.StatusErrIO
	}
	directory := indexedTree.Tree.Root
	if directory == nil {
		trc.LogError(status.Error(codes.InvalidArgument, "Tree does not contain a root directory"))
		return nil, nil, re_vfs.StatusErrIO
	}
	return directory, &treeContentsContext{
		treeContext:   trc.treeContext,
		logError:      trc.LogError,
		directories:   directory.Directories,
		childIndexMap: indexedTree.Index,
	}, re_vfs.StatusOK
}

func (trc treeRootContext) LogError(err error) {
	trc.errorLogger.Log(util.StatusWrapf(err, "Tree %#v root directory", trc.treeDigest.String()))
}

// treeChildContext contains the state associated with an instance of
// ContentAddressableStorageDirectory, representing a child directory of
// a tree (i.e., not the root directory).
type treeChildContext struct {
	*treeContext

	index uint64
}

func (tcc *treeChildContext) GetDirectoryContents() (*remoteexecution.Directory, cd_vfs.DirectoryContentsContext, re_vfs.Status) {
	indexedTree, err := tcc.indexedTreeFetcher.GetIndexedTree(tcc.context, tcc.treeDigest)
	if err != nil {
		tcc.LogError(err)
		return nil, nil, re_vfs.StatusErrIO
	}
	if tcc.index >= uint64(len(indexedTree.Tree.Children)) {
		tcc.LogError(status.Errorf(codes.InvalidArgument, "Directory index exceeds children list length of %d", len(indexedTree.Index)))
		return nil, nil, re_vfs.StatusErrIO
	}
	directory := indexedTree.Tree.Children[tcc.index]
	return directory, &treeContentsContext{
		treeContext:   tcc.treeContext,
		logError:      tcc.LogError,
		directories:   directory.Directories,
		childIndexMap: indexedTree.Index,
	}, re_vfs.StatusOK
}

func (tcc *treeChildContext) LogError(err error) {
	tcc.errorLogger.Log(util.StatusWrapf(err, "Tree %#v child directory index %d", tcc.treeDigest.String(), tcc.index))
}

type treeContentsContext struct {
	treeContext   *treeContext
	logError      func(err error)
	directories   []*remoteexecution.DirectoryNode
	childIndexMap map[string]int
}

func (tcc *treeContentsContext) LookupDirectory(childDigest digest.Digest) (re_vfs.Directory, re_vfs.Status) {
	childIndex, ok := tcc.childIndexMap[childDigest.GetKey(digest.KeyWithoutInstance)]
	if !ok {
		tcc.logError(status.Errorf(codes.InvalidArgument, "Child directory with digest %#v not found in tree", childDigest.String()))
		return nil, re_vfs.StatusErrIO
	}
	d, _ := tcc.treeContext.createChildDirectory(uint64(childIndex))
	return d, re_vfs.StatusOK
}
