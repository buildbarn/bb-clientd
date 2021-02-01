package main

import (
	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	cd_cas "github.com/buildbarn/bb-clientd/pkg/cas"
	cd_fuse "github.com/buildbarn/bb-clientd/pkg/filesystem/fuse"
	re_fuse "github.com/buildbarn/bb-remote-execution/pkg/filesystem/fuse"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/hanwen/go-fuse/v2/fuse"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GlobalTreeContext contains all attributes that are needed to
// construct Content Addressable Storage (CAS) backed directories.
// Directory contents are read from remoteexecution.Tree messages.
type GlobalTreeContext struct {
	*GlobalFileContext

	indexedTreeFetcher  cd_cas.IndexedTreeFetcher
	treeInodeNumberTree re_fuse.InodeNumberTree
}

// NewGlobalTreeContext creates a new GlobalTreeContext. Because
// directories created through GlobalTreeContext can contain files, a
// GlobalFileContext needs to be provided. remoteexecution.Tree messages
// are read through an IndexedTreeFetcher.
func NewGlobalTreeContext(globalFileContext *GlobalFileContext, indexedTreeFetcher cd_cas.IndexedTreeFetcher) *GlobalTreeContext {
	return &GlobalTreeContext{
		GlobalFileContext:   globalFileContext,
		indexedTreeFetcher:  indexedTreeFetcher,
		treeInodeNumberTree: re_fuse.NewRandomInodeNumberTree(),
	}
}

// LookupTree creates a directory corresponding with the root directory
// of a remoteexecution.Tree message of a given digest.
func (gtc *GlobalTreeContext) LookupTree(blobDigest digest.Digest, out *fuse.Attr) re_fuse.Directory {
	inodeNumberTree := gtc.treeInodeNumberTree.AddString(blobDigest.GetKey(digest.KeyWithInstance))
	d := cd_fuse.NewContentAddressableStorageDirectory(
		treeRootContext{
			treeContext: &treeContext{
				GlobalTreeContext: gtc,
				treeDigest:        blobDigest,
				inodeNumberTree:   inodeNumberTree,
			},
		},
		blobDigest.GetInstanceName(),
		inodeNumberTree.Get())
	d.FUSEGetAttr(out)
	return d
}

// treeContext contains the state associated with one or more
// directories that are all part of the same tree object.
type treeContext struct {
	*GlobalTreeContext

	treeDigest      digest.Digest
	inodeNumberTree re_fuse.InodeNumberTree
}

func (tc *treeContext) GetDirectoryInodeNumber(blobDigest digest.Digest) uint64 {
	return tc.inodeNumberTree.AddString(blobDigest.GetKey(digest.KeyWithInstance)).Get()
}

func (tc *treeContext) LookupDirectory(blobDigest digest.Digest, out *fuse.Attr) re_fuse.Directory {
	d := cd_fuse.NewContentAddressableStorageDirectory(
		&treeChildContext{
			treeContext: tc,
			childDigest: blobDigest.GetKey(digest.KeyWithoutInstance),
		},
		blobDigest.GetInstanceName(),
		tc.GetDirectoryInodeNumber(blobDigest))
	d.FUSEGetAttr(out)
	return d
}

// treeRootContext contains the state associated with an instance of
// ContentAddressableStorageDirectory, representing the root directory
// of a tree.
type treeRootContext struct {
	*treeContext
}

func (trc treeRootContext) GetDirectoryContents() (*remoteexecution.Directory, fuse.Status) {
	indexedTree, err := trc.indexedTreeFetcher.GetIndexedTree(trc.context, trc.treeDigest)
	if err != nil {
		trc.LogError(err)
		return nil, fuse.EIO
	}
	if indexedTree.Root == nil {
		trc.LogError(status.Error(codes.InvalidArgument, "Tree does not contain a root directory"))
		return nil, fuse.EIO
	}
	return indexedTree.Root, fuse.OK
}

func (trc treeRootContext) LogError(err error) {
	trc.errorLogger.Log(util.StatusWrapf(err, "Tree %#v root directory", trc.treeDigest.String()))
}

// treeChildContext contains the state associated with an instance of
// ContentAddressableStorageDirectory, representing a child directory of
// a tree (i.e., not the root directory).
type treeChildContext struct {
	*treeContext

	childDigest string
}

func (tcc *treeChildContext) GetDirectoryContents() (*remoteexecution.Directory, fuse.Status) {
	indexedTree, err := tcc.indexedTreeFetcher.GetIndexedTree(tcc.context, tcc.treeDigest)
	if err != nil {
		tcc.LogError(err)
		return nil, fuse.EIO
	}
	child, ok := indexedTree.Children[tcc.childDigest]
	if !ok {
		tcc.LogError(status.Error(codes.InvalidArgument, "Child directory not found in tree"))
		return nil, fuse.EIO
	}
	return child, fuse.OK
}

func (tcc *treeChildContext) LogError(err error) {
	tcc.errorLogger.Log(util.StatusWrapf(err, "Tree %#v child directory %#v", tcc.treeDigest.String(), tcc.childDigest))
}
