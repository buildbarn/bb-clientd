package cas

import (
	"context"
	"fmt"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type treeDirectoryWalker struct {
	fetcher    IndexedTreeFetcher
	treeDigest digest.Digest
}

// NewTreeDirectoryWalker creates a DirectoryWalker that assumes that
// all Directory messages are stored in a single Tree object in the
// Content Addressable Storage (CAS). This is the case for output
// directories of build actions.
func NewTreeDirectoryWalker(fetcher IndexedTreeFetcher, treeDigest digest.Digest) cas.DirectoryWalker {
	return &treeRootDirectoryWalker{
		treeDirectoryWalker: treeDirectoryWalker{
			fetcher:    fetcher,
			treeDigest: treeDigest,
		},
	}
}

func (dw *treeDirectoryWalker) GetChild(childDigest digest.Digest) cas.DirectoryWalker {
	return &treeChildDirectoryWalker{
		treeDirectoryWalker: dw,
		childDigest:         childDigest.GetKey(digest.KeyWithoutInstance),
	}
}

func (dw *treeDirectoryWalker) GetContainingDigest() digest.Digest {
	return dw.treeDigest
}

type treeRootDirectoryWalker struct {
	treeDirectoryWalker
}

func (dw *treeRootDirectoryWalker) GetDirectory(ctx context.Context) (*remoteexecution.Directory, error) {
	indexedTree, err := dw.fetcher.GetIndexedTree(ctx, dw.treeDigest)
	if err != nil {
		return nil, err
	}
	if indexedTree.Tree.Root == nil {
		return nil, status.Error(codes.InvalidArgument, "Tree does not contain a root directory")
	}
	return indexedTree.Tree.Root, nil
}

func (dw *treeRootDirectoryWalker) GetDescription() string {
	return fmt.Sprintf("Tree %#v root directory", dw.treeDigest.String())
}

type treeChildDirectoryWalker struct {
	*treeDirectoryWalker
	childDigest string
}

func (dw *treeChildDirectoryWalker) GetDirectory(ctx context.Context) (*remoteexecution.Directory, error) {
	indexedTree, err := dw.fetcher.GetIndexedTree(ctx, dw.treeDigest)
	if err != nil {
		return nil, err
	}
	index, ok := indexedTree.Index[dw.childDigest]
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "Child directory not found in tree")
	}
	return indexedTree.Tree.Children[index], nil
}

func (dw *treeChildDirectoryWalker) GetDescription() string {
	return fmt.Sprintf("Tree %#v child directory %#v", dw.treeDigest.String(), dw.childDigest)
}
