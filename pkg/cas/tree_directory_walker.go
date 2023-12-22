package cas

import (
	"context"
	"fmt"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

type treeDirectoryWalker struct {
	fetcher    cas.DirectoryFetcher
	treeDigest digest.Digest
}

// NewTreeDirectoryWalker creates a DirectoryWalker that assumes that
// all Directory messages are stored in a single Tree object in the
// Content Addressable Storage (CAS). This is the case for output
// directories of build actions.
func NewTreeDirectoryWalker(fetcher cas.DirectoryFetcher, treeDigest digest.Digest, rootDirectoryDigest *digest.Digest) cas.DirectoryWalker {
	dw := treeDirectoryWalker{
		fetcher:    fetcher,
		treeDigest: treeDigest,
	}
	if rootDirectoryDigest == nil {
		// Digest of the root directory contained in the Tree
		// message is unknown. Fall back to calling
		// DirectoryFetcher.GetTreeRootDirectory(), which is far
		// less efficient.
		return &treeRootDirectoryWalker{
			treeDirectoryWalker: dw,
		}
	}
	return &treeChildDirectoryWalker{
		treeDirectoryWalker: &dw,
		childDigest:         *rootDirectoryDigest,
	}
}

func (dw *treeDirectoryWalker) GetChild(childDigest digest.Digest) cas.DirectoryWalker {
	return &treeChildDirectoryWalker{
		treeDirectoryWalker: dw,
		childDigest:         childDigest,
	}
}

func (dw *treeDirectoryWalker) GetContainingDigest() digest.Digest {
	return dw.treeDigest
}

type treeRootDirectoryWalker struct {
	treeDirectoryWalker
}

func (dw *treeRootDirectoryWalker) GetDirectory(ctx context.Context) (*remoteexecution.Directory, error) {
	return dw.fetcher.GetTreeRootDirectory(ctx, dw.treeDigest)
}

func (dw *treeRootDirectoryWalker) GetDescription() string {
	return fmt.Sprintf("Tree %#v root directory", dw.treeDigest.String())
}

type treeChildDirectoryWalker struct {
	*treeDirectoryWalker
	childDigest digest.Digest
}

func (dw *treeChildDirectoryWalker) GetDirectory(ctx context.Context) (*remoteexecution.Directory, error) {
	return dw.fetcher.GetTreeChildDirectory(ctx, dw.treeDigest, dw.childDigest)
}

func (dw *treeChildDirectoryWalker) GetDescription() string {
	return fmt.Sprintf("Tree %#v child directory %#v", dw.treeDigest.String(), dw.childDigest.String())
}
