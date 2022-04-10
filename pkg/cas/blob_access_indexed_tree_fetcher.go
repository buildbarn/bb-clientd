package cas

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

type blobAccessIndexedTreeFetcher struct {
	blobAccess              blobstore.BlobAccess
	maximumMessageSizeBytes int
}

// NewBlobAccessIndexedTreeFetcher creates an IndexedTreeFetcher that
// reads Tree objects from a BlobAccess based store.
func NewBlobAccessIndexedTreeFetcher(blobAccess blobstore.BlobAccess, maximumMessageSizeBytes int) IndexedTreeFetcher {
	return &blobAccessIndexedTreeFetcher{
		blobAccess:              blobAccess,
		maximumMessageSizeBytes: maximumMessageSizeBytes,
	}
}

func (itf *blobAccessIndexedTreeFetcher) GetIndexedTree(ctx context.Context, treeDigest digest.Digest) (*IndexedTree, error) {
	// Read the Tree message.
	m, err := itf.blobAccess.Get(ctx, treeDigest).ToProto(&remoteexecution.Tree{}, itf.maximumMessageSizeBytes)
	if err != nil {
		return nil, err
	}

	// Convert the Tree message to an IndexedTree.
	tree := m.(*remoteexecution.Tree)
	indexedTree := IndexedTree{
		Tree:  tree,
		Index: make(map[string]int, len(tree.Children)),
	}
	digestFunction := treeDigest.GetDigestFunction()
	for index, child := range tree.Children {
		// Marshal each of the directories, so that its digest
		// can be reobtained.
		data, err := proto.Marshal(child)
		if err != nil {
			return nil, util.StatusWrapfWithCode(err, codes.InvalidArgument, "Failed to marshal child directory at index %d", index)
		}
		digestGenerator := digestFunction.NewGenerator()
		if _, err := digestGenerator.Write(data); err != nil {
			panic(err)
		}
		indexedTree.Index[digestGenerator.Sum().GetKey(digest.KeyWithoutInstance)] = index
	}
	return &indexedTree, nil
}
