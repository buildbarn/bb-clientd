package cas_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-clientd/internal/mock"
	"github.com/buildbarn/bb-clientd/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBlobAccessIndexedTreeFetcher(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	indexedTreeFetcher := cas.NewBlobAccessIndexedTreeFetcher(contentAddressableStorage, 10000)
	treeDigest := digest.MustNewDigest("hello", "cd1a9fb96ac93a583d6beadb6d7a5699", 1230)

	t.Run("IOError", func(t *testing.T) {
		// I/O errors on the backend should be propagated.
		contentAddressableStorage.EXPECT().Get(ctx, treeDigest).
			Return(buffer.NewBufferFromError(status.Error(codes.Internal, "Server on fire")))

		_, err := indexedTreeFetcher.GetIndexedTree(ctx, treeDigest)
		require.Equal(t, status.Error(codes.Internal, "Server on fire"), err)
	})

	t.Run("Success", func(t *testing.T) {
		// One root directory containing two subdirectories. The
		// resulting IndexedTree should have the two
		// subdirectories placed in a map indexed by digest.
		root := remoteexecution.Directory{
			Directories: []*remoteexecution.DirectoryNode{
				{
					Name: "foodir",
					Digest: &remoteexecution.Digest{
						Hash:      "0cf45a4d131d7833703567a271957409",
						SizeBytes: 45,
					},
				},
				{
					Name: "bardir",
					Digest: &remoteexecution.Digest{
						Hash:      "d9a4684f0733e504a39f6f1860acfcf6",
						SizeBytes: 46,
					},
				},
			},
		}
		subdir1 := remoteexecution.Directory{
			Files: []*remoteexecution.FileNode{
				{
					Name: "foo",
					Digest: &remoteexecution.Digest{
						Hash:      "218893837c9e94c2271b95fdac28fa4d",
						SizeBytes: 123,
					},
				},
			},
		}
		subdir2 := remoteexecution.Directory{
			Files: []*remoteexecution.FileNode{
				{
					Name: "bar",
					Digest: &remoteexecution.Digest{
						Hash:      "f988a36ed06e17f6c4a258ec8e03fe88",
						SizeBytes: 456,
					},
				},
			},
		}
		tree := &remoteexecution.Tree{
			Root:     &root,
			Children: []*remoteexecution.Directory{&subdir1, &subdir2},
		}
		contentAddressableStorage.EXPECT().Get(ctx, treeDigest).Return(buffer.NewProtoBufferFromProto(tree, buffer.UserProvided))

		indexedTree, err := indexedTreeFetcher.GetIndexedTree(ctx, treeDigest)
		require.NoError(t, err)
		require.Equal(t, &cas.IndexedTree{
			Tree: tree,
			Index: map[string]int{
				"0cf45a4d131d7833703567a271957409-45": 0,
				"d9a4684f0733e504a39f6f1860acfcf6-46": 1,
			},
		}, indexedTree)
	})
}
