package cas_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-clientd/internal/mock"
	"github.com/buildbarn/bb-clientd/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestTreeDirectoryWalker(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	indexedTreeFetcher := mock.NewMockIndexedTreeFetcher(ctrl)
	treeDigest := digest.MustNewDigest("example", "6884a9e20905b512d1122a2b1ad8ba16", 123)
	rootDirectoryWalker := cas.NewTreeDirectoryWalker(indexedTreeFetcher, treeDigest)

	exampleRootDirectory := &remoteexecution.Directory{
		Directories: []*remoteexecution.DirectoryNode{
			{
				Name: "foo",
				Digest: &remoteexecution.Digest{
					Hash:      "4df5f448a5e6b3c41e6aae7a8a9832aa",
					SizeBytes: 456,
				},
			},
		},
	}
	exampleChildDirectory := &remoteexecution.Directory{
		Files: []*remoteexecution.FileNode{
			{
				Name: "bar",
				Digest: &remoteexecution.Digest{
					Hash:      "f9c2df111171a614b738e157a482e117",
					SizeBytes: 789,
				},
			},
		},
	}
	exampleIndexedTree := &cas.IndexedTree{
		Tree: &remoteexecution.Tree{
			Root: exampleRootDirectory,
			Children: []*remoteexecution.Directory{
				exampleChildDirectory,
			},
		},
		Index: map[string]int{
			"4df5f448a5e6b3c41e6aae7a8a9832aa-456": 0,
		},
	}

	// Test that the DirectoryWalker loads the right object from the
	// CAS, and that error messages use the right prefix.

	t.Run("RootGetDirectorySuccess", func(t *testing.T) {
		indexedTreeFetcher.EXPECT().GetIndexedTree(ctx, treeDigest).
			Return(exampleIndexedTree, nil)
		rootDirectory, err := rootDirectoryWalker.GetDirectory(ctx)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, exampleRootDirectory, rootDirectory)
	})

	t.Run("RootGetDirectoryFailure", func(t *testing.T) {
		indexedTreeFetcher.EXPECT().GetIndexedTree(ctx, treeDigest).
			Return(nil, status.Error(codes.Internal, "Server failure"))
		_, err := rootDirectoryWalker.GetDirectory(ctx)
		require.Equal(t, status.Error(codes.Internal, "Server failure"), err)
	})

	t.Run("RootGetDirectoryMissing", func(t *testing.T) {
		indexedTreeFetcher.EXPECT().GetIndexedTree(ctx, treeDigest).Return(&cas.IndexedTree{
			Tree: &remoteexecution.Tree{},
		}, nil)
		_, err := rootDirectoryWalker.GetDirectory(ctx)
		require.Equal(t, status.Error(codes.InvalidArgument, "Tree does not contain a root directory"), err)
	})

	t.Run("RootGetDescription", func(t *testing.T) {
		require.Equal(
			t,
			"Tree \"6884a9e20905b512d1122a2b1ad8ba16-123-example\" root directory",
			rootDirectoryWalker.GetDescription())
	})

	t.Run("RootGetContainingDigest", func(t *testing.T) {
		require.Equal(
			t,
			treeDigest,
			rootDirectoryWalker.GetContainingDigest())
	})

	childDigest := digest.MustNewDigest("example", "4df5f448a5e6b3c41e6aae7a8a9832aa", 456)
	childDirectoryWalker := rootDirectoryWalker.GetChild(childDigest)

	// Repeat the tests above against a child directory, to make
	// sure those also return the the right object from the same
	// Tree object.

	t.Run("ChildGetDirectorySuccess", func(t *testing.T) {
		indexedTreeFetcher.EXPECT().GetIndexedTree(ctx, treeDigest).
			Return(exampleIndexedTree, nil)
		childDirectory, err := childDirectoryWalker.GetDirectory(ctx)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, exampleChildDirectory, childDirectory)
	})

	t.Run("ChildGetDirectoryMissing", func(t *testing.T) {
		indexedTreeFetcher.EXPECT().GetIndexedTree(ctx, treeDigest).
			Return(&cas.IndexedTree{}, nil)
		_, err := childDirectoryWalker.GetDirectory(ctx)
		require.Equal(t, status.Error(codes.InvalidArgument, "Child directory not found in tree"), err)
	})

	t.Run("ChildGetDescription", func(t *testing.T) {
		require.Equal(
			t,
			"Tree \"6884a9e20905b512d1122a2b1ad8ba16-123-example\" child directory \"4df5f448a5e6b3c41e6aae7a8a9832aa-456\"",
			childDirectoryWalker.GetDescription())
	})

	t.Run("ChildGetContainingDigest", func(t *testing.T) {
		require.Equal(
			t,
			treeDigest,
			childDirectoryWalker.GetContainingDigest())
	})
}
