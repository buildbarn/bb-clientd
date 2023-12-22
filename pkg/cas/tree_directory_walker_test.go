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

	directoryFetcher := mock.NewMockDirectoryFetcher(ctrl)
	treeDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "6884a9e20905b512d1122a2b1ad8ba16", 123)
	rootDirectoryWalker := cas.NewTreeDirectoryWalker(directoryFetcher, treeDigest, nil)

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

	// Test that the DirectoryWalker loads the right object from the
	// CAS, and that error messages use the right prefix.

	t.Run("RootGetDirectorySuccess", func(t *testing.T) {
		directoryFetcher.EXPECT().GetTreeRootDirectory(ctx, treeDigest).
			Return(exampleRootDirectory, nil)
		rootDirectory, err := rootDirectoryWalker.GetDirectory(ctx)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, exampleRootDirectory, rootDirectory)
	})

	t.Run("RootGetDirectoryFailure", func(t *testing.T) {
		directoryFetcher.EXPECT().GetTreeRootDirectory(ctx, treeDigest).
			Return(nil, status.Error(codes.Internal, "Server failure"))
		_, err := rootDirectoryWalker.GetDirectory(ctx)
		require.Equal(t, status.Error(codes.Internal, "Server failure"), err)
	})

	t.Run("RootGetDescription", func(t *testing.T) {
		require.Equal(
			t,
			"Tree \"3-6884a9e20905b512d1122a2b1ad8ba16-123-example\" root directory",
			rootDirectoryWalker.GetDescription())
	})

	t.Run("RootGetContainingDigest", func(t *testing.T) {
		require.Equal(
			t,
			treeDigest,
			rootDirectoryWalker.GetContainingDigest())
	})

	childDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "4df5f448a5e6b3c41e6aae7a8a9832aa", 456)
	childDirectoryWalker := rootDirectoryWalker.GetChild(childDigest)

	// Repeat the tests above against a child directory, to make
	// sure those also return the the right object from the same
	// Tree object.

	t.Run("ChildGetDirectorySuccess", func(t *testing.T) {
		directoryFetcher.EXPECT().GetTreeChildDirectory(ctx, treeDigest, childDigest).
			Return(exampleChildDirectory, nil)
		childDirectory, err := childDirectoryWalker.GetDirectory(ctx)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, exampleChildDirectory, childDirectory)
	})

	t.Run("ChildGetDirectoryFailure", func(t *testing.T) {
		directoryFetcher.EXPECT().GetTreeChildDirectory(ctx, treeDigest, childDigest).
			Return(nil, status.Error(codes.Internal, "Server failure"))
		_, err := childDirectoryWalker.GetDirectory(ctx)
		require.Equal(t, status.Error(codes.Internal, "Server failure"), err)
	})

	t.Run("ChildGetDescription", func(t *testing.T) {
		require.Equal(
			t,
			"Tree \"3-6884a9e20905b512d1122a2b1ad8ba16-123-example\" child directory \"3-4df5f448a5e6b3c41e6aae7a8a9832aa-456-example\"",
			childDirectoryWalker.GetDescription())
	})

	t.Run("ChildGetContainingDigest", func(t *testing.T) {
		require.Equal(
			t,
			treeDigest,
			childDirectoryWalker.GetContainingDigest())
	})
}
