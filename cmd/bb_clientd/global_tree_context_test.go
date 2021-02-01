package main_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	bb_clientd "github.com/buildbarn/bb-clientd/cmd/bb_clientd"
	"github.com/buildbarn/bb-clientd/internal/mock"
	"github.com/buildbarn/bb-clientd/pkg/cas"
	re_fuse "github.com/buildbarn/bb-remote-execution/pkg/filesystem/fuse"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/golang/mock/gomock"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGlobalTreeContextLookupTree(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	indexedTreeFetcher := mock.NewMockIndexedTreeFetcher(ctrl)
	globalTreeContext := bb_clientd.NewGlobalTreeContext(
		bb_clientd.NewGlobalFileContext(ctx, contentAddressableStorage, errorLogger),
		indexedTreeFetcher)

	// Start off testing on a root directory of a tree.
	treeDigest := digest.MustNewDigest("hello", "e0f28d311a9b2deff103e32f6105b2b29d636c287797ca72077a648cd736cd36", 123)
	var out fuse.Attr
	dRoot := globalTreeContext.LookupTree(treeDigest, &out)
	require.Equal(t, uint32(fuse.S_IFDIR|0555), out.Mode)
	require.Equal(t, uint32(re_fuse.ImplicitDirectoryLinkCount), out.Nlink)

	t.Run("RootIOError", func(t *testing.T) {
		// I/O errors when requesting the directory contents
		// should be forwarded to the error logger. The digest
		// of the tree should be prepended, included the fact
		// that this applied to loading the root directory.
		indexedTreeFetcher.EXPECT().GetIndexedTree(ctx, treeDigest).Return(nil, status.Error(codes.Internal, "Server on fire"))
		errorLogger.EXPECT().Log(status.Error(codes.Internal, "Tree \"e0f28d311a9b2deff103e32f6105b2b29d636c287797ca72077a648cd736cd36-123-hello\" root directory: Server on fire"))

		_, s := dRoot.FUSEReadDir()
		require.Equal(t, fuse.EIO, s)
	})

	t.Run("RootMissing", func(t *testing.T) {
		// The underlying remoteexecution.Tree object may have
		// been malformed, due to it not containing a root
		// directory. Such errors should be reported.
		indexedTreeFetcher.EXPECT().GetIndexedTree(ctx, treeDigest).Return(&cas.IndexedTree{}, nil)
		errorLogger.EXPECT().Log(status.Error(codes.InvalidArgument, "Tree \"e0f28d311a9b2deff103e32f6105b2b29d636c287797ca72077a648cd736cd36-123-hello\" root directory: Tree does not contain a root directory"))

		_, s := dRoot.FUSEReadDir()
		require.Equal(t, fuse.EIO, s)
	})

	t.Run("RootSuccess", func(t *testing.T) {
		indexedTreeFetcher.EXPECT().GetIndexedTree(ctx, treeDigest).Return(&cas.IndexedTree{
			Root: &remoteexecution.Directory{
				Files: []*remoteexecution.FileNode{
					{
						Name: "executable",
						Digest: &remoteexecution.Digest{
							Hash:      "32d757ab2b5c09e11daf0b0c04a3ba9da78e96fd24f9f838be0333f093354c82",
							SizeBytes: 42,
						},
						IsExecutable: true,
					},
					{
						Name: "file",
						Digest: &remoteexecution.Digest{
							Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
							SizeBytes: 11,
						},
					},
				},
			},
		}, nil)

		entries, s := dRoot.FUSEReadDir()
		require.Equal(t, fuse.OK, s)
		require.ElementsMatch(t, []fuse.DirEntry{
			{
				Name: "executable",
				Mode: fuse.S_IFREG,
				Ino: globalTreeContext.GetFileInodeNumber(
					digest.MustNewDigest("hello", "32d757ab2b5c09e11daf0b0c04a3ba9da78e96fd24f9f838be0333f093354c82", 42),
					true),
			},
			{
				Name: "file",
				Mode: fuse.S_IFREG,
				Ino: globalTreeContext.GetFileInodeNumber(
					digest.MustNewDigest("hello", "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c", 11),
					false),
			},
		}, entries)
	})

	// Continue testing on a child directory of a tree.
	indexedTreeFetcher.EXPECT().GetIndexedTree(ctx, treeDigest).Return(&cas.IndexedTree{
		Root: &remoteexecution.Directory{
			Directories: []*remoteexecution.DirectoryNode{
				{
					Name: "directory",
					Digest: &remoteexecution.Digest{
						Hash:      "cde6e00a0f207b218b57fe1a343c9bad353ad93a1cdacce29846acbf3c227842",
						SizeBytes: 112,
					},
				},
			},
		},
	}, nil)
	var outChild fuse.Attr
	dChild, _, s := dRoot.FUSELookup(path.MustNewComponent("directory"), &outChild)
	require.Equal(t, fuse.OK, s)
	require.Equal(t, uint32(fuse.S_IFDIR|0555), out.Mode)
	require.Equal(t, uint32(re_fuse.ImplicitDirectoryLinkCount), out.Nlink)

	t.Run("ChildIOError", func(t *testing.T) {
		// Just like for the root directory, I/O errors should
		// be captured. The error string should make it explicit
		// which child was being accessed.
		indexedTreeFetcher.EXPECT().GetIndexedTree(ctx, treeDigest).Return(nil, status.Error(codes.Internal, "Server on fire"))
		errorLogger.EXPECT().Log(status.Error(codes.Internal, "Tree \"e0f28d311a9b2deff103e32f6105b2b29d636c287797ca72077a648cd736cd36-123-hello\" child directory \"cde6e00a0f207b218b57fe1a343c9bad353ad93a1cdacce29846acbf3c227842-112\": Server on fire"))

		_, s := dChild.FUSEReadDir()
		require.Equal(t, fuse.EIO, s)
	})

	t.Run("ChildMissing", func(t *testing.T) {
		// The underlying remoteexecution.Tree object may have
		// been malformed, due to it containing references to
		// directories not present in the tree. Such errors
		// should be reported.
		indexedTreeFetcher.EXPECT().GetIndexedTree(ctx, treeDigest).Return(&cas.IndexedTree{}, nil)
		errorLogger.EXPECT().Log(status.Error(codes.InvalidArgument, "Tree \"e0f28d311a9b2deff103e32f6105b2b29d636c287797ca72077a648cd736cd36-123-hello\" child directory \"cde6e00a0f207b218b57fe1a343c9bad353ad93a1cdacce29846acbf3c227842-112\": Child directory not found in tree"))

		_, s := dChild.FUSEReadDir()
		require.Equal(t, fuse.EIO, s)
	})

	t.Run("ChildSuccess", func(t *testing.T) {
		indexedTreeFetcher.EXPECT().GetIndexedTree(ctx, treeDigest).Return(&cas.IndexedTree{
			Children: map[string]*remoteexecution.Directory{
				"cde6e00a0f207b218b57fe1a343c9bad353ad93a1cdacce29846acbf3c227842-112": {
					Files: []*remoteexecution.FileNode{
						{
							Name: "file",
							Digest: &remoteexecution.Digest{
								Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
								SizeBytes: 11,
							},
						},
					},
				},
			},
		}, nil)

		entries, s := dChild.FUSEReadDir()
		require.Equal(t, fuse.OK, s)
		require.ElementsMatch(t, []fuse.DirEntry{
			{
				Name: "file",
				Mode: fuse.S_IFREG,
				Ino: globalTreeContext.GetFileInodeNumber(
					digest.MustNewDigest("hello", "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c", 11),
					false),
			},
		}, entries)
	})
}
