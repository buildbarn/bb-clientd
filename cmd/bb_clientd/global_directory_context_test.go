package main_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	bb_clientd "github.com/buildbarn/bb-clientd/cmd/bb_clientd"
	"github.com/buildbarn/bb-clientd/internal/mock"
	re_fuse "github.com/buildbarn/bb-remote-execution/pkg/filesystem/fuse"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/golang/mock/gomock"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGlobalDirectoryContextLookupDirectory(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	directoryFetcher := mock.NewMockDirectoryFetcher(ctrl)
	globalDirectoryContext := bb_clientd.NewGlobalDirectoryContext(
		bb_clientd.NewGlobalFileContext(ctx, contentAddressableStorage, errorLogger),
		directoryFetcher)

	directoryDigest := digest.MustNewDigest("hello", "e0f28d311a9b2deff103e32f6105b2b29d636c287797ca72077a648cd736cd36", 123)
	var out fuse.Attr
	d := globalDirectoryContext.LookupDirectory(directoryDigest, &out)
	require.Equal(t, fuse.Attr{
		Mode:  fuse.S_IFDIR | 0o555,
		Ino:   globalDirectoryContext.GetDirectoryInodeNumber(directoryDigest),
		Nlink: re_fuse.ImplicitDirectoryLinkCount,
	}, out)

	t.Run("IOError", func(t *testing.T) {
		// I/O errors when requesting the directory contents
		// should be forwarded to the error logger. The digest
		// of the directory should be prepended to the error
		// message.
		directoryFetcher.EXPECT().GetDirectory(ctx, directoryDigest).Return(nil, status.Error(codes.Internal, "Server on fire"))
		errorLogger.EXPECT().Log(status.Error(codes.Internal, "Directory \"e0f28d311a9b2deff103e32f6105b2b29d636c287797ca72077a648cd736cd36-123-hello\": Server on fire"))

		_, s := d.FUSEReadDir()
		require.Equal(t, fuse.EIO, s)
	})

	t.Run("MalformedDirectory", func(t *testing.T) {
		// Malformed directories should also be reported.
		directoryFetcher.EXPECT().GetDirectory(ctx, directoryDigest).Return(&remoteexecution.Directory{
			Files: []*remoteexecution.FileNode{
				{
					Name: "broken",
					Digest: &remoteexecution.Digest{
						Hash: "This is not a valid hash",
					},
				},
			},
		}, nil)
		errorLogger.EXPECT().Log(status.Error(codes.InvalidArgument, "Directory \"e0f28d311a9b2deff103e32f6105b2b29d636c287797ca72077a648cd736cd36-123-hello\": Failed to parse digest for file \"broken\": Unknown digest hash length: 24 characters"))

		_, s := d.FUSEReadDir()
		require.Equal(t, fuse.EIO, s)
	})

	t.Run("Success", func(t *testing.T) {
		// Successfully obtain a directory listing. All entries
		// should be converted to a FUSE directory entry.
		directoryFetcher.EXPECT().GetDirectory(ctx, directoryDigest).Return(&remoteexecution.Directory{
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
			Directories: []*remoteexecution.DirectoryNode{
				{
					Name: "directory",
					Digest: &remoteexecution.Digest{
						Hash:      "cde6e00a0f207b218b57fe1a343c9bad353ad93a1cdacce29846acbf3c227842",
						SizeBytes: 112,
					},
				},
			},
		}, nil)

		entries, s := d.FUSEReadDir()
		require.Equal(t, fuse.OK, s)
		require.ElementsMatch(t, []fuse.DirEntry{
			{
				Name: "executable",
				Mode: fuse.S_IFREG,
				Ino: globalDirectoryContext.GetFileInodeNumber(
					digest.MustNewDigest("hello", "32d757ab2b5c09e11daf0b0c04a3ba9da78e96fd24f9f838be0333f093354c82", 42),
					true),
			},
			{
				Name: "file",
				Mode: fuse.S_IFREG,
				Ino: globalDirectoryContext.GetFileInodeNumber(
					digest.MustNewDigest("hello", "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c", 11),
					false),
			},
			{
				Name: "directory",
				Mode: fuse.S_IFDIR,
				Ino: globalDirectoryContext.GetDirectoryInodeNumber(
					digest.MustNewDigest("hello", "cde6e00a0f207b218b57fe1a343c9bad353ad93a1cdacce29846acbf3c227842", 112)),
			},
		}, entries)
	})
}
