package fuse_test

import (
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-clientd/internal/mock"
	cd_fuse "github.com/buildbarn/bb-clientd/pkg/filesystem/fuse"
	re_fuse "github.com/buildbarn/bb-remote-execution/pkg/filesystem/fuse"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/golang/mock/gomock"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestContentAddressableStorageDirectoryFUSELookup(t *testing.T) {
	ctrl := gomock.NewController(t)

	directoryContext := mock.NewMockDirectoryContext(ctrl)
	d := cd_fuse.NewContentAddressableStorageDirectory(
		directoryContext,
		digest.MustNewInstanceName("example"),
		12345)

	t.Run("IOError", func(t *testing.T) {
		// I/O error while loading directory contents. There is
		// no need to log this explicitly, as that is done by
		// GetDirectoryContents() already.
		directoryContext.EXPECT().GetDirectoryContents().Return(nil, fuse.EIO)

		var out fuse.Attr
		_, _, s := d.FUSELookup(path.MustNewComponent("myfile"), &out)
		require.Equal(t, fuse.EIO, s)
	})

	// The remainder of the tests assume that GetDirectoryContents()
	// succeeds and always returns the following directory contents.
	directoryContext.EXPECT().GetDirectoryContents().Return(&remoteexecution.Directory{
		Directories: []*remoteexecution.DirectoryNode{
			{
				Name: "directory",
				Digest: &remoteexecution.Digest{
					Hash:      "47473788bad5e9991fcd8e8a2b6012745031089ebe6cc7342f78bf92570e4f52",
					SizeBytes: 42,
				},
			},
			{
				Name: "malformed_directory",
				Digest: &remoteexecution.Digest{
					Hash:      "This is a directory with a malformed hash",
					SizeBytes: 123,
				},
			},
		},
		Files: []*remoteexecution.FileNode{
			{
				Name: "executable",
				Digest: &remoteexecution.Digest{
					Hash:      "d3dda0e30611a0b3e98ee84a6c64d3eb7f174cd197a3713d0c44a35228bb33a7",
					SizeBytes: 12,
				},
				IsExecutable: true,
			},
			{
				Name: "file",
				Digest: &remoteexecution.Digest{
					Hash:      "059458af6543753150ceb7bcd4cc215e8aaabd61934ff6c67acdd9e7fb4cc96d",
					SizeBytes: 34,
				},
			},
			{
				Name: "malformed_file",
				Digest: &remoteexecution.Digest{
					Hash:      "This is a file with a malformed hash",
					SizeBytes: 123,
				},
			},
		},
		Symlinks: []*remoteexecution.SymlinkNode{
			{
				Name:   "symlink",
				Target: "target",
			},
		},
	}, fuse.OK).AnyTimes()

	t.Run("NotFound", func(t *testing.T) {
		// Attempting to look up files that don't exist.
		// Explicitly picking "aaa" and "zzz", as these both
		// cause the binary searching function to behave
		// differently.
		var out fuse.Attr
		_, _, s := d.FUSELookup(path.MustNewComponent("aaa"), &out)
		require.Equal(t, fuse.ENOENT, s)

		_, _, s = d.FUSELookup(path.MustNewComponent("zzz"), &out)
		require.Equal(t, fuse.ENOENT, s)
	})

	t.Run("MalformedDirectory", func(t *testing.T) {
		// Attempting to look up a directory for which the
		// digest is malformed.
		directoryContext.EXPECT().LogError(status.Error(codes.InvalidArgument, "Failed to parse digest for directory \"malformed_directory\": Unknown digest hash length: 41 characters"))

		var out fuse.Attr
		_, _, s := d.FUSELookup(path.MustNewComponent("malformed_directory"), &out)
		require.Equal(t, fuse.EIO, s)
	})

	t.Run("MalformedFile", func(t *testing.T) {
		// Attempting to look up a file for which the digest is
		// malformed.
		directoryContext.EXPECT().LogError(status.Error(codes.InvalidArgument, "Failed to parse digest for file \"malformed_file\": Unknown digest hash length: 36 characters"))

		var out fuse.Attr
		_, _, s := d.FUSELookup(path.MustNewComponent("malformed_file"), &out)
		require.Equal(t, fuse.EIO, s)
	})

	t.Run("SuccessDirectory", func(t *testing.T) {
		// Successfully looking up a directory.
		childDirectory := mock.NewMockFUSEDirectory(ctrl)
		directoryContext.EXPECT().LookupDirectory(
			digest.MustNewDigest("example", "47473788bad5e9991fcd8e8a2b6012745031089ebe6cc7342f78bf92570e4f52", 42),
			gomock.Any(),
		).DoAndReturn(func(digest digest.Digest, out *fuse.Attr) re_fuse.Directory {
			out.Ino = 123
			out.Mode = fuse.S_IFDIR | 0o111
			out.Nlink = 456
			return childDirectory
		})

		var out fuse.Attr
		actualDirectory, actualLeaf, s := d.FUSELookup(path.MustNewComponent("directory"), &out)
		require.Equal(t, fuse.OK, s)
		require.Equal(t, childDirectory, actualDirectory)
		require.Nil(t, actualLeaf)
		require.Equal(t, fuse.Attr{
			Ino:   123,
			Mode:  fuse.S_IFDIR | 0o111,
			Nlink: 456,
		}, out)
	})

	t.Run("SuccessExecutable", func(t *testing.T) {
		// Successfully looking up an executable file.
		childLeaf := mock.NewMockNativeLeaf(ctrl)
		directoryContext.EXPECT().LookupFile(
			digest.MustNewDigest("example", "d3dda0e30611a0b3e98ee84a6c64d3eb7f174cd197a3713d0c44a35228bb33a7", 12),
			true,
			gomock.Any(),
		).DoAndReturn(func(digest digest.Digest, isExecutable bool, out *fuse.Attr) re_fuse.Leaf {
			out.Ino = 123
			out.Mode = fuse.S_IFREG | 0o555
			out.Nlink = 456
			return childLeaf
		})

		var out fuse.Attr
		actualDirectory, actualLeaf, s := d.FUSELookup(path.MustNewComponent("executable"), &out)
		require.Equal(t, fuse.OK, s)
		require.Nil(t, actualDirectory)
		require.Equal(t, childLeaf, actualLeaf)
		require.Equal(t, fuse.Attr{
			Ino:   123,
			Mode:  fuse.S_IFREG | 0o555,
			Nlink: 456,
		}, out)
	})

	t.Run("SuccessFile", func(t *testing.T) {
		// Successfully looking up a non-executable file.
		childLeaf := mock.NewMockNativeLeaf(ctrl)
		directoryContext.EXPECT().LookupFile(
			digest.MustNewDigest("example", "059458af6543753150ceb7bcd4cc215e8aaabd61934ff6c67acdd9e7fb4cc96d", 34),
			false,
			gomock.Any(),
		).DoAndReturn(func(digest digest.Digest, isExecutable bool, out *fuse.Attr) re_fuse.Leaf {
			out.Ino = 123
			out.Mode = fuse.S_IFREG | 0o444
			out.Nlink = 456
			return childLeaf
		})

		var out fuse.Attr
		actualDirectory, actualLeaf, s := d.FUSELookup(path.MustNewComponent("file"), &out)
		require.Equal(t, fuse.OK, s)
		require.Nil(t, actualDirectory)
		require.Equal(t, childLeaf, actualLeaf)
		require.Equal(t, fuse.Attr{
			Ino:   123,
			Mode:  fuse.S_IFREG | 0o444,
			Nlink: 456,
		}, out)
	})

	t.Run("SuccessSymlink", func(t *testing.T) {
		// Successfully looking up a symbolic link.
		var out fuse.Attr
		actualDirectory, actualLeaf, s := d.FUSELookup(path.MustNewComponent("symlink"), &out)
		require.Equal(t, fuse.OK, s)
		require.Nil(t, actualDirectory)
		require.Equal(t, uint32(fuse.S_IFLNK|0o777), out.Mode)

		target, s := actualLeaf.FUSEReadlink()
		require.Equal(t, fuse.OK, s)
		require.Equal(t, []byte("target"), target)
	})
}

// TODO: Add unit testing coverage for FUSEReadDir() and FUSEReadDirPlus().
