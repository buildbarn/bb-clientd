package virtual_test

import (
	"bytes"
	"io"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-clientd/internal/mock"
	cd_vfs "github.com/buildbarn/bb-clientd/pkg/filesystem/virtual"
	re_vfs "github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func contentAddressableStorageDirectoryExpectLookupSelf(t *testing.T, ctrl *gomock.Controller, handleAllocator *mock.MockResolvableHandleAllocator) {
	handleAllocation := mock.NewMockResolvableHandleAllocation(ctrl)
	handleAllocator.EXPECT().New(gomock.Any()).DoAndReturn(func(id io.WriterTo) re_vfs.ResolvableHandleAllocation {
		actualIdentifier := bytes.NewBuffer(nil)
		n, err := id.WriteTo(actualIdentifier)
		require.NoError(t, err)
		require.Equal(t, int64(1), n)
		require.Equal(t, []byte{0}, actualIdentifier.Bytes())
		return handleAllocation
	})
	handleAllocation.EXPECT().AsStatelessDirectory(gomock.Any()).
		DoAndReturn(func(directory re_vfs.Directory) re_vfs.Directory { return directory })
}

func contentAddressableStorageDirectoryExpectLookupSymlink(t *testing.T, ctrl *gomock.Controller, handleAllocator *mock.MockResolvableHandleAllocator, expectedIdentifier []byte) {
	handleAllocation := mock.NewMockResolvableHandleAllocation(ctrl)
	handleAllocator.EXPECT().New(gomock.Any()).DoAndReturn(func(id io.WriterTo) re_vfs.ResolvableHandleAllocation {
		actualIdentifier := bytes.NewBuffer(nil)
		n, err := id.WriteTo(actualIdentifier)
		require.NoError(t, err)
		require.Equal(t, int64(len(expectedIdentifier)), n)
		require.Equal(t, expectedIdentifier, actualIdentifier.Bytes())
		return handleAllocation
	})
	handleAllocation.EXPECT().AsNativeLeaf(gomock.Any()).
		DoAndReturn(func(leaf re_vfs.NativeLeaf) re_vfs.NativeLeaf { return leaf })
}

func TestContentAddressableStorageDirectoryVirtualLookup(t *testing.T) {
	ctrl := gomock.NewController(t)

	directoryContext := mock.NewMockDirectoryContext(ctrl)
	rootHandleAllocation := mock.NewMockResolvableHandleAllocation(ctrl)
	handleAllocator := mock.NewMockResolvableHandleAllocator(ctrl)
	rootHandleAllocation.EXPECT().AsResolvableAllocator(gomock.Any()).Return(handleAllocator)
	contentAddressableStorageDirectoryExpectLookupSelf(t, ctrl, handleAllocator)
	d, _ := cd_vfs.NewContentAddressableStorageDirectory(
		directoryContext,
		digest.MustNewInstanceName("example"),
		rootHandleAllocation,
		/* sizeBytes = */ 42)

	t.Run("IOError", func(t *testing.T) {
		// I/O error while loading directory contents. There is
		// no need to log this explicitly, as that is done by
		// GetDirectoryContents() already.
		directoryContext.EXPECT().GetDirectoryContents().Return(nil, nil, re_vfs.StatusErrIO)

		var out re_vfs.Attributes
		_, _, s := d.VirtualLookup(path.MustNewComponent("myfile"), 0, &out)
		require.Equal(t, re_vfs.StatusErrIO, s)
	})

	// The remainder of the tests assume that GetDirectoryContents()
	// succeeds and always returns the following directory contents.
	directoryContentsContext := mock.NewMockDirectoryContentsContext(ctrl)
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
	}, directoryContentsContext, re_vfs.StatusOK).AnyTimes()

	t.Run("NotFound", func(t *testing.T) {
		// Attempting to look up files that don't exist.
		// Explicitly picking "aaa" and "zzz", as these both
		// cause the binary searching function to behave
		// differently.
		var out re_vfs.Attributes
		_, _, s := d.VirtualLookup(path.MustNewComponent("aaa"), 0, &out)
		require.Equal(t, re_vfs.StatusErrNoEnt, s)

		_, _, s = d.VirtualLookup(path.MustNewComponent("zzz"), 0, &out)
		require.Equal(t, re_vfs.StatusErrNoEnt, s)
	})

	t.Run("MalformedDirectory", func(t *testing.T) {
		// Attempting to look up a directory for which the
		// digest is malformed.
		directoryContext.EXPECT().LogError(status.Error(codes.InvalidArgument, "Failed to parse digest for directory \"malformed_directory\": Unknown digest hash length: 41 characters"))

		var out re_vfs.Attributes
		_, _, s := d.VirtualLookup(path.MustNewComponent("malformed_directory"), 0, &out)
		require.Equal(t, re_vfs.StatusErrIO, s)
	})

	t.Run("MalformedFile", func(t *testing.T) {
		// Attempting to look up a file for which the digest is
		// malformed.
		directoryContext.EXPECT().LogError(status.Error(codes.InvalidArgument, "Failed to parse digest for file \"malformed_file\": Unknown digest hash length: 36 characters"))

		var out re_vfs.Attributes
		_, _, s := d.VirtualLookup(path.MustNewComponent("malformed_file"), 0, &out)
		require.Equal(t, re_vfs.StatusErrIO, s)
	})

	t.Run("SuccessDirectory", func(t *testing.T) {
		// Successfully looking up a directory.
		childDirectory := mock.NewMockVirtualDirectory(ctrl)
		directoryContentsContext.EXPECT().LookupDirectory(
			digest.MustNewDigest("example", "47473788bad5e9991fcd8e8a2b6012745031089ebe6cc7342f78bf92570e4f52", 42),
		).Return(childDirectory, re_vfs.StatusOK)
		childDirectory.EXPECT().VirtualGetAttributes(
			re_vfs.AttributesMaskInodeNumber,
			gomock.Any(),
		).Do(func(requested re_vfs.AttributesMask, out *re_vfs.Attributes) {
			out.SetInodeNumber(123)
		})

		var out re_vfs.Attributes
		actualDirectory, actualLeaf, s := d.VirtualLookup(path.MustNewComponent("directory"), re_vfs.AttributesMaskInodeNumber, &out)
		require.Equal(t, re_vfs.StatusOK, s)
		require.Equal(t, childDirectory, actualDirectory)
		require.Nil(t, actualLeaf)
		require.Equal(t, *(&re_vfs.Attributes{}).SetInodeNumber(123), out)
	})

	t.Run("SuccessExecutable", func(t *testing.T) {
		// Successfully looking up an executable file.
		childLeaf := mock.NewMockNativeLeaf(ctrl)
		directoryContext.EXPECT().LookupFile(
			digest.MustNewDigest("example", "d3dda0e30611a0b3e98ee84a6c64d3eb7f174cd197a3713d0c44a35228bb33a7", 12),
			true,
		).Return(childLeaf)
		childLeaf.EXPECT().VirtualGetAttributes(
			re_vfs.AttributesMaskInodeNumber,
			gomock.Any(),
		).Do(func(requested re_vfs.AttributesMask, out *re_vfs.Attributes) {
			out.SetInodeNumber(123)
		})

		var out re_vfs.Attributes
		actualDirectory, actualLeaf, s := d.VirtualLookup(path.MustNewComponent("executable"), re_vfs.AttributesMaskInodeNumber, &out)
		require.Equal(t, re_vfs.StatusOK, s)
		require.Nil(t, actualDirectory)
		require.Equal(t, childLeaf, actualLeaf)
		require.Equal(t, *(&re_vfs.Attributes{}).SetInodeNumber(123), out)
	})

	t.Run("SuccessFile", func(t *testing.T) {
		// Successfully looking up a non-executable file.
		childLeaf := mock.NewMockNativeLeaf(ctrl)
		directoryContext.EXPECT().LookupFile(
			digest.MustNewDigest("example", "059458af6543753150ceb7bcd4cc215e8aaabd61934ff6c67acdd9e7fb4cc96d", 34),
			false,
		).Return(childLeaf)
		childLeaf.EXPECT().VirtualGetAttributes(
			re_vfs.AttributesMaskInodeNumber,
			gomock.Any(),
		).Do(func(requested re_vfs.AttributesMask, out *re_vfs.Attributes) {
			out.SetInodeNumber(123)
		})

		var out re_vfs.Attributes
		actualDirectory, actualLeaf, s := d.VirtualLookup(path.MustNewComponent("file"), re_vfs.AttributesMaskInodeNumber, &out)
		require.Equal(t, re_vfs.StatusOK, s)
		require.Nil(t, actualDirectory)
		require.Equal(t, childLeaf, actualLeaf)
		require.Equal(t, *(&re_vfs.Attributes{}).SetInodeNumber(123), out)
	})

	t.Run("SuccessSymlink", func(t *testing.T) {
		// Successfully looking up a symbolic link.
		contentAddressableStorageDirectoryExpectLookupSymlink(t, ctrl, handleAllocator, []byte{1})

		var out re_vfs.Attributes
		actualDirectory, actualLeaf, s := d.VirtualLookup(path.MustNewComponent("symlink"), re_vfs.AttributesMaskFileType, &out)
		require.Equal(t, re_vfs.StatusOK, s)
		require.Nil(t, actualDirectory)
		require.Equal(t, filesystem.FileTypeSymlink, out.GetFileType())

		target, s := actualLeaf.VirtualReadlink()
		require.Equal(t, re_vfs.StatusOK, s)
		require.Equal(t, []byte("target"), target)
	})
}

func TestContentAddressableStorageDirectoryVirtualReadDir(t *testing.T) {
	ctrl := gomock.NewController(t)

	directoryContext := mock.NewMockDirectoryContext(ctrl)
	rootHandleAllocation := mock.NewMockResolvableHandleAllocation(ctrl)
	handleAllocator := mock.NewMockResolvableHandleAllocator(ctrl)
	rootHandleAllocation.EXPECT().AsResolvableAllocator(gomock.Any()).Return(handleAllocator)
	contentAddressableStorageDirectoryExpectLookupSelf(t, ctrl, handleAllocator)
	d, _ := cd_vfs.NewContentAddressableStorageDirectory(
		directoryContext,
		digest.MustNewInstanceName("example"),
		rootHandleAllocation,
		/* sizeBytes = */ 42)

	t.Run("IOError", func(t *testing.T) {
		// I/O error while loading directory contents. There is
		// no need to log this explicitly, as that is done by
		// GetDirectoryContents() already.
		directoryContext.EXPECT().GetDirectoryContents().Return(nil, nil, re_vfs.StatusErrIO)
		reporter := mock.NewMockDirectoryEntryReporter(ctrl)

		require.Equal(
			t,
			re_vfs.StatusErrIO,
			d.VirtualReadDir(0, re_vfs.AttributesMaskInodeNumber, reporter))
	})

	t.Run("MalformedDirectory1", func(t *testing.T) {
		// Directories with malformed names may not be reported.
		directoryContentsContext := mock.NewMockDirectoryContentsContext(ctrl)
		directoryContext.EXPECT().GetDirectoryContents().Return(&remoteexecution.Directory{
			Directories: []*remoteexecution.DirectoryNode{
				{
					Name: "..",
					Digest: &remoteexecution.Digest{
						Hash:      "47473788bad5e9991fcd8e8a2b6012745031089ebe6cc7342f78bf92570e4f52",
						SizeBytes: 42,
					},
				},
			},
		}, directoryContentsContext, re_vfs.StatusOK)
		directoryContext.EXPECT().LogError(status.Error(codes.InvalidArgument, "Directory \"..\" has an invalid name"))
		reporter := mock.NewMockDirectoryEntryReporter(ctrl)

		require.Equal(
			t,
			re_vfs.StatusErrIO,
			d.VirtualReadDir(0, re_vfs.AttributesMaskInodeNumber, reporter))
	})

	t.Run("MalformedDirectory2", func(t *testing.T) {
		// Directories with malformed digests may not be reported.
		directoryContentsContext := mock.NewMockDirectoryContentsContext(ctrl)
		directoryContext.EXPECT().GetDirectoryContents().Return(&remoteexecution.Directory{
			Directories: []*remoteexecution.DirectoryNode{
				{
					Name: "hello",
					Digest: &remoteexecution.Digest{
						Hash:      "This is a directory with a malformed hash",
						SizeBytes: 123,
					},
				},
			},
		}, directoryContentsContext, re_vfs.StatusOK)
		directoryContext.EXPECT().LogError(status.Error(codes.InvalidArgument, "Failed to parse digest for directory \"hello\": Unknown digest hash length: 41 characters"))
		reporter := mock.NewMockDirectoryEntryReporter(ctrl)

		require.Equal(
			t,
			re_vfs.StatusErrIO,
			d.VirtualReadDir(0, re_vfs.AttributesMaskInodeNumber, reporter))
	})

	t.Run("NoSpaceDirectory", func(t *testing.T) {
		// If there is no space to fit the directory, iteration
		// should stop.
		directoryContentsContext := mock.NewMockDirectoryContentsContext(ctrl)
		directoryContext.EXPECT().GetDirectoryContents().Return(&remoteexecution.Directory{
			Directories: []*remoteexecution.DirectoryNode{
				{
					Name: "hello",
					Digest: &remoteexecution.Digest{
						Hash:      "47473788bad5e9991fcd8e8a2b6012745031089ebe6cc7342f78bf92570e4f52",
						SizeBytes: 42,
					},
				},
				{
					Name: "world",
					Digest: &remoteexecution.Digest{
						Hash:      "fc3978ff06a7e5f84737097f8ecc9a3891bf008e78b7cb601997b9ad2de62009",
						SizeBytes: 1000,
					},
				},
			},
		}, directoryContentsContext, re_vfs.StatusOK)
		reporter := mock.NewMockDirectoryEntryReporter(ctrl)
		childDirectory := mock.NewMockVirtualDirectory(ctrl)
		directoryContentsContext.EXPECT().LookupDirectory(
			digest.MustNewDigest("example", "47473788bad5e9991fcd8e8a2b6012745031089ebe6cc7342f78bf92570e4f52", 42),
		).Return(childDirectory, re_vfs.StatusOK)
		childDirectory.EXPECT().VirtualGetAttributes(
			re_vfs.AttributesMaskInodeNumber,
			gomock.Any(),
		).Do(func(requested re_vfs.AttributesMask, out *re_vfs.Attributes) {
			out.SetInodeNumber(123)
		})
		reporter.EXPECT().ReportDirectory(
			uint64(1),
			path.MustNewComponent("hello"),
			childDirectory,
			(&re_vfs.Attributes{}).SetInodeNumber(123),
		).Return(false)

		require.Equal(
			t,
			re_vfs.StatusOK,
			d.VirtualReadDir(0, re_vfs.AttributesMaskInodeNumber, reporter))
	})

	directoryContentsContext := mock.NewMockDirectoryContentsContext(ctrl)
	directoryContext.EXPECT().GetDirectoryContents().Return(&remoteexecution.Directory{
		Directories: []*remoteexecution.DirectoryNode{
			{
				Name: "directory1",
				Digest: &remoteexecution.Digest{
					Hash:      "f514a041bf7ae6ea7ec82e8296e17e10cffdf799ba565e052af59187936f1865",
					SizeBytes: 123,
				},
			},
		},
		Files: []*remoteexecution.FileNode{
			{
				Name: "executable",
				Digest: &remoteexecution.Digest{
					Hash:      "473b6cb5358c3f8a086db591259ac33eac875d1ae3e37737bce210c1e9ea3503",
					SizeBytes: 100,
				},
				IsExecutable: true,
			},
			{
				Name: "file",
				Digest: &remoteexecution.Digest{
					Hash:      "0ac567103ab10e4b6bfca9b1d3387baad93dee899be5e5cbc3859e01363fbdaa",
					SizeBytes: 200,
				},
			},
		},
		Symlinks: []*remoteexecution.SymlinkNode{
			{
				Name:   "symlink",
				Target: "target",
			},
		},
	}, directoryContentsContext, re_vfs.StatusOK).AnyTimes()

	t.Run("FromStart", func(t *testing.T) {
		reporter := mock.NewMockDirectoryEntryReporter(ctrl)
		childDirectory := mock.NewMockVirtualDirectory(ctrl)
		directoryContentsContext.EXPECT().LookupDirectory(
			digest.MustNewDigest("example", "f514a041bf7ae6ea7ec82e8296e17e10cffdf799ba565e052af59187936f1865", 123),
		).Return(childDirectory, re_vfs.StatusOK)
		childDirectory.EXPECT().VirtualGetAttributes(
			re_vfs.AttributesMaskInodeNumber,
			gomock.Any(),
		).Do(func(requested re_vfs.AttributesMask, out *re_vfs.Attributes) {
			out.SetInodeNumber(123)
		})
		reporter.EXPECT().ReportDirectory(
			uint64(1),
			path.MustNewComponent("directory1"),
			childDirectory,
			(&re_vfs.Attributes{}).SetInodeNumber(123),
		).Return(true)
		childLeaf1 := mock.NewMockNativeLeaf(ctrl)
		directoryContext.EXPECT().LookupFile(
			digest.MustNewDigest("example", "473b6cb5358c3f8a086db591259ac33eac875d1ae3e37737bce210c1e9ea3503", 100),
			true,
		).Return(childLeaf1)
		childLeaf1.EXPECT().VirtualGetAttributes(
			re_vfs.AttributesMaskInodeNumber,
			gomock.Any(),
		).Do(func(requested re_vfs.AttributesMask, out *re_vfs.Attributes) {
			out.SetInodeNumber(100)
		})
		reporter.EXPECT().ReportLeaf(
			uint64(2),
			path.MustNewComponent("executable"),
			childLeaf1,
			(&re_vfs.Attributes{}).SetInodeNumber(100),
		).Return(true)
		childLeaf2 := mock.NewMockNativeLeaf(ctrl)
		directoryContext.EXPECT().LookupFile(
			digest.MustNewDigest("example", "0ac567103ab10e4b6bfca9b1d3387baad93dee899be5e5cbc3859e01363fbdaa", 200),
			false,
		).Return(childLeaf2)
		childLeaf2.EXPECT().VirtualGetAttributes(
			re_vfs.AttributesMaskInodeNumber,
			gomock.Any(),
		).Do(func(requested re_vfs.AttributesMask, out *re_vfs.Attributes) {
			out.SetInodeNumber(200)
		})
		reporter.EXPECT().ReportLeaf(
			uint64(3),
			path.MustNewComponent("file"),
			childLeaf2,
			(&re_vfs.Attributes{}).SetInodeNumber(200),
		).Return(true)
		contentAddressableStorageDirectoryExpectLookupSymlink(t, ctrl, handleAllocator, []byte{1})
		reporter.EXPECT().ReportLeaf(
			uint64(4),
			path.MustNewComponent("symlink"),
			gomock.Any(),
			gomock.Any(),
		).Return(true)

		require.Equal(
			t,
			re_vfs.StatusOK,
			d.VirtualReadDir(0, re_vfs.AttributesMaskInodeNumber, reporter))
	})

	t.Run("Partial", func(t *testing.T) {
		reporter := mock.NewMockDirectoryEntryReporter(ctrl)
		childLeaf := mock.NewMockNativeLeaf(ctrl)
		directoryContext.EXPECT().LookupFile(
			digest.MustNewDigest("example", "0ac567103ab10e4b6bfca9b1d3387baad93dee899be5e5cbc3859e01363fbdaa", 200),
			false,
		).Return(childLeaf)
		childLeaf.EXPECT().VirtualGetAttributes(
			re_vfs.AttributesMaskInodeNumber,
			gomock.Any(),
		).Do(func(requested re_vfs.AttributesMask, out *re_vfs.Attributes) {
			out.SetInodeNumber(200)
		})
		reporter.EXPECT().ReportLeaf(
			uint64(3),
			path.MustNewComponent("file"),
			childLeaf,
			(&re_vfs.Attributes{}).SetInodeNumber(200),
		).Return(true)
		contentAddressableStorageDirectoryExpectLookupSymlink(t, ctrl, handleAllocator, []byte{1})
		reporter.EXPECT().ReportLeaf(
			uint64(4),
			path.MustNewComponent("symlink"),
			gomock.Any(),
			gomock.Any(),
		).Return(true)

		require.Equal(
			t,
			re_vfs.StatusOK,
			d.VirtualReadDir(2, re_vfs.AttributesMaskInodeNumber, reporter))
	})

	t.Run("AtEOF", func(t *testing.T) {
		reporter := mock.NewMockDirectoryEntryReporter(ctrl)

		require.Equal(
			t,
			re_vfs.StatusOK,
			d.VirtualReadDir(4, re_vfs.AttributesMaskInodeNumber, reporter))
	})

	t.Run("BeyondEOF", func(t *testing.T) {
		reporter := mock.NewMockDirectoryEntryReporter(ctrl)

		require.Equal(
			t,
			re_vfs.StatusOK,
			d.VirtualReadDir(5, re_vfs.AttributesMaskInodeNumber, reporter))
	})
}

func TestContentAddressableStorageDirectoryHandleResolver(t *testing.T) {
	ctrl := gomock.NewController(t)

	directoryContext := mock.NewMockDirectoryContext(ctrl)
	rootHandleAllocation := mock.NewMockResolvableHandleAllocation(ctrl)
	handleAllocator := mock.NewMockResolvableHandleAllocator(ctrl)
	rootHandleAllocation.EXPECT().AsResolvableAllocator(gomock.Any()).Return(handleAllocator)
	contentAddressableStorageDirectoryExpectLookupSelf(t, ctrl, handleAllocator)
	d, handleResolver := cd_vfs.NewContentAddressableStorageDirectory(
		directoryContext,
		digest.MustNewInstanceName("example"),
		rootHandleAllocation,
		/* sizeBytes = */ 42)

	t.Run("EmptyIdentifier", func(t *testing.T) {
		// A variable length encoded integer should be provided
		// as an identifier.
		_, _, s := handleResolver(bytes.NewBuffer(nil))
		require.Equal(t, re_vfs.StatusErrBadHandle, s)
	})

	t.Run("Self", func(t *testing.T) {
		// Providing a zero identifier will end up resolving the
		// directory itself.
		contentAddressableStorageDirectoryExpectLookupSelf(t, ctrl, handleAllocator)

		directory, leaf, s := handleResolver(bytes.NewBuffer([]byte{0}))
		require.Equal(t, re_vfs.StatusOK, s)
		require.Equal(t, d, directory)
		require.Nil(t, leaf)
	})

	t.Run("SymlinkIOError", func(t *testing.T) {
		// Providing a non-zero identifier will end up resolving
		// a symbolic link inside the directory. Let this fail
		// with an I/O error.
		directoryContext.EXPECT().GetDirectoryContents().Return(nil, nil, re_vfs.StatusErrIO)

		_, _, s := handleResolver(bytes.NewBuffer([]byte{1}))
		require.Equal(t, re_vfs.StatusErrIO, s)
	})

	directoryContentsContext := mock.NewMockDirectoryContentsContext(ctrl)
	directoryContext.EXPECT().GetDirectoryContents().Return(&remoteexecution.Directory{
		Symlinks: []*remoteexecution.SymlinkNode{
			{
				Name:   "symlink1",
				Target: "target1",
			},
			{
				Name:   "symlink2",
				Target: "target2",
			},
		},
	}, directoryContentsContext, re_vfs.StatusOK).AnyTimes()

	t.Run("SymlinkOutOfBounds", func(t *testing.T) {
		// Provide a symlink index that is out of bounds.
		_, _, s := handleResolver(bytes.NewBuffer([]byte{3}))
		require.Equal(t, re_vfs.StatusErrBadHandle, s)
	})

	t.Run("SymlinkSuccess", func(t *testing.T) {
		// Successfully resolve a symbolic link.
		contentAddressableStorageDirectoryExpectLookupSymlink(t, ctrl, handleAllocator, []byte{2})

		directory, leaf, s := handleResolver(bytes.NewBuffer([]byte{2}))
		require.Equal(t, re_vfs.StatusOK, s)
		require.Nil(t, directory)
		require.NotNil(t, leaf)

		target, s := leaf.VirtualReadlink()
		require.Equal(t, re_vfs.StatusOK, s)
		require.Equal(t, []byte("target2"), target)
	})
}
