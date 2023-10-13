package virtual_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-clientd/internal/mock"
	cd_vfs "github.com/buildbarn/bb-clientd/pkg/filesystem/virtual"
	re_vfs "github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func decomposedCASDirectoryFactoryExpectLookup(t *testing.T, ctrl *gomock.Controller, rootHandleAllocator *mock.MockStatelessHandleAllocator, instanceNameID, digestID, childID []byte) {
	instanceNameHandleAllocation := mock.NewMockStatelessHandleAllocation(ctrl)
	rootHandleAllocator.EXPECT().New(gomock.Any()).
		DoAndReturn(func(id io.WriterTo) re_vfs.StatelessHandleAllocation {
			actualIdentifier := bytes.NewBuffer(nil)
			n, err := id.WriteTo(actualIdentifier)
			require.NoError(t, err)
			require.Equal(t, int64(len(instanceNameID)), n)
			require.Equal(t, instanceNameID, actualIdentifier.Bytes())
			return instanceNameHandleAllocation
		})
	instanceNameHandleAllocator := mock.NewMockResolvableHandleAllocator(ctrl)
	instanceNameHandleAllocation.EXPECT().AsResolvableAllocator(gomock.Any()).Return(instanceNameHandleAllocator)

	dHandleAllocation := mock.NewMockResolvableHandleAllocation(ctrl)
	instanceNameHandleAllocator.EXPECT().New(gomock.Any()).
		DoAndReturn(func(id io.WriterTo) re_vfs.ResolvableHandleAllocation {
			actualIdentifier := bytes.NewBuffer(nil)
			n, err := id.WriteTo(actualIdentifier)
			require.NoError(t, err)
			require.Equal(t, int64(len(digestID)), n)
			require.Equal(t, digestID, actualIdentifier.Bytes())
			return dHandleAllocation
		})
	dHandleAllocator := mock.NewMockResolvableHandleAllocator(ctrl)
	dHandleAllocation.EXPECT().AsResolvableAllocator(gomock.Any()).Return(dHandleAllocator)

	dDirectoryHandleAllocation := mock.NewMockResolvableHandleAllocation(ctrl)
	dHandleAllocator.EXPECT().New(gomock.Any()).
		DoAndReturn(func(id io.WriterTo) re_vfs.ResolvableHandleAllocation {
			actualIdentifier := bytes.NewBuffer(nil)
			n, err := id.WriteTo(actualIdentifier)
			require.NoError(t, err)
			require.Equal(t, int64(len(childID)), n)
			require.Equal(t, childID, actualIdentifier.Bytes())
			return dDirectoryHandleAllocation
		})
	dDirectoryHandleAllocation.EXPECT().AsStatelessDirectory(gomock.Any()).
		DoAndReturn(func(directory re_vfs.Directory) re_vfs.Directory { return directory })
}

func TestDecomposedCASDirectoryFactoryLookupDirectory(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	casFileFactory := mock.NewMockCASFileFactory(ctrl)
	directoryFetcher := mock.NewMockDirectoryFetcher(ctrl)
	rootHandleAllocation := mock.NewMockStatelessHandleAllocation(ctrl)
	rootHandleAllocator := mock.NewMockStatelessHandleAllocator(ctrl)
	rootHandleAllocation.EXPECT().AsStatelessAllocator().Return(rootHandleAllocator)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	casDirectoryFactory := cd_vfs.NewDecomposedCASDirectoryFactory(
		ctx,
		casFileFactory,
		directoryFetcher,
		rootHandleAllocation,
		errorLogger)

	directoryDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_SHA256, "e0f28d311a9b2deff103e32f6105b2b29d636c287797ca72077a648cd736cd36", 123)
	attributesMask := re_vfs.AttributesMaskChangeID |
		re_vfs.AttributesMaskFileType |
		re_vfs.AttributesMaskInodeNumber |
		re_vfs.AttributesMaskLinkCount |
		re_vfs.AttributesMaskPermissions |
		re_vfs.AttributesMaskSizeBytes

	decomposedCASDirectoryFactoryExpectLookup(
		t,
		ctrl,
		rootHandleAllocator,
		// Instance name.
		[]byte("\x05hello"),
		[]byte{
			// Digest function: remoteexecution.DigestFunction_SHA256.
			0x01,
			// Hash.
			0xe0, 0xf2, 0x8d, 0x31, 0x1a, 0x9b, 0x2d, 0xef,
			0xf1, 0x03, 0xe3, 0x2f, 0x61, 0x05, 0xb2, 0xb2,
			0x9d, 0x63, 0x6c, 0x28, 0x77, 0x97, 0xca, 0x72,
			0x07, 0x7a, 0x64, 0x8c, 0xd7, 0x36, 0xcd, 0x36,
			// Size.
			0xf6, 0x01,
		},
		// Directory itself.
		[]byte{0})

	d := casDirectoryFactory.LookupDirectory(directoryDigest)
	var out re_vfs.Attributes
	d.VirtualGetAttributes(ctx, attributesMask, &out)
	require.Equal(
		t,
		(&re_vfs.Attributes{}).
			SetChangeID(0).
			SetFileType(filesystem.FileTypeDirectory).
			SetInodeNumber(0).
			SetLinkCount(re_vfs.ImplicitDirectoryLinkCount).
			SetPermissions(re_vfs.PermissionsRead|re_vfs.PermissionsExecute).
			SetSizeBytes(123),
		out.SetInodeNumber(0))

	t.Run("IOError", func(t *testing.T) {
		// I/O errors when requesting the directory contents
		// should be forwarded to the error logger. The digest
		// of the directory should be prepended to the error
		// message.
		directoryFetcher.EXPECT().GetDirectory(ctx, directoryDigest).Return(nil, status.Error(codes.Internal, "Server on fire"))
		errorLogger.EXPECT().Log(testutil.EqStatus(t, status.Error(codes.Internal, "Directory \"1-e0f28d311a9b2deff103e32f6105b2b29d636c287797ca72077a648cd736cd36-123-hello\": Server on fire")))
		reporter := mock.NewMockDirectoryEntryReporter(ctrl)

		require.Equal(t, re_vfs.StatusErrIO, d.VirtualReadDir(ctx, 0, 0, reporter))
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
		errorLogger.EXPECT().Log(testutil.EqStatus(t, status.Error(codes.InvalidArgument, "Directory \"1-e0f28d311a9b2deff103e32f6105b2b29d636c287797ca72077a648cd736cd36-123-hello\": Failed to parse digest for file \"broken\": Hash has length 24, while 64 characters were expected")))
		reporter := mock.NewMockDirectoryEntryReporter(ctrl)

		require.Equal(t, re_vfs.StatusErrIO, d.VirtualReadDir(ctx, 0, 0, reporter))
	})

	t.Run("Success", func(t *testing.T) {
		// Successfully obtain a directory listing. All entries
		// should be converted to a FUSE directory entry.
		decomposedCASDirectoryFactoryExpectLookup(
			t,
			ctrl,
			rootHandleAllocator,
			[]byte("\x05hello"),
			[]byte{
				// Digest function: remoteexecution.DigestFunction_SHA256.
				0x01,
				// Hash.
				0xcd, 0xe6, 0xe0, 0x0a, 0x0f, 0x20, 0x7b, 0x21,
				0x8b, 0x57, 0xfe, 0x1a, 0x34, 0x3c, 0x9b, 0xad,
				0x35, 0x3a, 0xd9, 0x3a, 0x1c, 0xda, 0xcc, 0xe2,
				0x98, 0x46, 0xac, 0xbf, 0x3c, 0x22, 0x78, 0x42,
				// Size.
				0xe0, 0x01,
			},
			[]byte{0})
		executable := mock.NewMockNativeLeaf(ctrl)
		casFileFactory.EXPECT().LookupFile(
			digest.MustNewDigest("hello", remoteexecution.DigestFunction_SHA256, "32d757ab2b5c09e11daf0b0c04a3ba9da78e96fd24f9f838be0333f093354c82", 42),
			/* isExecutable = */ true,
			/* readMonitor = */ nil,
		).Return(executable)
		executable.EXPECT().VirtualGetAttributes(ctx, re_vfs.AttributesMask(0), gomock.Any())
		file := mock.NewMockNativeLeaf(ctrl)
		casFileFactory.EXPECT().LookupFile(
			digest.MustNewDigest("hello", remoteexecution.DigestFunction_SHA256, "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c", 11),
			/* isExecutable = */ false,
			/* readMonitor = */ nil,
		).Return(file)
		file.EXPECT().VirtualGetAttributes(ctx, re_vfs.AttributesMask(0), gomock.Any())

		directoryFetcher.EXPECT().GetDirectory(ctx, directoryDigest).Return(&remoteexecution.Directory{
			Directories: []*remoteexecution.DirectoryNode{
				{
					Name: "directory",
					Digest: &remoteexecution.Digest{
						Hash:      "cde6e00a0f207b218b57fe1a343c9bad353ad93a1cdacce29846acbf3c227842",
						SizeBytes: 112,
					},
				},
			},
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
		}, nil)
		reporter := mock.NewMockDirectoryEntryReporter(ctrl)
		reporter.EXPECT().ReportEntry(uint64(1), path.MustNewComponent("directory"), gomock.Any(), gomock.Any()).Return(true)
		reporter.EXPECT().ReportEntry(uint64(2), path.MustNewComponent("executable"), re_vfs.DirectoryChild{}.FromLeaf(executable), gomock.Any()).Return(true)
		reporter.EXPECT().ReportEntry(uint64(3), path.MustNewComponent("file"), re_vfs.DirectoryChild{}.FromLeaf(file), gomock.Any()).Return(true)

		require.Equal(t, re_vfs.StatusOK, d.VirtualReadDir(ctx, 0, 0, reporter))
	})
}
