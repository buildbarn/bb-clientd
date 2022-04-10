package main_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	bb_clientd "github.com/buildbarn/bb-clientd/cmd/bb_clientd"
	"github.com/buildbarn/bb-clientd/internal/mock"
	"github.com/buildbarn/bb-clientd/pkg/cas"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func globalTreeContextExpectLookupTree(t *testing.T, ctrl *gomock.Controller, rootHandleAllocator *mock.MockStatelessHandleAllocator, instanceNameID, digestID []byte) *mock.MockResolvableHandleAllocator {
	instanceNameHandleAllocation := mock.NewMockStatelessHandleAllocation(ctrl)
	rootHandleAllocator.EXPECT().New(gomock.Any()).
		DoAndReturn(func(id io.WriterTo) virtual.StatelessHandleAllocation {
			actualIdentifier := bytes.NewBuffer(nil)
			n, err := id.WriteTo(actualIdentifier)
			require.NoError(t, err)
			require.Equal(t, int64(len(instanceNameID)), n)
			require.Equal(t, instanceNameID, actualIdentifier.Bytes())
			return instanceNameHandleAllocation
		})
	instanceNameHandleAllocator := mock.NewMockResolvableHandleAllocator(ctrl)
	instanceNameHandleAllocation.EXPECT().AsResolvableAllocator(gomock.Any()).Return(instanceNameHandleAllocator)

	treeHandleAllocation := mock.NewMockResolvableHandleAllocation(ctrl)
	instanceNameHandleAllocator.EXPECT().New(gomock.Any()).
		DoAndReturn(func(id io.WriterTo) virtual.ResolvableHandleAllocation {
			actualIdentifier := bytes.NewBuffer(nil)
			n, err := id.WriteTo(actualIdentifier)
			require.NoError(t, err)
			require.Equal(t, int64(len(digestID)), n)
			require.Equal(t, digestID, actualIdentifier.Bytes())
			return treeHandleAllocation
		})
	treeHandleAllocator := mock.NewMockResolvableHandleAllocator(ctrl)
	treeHandleAllocation.EXPECT().AsResolvableAllocator(gomock.Any()).Return(treeHandleAllocator)
	return treeHandleAllocator
}

func globalTreeContextExpectLookupDirectory(t *testing.T, ctrl *gomock.Controller, treeHandleAllocator *mock.MockResolvableHandleAllocator, directoryID, childID []byte) {
	dHandleAllocation := mock.NewMockResolvableHandleAllocation(ctrl)
	treeHandleAllocator.EXPECT().New(gomock.Any()).
		DoAndReturn(func(id io.WriterTo) virtual.ResolvableHandleAllocation {
			actualIdentifier := bytes.NewBuffer(nil)
			n, err := id.WriteTo(actualIdentifier)
			require.NoError(t, err)
			require.Equal(t, int64(len(directoryID)), n)
			require.Equal(t, directoryID, actualIdentifier.Bytes())
			return dHandleAllocation
		})
	dHandleAllocator := mock.NewMockResolvableHandleAllocator(ctrl)
	dHandleAllocation.EXPECT().AsResolvableAllocator(gomock.Any()).Return(dHandleAllocator)

	dDirectoryHandleAllocation := mock.NewMockResolvableHandleAllocation(ctrl)
	dHandleAllocator.EXPECT().New(gomock.Any()).
		DoAndReturn(func(id io.WriterTo) virtual.ResolvableHandleAllocation {
			actualIdentifier := bytes.NewBuffer(nil)
			n, err := id.WriteTo(actualIdentifier)
			require.NoError(t, err)
			require.Equal(t, int64(len(childID)), n)
			require.Equal(t, childID, actualIdentifier.Bytes())
			return dDirectoryHandleAllocation
		})
	dDirectoryHandleAllocation.EXPECT().AsStatelessDirectory(gomock.Any()).
		DoAndReturn(func(directory virtual.Directory) virtual.Directory { return directory })
}

func TestGlobalTreeContextLookupTree(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	casFileFactory := mock.NewMockCASFileFactory(ctrl)
	indexedTreeFetcher := mock.NewMockIndexedTreeFetcher(ctrl)
	rootHandleAllocation := mock.NewMockStatelessHandleAllocation(ctrl)
	rootHandleAllocator := mock.NewMockStatelessHandleAllocator(ctrl)
	rootHandleAllocation.EXPECT().AsStatelessAllocator().Return(rootHandleAllocator)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	globalTreeContext := bb_clientd.NewGlobalTreeContext(
		ctx,
		casFileFactory,
		indexedTreeFetcher,
		rootHandleAllocation,
		errorLogger)

	treeHandleAllocator := globalTreeContextExpectLookupTree(
		t,
		ctrl,
		rootHandleAllocator,
		// Instance name.
		[]byte("\x05hello"),
		[]byte{
			// Hash size.
			0x20,
			// Hash.
			0xe0, 0xf2, 0x8d, 0x31, 0x1a, 0x9b, 0x2d, 0xef,
			0xf1, 0x03, 0xe3, 0x2f, 0x61, 0x05, 0xb2, 0xb2,
			0x9d, 0x63, 0x6c, 0x28, 0x77, 0x97, 0xca, 0x72,
			0x07, 0x7a, 0x64, 0x8c, 0xd7, 0x36, 0xcd, 0x36,
			// Size.
			0xf6, 0x01,
		})
	globalTreeContextExpectLookupDirectory(
		t,
		ctrl,
		treeHandleAllocator,
		// Root directory.
		[]byte{0},
		// Directory itself.
		[]byte{0})

	treeDigest := digest.MustNewDigest("hello", "e0f28d311a9b2deff103e32f6105b2b29d636c287797ca72077a648cd736cd36", 123)
	attributesMask := virtual.AttributesMaskFileType |
		virtual.AttributesMaskInodeNumber |
		virtual.AttributesMaskLinkCount |
		virtual.AttributesMaskPermissions |
		virtual.AttributesMaskSizeBytes

	// Start off testing on a root directory of a tree.
	dRoot := globalTreeContext.LookupTree(treeDigest)
	var out virtual.Attributes
	dRoot.VirtualGetAttributes(attributesMask, &out)
	require.Equal(t, filesystem.FileTypeDirectory, out.GetFileType())
	require.Equal(t, virtual.ImplicitDirectoryLinkCount, out.GetLinkCount())
	permissions, ok := out.GetPermissions()
	require.True(t, ok)
	require.Equal(t, virtual.PermissionsRead|virtual.PermissionsExecute, permissions)

	t.Run("RootIOError", func(t *testing.T) {
		// I/O errors when requesting the directory contents
		// should be forwarded to the error logger. The digest
		// of the tree should be prepended, included the fact
		// that this applied to loading the root directory.
		indexedTreeFetcher.EXPECT().GetIndexedTree(ctx, treeDigest).Return(nil, status.Error(codes.Internal, "Server on fire"))
		errorLogger.EXPECT().Log(status.Error(codes.Internal, "Tree \"e0f28d311a9b2deff103e32f6105b2b29d636c287797ca72077a648cd736cd36-123-hello\" root directory: Server on fire"))
		reporter := mock.NewMockDirectoryEntryReporter(ctrl)

		require.Equal(t, virtual.StatusErrIO, dRoot.VirtualReadDir(0, 0, reporter))
	})

	t.Run("RootMissing", func(t *testing.T) {
		// The underlying remoteexecution.Tree object may have
		// been malformed, due to it not containing a root
		// directory. Such errors should be reported.
		indexedTreeFetcher.EXPECT().GetIndexedTree(ctx, treeDigest).Return(&cas.IndexedTree{
			Tree: &remoteexecution.Tree{},
		}, nil)
		errorLogger.EXPECT().Log(status.Error(codes.InvalidArgument, "Tree \"e0f28d311a9b2deff103e32f6105b2b29d636c287797ca72077a648cd736cd36-123-hello\" root directory: Tree does not contain a root directory"))
		reporter := mock.NewMockDirectoryEntryReporter(ctrl)

		require.Equal(t, virtual.StatusErrIO, dRoot.VirtualReadDir(0, 0, reporter))
	})

	t.Run("RootSuccess", func(t *testing.T) {
		executable := mock.NewMockNativeLeaf(ctrl)
		casFileFactory.EXPECT().
			LookupFile(digest.MustNewDigest("hello", "32d757ab2b5c09e11daf0b0c04a3ba9da78e96fd24f9f838be0333f093354c82", 42), true).
			Return(executable)
		executable.EXPECT().VirtualGetAttributes(virtual.AttributesMask(0), gomock.Any())
		file := mock.NewMockNativeLeaf(ctrl)
		casFileFactory.EXPECT().
			LookupFile(digest.MustNewDigest("hello", "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c", 11), false).
			Return(file)
		file.EXPECT().VirtualGetAttributes(virtual.AttributesMask(0), gomock.Any())

		indexedTreeFetcher.EXPECT().GetIndexedTree(ctx, treeDigest).Return(&cas.IndexedTree{
			Tree: &remoteexecution.Tree{
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
			},
		}, nil)
		reporter := mock.NewMockDirectoryEntryReporter(ctrl)
		reporter.EXPECT().ReportLeaf(uint64(1), path.MustNewComponent("executable"), executable, gomock.Any()).Return(true)
		reporter.EXPECT().ReportLeaf(uint64(2), path.MustNewComponent("file"), file, gomock.Any()).Return(true)

		require.Equal(t, virtual.StatusOK, dRoot.VirtualReadDir(0, 0, reporter))
	})

	// Continue testing on a child directory of a tree.
	indexedTreeFetcher.EXPECT().GetIndexedTree(ctx, treeDigest).Return(&cas.IndexedTree{
		Tree: &remoteexecution.Tree{
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
			Children: []*remoteexecution.Directory{
				{
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
		},
		Index: map[string]int{
			"cde6e00a0f207b218b57fe1a343c9bad353ad93a1cdacce29846acbf3c227842-112": 0,
		},
	}, nil)
	globalTreeContextExpectLookupDirectory(
		t,
		ctrl,
		treeHandleAllocator,
		// Root directory.
		[]byte{1},
		// Directory itself.
		[]byte{0})

	var outChild virtual.Attributes
	dChild, _, s := dRoot.VirtualLookup(path.MustNewComponent("directory"), attributesMask, &outChild)
	require.Equal(t, virtual.StatusOK, s)
	require.Equal(t, filesystem.FileTypeDirectory, out.GetFileType())
	require.Equal(t, virtual.ImplicitDirectoryLinkCount, out.GetLinkCount())
	permissions, ok = out.GetPermissions()
	require.True(t, ok)
	require.Equal(t, virtual.PermissionsRead|virtual.PermissionsExecute, permissions)

	t.Run("ChildIOError", func(t *testing.T) {
		// Just like for the root directory, I/O errors should
		// be captured. The error string should make it explicit
		// which child was being accessed.
		indexedTreeFetcher.EXPECT().GetIndexedTree(ctx, treeDigest).Return(nil, status.Error(codes.Internal, "Server on fire"))
		errorLogger.EXPECT().Log(status.Error(codes.Internal, "Tree \"e0f28d311a9b2deff103e32f6105b2b29d636c287797ca72077a648cd736cd36-123-hello\" child directory index 0: Server on fire"))
		reporter := mock.NewMockDirectoryEntryReporter(ctrl)

		require.Equal(t, virtual.StatusErrIO, dChild.VirtualReadDir(0, 0, reporter))
	})

	t.Run("ChildMissing", func(t *testing.T) {
		// The underlying remoteexecution.Tree object may have
		// been malformed, due to it containing references to
		// directories not present in the tree. Such errors
		// should be reported.
		indexedTreeFetcher.EXPECT().GetIndexedTree(ctx, treeDigest).Return(&cas.IndexedTree{
			Tree: &remoteexecution.Tree{},
		}, nil)
		errorLogger.EXPECT().Log(status.Error(codes.InvalidArgument, "Tree \"e0f28d311a9b2deff103e32f6105b2b29d636c287797ca72077a648cd736cd36-123-hello\" child directory index 0: Directory index exceeds children list length of 0"))
		reporter := mock.NewMockDirectoryEntryReporter(ctrl)

		require.Equal(t, virtual.StatusErrIO, dChild.VirtualReadDir(0, 0, reporter))
	})

	t.Run("ChildSuccess", func(t *testing.T) {
		file := mock.NewMockNativeLeaf(ctrl)
		casFileFactory.EXPECT().
			LookupFile(digest.MustNewDigest("hello", "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c", 11), false).
			Return(file)
		file.EXPECT().VirtualGetAttributes(virtual.AttributesMask(0), gomock.Any())

		indexedTreeFetcher.EXPECT().GetIndexedTree(ctx, treeDigest).Return(&cas.IndexedTree{
			Tree: &remoteexecution.Tree{
				Children: []*remoteexecution.Directory{
					{
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
			},
			Index: map[string]int{
				"cde6e00a0f207b218b57fe1a343c9bad353ad93a1cdacce29846acbf3c227842-112": 0,
			},
		}, nil)
		reporter := mock.NewMockDirectoryEntryReporter(ctrl)
		reporter.EXPECT().ReportLeaf(uint64(1), path.MustNewComponent("file"), gomock.Any(), gomock.Any()).Return(true)

		require.Equal(t, virtual.StatusOK, dChild.VirtualReadDir(0, 0, reporter))
	})
}
