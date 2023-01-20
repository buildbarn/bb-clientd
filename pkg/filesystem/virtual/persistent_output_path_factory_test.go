package virtual_test

import (
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-clientd/internal/mock"
	cd_vfs "github.com/buildbarn/bb-clientd/pkg/filesystem/virtual"
	re_vfs "github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/outputpathpersistency"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPersistentOutputPathFactoryStartInitialBuild(t *testing.T) {
	ctrl := gomock.NewController(t)

	baseOutputPathFactory := mock.NewMockOutputPathFactory(ctrl)
	store := mock.NewMockOutputPathPersistencyStore(ctrl)
	clock := mock.NewMockClock(ctrl)
	globalErrorLogger := mock.NewMockErrorLogger(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	outputPathFactory := cd_vfs.NewPersistentOutputPathFactory(baseOutputPathFactory, store, clock, globalErrorLogger, symlinkFactory)
	digestFunction := digest.MustNewFunction("default", remoteexecution.DigestFunction_SHA256)

	t.Run("StateNotFound", func(t *testing.T) {
		// In case we're not able to open an output path state
		// file, simply return an empty directory.
		baseOutputPath := mock.NewMockOutputPath(ctrl)
		outputBaseID := path.MustNewComponent("1603ee70687380f12cc8e7417a83f581")
		casFileFactory := mock.NewMockCASFileFactory(ctrl)
		fileErrorLogger := mock.NewMockErrorLogger(ctrl)
		baseOutputPathFactory.EXPECT().StartInitialBuild(outputBaseID, casFileFactory, digestFunction, fileErrorLogger).
			Return(baseOutputPath)
		store.EXPECT().Read(outputBaseID).Return(nil, nil, status.Error(codes.NotFound, "No data found"))
		globalErrorLogger.EXPECT().Log(testutil.EqStatus(t, status.Error(codes.NotFound, "Failed to open state file for output path \"1603ee70687380f12cc8e7417a83f581\": No data found")))
		clock.EXPECT().Now().Return(time.Unix(1000, 0))

		outputPath := outputPathFactory.StartInitialBuild(outputBaseID, casFileFactory, digestFunction, fileErrorLogger)
		require.NotNil(t, outputPath)
	})

	t.Run("MalformedStateFile", func(t *testing.T) {
		// Malformed root directories that do not have any
		// contents should not cause us to crash.
		baseOutputPath := mock.NewMockOutputPath(ctrl)
		outputBaseID := path.MustNewComponent("d0657f2e9484212cb081e8cd6d73e998")
		casFileFactory := mock.NewMockCASFileFactory(ctrl)
		fileErrorLogger := mock.NewMockErrorLogger(ctrl)
		baseOutputPathFactory.EXPECT().StartInitialBuild(outputBaseID, casFileFactory, digestFunction, fileErrorLogger).
			Return(baseOutputPath)
		reader := mock.NewMockOutputPathPersistencyReadCloser(ctrl)
		store.EXPECT().Read(outputBaseID).Return(reader, &outputpathpersistency.RootDirectory{}, nil)
		reader.EXPECT().Close()
		globalErrorLogger.EXPECT().Log(testutil.EqStatus(t, status.Error(codes.InvalidArgument, "State file for output path \"d0657f2e9484212cb081e8cd6d73e998\" does not contain a root directory")))
		clock.EXPECT().Now().Return(time.Unix(1000, 0))

		outputPath := outputPathFactory.StartInitialBuild(outputBaseID, casFileFactory, digestFunction, fileErrorLogger)
		require.NotNil(t, outputPath)
	})

	t.Run("MalformedDirectoryName", func(t *testing.T) {
		// Directories with bad names should not be restored, as
		// those would cause us to create trees that cannot be
		// exposed through FUSE.
		baseOutputPath := mock.NewMockOutputPath(ctrl)
		outputBaseID := path.MustNewComponent("0226bea917a1c8c9c2ad4f7d4229de01")
		casFileFactory := mock.NewMockCASFileFactory(ctrl)
		fileErrorLogger := mock.NewMockErrorLogger(ctrl)
		baseOutputPathFactory.EXPECT().StartInitialBuild(outputBaseID, casFileFactory, digestFunction, fileErrorLogger).
			Return(baseOutputPath)
		reader := mock.NewMockOutputPathPersistencyReadCloser(ctrl)
		store.EXPECT().Read(outputBaseID).Return(reader, &outputpathpersistency.RootDirectory{
			Contents: &outputpathpersistency.Directory{
				Directories: []*outputpathpersistency.DirectoryNode{
					{
						Name: "hello/world",
						FileRegion: &outputpathpersistency.FileRegion{
							OffsetBytes: 123,
							SizeBytes:   456,
						},
					},
				},
			},
		}, nil)
		reader.EXPECT().Close()
		globalErrorLogger.EXPECT().Log(testutil.EqStatus(t, status.Error(codes.InvalidArgument, "Failed to restore state file for output path \"0226bea917a1c8c9c2ad4f7d4229de01\": Directory \"hello/world\" inside directory \".\" has an invalid name")))
		clock.EXPECT().Now().Return(time.Unix(1000, 0))

		outputPath := outputPathFactory.StartInitialBuild(outputBaseID, casFileFactory, digestFunction, fileErrorLogger)
		require.NotNil(t, outputPath)
	})

	t.Run("SubdirectoryLoadFailure", func(t *testing.T) {
		// Errors accessing nested directories should be logged.
		// The pathname should be included in the message.
		baseOutputPath := mock.NewMockOutputPath(ctrl)
		outputBaseID := path.MustNewComponent("054f6c2d674d23e67e011b1bb1ba7a5e")
		casFileFactory := mock.NewMockCASFileFactory(ctrl)
		fileErrorLogger := mock.NewMockErrorLogger(ctrl)
		baseOutputPathFactory.EXPECT().StartInitialBuild(outputBaseID, casFileFactory, digestFunction, fileErrorLogger).
			Return(baseOutputPath)
		reader := mock.NewMockOutputPathPersistencyReadCloser(ctrl)
		store.EXPECT().Read(outputBaseID).Return(reader, &outputpathpersistency.RootDirectory{
			Contents: &outputpathpersistency.Directory{
				Directories: []*outputpathpersistency.DirectoryNode{
					{
						Name: "hello",
						FileRegion: &outputpathpersistency.FileRegion{
							OffsetBytes: 123,
							SizeBytes:   456,
						},
					},
				},
			},
		}, nil)
		reader.EXPECT().ReadDirectory(testutil.EqProto(t, &outputpathpersistency.FileRegion{
			OffsetBytes: 123,
			SizeBytes:   456,
		})).Return(nil, nil, status.Error(codes.Internal, "Disk I/O failure"))

		reader.EXPECT().Close()
		globalErrorLogger.EXPECT().Log(testutil.EqStatus(t, status.Error(codes.Internal, "Failed to restore state file for output path \"054f6c2d674d23e67e011b1bb1ba7a5e\": Failed to load directory \"hello\": Disk I/O failure")))
		clock.EXPECT().Now().Return(time.Unix(1000, 0))

		outputPath := outputPathFactory.StartInitialBuild(outputBaseID, casFileFactory, digestFunction, fileErrorLogger)
		require.NotNil(t, outputPath)
	})

	t.Run("SuccessiveFileCreationFailure", func(t *testing.T) {
		// If a successive file cannot be created, previously
		// created files that have not been added to the
		// directory yet should be unlinked. This ensures they
		// don't leak.
		baseOutputPath := mock.NewMockOutputPath(ctrl)
		outputBaseID := path.MustNewComponent("0226bea917a1c8c9c2ad4f7d4229de01")
		casFileFactory := mock.NewMockCASFileFactory(ctrl)
		fileErrorLogger := mock.NewMockErrorLogger(ctrl)
		baseOutputPathFactory.EXPECT().StartInitialBuild(outputBaseID, casFileFactory, digestFunction, fileErrorLogger).
			Return(baseOutputPath)
		reader := mock.NewMockOutputPathPersistencyReadCloser(ctrl)
		store.EXPECT().Read(outputBaseID).Return(reader, &outputpathpersistency.RootDirectory{
			Contents: &outputpathpersistency.Directory{
				Files: []*remoteexecution.FileNode{
					{
						Name: "file1",
						Digest: &remoteexecution.Digest{
							Hash:      "f132632084ca4e2124fbc88223901e3976e126ebb8f8cc5a09116a0191369d9b",
							SizeBytes: 34,
						},
					},
					{
						Name: "file2",
						Digest: &remoteexecution.Digest{
							Hash:      "This is a bad digest",
							SizeBytes: 42,
						},
					},
				},
			},
		}, nil)
		file1 := mock.NewMockNativeLeaf(ctrl)
		casFileFactory.EXPECT().LookupFile(digest.MustNewDigest("default", remoteexecution.DigestFunction_SHA256, "f132632084ca4e2124fbc88223901e3976e126ebb8f8cc5a09116a0191369d9b", 34), false).Return(file1)
		file1.EXPECT().Unlink()
		reader.EXPECT().Close()
		globalErrorLogger.EXPECT().Log(testutil.EqStatus(t, status.Error(codes.InvalidArgument, "Failed to restore state file for output path \"0226bea917a1c8c9c2ad4f7d4229de01\": Failed to obtain digest for file \"file2\": Hash has length 20, while 64 characters were expected")))
		clock.EXPECT().Now().Return(time.Unix(1000, 0))

		outputPath := outputPathFactory.StartInitialBuild(outputBaseID, casFileFactory, digestFunction, fileErrorLogger)
		require.NotNil(t, outputPath)
	})

	t.Run("Success", func(t *testing.T) {
		baseOutputPath := mock.NewMockOutputPath(ctrl)
		outputBaseID := path.MustNewComponent("0226bea917a1c8c9c2ad4f7d4229de01")
		casFileFactory := mock.NewMockCASFileFactory(ctrl)
		fileErrorLogger := mock.NewMockErrorLogger(ctrl)
		baseOutputPathFactory.EXPECT().StartInitialBuild(outputBaseID, casFileFactory, digestFunction, fileErrorLogger).
			Return(baseOutputPath)
		reader := mock.NewMockOutputPathPersistencyReadCloser(ctrl)
		store.EXPECT().Read(outputBaseID).Return(reader, &outputpathpersistency.RootDirectory{
			Contents: &outputpathpersistency.Directory{
				Files: []*remoteexecution.FileNode{
					{
						Name: "file1",
						Digest: &remoteexecution.Digest{
							Hash:      "f132632084ca4e2124fbc88223901e3976e126ebb8f8cc5a09116a0191369d9b",
							SizeBytes: 34,
						},
					},
					{
						Name: "file2",
						Digest: &remoteexecution.Digest{
							Hash:      "ce22e2423b7d501d45f6b8aeab15cb40f28c73fb2edfe03e0f3cd450583fcef8",
							SizeBytes: 42,
						},
						IsExecutable: true,
					},
				},
				Symlinks: []*remoteexecution.SymlinkNode{
					{
						Name:   "symlink1",
						Target: "target1",
					},
				},
			},
		}, nil)
		file1 := mock.NewMockNativeLeaf(ctrl)
		casFileFactory.EXPECT().LookupFile(digest.MustNewDigest("default", remoteexecution.DigestFunction_SHA256, "f132632084ca4e2124fbc88223901e3976e126ebb8f8cc5a09116a0191369d9b", 34), false).Return(file1)
		file2 := mock.NewMockNativeLeaf(ctrl)
		casFileFactory.EXPECT().LookupFile(digest.MustNewDigest("default", remoteexecution.DigestFunction_SHA256, "ce22e2423b7d501d45f6b8aeab15cb40f28c73fb2edfe03e0f3cd450583fcef8", 42), true).Return(file2)
		symlink1 := mock.NewMockNativeLeaf(ctrl)
		symlinkFactory.EXPECT().LookupSymlink([]byte("target1")).Return(symlink1)
		baseOutputPath.EXPECT().CreateChildren(map[path.Component]re_vfs.InitialNode{
			path.MustNewComponent("file1"):    re_vfs.InitialNode{}.FromLeaf(file1),
			path.MustNewComponent("file2"):    re_vfs.InitialNode{}.FromLeaf(file2),
			path.MustNewComponent("symlink1"): re_vfs.InitialNode{}.FromLeaf(symlink1),
		}, true)
		reader.EXPECT().Close()
		clock.EXPECT().Now().Return(time.Unix(1000, 0))

		outputPath := outputPathFactory.StartInitialBuild(outputBaseID, casFileFactory, digestFunction, fileErrorLogger)
		require.NotNil(t, outputPath)
	})

	// TODO: Are there more cases we want to test?
}

func TestPersistentOutputPathFactoryClean(t *testing.T) {
	ctrl := gomock.NewController(t)

	baseOutputPathFactory := mock.NewMockOutputPathFactory(ctrl)
	store := mock.NewMockOutputPathPersistencyStore(ctrl)
	clock := mock.NewMockClock(ctrl)
	globalErrorLogger := mock.NewMockErrorLogger(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	outputPathFactory := cd_vfs.NewPersistentOutputPathFactory(baseOutputPathFactory, store, clock, globalErrorLogger, symlinkFactory)
	outputBaseID := path.MustNewComponent("1603ee70687380f12cc8e7417a83f581")

	t.Run("BaseFailure", func(t *testing.T) {
		baseOutputPathFactory.EXPECT().Clean(outputBaseID).
			Return(status.Error(codes.Internal, "Disk failure while cleaning output path"))

		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Internal, "Disk failure while cleaning output path"),
			outputPathFactory.Clean(outputBaseID))
	})

	t.Run("StoreFailure", func(t *testing.T) {
		baseOutputPathFactory.EXPECT().Clean(outputBaseID)
		store.EXPECT().Clean(outputBaseID).
			Return(status.Error(codes.Internal, "Disk failure"))

		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Internal, "Failed to remove persistent state for output path: Disk failure"),
			outputPathFactory.Clean(outputBaseID))
	})

	t.Run("Success", func(t *testing.T) {
		baseOutputPathFactory.EXPECT().Clean(outputBaseID)
		store.EXPECT().Clean(outputBaseID)

		require.NoError(t, outputPathFactory.Clean(outputBaseID))
	})
}
