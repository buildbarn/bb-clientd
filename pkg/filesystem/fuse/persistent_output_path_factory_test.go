package fuse_test

import (
	"testing"
	"time"

	"github.com/buildbarn/bb-clientd/internal/mock"
	cd_fuse "github.com/buildbarn/bb-clientd/pkg/filesystem/fuse"
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
	outputPathFactory := cd_fuse.NewPersistentOutputPathFactory(baseOutputPathFactory, store, clock, globalErrorLogger)
	instanceName := digest.MustNewInstanceName("default")

	t.Run("StateNotFound", func(t *testing.T) {
		// In case we're not able to open an output path state
		// file, simply return an empty directory.
		baseOutputPath := mock.NewMockOutputPath(ctrl)
		outputBaseID := path.MustNewComponent("1603ee70687380f12cc8e7417a83f581")
		casFileFactory := mock.NewMockCASFileFactory(ctrl)
		fileErrorLogger := mock.NewMockErrorLogger(ctrl)
		baseOutputPathFactory.EXPECT().StartInitialBuild(outputBaseID, casFileFactory, instanceName, fileErrorLogger, uint64(100)).
			Return(baseOutputPath)
		store.EXPECT().Read(outputBaseID).Return(nil, nil, status.Error(codes.NotFound, "No data found"))
		globalErrorLogger.EXPECT().Log(status.Error(codes.NotFound, "Failed to open state file for output path \"1603ee70687380f12cc8e7417a83f581\": No data found"))
		clock.EXPECT().Now().Return(time.Unix(1000, 0))

		outputPath := outputPathFactory.StartInitialBuild(outputBaseID, casFileFactory, instanceName, fileErrorLogger, 100)
		require.NotNil(t, outputPath)
	})

	t.Run("MalformedStateFile", func(t *testing.T) {
		// Malformed root directories that do not have any
		// contents should not cause us to crash.
		baseOutputPath := mock.NewMockOutputPath(ctrl)
		outputBaseID := path.MustNewComponent("d0657f2e9484212cb081e8cd6d73e998")
		casFileFactory := mock.NewMockCASFileFactory(ctrl)
		fileErrorLogger := mock.NewMockErrorLogger(ctrl)
		baseOutputPathFactory.EXPECT().StartInitialBuild(outputBaseID, casFileFactory, instanceName, fileErrorLogger, uint64(100)).
			Return(baseOutputPath)
		reader := mock.NewMockOutputPathPersistencyReadCloser(ctrl)
		store.EXPECT().Read(outputBaseID).Return(reader, &outputpathpersistency.RootDirectory{}, nil)
		reader.EXPECT().Close()
		globalErrorLogger.EXPECT().Log(status.Error(codes.InvalidArgument, "State file for output path \"d0657f2e9484212cb081e8cd6d73e998\" does not contain a root directory"))
		clock.EXPECT().Now().Return(time.Unix(1000, 0))

		outputPath := outputPathFactory.StartInitialBuild(outputBaseID, casFileFactory, instanceName, fileErrorLogger, 100)
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
		baseOutputPathFactory.EXPECT().StartInitialBuild(outputBaseID, casFileFactory, instanceName, fileErrorLogger, uint64(100)).
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
		globalErrorLogger.EXPECT().Log(status.Error(codes.InvalidArgument, "Failed to restore state file for output path \"0226bea917a1c8c9c2ad4f7d4229de01\": Directory \"hello/world\" inside directory \".\" has an invalid name"))
		clock.EXPECT().Now().Return(time.Unix(1000, 0))

		outputPath := outputPathFactory.StartInitialBuild(outputBaseID, casFileFactory, instanceName, fileErrorLogger, 100)
		require.NotNil(t, outputPath)
	})

	t.Run("SubdirectoryLoadFailure", func(t *testing.T) {
		// Errors accessing nested directories should be logged.
		// The pathname should be included in the message.
		baseOutputPath := mock.NewMockOutputPath(ctrl)
		outputBaseID := path.MustNewComponent("054f6c2d674d23e67e011b1bb1ba7a5e")
		casFileFactory := mock.NewMockCASFileFactory(ctrl)
		fileErrorLogger := mock.NewMockErrorLogger(ctrl)
		baseOutputPathFactory.EXPECT().StartInitialBuild(outputBaseID, casFileFactory, instanceName, fileErrorLogger, uint64(100)).
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
		globalErrorLogger.EXPECT().Log(status.Error(codes.Internal, "Failed to restore state file for output path \"054f6c2d674d23e67e011b1bb1ba7a5e\": Failed to load directory \"hello\": Disk I/O failure"))
		clock.EXPECT().Now().Return(time.Unix(1000, 0))

		outputPath := outputPathFactory.StartInitialBuild(outputBaseID, casFileFactory, instanceName, fileErrorLogger, 100)
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
	outputPathFactory := cd_fuse.NewPersistentOutputPathFactory(baseOutputPathFactory, store, clock, globalErrorLogger)
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
