package outputpathpersistency_test

import (
	"syscall"
	"testing"

	"github.com/buildbarn/bb-clientd/internal/mock"
	"github.com/buildbarn/bb-clientd/pkg/outputpathpersistency"
	outputpathpersistency_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/outputpathpersistency"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestDirectoryBackedStoreRead(t *testing.T) {
	ctrl := gomock.NewController(t)

	directory := mock.NewMockDirectory(ctrl)
	store := outputpathpersistency.NewDirectoryBackedStore(directory, 1024*1024)
	outputBaseID := path.MustNewComponent("45ae96d6effc5963e9378529a68c4032")

	t.Run("NotFound", func(t *testing.T) {
		directory.EXPECT().OpenRead(outputBaseID).Return(nil, status.Error(codes.NotFound, "File not found"))

		_, _, err := store.Read(outputBaseID)
		testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Failed to open state file: File not found"), err)
	})

	t.Run("ReadError", func(t *testing.T) {
		fileReader := mock.NewMockFileReader(ctrl)
		directory.EXPECT().OpenRead(outputBaseID).Return(fileReader, nil)
		fileReader.EXPECT().ReadAt(gomock.Any(), gomock.Any()).Return(0, status.Error(codes.Internal, "Disk failure"))
		fileReader.EXPECT().Close()

		_, _, err := store.Read(outputBaseID)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to read header: Disk failure"), err)
	})

	t.Run("Success", func(t *testing.T) {
		fileReader := mock.NewMockFileReader(ctrl)
		directory.EXPECT().OpenRead(outputBaseID).Return(fileReader, nil)
		fileReader.EXPECT().ReadAt(gomock.Len(16), int64(0)).DoAndReturn(func(p []byte, off int64) (int, error) {
			return copy(p, []byte{
				// Magic.
				0xfa, 0x12, 0xa4, 0xa5,
				// Root directory offset: 40.
				0x28, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// Root directory size: 50.
				0x32, 0x00, 0x00, 0x00,
			}), nil
		})
		fileReader.EXPECT().ReadAt(gomock.Len(50), int64(40)).DoAndReturn(func(p []byte, off int64) (int, error) {
			return copy(p, []byte{
				0x0a, 0x06, 0x08, 0xeb, 0xc4, 0x89, 0x84, 0x06,
				0x12, 0x28, 0x12, 0x15, 0x0a, 0x0d, 0x73, 0x75,
				0x62, 0x64, 0x69, 0x72, 0x65, 0x63, 0x74, 0x6f,
				0x72, 0x79, 0x31, 0x12, 0x04, 0x08, 0x10, 0x10,
				0x18, 0x12, 0x0f, 0x0a, 0x0d, 0x73, 0x75, 0x62,
				0x64, 0x69, 0x72, 0x65, 0x63, 0x74, 0x6f, 0x72,
				0x79, 0x32,
			}), nil
		})

		reader, rootDirectory, err := store.Read(outputBaseID)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &outputpathpersistency_pb.RootDirectory{
			InitialCreationTime: &timestamppb.Timestamp{Seconds: 1619157611},
			Contents: &outputpathpersistency_pb.Directory{
				Directories: []*outputpathpersistency_pb.DirectoryNode{
					{
						Name: "subdirectory1",
						FileRegion: &outputpathpersistency_pb.FileRegion{
							OffsetBytes: 16,
							SizeBytes:   24,
						},
					},
					{
						Name: "subdirectory2",
					},
				},
			},
		}, rootDirectory)

		fileReader.EXPECT().Close()
		require.NoError(t, reader.Close())
	})
}

func TestDirectoryBackedStoreWrite(t *testing.T) {
	ctrl := gomock.NewController(t)

	directory := mock.NewMockDirectory(ctrl)
	store := outputpathpersistency.NewDirectoryBackedStore(directory, 1024*1024)
	outputBaseID := path.MustNewComponent("45ae96d6effc5963e9378529a68c4032")
	temporaryName := path.MustNewComponent("45ae96d6effc5963e9378529a68c4032.tmp")

	t.Run("OldTemporaryRemovalFailure", func(t *testing.T) {
		directory.EXPECT().Remove(temporaryName).Return(status.Error(codes.PermissionDenied, "Cannot make changes to this directory"))

		_, err := store.Write(outputBaseID)
		testutil.RequireEqualStatus(t, status.Error(codes.PermissionDenied, "Failed to remove persistent state temporary file: Cannot make changes to this directory"), err)
	})

	t.Run("TemporaryCreationFailure", func(t *testing.T) {
		directory.EXPECT().Remove(temporaryName).Return(syscall.ENOENT)
		directory.EXPECT().OpenWrite(temporaryName, filesystem.CreateExcl(0o666)).Return(nil, status.Error(codes.ResourceExhausted, "Ran out of inodes"))

		_, err := store.Write(outputBaseID)
		testutil.RequireEqualStatus(t, status.Error(codes.ResourceExhausted, "Failed to create persistent state temporary file: Ran out of inodes"), err)
	})

	t.Run("WriteFailure", func(t *testing.T) {
		directory.EXPECT().Remove(temporaryName).Return(syscall.ENOENT)
		fileWriter := mock.NewMockFileWriter(ctrl)
		directory.EXPECT().OpenWrite(temporaryName, filesystem.CreateExcl(0o666)).Return(fileWriter, nil)

		writer, err := store.Write(outputBaseID)
		require.NoError(t, err)

		fileWriter.EXPECT().WriteAt(gomock.Any(), gomock.Any()).Return(0, status.Error(codes.Internal, "Disk failure"))
		fileWriter.EXPECT().Close()
		directory.EXPECT().Remove(temporaryName)

		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Internal, "Disk failure"),
			writer.Finalize(&outputpathpersistency_pb.RootDirectory{
				InitialCreationTime: &timestamppb.Timestamp{Seconds: 1619157611},
				Contents:            &outputpathpersistency_pb.Directory{},
			}))
	})

	t.Run("SyncFailure", func(t *testing.T) {
		directory.EXPECT().Remove(temporaryName).Return(syscall.ENOENT)
		fileWriter := mock.NewMockFileWriter(ctrl)
		directory.EXPECT().OpenWrite(temporaryName, filesystem.CreateExcl(0o666)).Return(fileWriter, nil)

		writer, err := store.Write(outputBaseID)
		require.NoError(t, err)

		fileWriter.EXPECT().WriteAt(gomock.Any(), gomock.Any()).
			DoAndReturn(func(p []byte, off int64) (int, error) { return len(p), nil }).
			AnyTimes()
		fileWriter.EXPECT().Sync().Return(status.Error(codes.Internal, "Disk failure"))
		fileWriter.EXPECT().Close()
		directory.EXPECT().Remove(temporaryName)

		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Internal, "Failed to synchronize contents of persistent state temporary file: Disk failure"),
			writer.Finalize(&outputpathpersistency_pb.RootDirectory{
				InitialCreationTime: &timestamppb.Timestamp{Seconds: 1619157611},
				Contents:            &outputpathpersistency_pb.Directory{},
			}))
	})

	t.Run("CloseFailure", func(t *testing.T) {
		directory.EXPECT().Remove(temporaryName).Return(syscall.ENOENT)
		fileWriter := mock.NewMockFileWriter(ctrl)
		directory.EXPECT().OpenWrite(temporaryName, filesystem.CreateExcl(0o666)).Return(fileWriter, nil)

		writer, err := store.Write(outputBaseID)
		require.NoError(t, err)

		fileWriter.EXPECT().WriteAt(gomock.Any(), gomock.Any()).
			DoAndReturn(func(p []byte, off int64) (int, error) { return len(p), nil }).
			AnyTimes()
		fileWriter.EXPECT().Sync()
		fileWriter.EXPECT().Close().Return(status.Error(codes.Internal, "Disk failure"))
		directory.EXPECT().Remove(temporaryName)

		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Internal, "Failed to close persistent state temporary file: Disk failure"),
			writer.Finalize(&outputpathpersistency_pb.RootDirectory{
				InitialCreationTime: &timestamppb.Timestamp{Seconds: 1619157611},
				Contents:            &outputpathpersistency_pb.Directory{},
			}))
	})

	t.Run("CloseFailure", func(t *testing.T) {
		directory.EXPECT().Remove(temporaryName).Return(syscall.ENOENT)
		fileWriter := mock.NewMockFileWriter(ctrl)
		directory.EXPECT().OpenWrite(temporaryName, filesystem.CreateExcl(0o666)).Return(fileWriter, nil)

		writer, err := store.Write(outputBaseID)
		require.NoError(t, err)

		fileWriter.EXPECT().WriteAt(gomock.Any(), gomock.Any()).
			DoAndReturn(func(p []byte, off int64) (int, error) { return len(p), nil }).
			AnyTimes()
		fileWriter.EXPECT().Sync()
		fileWriter.EXPECT().Close()
		directory.EXPECT().Rename(temporaryName, directory, outputBaseID).Return(status.Error(codes.Internal, "Disk failure"))
		directory.EXPECT().Remove(temporaryName)

		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Internal, "Failed to rename persistent state temporary file: Disk failure"),
			writer.Finalize(&outputpathpersistency_pb.RootDirectory{
				InitialCreationTime: &timestamppb.Timestamp{Seconds: 1619157611},
				Contents:            &outputpathpersistency_pb.Directory{},
			}))
	})

	t.Run("Success", func(t *testing.T) {
		directory.EXPECT().Remove(temporaryName).Return(syscall.ENOENT)
		fileWriter := mock.NewMockFileWriter(ctrl)
		directory.EXPECT().OpenWrite(temporaryName, filesystem.CreateExcl(0o666)).Return(fileWriter, nil)

		writer, err := store.Write(outputBaseID)
		require.NoError(t, err)

		fileWriter.EXPECT().WriteAt(gomock.Any(), gomock.Any()).
			DoAndReturn(func(p []byte, off int64) (int, error) { return len(p), nil }).
			AnyTimes()
		fileWriter.EXPECT().Sync()
		fileWriter.EXPECT().Close()
		directory.EXPECT().Rename(temporaryName, directory, outputBaseID)

		require.NoError(
			t,
			writer.Finalize(&outputpathpersistency_pb.RootDirectory{
				InitialCreationTime: &timestamppb.Timestamp{Seconds: 1619157611},
				Contents:            &outputpathpersistency_pb.Directory{},
			}))
	})
}
