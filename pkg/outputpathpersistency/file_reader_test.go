package outputpathpersistency_test

import (
	"io"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-clientd/internal/mock"
	"github.com/buildbarn/bb-clientd/pkg/outputpathpersistency"
	outputpathpersistency_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/outputpathpersistency"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.uber.org/mock/gomock"
)

func TestFileReader(t *testing.T) {
	ctrl := gomock.NewController(t)

	t.Run("HeaderReadFailure", func(t *testing.T) {
		rawReader := mock.NewMockReaderAt(ctrl)
		rawReader.EXPECT().ReadAt(gomock.Len(16), int64(0)).Return(0, status.Error(codes.Internal, "Disk failure"))

		_, _, err := outputpathpersistency.NewFileReader(rawReader, 1024*1024)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to read header: Disk failure"), err)
	})

	t.Run("HeaderTooShort", func(t *testing.T) {
		rawReader := mock.NewMockReaderAt(ctrl)
		rawReader.EXPECT().ReadAt(gomock.Len(16), int64(0)).DoAndReturn(func(p []byte, off int64) (int, error) {
			return copy(p, "Hello"), io.EOF
		})

		_, _, err := outputpathpersistency.NewFileReader(rawReader, 1024*1024)
		testutil.RequireEqualStatus(t, status.Error(codes.Unknown, "Failed to read header: unexpected EOF"), err)
	})

	t.Run("HeaderInvalidMagic", func(t *testing.T) {
		rawReader := mock.NewMockReaderAt(ctrl)
		rawReader.EXPECT().ReadAt(gomock.Len(16), int64(0)).DoAndReturn(func(p []byte, off int64) (int, error) {
			return copy(p, "<html><title>My first webpage"), nil
		})

		_, _, err := outputpathpersistency.NewFileReader(rawReader, 1024*1024)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Header contains invalid magic"), err)
	})

	t.Run("RootDirectoryInvalidOffset", func(t *testing.T) {
		rawReader := mock.NewMockReaderAt(ctrl)
		rawReader.EXPECT().ReadAt(gomock.Len(16), int64(0)).DoAndReturn(func(p []byte, off int64) (int, error) {
			return copy(p, []byte{
				// Magic.
				0xfa, 0x12, 0xa4, 0xa5,
				// Root directory offset: 12345.
				0x39, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// Root directory size: 200.
				0xc8, 0x00, 0x00, 0x00,
			}), nil
		})

		_, _, err := outputpathpersistency.NewFileReader(rawReader, 10000)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Failed to read root directory: Message at offset [12345, 12545) does not lie in permitted range [16, 10000)"), err)
	})

	t.Run("RootDirectoryReadFailure", func(t *testing.T) {
		rawReader := mock.NewMockReaderAt(ctrl)
		rawReader.EXPECT().ReadAt(gomock.Len(16), int64(0)).DoAndReturn(func(p []byte, off int64) (int, error) {
			return copy(p, []byte{
				// Magic.
				0xfa, 0x12, 0xa4, 0xa5,
				// Root directory offset: 12345.
				0x39, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// Root directory size: 200.
				0xc8, 0x00, 0x00, 0x00,
			}), nil
		})
		rawReader.EXPECT().ReadAt(gomock.Len(200), int64(12345)).Return(0, status.Error(codes.Internal, "Disk failure"))

		_, _, err := outputpathpersistency.NewFileReader(rawReader, 100000)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to read root directory: Disk failure"), err)
	})

	t.Run("RootDirectoryBadProto", func(t *testing.T) {
		rawReader := mock.NewMockReaderAt(ctrl)
		rawReader.EXPECT().ReadAt(gomock.Len(16), int64(0)).DoAndReturn(func(p []byte, off int64) (int, error) {
			return copy(p, []byte{
				// Magic.
				0xfa, 0x12, 0xa4, 0xa5,
				// Root directory offset: 123.
				0x7b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// Root directory size: 10.
				0x0a, 0x00, 0x00, 0x00,
			}), nil
		})
		rawReader.EXPECT().ReadAt(gomock.Len(10), int64(123)).DoAndReturn(func(p []byte, off int64) (int, error) {
			return copy(p, "This is not a valid Protobuf"), io.EOF
		})

		_, _, err := outputpathpersistency.NewFileReader(rawReader, 100000)
		testutil.RequirePrefixedStatus(t, status.Error(codes.InvalidArgument, "Failed to read root directory: proto:"), err)
	})

	t.Run("Success", func(t *testing.T) {
		// Successfully read a root directory.
		rawReader := mock.NewMockReaderAt(ctrl)
		rawReader.EXPECT().ReadAt(gomock.Len(16), int64(0)).DoAndReturn(func(p []byte, off int64) (int, error) {
			return copy(p, []byte{
				// Magic.
				0xfa, 0x12, 0xa4, 0xa5,
				// Root directory offset: 40.
				0x28, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// Root directory size: 50.
				0x32, 0x00, 0x00, 0x00,
			}), nil
		})
		rawReader.EXPECT().ReadAt(gomock.Len(50), int64(40)).DoAndReturn(func(p []byte, off int64) (int, error) {
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

		rootReader, rootDirectory, err := outputpathpersistency.NewFileReader(rawReader, 100000)
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

		// Use the resulting reader to read subdirectory1.
		rawReader.EXPECT().ReadAt(gomock.Len(24), int64(16)).DoAndReturn(func(p []byte, off int64) (int, error) {
			return copy(p, []byte{
				0x1a, 0x16, 0x0a, 0x07, 0x73, 0x79, 0x6d, 0x6c,
				0x69, 0x6e, 0x6b, 0x12, 0x0b, 0x2f, 0x65, 0x74,
				0x63, 0x2f, 0x70, 0x61, 0x73, 0x73, 0x77, 0x64,
			}), nil
		})

		_, subdirectory1, err := rootReader.ReadDirectory(&outputpathpersistency_pb.FileRegion{
			OffsetBytes: 16,
			SizeBytes:   24,
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &outputpathpersistency_pb.Directory{
			Symlinks: []*remoteexecution.SymlinkNode{
				{
					Name:   "symlink",
					Target: "/etc/passwd",
				},
			},
		}, subdirectory1)

		// Use the resulting reader to read subdirectory2.
		_, subdirectory2, err := rootReader.ReadDirectory(nil)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &outputpathpersistency_pb.Directory{}, subdirectory2)
	})
}
