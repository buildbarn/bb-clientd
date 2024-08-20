package outputpathpersistency_test

import (
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-clientd/internal/mock"
	"github.com/buildbarn/bb-clientd/pkg/outputpathpersistency"
	outputpathpersistency_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/outputpathpersistency"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/types/known/timestamppb"

	"go.uber.org/mock/gomock"
)

func TestFileWriter(t *testing.T) {
	ctrl := gomock.NewController(t)

	t.Run("Success", func(t *testing.T) {
		rawWriter := mock.NewMockWriterAt(ctrl)
		rawWriter.EXPECT().WriteAt([]byte{
			// Subdirectory.
			0x1a, 0x16, 0x0a, 0x07, 0x73, 0x79, 0x6d, 0x6c,
			0x69, 0x6e, 0x6b, 0x12, 0x0b, 0x2f, 0x65, 0x74,
			0x63, 0x2f, 0x70, 0x61, 0x73, 0x73, 0x77, 0x64,
			// Root directory.
			0x0a, 0x06, 0x08, 0xeb, 0xc4, 0x89, 0x84, 0x06,
			0x12, 0x28, 0x12, 0x15, 0x0a, 0x0d, 0x73, 0x75,
			0x62, 0x64, 0x69, 0x72, 0x65, 0x63, 0x74, 0x6f,
			0x72, 0x79, 0x31, 0x12, 0x04, 0x08, 0x10, 0x10,
			0x18, 0x12, 0x0f, 0x0a, 0x0d, 0x73, 0x75, 0x62,
			0x64, 0x69, 0x72, 0x65, 0x63, 0x74, 0x6f, 0x72,
			0x79, 0x32,
		}, int64(16)).Return(74, nil)
		rawWriter.EXPECT().WriteAt([]byte{
			// Magic.
			0xfa, 0x12, 0xa4, 0xa5,
			// Root directory offset: 40.
			0x28, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			// Root directory size: 50.
			0x32, 0x00, 0x00, 0x00,
		}, int64(0)).Return(16, nil)

		// Write subdirectory #1.
		w := outputpathpersistency.NewFileWriter(rawWriter)
		fileRegion1, err := w.WriteDirectory(&outputpathpersistency_pb.Directory{
			Symlinks: []*remoteexecution.SymlinkNode{
				{
					Name:   "symlink",
					Target: "/etc/passwd",
				},
			},
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &outputpathpersistency_pb.FileRegion{
			OffsetBytes: 16,
			SizeBytes:   24,
		}, fileRegion1)

		// Write subdirectory #2. Because it's empty, we don't
		// allocate any space for it explicitly.
		fileRegion2, err := w.WriteDirectory(&outputpathpersistency_pb.Directory{})
		require.NoError(t, err)
		require.Nil(t, fileRegion2)

		require.NoError(t, w.Finalize(&outputpathpersistency_pb.RootDirectory{
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
		}))
	})
}
