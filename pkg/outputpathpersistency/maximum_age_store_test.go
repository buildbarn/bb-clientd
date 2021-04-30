package outputpathpersistency_test

import (
	"testing"
	"time"

	"github.com/buildbarn/bb-clientd/internal/mock"
	"github.com/buildbarn/bb-clientd/pkg/outputpathpersistency"
	outputpathpersistency_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/outputpathpersistency"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestMaximumAgeStore(t *testing.T) {
	ctrl := gomock.NewController(t)

	baseStore := mock.NewMockOutputPathPersistencyStore(ctrl)
	clock := mock.NewMockClock(ctrl)
	store := outputpathpersistency.NewMaximumAgeStore(baseStore, clock, 24*time.Hour)
	outputBaseID := path.MustNewComponent("94f8674212b1cbe13261338370a0bc33")

	t.Run("BaseFailure", func(t *testing.T) {
		// Failures returned by the backend should be propagated.
		baseStore.EXPECT().Read(outputBaseID).
			Return(nil, nil, status.Error(codes.Internal, "Disk I/O failure"))

		_, _, err := store.Read(outputBaseID)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Disk I/O failure"), err)
	})

	t.Run("MissingInitialCreationTime", func(t *testing.T) {
		baseReader := mock.NewMockOutputPathPersistencyReadCloser(ctrl)
		baseStore.EXPECT().Read(outputBaseID).
			Return(baseReader, &outputpathpersistency_pb.RootDirectory{
				Contents: &outputpathpersistency_pb.Directory{},
			}, nil)
		baseReader.EXPECT().Close()

		_, _, err := store.Read(outputBaseID)
		testutil.RequirePrefixedStatus(t, status.Error(codes.InvalidArgument, "State file contains an invalid initial creation time: proto:"), err)
	})

	t.Run("TooOldInitialCreationTime", func(t *testing.T) {
		baseReader := mock.NewMockOutputPathPersistencyReadCloser(ctrl)
		baseStore.EXPECT().Read(outputBaseID).
			Return(baseReader, &outputpathpersistency_pb.RootDirectory{
				InitialCreationTime: &timestamppb.Timestamp{
					Seconds: 1616729348,
				},
				Contents: &outputpathpersistency_pb.Directory{},
			}, nil)
		clock.EXPECT().Now().Return(time.Unix(1619379857, 0))
		baseReader.EXPECT().Close()

		_, _, err := store.Read(outputBaseID)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "State file was initially created at 2021-03-26T03:29:08Z, which is more than 24h0m0s in the past"), err)
	})

	t.Run("Success", func(t *testing.T) {
		// If the initial creation time is recent enough, the
		// Read() call should succeed.
		baseReader := mock.NewMockOutputPathPersistencyReadCloser(ctrl)
		baseStore.EXPECT().Read(outputBaseID).
			Return(baseReader, &outputpathpersistency_pb.RootDirectory{
				InitialCreationTime: &timestamppb.Timestamp{
					Seconds: 1619377482,
				},
				Contents: &outputpathpersistency_pb.Directory{},
			}, nil)
		clock.EXPECT().Now().Return(time.Unix(1619379857, 0))

		reader, _, err := store.Read(outputBaseID)
		require.NoError(t, err)

		baseReader.EXPECT().Close()
		require.NoError(t, reader.Close())
	})
}
