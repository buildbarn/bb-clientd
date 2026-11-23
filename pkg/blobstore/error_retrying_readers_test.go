package blobstore_test

import (
	"context"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-clientd/internal/mock"
	"github.com/buildbarn/bb-clientd/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

func TestErrorRetryingMappingFetcher(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseMappingFetcher := mock.NewMockMappingFetcher(ctrl)
	clock := mock.NewMockClock(ctrl)
	randomNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	mappingFetcher := blobstore.NewErrorRetryingMappingFetcher(
		baseMappingFetcher,
		clock,
		randomNumberGenerator,
		errorLogger,
		1*time.Second,
		3*time.Second,
		5*time.Minute,
	)

	helloDigest := digest.MustNewDigest("instance_name", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)

	t.Run("NonRetriableError", func(t *testing.T) {
		// Errors for codes other than INTERNAL, UNAVAILABLE and
		// UNKNOWN should never be retried.
		clock.EXPECT().Now().Return(time.Unix(1000, 0))
		baseMappingFetcher.EXPECT().FetchChunkMapping(ctx, helloDigest).Return(chunk.Mapping{}, status.Error(codes.Unauthenticated, "No credentials provided"))

		_, err := mappingFetcher.FetchChunkMapping(ctx, helloDigest)
		testutil.RequireEqualStatus(t, status.Error(codes.Unauthenticated, "No credentials provided"), err)
	})

	t.Run("TooLong", func(t *testing.T) {
		// We should stop doing retries in case more time has
		// passed than the configured maximum delay.
		clock.EXPECT().Now().Return(time.Unix(1000, 0))
		baseMappingFetcher.EXPECT().FetchChunkMapping(ctx, helloDigest).Return(chunk.Mapping{}, status.Error(codes.Internal, "Server on fire"))
		clock.EXPECT().Now().Return(time.Unix(1301, 0))

		_, err := mappingFetcher.FetchChunkMapping(ctx, helloDigest)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Server on fire"), err)
	})

	t.Run("InitialSuccess", func(t *testing.T) {
		// Call that succeeds immediately.
		clock.EXPECT().Now().Return(time.Unix(1000, 0))
		mapping := chunk.Mapping{}
		baseMappingFetcher.EXPECT().FetchChunkMapping(ctx, helloDigest).Return(mapping, nil)

		returnedMapping, err := mappingFetcher.FetchChunkMapping(ctx, helloDigest)
		require.NoError(t, err)
		require.Equal(t, mapping, returnedMapping)
	})

	t.Run("SomeRetries", func(t *testing.T) {
		// Call that only succeeds after a number of retries.
		clock.EXPECT().Now().Return(time.Unix(1000, 0))

		baseMappingFetcher.EXPECT().FetchChunkMapping(ctx, helloDigest).Return(chunk.Mapping{}, status.Error(codes.Internal, "Server on fire"))
		clock.EXPECT().Now().Return(time.Unix(1001, 0))
		errorLogger.EXPECT().Log(testutil.EqPrefixedStatus(status.Error(codes.Internal, "Retrying failed operation after 750ms: Server on fire")))
		randomNumberGenerator.EXPECT().Int64N(int64(1000000000)).Return(int64(750000000))
		timer1 := mock.NewMockTimer(ctrl)
		timer1Wakeup := make(chan time.Time, 1)
		timer1Wakeup <- time.Unix(1001, 750000000)
		clock.EXPECT().NewTimer(750*time.Millisecond).Return(timer1, timer1Wakeup)

		baseMappingFetcher.EXPECT().FetchChunkMapping(ctx, helloDigest).Return(chunk.Mapping{}, status.Error(codes.Internal, "Server on fire"))
		clock.EXPECT().Now().Return(time.Unix(1002, 0))
		errorLogger.EXPECT().Log(testutil.EqPrefixedStatus(status.Error(codes.Internal, "Retrying failed operation after 1.5s: Server on fire")))
		randomNumberGenerator.EXPECT().Int64N(int64(2000000000)).Return(int64(1500000000))
		timer2 := mock.NewMockTimer(ctrl)
		timer2Wakeup := make(chan time.Time, 1)
		timer2Wakeup <- time.Unix(1003, 500000000)
		clock.EXPECT().NewTimer(1500*time.Millisecond).Return(timer2, timer2Wakeup)

		baseMappingFetcher.EXPECT().FetchChunkMapping(ctx, helloDigest).Return(chunk.Mapping{}, status.Error(codes.Internal, "Server on fire"))
		clock.EXPECT().Now().Return(time.Unix(1004, 0))
		errorLogger.EXPECT().Log(testutil.EqPrefixedStatus(status.Error(codes.Internal, "Retrying failed operation after 2s: Server on fire")))
		randomNumberGenerator.EXPECT().Int64N(int64(3000000000)).Return(int64(2000000000))
		timer3 := mock.NewMockTimer(ctrl)
		timer3Wakeup := make(chan time.Time, 1)
		timer3Wakeup <- time.Unix(1002, 0)
		clock.EXPECT().NewTimer(2*time.Second).Return(timer3, timer3Wakeup)

		mapping := chunk.Mapping{}
		baseMappingFetcher.EXPECT().FetchChunkMapping(ctx, helloDigest).Return(mapping, nil)

		returnedMapping, err := mappingFetcher.FetchChunkMapping(ctx, helloDigest)
		require.NoError(t, err)
		require.Equal(t, mapping, returnedMapping)
	})
}

func TestErrorRetryingCDCParametersFetcher(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseCDCParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
	clock := mock.NewMockClock(ctrl)
	randomNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	cdcParametersFetcher := blobstore.NewErrorRetryingCDCParametersFetcher(
		baseCDCParametersFetcher,
		clock,
		randomNumberGenerator,
		errorLogger,
		1*time.Second,
		3*time.Second,
		5*time.Minute,
	)

	instanceName := util.Must(digest.NewInstanceName("instance_name"))

	t.Run("NonRetriableError", func(t *testing.T) {
		// Errors for codes other than INTERNAL, UNAVAILABLE and
		// UNKNOWN should never be retried.
		clock.EXPECT().Now().Return(time.Unix(1000, 0))
		baseCDCParametersFetcher.EXPECT().FetchCDCParameters(ctx, instanceName).Return(nil, status.Error(codes.Unauthenticated, "No credentials provided"))

		_, err := cdcParametersFetcher.FetchCDCParameters(ctx, instanceName)
		testutil.RequireEqualStatus(t, status.Error(codes.Unauthenticated, "No credentials provided"), err)
	})

	t.Run("TooLong", func(t *testing.T) {
		// We should stop doing retries in case more time has
		// passed than the configured maximum delay.
		clock.EXPECT().Now().Return(time.Unix(1000, 0))
		baseCDCParametersFetcher.EXPECT().FetchCDCParameters(ctx, instanceName).Return(nil, status.Error(codes.Internal, "Server on fire"))
		clock.EXPECT().Now().Return(time.Unix(1301, 0))

		_, err := cdcParametersFetcher.FetchCDCParameters(ctx, instanceName)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Server on fire"), err)
	})

	t.Run("InitialSuccess", func(t *testing.T) {
		// Call that succeeds immediately.
		clock.EXPECT().Now().Return(time.Unix(1000, 0))
		cdcParams := &remoteexecution.RepMaxCdcParams{}
		baseCDCParametersFetcher.EXPECT().FetchCDCParameters(ctx, instanceName).Return(cdcParams, nil)

		returnedCdcParams, err := cdcParametersFetcher.FetchCDCParameters(ctx, instanceName)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, cdcParams, returnedCdcParams)
	})
}

func TestErrorRetryingReader(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseReader := mock.NewMockReader[[]byte](ctrl)
	clock := mock.NewMockClock(ctrl)
	randomNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	reader := blobstore.NewErrorRetryingReader(
		baseReader,
		clock,
		randomNumberGenerator,
		errorLogger,
		1*time.Second,
		3*time.Second,
		5*time.Minute,
	)

	helloDigest := digest.MustNewDigest("instance_name", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)

	t.Run("NonRetriableError", func(t *testing.T) {
		clock.EXPECT().Now().Return(time.Unix(1000, 0))
		baseReader.EXPECT().Read(ctx, helloDigest).Return(nil, status.Error(codes.NotFound, "Object not found"))

		_, err := reader.Read(ctx, helloDigest)
		testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Object not found"), err)
	})

	t.Run("InitialSuccess", func(t *testing.T) {
		clock.EXPECT().Now().Return(time.Unix(1000, 0))
		baseReader.EXPECT().Read(ctx, helloDigest).Return([]byte("Hello"), nil)

		data, err := reader.Read(ctx, helloDigest)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
	})

	t.Run("SomeRetries", func(t *testing.T) {
		clock.EXPECT().Now().Return(time.Unix(1000, 0))

		baseReader.EXPECT().Read(ctx, helloDigest).Return(nil, status.Error(codes.Internal, "Server on fire"))
		clock.EXPECT().Now().Return(time.Unix(1001, 0))
		errorLogger.EXPECT().Log(testutil.EqPrefixedStatus(status.Error(codes.Internal, "Retrying failed operation after 750ms: Server on fire")))
		randomNumberGenerator.EXPECT().Int64N(int64(1000000000)).Return(int64(750000000))
		timer1 := mock.NewMockTimer(ctrl)
		timer1Wakeup := make(chan time.Time, 1)
		timer1Wakeup <- time.Unix(1001, 750000000)
		clock.EXPECT().NewTimer(750*time.Millisecond).Return(timer1, timer1Wakeup)

		baseReader.EXPECT().Read(ctx, helloDigest).Return([]byte("Hello"), nil)

		data, err := reader.Read(ctx, helloDigest)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
	})
}
