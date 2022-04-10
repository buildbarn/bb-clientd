package blobstore_test

import (
	"context"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-clientd/internal/mock"
	"github.com/buildbarn/bb-clientd/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestErrorRetryingBlobAccessGet(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseBlobAccess := mock.NewMockBlobAccess(ctrl)
	clock := mock.NewMockClock(ctrl)
	randomNumberGenerator := mock.NewMockThreadSafeGenerator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	blobAccess := blobstore.NewErrorRetryingBlobAccess(
		baseBlobAccess,
		clock,
		randomNumberGenerator,
		errorLogger,
		1*time.Second,
		3*time.Second,
		5*time.Minute)

	helloDigest := digest.MustNewDigest("instance_name", "8b1a9953c4611296a827abf8c47804d7", 5)
	helloDigestSet := helloDigest.ToSingletonSet()
	instanceName := digest.MustNewInstanceName("instance_name")

	t.Run("GetNonRetriableError", func(t *testing.T) {
		// Errors for codes other than INTERNAL, UNAVAILABLE and
		// UNKNOWN should never be retried.
		clock.EXPECT().Now().Return(time.Unix(1000, 0))
		baseBlobAccess.EXPECT().Get(ctx, helloDigest).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Object not found")))

		_, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(10000)
		testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Object not found"), err)
	})

	t.Run("GetTooLong", func(t *testing.T) {
		// We should stop doing retries in case more time has
		// passed than the configured maximum delay.
		clock.EXPECT().Now().Return(time.Unix(1000, 0))
		baseBlobAccess.EXPECT().Get(ctx, helloDigest).Return(buffer.NewBufferFromError(status.Error(codes.Internal, "Server on fire")))
		clock.EXPECT().Now().Return(time.Unix(1301, 0))

		_, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(10000)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Server on fire"), err)
	})

	t.Run("GetInitialSuccess", func(t *testing.T) {
		// Call that succeeds immediately.
		clock.EXPECT().Now().Return(time.Unix(1000, 0))
		baseBlobAccess.EXPECT().Get(ctx, helloDigest).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))

		data, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(10000)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
	})

	t.Run("GetSomeRetries", func(t *testing.T) {
		// Call that only succeeds after a number of retries.
		clock.EXPECT().Now().Return(time.Unix(1000, 0))

		baseBlobAccess.EXPECT().Get(ctx, helloDigest).Return(buffer.NewBufferFromError(status.Error(codes.Internal, "Server on fire")))
		clock.EXPECT().Now().Return(time.Unix(1001, 0))
		errorLogger.EXPECT().Log(testutil.EqPrefixedStatus(status.Error(codes.Internal, "Retrying failed operation after 750ms: Server on fire")))
		randomNumberGenerator.EXPECT().Int63n(int64(1000000000)).Return(int64(750000000))
		timer1 := mock.NewMockTimer(ctrl)
		timer1Wakeup := make(chan time.Time, 1)
		timer1Wakeup <- time.Unix(1001, 750000000)
		clock.EXPECT().NewTimer(750*time.Millisecond).Return(timer1, timer1Wakeup)

		baseBlobAccess.EXPECT().Get(ctx, helloDigest).Return(buffer.NewBufferFromError(status.Error(codes.Internal, "Server on fire")))
		clock.EXPECT().Now().Return(time.Unix(1002, 0))
		errorLogger.EXPECT().Log(testutil.EqPrefixedStatus(status.Error(codes.Internal, "Retrying failed operation after 1.5s: Server on fire")))
		randomNumberGenerator.EXPECT().Int63n(int64(2000000000)).Return(int64(1500000000))
		timer2 := mock.NewMockTimer(ctrl)
		timer2Wakeup := make(chan time.Time, 1)
		timer2Wakeup <- time.Unix(1003, 500000000)
		clock.EXPECT().NewTimer(1500*time.Millisecond).Return(timer2, timer2Wakeup)

		baseBlobAccess.EXPECT().Get(ctx, helloDigest).Return(buffer.NewBufferFromError(status.Error(codes.Internal, "Server on fire")))
		clock.EXPECT().Now().Return(time.Unix(1004, 0))
		errorLogger.EXPECT().Log(testutil.EqPrefixedStatus(status.Error(codes.Internal, "Retrying failed operation after 2s: Server on fire")))
		randomNumberGenerator.EXPECT().Int63n(int64(3000000000)).Return(int64(2000000000))
		timer3 := mock.NewMockTimer(ctrl)
		timer3Wakeup := make(chan time.Time, 1)
		timer3Wakeup <- time.Unix(1002, 0)
		clock.EXPECT().NewTimer(2*time.Second).Return(timer3, timer3Wakeup)

		baseBlobAccess.EXPECT().Get(ctx, helloDigest).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))

		data, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(10000)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
	})

	// Coverage for FindMissing() and GetCapabilities() is minimal,
	// as the tests for Get() already exercise the retry logic that
	// is shared by both methods.

	t.Run("FindMissingNonRetriableError", func(t *testing.T) {
		// Errors for codes other than INTERNAL, UNAVAILABLE and
		// UNKNOWN should never be retried.
		clock.EXPECT().Now().Return(time.Unix(1000, 0))
		baseBlobAccess.EXPECT().FindMissing(ctx, helloDigestSet).Return(digest.EmptySet, status.Error(codes.Unauthenticated, "No credentials provided"))

		_, err := blobAccess.FindMissing(ctx, helloDigestSet)
		testutil.RequireEqualStatus(t, status.Error(codes.Unauthenticated, "No credentials provided"), err)
	})

	t.Run("FindMissingTooLong", func(t *testing.T) {
		// We should stop doing retries in case more time has
		// passed than the configured maximum delay.
		clock.EXPECT().Now().Return(time.Unix(1000, 0))
		baseBlobAccess.EXPECT().FindMissing(ctx, helloDigestSet).Return(digest.EmptySet, status.Error(codes.Internal, "Server on fire"))
		clock.EXPECT().Now().Return(time.Unix(1301, 0))

		_, err := blobAccess.FindMissing(ctx, helloDigestSet)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Server on fire"), err)
	})

	t.Run("FindMissingInitialSuccess", func(t *testing.T) {
		// Call that succeeds immediately.
		clock.EXPECT().Now().Return(time.Unix(1000, 0))
		baseBlobAccess.EXPECT().FindMissing(ctx, helloDigestSet).Return(helloDigestSet, nil)

		missing, err := blobAccess.FindMissing(ctx, helloDigestSet)
		require.NoError(t, err)
		require.Equal(t, helloDigestSet, missing)
	})

	t.Run("GetCapabilitiesNonRetriableError", func(t *testing.T) {
		// Errors for codes other than INTERNAL, UNAVAILABLE and
		// UNKNOWN should never be retried.
		clock.EXPECT().Now().Return(time.Unix(1000, 0))
		baseBlobAccess.EXPECT().GetCapabilities(ctx, instanceName).Return(nil, status.Error(codes.Unauthenticated, "No credentials provided"))

		_, err := blobAccess.GetCapabilities(ctx, instanceName)
		testutil.RequireEqualStatus(t, status.Error(codes.Unauthenticated, "No credentials provided"), err)
	})

	t.Run("GetCapabilitiesTooLong", func(t *testing.T) {
		// We should stop doing retries in case more time has
		// passed than the configured maximum delay.
		clock.EXPECT().Now().Return(time.Unix(1000, 0))
		baseBlobAccess.EXPECT().GetCapabilities(ctx, instanceName).Return(nil, status.Error(codes.Internal, "Server on fire"))
		clock.EXPECT().Now().Return(time.Unix(1301, 0))

		_, err := blobAccess.GetCapabilities(ctx, instanceName)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Server on fire"), err)
	})

	t.Run("GetCapabilitiesInitialSuccess", func(t *testing.T) {
		// Call that succeeds immediately.
		clock.EXPECT().Now().Return(time.Unix(1000, 0))
		baseBlobAccess.EXPECT().GetCapabilities(ctx, instanceName).Return(&remoteexecution.ServerCapabilities{
			CacheCapabilities: &remoteexecution.CacheCapabilities{},
		}, nil)

		serverCapabilities, err := blobAccess.GetCapabilities(ctx, instanceName)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &remoteexecution.ServerCapabilities{
			CacheCapabilities: &remoteexecution.CacheCapabilities{},
		}, serverCapabilities)
	})
}
