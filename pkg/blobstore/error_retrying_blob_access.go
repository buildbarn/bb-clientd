package blobstore

import (
	"context"
	"time"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/random"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type errorRetryingBlobAccess struct {
	blobstore.BlobAccess
	clock                      clock.Clock
	randomNumberGenerator      random.ThreadSafeGenerator
	errorLogger                util.ErrorLogger
	initialIntervalNanoseconds int64
	maximumIntervalNanoseconds int64
	maximumDelay               time.Duration
}

// NewErrorRetryingBlobAccess creates a decorator for BlobAccess that
// performs retrying of Get() and FindMissing() operations that fail
// with INTERNAL, UNAVAILABLE or UNKNOWN gRPC status codes. Put()
// operations cannot be retried, as the buffer provided to this method
// is destroyed upon failure.
//
// Retries are performed using exponential backoff, using an algorithm
// that the following blog post refers to as "Full Jitter":
// https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
//
// In pure gRPC contexts, a decorator like this isn't needed. Errors can
// be propagated all the way up, and tools such as Bazel can simply
// retry execution of build actions. Unfortunately, Bazel's
// OutputService interface doesn't provide facilities for bb_clientd to
// propagate transient errors. We have no choice but to do some basic
// retrying of FUSE file system operations.
func NewErrorRetryingBlobAccess(base blobstore.BlobAccess, clock clock.Clock, randomNumberGenerator random.ThreadSafeGenerator, errorLogger util.ErrorLogger, initialInterval, maximumInterval, maximumDelay time.Duration) blobstore.BlobAccess {
	return &errorRetryingBlobAccess{
		BlobAccess:                 base,
		clock:                      clock,
		randomNumberGenerator:      randomNumberGenerator,
		errorLogger:                errorLogger,
		initialIntervalNanoseconds: initialInterval.Nanoseconds(),
		maximumIntervalNanoseconds: maximumInterval.Nanoseconds(),
		maximumDelay:               maximumDelay,
	}
}

// RetryState contains the state that needs to be tracked for every
// operation, storing for how long retries are permitted to continue and
// the interval of the next retry.
type retryState struct {
	endTime             time.Time
	intervalNanoseconds int64
}

func (ba *errorRetryingBlobAccess) getRetryState(ctx context.Context) retryState {
	return retryState{
		endTime:             ba.clock.Now().Add(ba.maximumDelay),
		intervalNanoseconds: ba.initialIntervalNanoseconds,
	}
}

// MaybeSleep checks whether a provided error is retriable. If so, it
// sleeps for a random amount of time. When this function returns true,
// the caller should stop doing retries.
func (ba *errorRetryingBlobAccess) maybeSleep(ctx context.Context, retryState *retryState, err error) bool {
	if code := status.Code(err); (code != codes.Internal && code != codes.Unavailable && code != codes.Unknown) || ba.clock.Now().After(retryState.endTime) {
		// The error is not retriable, or we've exhausted the
		// maximum retry duration.
		return true
	}

	// Wait for a random amount of time, up to the current interval.
	randomInterval := time.Duration(ba.randomNumberGenerator.Int63n(retryState.intervalNanoseconds)) * time.Nanosecond
	ba.errorLogger.Log(util.StatusWrapf(err, "Retrying failed operation after %s", randomInterval))
	timer, ch := ba.clock.NewTimer(randomInterval)
	select {
	case <-ch:
		// Sleeping succeeded. Retry, using a larger interval
		// the next time.
		retryState.intervalNanoseconds *= 2
		if retryState.intervalNanoseconds > ba.maximumIntervalNanoseconds {
			retryState.intervalNanoseconds = ba.maximumIntervalNanoseconds
		}
		return false
	case <-ctx.Done():
		// Sleeping got interrupted.
		timer.Stop()
		return true
	}
}

func (ba *errorRetryingBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	retryState := ba.getRetryState(ctx)
	return buffer.WithErrorHandler(
		ba.BlobAccess.Get(ctx, digest),
		&retryingErrorHandler{
			blobAccess: ba,
			context:    ctx,
			digest:     digest,
			retryState: retryState,
		})
}

func (ba *errorRetryingBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	retryState := ba.getRetryState(ctx)
	for {
		missing, err := ba.BlobAccess.FindMissing(ctx, digests)
		if err == nil {
			return missing, nil
		}
		if ba.maybeSleep(ctx, &retryState, err) {
			return digest.EmptySet, err
		}
	}
}

// RetryingErrorHandler is an ErrorHandler that is used by Get() to
// perform retries.
type retryingErrorHandler struct {
	blobAccess *errorRetryingBlobAccess
	context    context.Context
	digest     digest.Digest
	retryState retryState
}

func (eh *retryingErrorHandler) OnError(err error) (buffer.Buffer, error) {
	if eh.blobAccess.maybeSleep(eh.context, &eh.retryState, err) {
		return nil, err
	}
	return eh.blobAccess.BlobAccess.Get(eh.context, eh.digest), nil
}

func (eh *retryingErrorHandler) Done() {}
