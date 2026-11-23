package blobstore

import (
	"context"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	bb_reader "github.com/buildbarn/bb-storage/pkg/cas/reader"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/random"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type errorRetryingState struct {
	clock                 clock.Clock
	randomNumberGenerator random.ThreadSafeGenerator
	errorLogger           util.ErrorLogger
	initialInterval       time.Duration
	maximumInterval       time.Duration
	maximumDelay          time.Duration
}

// NewErrorRetryingState creates the state shared by the retrying
// decorators in this file. Retries are performed using exponential
// backoff, using an algorithm that the following blog post refers to as
// "Full Jitter":
// https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
//
// In pure gRPC contexts, a decorator like this isn't needed. Errors can
// be propagated all the way up, and tools such as Bazel can simply
// retry execution of build actions. Unfortunately, Bazel's
// OutputService interface doesn't provide facilities for bb_clientd to
// propagate transient errors. We have no choice but to do some basic
// retrying of FUSE file system operations.
func newErrorRetryingState(clock clock.Clock, randomNumberGenerator random.ThreadSafeGenerator, errorLogger util.ErrorLogger, initialInterval, maximumInterval, maximumDelay time.Duration) errorRetryingState {
	return errorRetryingState{
		clock:                 clock,
		randomNumberGenerator: randomNumberGenerator,
		errorLogger:           errorLogger,
		initialInterval:       initialInterval,
		maximumInterval:       maximumInterval,
		maximumDelay:          maximumDelay,
	}
}

// retryState contains the state that needs to be tracked for every
// operation, storing for how long retries are permitted to continue and
// the interval of the next retry.
type retryState struct {
	endTime  time.Time
	interval time.Duration
}

func (s *errorRetryingState) getRetryState(ctx context.Context) retryState {
	return retryState{
		endTime:  s.clock.Now().Add(s.maximumDelay),
		interval: s.initialInterval,
	}
}

// MaybeSleep checks whether a provided error is retriable. If so, it
// sleeps for a random amount of time. When this function returns true,
// the caller should stop doing retries.
func (s *errorRetryingState) maybeSleep(ctx context.Context, retryState *retryState, err error) bool {
	if !util.IsInfrastructureError(err) || s.clock.Now().After(retryState.endTime) {
		// The error is not retriable, or we've exhausted the
		// maximum retry duration.
		return true
	}

	// Wait for a random amount of time, up to the current interval.
	randomInterval := random.Duration(s.randomNumberGenerator, retryState.interval)
	s.errorLogger.Log(util.StatusWrapf(err, "Retrying failed operation after %s", randomInterval))
	timer, ch := s.clock.NewTimer(randomInterval)
	select {
	case <-ch:
		// Sleeping succeeded. Retry, using a larger interval
		// the next time.
		retryState.interval *= 2
		if retryState.interval > s.maximumInterval {
			retryState.interval = s.maximumInterval
		}
		return false
	case <-ctx.Done():
		// Sleeping got interrupted.
		timer.Stop()
		return true
	}
}

// errorRetryingReader is a decorator for reader.Reader[T] that retries
// Read() operations that fail with INTERNAL, UNAVAILABLE or UNKNOWN
// gRPC status codes.
type errorRetryingReader[T any] struct {
	base bb_reader.Reader[T]
	errorRetryingState
}

// NewErrorRetryingReader creates a decorator for reader.Reader[T] that
// performs retrying of Read() operations that fail with INTERNAL,
// UNAVAILABLE or UNKNOWN gRPC status codes.
func NewErrorRetryingReader[T any](base bb_reader.Reader[T], clock clock.Clock, randomNumberGenerator random.ThreadSafeGenerator, errorLogger util.ErrorLogger, initialInterval, maximumInterval, maximumDelay time.Duration) bb_reader.Reader[T] {
	return &errorRetryingReader[T]{
		base: base,
		errorRetryingState: newErrorRetryingState(
			clock,
			randomNumberGenerator,
			errorLogger,
			initialInterval,
			maximumInterval,
			maximumDelay,
		),
	}
}

func (r *errorRetryingReader[T]) Read(ctx context.Context, d digest.Digest) (T, error) {
	retryState := r.getRetryState(ctx)
	for {
		data, err := r.base.Read(ctx, d)
		if err == nil {
			return data, nil
		}
		if r.maybeSleep(ctx, &retryState, err) {
			var zero T
			return zero, err
		}
	}
}

// errorRetryingMappingFetcher is a decorator for chunk.MappingFetcher
// that retries FetchChunkMapping() operations that fail with INTERNAL,
// UNAVAILABLE or UNKNOWN gRPC status codes.
type errorRetryingMappingFetcher struct {
	base chunk.MappingFetcher
	errorRetryingState
}

// NewErrorRetryingMappingFetcher creates a decorator for
// chunk.MappingFetcher that performs retrying of FetchChunkMapping()
// operations that fail with INTERNAL, UNAVAILABLE or UNKNOWN gRPC
// status codes.
func NewErrorRetryingMappingFetcher(base chunk.MappingFetcher, clock clock.Clock, randomNumberGenerator random.ThreadSafeGenerator, errorLogger util.ErrorLogger, initialInterval, maximumInterval, maximumDelay time.Duration) chunk.MappingFetcher {
	return &errorRetryingMappingFetcher{
		base: base,
		errorRetryingState: newErrorRetryingState(
			clock,
			randomNumberGenerator,
			errorLogger,
			initialInterval,
			maximumInterval,
			maximumDelay,
		),
	}
}

func (f *errorRetryingMappingFetcher) FetchChunkMapping(ctx context.Context, d digest.Digest) (chunk.Mapping, error) {
	retryState := f.getRetryState(ctx)
	for {
		mapping, err := f.base.FetchChunkMapping(ctx, d)
		if err == nil {
			return mapping, nil
		}
		if f.maybeSleep(ctx, &retryState, err) {
			return chunk.Mapping{}, err
		}
	}
}

// errorRetryingCDCParametersFetcher is a decorator for
// capabilities.CDCParametersFetcher that retries FetchCDCParameters()
// operations that fail with INTERNAL, UNAVAILABLE or UNKNOWN gRPC
// status codes.
type errorRetryingCDCParametersFetcher struct {
	base capabilities.CDCParametersFetcher
	errorRetryingState
}

// NewErrorRetryingCDCParametersFetcher creates a decorator for
// capabilities.CDCParametersFetcher that performs retrying of
// FetchCDCParameters() operations that fail with INTERNAL, UNAVAILABLE
// or UNKNOWN gRPC status codes.
func NewErrorRetryingCDCParametersFetcher(base capabilities.CDCParametersFetcher, clock clock.Clock, randomNumberGenerator random.ThreadSafeGenerator, errorLogger util.ErrorLogger, initialInterval, maximumInterval, maximumDelay time.Duration) capabilities.CDCParametersFetcher {
	return &errorRetryingCDCParametersFetcher{
		base: base,
		errorRetryingState: newErrorRetryingState(
			clock,
			randomNumberGenerator,
			errorLogger,
			initialInterval,
			maximumInterval,
			maximumDelay,
		),
	}
}

func (f *errorRetryingCDCParametersFetcher) FetchCDCParameters(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.RepMaxCdcParams, error) {
	retryState := f.getRetryState(ctx)
	for {
		params, err := f.base.FetchCDCParameters(ctx, instanceName)
		if err == nil {
			return params, nil
		}
		if f.maybeSleep(ctx, &retryState, err) {
			return nil, err
		}
	}
}
