package outputpathpersistency

import (
	"time"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/outputpathpersistency"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type maximumAgeStore struct {
	Store
	clock               clock.Clock
	maximumStateFileAge time.Duration
}

// NewMaximumAgeStore creates a decorator for Store that rejects loading
// output path state files that exceed a certain age. This can be used
// to ensure that output paths don't accumulate data indefinitely.
func NewMaximumAgeStore(base Store, clock clock.Clock, maximumStateFileAge time.Duration) Store {
	return &maximumAgeStore{
		Store:               base,
		clock:               clock,
		maximumStateFileAge: maximumStateFileAge,
	}
}

func (s *maximumAgeStore) Read(outputBaseID path.Component) (ReadCloser, *outputpathpersistency.RootDirectory, error) {
	reader, rootDirectory, err := s.Store.Read(outputBaseID)
	if err != nil {
		return nil, nil, err
	}
	if err := rootDirectory.InitialCreationTime.CheckValid(); err != nil {
		reader.Close()
		return nil, nil, util.StatusWrapWithCode(err, codes.InvalidArgument, "State file contains an invalid initial creation time")
	}
	if initialCreationTime := rootDirectory.InitialCreationTime.AsTime(); initialCreationTime.Before(s.clock.Now().Add(-s.maximumStateFileAge)) {
		reader.Close()
		return nil, nil, status.Errorf(codes.InvalidArgument, "State file was initially created at %s, which is more than %s in the past", initialCreationTime.Format(time.RFC3339), s.maximumStateFileAge)
	}
	return reader, rootDirectory, err
}
