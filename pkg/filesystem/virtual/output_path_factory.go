package virtual

import (
	"context"

	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"
)

// OutputPath is a directory where build results may be stored. Simple
// implementations may store these results in memory, while more complex
// ones use local or networked storage.
type OutputPath interface {
	virtual.PrepopulatedDirectory

	// FinalizeBuild() is called at the end of every build.
	// Implementations of OutputPath may use this method to persist
	// state.
	FinalizeBuild(ctx context.Context, digestFunction digest.Function)
}

// OutputPathFactory is an interface that is invoked by
// RemoteOutputServiceDirectory to manage individual directories where
// build results may be stored.
type OutputPathFactory interface {
	// StartInitialBuild() is called when a build is started that
	// uses an output base ID that hasn't been observed before, or
	// was cleaned previously.
	StartInitialBuild(outputBaseID path.Component, casFileFactory virtual.CASFileFactory, instanceName digest.InstanceName, errorLogger util.ErrorLogger) OutputPath

	// Clean() is called when the RemoteOutputServiceDirectory
	// service is instructed to clean an output path that is not yet
	// managed by the RemoteOutputServiceDirectory. This may occur
	// if a 'bazel clean' is invoked right after startup. In that
	// case the output path may not yet be attached, though we
	// should clean up any persistent data associated with it.
	//
	// Output paths that have already been returned by
	// StartInitialBuild() will be cleaned by
	// RemoteOutputServiceDirectory by calling
	// PrepopulatedDirectory.RemoveAllChildren(true).
	Clean(outputBaseID path.Component) error
}
