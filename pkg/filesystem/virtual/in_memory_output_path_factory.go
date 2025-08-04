package virtual

import (
	"context"

	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/pool"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type inMemoryOutputPathFactory struct {
	filePool              pool.FilePool
	symlinkFactory        virtual.SymlinkFactory
	handleAllocator       virtual.StatefulHandleAllocator
	initialContentsSorter virtual.Sorter
	clock                 clock.Clock
}

// NewInMemoryOutputPathFactory creates an OutputPathFactory that simply
// creates output paths that store all of their data in memory.
func NewInMemoryOutputPathFactory(filePool pool.FilePool, symlinkFactory virtual.SymlinkFactory, handleAllocator virtual.StatefulHandleAllocator, initialContentsSorter virtual.Sorter, clock clock.Clock) OutputPathFactory {
	return &inMemoryOutputPathFactory{
		filePool:              filePool,
		symlinkFactory:        symlinkFactory,
		handleAllocator:       handleAllocator,
		initialContentsSorter: initialContentsSorter,
		clock:                 clock,
	}
}

func (opf *inMemoryOutputPathFactory) StartInitialBuild(outputBaseID path.Component, casFileFactory virtual.CASFileFactory, digestFunction digest.Function, errorLogger util.ErrorLogger) OutputPath {
	return inMemoryOutputPath{
		PrepopulatedDirectory: virtual.NewInMemoryPrepopulatedDirectory(
			virtual.NewHandleAllocatingFileAllocator(
				virtual.NewPoolBackedFileAllocator(
					opf.filePool,
					errorLogger),
				opf.handleAllocator),
			opf.symlinkFactory,
			errorLogger,
			opf.handleAllocator,
			opf.initialContentsSorter,
			/* hiddenFilesMatcher = */ func(string) bool { return false },
			opf.clock,
			/* defaultAttributesSetter = */ func(requested virtual.AttributesMask, attributes *virtual.Attributes) {},
		),
	}
}

func (opf *inMemoryOutputPathFactory) Clean(outputBaseID path.Component) error {
	// No persistent state associated with in-memory output paths.
	return nil
}

type inMemoryOutputPath struct {
	virtual.PrepopulatedDirectory
}

func (op inMemoryOutputPath) FinalizeBuild(ctx context.Context, digestFunction digest.Function) {}
