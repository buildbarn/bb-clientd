package fuse

import (
	"context"

	"github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/fuse"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/random"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type inMemoryOutputPathFactory struct {
	filePool      filesystem.FilePool
	entryNotifier fuse.EntryNotifier
}

// NewInMemoryOutputPathFactory creates an OutputPathFactory that simply
// creates output paths that store all of their data in memory.
func NewInMemoryOutputPathFactory(filePool filesystem.FilePool, entryNotifier fuse.EntryNotifier) OutputPathFactory {
	return &inMemoryOutputPathFactory{
		filePool:      filePool,
		entryNotifier: entryNotifier,
	}
}

func (opf *inMemoryOutputPathFactory) StartInitialBuild(outputBaseID path.Component, casFileFactory fuse.CASFileFactory, instanceName digest.InstanceName, errorLogger util.ErrorLogger, inodeNumber uint64) OutputPath {
	return inMemoryOutputPath{
		PrepopulatedDirectory: fuse.NewInMemoryPrepopulatedDirectory(
			fuse.NewPoolBackedFileAllocator(
				opf.filePool,
				errorLogger,
				random.FastThreadSafeGenerator),
			errorLogger,
			inodeNumber,
			random.FastThreadSafeGenerator,
			opf.entryNotifier),
	}
}

func (opf *inMemoryOutputPathFactory) Clean(outputBaseID path.Component) error {
	// No persistent state associated with in-memory output paths.
	return nil
}

type inMemoryOutputPath struct {
	fuse.PrepopulatedDirectory
}

func (op inMemoryOutputPath) FinalizeBuild(ctx context.Context, digestFunction digest.Function) {}
