package virtual

import (
	"context"

	"github.com/buildbarn/bb-clientd/pkg/outputpathpersistency"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	outputpathpersistency_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/outputpathpersistency"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type persistentOutputPathFactory struct {
	base           OutputPathFactory
	store          outputpathpersistency.Store
	clock          clock.Clock
	errorLogger    util.ErrorLogger
	symlinkFactory virtual.SymlinkFactory
}

// NewPersistentOutputPathFactory creates a decorator for
// OutputPathFactory that persists the contents of an OutputPath to disk
// after every build. When an OutputPath is created, it will attempt to
// reload the state from disk.
func NewPersistentOutputPathFactory(base OutputPathFactory, store outputpathpersistency.Store, clock clock.Clock, errorLogger util.ErrorLogger, symlinkFactory virtual.SymlinkFactory) OutputPathFactory {
	return &persistentOutputPathFactory{
		base:           base,
		store:          store,
		clock:          clock,
		errorLogger:    errorLogger,
		symlinkFactory: symlinkFactory,
	}
}

type stateRestorer struct {
	casFileFactory virtual.CASFileFactory
	symlinkFactory virtual.SymlinkFactory
	digestFunction digest.Function
}

func (sr *stateRestorer) restoreDirectoryRecursive(reader outputpathpersistency.Reader, contents *outputpathpersistency_pb.Directory, d virtual.PrepopulatedDirectory, dPath *path.Trace) error {
	// Recursively create nested directories.
	for _, entry := range contents.Directories {
		component, ok := path.NewComponent(entry.Name)
		if !ok {
			return status.Errorf(codes.InvalidArgument, "Directory %#v inside directory %#v has an invalid name", entry.Name, dPath.GetUNIXString())
		}
		childPath := dPath.Append(component)
		childReader, childContents, err := reader.ReadDirectory(entry.FileRegion)
		if err != nil {
			return util.StatusWrapf(err, "Failed to load directory %#v", childPath.GetUNIXString())
		}
		childDirectory, err := d.CreateAndEnterPrepopulatedDirectory(component)
		if err != nil {
			return util.StatusWrapf(err, "Failed to create directory %#v", childPath.GetUNIXString())
		}
		if err := sr.restoreDirectoryRecursive(childReader, childContents, childDirectory, childPath); err != nil {
			return err
		}
	}

	// Create files and symbolic links. Ensure that leaves are properly
	// unlinked if this method fails.
	initialNodes := map[path.Component]virtual.InitialChild{}
	defer func() {
		for _, initialNode := range initialNodes {
			_, leaf := initialNode.GetPair()
			leaf.Unlink()
		}
	}()

	for _, entry := range contents.Files {
		component, ok := path.NewComponent(entry.Name)
		if !ok {
			return status.Errorf(codes.InvalidArgument, "File %#v inside directory %#v has an invalid name", entry.Name, dPath.GetUNIXString())
		}
		if _, ok := initialNodes[component]; ok {
			return status.Errorf(codes.InvalidArgument, "Directory contains multiple children named %#v", entry.Name)
		}

		childPath := dPath.Append(component)
		childDigest, err := sr.digestFunction.NewDigestFromProto(entry.Digest)
		if err != nil {
			return util.StatusWrapf(err, "Failed to obtain digest for file %#v", childPath.GetUNIXString())
		}
		initialNodes[component] = virtual.InitialChild{}.FromLeaf(sr.casFileFactory.LookupFile(childDigest, entry.IsExecutable, nil))
	}
	for _, entry := range contents.Symlinks {
		component, ok := path.NewComponent(entry.Name)
		if !ok {
			return status.Errorf(codes.InvalidArgument, "Symlink %#v inside directory %#v has an invalid name", entry.Name, dPath.GetUNIXString())
		}
		if _, ok := initialNodes[component]; ok {
			return status.Errorf(codes.InvalidArgument, "Directory contains multiple children named %#v", entry.Name)
		}

		initialNodes[component] = virtual.InitialChild{}.FromLeaf(sr.symlinkFactory.LookupSymlink([]byte(entry.Target)))
	}
	if err := d.CreateChildren(initialNodes, true); err != nil {
		return util.StatusWrap(err, "Failed to create files and symbolic links")
	}

	initialNodes = nil
	return nil
}

func (opf *persistentOutputPathFactory) StartInitialBuild(outputBaseID path.Component, casFileFactory virtual.CASFileFactory, digestFunction digest.Function, errorLogger util.ErrorLogger) OutputPath {
	d := opf.base.StartInitialBuild(outputBaseID, casFileFactory, digestFunction, errorLogger)

	var initialCreationTime *timestamppb.Timestamp
	if reader, rootDirectory, err := opf.store.Read(outputBaseID); err != nil {
		opf.errorLogger.Log(util.StatusWrapf(err, "Failed to open state file for output path %#v", outputBaseID.String()))
	} else if rootDirectory.Contents == nil {
		reader.Close()
		opf.errorLogger.Log(status.Errorf(codes.InvalidArgument, "State file for output path %#v does not contain a root directory", outputBaseID.String()))
	} else {
		initialCreationTime = rootDirectory.InitialCreationTime
		sr := stateRestorer{
			casFileFactory: casFileFactory,
			digestFunction: digestFunction,
			symlinkFactory: opf.symlinkFactory,
		}
		err = sr.restoreDirectoryRecursive(reader, rootDirectory.Contents, d, nil)
		reader.Close()
		if err != nil {
			opf.errorLogger.Log(util.StatusWrapf(err, "Failed to restore state file for output path %#v", outputBaseID.String()))
		}
	}
	if initialCreationTime == nil {
		// The output path has not been initialized with any
		// existing state.
		initialCreationTime = timestamppb.New(opf.clock.Now())
	}

	return &persistentOutputPath{
		OutputPath:          d,
		factory:             opf,
		outputBaseID:        outputBaseID,
		initialCreationTime: initialCreationTime,
	}
}

func (opf *persistentOutputPathFactory) Clean(outputBaseID path.Component) error {
	if err := opf.base.Clean(outputBaseID); err != nil {
		return err
	}
	if err := opf.store.Clean(outputBaseID); err != nil {
		return util.StatusWrap(err, "Failed to remove persistent state for output path")
	}
	return nil
}

type persistentOutputPath struct {
	OutputPath
	factory             *persistentOutputPathFactory
	outputBaseID        path.Component
	initialCreationTime *timestamppb.Timestamp
}

func (op *persistentOutputPath) FinalizeBuild(ctx context.Context, digestFunction digest.Function) {
	op.OutputPath.FinalizeBuild(ctx, digestFunction)

	if err := op.saveOutputPath(); err != nil {
		op.factory.errorLogger.Log(util.StatusWrapf(err, "Failed to save the contents of output path %#v", op.outputBaseID.String()))
	}
}

func (op *persistentOutputPath) saveOutputPath() error {
	writer, err := op.factory.store.Write(op.outputBaseID)
	if err != nil {
		return err
	}
	contents, err := saveDirectoryRecursive(op, nil, writer)
	if err != nil {
		writer.Close()
		return err
	}
	if err := writer.Finalize(&outputpathpersistency_pb.RootDirectory{
		InitialCreationTime: op.initialCreationTime,
		Contents:            contents,
	}); err != nil {
		return err
	}
	return nil
}

func saveDirectoryRecursive(d virtual.PrepopulatedDirectory, dPath *path.Trace, w outputpathpersistency.Writer) (*outputpathpersistency_pb.Directory, error) {
	directories, leaves, err := d.LookupAllChildren()
	if err != nil {
		return nil, util.StatusWrapf(err, "Failed to look up children of directory %#v", dPath.GetUNIXString())
	}
	var directory outputpathpersistency_pb.Directory
	for _, entry := range directories {
		childPath := dPath.Append(entry.Name)
		childDirectory, err := saveDirectoryRecursive(entry.Child, childPath, w)
		if err != nil {
			return nil, err
		}
		fileRegion, err := w.WriteDirectory(childDirectory)
		if err != nil {
			return nil, util.StatusWrapf(err, "Failed to write directory %#v to state file", childPath.GetUNIXString())
		}
		directory.Directories = append(directory.Directories, &outputpathpersistency_pb.DirectoryNode{
			Name:       entry.Name.String(),
			FileRegion: fileRegion,
		})
	}
	for _, entry := range leaves {
		// We can't preserve the stable-status.txt and
		// volatile-status.txt files, as Bazel assumes that
		// these files remain writable.
		//
		// TODO: We should patch Bazel to not modify these files
		// in-place. It should create new files instead.
		if dPath == nil {
			if s := entry.Name.String(); s == "stable-status.txt" || s == "volatile-status.txt" {
				continue
			}
		}
		p := virtual.ApplyAppendOutputPathPersistencyDirectoryNode{
			Directory: &directory,
			Name:      entry.Name,
		}
		if !entry.Child.VirtualApply(&p) {
			panic("output path contains leaves that don't support ApplyAppendOutputPathPersistencyDirectoryNode")
		}
	}
	return &directory, nil
}

func (op *persistentOutputPath) RemoveAllChildren(forbidNewChildren bool) error {
	if err := op.OutputPath.RemoveAllChildren(forbidNewChildren); err != nil {
		return err
	}
	if forbidNewChildren {
		// Invocation of 'bazel clean'. Not only remove all data
		// from the underlying OutputPath, also remove the state
		// file, so that future invocations don't end up
		// reloading the previous state.
		if err := op.factory.store.Clean(op.outputBaseID); err != nil {
			return util.StatusWrap(err, "Failed to remove persistent state for output path")
		}
	}
	return nil
}
