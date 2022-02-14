package fuse

import (
	"context"

	"github.com/buildbarn/bb-clientd/pkg/outputpathpersistency"
	re_fuse "github.com/buildbarn/bb-remote-execution/pkg/filesystem/fuse"
	outputpathpersistency_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/outputpathpersistency"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/hanwen/go-fuse/v2/fuse"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type persistentOutputPathFactory struct {
	base        OutputPathFactory
	store       outputpathpersistency.Store
	clock       clock.Clock
	errorLogger util.ErrorLogger
}

// NewPersistentOutputPathFactory creates a decorator for
// OutputPathFactory that persists the contents of an OutputPath to disk
// after every build. When an OutputPath is created, it will attempt to
// reload the state from disk.
func NewPersistentOutputPathFactory(base OutputPathFactory, store outputpathpersistency.Store, clock clock.Clock, errorLogger util.ErrorLogger) OutputPathFactory {
	return &persistentOutputPathFactory{
		base:        base,
		store:       store,
		clock:       clock,
		errorLogger: errorLogger,
	}
}

type stateRestorer struct {
	casFileFactory re_fuse.CASFileFactory
	instanceName   digest.InstanceName
}

func (sr *stateRestorer) restoreDirectoryRecursive(reader outputpathpersistency.Reader, contents *outputpathpersistency_pb.Directory, d re_fuse.PrepopulatedDirectory, dPath *path.Trace) error {
	// Recursively create nested directories.
	for _, entry := range contents.Directories {
		component, ok := path.NewComponent(entry.Name)
		if !ok {
			return status.Errorf(codes.InvalidArgument, "Directory %#v inside directory %#v has an invalid name", entry.Name, dPath.String())
		}
		childPath := dPath.Append(component)
		childReader, childContents, err := reader.ReadDirectory(entry.FileRegion)
		if err != nil {
			return util.StatusWrapf(err, "Failed to load directory %#v", childPath.String())
		}
		childDirectory, err := d.CreateAndEnterPrepopulatedDirectory(component)
		if err != nil {
			return util.StatusWrapf(err, "Failed to create directory %#v", childPath.String())
		}
		if err := sr.restoreDirectoryRecursive(childReader, childContents, childDirectory, childPath); err != nil {
			return err
		}
	}

	// Create files and symbolic links.
	initialNodes := map[path.Component]re_fuse.InitialNode{}
	for _, entry := range contents.Files {
		component, ok := path.NewComponent(entry.Name)
		if !ok {
			return status.Errorf(codes.InvalidArgument, "File %#v inside directory %#v has an invalid name", entry.Name, dPath.String())
		}
		childPath := dPath.Append(component)
		childDigest, err := sr.instanceName.NewDigestFromProto(entry.Digest)
		if err != nil {
			return util.StatusWrapf(err, "Failed to obtain digest for file %#v", childPath.String())
		}
		var out fuse.Attr
		initialNodes[component] = re_fuse.InitialNode{
			Leaf: sr.casFileFactory.LookupFile(childDigest, entry.IsExecutable, &out),
		}
	}
	for _, entry := range contents.Symlinks {
		component, ok := path.NewComponent(entry.Name)
		if !ok {
			return status.Errorf(codes.InvalidArgument, "Symlink %#v inside directory %#v has an invalid name", entry.Name, dPath.String())
		}
		initialNodes[component] = re_fuse.InitialNode{
			Leaf: re_fuse.NewSymlink(entry.Target),
		}
	}
	if err := d.CreateChildren(initialNodes, true); err != nil {
		return util.StatusWrap(err, "Failed to create files and symbolic links")
	}
	return nil
}

func (opf *persistentOutputPathFactory) StartInitialBuild(outputBaseID path.Component, casFileFactory re_fuse.CASFileFactory, instanceName digest.InstanceName, errorLogger util.ErrorLogger, inodeNumber uint64) OutputPath {
	d := opf.base.StartInitialBuild(outputBaseID, casFileFactory, instanceName, errorLogger, inodeNumber)

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
			instanceName:   instanceName,
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

func saveDirectoryRecursive(d re_fuse.PrepopulatedDirectory, dPath *path.Trace, w outputpathpersistency.Writer) (*outputpathpersistency_pb.Directory, error) {
	directories, leaves, err := d.LookupAllChildren()
	if err != nil {
		return nil, util.StatusWrapf(err, "Failed to look up children of directory %#v", dPath.String())
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
			return nil, util.StatusWrapf(err, "Failed to write directory %#v to state file", childPath.String())
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
		entry.Child.AppendOutputPathPersistencyDirectoryNode(&directory, entry.Name)
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
