package virtual

import (
	"context"
	"sync"
	"syscall"

	cd_cas "github.com/buildbarn/bb-clientd/pkg/cas"
	re_cas "github.com/buildbarn/bb-remote-execution/pkg/cas"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/bazeloutputservice"
	bazeloutputservicerev2 "github.com/buildbarn/bb-remote-execution/pkg/proto/bazeloutputservice/rev2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type buildState struct {
	id                 string
	digestFunction     digest.Function
	scopeWalkerFactory *path.VirtualRootScopeWalkerFactory
}

type outputPathState struct {
	buildState     *buildState
	rootDirectory  OutputPath
	casFileFactory virtual.CASFileFactory

	// Circular linked list, used by VirtualReadDir(). By only
	// inserting new output paths at the end and ensuring that
	// cookies are monotonically increasing, we can reliably perform
	// partial reads against this directory.
	previous     *outputPathState
	next         *outputPathState
	cookie       uint64
	outputBaseID path.Component
}

// BazelOutputServiceDirectory is FUSE directory that acts as the
// top-level directory for Bazel Output Service. The Bazel Output
// Service can be used by build clients to efficiently populate a
// directory with build outputs.
//
// In addition to acting as a FUSE directory, this type also implements
// a gRPC server for the Bazel Output Service. This gRPC service can be
// used to start and finalize builds, but also to perform bulk creation
// and stat() operations.
//
// This implementation of the Bazel Output Service is relatively simple:
//
//   - There is no persistency of build information across restarts.
//   - No snapshotting of completed builds takes place, meaning that only
//     the results of the latest build of a given output base are exposed.
//   - Every output path is backed by an InMemoryPrepopulatedDirectory,
//     meaning that memory usage may be high.
//   - No automatic garbage collection of old output paths is performed.
//
// This implementation should eventually be extended to address the
// issues listed above.
type BazelOutputServiceDirectory struct {
	virtual.ReadOnlyDirectory

	handleAllocator                   virtual.StatefulHandleAllocator
	handle                            virtual.StatefulDirectoryHandle
	outputPathFactory                 OutputPathFactory
	bareContentAddressableStorage     blobstore.BlobAccess
	retryingContentAddressableStorage blobstore.BlobAccess
	directoryFetcher                  re_cas.DirectoryFetcher
	symlinkFactory                    virtual.SymlinkFactory
	maximumTreeSizeBytes              int64

	lock          sync.Mutex
	changeID      uint64
	outputBaseIDs map[path.Component]*outputPathState
	buildIDs      map[string]*outputPathState
	outputPaths   outputPathState
}

var (
	_ virtual.Directory                           = &BazelOutputServiceDirectory{}
	_ bazeloutputservice.BazelOutputServiceServer = &BazelOutputServiceDirectory{}
)

// NewBazelOutputServiceDirectory creates a new instance of
// BazelOutputServiceDirectory.
func NewBazelOutputServiceDirectory(handleAllocator virtual.StatefulHandleAllocator, outputPathFactory OutputPathFactory, bareContentAddressableStorage, retryingContentAddressableStorage blobstore.BlobAccess, directoryFetcher re_cas.DirectoryFetcher, symlinkFactory virtual.SymlinkFactory, maximumTreeSizeBytes int64) *BazelOutputServiceDirectory {
	d := &BazelOutputServiceDirectory{
		handleAllocator:                   handleAllocator,
		outputPathFactory:                 outputPathFactory,
		bareContentAddressableStorage:     bareContentAddressableStorage,
		retryingContentAddressableStorage: retryingContentAddressableStorage,
		directoryFetcher:                  directoryFetcher,
		symlinkFactory:                    symlinkFactory,
		maximumTreeSizeBytes:              maximumTreeSizeBytes,

		outputBaseIDs: map[path.Component]*outputPathState{},
		buildIDs:      map[string]*outputPathState{},
	}
	d.handle = handleAllocator.New().AsStatefulDirectory(d)
	d.outputPaths.previous = &d.outputPaths
	d.outputPaths.next = &d.outputPaths
	return d
}

// Clean all build outputs associated with a single output base.
func (d *BazelOutputServiceDirectory) Clean(ctx context.Context, request *bazeloutputservice.CleanRequest) (*bazeloutputservice.CleanResponse, error) {
	outputBaseID, ok := path.NewComponent(request.OutputBaseId)
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "Output base ID is not a valid filename")
	}

	d.lock.Lock()
	outputPathState, ok := d.outputBaseIDs[outputBaseID]
	d.lock.Unlock()
	if ok {
		// Remove all data stored inside the output path. This
		// must be done without holding the directory lock, as
		// NotifyRemoval() calls generated by the output path
		// could deadlock otherwise.
		if err := outputPathState.rootDirectory.RemoveAllChildren(true); err != nil {
			return nil, err
		}

		d.lock.Lock()
		if outputPathState == d.outputBaseIDs[outputBaseID] {
			delete(d.outputBaseIDs, outputBaseID)
			outputPathState.previous.next = outputPathState.next
			outputPathState.next.previous = outputPathState.previous
			d.changeID++
			if buildState := outputPathState.buildState; buildState != nil {
				delete(d.buildIDs, buildState.id)
				outputPathState.buildState = nil
			}
		}
		d.lock.Unlock()

		d.handle.NotifyRemoval(outputBaseID)
	} else if err := d.outputPathFactory.Clean(outputBaseID); err != nil {
		// This output path hasn't been accessed since startup.
		// It may be the case that there is persistent state
		// associated with this output path, so make sure that
		// is removed as well.
		return nil, err
	}
	return &bazeloutputservice.CleanResponse{}, nil
}

// findMissingAndRemove is called during StartBuild() to remove a single
// batch of files from the output path that are no longer present in the
// Content Addressable Storage.
func (d *BazelOutputServiceDirectory) findMissingAndRemove(ctx context.Context, queue map[digest.Digest][]func() error) error {
	set := digest.NewSetBuilder()
	for digest := range queue {
		set.Add(digest)
	}
	missing, err := d.bareContentAddressableStorage.FindMissing(ctx, set.Build())
	if err != nil {
		return util.StatusWrap(err, "Failed to find missing blobs")
	}
	for _, digest := range missing.Items() {
		for _, removeFunc := range queue[digest] {
			if err := removeFunc(); err != nil {
				return util.StatusWrapf(err, "Failed to remove file with digest %#v", digest.String())
			}
		}
	}
	return nil
}

// filterMissingChildren is called during StartBuild() to traverse over
// all files in the output path, calling FindMissingBlobs() on them to
// ensure that they will not disappear during the build. Any files that
// are missing are removed from the output path.
func (d *BazelOutputServiceDirectory) filterMissingChildren(ctx context.Context, rootDirectory virtual.PrepopulatedDirectory, digestFunction digest.Function) error {
	queue := map[digest.Digest][]func() error{}
	var savedErr error
	if err := rootDirectory.FilterChildren(func(node virtual.InitialNode, removeFunc virtual.ChildRemover) bool {
		// Obtain the transitive closure of digests on which
		// this file or directory depends.
		var digests digest.Set
		if directory, leaf := node.GetPair(); leaf != nil {
			digests = leaf.GetContainingDigests()
		} else if digests, savedErr = directory.GetContainingDigests(ctx); savedErr != nil {
			// Can't compute the set of digests underneath
			// this directory. Remove the directory
			// entirely.
			if status.Code(savedErr) == codes.NotFound {
				savedErr = nil
				if err := removeFunc(); err != nil {
					savedErr = util.StatusWrap(err, "Failed to remove non-existent directory")
					return false
				}
				return true
			}
			return false
		}

		// Remove files that use a different instance name or
		// digest function. It may be technically valid to
		// retain these, but it comes at the cost of requiring
		// the build client to copy files between clusters, or
		// reupload them with a different hash. This may be
		// slower than requiring a rebuild.
		for _, blobDigest := range digests.Items() {
			if !blobDigest.UsesDigestFunction(digestFunction) {
				if err := removeFunc(); err != nil {
					savedErr = util.StatusWrapf(err, "Failed to remove file with different instance name or digest function with digest %#v", blobDigest.String())
					return false
				}
				return true
			}
		}

		for _, blobDigest := range digests.Items() {
			if len(queue) >= blobstore.RecommendedFindMissingDigestsCount {
				// Maximum number of digests reached.
				savedErr = d.findMissingAndRemove(ctx, queue)
				if savedErr != nil {
					return false
				}
				queue = map[digest.Digest][]func() error{}
			}
			queue[blobDigest] = append(queue[blobDigest], removeFunc)
		}
		return true
	}); err != nil {
		return err
	}
	if savedErr != nil {
		return savedErr
	}

	// Process the final batch of files.
	if len(queue) > 0 {
		return d.findMissingAndRemove(ctx, queue)
	}
	return nil
}

// StartBuild is called by a build client to indicate that a new build
// in a given output base is starting.
func (d *BazelOutputServiceDirectory) StartBuild(ctx context.Context, request *bazeloutputservice.StartBuildRequest) (*bazeloutputservice.StartBuildResponse, error) {
	// Compute the full output path and the output path suffix. The
	// former needs to be used by us, while the latter is
	// communicated back to the client.
	outputPath, scopeWalker := path.EmptyBuilder.Join(path.NewAbsoluteScopeWalker(path.VoidComponentWalker))
	if err := path.Resolve(request.OutputPathPrefix, scopeWalker); err != nil {
		return nil, util.StatusWrap(err, "Failed to resolve output path prefix")
	}
	outputPathSuffix, scopeWalker := path.EmptyBuilder.Join(path.VoidScopeWalker)
	outputPath, scopeWalker = outputPath.Join(scopeWalker)
	outputBaseID, ok := path.NewComponent(request.OutputBaseId)
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "Output base ID is not a valid filename")
	}
	componentWalker, err := scopeWalker.OnScope(false)
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to resolve output path")
	}
	if _, err := componentWalker.OnTerminal(outputBaseID); err != nil {
		return nil, util.StatusWrap(err, "Failed to resolve output path")
	}

	// Create a virtual root based on the output path and provided
	// aliases. This will be used to properly resolve targets of
	// symbolic links stored in the output path.
	scopeWalkerFactory, err := path.NewVirtualRootScopeWalkerFactory(outputPath.String(), request.OutputPathAliases)
	if err != nil {
		return nil, err
	}

	var args bazeloutputservicerev2.StartBuildArgs
	if err := request.Args.UnmarshalTo(&args); err != nil {
		return nil, util.StatusWrap(err, "Failed to unmarshal start build arguments")
	}

	instanceName, err := digest.NewInstanceName(args.InstanceName)
	if err != nil {
		return nil, util.StatusWrapf(err, "Failed to parse instance name %#v", args.InstanceName)
	}
	digestFunction, err := instanceName.GetDigestFunction(args.DigestFunction, 0)
	if err != nil {
		return nil, err
	}

	d.lock.Lock()
	state, ok := d.buildIDs[request.BuildId]
	if !ok {
		state, ok = d.outputBaseIDs[outputBaseID]
		if ok {
			if buildState := state.buildState; buildState != nil {
				// A previous build is running that wasn't
				// finalized properly. Forcefully finalize it.
				delete(d.buildIDs, buildState.id)
				state.buildState = nil
			}
		} else {
			// No previous builds have been run for this
			// output base. Create a new output path.
			//
			// TODO: This should not use DefaultErrorLogger.
			// Instead, we should capture errors, so that we
			// can propagate them back to the build client.
			// This allows the client to retry, or at least
			// display the error immediately, so that users
			// don't need to check logs.
			errorLogger := util.DefaultErrorLogger
			casFileFactory := virtual.NewStatelessHandleAllocatingCASFileFactory(
				virtual.NewBlobAccessCASFileFactory(
					context.Background(),
					d.retryingContentAddressableStorage,
					errorLogger),
				d.handleAllocator.New())
			state = &outputPathState{
				rootDirectory:  d.outputPathFactory.StartInitialBuild(outputBaseID, casFileFactory, digestFunction, errorLogger),
				casFileFactory: casFileFactory,

				previous:     d.outputPaths.previous,
				next:         &d.outputPaths,
				cookie:       d.changeID,
				outputBaseID: outputBaseID,
			}
			d.outputBaseIDs[outputBaseID] = state
			state.previous.next = state
			state.next.previous = state
			d.changeID++
		}

		// Allow StageArtifacts() and BatchStat() requests for
		// the new build ID.
		state.buildState = &buildState{
			id:                 request.BuildId,
			digestFunction:     digestFunction,
			scopeWalkerFactory: scopeWalkerFactory,
		}
		d.buildIDs[request.BuildId] = state
	}
	d.lock.Unlock()

	// Call ContentAddressableStorage.FindMissingBlobs() on all of
	// the files and tree objects contained within the output path,
	// so that we have the certainty that they don't disappear
	// during the build. Remove all of the files and directories
	// that are missing, so that the client can detect their absence
	// and rebuild them.
	if err := d.filterMissingChildren(ctx, state.rootDirectory, digestFunction); err != nil {
		return nil, util.StatusWrap(err, "Failed to filter contents of the output path")
	}

	return &bazeloutputservice.StartBuildResponse{
		// TODO: Fill in InitialOutputPathContents, so that the
		// client can skip parts of its analysis. The easiest
		// way to achieve this would be to freeze the contents
		// of the output path between builds.
		OutputPathSuffix: outputPathSuffix.String(),
	}, nil
}

// getOutputPathAndBuildState returns the state objects associated with
// a given build ID. This function is used by all gRPC methods that can
// only be invoked as part of a build (e.g., StageArtifacts(),
// BatchStat()).
func (d *BazelOutputServiceDirectory) getOutputPathAndBuildState(buildID string) (*outputPathState, *buildState, error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	outputPathState, ok := d.buildIDs[buildID]
	if !ok {
		return nil, nil, status.Error(codes.FailedPrecondition, "Build ID is not associated with any running build")
	}
	return outputPathState, outputPathState.buildState, nil
}

// parentDirectoryCreatingComponentWalker is an implementation of
// ComponentWalker that is used by StageArtifacts() to resolve the
// parent directory of the path where a file, directory or symlink needs
// to be created.
type parentDirectoryCreatingComponentWalker struct {
	path.TerminalNameTrackingComponentWalker
	stack util.NonEmptyStack[virtual.PrepopulatedDirectory]
}

func (cw *parentDirectoryCreatingComponentWalker) OnDirectory(name path.Component) (path.GotDirectoryOrSymlink, error) {
	child, err := cw.stack.Peek().CreateAndEnterPrepopulatedDirectory(name)
	if err != nil {
		return nil, err
	}
	cw.stack.Push(child)
	return path.GotDirectory{
		Child:        cw,
		IsReversible: true,
	}, nil
}

func (cw *parentDirectoryCreatingComponentWalker) OnUp() (path.ComponentWalker, error) {
	if _, ok := cw.stack.PopSingle(); !ok {
		return nil, status.Error(codes.InvalidArgument, "Path resolves to a location outside the output path")
	}
	return cw, nil
}

func (d *BazelOutputServiceDirectory) stageSingleArtifact(ctx context.Context, artifact *bazeloutputservice.StageArtifactsRequest_Artifact, outputPathState *outputPathState, buildState *buildState) error {
	// Resolve the parent directory and filename of the artifact to create.
	outputParentCreator := parentDirectoryCreatingComponentWalker{
		stack: util.NewNonEmptyStack[virtual.PrepopulatedDirectory](outputPathState.rootDirectory),
	}
	if err := path.Resolve(artifact.Path, path.NewRelativeScopeWalker(&outputParentCreator)); err != nil {
		return util.StatusWrap(err, "Failed to resolve path")
	}
	name := outputParentCreator.TerminalName
	if name == nil {
		return status.Errorf(codes.InvalidArgument, "Path resolves to a directory")
	}

	anyLocator, err := artifact.Locator.UnmarshalNew()
	if err != nil {
		return util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to unmarshal locator")
	}
	switch locator := anyLocator.(type) {
	case *bazeloutputservicerev2.FileArtifactLocator:
		// Client wants to create a lazy loading file.
		childDigest, err := buildState.digestFunction.NewDigestFromProto(locator.Digest)
		if err != nil {
			return util.StatusWrap(err, "Invalid digest")
		}

		return outputParentCreator.stack.Peek().CreateChildren(
			map[path.Component]virtual.InitialNode{
				*name: virtual.InitialNode{}.FromLeaf(
					outputPathState.casFileFactory.LookupFile(
						childDigest,
						/* isExecutable = */ true,
						/* fileReadMonitorFactory = */ nil,
					),
				),
			},
			/* overwrite = */ true,
		)
	case *bazeloutputservicerev2.TreeArtifactLocator:
		// Client wants to create a lazy loading directory.
		childDigest, err := buildState.digestFunction.NewDigestFromProto(locator.TreeDigest)
		if err != nil {
			return util.StatusWrap(err, "Invalid tree digest")
		}
		if sizeBytes := childDigest.GetSizeBytes(); sizeBytes > d.maximumTreeSizeBytes {
			return status.Errorf(codes.InvalidArgument, "Directory is %d bytes in size, which exceeds the permitted maximum of %d bytes", sizeBytes, d.maximumTreeSizeBytes)
		}
		var rootDirectoryDigest *digest.Digest
		if locator.RootDirectoryDigest != nil {
			d, err := buildState.digestFunction.NewDigestFromProto(locator.RootDirectoryDigest)
			if err != nil {
				return util.StatusWrap(err, "Invalid root directory digest")
			}
			rootDirectoryDigest = &d
		}

		return outputParentCreator.stack.Peek().CreateChildren(
			map[path.Component]virtual.InitialNode{
				*name: virtual.InitialNode{}.FromDirectory(
					virtual.NewCASInitialContentsFetcher(
						context.Background(),
						cd_cas.NewTreeDirectoryWalker(d.directoryFetcher, childDigest, rootDirectoryDigest),
						outputPathState.casFileFactory,
						d.symlinkFactory,
						buildState.digestFunction,
					),
				),
			},
			/* overwrite = */ true,
		)
	default:
		return status.Error(codes.InvalidArgument, "Locator is of an unknown type")
	}
}

// StageArtifacts can be called by a build client to create files and
// directories.
//
// Because files and directories are provided in the form of REv2
// digests, this implementation is capable of creating files and
// directories whose contents get loaded from the Content Addressable
// Storage lazily.
func (d *BazelOutputServiceDirectory) StageArtifacts(ctx context.Context, request *bazeloutputservice.StageArtifactsRequest) (*bazeloutputservice.StageArtifactsResponse, error) {
	outputPathState, buildState, err := d.getOutputPathAndBuildState(request.BuildId)
	if err != nil {
		return nil, err
	}

	responses := make([]*bazeloutputservice.StageArtifactsResponse_Response, 0, len(request.Artifacts))
	for _, artifact := range request.Artifacts {
		responses = append(
			responses,
			&bazeloutputservice.StageArtifactsResponse_Response{
				Status: status.Convert(d.stageSingleArtifact(ctx, artifact, outputPathState, buildState)).Proto(),
			})
	}
	return &bazeloutputservice.StageArtifactsResponse{
		Responses: responses,
	}, nil
}

// statWalker is an implementation of ScopeWalker and ComponentWalker
// that is used by BatchStat() to resolve the file or directory
// corresponding to a requested path. It is capable of expanding
// symbolic links, if encountered.
type statWalker struct {
	digestFunction *digest.Function

	stack util.NonEmptyStack[virtual.PrepopulatedDirectory]
	stat  *bazeloutputservice.BatchStatResponse_Stat
}

func (cw *statWalker) OnScope(absolute bool) (path.ComponentWalker, error) {
	if absolute {
		cw.stack.PopAll()
	}
	// Currently in a known directory.
	cw.stat = &bazeloutputservice.BatchStatResponse_Stat{
		Type: &bazeloutputservice.BatchStatResponse_Stat_Directory_{},
	}
	return cw, nil
}

func (cw *statWalker) OnDirectory(name path.Component) (path.GotDirectoryOrSymlink, error) {
	child, err := cw.stack.Peek().LookupChild(name)
	if err != nil {
		return nil, err
	}

	directory, leaf := child.GetPair()
	if directory != nil {
		// Got a directory.
		cw.stack.Push(directory)
		return path.GotDirectory{
			Child:        cw,
			IsReversible: true,
		}, nil
	}

	target, err := leaf.Readlink()
	if err == syscall.EINVAL {
		return nil, syscall.ENOTDIR
	} else if err != nil {
		return nil, err
	}

	// Got a symbolic link in the middle of a path. Those should
	// always be followed.
	cw.stat = &bazeloutputservice.BatchStatResponse_Stat{}
	return path.GotSymlink{
		Parent: cw,
		Target: target,
	}, nil
}

func (cw *statWalker) OnTerminal(name path.Component) (*path.GotSymlink, error) {
	child, err := cw.stack.Peek().LookupChild(name)
	if err != nil {
		return nil, err
	}

	directory, leaf := child.GetPair()
	if directory != nil {
		// Got a directory. The existing Stat is sufficient.
		cw.stack.Push(directory)
		return nil, nil
	}

	stat, err := leaf.GetBazelOutputServiceStat(cw.digestFunction)
	if err != nil {
		return nil, err
	}
	cw.stat = stat
	return nil, nil
}

func (cw *statWalker) OnUp() (path.ComponentWalker, error) {
	if _, ok := cw.stack.PopSingle(); !ok {
		cw.stat = &bazeloutputservice.BatchStatResponse_Stat{}
		return path.VoidComponentWalker, nil
	}
	return cw, nil
}

// BatchStat can be called by a build client to obtain the status of
// files and directories.
//
// Calling this method over gRPC may be far more efficient than
// obtaining this information through the FUSE file system, as batching
// significantly reduces the amount of context switching. It also
// prevents the computation of digests for files for which the digest is
// already known.
func (d *BazelOutputServiceDirectory) BatchStat(ctx context.Context, request *bazeloutputservice.BatchStatRequest) (*bazeloutputservice.BatchStatResponse, error) {
	outputPathState, buildState, err := d.getOutputPathAndBuildState(request.BuildId)
	if err != nil {
		return nil, err
	}

	response := bazeloutputservice.BatchStatResponse{
		Responses: make([]*bazeloutputservice.BatchStatResponse_StatResponse, 0, len(request.Paths)),
	}
	for _, statPath := range request.Paths {
		statWalker := statWalker{
			digestFunction: &buildState.digestFunction,
			stack:          util.NewNonEmptyStack[virtual.PrepopulatedDirectory](outputPathState.rootDirectory),
			stat:           &bazeloutputservice.BatchStatResponse_Stat{},
		}
		resolvedPath, scopeWalker := path.EmptyBuilder.Join(
			buildState.scopeWalkerFactory.New(path.NewLoopDetectingScopeWalker(&statWalker)))
		if err := path.Resolve(statPath, scopeWalker); err == syscall.ENOENT {
			// Path does not exist.
			response.Responses = append(response.Responses, &bazeloutputservice.BatchStatResponse_StatResponse{})
		} else if err != nil {
			// Some other error occurred.
			return nil, util.StatusWrapf(err, "Failed to resolve path %#v beyond %#v", statPath, resolvedPath.String())
		} else {
			response.Responses = append(response.Responses, &bazeloutputservice.BatchStatResponse_StatResponse{
				Stat: statWalker.stat,
			})
		}
	}
	return &response, nil
}

// FinalizeArtifacts can be called by a build client to indicate that
// files are no longer expected to be modified by the build client. If
// the file is modified after finalization, it may be reported through
// InitialOutputPathContents.
func (d *BazelOutputServiceDirectory) FinalizeArtifacts(ctx context.Context, request *bazeloutputservice.FinalizeArtifactsRequest) (*bazeloutputservice.FinalizeArtifactsResponse, error) {
	// TODO: Properly track which artifacts are finalized, so that
	// we can accurately report which files are modified.
	return &bazeloutputservice.FinalizeArtifactsResponse{}, nil
}

// FinalizeBuild can be called by a build client to indicate the current
// build has completed. This prevents successive StageArtifacts() and
// BatchStat() calls from being processed.
func (d *BazelOutputServiceDirectory) FinalizeBuild(ctx context.Context, request *bazeloutputservice.FinalizeBuildRequest) (*bazeloutputservice.FinalizeBuildResponse, error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	// Silently ignore requests for unknown build IDs. This ensures
	// that FinalizeBuild() remains idempotent.
	if outputPathState, ok := d.buildIDs[request.BuildId]; ok {
		buildState := outputPathState.buildState
		outputPathState.rootDirectory.FinalizeBuild(ctx, buildState.digestFunction)
		delete(d.buildIDs, buildState.id)
		outputPathState.buildState = nil
	}
	return &bazeloutputservice.FinalizeBuildResponse{}, nil
}

// VirtualGetAttributes returns the attributes of the root directory of
// the Bazel Output Service.
func (d *BazelOutputServiceDirectory) VirtualGetAttributes(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
	attributes.SetFileType(filesystem.FileTypeDirectory)
	attributes.SetPermissions(virtual.PermissionsRead | virtual.PermissionsExecute)
	attributes.SetSizeBytes(0)
	if requested&(virtual.AttributesMaskChangeID|virtual.AttributesMaskLinkCount) != 0 {
		d.lock.Lock()
		attributes.SetChangeID(d.changeID)
		attributes.SetLinkCount(virtual.EmptyDirectoryLinkCount + uint32(len(d.outputBaseIDs)))
		d.lock.Unlock()
	}
	d.handle.GetAttributes(requested, attributes)
}

// VirtualLookup can be used to look up the root directory of an output
// path for a given output base.
func (d *BazelOutputServiceDirectory) VirtualLookup(ctx context.Context, name path.Component, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.DirectoryChild, virtual.Status) {
	d.lock.Lock()
	outputPathState, ok := d.outputBaseIDs[name]
	d.lock.Unlock()
	if !ok {
		return virtual.DirectoryChild{}, virtual.StatusErrNoEnt
	}
	outputPathState.rootDirectory.VirtualGetAttributes(ctx, requested, out)
	return virtual.DirectoryChild{}.FromDirectory(outputPathState.rootDirectory), virtual.StatusOK
}

// VirtualOpenChild can be used to open or create a file in the root
// directory of the Bazel Output Service. Because this directory never
// contains any files, this function is guaranteed to fail.
func (d *BazelOutputServiceDirectory) VirtualOpenChild(ctx context.Context, name path.Component, shareAccess virtual.ShareMask, createAttributes *virtual.Attributes, existingOptions *virtual.OpenExistingOptions, requested virtual.AttributesMask, openedFileAttributes *virtual.Attributes) (virtual.Leaf, virtual.AttributesMask, virtual.ChangeInfo, virtual.Status) {
	d.lock.Lock()
	_, ok := d.outputBaseIDs[name]
	d.lock.Unlock()
	if ok {
		return virtual.ReadOnlyDirectoryOpenChildWrongFileType(existingOptions, virtual.StatusErrIsDir)
	}
	return virtual.ReadOnlyDirectoryOpenChildDoesntExist(createAttributes)
}

// VirtualReadDir returns a list of all the output paths managed by this
// Bazel Output Service.
func (d *BazelOutputServiceDirectory) VirtualReadDir(ctx context.Context, firstCookie uint64, requested virtual.AttributesMask, reporter virtual.DirectoryEntryReporter) virtual.Status {
	d.lock.Lock()
	defer d.lock.Unlock()

	// Find the first output path past the provided cookie.
	outputPathState := d.outputPaths.next
	for {
		if outputPathState == &d.outputPaths {
			return virtual.StatusOK
		}
		if outputPathState.cookie >= firstCookie {
			break
		}
		outputPathState = outputPathState.next
	}

	// Return information for the remaining output paths.
	for ; outputPathState != &d.outputPaths; outputPathState = outputPathState.next {
		child := outputPathState.rootDirectory
		var attributes virtual.Attributes
		child.VirtualGetAttributes(ctx, requested, &attributes)
		if !reporter.ReportEntry(outputPathState.cookie+1, outputPathState.outputBaseID, virtual.DirectoryChild{}.FromDirectory(child), &attributes) {
			break
		}
	}
	return virtual.StatusOK
}
