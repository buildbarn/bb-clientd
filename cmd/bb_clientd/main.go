package main

import (
	"bytes"
	"context"
	"os"
	"sort"
	"strings"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/bazelbuild/remote-apis/build/bazel/semver"
	cd_blobstore "github.com/buildbarn/bb-clientd/pkg/blobstore"
	cd_vfs "github.com/buildbarn/bb-clientd/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-clientd/pkg/outputpathpersistency"
	"github.com/buildbarn/bb-clientd/pkg/proto/configuration/bb_clientd"
	re_cas "github.com/buildbarn/bb-remote-execution/pkg/cas"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/pool"
	re_vfs "github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	virtual_configuration "github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual/configuration"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/bazeloutputservice"
	blobstore_configuration "github.com/buildbarn/bb-storage/pkg/blobstore/configuration"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcservers"
	"github.com/buildbarn/bb-storage/pkg/builder"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/global"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/program"
	"github.com/buildbarn/bb-storage/pkg/random"
	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sync/semaphore"
	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	program.RunMain(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
		if len(os.Args) != 2 {
			return status.Error(codes.InvalidArgument, "Usage: bb_clientd bb_clientd.jsonnet")
		}
		var configuration bb_clientd.ApplicationConfiguration
		if err := util.UnmarshalConfigurationFromFile(os.Args[1], &configuration); err != nil {
			return util.StatusWrapf(err, "Failed to read configuration from %s", os.Args[1])
		}
		lifecycleState, grpcClientFactory, err := global.ApplyConfiguration(configuration.Global, dependenciesGroup)
		if err != nil {
			return util.StatusWrap(err, "Failed to apply global configuration options")
		}

		// Storage access.
		bareContentAddressableStorage, actionCache, err := blobstore_configuration.NewCASAndACBlobAccessFromConfiguration(
			dependenciesGroup,
			configuration.Blobstore,
			grpcClientFactory,
			int(configuration.MaximumMessageSizeBytes))
		if err != nil {
			return err
		}

		// Create a demultiplexing build queue that forwards traffic to
		// one or more schedulers specified in the configuration file.
		buildQueue, err := builder.NewDemultiplexingBuildQueueFromConfiguration(configuration.Schedulers, dependenciesGroup, grpcClientFactory)
		if err != nil {
			return err
		}

		// Storage of files created through the virtual file system.
		filePool, err := pool.NewFilePoolFromConfiguration(configuration.FilePool)
		if err != nil {
			return util.StatusWrap(err, "Failed to create file pool")
		}

		// Separate BlobAccess that does retries in case of read errors.
		// This is necessary, because it isn't always possible to
		// directly propagate I/O errors returned by the virtual file
		// system to clients.
		retryingContentAddressableStorage := bareContentAddressableStorage
		if maximumDelay := configuration.MaximumFileSystemRetryDelay; maximumDelay != nil {
			if err := maximumDelay.CheckValid(); err != nil {
				return util.StatusWrap(err, "Invalid maximum file system retry delay")
			}
			retryingContentAddressableStorage = cd_blobstore.NewErrorRetryingBlobAccess(
				bareContentAddressableStorage,
				clock.SystemClock,
				random.FastThreadSafeGenerator,
				util.DefaultErrorLogger,
				time.Second,
				30*time.Second,
				maximumDelay.AsDuration())
		}

		// Create the virtual file system.
		mount, rootHandleAllocator, err := virtual_configuration.NewMountFromConfiguration(
			configuration.Mount,
			"bb_clientd",
			/* rootDirectory = */ virtual_configuration.LongAttributeCaching,
			/* childDirectories = */ virtual_configuration.NoAttributeCaching,
			/* leaves = */ virtual_configuration.LongAttributeCaching,
			/* caseSensitive = */ true,
		)
		if err != nil {
			return util.StatusWrap(err, "Failed to create virtual file system mount")
		}

		// Factories for virtual file system nodes corresponding to
		// plain files, executable files, directories and trees.
		//
		// We create a CachingDirectoryFetcher for REv2 Directory nodes,
		// as these tend to be loaded repeatedly when traversing a
		// directory hierarchy, especially through "outputs/*".
		directoryFetcher, err := re_cas.NewCachingDirectoryFetcherFromConfiguration(
			configuration.DirectoryCache,
			re_cas.NewBlobAccessDirectoryFetcher(
				retryingContentAddressableStorage,
				int(configuration.MaximumMessageSizeBytes),
				configuration.MaximumTreeSizeBytes))
		if err != nil {
			return util.StatusWrap(err, "Failed to create caching directory fetcher")
		}
		casFileFactory := re_vfs.NewResolvableHandleAllocatingCASFileFactory(
			re_vfs.NewBlobAccessCASFileFactory(
				context.Background(),
				retryingContentAddressableStorage,
				util.DefaultErrorLogger),
			rootHandleAllocator.New())
		decomposedCASDirectoryFactory := cd_vfs.NewDecomposedCASDirectoryFactory(
			context.Background(),
			casFileFactory,
			directoryFetcher,
			rootHandleAllocator.New(),
			util.DefaultErrorLogger)
		treeCASDirectoryFactory := cd_vfs.NewTreeCASDirectoryFactory(
			context.Background(),
			casFileFactory,
			directoryFetcher,
			rootHandleAllocator.New(),
			util.DefaultErrorLogger)

		// Factory function for per instance name "blobs" directories
		// that give access to arbitrary files, directories and trees.
		blobsDirectoryHandleAllocator := rootHandleAllocator.New().AsStatelessAllocator()
		commandFileFactory := cd_vfs.NewHandleAllocatingCommandFileFactory(
			cd_vfs.NewBlobAccessCommandFileFactory(
				context.Background(),
				retryingContentAddressableStorage,
				int(configuration.MaximumMessageSizeBytes),
				util.DefaultErrorLogger),
			rootHandleAllocator.New())
		blobsDirectoryLookupFunc := func(instanceName digest.InstanceName) re_vfs.Directory {
			handleAllocator := blobsDirectoryHandleAllocator.
				New(re_vfs.ByteSliceID([]byte(instanceName.String()))).
				AsStatelessAllocator()
			var allocationCounter byte
			allocateHandle := func() re_vfs.StatelessHandleAllocation {
				allocationCounter++
				return handleAllocator.New(bytes.NewBuffer([]byte{allocationCounter}))
			}
			blobsDirectoryContents := map[path.Component]re_vfs.DirectoryChild{}
			for _, digestFunctionValue := range digest.SupportedDigestFunctions {
				digestFunction, err := instanceName.GetDigestFunction(digestFunctionValue, 0)
				if err != nil {
					panic("Using a supported digest function should always succeed")
				}
				blobsDirectoryContents[path.MustNewComponent(strings.ToLower(digestFunctionValue.String()))] = re_vfs.DirectoryChild{}.FromDirectory(
					allocateHandle().AsStatelessDirectory(re_vfs.NewStaticDirectory(
						re_vfs.CaseSensitiveComponentNormalizer,
						map[path.Component]re_vfs.DirectoryChild{
							path.MustNewComponent("command"): re_vfs.DirectoryChild{}.FromDirectory(
								allocateHandle().AsStatelessDirectory(cd_vfs.NewDigestParsingDirectory(
									digestFunction,
									func(digest digest.Digest) (re_vfs.DirectoryChild, re_vfs.Status) {
										f, s := commandFileFactory.LookupFile(digest)
										return re_vfs.DirectoryChild{}.FromLeaf(f), s
									}))),
							path.MustNewComponent("directory"): re_vfs.DirectoryChild{}.FromDirectory(
								allocateHandle().AsStatelessDirectory(cd_vfs.NewDigestParsingDirectory(
									digestFunction,
									func(digest digest.Digest) (re_vfs.DirectoryChild, re_vfs.Status) {
										return re_vfs.DirectoryChild{}.FromDirectory(decomposedCASDirectoryFactory.LookupDirectory(digest)), re_vfs.StatusOK
									}))),
							path.MustNewComponent("executable"): re_vfs.DirectoryChild{}.FromDirectory(
								allocateHandle().AsStatelessDirectory(cd_vfs.NewDigestParsingDirectory(
									digestFunction,
									func(digest digest.Digest) (re_vfs.DirectoryChild, re_vfs.Status) {
										return re_vfs.DirectoryChild{}.FromLeaf(casFileFactory.LookupFile(digest, true, nil)), re_vfs.StatusOK
									}))),
							path.MustNewComponent("file"): re_vfs.DirectoryChild{}.FromDirectory(
								allocateHandle().AsStatelessDirectory(cd_vfs.NewDigestParsingDirectory(
									digestFunction,
									func(digest digest.Digest) (re_vfs.DirectoryChild, re_vfs.Status) {
										return re_vfs.DirectoryChild{}.FromLeaf(casFileFactory.LookupFile(digest, false, nil)), re_vfs.StatusOK
									}))),
							path.MustNewComponent("tree"): re_vfs.DirectoryChild{}.FromDirectory(
								allocateHandle().AsStatelessDirectory(cd_vfs.NewDigestParsingDirectory(
									digestFunction,
									func(digest digest.Digest) (re_vfs.DirectoryChild, re_vfs.Status) {
										return re_vfs.DirectoryChild{}.FromDirectory(treeCASDirectoryFactory.LookupDirectory(digest)), re_vfs.StatusOK
									}))),
						})))
			}
			return allocateHandle().AsStatelessDirectory(re_vfs.NewStaticDirectory(re_vfs.CaseSensitiveComponentNormalizer, blobsDirectoryContents))
		}

		// Implementation of the Bazel Output Service. The Bazel
		// Output Service allows Bazel to place its bazel-out/
		// directories on a virtual file system, thereby
		// allowing data to be loaded lazily.
		symlinkFactory := re_vfs.NewHandleAllocatingSymlinkFactory(
			re_vfs.BaseSymlinkFactory,
			rootHandleAllocator.New())
		outputPathFactory := cd_vfs.NewInMemoryOutputPathFactory(filePool, symlinkFactory, rootHandleAllocator, sort.Sort, clock.SystemClock)
		if persistencyConfiguration := configuration.OutputPathPersistency; persistencyConfiguration != nil {
			// Upload local files at the end of every build. This
			// decorator needs to be added before
			// PersistentOutputPathFactory, as it also ensures that
			// local files have a digest.
			if concurrency := persistencyConfiguration.LocalFileUploadConcurrency; concurrency > 0 {
				outputPathFactory = cd_vfs.NewLocalFileUploadingOutputPathFactory(
					outputPathFactory,
					bareContentAddressableStorage,
					util.DefaultErrorLogger,
					semaphore.NewWeighted(concurrency))
			}

			// Enable persistent storage of bazel-out/ directories.
			stateDirectory, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(persistencyConfiguration.StateDirectoryPath))
			if err != nil {
				return util.StatusWrapf(err, "Failed to open persistent output path state directory %#v", persistencyConfiguration.StateDirectoryPath)
			}
			maximumStateFileAge := persistencyConfiguration.MaximumStateFileAge
			if err := maximumStateFileAge.CheckValid(); err != nil {
				return util.StatusWrap(err, "Invalid maximum state file age")
			}
			outputPathFactory = cd_vfs.NewPersistentOutputPathFactory(
				outputPathFactory,
				outputpathpersistency.NewMaximumAgeStore(
					outputpathpersistency.NewDirectoryBackedStore(
						stateDirectory,
						persistencyConfiguration.MaximumStateFileSizeBytes),
					clock.SystemClock,
					maximumStateFileAge.AsDuration()),
				clock.SystemClock,
				util.DefaultErrorLogger,
				symlinkFactory)
		}

		outputsDirectory := cd_vfs.NewBazelOutputServiceDirectory(
			rootHandleAllocator,
			outputPathFactory,
			bareContentAddressableStorage,
			retryingContentAddressableStorage,
			directoryFetcher,
			symlinkFactory,
			configuration.MaximumTreeSizeBytes)

		// Construct the top-level directory of the virtual file system
		// mount. It contains three subdirectories:
		//
		// - "cas": raw access to the Content Addressable Storage.
		// - "outputs": outputs of builds performed using Bazel.
		// - "scratch": a writable directory for testing.
		defaultAttributesSetter := func(requested re_vfs.AttributesMask, attributes *re_vfs.Attributes) {}
		namedAttributesFactory := re_vfs.NewInMemoryNamedAttributesFactory(
			re_vfs.NewHandleAllocatingFileAllocator(
				re_vfs.NewPoolBackedFileAllocator(
					filePool,
					util.DefaultErrorLogger,
					defaultAttributesSetter,
					re_vfs.InNamedAttributeDirectoryNamedAttributesFactory,
				),
				rootHandleAllocator,
			),
			symlinkFactory,
			util.DefaultErrorLogger,
			rootHandleAllocator,
			clock.SystemClock,
		)
		rootDirectory := rootHandleAllocator.New().AsStatelessDirectory(re_vfs.NewStaticDirectory(
			re_vfs.CaseSensitiveComponentNormalizer,
			map[path.Component]re_vfs.DirectoryChild{
				path.MustNewComponent("cas"): re_vfs.DirectoryChild{}.FromDirectory(
					cd_vfs.NewInstanceNameParsingDirectory(
						rootHandleAllocator.New(),
						map[path.Component]cd_vfs.InstanceNameLookupFunc{
							path.MustNewComponent("blobs"): blobsDirectoryLookupFunc,
						})),
				path.MustNewComponent("outputs"): re_vfs.DirectoryChild{}.FromDirectory(outputsDirectory),
				path.MustNewComponent("scratch"): re_vfs.DirectoryChild{}.FromDirectory(
					re_vfs.NewInMemoryPrepopulatedDirectory(
						re_vfs.NewHandleAllocatingFileAllocator(
							re_vfs.NewPoolBackedFileAllocator(
								filePool,
								util.DefaultErrorLogger,
								defaultAttributesSetter,
								namedAttributesFactory,
							),
							rootHandleAllocator,
						),
						symlinkFactory,
						util.DefaultErrorLogger,
						rootHandleAllocator,
						sort.Sort,
						/* hiddenFilesMatcher = */ func(string) bool { return false },
						clock.SystemClock,
						re_vfs.CaseSensitiveComponentNormalizer,
						defaultAttributesSetter,
						namedAttributesFactory,
					),
				),
			}))

		if err := mount.Expose(siblingsGroup, rootDirectory); err != nil {
			return util.StatusWrap(err, "Failed to expose virtual file system mount")
		}

		// Create a gRPC server that forwards requests to backend clusters.
		if err := bb_grpc.NewServersFromConfigurationAndServe(
			configuration.GrpcServers,
			func(s grpc.ServiceRegistrar) {
				remoteexecution.RegisterActionCacheServer(
					s,
					grpcservers.NewActionCacheServer(
						actionCache,
						int(configuration.MaximumMessageSizeBytes)))
				remoteexecution.RegisterContentAddressableStorageServer(
					s,
					grpcservers.NewContentAddressableStorageServer(
						bareContentAddressableStorage,
						configuration.MaximumMessageSizeBytes))
				bytestream.RegisterByteStreamServer(
					s,
					grpcservers.NewByteStreamServer(
						bareContentAddressableStorage,
						1<<16))
				remoteexecution.RegisterCapabilitiesServer(
					s,
					capabilities.NewServer(
						capabilities.NewMergingProvider([]capabilities.Provider{
							bareContentAddressableStorage,
							actionCache,
							buildQueue,
							capabilities.NewStaticProvider(&remoteexecution.ServerCapabilities{
								DeprecatedApiVersion: &semver.SemVer{Major: 2, Minor: 0},
								LowApiVersion:        &semver.SemVer{Major: 2, Minor: 0},
								HighApiVersion:       &semver.SemVer{Major: 2, Minor: 11},
							}),
						})))
				remoteexecution.RegisterExecutionServer(s, buildQueue)

				bazeloutputservice.RegisterBazelOutputServiceServer(s, outputsDirectory)
			},
			siblingsGroup,
			grpcClientFactory,
		); err != nil {
			return util.StatusWrap(err, "gRPC server failure")
		}

		lifecycleState.MarkReadyAndWait(siblingsGroup)
		return nil
	})
}
