package main

import (
	"bytes"
	"context"
	"log"
	"os"
	"sort"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	cd_blobstore "github.com/buildbarn/bb-clientd/pkg/blobstore"
	cd_vfs "github.com/buildbarn/bb-clientd/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-clientd/pkg/outputpathpersistency"
	"github.com/buildbarn/bb-clientd/pkg/proto/configuration/bb_clientd"
	re_cas "github.com/buildbarn/bb-remote-execution/pkg/cas"
	re_filesystem "github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	re_vfs "github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	virtual_configuration "github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual/configuration"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteoutputservice"
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
	"github.com/buildbarn/bb-storage/pkg/random"
	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sync/semaphore"
	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("Usage: bb_clientd bb_clientd.jsonnet")
	}
	var configuration bb_clientd.ApplicationConfiguration
	if err := util.UnmarshalConfigurationFromFile(os.Args[1], &configuration); err != nil {
		log.Fatalf("Failed to read configuration from %s: %s", os.Args[1], err)
	}
	lifecycleState, grpcClientFactory, err := global.ApplyConfiguration(configuration.Global)
	if err != nil {
		log.Fatal("Failed to apply global configuration options: ", err)
	}
	terminationContext, terminationGroup := global.InstallGracefulTerminationHandler()

	// Storage access.
	bareContentAddressableStorage, actionCache, err := blobstore_configuration.NewCASAndACBlobAccessFromConfiguration(
		terminationContext,
		terminationGroup,
		configuration.Blobstore,
		grpcClientFactory,
		int(configuration.MaximumMessageSizeBytes))
	if err != nil {
		log.Fatal(err)
	}

	// Create a demultiplexing build queue that forwards traffic to
	// one or more schedulers specified in the configuration file.
	buildQueue, err := builder.NewDemultiplexingBuildQueueFromConfiguration(configuration.Schedulers, grpcClientFactory)
	if err != nil {
		log.Fatal(err)
	}

	// Storage of files created through the FUSE file system.
	filePool, err := re_filesystem.NewFilePoolFromConfiguration(configuration.FilePool)
	if err != nil {
		log.Fatal("Failed to create file pool: ", err)
	}

	// Separate BlobAccess that does retries in case of read errors.
	// This is necessary, because it isn't always possible to
	// directly propagate I/O errors returned by the FUSE file
	// system to clients.
	retryingContentAddressableStorage := bareContentAddressableStorage
	if maximumDelay := configuration.MaximumFuseRetryDelay; maximumDelay != nil {
		if err := maximumDelay.CheckValid(); err != nil {
			log.Fatal("Invalid maximum FUSE retry delay: ", err)
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
		/* containsSelfMutatingSymlinks = */ false)
	if err != nil {
		log.Fatal("Failed to create virtual file system mount: ", err)
	}

	// Factories for FUSE nodes corresponding to plain files,
	// executable files, directories and trees.
	//
	// We create a CachingDirectoryFetcher for REv2 Directory nodes,
	// as these tend to be loaded repeatedly when traversing a
	// directory hierarchy, especially through "outputs/*".
	directoryFetcher, err := re_cas.NewCachingDirectoryFetcherFromConfiguration(
		configuration.DirectoryCache,
		re_cas.NewBlobAccessDirectoryFetcher(
			retryingContentAddressableStorage,
			int(configuration.MaximumMessageSizeBytes)))
	casFileFactory := re_vfs.NewResolvableHandleAllocatingCASFileFactory(
		re_vfs.NewBlobAccessCASFileFactory(
			context.Background(),
			retryingContentAddressableStorage,
			util.DefaultErrorLogger),
		rootHandleAllocator.New())
	globalDirectoryContext := NewGlobalDirectoryContext(
		context.Background(),
		casFileFactory,
		directoryFetcher,
		rootHandleAllocator.New(),
		util.DefaultErrorLogger)
	globalTreeContext := NewGlobalTreeContext(
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
		return handleAllocator.
			New(bytes.NewBuffer([]byte{0})).
			AsStatelessDirectory(re_vfs.NewStaticDirectory(
				map[path.Component]re_vfs.DirectoryChild{
					path.MustNewComponent("command"): re_vfs.DirectoryChild{}.FromDirectory(
						handleAllocator.
							New(bytes.NewBuffer([]byte{1})).
							AsStatelessDirectory(cd_vfs.NewDigestParsingDirectory(
								instanceName,
								func(digest digest.Digest) (re_vfs.DirectoryChild, re_vfs.Status) {
									f, s := commandFileFactory.LookupFile(digest)
									return re_vfs.DirectoryChild{}.FromLeaf(f), s
								}))),
					path.MustNewComponent("directory"): re_vfs.DirectoryChild{}.FromDirectory(
						handleAllocator.
							New(bytes.NewBuffer([]byte{2})).
							AsStatelessDirectory(cd_vfs.NewDigestParsingDirectory(
								instanceName,
								func(digest digest.Digest) (re_vfs.DirectoryChild, re_vfs.Status) {
									return re_vfs.DirectoryChild{}.FromDirectory(globalDirectoryContext.LookupDirectory(digest)), re_vfs.StatusOK
								}))),
					path.MustNewComponent("executable"): re_vfs.DirectoryChild{}.FromDirectory(
						handleAllocator.
							New(bytes.NewBuffer([]byte{3})).
							AsStatelessDirectory(cd_vfs.NewDigestParsingDirectory(
								instanceName,
								func(digest digest.Digest) (re_vfs.DirectoryChild, re_vfs.Status) {
									return re_vfs.DirectoryChild{}.FromLeaf(casFileFactory.LookupFile(digest, true)), re_vfs.StatusOK
								}))),
					path.MustNewComponent("file"): re_vfs.DirectoryChild{}.FromDirectory(
						handleAllocator.
							New(bytes.NewBuffer([]byte{4})).
							AsStatelessDirectory(cd_vfs.NewDigestParsingDirectory(
								instanceName,
								func(digest digest.Digest) (re_vfs.DirectoryChild, re_vfs.Status) {
									return re_vfs.DirectoryChild{}.FromLeaf(casFileFactory.LookupFile(digest, false)), re_vfs.StatusOK
								}))),
					path.MustNewComponent("tree"): re_vfs.DirectoryChild{}.FromDirectory(
						handleAllocator.
							New(bytes.NewBuffer([]byte{5})).
							AsStatelessDirectory(cd_vfs.NewDigestParsingDirectory(
								instanceName,
								func(digest digest.Digest) (re_vfs.DirectoryChild, re_vfs.Status) {
									return re_vfs.DirectoryChild{}.FromDirectory(globalTreeContext.LookupTree(digest)), re_vfs.StatusOK
								}))),
				}))
	}

	// Implementation of the Remote Output Service. The Remote
	// Output Service allows Bazel to place its bazel-out/
	// directories on a FUSE file system, thereby allowing data to
	// be loaded lazily.
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
		stateDirectory, err := filesystem.NewLocalDirectory(persistencyConfiguration.StateDirectoryPath)
		if err != nil {
			log.Fatalf("Failed to open persistent output path state directory %#v: %s", persistencyConfiguration.StateDirectoryPath, err)
		}
		maximumStateFileAge := persistencyConfiguration.MaximumStateFileAge
		if err := maximumStateFileAge.CheckValid(); err != nil {
			log.Fatal("Invalid maximum state file age: ", err)
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

	outputsDirectory := cd_vfs.NewRemoteOutputServiceDirectory(
		rootHandleAllocator,
		outputPathFactory,
		bareContentAddressableStorage,
		retryingContentAddressableStorage,
		directoryFetcher,
		symlinkFactory,
		configuration.MaximumMessageSizeBytes)

	// Construct the top-level directory of the FUSE mount. It contains
	// three subdirectories:
	//
	// - "cas": raw access to the Content Addressable Storage.
	// - "outputs": outputs of builds performed using Bazel.
	// - "scratch": a writable directory for testing.
	rootDirectory := rootHandleAllocator.New().AsStatelessDirectory(re_vfs.NewStaticDirectory(
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
							util.DefaultErrorLogger),
						rootHandleAllocator),
					symlinkFactory,
					util.DefaultErrorLogger,
					rootHandleAllocator,
					sort.Sort,
					/* hiddenFilesMatcher = */ func(string) bool { return false },
					clock.SystemClock)),
		}))

	if err := mount.Expose(rootDirectory); err != nil {
		log.Fatal("Failed to expose virtual file system mount: ", err)
	}

	// Create a gRPC server that forwards requests to backend clusters.
	go func() {
		log.Fatal(
			"gRPC server failure: ",
			bb_grpc.NewServersFromConfigurationAndServe(
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
							})))
					remoteexecution.RegisterExecutionServer(s, buildQueue)

					remoteoutputservice.RegisterRemoteOutputServiceServer(s, outputsDirectory)
				}))
	}()

	lifecycleState.MarkReadyAndWait()
}
