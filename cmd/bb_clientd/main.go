package main

import (
	"context"
	"log"
	"os"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	cd_blobstore "github.com/buildbarn/bb-clientd/pkg/blobstore"
	cd_cas "github.com/buildbarn/bb-clientd/pkg/cas"
	cd_fuse "github.com/buildbarn/bb-clientd/pkg/filesystem/fuse"
	"github.com/buildbarn/bb-clientd/pkg/outputpathpersistency"
	"github.com/buildbarn/bb-clientd/pkg/proto/configuration/bb_clientd"
	re_cas "github.com/buildbarn/bb-remote-execution/pkg/cas"
	re_filesystem "github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	re_fuse "github.com/buildbarn/bb-remote-execution/pkg/filesystem/fuse"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteoutputservice"
	"github.com/buildbarn/bb-storage/pkg/auth"
	blobstore_configuration "github.com/buildbarn/bb-storage/pkg/blobstore/configuration"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcservers"
	"github.com/buildbarn/bb-storage/pkg/builder"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/global"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/random"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/hanwen/go-fuse/v2/fuse"

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

	// Storage access.
	bareContentAddressableStorage, actionCache, err := blobstore_configuration.NewCASAndACBlobAccessFromConfiguration(
		configuration.Blobstore,
		grpcClientFactory,
		int(configuration.MaximumMessageSizeBytes))
	if err != nil {
		log.Fatal(err)
	}

	// Create a demultiplexing build queue that forwards traffic to
	// one or more schedulers specified in the configuration file.
	buildQueue, err := builder.NewDemultiplexingBuildQueueFromConfiguration(
		configuration.Schedulers,
		grpcClientFactory,
		auth.NewStaticAuthorizer(func(instanceName digest.InstanceName) bool { return false }))
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

	// Factories for FUSE nodes corresponding to plain files,
	// executable files, directories and trees.
	//
	// TODO: We should use Caching{Directory,IndexedTree}Fetchers,
	// so that don't call proto.Unmarshal() for every lookup within
	// directory and tree objects. Let's not address this for the
	// time being, as we mainly care about accessing individual
	// files.
	indexedTreeFetcher := cd_cas.NewBlobAccessIndexedTreeFetcher(
		retryingContentAddressableStorage,
		int(configuration.MaximumMessageSizeBytes))
	globalFileContext := NewGlobalFileContext(
		context.Background(),
		retryingContentAddressableStorage,
		util.DefaultErrorLogger)
	globalDirectoryContext := NewGlobalDirectoryContext(
		globalFileContext,
		re_cas.NewBlobAccessDirectoryFetcher(
			retryingContentAddressableStorage,
			int(configuration.MaximumMessageSizeBytes)))
	globalTreeContext := NewGlobalTreeContext(globalFileContext, indexedTreeFetcher)

	// Factory function for per instance name "blobs" directories
	// that give access to arbitrary files, directories and trees.
	blobsDirectoryInodeNumberTree := re_fuse.NewRandomInodeNumberTree()
	commandFileFactory := cd_fuse.NewCommandFileFactory(
		context.Background(),
		retryingContentAddressableStorage,
		int(configuration.MaximumMessageSizeBytes),
		util.DefaultErrorLogger)
	blobsDirectoryLookupFunc := func(instanceName digest.InstanceName, out *fuse.Attr) (re_fuse.Directory, re_fuse.Leaf) {
		inodeNumberTree := blobsDirectoryInodeNumberTree.AddString(instanceName.String())
		commandInodeNumber := inodeNumberTree.AddUint64(0).Get()
		directoryInodeNumber := inodeNumberTree.AddUint64(1).Get()
		executableInodeNumber := inodeNumberTree.AddUint64(2).Get()
		fileInodeNumber := inodeNumberTree.AddUint64(3).Get()
		treeInodeNumber := inodeNumberTree.AddUint64(4).Get()
		d := cd_fuse.NewStaticDirectory(
			inodeNumberTree.Get(),
			map[path.Component]cd_fuse.StaticDirectoryEntry{
				path.MustNewComponent("command"): {
					Child: cd_fuse.NewDigestParsingDirectory(
						instanceName,
						commandInodeNumber,
						func(digest digest.Digest, out *fuse.Attr) (re_fuse.Directory, re_fuse.Leaf, fuse.Status) {
							f, s := commandFileFactory.LookupFile(digest, out)
							return nil, f, s
						}),
					InodeNumber: commandInodeNumber,
				},
				path.MustNewComponent("directory"): {
					Child: cd_fuse.NewDigestParsingDirectory(
						instanceName,
						directoryInodeNumber,
						func(digest digest.Digest, out *fuse.Attr) (re_fuse.Directory, re_fuse.Leaf, fuse.Status) {
							return globalDirectoryContext.LookupDirectory(digest, out), nil, fuse.OK
						}),
					InodeNumber: directoryInodeNumber,
				},
				path.MustNewComponent("executable"): {
					Child: cd_fuse.NewDigestParsingDirectory(
						instanceName,
						executableInodeNumber,
						func(digest digest.Digest, out *fuse.Attr) (re_fuse.Directory, re_fuse.Leaf, fuse.Status) {
							return nil, globalFileContext.LookupFile(digest, true, out), fuse.OK
						}),
					InodeNumber: executableInodeNumber,
				},
				path.MustNewComponent("file"): {
					Child: cd_fuse.NewDigestParsingDirectory(
						instanceName,
						fileInodeNumber,
						func(digest digest.Digest, out *fuse.Attr) (re_fuse.Directory, re_fuse.Leaf, fuse.Status) {
							return nil, globalFileContext.LookupFile(digest, false, out), fuse.OK
						}),
					InodeNumber: fileInodeNumber,
				},
				path.MustNewComponent("tree"): {
					Child: cd_fuse.NewDigestParsingDirectory(
						instanceName,
						treeInodeNumber,
						func(digest digest.Digest, out *fuse.Attr) (re_fuse.Directory, re_fuse.Leaf, fuse.Status) {
							return globalTreeContext.LookupTree(digest, out), nil, fuse.OK
						}),
					InodeNumber: treeInodeNumber,
				},
			})
		d.FUSEGetAttr(out)
		return d, nil
	}

	// Implementation of the Remote Output Service. The Remote
	// Output Service allows Bazel to place its bazel-out/
	// directories on a FUSE file system, thereby allowing data to
	// be loaded lazily.
	var serverCallbacks re_fuse.SimpleRawFileSystemServerCallbacks
	outputPathFactory := cd_fuse.NewInMemoryOutputPathFactory(filePool, serverCallbacks.EntryNotify)
	if persistencyConfiguration := configuration.OutputPathPersistency; persistencyConfiguration != nil {
		// Upload local files at the end of every build. This
		// decorator needs to be added before
		// PersistentOutputPathFactory, as it also ensures that
		// local files have a digest.
		if concurrency := persistencyConfiguration.LocalFileUploadConcurrency; concurrency > 0 {
			outputPathFactory = cd_fuse.NewLocalFileUploadingOutputPathFactory(
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
		outputPathFactory = cd_fuse.NewPersistentOutputPathFactory(
			outputPathFactory,
			outputpathpersistency.NewMaximumAgeStore(
				outputpathpersistency.NewDirectoryBackedStore(
					stateDirectory,
					persistencyConfiguration.MaximumStateFileSizeBytes),
				clock.SystemClock,
				maximumStateFileAge.AsDuration()),
			clock.SystemClock,
			util.DefaultErrorLogger)
	}

	outputsInodeNumber := random.FastThreadSafeGenerator.Uint64()
	outputsDirectory := cd_fuse.NewRemoteOutputServiceDirectory(
		outputsInodeNumber,
		random.NewFastSingleThreadedGenerator(),
		serverCallbacks.EntryNotify,
		outputPathFactory,
		bareContentAddressableStorage,
		retryingContentAddressableStorage,
		indexedTreeFetcher)

	// Construct the top-level directory of the FUSE mount. It contains
	// three subdirectories:
	//
	// - "cas": raw access to the Content Addressable Storage.
	// - "outputs": outputs of builds performed using Bazel.
	// - "scratch": a writable directory for testing.
	rootInodeNumber := random.FastThreadSafeGenerator.Uint64()
	casInodeNumberTree := re_fuse.NewRandomInodeNumberTree()
	scratchInodeNumber := random.FastThreadSafeGenerator.Uint64()
	rootDirectory := cd_fuse.NewStaticDirectory(
		rootInodeNumber,
		map[path.Component]cd_fuse.StaticDirectoryEntry{
			path.MustNewComponent("cas"): {
				Child: cd_fuse.NewInstanceNameParsingDirectory(
					casInodeNumberTree,
					map[path.Component]cd_fuse.InstanceNameLookupFunc{
						path.MustNewComponent("blobs"): blobsDirectoryLookupFunc,
					}),
				InodeNumber: casInodeNumberTree.Get(),
			},
			path.MustNewComponent("outputs"): {
				Child:       outputsDirectory,
				InodeNumber: outputsInodeNumber,
			},
			path.MustNewComponent("scratch"): {
				Child: re_fuse.NewInMemoryPrepopulatedDirectory(
					re_fuse.NewPoolBackedFileAllocator(
						filePool,
						util.DefaultErrorLogger,
						random.FastThreadSafeGenerator),
					util.DefaultErrorLogger,
					scratchInodeNumber,
					random.FastThreadSafeGenerator,
					serverCallbacks.EntryNotify),
				InodeNumber: scratchInodeNumber,
			},
		})

	// Expose the FUSE file system.
	if err := re_fuse.NewMountFromConfiguration(
		configuration.Fuse,
		rootDirectory,
		rootInodeNumber,
		&serverCallbacks,
		"bb_clientd"); err != nil {
		log.Fatal("Failed to mount FUSE file system: ", err)
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
					remoteexecution.RegisterCapabilitiesServer(s, buildQueue)
					remoteexecution.RegisterExecutionServer(s, buildQueue)

					remoteoutputservice.RegisterRemoteOutputServiceServer(s, outputsDirectory)
				}))
	}()

	lifecycleState.MarkReadyAndWait()
}
