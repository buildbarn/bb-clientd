package main

import (
	"context"
	"log"
	"os"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	cd_cas "github.com/buildbarn/bb-clientd/pkg/cas"
	cd_fuse "github.com/buildbarn/bb-clientd/pkg/filesystem/fuse"
	"github.com/buildbarn/bb-clientd/pkg/proto/configuration/bb_clientd"
	re_cas "github.com/buildbarn/bb-remote-execution/pkg/cas"
	re_fuse "github.com/buildbarn/bb-remote-execution/pkg/filesystem/fuse"
	blobstore_configuration "github.com/buildbarn/bb-storage/pkg/blobstore/configuration"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcservers"
	"github.com/buildbarn/bb-storage/pkg/builder"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/global"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/hanwen/go-fuse/v2/fuse"

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
	lifecycleState, err := global.ApplyConfiguration(configuration.Global)
	if err != nil {
		log.Fatal("Failed to apply global configuration options: ", err)
	}

	// Storage access.
	contentAddressableStorage, actionCache, err := blobstore_configuration.NewCASAndACBlobAccessFromConfiguration(
		configuration.Blobstore,
		bb_grpc.DefaultClientFactory,
		int(configuration.MaximumMessageSizeBytes))
	if err != nil {
		log.Fatal(err)
	}

	// Create a demultiplexing build queue that forwards traffic to
	// one or more schedulers specified in the configuration file.
	buildQueue, err := builder.NewDemultiplexingBuildQueueFromConfiguration(
		configuration.Schedulers,
		bb_grpc.DefaultClientFactory,
		func(instanceName digest.InstanceName) bool { return false })
	if err != nil {
		log.Fatal(err)
	}

	// Factories for FUSE nodes corresponding to plain files,
	// executable files, directories and trees.
	//
	// TODO: We should use Caching{Directory,IndexedTree}Fetchers,
	// so that don't call proto.Unmarshal() for every lookup within
	// directory and tree objects. Let's not address this for the
	// time being, as we mainly care about accessing individual
	// files.
	globalFileContext := NewGlobalFileContext(
		context.Background(),
		contentAddressableStorage,
		util.DefaultErrorLogger)
	globalDirectoryContext := NewGlobalDirectoryContext(
		globalFileContext,
		re_cas.NewBlobAccessDirectoryFetcher(
			contentAddressableStorage,
			int(configuration.MaximumMessageSizeBytes)))
	globalTreeContext := NewGlobalTreeContext(
		globalFileContext,
		cd_cas.NewBlobAccessIndexedTreeFetcher(
			contentAddressableStorage,
			int(configuration.MaximumMessageSizeBytes)))

	// Factory function for per instance name "blobs" directories
	// that give access to arbitrary files, directories and trees.
	blobsDirectoryInodeNumberTree := re_fuse.NewRandomInodeNumberTree()
	blobsDirectoryLookupFunc := func(instanceName digest.InstanceName, out *fuse.Attr) (re_fuse.Directory, re_fuse.Leaf) {
		inodeNumberTree := blobsDirectoryInodeNumberTree.AddString(instanceName.String())
		directoryInodeNumber := inodeNumberTree.AddUint64(0).Get()
		executableInodeNumber := inodeNumberTree.AddUint64(1).Get()
		fileInodeNumber := inodeNumberTree.AddUint64(2).Get()
		treeInodeNumber := inodeNumberTree.AddUint64(3).Get()
		d := cd_fuse.NewStaticDirectory(
			inodeNumberTree.Get(),
			map[path.Component]cd_fuse.StaticDirectoryEntry{
				path.MustNewComponent("directory"): {
					Child: cd_fuse.NewDigestParsingDirectory(
						instanceName,
						directoryInodeNumber,
						func(digest digest.Digest, out *fuse.Attr) (re_fuse.Directory, re_fuse.Leaf) {
							return globalDirectoryContext.LookupDirectory(digest, out), nil
						}),
					InodeNumber: directoryInodeNumber,
				},
				path.MustNewComponent("executable"): {
					Child: cd_fuse.NewDigestParsingDirectory(
						instanceName,
						executableInodeNumber,
						func(digest digest.Digest, out *fuse.Attr) (re_fuse.Directory, re_fuse.Leaf) {
							return nil, globalFileContext.LookupFile(digest, true, out)
						}),
					InodeNumber: executableInodeNumber,
				},
				path.MustNewComponent("file"): {
					Child: cd_fuse.NewDigestParsingDirectory(
						instanceName,
						fileInodeNumber,
						func(digest digest.Digest, out *fuse.Attr) (re_fuse.Directory, re_fuse.Leaf) {
							return nil, globalFileContext.LookupFile(digest, false, out)
						}),
					InodeNumber: fileInodeNumber,
				},
				path.MustNewComponent("tree"): {
					Child: cd_fuse.NewDigestParsingDirectory(
						instanceName,
						treeInodeNumber,
						func(digest digest.Digest, out *fuse.Attr) (re_fuse.Directory, re_fuse.Leaf) {
							return globalTreeContext.LookupTree(digest, out), nil
						}),
					InodeNumber: treeInodeNumber,
				},
			})
		d.FUSEGetAttr(out)
		return d, nil
	}

	// Top-level directory that parses instance names until a
	// "blobs" pathname component is accessed.
	rootDirectoryInodeNumberTree := re_fuse.NewRandomInodeNumberTree()
	rootDirectory := cd_fuse.NewInstanceNameParsingDirectory(
		rootDirectoryInodeNumberTree,
		map[path.Component]cd_fuse.InstanceNameLookupFunc{
			path.MustNewComponent("blobs"): blobsDirectoryLookupFunc,
		})

	// Expose the FUSE file system.
	var serverCallbacks re_fuse.SimpleRawFileSystemServerCallbacks
	if err := re_fuse.NewMountFromConfiguration(
		configuration.Fuse,
		rootDirectory,
		rootDirectoryInodeNumberTree.Get(),
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
				func(s *grpc.Server) {
					remoteexecution.RegisterActionCacheServer(
						s,
						grpcservers.NewActionCacheServer(
							actionCache,
							int(configuration.MaximumMessageSizeBytes)))
					remoteexecution.RegisterContentAddressableStorageServer(
						s,
						grpcservers.NewContentAddressableStorageServer(
							contentAddressableStorage,
							configuration.MaximumMessageSizeBytes))
					bytestream.RegisterByteStreamServer(
						s,
						grpcservers.NewByteStreamServer(
							contentAddressableStorage,
							1<<16))
					remoteexecution.RegisterCapabilitiesServer(s, buildQueue)
					remoteexecution.RegisterExecutionServer(s, buildQueue)
				}))
	}()

	lifecycleState.MarkReadyAndWait()
}
