package virtual_test

import (
	"context"
	"syscall"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-clientd/internal/mock"
	cd_vfs "github.com/buildbarn/bb-clientd/pkg/filesystem/virtual"
	re_vfs "github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteoutputservice"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestRemoteOutputServiceDirectoryClean(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	handleAllocator := mock.NewMockStatefulHandleAllocator(ctrl)
	outputPathFactory := mock.NewMockOutputPathFactory(ctrl)
	bareContentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	retryingContentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	indexedTreeFetcher := mock.NewMockIndexedTreeFetcher(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	dHandleAllocation := mock.NewMockStatefulHandleAllocation(ctrl)
	handleAllocator.EXPECT().New().Return(dHandleAllocation)
	dHandle := mock.NewMockStatefulDirectoryHandle(ctrl)
	dHandleAllocation.EXPECT().AsStatefulDirectory(gomock.Any()).Return(dHandle)
	d := cd_vfs.NewRemoteOutputServiceDirectory(
		handleAllocator,
		outputPathFactory,
		bareContentAddressableStorage,
		retryingContentAddressableStorage,
		indexedTreeFetcher,
		symlinkFactory)

	t.Run("InvalidOutputBaseID", func(t *testing.T) {
		// The output base ID must be a valid directory name.
		_, err := d.Clean(ctx, &remoteoutputservice.CleanRequest{
			OutputBaseId: "..",
		})
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Output base ID is not a valid filename"), err)
	})

	t.Run("NonexistentOutputPath", func(t *testing.T) {
		// The output base ID hasn't been used since startup,
		// but there may be persistent data associated with it.
		// Ensure this persistent data is removed, to ensure
		// that the next call to StartBuild() yields an empty
		// output path.
		outputPathFactory.EXPECT().Clean(path.MustNewComponent("9e6defb5a0a8a7af63077e0623279b78"))

		_, err := d.Clean(ctx, &remoteoutputservice.CleanRequest{
			OutputBaseId: "9e6defb5a0a8a7af63077e0623279b78",
		})
		require.NoError(t, err)
	})

	t.Run("ExistentOutputPath", func(t *testing.T) {
		// Create an output path.
		casFileHandleAllocation := mock.NewMockStatefulHandleAllocation(ctrl)
		handleAllocator.EXPECT().New().Return(casFileHandleAllocation)
		casFileHandleAllocator := mock.NewMockStatelessHandleAllocator(ctrl)
		casFileHandleAllocation.EXPECT().AsStatelessAllocator().Return(casFileHandleAllocator)
		outputPath := mock.NewMockOutputPath(ctrl)
		outputPathFactory.EXPECT().StartInitialBuild(path.MustNewComponent("a448da900e7bd4b025ab91da2aba6244"), gomock.Any(), digest.EmptyInstanceName, gomock.Any()).Return(outputPath)
		outputPath.EXPECT().FilterChildren(gomock.Any())

		response, err := d.StartBuild(ctx, &remoteoutputservice.StartBuildRequest{
			OutputBaseId:     "a448da900e7bd4b025ab91da2aba6244",
			BuildId:          "37f5dbef-b117-4fb6-bce8-5c147cb603b4",
			DigestFunction:   remoteexecution.DigestFunction_SHA256,
			OutputPathPrefix: "/home/bob/bb_clientd/outputs",
			OutputPathAliases: map[string]string{
				"/home/bob/.cache/bazel/_bazel_bob/a448da900e7bd4b025ab91da2aba6244/execroot/myproject/bazel-out": ".",
			},
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &remoteoutputservice.StartBuildResponse{
			OutputPathSuffix: "a448da900e7bd4b025ab91da2aba6244",
		}, response)

		// Simulate the case where an I/O error occurs while
		// removing the files and directories contained within
		// the output path.
		outputPath.EXPECT().RemoveAllChildren(true).Return(status.Error(codes.Internal, "Disk on fire"))
		_, err = d.Clean(ctx, &remoteoutputservice.CleanRequest{
			OutputBaseId: "a448da900e7bd4b025ab91da2aba6244",
		})
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Disk on fire"), err)

		// A second removal attempt succeeds.
		outputPath.EXPECT().RemoveAllChildren(true)
		dHandle.EXPECT().NotifyRemoval(path.MustNewComponent("a448da900e7bd4b025ab91da2aba6244"))
		_, err = d.Clean(ctx, &remoteoutputservice.CleanRequest{
			OutputBaseId: "a448da900e7bd4b025ab91da2aba6244",
		})
		require.NoError(t, err)

		// Successive attempts should request the removal of
		// persistent state.
		outputPathFactory.EXPECT().Clean(path.MustNewComponent("a448da900e7bd4b025ab91da2aba6244"))
		_, err = d.Clean(ctx, &remoteoutputservice.CleanRequest{
			OutputBaseId: "a448da900e7bd4b025ab91da2aba6244",
		})
		require.NoError(t, err)
	})
}

func TestRemoteOutputServiceDirectoryStartBuild(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	handleAllocator := mock.NewMockStatefulHandleAllocator(ctrl)
	outputPathFactory := mock.NewMockOutputPathFactory(ctrl)
	bareContentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	retryingContentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	indexedTreeFetcher := mock.NewMockIndexedTreeFetcher(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	dHandleAllocation := mock.NewMockStatefulHandleAllocation(ctrl)
	handleAllocator.EXPECT().New().Return(dHandleAllocation)
	dHandle := mock.NewMockStatefulDirectoryHandle(ctrl)
	dHandleAllocation.EXPECT().AsStatefulDirectory(gomock.Any()).Return(dHandle)
	d := cd_vfs.NewRemoteOutputServiceDirectory(
		handleAllocator,
		outputPathFactory,
		bareContentAddressableStorage,
		retryingContentAddressableStorage,
		indexedTreeFetcher,
		symlinkFactory)

	t.Run("InvalidOutputBaseID", func(t *testing.T) {
		// The output base ID must be a valid directory name.
		_, err := d.StartBuild(ctx, &remoteoutputservice.StartBuildRequest{
			OutputBaseId:     "//////",
			BuildId:          "37f5dbef-b117-4fb6-bce8-5c147cb603b4",
			DigestFunction:   remoteexecution.DigestFunction_SHA256,
			OutputPathPrefix: "/home/bob/bb_clientd/outputs",
			OutputPathAliases: map[string]string{
				"/home/bob/.cache/bazel/_bazel_bob/a448da900e7bd4b025ab91da2aba6244/execroot/myproject/bazel-out": ".",
			},
		})
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Output base ID is not a valid filename"), err)
	})

	t.Run("InvalidOutputPathPrefix", func(t *testing.T) {
		// The output path prefix must be absolute.
		_, err := d.StartBuild(ctx, &remoteoutputservice.StartBuildRequest{
			OutputBaseId:     "9da951b8cb759233037166e28f7ea186",
			BuildId:          "37f5dbef-b117-4fb6-bce8-5c147cb603b4",
			DigestFunction:   remoteexecution.DigestFunction_SHA256,
			OutputPathPrefix: "relative/path",
			OutputPathAliases: map[string]string{
				"/home/bob/.cache/bazel/_bazel_bob/a448da900e7bd4b025ab91da2aba6244/execroot/myproject/bazel-out": ".",
			},
		})
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Failed to resolve output path prefix: Path is relative, while an absolute path was expected"), err)
	})

	t.Run("InvalidOutputPathAliases", func(t *testing.T) {
		// Output path aliases must also be absolute.
		_, err := d.StartBuild(ctx, &remoteoutputservice.StartBuildRequest{
			OutputBaseId:     "9da951b8cb759233037166e28f7ea186",
			BuildId:          "37f5dbef-b117-4fb6-bce8-5c147cb603b4",
			DigestFunction:   remoteexecution.DigestFunction_SHA256,
			OutputPathPrefix: "/home/bob/bb_clientd/outputs",
			OutputPathAliases: map[string]string{
				"relative/path": ".",
			},
		})
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Failed to resolve alias path \"relative/path\": Path is relative, while an absolute path was expected"), err)
	})

	t.Run("InvalidDigestFunction", func(t *testing.T) {
		// Digest function is not supported by this implementation.
		_, err := d.StartBuild(ctx, &remoteoutputservice.StartBuildRequest{
			OutputBaseId:     "9da951b8cb759233037166e28f7ea186",
			BuildId:          "37f5dbef-b117-4fb6-bce8-5c147cb603b4",
			DigestFunction:   remoteexecution.DigestFunction_UNKNOWN,
			OutputPathPrefix: "/home/bob/bb_clientd/outputs",
			OutputPathAliases: map[string]string{
				"/home/bob/.cache/bazel/_bazel_bob/a448da900e7bd4b025ab91da2aba6244/execroot/myproject/bazel-out": ".",
			},
		})
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Unknown digest function"), err)
	})

	t.Run("InitialSuccess", func(t *testing.T) {
		// An initial successful call should create the output
		// path directory.
		casFileHandleAllocation := mock.NewMockStatefulHandleAllocation(ctrl)
		handleAllocator.EXPECT().New().Return(casFileHandleAllocation)
		casFileHandleAllocator := mock.NewMockStatelessHandleAllocator(ctrl)
		casFileHandleAllocation.EXPECT().AsStatelessAllocator().Return(casFileHandleAllocator)
		outputPath := mock.NewMockOutputPath(ctrl)
		outputPathFactory.EXPECT().StartInitialBuild(path.MustNewComponent("9da951b8cb759233037166e28f7ea186"), gomock.Any(), digest.MustNewInstanceName("my-cluster"), gomock.Any()).Return(outputPath)
		outputPath.EXPECT().FilterChildren(gomock.Any())

		response, err := d.StartBuild(ctx, &remoteoutputservice.StartBuildRequest{
			OutputBaseId:     "9da951b8cb759233037166e28f7ea186",
			BuildId:          "37f5dbef-b117-4fb6-bce8-5c147cb603b4",
			InstanceName:     "my-cluster",
			DigestFunction:   remoteexecution.DigestFunction_SHA256,
			OutputPathPrefix: "/home/bob/bb_clientd/outputs",
			OutputPathAliases: map[string]string{
				"/home/bob/.cache/bazel/_bazel_bob/a448da900e7bd4b025ab91da2aba6244/execroot/myproject/bazel-out": ".",
			},
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &remoteoutputservice.StartBuildResponse{
			OutputPathSuffix: "9da951b8cb759233037166e28f7ea186",
		}, response)

		t.Run("FilterChildrenFailure", func(t *testing.T) {
			// Starting a second build should not create a
			// new output path directory. It should,
			// however, perform a scan against the file
			// system to remove files that are no longer
			// present remotely. Let this fail.
			outputPath.EXPECT().FilterChildren(gomock.Any()).Return(status.Error(codes.Internal, "Failed to read directory contents"))

			_, err = d.StartBuild(ctx, &remoteoutputservice.StartBuildRequest{
				OutputBaseId:     "9da951b8cb759233037166e28f7ea186",
				BuildId:          "2e3fd15a-f2ae-4855-ac69-bdd4a0ef7339",
				InstanceName:     "my-cluster",
				DigestFunction:   remoteexecution.DigestFunction_SHA256,
				OutputPathPrefix: "/home/bob/bb_clientd/outputs",
				OutputPathAliases: map[string]string{
					"/home/bob/.cache/bazel/_bazel_bob/a448da900e7bd4b025ab91da2aba6244/execroot/myproject/bazel-out": ".",
				},
			})
			testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to filter contents of the output path: Failed to read directory contents"), err)
		})

		t.Run("DirectoryGetContainingDigestsFailure", func(t *testing.T) {
			// Simulate the case where we can't check the
			// completeness of an uninitialized directory,
			// because its contents cannot be loaded from
			// the Content Addressable Storage.
			outputPath.EXPECT().FilterChildren(gomock.Any()).DoAndReturn(func(childFilter re_vfs.ChildFilter) error {
				child := mock.NewMockInitialContentsFetcher(ctrl)
				child.EXPECT().GetContainingDigests(ctx).Return(digest.EmptySet, status.Error(codes.Unavailable, "Tree \"4fb75adebd02251c9663125582e51102\": CAS unavailable"))
				remover := mock.NewMockChildRemover(ctrl)
				require.False(t, childFilter(re_vfs.InitialNode{Directory: child}, remover.Call))
				return nil
			})

			_, err = d.StartBuild(ctx, &remoteoutputservice.StartBuildRequest{
				OutputBaseId:     "9da951b8cb759233037166e28f7ea186",
				BuildId:          "2e3fd15a-f2ae-4855-ac69-bdd4a0ef7339",
				InstanceName:     "my-cluster",
				DigestFunction:   remoteexecution.DigestFunction_SHA256,
				OutputPathPrefix: "/home/bob/bb_clientd/outputs",
				OutputPathAliases: map[string]string{
					"/home/bob/.cache/bazel/_bazel_bob/a448da900e7bd4b025ab91da2aba6244/execroot/myproject/bazel-out": ".",
				},
			})
			testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "Failed to filter contents of the output path: Tree \"4fb75adebd02251c9663125582e51102\": CAS unavailable"), err)
		})

		t.Run("DirectoryMissingAndRemoveFailure", func(t *testing.T) {
			// Simulate the case where an uninitialized
			// directory cannot be loaded, because it's
			// absent, and removing its contents locally
			// fails due to local storage errors.
			outputPath.EXPECT().FilterChildren(gomock.Any()).DoAndReturn(func(childFilter re_vfs.ChildFilter) error {
				child := mock.NewMockInitialContentsFetcher(ctrl)
				child.EXPECT().GetContainingDigests(ctx).Return(digest.EmptySet, status.Error(codes.NotFound, "Tree \"4fb75adebd02251c9663125582e51102\": Object not found"))
				remover := mock.NewMockChildRemover(ctrl)
				remover.EXPECT().Call().Return(status.Error(codes.Internal, "Disk on fire"))
				require.False(t, childFilter(re_vfs.InitialNode{Directory: child}, remover.Call))
				return nil
			})

			_, err = d.StartBuild(ctx, &remoteoutputservice.StartBuildRequest{
				OutputBaseId:     "9da951b8cb759233037166e28f7ea186",
				BuildId:          "2e3fd15a-f2ae-4855-ac69-bdd4a0ef7339",
				InstanceName:     "my-cluster",
				DigestFunction:   remoteexecution.DigestFunction_SHA256,
				OutputPathPrefix: "/home/bob/bb_clientd/outputs",
				OutputPathAliases: map[string]string{
					"/home/bob/.cache/bazel/_bazel_bob/a448da900e7bd4b025ab91da2aba6244/execroot/myproject/bazel-out": ".",
				},
			})
			testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to filter contents of the output path: Failed to remove non-existent directory: Disk on fire"), err)
		})

		t.Run("InstanceNameMismatchAndRemoveFailure", func(t *testing.T) {
			// Simulate the case where a file in the output
			// path uses a different instance name. This
			// file should be removed to prevent unnecessary
			// copying of files between clusters.
			outputPath.EXPECT().FilterChildren(gomock.Any()).DoAndReturn(func(childFilter re_vfs.ChildFilter) error {
				child := mock.NewMockNativeLeaf(ctrl)
				child.EXPECT().GetContainingDigests().
					Return(digest.MustNewDigest("some-other-cluster", "338db227a0de09b4309e928cdbb7d40a", 42).ToSingletonSet())
				remover := mock.NewMockChildRemover(ctrl)
				remover.EXPECT().Call().Return(status.Error(codes.Internal, "Disk on fire"))
				require.False(t, childFilter(re_vfs.InitialNode{Leaf: child}, remover.Call))
				return nil
			})

			_, err = d.StartBuild(ctx, &remoteoutputservice.StartBuildRequest{
				OutputBaseId:     "9da951b8cb759233037166e28f7ea186",
				BuildId:          "2e3fd15a-f2ae-4855-ac69-bdd4a0ef7339",
				InstanceName:     "my-cluster",
				DigestFunction:   remoteexecution.DigestFunction_SHA256,
				OutputPathPrefix: "/home/bob/bb_clientd/outputs",
				OutputPathAliases: map[string]string{
					"/home/bob/.cache/bazel/_bazel_bob/a448da900e7bd4b025ab91da2aba6244/execroot/myproject/bazel-out": ".",
				},
			})
			testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to filter contents of the output path: Failed to remove file with different instance name or digest function with digest \"338db227a0de09b4309e928cdbb7d40a-42-some-other-cluster\": Disk on fire"), err)
		})

		t.Run("FindMissingFailure", func(t *testing.T) {
			// Successfully gathered digests of files stored
			// in the output, but failed to check for their
			// existence remotely.
			digests := digest.MustNewDigest("my-cluster", "338db227a0de09b4309e928cdbb7d40a", 42).ToSingletonSet()
			outputPath.EXPECT().FilterChildren(gomock.Any()).DoAndReturn(func(childFilter re_vfs.ChildFilter) error {
				child := mock.NewMockNativeLeaf(ctrl)
				child.EXPECT().GetContainingDigests().Return(digests)
				remover := mock.NewMockChildRemover(ctrl)
				require.True(t, childFilter(re_vfs.InitialNode{Leaf: child}, remover.Call))
				return nil
			})
			bareContentAddressableStorage.EXPECT().FindMissing(ctx, digests).
				Return(digest.EmptySet, status.Error(codes.Unavailable, "CAS unavailable"))

			_, err = d.StartBuild(ctx, &remoteoutputservice.StartBuildRequest{
				OutputBaseId:     "9da951b8cb759233037166e28f7ea186",
				BuildId:          "2e3fd15a-f2ae-4855-ac69-bdd4a0ef7339",
				InstanceName:     "my-cluster",
				DigestFunction:   remoteexecution.DigestFunction_MD5,
				OutputPathPrefix: "/home/bob/bb_clientd/outputs",
				OutputPathAliases: map[string]string{
					"/home/bob/.cache/bazel/_bazel_bob/a448da900e7bd4b025ab91da2aba6244/execroot/myproject/bazel-out": ".",
				},
			})
			testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "Failed to filter contents of the output path: Failed to find missing blobs: CAS unavailable"), err)
		})

		t.Run("RemoveFailure", func(t *testing.T) {
			// Successfully determined that a file is absent
			// remotely, but failed to remove it from local
			// storage.
			digests := digest.MustNewDigest("my-cluster", "338db227a0de09b4309e928cdbb7d40a", 42).ToSingletonSet()
			remover := mock.NewMockChildRemover(ctrl)
			outputPath.EXPECT().FilterChildren(gomock.Any()).DoAndReturn(func(childFilter re_vfs.ChildFilter) error {
				child := mock.NewMockNativeLeaf(ctrl)
				child.EXPECT().GetContainingDigests().Return(digests)
				require.True(t, childFilter(re_vfs.InitialNode{Leaf: child}, remover.Call))
				return nil
			})
			bareContentAddressableStorage.EXPECT().FindMissing(ctx, digests).Return(digests, nil)
			remover.EXPECT().Call().Return(status.Error(codes.Internal, "Disk on fire"))

			_, err = d.StartBuild(ctx, &remoteoutputservice.StartBuildRequest{
				OutputBaseId:     "9da951b8cb759233037166e28f7ea186",
				BuildId:          "2e3fd15a-f2ae-4855-ac69-bdd4a0ef7339",
				InstanceName:     "my-cluster",
				DigestFunction:   remoteexecution.DigestFunction_MD5,
				OutputPathPrefix: "/home/bob/bb_clientd/outputs",
				OutputPathAliases: map[string]string{
					"/home/bob/.cache/bazel/_bazel_bob/a448da900e7bd4b025ab91da2aba6244/execroot/myproject/bazel-out": ".",
				},
			})
			testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to filter contents of the output path: Failed to remove file with digest \"338db227a0de09b4309e928cdbb7d40a-42-my-cluster\": Disk on fire"), err)
		})

		t.Run("Success", func(t *testing.T) {
			remover4 := mock.NewMockChildRemover(ctrl)
			remover7 := mock.NewMockChildRemover(ctrl)
			outputPath.EXPECT().FilterChildren(gomock.Any()).DoAndReturn(func(childFilter re_vfs.ChildFilter) error {
				// Serve a file that uses a different instance
				// name. It should get removed immediately.
				child1 := mock.NewMockNativeLeaf(ctrl)
				child1.EXPECT().GetContainingDigests().
					Return(digest.MustNewDigest("other-instance-name", "3ec839e3d5d0af404c6dc6bf3ff7f2eb", 1).ToSingletonSet())
				remover1 := mock.NewMockChildRemover(ctrl)
				remover1.EXPECT().Call()
				require.True(t, childFilter(re_vfs.InitialNode{Leaf: child1}, remover1.Call))

				// Serve a file that uses a different
				// hashing algorithm. It should also get
				// removed immediately.
				child2 := mock.NewMockNativeLeaf(ctrl)
				child2.EXPECT().GetContainingDigests().
					Return(digest.MustNewDigest("my-cluster", "f11999245771a5c184b62dc5380e0d8b42df67b4", 2).ToSingletonSet())
				remover2 := mock.NewMockChildRemover(ctrl)
				remover2.EXPECT().Call()
				require.True(t, childFilter(re_vfs.InitialNode{Leaf: child2}, remover2.Call))

				// A file which we'll later report as present.
				child3 := mock.NewMockNativeLeaf(ctrl)
				child3.EXPECT().GetContainingDigests().
					Return(digest.MustNewDigest("my-cluster", "a32ea15346cf1848ab49e0913ff07531", 3).ToSingletonSet())
				remover3 := mock.NewMockChildRemover(ctrl)
				require.True(t, childFilter(re_vfs.InitialNode{Leaf: child3}, remover3.Call))

				// A file which we'll later report as missing.
				child4 := mock.NewMockNativeLeaf(ctrl)
				child4.EXPECT().GetContainingDigests().
					Return(digest.MustNewDigest("my-cluster", "9435918583fd2e37882751bbc51f4085", 4).ToSingletonSet())
				require.True(t, childFilter(re_vfs.InitialNode{Leaf: child4}, remover4.Call))

				// A directory that no longer exists. It
				// should be removed immediately.
				child5 := mock.NewMockInitialContentsFetcher(ctrl)
				child5.EXPECT().GetContainingDigests(ctx).Return(digest.EmptySet, status.Error(codes.NotFound, "Tree \"4fb75adebd02251c9663125582e51102\": Object not found"))
				remover5 := mock.NewMockChildRemover(ctrl)
				remover5.EXPECT().Call()
				require.True(t, childFilter(re_vfs.InitialNode{Directory: child5}, remover5.Call))

				// A directory for which all files exist.
				child6 := mock.NewMockInitialContentsFetcher(ctrl)
				child6.EXPECT().GetContainingDigests(ctx).Return(
					digest.NewSetBuilder().
						Add(digest.MustNewDigest("my-cluster", "23fef0c2a3414dd562ca70e4a4717609", 5)).
						Add(digest.MustNewDigest("my-cluster", "a60ffc49592e5045a61a8c99f3c86b4f", 6)).
						Build(),
					nil)
				remover6 := mock.NewMockChildRemover(ctrl)
				require.True(t, childFilter(re_vfs.InitialNode{Directory: child6}, remover6.Call))

				// A directory for which one file does not
				// exist. It should be removed later on.
				child7 := mock.NewMockInitialContentsFetcher(ctrl)
				child7.EXPECT().GetContainingDigests(ctx).Return(
					digest.NewSetBuilder().
						Add(digest.MustNewDigest("my-cluster", "2c0f843d40e00603f0d71e0d11a6e045", 7)).
						Add(digest.MustNewDigest("my-cluster", "6b9105a7125cb9f190a3e44ab5f22663", 8)).
						Build(),
					nil)
				require.True(t, childFilter(re_vfs.InitialNode{Directory: child7}, remover7.Call))
				return nil
			})

			// We should see a call to FindMissing() for all
			// of the files and directories that weren't
			// removed immediately. The file and directory
			// corresponding to the ones reported as missing
			// should be removed afterwards.
			bareContentAddressableStorage.EXPECT().FindMissing(
				ctx,
				digest.NewSetBuilder().
					Add(digest.MustNewDigest("my-cluster", "a32ea15346cf1848ab49e0913ff07531", 3)).
					Add(digest.MustNewDigest("my-cluster", "9435918583fd2e37882751bbc51f4085", 4)).
					Add(digest.MustNewDigest("my-cluster", "23fef0c2a3414dd562ca70e4a4717609", 5)).
					Add(digest.MustNewDigest("my-cluster", "a60ffc49592e5045a61a8c99f3c86b4f", 6)).
					Add(digest.MustNewDigest("my-cluster", "2c0f843d40e00603f0d71e0d11a6e045", 7)).
					Add(digest.MustNewDigest("my-cluster", "6b9105a7125cb9f190a3e44ab5f22663", 8)).
					Build(),
			).Return(
				digest.NewSetBuilder().
					Add(digest.MustNewDigest("my-cluster", "9435918583fd2e37882751bbc51f4085", 4)).
					Add(digest.MustNewDigest("my-cluster", "2c0f843d40e00603f0d71e0d11a6e045", 7)).
					Build(),
				nil)
			remover4.EXPECT().Call()
			remover7.EXPECT().Call()

			_, err = d.StartBuild(ctx, &remoteoutputservice.StartBuildRequest{
				OutputBaseId:     "9da951b8cb759233037166e28f7ea186",
				BuildId:          "2e3fd15a-f2ae-4855-ac69-bdd4a0ef7339",
				InstanceName:     "my-cluster",
				DigestFunction:   remoteexecution.DigestFunction_MD5,
				OutputPathPrefix: "/home/bob/bb_clientd/outputs",
				OutputPathAliases: map[string]string{
					"/home/bob/.cache/bazel/_bazel_bob/a448da900e7bd4b025ab91da2aba6244/execroot/myproject/bazel-out": ".",
				},
			})
			require.NoError(t, err)
		})
	})
}

func TestRemoteOutputServiceDirectoryBatchCreate(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	handleAllocator := mock.NewMockStatefulHandleAllocator(ctrl)
	outputPathFactory := mock.NewMockOutputPathFactory(ctrl)
	bareContentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	retryingContentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	indexedTreeFetcher := mock.NewMockIndexedTreeFetcher(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	dHandleAllocation := mock.NewMockStatefulHandleAllocation(ctrl)
	handleAllocator.EXPECT().New().Return(dHandleAllocation)
	dHandle := mock.NewMockStatefulDirectoryHandle(ctrl)
	dHandleAllocation.EXPECT().AsStatefulDirectory(gomock.Any()).Return(dHandle)
	d := cd_vfs.NewRemoteOutputServiceDirectory(
		handleAllocator,
		outputPathFactory,
		bareContentAddressableStorage,
		retryingContentAddressableStorage,
		indexedTreeFetcher,
		symlinkFactory)

	t.Run("InvalidBuildID", func(t *testing.T) {
		// StartBuild() should be called first.
		_, err := d.BatchCreate(ctx, &remoteoutputservice.BatchCreateRequest{
			BuildId: "ad778a53-48e6-4ae1-b1f5-01b84a508f5f",
			Files: []*remoteexecution.OutputFile{
				{
					Path: "foo.o",
					Digest: &remoteexecution.Digest{
						Hash:      "d0ab620af7f3e77f3adfa190d41a25ce",
						SizeBytes: 123,
					},
				},
			},
		})
		testutil.RequireEqualStatus(t, status.Error(codes.FailedPrecondition, "Build ID is not associated with any running build"), err)
	})

	// Let the remainder of the tests assume that a build is running.
	casFileHandleAllocation := mock.NewMockStatefulHandleAllocation(ctrl)
	handleAllocator.EXPECT().New().Return(casFileHandleAllocation)
	casFileHandleAllocator := mock.NewMockStatelessHandleAllocator(ctrl)
	casFileHandleAllocation.EXPECT().AsStatelessAllocator().Return(casFileHandleAllocator)
	outputPath := mock.NewMockOutputPath(ctrl)
	outputPathFactory.EXPECT().StartInitialBuild(path.MustNewComponent("c6adef0d5ca1888a4aa847fb51229a8c"), gomock.Any(), digest.MustNewInstanceName("my-cluster"), gomock.Any()).Return(outputPath)
	outputPath.EXPECT().FilterChildren(gomock.Any())

	response, err := d.StartBuild(ctx, &remoteoutputservice.StartBuildRequest{
		OutputBaseId:     "c6adef0d5ca1888a4aa847fb51229a8c",
		BuildId:          "ad778a53-48e6-4ae1-b1f5-01b84a508f5f",
		InstanceName:     "my-cluster",
		DigestFunction:   remoteexecution.DigestFunction_MD5,
		OutputPathPrefix: "/home/bob/bb_clientd/outputs",
		OutputPathAliases: map[string]string{
			"/home/bob/.cache/bazel/_bazel_bob/c6adef0d5ca1888a4aa847fb51229a8c/execroot/myproject/bazel-out": ".",
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteoutputservice.StartBuildResponse{
		OutputPathSuffix: "c6adef0d5ca1888a4aa847fb51229a8c",
	}, response)

	// Tests for the path_prefix and clean_path_prefix options.

	t.Run("InvalidPathPrefix", func(t *testing.T) {
		_, err := d.BatchCreate(ctx, &remoteoutputservice.BatchCreateRequest{
			BuildId:    "ad778a53-48e6-4ae1-b1f5-01b84a508f5f",
			PathPrefix: "/etc",
			Files: []*remoteexecution.OutputFile{
				{
					Path: "foo.o",
					Digest: &remoteexecution.Digest{
						Hash:      "d0ab620af7f3e77f3adfa190d41a25ce",
						SizeBytes: 123,
					},
				},
			},
		})
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Failed to create path prefix directory: Path is absolute, while a relative path was expected"), err)
	})

	t.Run("PathPrefixCreationFailure", func(t *testing.T) {
		child1 := mock.NewMockPrepopulatedDirectory(ctrl)
		outputPath.EXPECT().CreateAndEnterPrepopulatedDirectory(path.MustNewComponent("some")).
			Return(child1, nil)
		child1.EXPECT().CreateAndEnterPrepopulatedDirectory(path.MustNewComponent("sub")).
			Return(nil, status.Error(codes.Internal, "Disk failure"))

		_, err := d.BatchCreate(ctx, &remoteoutputservice.BatchCreateRequest{
			BuildId:    "ad778a53-48e6-4ae1-b1f5-01b84a508f5f",
			PathPrefix: "some/sub/directory",
			Files: []*remoteexecution.OutputFile{
				{
					Path: "foo.o",
					Digest: &remoteexecution.Digest{
						Hash:      "d0ab620af7f3e77f3adfa190d41a25ce",
						SizeBytes: 123,
					},
				},
			},
		})
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to create path prefix directory: Disk failure"), err)
	})

	t.Run("PathPrefixCleanFailure", func(t *testing.T) {
		child1 := mock.NewMockPrepopulatedDirectory(ctrl)
		outputPath.EXPECT().CreateAndEnterPrepopulatedDirectory(path.MustNewComponent("directory")).
			Return(child1, nil)
		child1.EXPECT().RemoveAllChildren(false).Return(status.Error(codes.Internal, "Disk failure"))

		_, err := d.BatchCreate(ctx, &remoteoutputservice.BatchCreateRequest{
			BuildId:         "ad778a53-48e6-4ae1-b1f5-01b84a508f5f",
			PathPrefix:      "directory",
			CleanPathPrefix: true,
			Files: []*remoteexecution.OutputFile{
				{
					Path: "foo.o",
					Digest: &remoteexecution.Digest{
						Hash:      "d0ab620af7f3e77f3adfa190d41a25ce",
						SizeBytes: 123,
					},
				},
			},
		})
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to clean path prefix directory: Disk failure"), err)
	})

	t.Run("SymlinkCreationFailure", func(t *testing.T) {
		symlink := mock.NewMockNativeLeaf(ctrl)
		symlinkFactory.EXPECT().LookupSymlink([]byte("target")).Return(symlink)
		outputPath.EXPECT().CreateChildren(map[path.Component]re_vfs.InitialNode{
			path.MustNewComponent("foo"): {Leaf: symlink},
		}, true).Return(status.Error(codes.Internal, "I/O error"))
		symlink.EXPECT().Unlink()

		_, err := d.BatchCreate(ctx, &remoteoutputservice.BatchCreateRequest{
			BuildId: "ad778a53-48e6-4ae1-b1f5-01b84a508f5f",
			Symlinks: []*remoteexecution.OutputSymlink{
				{
					Path:   "foo",
					Target: "target",
				},
			},
		})
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to create symbolic link \"foo\": I/O error"), err)
	})

	// The creation of actual files and directories is hard to test,
	// as the InitialNode arguments provided to CreateChildren()
	// contain objects that are hard to compare. At least provide a
	// test for the success case.

	t.Run("Success", func(t *testing.T) {
		child1 := mock.NewMockPrepopulatedDirectory(ctrl)
		outputPath.EXPECT().CreateAndEnterPrepopulatedDirectory(path.MustNewComponent("a")).
			Return(child1, nil)

		child2 := mock.NewMockPrepopulatedDirectory(ctrl)
		child1.EXPECT().CreateAndEnterPrepopulatedDirectory(path.MustNewComponent("b")).
			Return(child2, nil).Times(3)

		// Creation of "file".
		casFileHandleAllocation := mock.NewMockStatelessHandleAllocation(ctrl)
		casFileHandleAllocator.EXPECT().New(gomock.Any()).Return(casFileHandleAllocation)
		file := mock.NewMockNativeLeaf(ctrl)
		casFileHandleAllocation.EXPECT().AsNativeLeaf(gomock.Any()).Return(file)
		child2.EXPECT().CreateChildren(map[path.Component]re_vfs.InitialNode{
			path.MustNewComponent("file"): {Leaf: file},
		}, true)

		// Creation of "directory".
		child2.EXPECT().CreateChildren(gomock.Any(), true)

		// Creation of "symlink".
		symlink := mock.NewMockNativeLeaf(ctrl)
		symlinkFactory.EXPECT().LookupSymlink([]byte("file")).Return(symlink)
		child2.EXPECT().CreateChildren(map[path.Component]re_vfs.InitialNode{
			path.MustNewComponent("symlink"): {Leaf: symlink},
		}, true)

		_, err := d.BatchCreate(ctx, &remoteoutputservice.BatchCreateRequest{
			BuildId:    "ad778a53-48e6-4ae1-b1f5-01b84a508f5f",
			PathPrefix: "a",
			Files: []*remoteexecution.OutputFile{
				{
					Path:         "b/file",
					IsExecutable: true,
					Digest: &remoteexecution.Digest{
						Hash:      "d0ab620af7f3e77f3adfa190d41a25ce",
						SizeBytes: 123,
					},
				},
			},
			Directories: []*remoteexecution.OutputDirectory{
				{
					Path: "b/directory",
					TreeDigest: &remoteexecution.Digest{
						Hash:      "8e1554fc1ad824a6e9180c7b145790d2",
						SizeBytes: 123,
					},
				},
			},
			Symlinks: []*remoteexecution.OutputSymlink{
				{
					Path:   "b/symlink",
					Target: "file",
				},
			},
		})
		require.NoError(t, err)
	})
}

func TestRemoteOutputServiceDirectoryBatchStat(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	handleAllocator := mock.NewMockStatefulHandleAllocator(ctrl)
	outputPathFactory := mock.NewMockOutputPathFactory(ctrl)
	bareContentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	retryingContentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	indexedTreeFetcher := mock.NewMockIndexedTreeFetcher(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	dHandleAllocation := mock.NewMockStatefulHandleAllocation(ctrl)
	handleAllocator.EXPECT().New().Return(dHandleAllocation)
	dHandle := mock.NewMockStatefulDirectoryHandle(ctrl)
	dHandleAllocation.EXPECT().AsStatefulDirectory(gomock.Any()).Return(dHandle)
	d := cd_vfs.NewRemoteOutputServiceDirectory(
		handleAllocator,
		outputPathFactory,
		bareContentAddressableStorage,
		retryingContentAddressableStorage,
		indexedTreeFetcher,
		symlinkFactory)

	t.Run("InvalidBuildID", func(t *testing.T) {
		// StartBuild() should be called first.
		_, err := d.BatchStat(ctx, &remoteoutputservice.BatchStatRequest{
			BuildId: "140dbef8-1b24-4966-bb9e-8edc7fa61df8",
			Paths:   []string{"foo.o"},
		})
		testutil.RequireEqualStatus(t, status.Error(codes.FailedPrecondition, "Build ID is not associated with any running build"), err)
	})

	// Let the remainder of the tests assume that a build is running.
	casFileHandleAllocation := mock.NewMockStatefulHandleAllocation(ctrl)
	handleAllocator.EXPECT().New().Return(casFileHandleAllocation)
	casFileHandleAllocator := mock.NewMockStatelessHandleAllocator(ctrl)
	casFileHandleAllocation.EXPECT().AsStatelessAllocator().Return(casFileHandleAllocator)
	outputPath := mock.NewMockOutputPath(ctrl)
	outputPathFactory.EXPECT().StartInitialBuild(path.MustNewComponent("9da951b8cb759233037166e28f7ea186"), gomock.Any(), digest.MustNewInstanceName("my-cluster"), gomock.Any()).Return(outputPath)
	outputPath.EXPECT().FilterChildren(gomock.Any())

	response, err := d.StartBuild(ctx, &remoteoutputservice.StartBuildRequest{
		OutputBaseId:     "9da951b8cb759233037166e28f7ea186",
		BuildId:          "37f5dbef-b117-4fb6-bce8-5c147cb603b4",
		InstanceName:     "my-cluster",
		DigestFunction:   remoteexecution.DigestFunction_MD5,
		OutputPathPrefix: "/home/bob/bb_clientd/outputs",
		OutputPathAliases: map[string]string{
			"/home/bob/.cache/bazel/_bazel_bob/9da951b8cb759233037166e28f7ea186/execroot/myproject/bazel-out": ".",
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteoutputservice.StartBuildResponse{
		OutputPathSuffix: "9da951b8cb759233037166e28f7ea186",
	}, response)

	t.Run("Noop", func(t *testing.T) {
		// Requests that don't contain any paths shouldn't cause
		// any I/O against the output path.
		_, err := d.BatchStat(ctx, &remoteoutputservice.BatchStatRequest{
			BuildId: "37f5dbef-b117-4fb6-bce8-5c147cb603b4",
		})
		require.NoError(t, err)
	})

	t.Run("OnDirectoryLookupFailure", func(t *testing.T) {
		outputPath.EXPECT().LookupChild(path.MustNewComponent("stdio")).Return(nil, nil, status.Error(codes.Internal, "Disk failure"))

		_, err := d.BatchStat(ctx, &remoteoutputservice.BatchStatRequest{
			BuildId: "37f5dbef-b117-4fb6-bce8-5c147cb603b4",
			Paths:   []string{"stdio/printf.o"},
		})
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to resolve path \"stdio/printf.o\" beyond \".\": Disk failure"), err)
	})

	t.Run("OnDirectoryReadlinkFailure", func(t *testing.T) {
		leaf := mock.NewMockNativeLeaf(ctrl)
		outputPath.EXPECT().LookupChild(path.MustNewComponent("stdio")).Return(nil, leaf, nil)
		leaf.EXPECT().Readlink().Return("", status.Error(codes.Internal, "Disk failure"))

		_, err := d.BatchStat(ctx, &remoteoutputservice.BatchStatRequest{
			BuildId: "37f5dbef-b117-4fb6-bce8-5c147cb603b4",
			Paths:   []string{"stdio/printf.o"},
		})
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to resolve path \"stdio/printf.o\" beyond \".\": Disk failure"), err)
	})

	t.Run("OnTerminalLookupFailure", func(t *testing.T) {
		outputPath.EXPECT().LookupChild(path.MustNewComponent("printf.o")).Return(nil, nil, status.Error(codes.Internal, "Disk failure"))

		_, err := d.BatchStat(ctx, &remoteoutputservice.BatchStatRequest{
			BuildId: "37f5dbef-b117-4fb6-bce8-5c147cb603b4",
			Paths:   []string{"printf.o"},
		})
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to resolve path \"printf.o\" beyond \".\": Disk failure"), err)
	})

	t.Run("OnTerminalReadlinkFailure", func(t *testing.T) {
		leaf := mock.NewMockNativeLeaf(ctrl)
		outputPath.EXPECT().LookupChild(path.MustNewComponent("printf.o")).Return(nil, leaf, nil)
		leaf.EXPECT().Readlink().Return("", status.Error(codes.Internal, "Disk failure"))

		_, err := d.BatchStat(ctx, &remoteoutputservice.BatchStatRequest{
			BuildId:        "37f5dbef-b117-4fb6-bce8-5c147cb603b4",
			FollowSymlinks: true,
			Paths:          []string{"printf.o"},
		})
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to resolve path \"printf.o\" beyond \".\": Disk failure"), err)
	})

	t.Run("OnTerminalGetOutputServiceFileStatusFailure", func(t *testing.T) {
		leaf := mock.NewMockNativeLeaf(ctrl)
		outputPath.EXPECT().LookupChild(path.MustNewComponent("printf.o")).Return(nil, leaf, nil)
		leaf.EXPECT().GetOutputServiceFileStatus(gomock.Any()).Return(nil, status.Error(codes.Internal, "Disk failure"))

		_, err := d.BatchStat(ctx, &remoteoutputservice.BatchStatRequest{
			BuildId: "37f5dbef-b117-4fb6-bce8-5c147cb603b4",
			Paths:   []string{"printf.o"},
		})
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to resolve path \"printf.o\" beyond \".\": Disk failure"), err)
	})

	t.Run("Success", func(t *testing.T) {
		// Lookup of "file", pointing to directly to a file.
		leaf1 := mock.NewMockNativeLeaf(ctrl)
		outputPath.EXPECT().LookupChild(path.MustNewComponent("file")).Return(nil, leaf1, nil)
		leaf1.EXPECT().Readlink().Return("", syscall.EINVAL)
		leaf1.EXPECT().GetOutputServiceFileStatus(gomock.Any()).Return(&remoteoutputservice.FileStatus{
			FileType: &remoteoutputservice.FileStatus_File_{
				File: &remoteoutputservice.FileStatus_File{
					Digest: &remoteexecution.Digest{
						Hash:      "ad17450bb18953f249532a478d2150ba",
						SizeBytes: 72,
					},
				},
			},
		}, nil)

		// Lookup of "directory". pointing directly to a directory.
		directory1 := mock.NewMockPrepopulatedDirectory(ctrl)
		outputPath.EXPECT().LookupChild(path.MustNewComponent("directory")).Return(directory1, nil, nil)

		// Lookup of "nested/symlink_internal_relative_file",
		// being a symlink that points to a file.
		directory2 := mock.NewMockPrepopulatedDirectory(ctrl)
		outputPath.EXPECT().LookupChild(path.MustNewComponent("nested")).Return(directory2, nil, nil)
		leaf2 := mock.NewMockNativeLeaf(ctrl)
		directory2.EXPECT().LookupChild(path.MustNewComponent("symlink_internal_relative_file")).Return(nil, leaf2, nil)
		leaf2.EXPECT().Readlink().Return("../target", nil)
		leaf3 := mock.NewMockNativeLeaf(ctrl)
		outputPath.EXPECT().LookupChild(path.MustNewComponent("target")).Return(nil, leaf3, nil)
		leaf3.EXPECT().Readlink().Return("", syscall.EINVAL)
		leaf3.EXPECT().GetOutputServiceFileStatus(gomock.Any()).Return(&remoteoutputservice.FileStatus{
			FileType: &remoteoutputservice.FileStatus_File_{
				File: &remoteoutputservice.FileStatus_File{
					Digest: &remoteexecution.Digest{
						Hash:      "166d6efee3489f73be1c3c2304e50bca",
						SizeBytes: 85,
					},
				},
			},
		}, nil)

		// Lookup of "nested/symlink_internal_relative_directory",
		// being a symlink that points to a directory.
		directory3 := mock.NewMockPrepopulatedDirectory(ctrl)
		outputPath.EXPECT().LookupChild(path.MustNewComponent("nested")).Return(directory3, nil, nil)
		leaf4 := mock.NewMockNativeLeaf(ctrl)
		directory3.EXPECT().LookupChild(path.MustNewComponent("symlink_internal_relative_directory")).Return(nil, leaf4, nil)
		leaf4.EXPECT().Readlink().Return("..", nil)

		// Lookup of "nested/symlink_internal_absolute_path",
		// being a symlink containing an absolute path starting
		// with the output path.
		directory4 := mock.NewMockPrepopulatedDirectory(ctrl)
		outputPath.EXPECT().LookupChild(path.MustNewComponent("nested")).Return(directory4, nil, nil)
		leaf5 := mock.NewMockNativeLeaf(ctrl)
		directory4.EXPECT().LookupChild(path.MustNewComponent("symlink_internal_absolute_path")).Return(nil, leaf5, nil)
		leaf5.EXPECT().Readlink().Return("/home/bob/bb_clientd/outputs/9da951b8cb759233037166e28f7ea186/hello", nil)
		directory5 := mock.NewMockPrepopulatedDirectory(ctrl)
		outputPath.EXPECT().LookupChild(path.MustNewComponent("hello")).Return(directory5, nil, nil)

		// Lookup of "nested/symlink_internal_absolute_alias",
		// being a symlink containing an absolute path starting
		// with one of the output path aliases.
		directory6 := mock.NewMockPrepopulatedDirectory(ctrl)
		outputPath.EXPECT().LookupChild(path.MustNewComponent("nested")).Return(directory6, nil, nil)
		leaf6 := mock.NewMockNativeLeaf(ctrl)
		directory6.EXPECT().LookupChild(path.MustNewComponent("symlink_internal_absolute_alias")).Return(nil, leaf6, nil)
		leaf6.EXPECT().Readlink().Return("/home/bob/.cache/bazel/_bazel_bob/9da951b8cb759233037166e28f7ea186/execroot/myproject/bazel-out/hello", nil)
		directory7 := mock.NewMockPrepopulatedDirectory(ctrl)
		outputPath.EXPECT().LookupChild(path.MustNewComponent("hello")).Return(directory7, nil, nil)

		// Lookup of "nested/symlink_external", being a symlink
		// containing an absolute path that doesn't start with
		// any known prefix.
		directory8 := mock.NewMockPrepopulatedDirectory(ctrl)
		outputPath.EXPECT().LookupChild(path.MustNewComponent("nested")).Return(directory8, nil, nil)
		leaf7 := mock.NewMockNativeLeaf(ctrl)
		directory8.EXPECT().LookupChild(path.MustNewComponent("symlink_external")).Return(nil, leaf7, nil)
		leaf7.EXPECT().Readlink().Return("/etc/passwd", nil)

		// Lookup of "nonexistent".
		outputPath.EXPECT().LookupChild(path.MustNewComponent("nonexistent")).Return(nil, nil, syscall.ENOENT)

		response, err := d.BatchStat(ctx, &remoteoutputservice.BatchStatRequest{
			BuildId:           "37f5dbef-b117-4fb6-bce8-5c147cb603b4",
			IncludeFileDigest: true,
			FollowSymlinks:    true,
			Paths: []string{
				"file",
				"directory",
				"nested/symlink_internal_relative_file",
				"nested/symlink_internal_relative_directory",
				"nested/symlink_internal_absolute_path",
				"nested/symlink_internal_absolute_alias",
				"nested/symlink_external",
				"../foo",
				"/etc/passwd",
				"nonexistent",
				".",
			},
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &remoteoutputservice.BatchStatResponse{
			Responses: []*remoteoutputservice.StatResponse{
				// "file".
				{
					FileStatus: &remoteoutputservice.FileStatus{
						FileType: &remoteoutputservice.FileStatus_File_{
							File: &remoteoutputservice.FileStatus_File{
								Digest: &remoteexecution.Digest{
									Hash:      "ad17450bb18953f249532a478d2150ba",
									SizeBytes: 72,
								},
							},
						},
					},
				},
				// "directory".
				{
					FileStatus: &remoteoutputservice.FileStatus{
						FileType: &remoteoutputservice.FileStatus_Directory{
							Directory: &emptypb.Empty{},
						},
					},
				},
				// "nested/symlink_internal_relative_file".
				{
					FileStatus: &remoteoutputservice.FileStatus{
						FileType: &remoteoutputservice.FileStatus_File_{
							File: &remoteoutputservice.FileStatus_File{
								Digest: &remoteexecution.Digest{
									Hash:      "166d6efee3489f73be1c3c2304e50bca",
									SizeBytes: 85,
								},
							},
						},
					},
				},
				// "nested/symlink_internal_relative_directory".
				{
					FileStatus: &remoteoutputservice.FileStatus{
						FileType: &remoteoutputservice.FileStatus_Directory{
							Directory: &emptypb.Empty{},
						},
					},
				},
				// "nested/symlink_internal_absolute_path".
				{
					FileStatus: &remoteoutputservice.FileStatus{
						FileType: &remoteoutputservice.FileStatus_Directory{
							Directory: &emptypb.Empty{},
						},
					},
				},
				// "nested/symlink_internal_absolute_alias".
				{
					FileStatus: &remoteoutputservice.FileStatus{
						FileType: &remoteoutputservice.FileStatus_Directory{
							Directory: &emptypb.Empty{},
						},
					},
				},
				// "nested/symlink_external".
				{
					FileStatus: &remoteoutputservice.FileStatus{
						FileType: &remoteoutputservice.FileStatus_External_{
							External: &remoteoutputservice.FileStatus_External{
								NextPath: "/etc/passwd",
							},
						},
					},
				},
				// "../foo".
				{
					FileStatus: &remoteoutputservice.FileStatus{
						FileType: &remoteoutputservice.FileStatus_External_{
							External: &remoteoutputservice.FileStatus_External{
								NextPath: "../foo",
							},
						},
					},
				},
				// "/etc/passwd".
				{
					FileStatus: &remoteoutputservice.FileStatus{
						FileType: &remoteoutputservice.FileStatus_External_{
							External: &remoteoutputservice.FileStatus_External{
								NextPath: "/etc/passwd",
							},
						},
					},
				},
				// "nonexistent".
				{},
				// ".".
				{
					FileStatus: &remoteoutputservice.FileStatus{
						FileType: &remoteoutputservice.FileStatus_Directory{
							Directory: &emptypb.Empty{},
						},
					},
				},
			},
		}, response)
	})
}

func TestRemoteOutputServiceDirectoryVirtualLookup(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	handleAllocator := mock.NewMockStatefulHandleAllocator(ctrl)
	outputPathFactory := mock.NewMockOutputPathFactory(ctrl)
	bareContentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	retryingContentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	indexedTreeFetcher := mock.NewMockIndexedTreeFetcher(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	dHandleAllocation := mock.NewMockStatefulHandleAllocation(ctrl)
	handleAllocator.EXPECT().New().Return(dHandleAllocation)
	dHandle := mock.NewMockStatefulDirectoryHandle(ctrl)
	dHandleAllocation.EXPECT().AsStatefulDirectory(gomock.Any()).Return(dHandle)
	d := cd_vfs.NewRemoteOutputServiceDirectory(
		handleAllocator,
		outputPathFactory,
		bareContentAddressableStorage,
		retryingContentAddressableStorage,
		indexedTreeFetcher,
		symlinkFactory)

	// No output paths exist, so VirtualLookup() should always fail.
	var out1 re_vfs.Attributes
	_, _, s := d.VirtualLookup(path.MustNewComponent("eda09135e50ff6e877fe5f8136ddc759"), 0, &out1)
	require.Equal(t, re_vfs.StatusErrNoEnt, s)

	// Create an output path.
	casFileHandleAllocation := mock.NewMockStatefulHandleAllocation(ctrl)
	handleAllocator.EXPECT().New().Return(casFileHandleAllocation)
	casFileHandleAllocator := mock.NewMockStatelessHandleAllocator(ctrl)
	casFileHandleAllocation.EXPECT().AsStatelessAllocator().Return(casFileHandleAllocator)
	outputPath := mock.NewMockOutputPath(ctrl)
	outputPathFactory.EXPECT().StartInitialBuild(path.MustNewComponent("eaf1d65b7ab802934e6b57d0e14b3f30"), gomock.Any(), digest.EmptyInstanceName, gomock.Any()).Return(outputPath)
	outputPath.EXPECT().FilterChildren(gomock.Any())

	response, err := d.StartBuild(ctx, &remoteoutputservice.StartBuildRequest{
		OutputBaseId:     "eaf1d65b7ab802934e6b57d0e14b3f30",
		BuildId:          "2840d789-16ff-4fe4-9639-3245f9bb9106",
		DigestFunction:   remoteexecution.DigestFunction_SHA1,
		OutputPathPrefix: "/home/bob/bb_clientd/outputs",
		OutputPathAliases: map[string]string{
			"/home/bob/.cache/bazel/_bazel_bob/eaf1d65b7ab802934e6b57d0e14b3f30/execroot/myproject/bazel-out": ".",
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteoutputservice.StartBuildResponse{
		OutputPathSuffix: "eaf1d65b7ab802934e6b57d0e14b3f30",
	}, response)

	outputPath.EXPECT().VirtualGetAttributes(
		re_vfs.AttributesMaskInodeNumber,
		gomock.Any(),
	).Do(func(requested re_vfs.AttributesMask, out *re_vfs.Attributes) {
		out.SetInodeNumber(101)
	})

	// Call VirtualLookup() again. It should now succeed.
	var out2 re_vfs.Attributes
	directory, leaf, s := d.VirtualLookup(path.MustNewComponent("eaf1d65b7ab802934e6b57d0e14b3f30"), re_vfs.AttributesMaskInodeNumber, &out2)
	require.Equal(t, re_vfs.StatusOK, s)
	require.Equal(t, outputPath, directory)
	require.Nil(t, leaf)
	require.Equal(t, *(&re_vfs.Attributes{}).SetInodeNumber(101), out2)

	// Remove the output path.
	outputPath.EXPECT().RemoveAllChildren(true)
	dHandle.EXPECT().NotifyRemoval(path.MustNewComponent("eaf1d65b7ab802934e6b57d0e14b3f30"))

	_, err = d.Clean(ctx, &remoteoutputservice.CleanRequest{
		OutputBaseId: "eaf1d65b7ab802934e6b57d0e14b3f30",
	})
	require.NoError(t, err)

	// VirtualLookup() should fail once again.
	var out3 re_vfs.Attributes
	_, _, s = d.VirtualLookup(path.MustNewComponent("eda09135e50ff6e877fe5f8136ddc759"), 0, &out3)
	require.Equal(t, re_vfs.StatusErrNoEnt, s)
}

func TestRemoteOutputServiceDirectoryVirtualReadDir(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	handleAllocator := mock.NewMockStatefulHandleAllocator(ctrl)
	outputPathFactory := mock.NewMockOutputPathFactory(ctrl)
	bareContentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	retryingContentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	indexedTreeFetcher := mock.NewMockIndexedTreeFetcher(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	dHandleAllocation := mock.NewMockStatefulHandleAllocation(ctrl)
	handleAllocator.EXPECT().New().Return(dHandleAllocation)
	dHandle := mock.NewMockStatefulDirectoryHandle(ctrl)
	dHandleAllocation.EXPECT().AsStatefulDirectory(gomock.Any()).Return(dHandle)
	d := cd_vfs.NewRemoteOutputServiceDirectory(
		handleAllocator,
		outputPathFactory,
		bareContentAddressableStorage,
		retryingContentAddressableStorage,
		indexedTreeFetcher,
		symlinkFactory)

	t.Run("InitialState", func(t *testing.T) {
		// The directory should initially be empty.
		reporter := mock.NewMockDirectoryEntryReporter(ctrl)

		require.Equal(
			t,
			re_vfs.StatusOK,
			d.VirtualReadDir(0, re_vfs.AttributesMaskInodeNumber, reporter))
	})

	// Create two output paths.
	casFileHandleAllocation1 := mock.NewMockStatefulHandleAllocation(ctrl)
	handleAllocator.EXPECT().New().Return(casFileHandleAllocation1)
	casFileHandleAllocator1 := mock.NewMockStatelessHandleAllocator(ctrl)
	casFileHandleAllocation1.EXPECT().AsStatelessAllocator().Return(casFileHandleAllocator1)
	outputPath1 := mock.NewMockOutputPath(ctrl)
	outputPathFactory.EXPECT().StartInitialBuild(path.MustNewComponent("83f3e6ff93a5403cbfb14682d8165968"), gomock.Any(), digest.EmptyInstanceName, gomock.Any()).Return(outputPath1)
	outputPath1.EXPECT().FilterChildren(gomock.Any())

	response, err := d.StartBuild(ctx, &remoteoutputservice.StartBuildRequest{
		OutputBaseId:     "83f3e6ff93a5403cbfb14682d8165968",
		BuildId:          "2b2b974f-ed53-40f0-a75a-422c09ba8be8",
		DigestFunction:   remoteexecution.DigestFunction_SHA384,
		OutputPathPrefix: "/home/bob/bb_clientd/outputs",
		OutputPathAliases: map[string]string{
			"/home/bob/.cache/bazel/_bazel_bob/83f3e6ff93a5403cbfb14682d8165968/execroot/project1/bazel-out": ".",
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteoutputservice.StartBuildResponse{
		OutputPathSuffix: "83f3e6ff93a5403cbfb14682d8165968",
	}, response)

	casFileHandleAllocation2 := mock.NewMockStatefulHandleAllocation(ctrl)
	handleAllocator.EXPECT().New().Return(casFileHandleAllocation2)
	casFileHandleAllocator2 := mock.NewMockStatelessHandleAllocator(ctrl)
	casFileHandleAllocation2.EXPECT().AsStatelessAllocator().Return(casFileHandleAllocator2)
	outputPath2 := mock.NewMockOutputPath(ctrl)
	outputPathFactory.EXPECT().StartInitialBuild(path.MustNewComponent("d4b145a6191c6d8d037d13986274d08d"), gomock.Any(), digest.EmptyInstanceName, gomock.Any()).Return(outputPath2)
	outputPath2.EXPECT().FilterChildren(gomock.Any())

	response, err = d.StartBuild(ctx, &remoteoutputservice.StartBuildRequest{
		OutputBaseId:     "d4b145a6191c6d8d037d13986274d08d",
		BuildId:          "2f941206-fb17-460a-a779-10c621bc0d19",
		DigestFunction:   remoteexecution.DigestFunction_SHA512,
		OutputPathPrefix: "/home/bob/bb_clientd/outputs",
		OutputPathAliases: map[string]string{
			"/home/bob/.cache/bazel/_bazel_bob/d4b145a6191c6d8d037d13986274d08d/execroot/project2/bazel-out": ".",
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteoutputservice.StartBuildResponse{
		OutputPathSuffix: "d4b145a6191c6d8d037d13986274d08d",
	}, response)

	t.Run("FromStart", func(t *testing.T) {
		// The directory listing should contain both output paths.
		reporter := mock.NewMockDirectoryEntryReporter(ctrl)
		outputPath1.EXPECT().VirtualGetAttributes(
			re_vfs.AttributesMaskInodeNumber,
			gomock.Any(),
		).Do(func(requested re_vfs.AttributesMask, out *re_vfs.Attributes) {
			out.SetInodeNumber(101)
		})
		reporter.EXPECT().ReportDirectory(
			uint64(1),
			path.MustNewComponent("83f3e6ff93a5403cbfb14682d8165968"),
			outputPath1,
			(&re_vfs.Attributes{}).SetInodeNumber(101),
		).Return(true)
		outputPath2.EXPECT().VirtualGetAttributes(
			re_vfs.AttributesMaskInodeNumber,
			gomock.Any(),
		).Do(func(requested re_vfs.AttributesMask, out *re_vfs.Attributes) {
			out.SetInodeNumber(102)
		})
		reporter.EXPECT().ReportDirectory(
			uint64(2),
			path.MustNewComponent("d4b145a6191c6d8d037d13986274d08d"),
			outputPath1,
			(&re_vfs.Attributes{}).SetInodeNumber(102),
		).Return(true)

		require.Equal(
			t,
			re_vfs.StatusOK,
			d.VirtualReadDir(0, re_vfs.AttributesMaskInodeNumber, reporter))
	})

	t.Run("Partial", func(t *testing.T) {
		reporter := mock.NewMockDirectoryEntryReporter(ctrl)
		outputPath2.EXPECT().VirtualGetAttributes(
			re_vfs.AttributesMaskInodeNumber,
			gomock.Any(),
		).Do(func(requested re_vfs.AttributesMask, out *re_vfs.Attributes) {
			out.SetInodeNumber(102)
		})
		reporter.EXPECT().ReportDirectory(
			uint64(2),
			path.MustNewComponent("d4b145a6191c6d8d037d13986274d08d"),
			outputPath1,
			(&re_vfs.Attributes{}).SetInodeNumber(102),
		).Return(true)

		require.Equal(
			t,
			re_vfs.StatusOK,
			d.VirtualReadDir(1, re_vfs.AttributesMaskInodeNumber, reporter))
	})

	t.Run("AtEOF", func(t *testing.T) {
		reporter := mock.NewMockDirectoryEntryReporter(ctrl)
		require.Equal(
			t,
			re_vfs.StatusOK,
			d.VirtualReadDir(2, re_vfs.AttributesMaskInodeNumber, reporter))
	})

	t.Run("BeyondEOF", func(t *testing.T) {
		reporter := mock.NewMockDirectoryEntryReporter(ctrl)
		require.Equal(
			t,
			re_vfs.StatusOK,
			d.VirtualReadDir(3, re_vfs.AttributesMaskInodeNumber, reporter))
	})

	// Remove all output paths.
	outputPath1.EXPECT().RemoveAllChildren(true)
	dHandle.EXPECT().NotifyRemoval(path.MustNewComponent("83f3e6ff93a5403cbfb14682d8165968"))
	_, err = d.Clean(ctx, &remoteoutputservice.CleanRequest{
		OutputBaseId: "83f3e6ff93a5403cbfb14682d8165968",
	})
	require.NoError(t, err)

	outputPath2.EXPECT().RemoveAllChildren(true)
	dHandle.EXPECT().NotifyRemoval(path.MustNewComponent("d4b145a6191c6d8d037d13986274d08d"))
	_, err = d.Clean(ctx, &remoteoutputservice.CleanRequest{
		OutputBaseId: "d4b145a6191c6d8d037d13986274d08d",
	})
	require.NoError(t, err)

	t.Run("AfterClean", func(t *testing.T) {
		reporter := mock.NewMockDirectoryEntryReporter(ctrl)

		require.Equal(
			t,
			re_vfs.StatusOK,
			d.VirtualReadDir(0, re_vfs.AttributesMaskInodeNumber, reporter))
	})
}
