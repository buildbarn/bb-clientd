package virtual_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-clientd/internal/mock"
	cd_vfs "github.com/buildbarn/bb-clientd/pkg/filesystem/virtual"
	re_vfs "github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

func TestLocalFileUploadingOutputPathFactory(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	// Construct an output path factory.
	baseOutputPathFactory := mock.NewMockOutputPathFactory(ctrl)
	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	contentAddressableStorage.EXPECT().FindMissing(gomock.Any(), digest.EmptySet).
		Return(digest.EmptySet, nil).
		AnyTimes()
	globalErrorLogger := mock.NewMockErrorLogger(ctrl)
	outputPathFactory := cd_vfs.NewLocalFileUploadingOutputPathFactory(
		baseOutputPathFactory,
		contentAddressableStorage,
		globalErrorLogger,
		semaphore.NewWeighted(1))

	// Construct an output path.
	outputBaseID := path.MustNewComponent("15c974d0b2820c3ae15a237e186cd84b")
	casFileFactory := mock.NewMockCASFileFactory(ctrl)
	digestFunction := digest.MustNewFunction("default", remoteexecution.DigestFunction_SHA256)
	fileErrorLogger := mock.NewMockErrorLogger(ctrl)
	baseOutputPath := mock.NewMockOutputPath(ctrl)
	baseOutputPathFactory.EXPECT().StartInitialBuild(
		outputBaseID,
		casFileFactory,
		digestFunction,
		fileErrorLogger,
	).Return(baseOutputPath)

	outputPath := outputPathFactory.StartInitialBuild(
		outputBaseID,
		casFileFactory,
		digestFunction,
		fileErrorLogger)

	t.Run("Empty", func(t *testing.T) {
		// Output path is empty, meaning there is nothing to upload.
		digestFunction := digest.MustNewFunction("example", remoteexecution.DigestFunction_SHA256)
		baseOutputPath.EXPECT().FinalizeBuild(ctx, gomock.Any())
		baseOutputPath.EXPECT().LookupAllChildren()

		outputPath.FinalizeBuild(ctx, digestFunction)
	})

	t.Run("LookupAllChildrenFailure", func(t *testing.T) {
		digestFunction := digest.MustNewFunction("example", remoteexecution.DigestFunction_SHA256)
		baseOutputPath.EXPECT().FinalizeBuild(ctx, gomock.Any())

		subDirectory := mock.NewMockPrepopulatedDirectory(ctrl)
		baseOutputPath.EXPECT().LookupAllChildren().Return(
			[]re_vfs.DirectoryPrepopulatedDirEntry{
				{Name: path.MustNewComponent("subdirectory"), Child: subDirectory},
			},
			nil,
			nil)
		subDirectory.EXPECT().LookupAllChildren().Return(
			nil,
			nil,
			status.Error(codes.Internal, "I/O error"))
		globalErrorLogger.EXPECT().Log(testutil.EqStatus(t, status.Error(codes.Internal, "Failed to look up children of directory \"subdirectory\" in output path \"15c974d0b2820c3ae15a237e186cd84b\": I/O error")))

		outputPath.FinalizeBuild(ctx, digestFunction)
	})

	t.Run("UploadFileFailure", func(t *testing.T) {
		digestFunction := digest.MustNewFunction("example", remoteexecution.DigestFunction_SHA256)
		baseOutputPath.EXPECT().FinalizeBuild(ctx, gomock.Any())

		leaf := mock.NewMockNativeLeaf(ctrl)
		baseOutputPath.EXPECT().LookupAllChildren().Return(
			nil,
			[]re_vfs.LeafPrepopulatedDirEntry{
				{Name: path.MustNewComponent("leaf"), Child: leaf},
			},
			nil)
		leaf.EXPECT().UploadFile(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(digest.BadDigest, status.Error(codes.Internal, "Cannot compute digest due to read failure"))
		globalErrorLogger.EXPECT().Log(testutil.EqStatus(t, status.Error(codes.Internal, "Failed to upload local file \"leaf\" in output path \"15c974d0b2820c3ae15a237e186cd84b\": Cannot compute digest due to read failure")))

		outputPath.FinalizeBuild(ctx, digestFunction)
	})

	t.Run("FindMissingFailure", func(t *testing.T) {
		digestFunction := digest.MustNewFunction("example", remoteexecution.DigestFunction_SHA256)
		baseOutputPath.EXPECT().FinalizeBuild(ctx, gomock.Any())

		leaf := mock.NewMockNativeLeaf(ctrl)
		baseOutputPath.EXPECT().LookupAllChildren().Return(
			nil,
			[]re_vfs.LeafPrepopulatedDirEntry{
				{Name: path.MustNewComponent("leaf"), Child: leaf},
			},
			nil)
		leafDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_SHA256, "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c", 11)
		leaf.EXPECT().UploadFile(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, contentAddressableStorage blobstore.BlobAccess, digestFunction digest.Function, writableFileUploadDelay <-chan struct{}) (digest.Digest, error) {
				require.NoError(t, contentAddressableStorage.Put(ctx, leafDigest, buffer.NewValidatedBufferFromByteSlice([]byte("Hello world"))))
				return leafDigest, nil
			})
		contentAddressableStorage.EXPECT().FindMissing(
			gomock.Any(),
			leafDigest.ToSingletonSet(),
		).Return(digest.EmptySet, status.Error(codes.Internal, "Storage offline"))
		globalErrorLogger.EXPECT().Log(testutil.EqStatus(t, status.Error(codes.Internal, "Failed to upload the contents of output path \"15c974d0b2820c3ae15a237e186cd84b\": Failed to determine existence of previous batch of blobs: Storage offline")))

		outputPath.FinalizeBuild(ctx, digestFunction)
	})

	t.Run("Success", func(t *testing.T) {
		digestFunction := digest.MustNewFunction("example", remoteexecution.DigestFunction_SHA256)
		baseOutputPath.EXPECT().FinalizeBuild(ctx, gomock.Any())

		// Traverse the file system.
		subDirectory := mock.NewMockPrepopulatedDirectory(ctrl)
		symlink := mock.NewMockNativeLeaf(ctrl)
		baseOutputPath.EXPECT().LookupAllChildren().Return(
			[]re_vfs.DirectoryPrepopulatedDirEntry{
				{Name: path.MustNewComponent("subdirectory"), Child: subDirectory},
			},
			[]re_vfs.LeafPrepopulatedDirEntry{
				{Name: path.MustNewComponent("symlink"), Child: symlink},
			},
			nil)

		missingLocalFile := mock.NewMockNativeLeaf(ctrl)
		presentLocalFile := mock.NewMockNativeLeaf(ctrl)
		remoteFile := mock.NewMockNativeLeaf(ctrl)
		subDirectory.EXPECT().LookupAllChildren().Return(
			nil,
			[]re_vfs.LeafPrepopulatedDirEntry{
				{Name: path.MustNewComponent("missing_local_file"), Child: missingLocalFile},
				{Name: path.MustNewComponent("present_local_file"), Child: presentLocalFile},
				{Name: path.MustNewComponent("remote_file"), Child: remoteFile},
			},
			nil)

		// Attempt to upload all files in the file system. Some
		// of those are not uploadable, or they don't result in
		// calls against the Content Addressable Storage due to
		// them already being remote.
		symlink.EXPECT().UploadFile(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, contentAddressableStorage blobstore.BlobAccess, digestFunction digest.Function, writableFileUploadDelay <-chan struct{}) (digest.Digest, error) {
				return digest.BadDigest, status.Error(codes.InvalidArgument, "This file cannot be uploaded, as it is a symbolic link")
			})
		missingLocalFileDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_SHA256, "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c", 11)
		missingLocalFile.EXPECT().UploadFile(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, contentAddressableStorage blobstore.BlobAccess, digestFunction digest.Function, writableFileUploadDelay <-chan struct{}) (digest.Digest, error) {
				require.NoError(t, contentAddressableStorage.Put(ctx, missingLocalFileDigest, buffer.NewValidatedBufferFromByteSlice([]byte("Hello world"))))
				return missingLocalFileDigest, nil
			})
		presentLocalFileDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_SHA256, "b4dabda568d0368a42a46108e8c669e1d9b18c0dad248de2068b07a730f524a2", 13)
		presentLocalFile.EXPECT().UploadFile(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, contentAddressableStorage blobstore.BlobAccess, digestFunction digest.Function, writableFileUploadDelay <-chan struct{}) (digest.Digest, error) {
				require.NoError(t, contentAddressableStorage.Put(ctx, presentLocalFileDigest, buffer.NewValidatedBufferFromByteSlice([]byte("Goodbye world"))))
				return presentLocalFileDigest, nil
			})
		remoteFile.EXPECT().UploadFile(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, contentAddressableStorage blobstore.BlobAccess, digestFunction digest.Function, writableFileUploadDelay <-chan struct{}) (digest.Digest, error) {
				return digest.MustNewDigest("example", remoteexecution.DigestFunction_SHA256, "888ddbf1e9cd5b201f67629d4efa9a4a0fb1cf5a910e9a44209eb07485f5f99d", 123), nil
			})

		// Transfer files that are missing.
		contentAddressableStorage.EXPECT().FindMissing(
			gomock.Any(),
			digest.NewSetBuilder().Add(missingLocalFileDigest).Add(presentLocalFileDigest).Build(),
		).Return(missingLocalFileDigest.ToSingletonSet(), nil)
		contentAddressableStorage.EXPECT().Put(gomock.Any(), missingLocalFileDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(1000)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello world"), data)
				return nil
			})

		outputPath.FinalizeBuild(ctx, digestFunction)
	})
}
