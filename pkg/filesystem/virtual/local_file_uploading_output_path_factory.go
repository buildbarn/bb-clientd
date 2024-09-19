package virtual

import (
	"context"

	re_blobstore "github.com/buildbarn/bb-remote-execution/pkg/blobstore"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type localFileUploadingOutputPathFactory struct {
	OutputPathFactory
	contentAddressableStorage blobstore.BlobAccess
	errorLogger               util.ErrorLogger
	concurrency               *semaphore.Weighted
}

// NewLocalFileUploadingOutputPathFactory creates a decorator for
// OutputPathFactory that at the end of every build traverses the full
// output path and uploads any files into the Content Addressable
// Storage (CAS) that are not present remotely.
//
// This decorator can be used to ensure that PersistentOutputPathFactory
// is capable of persisting and restoring all files in the output path,
// even if they were at no point uploaded by Bazel.
func NewLocalFileUploadingOutputPathFactory(base OutputPathFactory, contentAddressableStorage blobstore.BlobAccess, errorLogger util.ErrorLogger, concurrency *semaphore.Weighted) OutputPathFactory {
	return &localFileUploadingOutputPathFactory{
		OutputPathFactory:         base,
		contentAddressableStorage: contentAddressableStorage,
		errorLogger:               errorLogger,
		concurrency:               concurrency,
	}
}

func (opf *localFileUploadingOutputPathFactory) StartInitialBuild(outputBaseID path.Component, casFileFactory virtual.CASFileFactory, digestFunction digest.Function, errorLogger util.ErrorLogger) OutputPath {
	return &localFileUploadingOutputPath{
		OutputPath:   opf.OutputPathFactory.StartInitialBuild(outputBaseID, casFileFactory, digestFunction, errorLogger),
		factory:      opf,
		outputBaseID: outputBaseID,
	}
}

type localFileUploadingOutputPath struct {
	OutputPath
	factory      *localFileUploadingOutputPathFactory
	outputBaseID path.Component
}

func (op *localFileUploadingOutputPath) FinalizeBuild(ctx context.Context, digestFunction digest.Function) {
	op.OutputPath.FinalizeBuild(ctx, digestFunction)

	contentAddressableStorage, flusher := re_blobstore.NewBatchedStoreBlobAccess(
		op.factory.contentAddressableStorage,
		digest.KeyWithoutInstance,
		blobstore.RecommendedFindMissingDigestsCount,
		op.factory.concurrency)

	// For the Bazel Output Service use case it's not important
	// enough to have a configurable delay here, because the time it
	// takes until Bazel calls FinalizeBuild() is sufficiently high.
	// Furthermore, if we upload files with missing contents, a
	// subsequent build would repair these files.
	writableFileUploadDelay := make(chan struct{})
	close(writableFileUploadDelay)

	uploader := localFileUploader{
		context:                   ctx,
		contentAddressableStorage: contentAddressableStorage,
		digestFunction:            digestFunction,
		outputBaseID:              op.outputBaseID,
		writableFileUploadDelay:   writableFileUploadDelay,
	}
	err1 := uploader.uploadLocalFilesRecursive(op.OutputPath, nil)
	err2 := flusher(ctx)
	if err1 != nil {
		op.factory.errorLogger.Log(err1)
	} else if err2 != nil {
		op.factory.errorLogger.Log(util.StatusWrapf(err2, "Failed to upload the contents of output path %#v", op.outputBaseID.String()))
	}
}

type localFileUploader struct {
	context                   context.Context
	contentAddressableStorage blobstore.BlobAccess
	digestFunction            digest.Function
	outputBaseID              path.Component
	writableFileUploadDelay   <-chan struct{}
}

func (u *localFileUploader) uploadLocalFilesRecursive(d virtual.PrepopulatedDirectory, dPath *path.Trace) error {
	directories, leaves, err := d.LookupAllChildren()
	if err != nil {
		return util.StatusWrapf(err, "Failed to look up children of directory %#v in output path %#v", dPath.GetUNIXString(), u.outputBaseID.String())
	}
	for _, entry := range directories {
		err := u.uploadLocalFilesRecursive(entry.Child, dPath.Append(entry.Name))
		if err != nil {
			return err
		}
	}
	for _, entry := range leaves {
		p := virtual.ApplyUploadFile{
			Context:                   u.context,
			ContentAddressableStorage: u.contentAddressableStorage,
			DigestFunction:            u.digestFunction,
			WritableFileUploadDelay:   u.writableFileUploadDelay,
		}
		if !entry.Child.VirtualApply(&p) {
			panic("output path contains leaves that don't support ApplyUploadFile")
		}
		if p.Err != nil && status.Code(p.Err) != codes.InvalidArgument {
			return util.StatusWrapf(p.Err, "Failed to upload local file %#v in output path %#v", dPath.Append(entry.Name).GetUNIXString(), u.outputBaseID.String())
		}
	}
	return nil
}
