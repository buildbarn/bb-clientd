package virtual

import (
	"context"
	"io"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	re_cas "github.com/buildbarn/bb-remote-execution/pkg/cas"
	re_vfs "github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type decomposedCASDirectoryFactory struct {
	re_vfs.CASFileFactory

	context          context.Context
	directoryFetcher re_cas.DirectoryFetcher
	handleAllocator  *re_vfs.ResolvableDigestHandleAllocator
	errorLogger      util.ErrorLogger
}

// NewDecomposedCASDirectoryFactory creates a CASDirectoryFactory that
// is capable of creating directories that are backed by individual REv2
// Directory messages that are stored in a Content Addressable Storage
// (CAS).
func NewDecomposedCASDirectoryFactory(ctx context.Context, casFileFactory re_vfs.CASFileFactory, directoryFetcher re_cas.DirectoryFetcher, handleAllocation re_vfs.StatelessHandleAllocation, errorLogger util.ErrorLogger) CASDirectoryFactory {
	cdf := &decomposedCASDirectoryFactory{
		CASFileFactory:   casFileFactory,
		context:          ctx,
		directoryFetcher: directoryFetcher,
		errorLogger:      errorLogger,
	}
	cdf.handleAllocator = re_vfs.NewResolvableDigestHandleAllocator(handleAllocation, cdf.resolve)
	return cdf
}

// LookupDirectory creates a directory corresponding with the
// remoteexecution.Digest message of a given digest.
func (cdf *decomposedCASDirectoryFactory) LookupDirectory(blobDigest digest.Digest) re_vfs.Directory {
	d, _ := cdf.createDirectory(blobDigest)
	return d
}

func (cdf *decomposedCASDirectoryFactory) createDirectory(blobDigest digest.Digest) (re_vfs.Directory, re_vfs.HandleResolver) {
	return NewCASDirectory(
		&decomposedCASDirectoryContext{
			decomposedCASDirectoryFactory: cdf,
			digest:                        blobDigest,
		},
		blobDigest.GetDigestFunction(),
		cdf.handleAllocator.New(blobDigest),
		uint64(blobDigest.GetSizeBytes()))
}

func (cdf *decomposedCASDirectoryFactory) resolve(blobDigest digest.Digest, r io.ByteReader) (re_vfs.DirectoryChild, re_vfs.Status) {
	_, handleResolver := cdf.createDirectory(blobDigest)
	return handleResolver(r)
}

// decomposedCASDirectoryContext contains the state associated with an
// instance of CASDirectory.
type decomposedCASDirectoryContext struct {
	*decomposedCASDirectoryFactory

	digest digest.Digest
}

func (cdc *decomposedCASDirectoryContext) GetDirectoryContents() (*remoteexecution.Directory, re_vfs.Status) {
	directory, err := cdc.directoryFetcher.GetDirectory(cdc.context, cdc.digest)
	if err != nil {
		cdc.LogError(err)
		return nil, re_vfs.StatusErrIO
	}
	return directory, re_vfs.StatusOK
}

func (cdc *decomposedCASDirectoryContext) LogError(err error) {
	cdc.errorLogger.Log(util.StatusWrapf(err, "Directory %#v", cdc.digest.String()))
}
