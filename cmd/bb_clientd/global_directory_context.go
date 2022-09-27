package main

import (
	"context"
	"io"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	cd_vfs "github.com/buildbarn/bb-clientd/pkg/filesystem/virtual"
	re_cas "github.com/buildbarn/bb-remote-execution/pkg/cas"
	re_vfs "github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
)

// GlobalDirectoryContext contains all attributes that are needed to
// construct Content Addressable Storage (CAS) backed directories.
// Directory contents are read from remoteexecution.Directory messages.
type GlobalDirectoryContext struct {
	re_vfs.CASFileFactory

	context          context.Context
	directoryFetcher re_cas.DirectoryFetcher
	handleAllocator  *re_vfs.ResolvableDigestHandleAllocator
	errorLogger      util.ErrorLogger
}

// NewGlobalDirectoryContext creates a new GlobalDirectoryContext.
// Because directories created through GlobalDirectoryContext can
// contain files, a CASFileFactory needs to be provided.
// remoteexecution.Directory messages are read through a
// DirectoryFetcher.
func NewGlobalDirectoryContext(ctx context.Context, casFileFactory re_vfs.CASFileFactory, directoryFetcher re_cas.DirectoryFetcher, handleAllocation re_vfs.StatelessHandleAllocation, errorLogger util.ErrorLogger) *GlobalDirectoryContext {
	gdc := &GlobalDirectoryContext{
		CASFileFactory:   casFileFactory,
		context:          ctx,
		directoryFetcher: directoryFetcher,
		errorLogger:      errorLogger,
	}
	gdc.handleAllocator = re_vfs.NewResolvableDigestHandleAllocator(handleAllocation, gdc.resolve)
	return gdc
}

// LookupDirectory creates a directory corresponding with the
// remoteexecution.Digest message of a given digest.
func (gdc *GlobalDirectoryContext) LookupDirectory(blobDigest digest.Digest) re_vfs.Directory {
	d, _ := gdc.createDirectory(blobDigest)
	return d
}

func (gdc *GlobalDirectoryContext) createDirectory(blobDigest digest.Digest) (re_vfs.Directory, re_vfs.HandleResolver) {
	return cd_vfs.NewContentAddressableStorageDirectory(
		&directoryContext{
			GlobalDirectoryContext: gdc,
			digest:                 blobDigest,
		},
		blobDigest.GetInstanceName(),
		gdc.handleAllocator.New(blobDigest),
		uint64(blobDigest.GetSizeBytes()))
}

func (gdc *GlobalDirectoryContext) resolve(blobDigest digest.Digest, r io.ByteReader) (re_vfs.DirectoryChild, re_vfs.Status) {
	_, handleResolver := gdc.createDirectory(blobDigest)
	return handleResolver(r)
}

// directoryContext contains the state associated with an instance of
// ContentAddressableStorageDirectory.
type directoryContext struct {
	*GlobalDirectoryContext

	digest digest.Digest
}

func (dc *directoryContext) GetDirectoryContents() (*remoteexecution.Directory, re_vfs.Status) {
	directory, err := dc.directoryFetcher.GetDirectory(dc.context, dc.digest)
	if err != nil {
		dc.LogError(err)
		return nil, re_vfs.StatusErrIO
	}
	return directory, re_vfs.StatusOK
}

func (dc *directoryContext) LogError(err error) {
	dc.errorLogger.Log(util.StatusWrapf(err, "Directory %#v", dc.digest.String()))
}
