package main

import (
	"context"

	re_fuse "github.com/buildbarn/bb-remote-execution/pkg/filesystem/fuse"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/util"
)

// GlobalFileContext contains all of the attributes that are needed to
// construct Content Addressable Storage (CAS) backed files. Files can
// either be plain or executable.
//
// It is possible to create files having the same digest multiple times,
// but they will share the same inode number. This ensures that the
// kernel deduplicates them, thereby giving a better page cache hit
// rate.
//
// TODO: This type feels a bit redundant now. Should we remove it?
type GlobalFileContext struct {
	re_fuse.CASFileFactory

	context                   context.Context
	contentAddressableStorage blobstore.BlobAccess
	errorLogger               util.ErrorLogger
}

// NewGlobalFileContext creates a new GlobalFileContext. All files
// created through the GlobalFileContext will attempt to load data
// through a provided BlobAccess. Any I/O errors are logged through an
// ErrorLogger.
func NewGlobalFileContext(ctx context.Context, contentAddressableStorage blobstore.BlobAccess, errorLogger util.ErrorLogger) *GlobalFileContext {
	return &GlobalFileContext{
		CASFileFactory:            re_fuse.NewBlobAccessCASFileFactory(ctx, contentAddressableStorage, errorLogger),
		context:                   ctx,
		contentAddressableStorage: contentAddressableStorage,
		errorLogger:               errorLogger,
	}
}
