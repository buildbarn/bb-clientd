package main

import (
	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	cd_fuse "github.com/buildbarn/bb-clientd/pkg/filesystem/fuse"
	re_cas "github.com/buildbarn/bb-remote-execution/pkg/cas"
	re_fuse "github.com/buildbarn/bb-remote-execution/pkg/filesystem/fuse"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// GlobalDirectoryContext contains all attributes that are needed to
// construct Content Addressable Storage (CAS) backed directories.
// Directory contents are read from remoteexecution.Directory messages.
type GlobalDirectoryContext struct {
	*GlobalFileContext

	directoryFetcher         re_cas.DirectoryFetcher
	directoryInodeNumberTree re_fuse.InodeNumberTree
}

// NewGlobalDirectoryContext creates a new GlobalDirectoryContext.
// Because directories created through GlobalDirectoryContext can
// contain files, a GlobalFileContext needs to be provided.
// remoteexecution.Directory messages are read through a
// DirectoryFetcher.
func NewGlobalDirectoryContext(globalFileContext *GlobalFileContext, directoryFetcher re_cas.DirectoryFetcher) *GlobalDirectoryContext {
	return &GlobalDirectoryContext{
		GlobalFileContext:        globalFileContext,
		directoryFetcher:         directoryFetcher,
		directoryInodeNumberTree: re_fuse.NewRandomInodeNumberTree(),
	}
}

// GetDirectoryInodeNumber computes the inode number of a directory
// given digest without explicitly instantiating the directory. This is
// used to efficiently generate listings of directories containing these
// directories.
func (gdc *GlobalDirectoryContext) GetDirectoryInodeNumber(blobDigest digest.Digest) uint64 {
	return gdc.directoryInodeNumberTree.AddString(blobDigest.GetKey(digest.KeyWithInstance)).Get()
}

// LookupDirectory creates a directory corresponding with the
// remoteexecution.Digest message of a given digest.
func (gdc *GlobalDirectoryContext) LookupDirectory(blobDigest digest.Digest, out *fuse.Attr) re_fuse.Directory {
	d := cd_fuse.NewContentAddressableStorageDirectory(
		&directoryContext{
			GlobalDirectoryContext: gdc,
			digest:                 blobDigest,
		},
		blobDigest.GetInstanceName(),
		gdc.GetDirectoryInodeNumber(blobDigest))
	d.FUSEGetAttr(out)
	return d
}

// directoryContext contains the state associated with an instance of
// ContentAddressableStorageDirectory.
type directoryContext struct {
	*GlobalDirectoryContext

	digest digest.Digest
}

func (dc *directoryContext) GetDirectoryContents() (*remoteexecution.Directory, fuse.Status) {
	directory, err := dc.directoryFetcher.GetDirectory(dc.context, dc.digest)
	if err != nil {
		dc.LogError(err)
		return nil, fuse.EIO
	}
	return directory, fuse.OK
}

func (dc *directoryContext) LogError(err error) {
	dc.errorLogger.Log(util.StatusWrapf(err, "Directory %#v", dc.digest.String()))
}
