package fuse

import (
	re_fuse "github.com/buildbarn/bb-remote-execution/pkg/filesystem/fuse"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// nonIterableDirectory can be embedded into a re_fuse.Directory to
// disable the readdir() operation.
type nonIterableDirectory struct{}

func (d nonIterableDirectory) FUSEAccess(mask uint32) fuse.Status {
	if mask&^fuse.X_OK != 0 {
		return fuse.EACCES
	}
	return fuse.OK
}

func (d nonIterableDirectory) FUSEReadDir() ([]fuse.DirEntry, fuse.Status) {
	return nil, fuse.EACCES
}

func (d nonIterableDirectory) FUSEReadDirPlus() ([]re_fuse.DirectoryDirEntry, []re_fuse.LeafDirEntry, fuse.Status) {
	return nil, nil, fuse.EACCES
}
