package fuse

import (
	re_fuse "github.com/buildbarn/bb-remote-execution/pkg/filesystem/fuse"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// readOnlyDirectory can be embedded into a re_fuse.Directory to disable
// all operations that mutate the directory contents.
type readOnlyDirectory struct{}

func (d readOnlyDirectory) FUSECreate(name path.Component, flags, mode uint32, out *fuse.Attr) (re_fuse.Leaf, fuse.Status) {
	return nil, fuse.EROFS
}

func (d readOnlyDirectory) FUSELink(name path.Component, leaf re_fuse.Leaf, out *fuse.Attr) fuse.Status {
	return fuse.EROFS
}

func (d readOnlyDirectory) FUSEMkdir(name path.Component, mode uint32, out *fuse.Attr) (re_fuse.Directory, fuse.Status) {
	return nil, fuse.EROFS
}

func (d readOnlyDirectory) FUSEMknod(name path.Component, mode, dev uint32, out *fuse.Attr) (re_fuse.Leaf, fuse.Status) {
	return nil, fuse.EROFS
}

func (d readOnlyDirectory) FUSERename(oldName path.Component, newDirectory re_fuse.Directory, newName path.Component) fuse.Status {
	return fuse.EROFS
}

func (d readOnlyDirectory) FUSERmdir(name path.Component) fuse.Status {
	return fuse.EROFS
}

func (d readOnlyDirectory) FUSESetAttr(in *fuse.SetAttrIn, out *fuse.Attr) fuse.Status {
	return fuse.EROFS
}

func (d readOnlyDirectory) FUSESymlink(pointedTo string, linkName path.Component, out *fuse.Attr) (re_fuse.Leaf, fuse.Status) {
	return nil, fuse.EROFS
}

func (d readOnlyDirectory) FUSEUnlink(name path.Component) fuse.Status {
	return fuse.EROFS
}
