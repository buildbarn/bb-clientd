package fuse

import (
	re_fuse "github.com/buildbarn/bb-remote-execution/pkg/filesystem/fuse"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// StaticDirectoryEntry contains all the properties of a single
// directory entry that is part of a Directory created using
// NewStaticDirectory().
type StaticDirectoryEntry struct {
	Child       re_fuse.Directory
	InodeNumber uint64
}

type staticDirectory struct {
	readOnlyDirectory

	inodeNumber uint64
	directories map[path.Component]StaticDirectoryEntry
}

// NewStaticDirectory creates a Directory that contains a hardcoded list
// of child directories. The contents of this directory are immutable.
func NewStaticDirectory(inodeNumber uint64, directories map[path.Component]StaticDirectoryEntry) re_fuse.Directory {
	return &staticDirectory{
		inodeNumber: inodeNumber,
		directories: directories,
	}
}

func (d *staticDirectory) FUSEAccess(mask uint32) fuse.Status {
	if mask&^(fuse.R_OK|fuse.X_OK) != 0 {
		return fuse.EACCES
	}
	return fuse.OK
}

func (d *staticDirectory) FUSEGetAttr(out *fuse.Attr) {
	out.Ino = d.inodeNumber
	out.Mode = fuse.S_IFDIR | 0555
	out.Nlink = uint32(re_fuse.EmptyDirectoryLinkCount + len(d.directories))
}

func (d *staticDirectory) FUSELookup(name path.Component, out *fuse.Attr) (re_fuse.Directory, re_fuse.Leaf, fuse.Status) {
	if child, ok := d.directories[name]; ok {
		child.Child.FUSEGetAttr(out)
		return child.Child, nil, fuse.OK
	}
	return nil, nil, fuse.ENOENT
}

func (d *staticDirectory) FUSEReadDir() ([]fuse.DirEntry, fuse.Status) {
	entries := make([]fuse.DirEntry, 0, len(d.directories))
	for name, child := range d.directories {
		entries = append(entries, fuse.DirEntry{
			Mode: fuse.S_IFDIR,
			Ino:  child.InodeNumber,
			Name: name.String(),
		})
	}
	return entries, fuse.OK
}

func (d *staticDirectory) FUSEReadDirPlus() ([]re_fuse.DirectoryDirEntry, []re_fuse.LeafDirEntry, fuse.Status) {
	directories := make([]re_fuse.DirectoryDirEntry, 0, len(d.directories))
	for name, child := range d.directories {
		directories = append(directories, re_fuse.DirectoryDirEntry{
			Child: child.Child,
			DirEntry: fuse.DirEntry{
				Mode: fuse.S_IFDIR,
				Ino:  child.InodeNumber,
				Name: name.String(),
			},
		})
	}
	return directories, nil, fuse.OK
}
