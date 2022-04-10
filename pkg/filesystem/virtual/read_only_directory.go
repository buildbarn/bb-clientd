package virtual

import (
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

// readOnlyDirectory can be embedded into a virtual.Directory to disable
// all operations that mutate the directory contents.
type readOnlyDirectory struct{}

func (d readOnlyDirectory) VirtualLink(name path.Component, leaf virtual.Leaf, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.ChangeInfo, virtual.Status) {
	return virtual.ChangeInfo{}, virtual.StatusErrROFS
}

func (d readOnlyDirectory) VirtualMkdir(name path.Component, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Directory, virtual.ChangeInfo, virtual.Status) {
	return nil, virtual.ChangeInfo{}, virtual.StatusErrROFS
}

func (d readOnlyDirectory) VirtualMknod(name path.Component, fileType filesystem.FileType, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Leaf, virtual.ChangeInfo, virtual.Status) {
	return nil, virtual.ChangeInfo{}, virtual.StatusErrROFS
}

func (d readOnlyDirectory) VirtualRename(oldName path.Component, newDirectory virtual.Directory, newName path.Component) (virtual.ChangeInfo, virtual.ChangeInfo, virtual.Status) {
	return virtual.ChangeInfo{}, virtual.ChangeInfo{}, virtual.StatusErrROFS
}

func (d readOnlyDirectory) VirtualRemove(name path.Component, removeDirectory, removeLeaf bool) (virtual.ChangeInfo, virtual.Status) {
	return virtual.ChangeInfo{}, virtual.StatusErrROFS
}

func (d readOnlyDirectory) VirtualSetAttributes(in *virtual.Attributes, requested virtual.AttributesMask, out *virtual.Attributes) virtual.Status {
	return virtual.StatusErrROFS
}

func (d readOnlyDirectory) VirtualSymlink(pointedTo []byte, linkName path.Component, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Leaf, virtual.ChangeInfo, virtual.Status) {
	return nil, virtual.ChangeInfo{}, virtual.StatusErrROFS
}

// virtualOpenChildWrongFileType is a helper function for implementing
// Directory.VirtualOpenChild() for read-only directories. It can be
// used to obtain return values in case the directory already contains a
// file under a given name.
func virtualOpenChildWrongFileType(existingOptions *virtual.OpenExistingOptions, s virtual.Status) (virtual.Leaf, virtual.AttributesMask, virtual.ChangeInfo, virtual.Status) {
	if existingOptions == nil {
		return nil, 0, virtual.ChangeInfo{}, virtual.StatusErrExist
	}
	return nil, 0, virtual.ChangeInfo{}, s
}

// virtualOpenChildDoesntExist is a helper function for implementing
// Directory.VirtualOpenChild() for read-only directories. It can be
// used to obtain return values in case the directory doesn't contains
// any file under a given name.
func virtualOpenChildDoesntExist(createAttributes *virtual.Attributes) (virtual.Leaf, virtual.AttributesMask, virtual.ChangeInfo, virtual.Status) {
	if createAttributes == nil {
		return nil, 0, virtual.ChangeInfo{}, virtual.StatusErrNoEnt
	}
	return nil, 0, virtual.ChangeInfo{}, virtual.StatusErrROFS
}
