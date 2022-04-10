package virtual

import (
	"sort"

	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

type staticDirectoryEntry struct {
	name  path.Component
	child virtual.Directory
}

type staticDirectoryEntryList []staticDirectoryEntry

func (l staticDirectoryEntryList) Len() int {
	return len(l)
}

func (l staticDirectoryEntryList) Less(i, j int) bool {
	return l[i].name.String() < l[j].name.String()
}

func (l staticDirectoryEntryList) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

type staticDirectory struct {
	readOnlyDirectory

	entries []staticDirectoryEntry
}

// NewStaticDirectory creates a Directory that contains a hardcoded list
// of child directories. The contents of this directory are immutable.
func NewStaticDirectory(directories map[path.Component]virtual.Directory) virtual.Directory {
	// Place all directory entries in a sorted list. This allows us
	// to do lookups by performing a binary search, while also
	// making it possible to implement readdir() deterministically.
	entries := make(staticDirectoryEntryList, 0, len(directories))
	for name, child := range directories {
		entries = append(entries, staticDirectoryEntry{
			name:  name,
			child: child,
		})
	}
	sort.Sort(entries)

	return &staticDirectory{
		entries: entries,
	}
}

func (d *staticDirectory) VirtualGetAttributes(requested virtual.AttributesMask, attributes *virtual.Attributes) {
	attributes.SetChangeID(0)
	attributes.SetFileType(filesystem.FileTypeDirectory)
	attributes.SetLinkCount(virtual.EmptyDirectoryLinkCount + uint32(len(d.entries)))
	attributes.SetPermissions(virtual.PermissionsRead | virtual.PermissionsExecute)
	attributes.SetSizeBytes(0)
}

func (d *staticDirectory) VirtualLookup(name path.Component, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Directory, virtual.Leaf, virtual.Status) {
	if i := sort.Search(len(d.entries), func(i int) bool {
		return d.entries[i].name.String() >= name.String()
	}); i < len(d.entries) && d.entries[i].name == name {
		child := d.entries[i].child
		child.VirtualGetAttributes(requested, out)
		return child, nil, virtual.StatusOK
	}
	return nil, nil, virtual.StatusErrNoEnt
}

func (d *staticDirectory) VirtualOpenChild(name path.Component, shareAccess virtual.ShareMask, createAttributes *virtual.Attributes, existingOptions *virtual.OpenExistingOptions, requested virtual.AttributesMask, openedFileAttributes *virtual.Attributes) (virtual.Leaf, virtual.AttributesMask, virtual.ChangeInfo, virtual.Status) {
	if i := sort.Search(len(d.entries), func(i int) bool {
		return d.entries[i].name.String() >= name.String()
	}); i < len(d.entries) && d.entries[i].name == name {
		return virtualOpenChildWrongFileType(existingOptions, virtual.StatusErrIsDir)
	}
	return virtualOpenChildDoesntExist(createAttributes)
}

func (d *staticDirectory) VirtualReadDir(firstCookie uint64, requested virtual.AttributesMask, reporter virtual.DirectoryEntryReporter) virtual.Status {
	for i := firstCookie; i < uint64(len(d.entries)); i++ {
		entry := d.entries[i]
		var attributes virtual.Attributes
		entry.child.VirtualGetAttributes(requested, &attributes)
		if !reporter.ReportDirectory(i+1, entry.name, entry.child, &attributes) {
			break
		}
	}
	return virtual.StatusOK
}
