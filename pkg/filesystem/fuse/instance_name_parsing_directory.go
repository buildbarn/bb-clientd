package fuse

import (
	re_fuse "github.com/buildbarn/bb-remote-execution/pkg/filesystem/fuse"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// InstanceNameLookupFunc is called by directories created using
// NewInstanceNameParsingDirectory to complete the parsing of an
// instance name. The callback can yield a directory or file
// corresponding with the instance name.
type InstanceNameLookupFunc func(instanceName digest.InstanceName, out *fuse.Attr) (re_fuse.Directory, re_fuse.Leaf)

type instanceNameParsingDirectory struct {
	nonIterableDirectory
	readOnlyDirectory

	path            []string
	inodeNumberTree re_fuse.InodeNumberTree
	lookupFuncs     map[path.Component]InstanceNameLookupFunc
}

// NewInstanceNameParsingDirectory creates a directory that can be
// exposed through FUSE that parses REv2 instance names.
//
// Because instance names are path-like and can contain a variable
// number of pathname components (zero or more), the path must be
// terminated with one of the REv2 reserved keywords ("blobs",
// "uploads", etc.). For each of these reserved keywords, a separate
// callback can be provided to complete the parsing.
func NewInstanceNameParsingDirectory(inodeNumberTree re_fuse.InodeNumberTree, lookupFuncs map[path.Component]InstanceNameLookupFunc) re_fuse.Directory {
	return &instanceNameParsingDirectory{
		inodeNumberTree: inodeNumberTree,
		lookupFuncs:     lookupFuncs,
	}
}

func (d *instanceNameParsingDirectory) FUSEGetAttr(out *fuse.Attr) {
	out.Ino = d.inodeNumberTree.Get()
	out.Mode = fuse.S_IFDIR | 0111
	out.Nlink = re_fuse.ImplicitDirectoryLinkCount
}

func (d *instanceNameParsingDirectory) FUSELookup(name path.Component, out *fuse.Attr) (re_fuse.Directory, re_fuse.Leaf, fuse.Status) {
	if lookupFunc, ok := d.lookupFuncs[name]; ok {
		// A reserved keyword that terminates the instance name
		// has been provided. Instantiate the target.
		instanceName, err := digest.NewInstanceNameFromComponents(d.path)
		if err != nil {
			return nil, nil, fuse.ENOENT
		}
		directory, leaf := lookupFunc(instanceName, out)
		return directory, leaf, fuse.OK
	}

	// A regular pathname component of the instance name. Create a
	// new directory that continues parsing.
	n := name.String()
	child := &instanceNameParsingDirectory{
		path:            append(append([]string{}, d.path...), n),
		inodeNumberTree: d.inodeNumberTree.AddString(n),
		lookupFuncs:     d.lookupFuncs,
	}
	child.FUSEGetAttr(out)
	return child, nil, fuse.OK
}
