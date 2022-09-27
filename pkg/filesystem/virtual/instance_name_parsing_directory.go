package virtual

import (
	"context"
	"io"

	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

type instanceNameParsingPath []string

// WriteTo emits a parsed instance name path string as an identifier for
// StatelessHandleAllocator. As StatelessHandleAllocator requires that
// identifiers are length terminated or properly terminated to prevent
// hash collisions, we add a redundant slash to indicate the end of
// string.
func (p instanceNameParsingPath) WriteTo(w io.Writer) (nTotal int64, err error) {
	for _, component := range p {
		n, _ := w.Write([]byte(component))
		nTotal += int64(n)

		n, _ = w.Write([]byte("/"))
		nTotal += int64(n)
	}

	n, _ := w.Write([]byte("/"))
	nTotal += int64(n)
	return
}

// InstanceNameLookupFunc is called by directories created using
// NewInstanceNameParsingDirectory to complete the parsing of an
// instance name. The callback can yield a directory corresponding to the
// instance name.
type InstanceNameLookupFunc func(instanceName digest.InstanceName) virtual.Directory

type instanceNameParsingDirectoryOptions struct {
	handleAllocator virtual.StatelessHandleAllocator
	lookupFuncs     map[path.Component]InstanceNameLookupFunc
}

func (o *instanceNameParsingDirectoryOptions) createDirectory(path instanceNameParsingPath) virtual.Directory {
	return o.handleAllocator.
		New(path).
		AsStatelessDirectory(&instanceNameParsingDirectory{
			options: o,
			path:    path,
		})
}

type instanceNameParsingDirectory struct {
	nonIterableDirectory
	virtual.ReadOnlyDirectory

	options *instanceNameParsingDirectoryOptions
	path    instanceNameParsingPath
}

// NewInstanceNameParsingDirectory creates a directory that can be
// exposed through FUSE that parses REv2 instance names.
//
// Because instance names are path-like and can contain a variable
// number of pathname components (zero or more), the path must be
// terminated with one of the REv2 reserved keywords ("blobs",
// "uploads", etc.). For each of these reserved keywords, a separate
// callback can be provided to complete the parsing.
func NewInstanceNameParsingDirectory(handleAllocation virtual.StatelessHandleAllocation, lookupFuncs map[path.Component]InstanceNameLookupFunc) virtual.Directory {
	options := &instanceNameParsingDirectoryOptions{
		handleAllocator: handleAllocation.AsStatelessAllocator(),
		lookupFuncs:     lookupFuncs,
	}
	return options.createDirectory(instanceNameParsingPath{})
}

func (d *instanceNameParsingDirectory) VirtualGetAttributes(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
	attributes.SetChangeID(0)
	attributes.SetFileType(filesystem.FileTypeDirectory)
	attributes.SetLinkCount(virtual.ImplicitDirectoryLinkCount)
	attributes.SetPermissions(virtual.PermissionsExecute)
	attributes.SetSizeBytes(0)
}

func (d *instanceNameParsingDirectory) VirtualLookup(ctx context.Context, name path.Component, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.DirectoryChild, virtual.Status) {
	if lookupFunc, ok := d.options.lookupFuncs[name]; ok {
		// A reserved keyword that terminates the instance name
		// has been provided. Instantiate the target.
		instanceName, err := digest.NewInstanceNameFromComponents(d.path)
		if err != nil {
			return virtual.DirectoryChild{}, virtual.StatusErrNoEnt
		}
		directory := lookupFunc(instanceName)
		directory.VirtualGetAttributes(ctx, requested, out)
		return virtual.DirectoryChild{}.FromDirectory(directory), virtual.StatusOK
	}

	// A regular pathname component of the instance name. Create a
	// new directory that continues parsing.
	childPath := append(append(instanceNameParsingPath{}, d.path...), name.String())
	child := d.options.createDirectory(childPath)
	child.VirtualGetAttributes(ctx, requested, out)
	return virtual.DirectoryChild{}.FromDirectory(child), virtual.StatusOK
}

func (d *instanceNameParsingDirectory) VirtualOpenChild(ctx context.Context, name path.Component, shareAccess virtual.ShareMask, createAttributes *virtual.Attributes, existingOptions *virtual.OpenExistingOptions, requested virtual.AttributesMask, openedFileAttributes *virtual.Attributes) (virtual.Leaf, virtual.AttributesMask, virtual.ChangeInfo, virtual.Status) {
	// This directory type resolves all names, and all children are
	// directories. This means that this should always fail with
	// either EEXIST or EISDIR.
	return virtual.ReadOnlyDirectoryOpenChildWrongFileType(existingOptions, virtual.StatusErrIsDir)
}
