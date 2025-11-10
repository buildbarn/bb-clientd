package virtual

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"sort"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CASDirectoryContext contains all of the methods that the directory
// created by NewCASDirectory uses to obtain its contents and
// instantiate inodes for its children.
type CASDirectoryContext interface {
	CASDirectoryFactory
	virtual.CASFileFactory

	GetDirectoryContents() (*remoteexecution.Directory, virtual.Status)
	LogError(err error)
}

type casDirectory struct {
	virtual.ReadOnlyDirectory

	directoryContext CASDirectoryContext
	digestFunction   digest.Function
	handleAllocator  virtual.ResolvableHandleAllocator
	sizeBytes        uint64
}

// NewCASDirectory creates an immutable directory that is backed by a
// Directory message stored in the Content Addressable Storage (CAS). In
// order to load the Directory message and to instantiate inodes for any
// of its children, calls are made into a CASDirectoryContext object.
//
// TODO: Reimplement this on top of cas.DirectoryWalker.
func NewCASDirectory(directoryContext CASDirectoryContext, digestFunction digest.Function, handleAllocation virtual.ResolvableHandleAllocation, sizeBytes uint64) (virtual.Directory, virtual.HandleResolver) {
	d := &casDirectory{
		directoryContext: directoryContext,
		digestFunction:   digestFunction,
		sizeBytes:        sizeBytes,
	}
	d.handleAllocator = handleAllocation.AsResolvableAllocator(d.resolveHandle)
	return d.createSelf(), d.resolveHandle
}

func (d *casDirectory) createSelf() virtual.Directory {
	return d.handleAllocator.New(bytes.NewBuffer([]byte{0})).AsStatelessDirectory(d)
}

func (d *casDirectory) createSymlink(index uint64, target string) virtual.LinkableLeaf {
	var encodedIndex [binary.MaxVarintLen64]byte
	return d.handleAllocator.
		New(bytes.NewBuffer(encodedIndex[:binary.PutUvarint(encodedIndex[:], index+1)])).
		AsLinkableLeaf(virtual.BaseSymlinkFactory.LookupSymlink([]byte(target)))
}

func (d *casDirectory) resolveHandle(r io.ByteReader) (virtual.DirectoryChild, virtual.Status) {
	index, err := binary.ReadUvarint(r)
	if err != nil {
		return virtual.DirectoryChild{}, virtual.StatusErrBadHandle
	}
	if index == 0 {
		return virtual.DirectoryChild{}.FromDirectory(d.createSelf()), virtual.StatusOK
	}
	// If a suffix is provided, it corresponds to a symbolic link
	// inside this directory. Files and directories will be
	// resolvable through other means.
	directory, s := d.directoryContext.GetDirectoryContents()
	if s != virtual.StatusOK {
		return virtual.DirectoryChild{}, s
	}
	index--
	if index >= uint64(len(directory.Symlinks)) {
		return virtual.DirectoryChild{}, virtual.StatusErrBadHandle
	}
	return virtual.DirectoryChild{}.FromLeaf(d.createSymlink(index, directory.Symlinks[index].Target)), virtual.StatusOK
}

func (d *casDirectory) VirtualGetAttributes(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
	attributes.SetChangeID(0)
	attributes.SetFileType(filesystem.FileTypeDirectory)
	attributes.SetHasNamedAttributes(false)
	attributes.SetIsInNamedAttributeDirectory(false)
	// This should be 2 + nDirectories, but that requires us to load
	// the directory. This is highly inefficient and error prone.
	attributes.SetLinkCount(virtual.ImplicitDirectoryLinkCount)
	attributes.SetPermissions(virtual.PermissionsRead | virtual.PermissionsExecute)
	attributes.SetSizeBytes(d.sizeBytes)
}

func (d *casDirectory) VirtualLookup(ctx context.Context, name path.Component, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.DirectoryChild, virtual.Status) {
	directory, s := d.directoryContext.GetDirectoryContents()
	if s != virtual.StatusOK {
		return virtual.DirectoryChild{}, s
	}

	// The Remote Execution protocol requires that entries stored in
	// a Directory message are sorted alphabetically. Make use of
	// this fact by performing binary searching when looking up
	// entries. There is no need to explicitly index the entries.
	n := name.String()
	directories := directory.Directories
	if i := sort.Search(len(directories), func(i int) bool { return directories[i].Name >= n }); i < len(directories) && directories[i].Name == n {
		entryDigest, err := d.digestFunction.NewDigestFromProto(directories[i].Digest)
		if err != nil {
			d.directoryContext.LogError(util.StatusWrapf(err, "Failed to parse digest for directory %#v", n))
			return virtual.DirectoryChild{}, virtual.StatusErrIO
		}
		child := d.directoryContext.LookupDirectory(entryDigest)
		child.VirtualGetAttributes(ctx, requested, out)
		return virtual.DirectoryChild{}.FromDirectory(child), virtual.StatusOK
	}

	files := directory.Files
	if i := sort.Search(len(files), func(i int) bool { return files[i].Name >= n }); i < len(files) && files[i].Name == n {
		entry := files[i]
		entryDigest, err := d.digestFunction.NewDigestFromProto(entry.Digest)
		if err != nil {
			d.directoryContext.LogError(util.StatusWrapf(err, "Failed to parse digest for file %#v", n))
			return virtual.DirectoryChild{}, virtual.StatusErrIO
		}
		child := d.directoryContext.LookupFile(entryDigest, entry.IsExecutable, nil)
		child.VirtualGetAttributes(ctx, requested, out)
		return virtual.DirectoryChild{}.FromLeaf(child), virtual.StatusOK
	}

	symlinks := directory.Symlinks
	if i := sort.Search(len(symlinks), func(i int) bool { return symlinks[i].Name >= n }); i < len(symlinks) && symlinks[i].Name == n {
		f := d.createSymlink(uint64(i), symlinks[i].Target)
		f.VirtualGetAttributes(ctx, requested, out)
		return virtual.DirectoryChild{}.FromLeaf(f), virtual.StatusOK
	}

	return virtual.DirectoryChild{}, virtual.StatusErrNoEnt
}

func (d *casDirectory) VirtualOpenChild(ctx context.Context, name path.Component, shareAccess virtual.ShareMask, createAttributes *virtual.Attributes, existingOptions *virtual.OpenExistingOptions, requested virtual.AttributesMask, openedFileAttributes *virtual.Attributes) (virtual.Leaf, virtual.AttributesMask, virtual.ChangeInfo, virtual.Status) {
	directory, s := d.directoryContext.GetDirectoryContents()
	if s != virtual.StatusOK {
		return nil, 0, virtual.ChangeInfo{}, s
	}

	n := name.String()
	directories := directory.Directories
	if i := sort.Search(len(directories), func(i int) bool { return directories[i].Name >= n }); i < len(directories) && directories[i].Name == n {
		return virtual.ReadOnlyDirectoryOpenChildWrongFileType(existingOptions, virtual.StatusErrIsDir)
	}

	files := directory.Files
	if i := sort.Search(len(files), func(i int) bool { return files[i].Name >= n }); i < len(files) && files[i].Name == n {
		if existingOptions == nil {
			return nil, 0, virtual.ChangeInfo{}, virtual.StatusErrExist
		}

		entry := files[i]
		entryDigest, err := d.digestFunction.NewDigestFromProto(entry.Digest)
		if err != nil {
			d.directoryContext.LogError(util.StatusWrapf(err, "Failed to parse digest for file %#v", n))
			return nil, 0, virtual.ChangeInfo{}, virtual.StatusErrIO
		}
		leaf := d.directoryContext.LookupFile(entryDigest, entry.IsExecutable, nil)
		s := leaf.VirtualOpenSelf(ctx, shareAccess, existingOptions, requested, openedFileAttributes)
		return leaf, existingOptions.ToAttributesMask(), virtual.ChangeInfo{}, s
	}

	symlinks := directory.Symlinks
	if i := sort.Search(len(symlinks), func(i int) bool { return symlinks[i].Name >= n }); i < len(symlinks) && symlinks[i].Name == n {
		return virtual.ReadOnlyDirectoryOpenChildWrongFileType(existingOptions, virtual.StatusErrSymlink)
	}

	return virtual.ReadOnlyDirectoryOpenChildDoesntExist(createAttributes)
}

func (d *casDirectory) VirtualReadDir(ctx context.Context, firstCookie uint64, requested virtual.AttributesMask, reporter virtual.DirectoryEntryReporter) virtual.Status {
	directory, s := d.directoryContext.GetDirectoryContents()
	if s != virtual.StatusOK {
		return s
	}

	i := firstCookie
	nextCookieOffset := uint64(1)

	for ; i < uint64(len(directory.Directories)); i++ {
		entry := directory.Directories[i]
		name, ok := path.NewComponent(entry.Name)
		if !ok {
			d.directoryContext.LogError(status.Errorf(codes.InvalidArgument, "Directory %#v has an invalid name", entry.Name))
			return virtual.StatusErrIO
		}
		entryDigest, err := d.digestFunction.NewDigestFromProto(entry.Digest)
		if err != nil {
			d.directoryContext.LogError(util.StatusWrapf(err, "Failed to parse digest for directory %#v", entry.Name))
			return virtual.StatusErrIO
		}
		child := d.directoryContext.LookupDirectory(entryDigest)
		var attributes virtual.Attributes
		child.VirtualGetAttributes(ctx, requested, &attributes)
		if !reporter.ReportEntry(nextCookieOffset+i, name, virtual.DirectoryChild{}.FromDirectory(child), &attributes) {
			return virtual.StatusOK
		}
	}
	i -= uint64(len(directory.Directories))
	nextCookieOffset += uint64(len(directory.Directories))

	for ; i < uint64(len(directory.Files)); i++ {
		entry := directory.Files[i]
		name, ok := path.NewComponent(entry.Name)
		if !ok {
			d.directoryContext.LogError(status.Errorf(codes.InvalidArgument, "File %#v has an invalid name", entry.Name))
			return virtual.StatusErrIO
		}
		entryDigest, err := d.digestFunction.NewDigestFromProto(entry.Digest)
		if err != nil {
			d.directoryContext.LogError(util.StatusWrapf(err, "Failed to parse digest for file %#v", entry.Name))
			return virtual.StatusErrIO
		}
		child := d.directoryContext.LookupFile(entryDigest, entry.IsExecutable, nil)
		var attributes virtual.Attributes
		child.VirtualGetAttributes(ctx, requested, &attributes)
		if !reporter.ReportEntry(nextCookieOffset+i, name, virtual.DirectoryChild{}.FromLeaf(child), &attributes) {
			return virtual.StatusOK
		}
	}
	i -= uint64(len(directory.Files))
	nextCookieOffset += uint64(len(directory.Files))

	for ; i < uint64(len(directory.Symlinks)); i++ {
		entry := directory.Symlinks[i]
		name, ok := path.NewComponent(entry.Name)
		if !ok {
			d.directoryContext.LogError(status.Errorf(codes.InvalidArgument, "Symbolic link %#v has an invalid name", entry.Name))
			return virtual.StatusErrIO
		}
		child := d.createSymlink(i, entry.Target)
		var attributes virtual.Attributes
		child.VirtualGetAttributes(ctx, requested, &attributes)
		if !reporter.ReportEntry(nextCookieOffset+i, name, virtual.DirectoryChild{}.FromLeaf(child), &attributes) {
			return virtual.StatusOK
		}
	}

	return virtual.StatusOK
}

func (casDirectory) VirtualApply(data any) bool {
	return false
}
