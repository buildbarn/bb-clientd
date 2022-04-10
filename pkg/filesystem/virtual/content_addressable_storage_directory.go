package virtual

import (
	"bytes"
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

// DirectoryContentsContext can be used by
// ContentAddressableStorageDirectory's VirtualLookup() and
// VirtualReaddir() to resolve one of its child directories.
//
// This object is returned together with
// DirectoryContext.GetDirectoryContents(), allowing DirectoryContext to
// preserve state between that call and child directory lookups.
type DirectoryContentsContext interface {
	LookupDirectory(digest digest.Digest) (virtual.Directory, virtual.Status)
}

// DirectoryContext contains all of the methods that the directory
// created by NewContentAddressableStorageDirectory uses to obtain its
// contents and instantiate inodes for its children.
type DirectoryContext interface {
	GetDirectoryContents() (*remoteexecution.Directory, DirectoryContentsContext, virtual.Status)
	LogError(err error)

	// Same as the above, but for regular files.
	virtual.CASFileFactory
}

type contentAddressableStorageDirectory struct {
	readOnlyDirectory

	directoryContext DirectoryContext
	instanceName     digest.InstanceName
	handleAllocator  virtual.ResolvableHandleAllocator
	sizeBytes        uint64
}

// NewContentAddressableStorageDirectory creates an immutable directory
// that is backed by a Directory message stored in the Content
// Addressable Storage (CAS). In order to load the Directory message and
// to instantiate inodes for any of its children, calls are made into a
// DirectoryContext object.
//
// TODO: Reimplement this on top of cas.DirectoryWalker.
func NewContentAddressableStorageDirectory(directoryContext DirectoryContext, instanceName digest.InstanceName, handleAllocation virtual.ResolvableHandleAllocation, sizeBytes uint64) (virtual.Directory, virtual.HandleResolver) {
	d := &contentAddressableStorageDirectory{
		directoryContext: directoryContext,
		instanceName:     instanceName,
		sizeBytes:        sizeBytes,
	}
	d.handleAllocator = handleAllocation.AsResolvableAllocator(d.resolveHandle)
	return d.createSelf(), d.resolveHandle
}

func (d *contentAddressableStorageDirectory) createSelf() virtual.Directory {
	return d.handleAllocator.New(bytes.NewBuffer([]byte{0})).AsStatelessDirectory(d)
}

func (d *contentAddressableStorageDirectory) createSymlink(index uint64, target string) virtual.NativeLeaf {
	var encodedIndex [binary.MaxVarintLen64]byte
	return d.handleAllocator.
		New(bytes.NewBuffer(encodedIndex[:binary.PutUvarint(encodedIndex[:], index+1)])).
		AsNativeLeaf(virtual.BaseSymlinkFactory.LookupSymlink([]byte(target)))
}

func (d *contentAddressableStorageDirectory) resolveHandle(r io.ByteReader) (virtual.Directory, virtual.Leaf, virtual.Status) {
	index, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, nil, virtual.StatusErrBadHandle
	}
	if index == 0 {
		return d.createSelf(), nil, virtual.StatusOK
	}
	// If a suffix is provided, it corresponds to a symbolic link
	// inside this directory. Files and directories will be
	// resolvable through other means.
	directory, _, s := d.directoryContext.GetDirectoryContents()
	if s != virtual.StatusOK {
		return nil, nil, s
	}
	index--
	if index >= uint64(len(directory.Symlinks)) {
		return nil, nil, virtual.StatusErrBadHandle
	}
	return nil, d.createSymlink(index, directory.Symlinks[index].Target), virtual.StatusOK
}

func (d *contentAddressableStorageDirectory) VirtualGetAttributes(requested virtual.AttributesMask, attributes *virtual.Attributes) {
	attributes.SetChangeID(0)
	// This should be 2 + nDirectories, but that requires us to load
	// the directory. This is highly inefficient and error prone.
	attributes.SetLinkCount(virtual.ImplicitDirectoryLinkCount)
	attributes.SetFileType(filesystem.FileTypeDirectory)
	attributes.SetPermissions(virtual.PermissionsRead | virtual.PermissionsExecute)
	attributes.SetSizeBytes(d.sizeBytes)
}

func (d *contentAddressableStorageDirectory) VirtualLookup(name path.Component, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Directory, virtual.Leaf, virtual.Status) {
	directory, contentsContext, s := d.directoryContext.GetDirectoryContents()
	if s != virtual.StatusOK {
		return nil, nil, s
	}

	// The Remote Execution protocol requires that entries stored in
	// a Directory message are sorted alphabetically. Make use of
	// this fact by performing binary searching when looking up
	// entries. There is no need to explicitly index the entries.
	n := name.String()
	directories := directory.Directories
	if i := sort.Search(len(directories), func(i int) bool { return directories[i].Name >= n }); i < len(directories) && directories[i].Name == n {
		entryDigest, err := d.instanceName.NewDigestFromProto(directories[i].Digest)
		if err != nil {
			d.directoryContext.LogError(util.StatusWrapf(err, "Failed to parse digest for directory %#v", n))
			return nil, nil, virtual.StatusErrIO
		}
		child, s := contentsContext.LookupDirectory(entryDigest)
		if s != virtual.StatusOK {
			return nil, nil, s
		}
		child.VirtualGetAttributes(requested, out)
		return child, nil, virtual.StatusOK
	}

	files := directory.Files
	if i := sort.Search(len(files), func(i int) bool { return files[i].Name >= n }); i < len(files) && files[i].Name == n {
		entry := files[i]
		entryDigest, err := d.instanceName.NewDigestFromProto(entry.Digest)
		if err != nil {
			d.directoryContext.LogError(util.StatusWrapf(err, "Failed to parse digest for file %#v", n))
			return nil, nil, virtual.StatusErrIO
		}
		child := d.directoryContext.LookupFile(entryDigest, entry.IsExecutable)
		child.VirtualGetAttributes(requested, out)
		return nil, child, virtual.StatusOK
	}

	symlinks := directory.Symlinks
	if i := sort.Search(len(symlinks), func(i int) bool { return symlinks[i].Name >= n }); i < len(symlinks) && symlinks[i].Name == n {
		f := d.createSymlink(uint64(i), symlinks[i].Target)
		f.VirtualGetAttributes(requested, out)
		return nil, f, virtual.StatusOK
	}

	return nil, nil, virtual.StatusErrNoEnt
}

func (d *contentAddressableStorageDirectory) VirtualOpenChild(name path.Component, shareAccess virtual.ShareMask, createAttributes *virtual.Attributes, existingOptions *virtual.OpenExistingOptions, requested virtual.AttributesMask, openedFileAttributes *virtual.Attributes) (virtual.Leaf, virtual.AttributesMask, virtual.ChangeInfo, virtual.Status) {
	directory, _, s := d.directoryContext.GetDirectoryContents()
	if s != virtual.StatusOK {
		return nil, 0, virtual.ChangeInfo{}, s
	}

	n := name.String()
	directories := directory.Directories
	if i := sort.Search(len(directories), func(i int) bool { return directories[i].Name >= n }); i < len(directories) && directories[i].Name == n {
		return virtualOpenChildWrongFileType(existingOptions, virtual.StatusErrIsDir)
	}

	files := directory.Files
	if i := sort.Search(len(files), func(i int) bool { return files[i].Name >= n }); i < len(files) && files[i].Name == n {
		if existingOptions == nil {
			return nil, 0, virtual.ChangeInfo{}, virtual.StatusErrExist
		}

		entry := files[i]
		entryDigest, err := d.instanceName.NewDigestFromProto(entry.Digest)
		if err != nil {
			d.directoryContext.LogError(util.StatusWrapf(err, "Failed to parse digest for file %#v", n))
			return nil, 0, virtual.ChangeInfo{}, virtual.StatusErrIO
		}
		leaf := d.directoryContext.LookupFile(entryDigest, entry.IsExecutable)
		s := leaf.VirtualOpenSelf(shareAccess, existingOptions, requested, openedFileAttributes)
		return leaf, existingOptions.ToAttributesMask(), virtual.ChangeInfo{}, s
	}

	symlinks := directory.Symlinks
	if i := sort.Search(len(symlinks), func(i int) bool { return symlinks[i].Name >= n }); i < len(symlinks) && symlinks[i].Name == n {
		return virtualOpenChildWrongFileType(existingOptions, virtual.StatusErrSymlink)
	}

	return virtualOpenChildDoesntExist(createAttributes)
}

func (d *contentAddressableStorageDirectory) VirtualReadDir(firstCookie uint64, requested virtual.AttributesMask, reporter virtual.DirectoryEntryReporter) virtual.Status {
	directory, contentsContext, s := d.directoryContext.GetDirectoryContents()
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
		entryDigest, err := d.instanceName.NewDigestFromProto(entry.Digest)
		if err != nil {
			d.directoryContext.LogError(util.StatusWrapf(err, "Failed to parse digest for directory %#v", entry.Name))
			return virtual.StatusErrIO
		}
		child, s := contentsContext.LookupDirectory(entryDigest)
		if s != virtual.StatusOK {
			return s
		}
		var attributes virtual.Attributes
		child.VirtualGetAttributes(requested, &attributes)
		if !reporter.ReportDirectory(nextCookieOffset+i, name, child, &attributes) {
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
		entryDigest, err := d.instanceName.NewDigestFromProto(entry.Digest)
		if err != nil {
			d.directoryContext.LogError(util.StatusWrapf(err, "Failed to parse digest for file %#v", entry.Name))
			return virtual.StatusErrIO
		}
		child := d.directoryContext.LookupFile(entryDigest, entry.IsExecutable)
		var attributes virtual.Attributes
		child.VirtualGetAttributes(requested, &attributes)
		if !reporter.ReportLeaf(nextCookieOffset+i, name, child, &attributes) {
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
		child.VirtualGetAttributes(requested, &attributes)
		if !reporter.ReportLeaf(nextCookieOffset+i, name, child, &attributes) {
			return virtual.StatusOK
		}
	}

	return virtual.StatusOK
}
