package virtual

import (
	"context"
	"strconv"
	"strings"

	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

// DigestLookupFunc is called by directories created using
// NewDigestParsingDirectory to complete the parsing of a digest. The
// callback can yield a directory or file corresponding with the digest.
type DigestLookupFunc func(digest digest.Digest) (virtual.DirectoryChild, virtual.Status)

type digestParsingDirectory struct {
	nonIterableDirectory
	virtual.ReadOnlyDirectory

	instanceName digest.InstanceName
	lookupFunc   DigestLookupFunc
}

// NewDigestParsingDirectory creates a directory that can be exposed
// through FUSE that parses filenames as REv2 digests.
//
// Though REv2 uses ByteStream paths that encode digests in the form of
// {hash}/{sizeBytes}, this type uses the format {hash}-{sizeBytes}.
// This is done to ensure every resulting file only uses a single inode,
// as opposed to two.
func NewDigestParsingDirectory(instanceName digest.InstanceName, lookupFunc DigestLookupFunc) virtual.Directory {
	return &digestParsingDirectory{
		instanceName: instanceName,
		lookupFunc:   lookupFunc,
	}
}

func (d *digestParsingDirectory) VirtualGetAttributes(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
	attributes.SetChangeID(0)
	attributes.SetFileType(filesystem.FileTypeDirectory)
	attributes.SetLinkCount(virtual.ImplicitDirectoryLinkCount)
	attributes.SetPermissions(virtual.PermissionsExecute)
	attributes.SetSizeBytes(0)
}

func (d *digestParsingDirectory) parseFilename(name path.Component) (digest.Digest, bool) {
	n := name.String()
	i := strings.LastIndex(n, "-")
	if i < 0 {
		return digest.BadDigest, false
	}
	sizeBytes, err := strconv.ParseInt(n[i+1:], 10, 64)
	if err != nil {
		return digest.BadDigest, false
	}
	fileDigest, err := d.instanceName.NewDigest(n[:i], sizeBytes)
	if err != nil {
		return digest.BadDigest, false
	}
	return fileDigest, true
}

func (d *digestParsingDirectory) VirtualLookup(ctx context.Context, name path.Component, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.DirectoryChild, virtual.Status) {
	digest, ok := d.parseFilename(name)
	if !ok {
		return virtual.DirectoryChild{}, virtual.StatusErrNoEnt
	}
	child, s := d.lookupFunc(digest)
	if s != virtual.StatusOK {
		return virtual.DirectoryChild{}, s
	}
	child.GetNode().VirtualGetAttributes(ctx, requested, out)
	return child, s
}

func (d *digestParsingDirectory) VirtualOpenChild(ctx context.Context, name path.Component, shareAccess virtual.ShareMask, createAttributes *virtual.Attributes, existingOptions *virtual.OpenExistingOptions, requested virtual.AttributesMask, openedFileAttributes *virtual.Attributes) (virtual.Leaf, virtual.AttributesMask, virtual.ChangeInfo, virtual.Status) {
	digest, ok := d.parseFilename(name)
	if !ok {
		return virtual.ReadOnlyDirectoryOpenChildDoesntExist(createAttributes)
	}
	if existingOptions == nil {
		return nil, 0, virtual.ChangeInfo{}, virtual.StatusErrExist
	}

	child, s := d.lookupFunc(digest)
	if s != virtual.StatusOK {
		return nil, 0, virtual.ChangeInfo{}, s
	}
	directory, leaf := child.GetPair()
	if directory != nil {
		return nil, 0, virtual.ChangeInfo{}, virtual.StatusErrIsDir
	}
	s = leaf.VirtualOpenSelf(ctx, shareAccess, existingOptions, requested, openedFileAttributes)
	return leaf, existingOptions.ToAttributesMask(), virtual.ChangeInfo{}, s
}
