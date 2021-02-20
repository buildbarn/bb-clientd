package fuse

import (
	"strconv"
	"strings"

	re_fuse "github.com/buildbarn/bb-remote-execution/pkg/filesystem/fuse"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// DigestLookupFunc is called by directories created using
// NewDigestParsingDirectory to complete the parsing of a digest. The
// callback can yield a directory or file corresponding with the digest.
type DigestLookupFunc func(digest digest.Digest, out *fuse.Attr) (re_fuse.Directory, re_fuse.Leaf)

type digestParsingDirectory struct {
	nonIterableDirectory
	readOnlyDirectory

	instanceName digest.InstanceName
	inodeNumber  uint64
	lookupFunc   DigestLookupFunc
}

// NewDigestParsingDirectory creates a directory that can be exposed
// through FUSE that parses filenames as REv2 digests.
//
// Though REv2 uses ByteStream paths that encode digests in the form of
// {hash}/{sizeBytes}, this type uses the format {hash}-{sizeBytes}.
// This is done to ensure every resulting file only uses a single inode,
// as opposed to two.
func NewDigestParsingDirectory(instanceName digest.InstanceName, inodeNumber uint64, lookupFunc DigestLookupFunc) re_fuse.Directory {
	return &digestParsingDirectory{
		instanceName: instanceName,
		inodeNumber:  inodeNumber,
		lookupFunc:   lookupFunc,
	}
}

func (d *digestParsingDirectory) FUSEGetAttr(out *fuse.Attr) {
	out.Ino = d.inodeNumber
	out.Mode = fuse.S_IFDIR | 0o111
	out.Nlink = re_fuse.ImplicitDirectoryLinkCount
}

func (d *digestParsingDirectory) FUSELookup(name path.Component, out *fuse.Attr) (re_fuse.Directory, re_fuse.Leaf, fuse.Status) {
	// Parse the filename.
	n := name.String()
	i := strings.LastIndex(n, "-")
	if i < 0 {
		return nil, nil, fuse.ENOENT
	}
	sizeBytes, err := strconv.ParseInt(n[i+1:], 10, 64)
	if err != nil {
		return nil, nil, fuse.ENOENT
	}
	digest, err := d.instanceName.NewDigest(n[:i], sizeBytes)
	if err != nil {
		return nil, nil, fuse.ENOENT
	}

	// Look up the resulting directory or file.
	directory, leaf := d.lookupFunc(digest, out)
	return directory, leaf, fuse.OK
}
