package virtual

import (
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

// CASDirectoryFactory is a factory type for directories whose contents
// correspond with an object stored in the Content Addressable Storage
// (CAS).
type CASDirectoryFactory interface {
	LookupDirectory(digest digest.Digest) virtual.Directory
}
