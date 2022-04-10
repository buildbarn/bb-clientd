package virtual

import (
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

// CommandFileFactory is a factory type for virtual files that contain
// shell scripts that launch build actions according to the contents of
// an REv2 Command message. This makes it easy to run a build action
// locally.
type CommandFileFactory interface {
	LookupFile(blobDigest digest.Digest) (virtual.Leaf, virtual.Status)
}
