package outputpathpersistency

import (
	"github.com/buildbarn/bb-remote-execution/pkg/proto/outputpathpersistency"
)

// Reader of output path state files.
type Reader interface {
	// ReadDirectory() reads a child directory from an output path
	// state file. In addition to the contents of the child
	// directory, a Reader is returned that may be used to access
	// grandchildren.
	ReadDirectory(fileRegion *outputpathpersistency.FileRegion) (Reader, *outputpathpersistency.Directory, error)
}
