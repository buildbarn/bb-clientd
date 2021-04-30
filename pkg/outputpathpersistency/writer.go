package outputpathpersistency

import (
	"github.com/buildbarn/bb-remote-execution/pkg/proto/outputpathpersistency"
)

// Writer for output path state files. These files contain a header,
// followed a sequence of Directory messages and a trailing
// RootDirectory message.
type Writer interface {
	WriteDirectory(directory *outputpathpersistency.Directory) (*outputpathpersistency.FileRegion, error)
	Finalize(rootDirectory *outputpathpersistency.RootDirectory) error
}
