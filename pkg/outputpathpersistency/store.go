package outputpathpersistency

import (
	"io"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/outputpathpersistency"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

// ReadCloser is identical to the Reader type, except that it has a
// Close() function that must be called exactly once to release any
// associated resources. Calling this function invalidates both the
// current ReadCloser and any Readers that were obtained recursively.
type ReadCloser interface {
	Reader
	io.Closer
}

// WriteCloser is identical to the Writer type, except that it has a
// Close() function that may be called to abandon a partially written
// state file.
//
// Either the Finalize() or Close() function must be called exactly once
// to release any associated resources.
type WriteCloser interface {
	Writer
	io.Closer
}

// Store for contents of an output path. The contents of output paths
// are preserved as a tree of Protobuf directory messages, referring to
// child directories.
type Store interface {
	// Read the persisted contents of an output path.
	Read(outputBaseID path.Component) (ReadCloser, *outputpathpersistency.RootDirectory, error)
	// Write the contents of an output path to a persistent state file.
	Write(outputBaseID path.Component) (WriteCloser, error)
	// Remove any state associated with given output path.
	Clean(outputBaseID path.Component) error
}
