package virtual

import (
	"context"

	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
)

// nonIterableDirectory can be embedded into a virtual.Directory to
// disable the readdir() operation.
type nonIterableDirectory struct{}

func (d nonIterableDirectory) VirtualReadDir(ctx context.Context, firstCookie uint64, requested virtual.AttributesMask, reporter virtual.DirectoryEntryReporter) virtual.Status {
	return virtual.StatusErrAccess
}
