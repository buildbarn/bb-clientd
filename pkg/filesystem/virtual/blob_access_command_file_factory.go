package virtual

import (
	"context"
	"io"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type blobAccessCommandFileFactory struct {
	context                   context.Context
	contentAddressableStorage blobstore.BlobAccess
	maximumMessageSizeBytes   int
	errorLogger               util.ErrorLogger
}

// NewBlobAccessCommandFileFactory creates a new CommandFileFactory that
// loads REv2 Command messages from the provided Content Addressable
// Storage, turning them into shell scripts capable of launching build
// actions.
func NewBlobAccessCommandFileFactory(ctx context.Context, contentAddressableStorage blobstore.BlobAccess, maximumMessageSizeBytes int, errorLogger util.ErrorLogger) CommandFileFactory {
	return &blobAccessCommandFileFactory{
		context:                   ctx,
		contentAddressableStorage: contentAddressableStorage,
		maximumMessageSizeBytes:   maximumMessageSizeBytes,
		errorLogger:               errorLogger,
	}
}

// LookupFile creates a FUSE file that corresponds with the REv2 Command
// message of a given digest.
func (ff *blobAccessCommandFileFactory) LookupFile(blobDigest digest.Digest) (virtual.Leaf, virtual.Status) {
	var w sizeCountingWriter
	if s := ff.writeShellScript(blobDigest, &w); s != virtual.StatusOK {
		return nil, s
	}
	f := &commandFile{
		factory:    ff,
		blobDigest: blobDigest,
		size:       w.size,
	}
	return f, virtual.StatusOK
}

func (ff *blobAccessCommandFileFactory) writeShellScript(blobDigest digest.Digest, w io.StringWriter) virtual.Status {
	m, err := ff.contentAddressableStorage.Get(ff.context, blobDigest).ToProto(&remoteexecution.Command{}, ff.maximumMessageSizeBytes)
	if err != nil {
		ff.errorLogger.Log(util.StatusWrapf(err, "Failed to load command %#v", blobDigest.String()))
		return virtual.StatusErrIO
	}
	if err := builder.ConvertCommandToShellScript(m.(*remoteexecution.Command), w); err != nil {
		ff.errorLogger.Log(util.StatusWrapf(err, "Failed to convert command %#v to a shell script", blobDigest.String()))
		return virtual.StatusErrIO
	}
	return virtual.StatusOK
}

type commandFile struct {
	factory    *blobAccessCommandFileFactory
	blobDigest digest.Digest
	size       uint64
}

func (f *commandFile) VirtualGetAttributes(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
	attributes.SetChangeID(0)
	attributes.SetFileType(filesystem.FileTypeRegularFile)
	attributes.SetPermissions(virtual.PermissionsRead | virtual.PermissionsExecute)
	attributes.SetSizeBytes(f.size)
}

func (f *commandFile) VirtualSetAttributes(ctx context.Context, in *virtual.Attributes, requested virtual.AttributesMask, out *virtual.Attributes) virtual.Status {
	if _, ok := in.GetPermissions(); ok {
		return virtual.StatusErrPerm
	}
	if _, ok := in.GetSizeBytes(); ok {
		return virtual.StatusErrAccess
	}
	f.VirtualGetAttributes(ctx, requested, out)
	return virtual.StatusOK
}

func (f *commandFile) VirtualAllocate(off, size uint64) virtual.Status {
	return virtual.StatusErrWrongType
}

func (f *commandFile) VirtualSeek(offset uint64, regionType filesystem.RegionType) (*uint64, virtual.Status) {
	sizeBytes := f.size
	switch regionType {
	case filesystem.Data:
		if offset >= sizeBytes {
			return nil, virtual.StatusErrNXIO
		}
		return &offset, virtual.StatusOK
	case filesystem.Hole:
		if offset >= sizeBytes {
			return nil, virtual.StatusErrNXIO
		}
		return &sizeBytes, virtual.StatusOK
	default:
		panic("Requests for other seek modes should have been intercepted")
	}
}

func (f *commandFile) VirtualOpenSelf(ctx context.Context, shareAccess virtual.ShareMask, options *virtual.OpenExistingOptions, requested virtual.AttributesMask, attributes *virtual.Attributes) virtual.Status {
	if shareAccess&^virtual.ShareMaskRead != 0 || options.Truncate {
		return virtual.StatusErrAccess
	}
	f.VirtualGetAttributes(ctx, requested, attributes)
	return virtual.StatusOK
}

func (f *commandFile) VirtualRead(buf []byte, offset uint64) (int, bool, virtual.Status) {
	buf, eof := virtual.BoundReadToFileSize(buf, offset, f.size)
	if len(buf) > 0 {
		w := regionExtractingWriter{
			buf:    buf,
			offset: offset,
		}
		if s := f.factory.writeShellScript(f.blobDigest, &w); s != virtual.StatusOK {
			return 0, false, s
		}
		if len(w.buf) != 0 {
			panic("writeShellScript() only returned partial data, which conflicts with the originally computed size")
		}
	}
	return len(buf), eof, virtual.StatusOK
}

func (f *commandFile) VirtualReadlink(ctx context.Context) ([]byte, virtual.Status) {
	return nil, virtual.StatusErrInval
}

func (f *commandFile) VirtualClose(shareAccess virtual.ShareMask) {}

func (f *commandFile) VirtualWrite(buf []byte, offset uint64) (int, virtual.Status) {
	panic("Request to write to read-only file should have been intercepted")
}

func (commandFile) VirtualOpenNamedAttributes(ctx context.Context, createDirectory bool, requested virtual.AttributesMask, attributes *virtual.Attributes) (virtual.Directory, virtual.Status) {
	if createDirectory {
		return nil, virtual.StatusErrAccess
	}
	return nil, virtual.StatusErrNoEnt
}

func (commandFile) VirtualApply(data any) bool {
	return false
}

type sizeCountingWriter struct {
	size uint64
}

func (w *sizeCountingWriter) WriteString(s string) (int, error) {
	w.size += uint64(len(s))
	return len(s), nil
}

type regionExtractingWriter struct {
	buf    []byte
	offset uint64
}

func (w *regionExtractingWriter) WriteString(s string) (int, error) {
	if w.offset >= uint64(len(s)) {
		w.offset -= uint64(len(s))
	} else {
		w.buf = w.buf[copy(w.buf, s[w.offset:]):]
		w.offset = 0
	}
	return len(s), nil
}
