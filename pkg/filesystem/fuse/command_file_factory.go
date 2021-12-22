package fuse

import (
	"context"
	"io"
	"syscall"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	re_fuse "github.com/buildbarn/bb-remote-execution/pkg/filesystem/fuse"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/hanwen/go-fuse/v2/fuse"

	"golang.org/x/sys/unix"
)

// CommandFileFactory is a factory type for FUSE files that contain
// shell scripts that launch build actions according to the contents of
// an REv2 Command message. This makes it easy to run a build action
// locally.
type CommandFileFactory struct {
	context                   context.Context
	contentAddressableStorage blobstore.BlobAccess
	maximumMessageSizeBytes   int
	errorLogger               util.ErrorLogger
	inodeNumberTree           re_fuse.InodeNumberTree
}

// NewCommandFileFactory creates a new CommandFileFactory that loads
// REv2 Command messages from the provided Content Addressable Storage,
// turning them into shell scripts capable of launching build actions.
func NewCommandFileFactory(ctx context.Context, contentAddressableStorage blobstore.BlobAccess, maximumMessageSizeBytes int, errorLogger util.ErrorLogger) *CommandFileFactory {
	return &CommandFileFactory{
		context:                   ctx,
		contentAddressableStorage: contentAddressableStorage,
		maximumMessageSizeBytes:   maximumMessageSizeBytes,
		errorLogger:               errorLogger,
		inodeNumberTree:           re_fuse.NewRandomInodeNumberTree(),
	}
}

// LookupFile creates a FUSE file that corresponds with the REv2 Command
// message of a given digest.
func (ff *CommandFileFactory) LookupFile(blobDigest digest.Digest, out *fuse.Attr) (re_fuse.Leaf, fuse.Status) {
	var w sizeCountingWriter
	if s := ff.writeShellScript(blobDigest, &w); s != fuse.OK {
		return nil, s
	}
	f := &commandFile{
		factory:     ff,
		blobDigest:  blobDigest,
		inodeNumber: ff.inodeNumberTree.AddString(blobDigest.GetKey(digest.KeyWithInstance)).Get(),
		size:        w.size,
	}
	f.FUSEGetAttr(out)
	return f, fuse.OK
}

func (ff *CommandFileFactory) writeShellScript(blobDigest digest.Digest, w io.StringWriter) fuse.Status {
	m, err := ff.contentAddressableStorage.Get(ff.context, blobDigest).ToProto(&remoteexecution.Command{}, ff.maximumMessageSizeBytes)
	if err != nil {
		ff.errorLogger.Log(util.StatusWrapf(err, "Failed to load command %#v", blobDigest.String()))
		return fuse.EIO
	}
	if err := builder.ConvertCommandToShellScript(m.(*remoteexecution.Command), w); err != nil {
		ff.errorLogger.Log(util.StatusWrapf(err, "Failed to convert command %#v to a shell script", blobDigest.String()))
		return fuse.EIO
	}
	return fuse.OK
}

type commandFile struct {
	factory     *CommandFileFactory
	blobDigest  digest.Digest
	inodeNumber uint64
	size        uint64
}

func (f *commandFile) FUSEAccess(mask uint32) fuse.Status {
	if mask&^(fuse.R_OK|fuse.X_OK) != 0 {
		return fuse.EACCES
	}
	return fuse.OK
}

func (f *commandFile) FUSEGetAttr(out *fuse.Attr) {
	out.Ino = f.inodeNumber
	out.Size = f.size
	out.Mode = fuse.S_IFREG | 0o555
	out.Nlink = re_fuse.StatelessLeafLinkCount
}

func (f *commandFile) FUSESetAttr(in *fuse.SetAttrIn, out *fuse.Attr) fuse.Status {
	if in.Valid&(fuse.FATTR_UID|fuse.FATTR_GID) != 0 {
		return fuse.EPERM
	}
	if in.Valid&fuse.FATTR_SIZE != 0 {
		return fuse.EACCES
	}
	return fuse.OK
}

func (f *commandFile) FUSEFallocate(off, size uint64) fuse.Status {
	return fuse.EBADF
}

func (f *commandFile) FUSELseek(in *fuse.LseekIn, out *fuse.LseekOut) fuse.Status {
	switch in.Whence {
	case unix.SEEK_DATA:
		if in.Offset >= f.size {
			return fuse.Status(syscall.ENXIO)
		}
		out.Offset = in.Offset
	case unix.SEEK_HOLE:
		if in.Offset >= f.size {
			return fuse.Status(syscall.ENXIO)
		}
		out.Offset = f.size
	default:
		panic("Requests for other seek modes should have been intercepted")
	}
	return fuse.OK
}

func (f *commandFile) FUSEOpen(flags uint32) fuse.Status {
	if flags&fuse.O_ANYWRITE != 0 {
		return fuse.EACCES
	}
	return fuse.OK
}

func (f *commandFile) FUSERead(buf []byte, offset uint64) (fuse.ReadResult, fuse.Status) {
	w := regionExtractingWriter{
		buf:    buf,
		offset: offset,
	}
	s := f.factory.writeShellScript(f.blobDigest, &w)
	return fuse.ReadResultData(buf[:len(buf)-len(w.buf)]), s
}

func (f *commandFile) FUSEReadlink() ([]byte, fuse.Status) {
	return nil, fuse.EINVAL
}

func (f *commandFile) FUSERelease() {}

func (f *commandFile) FUSEWrite(buf []byte, offset uint64) (uint32, fuse.Status) {
	panic("Request to write to read-only file should have been intercepted")
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
