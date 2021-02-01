package main_test

import (
	"context"
	"testing"

	bb_clientd "github.com/buildbarn/bb-clientd/cmd/bb_clientd"
	"github.com/buildbarn/bb-clientd/internal/mock"
	re_fuse "github.com/buildbarn/bb-remote-execution/pkg/filesystem/fuse"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/golang/mock/gomock"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGlobalFileContextLookupFile(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	globalFileContext := bb_clientd.NewGlobalFileContext(ctx, contentAddressableStorage, errorLogger)
	helloDigest := digest.MustNewDigest("hello", "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c", 11)

	t.Run("PlainIOError", func(t *testing.T) {
		// Create a plain file based on a given digest.
		var out fuse.Attr
		f := globalFileContext.LookupFile(helloDigest, false, &out)
		require.Equal(t, fuse.Attr{
			Mode:  fuse.S_IFREG | 0444,
			Ino:   globalFileContext.GetFileInodeNumber(helloDigest, false),
			Size:  11,
			Nlink: re_fuse.StatelessLeafLinkCount,
		}, out)

		// Read errors should be forwarded to the error logger.
		errorLogger.EXPECT().Log(status.Error(codes.Internal, "Failed to read from 64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c-11-hello at offset 0: Server on fire"))
		contentAddressableStorage.EXPECT().Get(ctx, helloDigest).
			Return(buffer.NewBufferFromError(status.Error(codes.Internal, "Server on fire")))
		_, s := f.FUSERead(make([]byte, 11), 0)
		require.Equal(t, fuse.EIO, s)
	})

	t.Run("ExecutableSuccess", func(t *testing.T) {
		// Create an executable file based on a given digest.
		var out fuse.Attr
		f := globalFileContext.LookupFile(helloDigest, true, &out)
		require.Equal(t, fuse.Attr{
			Mode:  fuse.S_IFREG | 0555,
			Ino:   globalFileContext.GetFileInodeNumber(helloDigest, true),
			Size:  11,
			Nlink: re_fuse.StatelessLeafLinkCount,
		}, out)

		// Reads should be targeted against the right backend and blob.
		contentAddressableStorage.EXPECT().Get(ctx, helloDigest).
			Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world")))
		rr, s := f.FUSERead(make([]byte, 11), 0)
		require.Equal(t, fuse.OK, s)
		data, s := rr.Bytes(make([]byte, 11))
		require.Equal(t, fuse.OK, s)
		require.Equal(t, []byte("Hello world"), data)
	})
}
