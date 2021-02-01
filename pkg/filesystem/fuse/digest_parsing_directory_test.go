package fuse_test

import (
	"testing"

	"github.com/buildbarn/bb-clientd/internal/mock"
	cd_fuse "github.com/buildbarn/bb-clientd/pkg/filesystem/fuse"
	re_fuse "github.com/buildbarn/bb-remote-execution/pkg/filesystem/fuse"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/golang/mock/gomock"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
)

func TestDigestParsingDirectory(t *testing.T) {
	ctrl := gomock.NewController(t)

	lookupFunc := mock.NewMockDigestLookupFunc(ctrl)
	d := cd_fuse.NewDigestParsingDirectory(
		digest.MustNewInstanceName("hello"),
		123,
		lookupFunc.Call)

	t.Run("NoDash", func(t *testing.T) {
		// The filename must contain a dash to separate the hash
		// and size in bytes.
		var out fuse.Attr
		_, _, s := d.FUSELookup(path.MustNewComponent("hello123"), &out)
		require.Equal(t, fuse.ENOENT, s)
	})

	t.Run("InvalidHash", func(t *testing.T) {
		// "hello" is not a valid cryptographic hash.
		var out fuse.Attr
		_, _, s := d.FUSELookup(path.MustNewComponent("hello-123"), &out)
		require.Equal(t, fuse.ENOENT, s)
	})

	t.Run("InvalidSizeBytesNotAnInteger", func(t *testing.T) {
		// The size in bytes must be an integer.
		var out fuse.Attr
		_, _, s := d.FUSELookup(path.MustNewComponent("8b1a9953c4611296a827abf8c47804d7-five"), &out)
		require.Equal(t, fuse.ENOENT, s)
	})

	t.Run("InvalidSizeBytesNotAnInteger", func(t *testing.T) {
		// The size in bytes must fit in int64.
		var out fuse.Attr
		_, _, s := d.FUSELookup(path.MustNewComponent("8b1a9953c4611296a827abf8c47804d7-13209483450980482109834"), &out)
		require.Equal(t, fuse.ENOENT, s)
	})

	t.Run("Success", func(t *testing.T) {
		// A directory or file must be looked up when the
		// filename is a valid digest.
		mockChildFile := mock.NewMockLeaf(ctrl)
		lookupFunc.EXPECT().Call(
			digest.MustNewDigest("hello", "8b1a9953c4611296a827abf8c47804d7", 5),
			gomock.Any(),
		).DoAndReturn(func(digest digest.Digest, out *fuse.Attr) (re_fuse.Directory, re_fuse.Leaf) {
			out.Ino = 123
			out.Mode = fuse.S_IFREG | 0777
			out.Nlink = 456
			return nil, mockChildFile
		})

		var out fuse.Attr
		childDirectory, childFile, s := d.FUSELookup(path.MustNewComponent("8b1a9953c4611296a827abf8c47804d7-5"), &out)
		require.Equal(t, fuse.OK, s)
		require.Nil(t, childDirectory)
		require.Equal(t, mockChildFile, childFile)
		require.Equal(t, fuse.Attr{
			Ino:   123,
			Mode:  fuse.S_IFREG | 0777,
			Nlink: 456,
		}, out)
	})
}
