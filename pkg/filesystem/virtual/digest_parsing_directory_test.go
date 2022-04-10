package virtual_test

import (
	"testing"

	"github.com/buildbarn/bb-clientd/internal/mock"
	cd_vfs "github.com/buildbarn/bb-clientd/pkg/filesystem/virtual"
	re_vfs "github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestDigestParsingDirectory(t *testing.T) {
	ctrl := gomock.NewController(t)

	lookupFunc := mock.NewMockDigestLookupFunc(ctrl)
	d := cd_vfs.NewDigestParsingDirectory(
		digest.MustNewInstanceName("hello"),
		lookupFunc.Call)

	t.Run("NoDash", func(t *testing.T) {
		// The filename must contain a dash to separate the hash
		// and size in bytes.
		var out re_vfs.Attributes
		_, _, s := d.VirtualLookup(path.MustNewComponent("hello123"), 0, &out)
		require.Equal(t, re_vfs.StatusErrNoEnt, s)
	})

	t.Run("InvalidHash", func(t *testing.T) {
		// "hello" is not a valid cryptographic hash.
		var out re_vfs.Attributes
		_, _, s := d.VirtualLookup(path.MustNewComponent("hello-123"), 0, &out)
		require.Equal(t, re_vfs.StatusErrNoEnt, s)
	})

	t.Run("InvalidSizeBytesNotAnInteger", func(t *testing.T) {
		// The size in bytes must be an integer.
		var out re_vfs.Attributes
		_, _, s := d.VirtualLookup(path.MustNewComponent("8b1a9953c4611296a827abf8c47804d7-five"), 0, &out)
		require.Equal(t, re_vfs.StatusErrNoEnt, s)
	})

	t.Run("InvalidSizeBytesNotAnInteger", func(t *testing.T) {
		// The size in bytes must fit in int64.
		var out re_vfs.Attributes
		_, _, s := d.VirtualLookup(path.MustNewComponent("8b1a9953c4611296a827abf8c47804d7-13209483450980482109834"), 0, &out)
		require.Equal(t, re_vfs.StatusErrNoEnt, s)
	})

	t.Run("Success", func(t *testing.T) {
		// A directory or file must be looked up when the
		// filename is a valid digest.
		mockChildFile := mock.NewMockVirtualLeaf(ctrl)
		lookupFunc.EXPECT().Call(
			digest.MustNewDigest("hello", "8b1a9953c4611296a827abf8c47804d7", 5),
		).Return(nil, mockChildFile, re_vfs.StatusOK)
		mockChildFile.EXPECT().VirtualGetAttributes(
			re_vfs.AttributesMaskInodeNumber,
			gomock.Any(),
		).Do(func(requested re_vfs.AttributesMask, out *re_vfs.Attributes) {
			out.SetInodeNumber(123)
		})

		var out re_vfs.Attributes
		childDirectory, childFile, s := d.VirtualLookup(path.MustNewComponent("8b1a9953c4611296a827abf8c47804d7-5"), re_vfs.AttributesMaskInodeNumber, &out)
		require.Equal(t, re_vfs.StatusOK, s)
		require.Nil(t, childDirectory)
		require.Equal(t, mockChildFile, childFile)
		require.Equal(t, *(&re_vfs.Attributes{}).SetInodeNumber(123), out)
	})
}
