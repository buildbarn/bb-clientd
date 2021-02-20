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

func TestInstanceNameParsingDirectory(t *testing.T) {
	ctrl := gomock.NewController(t)

	lookupFunc := mock.NewMockInstanceNameLookupFunc(ctrl)
	d := cd_fuse.NewInstanceNameParsingDirectory(
		re_fuse.DeterministicInodeNumberTree,
		map[path.Component]cd_fuse.InstanceNameLookupFunc{
			path.MustNewComponent("blobs"): lookupFunc.Call,
		})

	t.Run("EmptyInstanceName", func(t *testing.T) {
		// Parse the empty instance name by just traversing the
		// path "blobs".
		mockChildDirectory := mock.NewMockDirectory(ctrl)
		lookupFunc.EXPECT().Call(
			digest.MustNewInstanceName(""),
			gomock.Any(),
		).DoAndReturn(func(instanceName digest.InstanceName, out *fuse.Attr) (re_fuse.Directory, re_fuse.Leaf) {
			out.Ino = 123
			out.Mode = fuse.S_IFDIR | 0o777
			out.Nlink = 456
			return mockChildDirectory, nil
		})

		var out fuse.Attr
		childDirectory, childFile, s := d.FUSELookup(path.MustNewComponent("blobs"), &out)
		require.Equal(t, fuse.OK, s)
		require.Equal(t, mockChildDirectory, childDirectory)
		require.Nil(t, childFile)
		require.Equal(t, fuse.Attr{
			Ino:   123,
			Mode:  fuse.S_IFDIR | 0o777,
			Nlink: 456,
		}, out)
	})

	t.Run("SingleComponentInstanceName", func(t *testing.T) {
		// Parse the instance name "hello" by traversing the
		// path "hello/blobs".
		mockChildFile := mock.NewMockLeaf(ctrl)
		lookupFunc.EXPECT().Call(
			digest.MustNewInstanceName("hello"),
			gomock.Any(),
		).DoAndReturn(func(instanceName digest.InstanceName, out *fuse.Attr) (re_fuse.Directory, re_fuse.Leaf) {
			out.Ino = 123
			out.Mode = fuse.S_IFREG | 0o777
			out.Nlink = 456
			return nil, mockChildFile
		})

		var out1 fuse.Attr
		childDirectory1, childFile1, s := d.FUSELookup(path.MustNewComponent("hello"), &out1)
		require.Equal(t, fuse.OK, s)
		require.Nil(t, childFile1)
		require.Equal(t, fuse.Attr{
			Ino:   14973036441362892679,
			Mode:  fuse.S_IFDIR | 0o111,
			Nlink: re_fuse.ImplicitDirectoryLinkCount,
		}, out1)

		var out2 fuse.Attr
		childDirectory2, childFile2, s := childDirectory1.FUSELookup(path.MustNewComponent("blobs"), &out2)
		require.Equal(t, fuse.OK, s)
		require.Nil(t, childDirectory2)
		require.Equal(t, mockChildFile, childFile2)
		require.Equal(t, fuse.Attr{
			Ino:   123,
			Mode:  fuse.S_IFREG | 0o777,
			Nlink: 456,
		}, out2)
	})

	t.Run("DoubleComponentInstanceName", func(t *testing.T) {
		// Parse the instance name "hello/world" by traversing
		// the path "hello/world/blobs".
		mockChildFile := mock.NewMockLeaf(ctrl)
		lookupFunc.EXPECT().Call(
			digest.MustNewInstanceName("hello/world"),
			gomock.Any(),
		).DoAndReturn(func(instanceName digest.InstanceName, out *fuse.Attr) (re_fuse.Directory, re_fuse.Leaf) {
			out.Ino = 123
			out.Mode = fuse.S_IFREG | 0o777
			out.Nlink = 456
			return nil, mockChildFile
		})

		var out1 fuse.Attr
		childDirectory1, childFile1, s := d.FUSELookup(path.MustNewComponent("hello"), &out1)
		require.Equal(t, fuse.OK, s)
		require.Nil(t, childFile1)
		require.Equal(t, fuse.Attr{
			Ino:   14973036441362892679,
			Mode:  fuse.S_IFDIR | 0o111,
			Nlink: re_fuse.ImplicitDirectoryLinkCount,
		}, out1)

		var out2 fuse.Attr
		childDirectory2, childFile2, s := childDirectory1.FUSELookup(path.MustNewComponent("world"), &out2)
		require.Equal(t, fuse.OK, s)
		require.Nil(t, childFile2)
		require.Equal(t, fuse.Attr{
			Ino:   15913511176745189594,
			Mode:  fuse.S_IFDIR | 0o111,
			Nlink: re_fuse.ImplicitDirectoryLinkCount,
		}, out2)

		var out3 fuse.Attr
		childDirectory3, childFile3, s := childDirectory2.FUSELookup(path.MustNewComponent("blobs"), &out3)
		require.Equal(t, fuse.OK, s)
		require.Nil(t, childDirectory3)
		require.Equal(t, mockChildFile, childFile3)
		require.Equal(t, fuse.Attr{
			Ino:   123,
			Mode:  fuse.S_IFREG | 0o777,
			Nlink: 456,
		}, out3)
	})

	t.Run("InvalidInstanceName", func(t *testing.T) {
		// Parse the instance name "operations" by traversing
		// the path "operations/blobs". This should return an
		// error, due to "operations" being a reserved pathname
		// component.
		var out1 fuse.Attr
		childDirectory1, childFile1, s := d.FUSELookup(path.MustNewComponent("operations"), &out1)
		require.Equal(t, fuse.OK, s)
		require.Nil(t, childFile1)
		require.Equal(t, fuse.Attr{
			Ino:   3542646155446109460,
			Mode:  fuse.S_IFDIR | 0o111,
			Nlink: re_fuse.ImplicitDirectoryLinkCount,
		}, out1)

		var out2 fuse.Attr
		_, _, s = childDirectory1.FUSELookup(path.MustNewComponent("blobs"), &out2)
		require.Equal(t, fuse.ENOENT, s)
	})
}
