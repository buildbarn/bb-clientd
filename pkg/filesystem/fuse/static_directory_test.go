package fuse_test

import (
	"testing"

	"github.com/buildbarn/bb-clientd/internal/mock"
	cd_fuse "github.com/buildbarn/bb-clientd/pkg/filesystem/fuse"
	re_fuse "github.com/buildbarn/bb-remote-execution/pkg/filesystem/fuse"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/golang/mock/gomock"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
)

func TestStaticDirectoryFUSELookup(t *testing.T) {
	ctrl := gomock.NewController(t)

	child := mock.NewMockFUSEDirectory(ctrl)
	d := cd_fuse.NewStaticDirectory(123, map[path.Component]cd_fuse.StaticDirectoryEntry{
		path.MustNewComponent("child"): {
			Child:       child,
			InodeNumber: 456,
		},
	})

	t.Run("NotFound", func(t *testing.T) {
		var out fuse.Attr
		_, _, s := d.FUSELookup(path.MustNewComponent("nonexistent"), &out)
		require.Equal(t, fuse.ENOENT, s)
	})

	t.Run("Success", func(t *testing.T) {
		child.EXPECT().FUSEGetAttr(gomock.Any()).DoAndReturn(func(out *fuse.Attr) {
			out.Ino = 456
			out.Mode = fuse.S_IFDIR | 0o555
			out.Nlink = re_fuse.EmptyDirectoryLinkCount
		})

		var out fuse.Attr
		actualChild, _, s := d.FUSELookup(path.MustNewComponent("child"), &out)
		require.Equal(t, fuse.OK, s)
		require.Equal(t, child, actualChild)
		require.Equal(t, fuse.Attr{
			Ino:   456,
			Mode:  fuse.S_IFDIR | 0o555,
			Nlink: re_fuse.EmptyDirectoryLinkCount,
		}, out)
	})
}

func TestStaticDirectoryFUSEReadDir(t *testing.T) {
	ctrl := gomock.NewController(t)

	childA := mock.NewMockFUSEDirectory(ctrl)
	childB := mock.NewMockFUSEDirectory(ctrl)
	d := cd_fuse.NewStaticDirectory(123, map[path.Component]cd_fuse.StaticDirectoryEntry{
		path.MustNewComponent("a"): {
			Child:       childA,
			InodeNumber: 456,
		},
		path.MustNewComponent("b"): {
			Child:       childB,
			InodeNumber: 789,
		},
	})

	entries, s := d.FUSEReadDir()
	require.Equal(t, fuse.OK, s)
	require.ElementsMatch(
		t,
		[]fuse.DirEntry{
			{
				Name: "a",
				Mode: fuse.S_IFDIR,
				Ino:  456,
			},
			{
				Name: "b",
				Mode: fuse.S_IFDIR,
				Ino:  789,
			},
		},
		entries)
}

func TestStaticDirectoryFUSEReadDirPlus(t *testing.T) {
	ctrl := gomock.NewController(t)

	childA := mock.NewMockFUSEDirectory(ctrl)
	childB := mock.NewMockFUSEDirectory(ctrl)
	d := cd_fuse.NewStaticDirectory(123, map[path.Component]cd_fuse.StaticDirectoryEntry{
		path.MustNewComponent("a"): {
			Child:       childA,
			InodeNumber: 456,
		},
		path.MustNewComponent("b"): {
			Child:       childB,
			InodeNumber: 789,
		},
	})

	directories, leaves, s := d.FUSEReadDirPlus()
	require.Equal(t, fuse.OK, s)
	require.ElementsMatch(
		t,
		[]re_fuse.DirectoryDirEntry{
			{
				Child: childA,
				DirEntry: fuse.DirEntry{
					Name: "a",
					Mode: fuse.S_IFDIR,
					Ino:  456,
				},
			},
			{
				Child: childB,
				DirEntry: fuse.DirEntry{
					Name: "b",
					Mode: fuse.S_IFDIR,
					Ino:  789,
				},
			},
		},
		directories)
	require.Empty(t, leaves)
}
