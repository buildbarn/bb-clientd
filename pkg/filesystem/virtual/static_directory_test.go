package virtual_test

import (
	"testing"

	"github.com/buildbarn/bb-clientd/internal/mock"
	cd_vfs "github.com/buildbarn/bb-clientd/pkg/filesystem/virtual"
	re_vfs "github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestStaticDirectoryVirtualLookup(t *testing.T) {
	ctrl := gomock.NewController(t)

	child := mock.NewMockVirtualDirectory(ctrl)
	d := cd_vfs.NewStaticDirectory(map[path.Component]re_vfs.Directory{
		path.MustNewComponent("child"): child,
	})

	t.Run("NotFound", func(t *testing.T) {
		var out re_vfs.Attributes
		_, _, s := d.VirtualLookup(path.MustNewComponent("nonexistent"), 0, &out)
		require.Equal(t, re_vfs.StatusErrNoEnt, s)
	})

	t.Run("Success", func(t *testing.T) {
		child.EXPECT().VirtualGetAttributes(
			re_vfs.AttributesMaskInodeNumber,
			gomock.Any(),
		).Do(func(requested re_vfs.AttributesMask, out *re_vfs.Attributes) {
			out.SetInodeNumber(456)
		})

		var out re_vfs.Attributes
		actualChild, _, s := d.VirtualLookup(path.MustNewComponent("child"), re_vfs.AttributesMaskInodeNumber, &out)
		require.Equal(t, re_vfs.StatusOK, s)
		require.Equal(t, child, actualChild)
		require.Equal(t, *(&re_vfs.Attributes{}).SetInodeNumber(456), out)
	})
}

func TestStaticDirectoryVirtualOpenChild(t *testing.T) {
	ctrl := gomock.NewController(t)

	child := mock.NewMockVirtualDirectory(ctrl)
	d := cd_vfs.NewStaticDirectory(map[path.Component]re_vfs.Directory{
		path.MustNewComponent("child"): child,
	})

	t.Run("NotFound", func(t *testing.T) {
		// Child does not exist, and we're not instructed to
		// create anything.
		var out re_vfs.Attributes
		_, _, _, s := d.VirtualOpenChild(
			path.MustNewComponent("nonexistent"),
			re_vfs.ShareMaskRead,
			nil,
			&re_vfs.OpenExistingOptions{},
			re_vfs.AttributesMaskInodeNumber,
			&out)
		require.Equal(t, re_vfs.StatusErrNoEnt, s)
	})

	t.Run("ReadOnlyFileSystem", func(t *testing.T) {
		// Child does not exist, and we're don't support
		// creating anything.
		var out re_vfs.Attributes
		_, _, _, s := d.VirtualOpenChild(
			path.MustNewComponent("nonexistent"),
			re_vfs.ShareMaskWrite,
			(&re_vfs.Attributes{}).SetPermissions(re_vfs.PermissionsRead|re_vfs.PermissionsWrite),
			&re_vfs.OpenExistingOptions{},
			re_vfs.AttributesMaskInodeNumber,
			&out)
		require.Equal(t, re_vfs.StatusErrROFS, s)
	})

	t.Run("Exists", func(t *testing.T) {
		// A directory already exists under this name, so we
		// can't create a file.
		var out re_vfs.Attributes
		_, _, _, s := d.VirtualOpenChild(
			path.MustNewComponent("child"),
			re_vfs.ShareMaskWrite,
			(&re_vfs.Attributes{}).SetPermissions(re_vfs.PermissionsRead|re_vfs.PermissionsWrite),
			nil,
			re_vfs.AttributesMaskInodeNumber,
			&out)
		require.Equal(t, re_vfs.StatusErrExist, s)
	})

	t.Run("IsDirectory", func(t *testing.T) {
		// A directory already exists under this name, so we
		// can't open it as a file.
		var out re_vfs.Attributes
		_, _, _, s := d.VirtualOpenChild(
			path.MustNewComponent("child"),
			re_vfs.ShareMaskWrite,
			(&re_vfs.Attributes{}).SetPermissions(re_vfs.PermissionsRead|re_vfs.PermissionsWrite),
			&re_vfs.OpenExistingOptions{},
			re_vfs.AttributesMaskInodeNumber,
			&out)
		require.Equal(t, re_vfs.StatusErrIsDir, s)
	})
}

func TestStaticDirectoryVirtualReadDir(t *testing.T) {
	ctrl := gomock.NewController(t)

	childA := mock.NewMockVirtualDirectory(ctrl)
	childB := mock.NewMockVirtualDirectory(ctrl)
	d := cd_vfs.NewStaticDirectory(map[path.Component]re_vfs.Directory{
		path.MustNewComponent("a"): childA,
		path.MustNewComponent("b"): childB,
	})

	t.Run("FromStart", func(t *testing.T) {
		// Read the directory in its entirety.
		reporter := mock.NewMockDirectoryEntryReporter(ctrl)
		childA.EXPECT().VirtualGetAttributes(re_vfs.AttributesMaskInodeNumber, gomock.Any()).Do(
			func(requested re_vfs.AttributesMask, out *re_vfs.Attributes) {
				out.SetInodeNumber(123)
			})
		reporter.EXPECT().ReportDirectory(
			uint64(1),
			path.MustNewComponent("a"),
			childA,
			(&re_vfs.Attributes{}).SetInodeNumber(123),
		).Return(true)
		childB.EXPECT().VirtualGetAttributes(re_vfs.AttributesMaskInodeNumber, gomock.Any()).Do(
			func(requested re_vfs.AttributesMask, out *re_vfs.Attributes) {
				out.SetInodeNumber(456)
			})
		reporter.EXPECT().ReportDirectory(
			uint64(2),
			path.MustNewComponent("b"),
			childB,
			(&re_vfs.Attributes{}).SetInodeNumber(456),
		).Return(true)

		require.Equal(
			t,
			re_vfs.StatusOK,
			d.VirtualReadDir(0, re_vfs.AttributesMaskInodeNumber, reporter))
	})

	t.Run("NoSpace", func(t *testing.T) {
		// Not even enough space to store the first entry.
		reporter := mock.NewMockDirectoryEntryReporter(ctrl)
		childA.EXPECT().VirtualGetAttributes(re_vfs.AttributesMaskInodeNumber, gomock.Any()).Do(
			func(requested re_vfs.AttributesMask, out *re_vfs.Attributes) {
				out.SetInodeNumber(123)
			})
		reporter.EXPECT().ReportDirectory(
			uint64(1),
			path.MustNewComponent("a"),
			childA,
			(&re_vfs.Attributes{}).SetInodeNumber(123),
		).Return(false)

		require.Equal(
			t,
			re_vfs.StatusOK,
			d.VirtualReadDir(0, re_vfs.AttributesMaskInodeNumber, reporter))
	})

	t.Run("Partial", func(t *testing.T) {
		// Only read the last entry.
		reporter := mock.NewMockDirectoryEntryReporter(ctrl)
		childB.EXPECT().VirtualGetAttributes(re_vfs.AttributesMaskInodeNumber, gomock.Any()).Do(
			func(requested re_vfs.AttributesMask, out *re_vfs.Attributes) {
				out.SetInodeNumber(456)
			})
		reporter.EXPECT().ReportDirectory(
			uint64(2),
			path.MustNewComponent("b"),
			childB,
			(&re_vfs.Attributes{}).SetInodeNumber(456),
		).Return(true)

		require.Equal(
			t,
			re_vfs.StatusOK,
			d.VirtualReadDir(1, re_vfs.AttributesMaskInodeNumber, reporter))
	})

	t.Run("AtEOF", func(t *testing.T) {
		// Reading at the end-of-file should yield no entries.
		reporter := mock.NewMockDirectoryEntryReporter(ctrl)

		require.Equal(
			t,
			re_vfs.StatusOK,
			d.VirtualReadDir(2, re_vfs.AttributesMaskInodeNumber, reporter))
	})

	t.Run("BeyondEOF", func(t *testing.T) {
		// Reading past the end-of-file should not cause incorrect
		// behaviour.
		reporter := mock.NewMockDirectoryEntryReporter(ctrl)

		require.Equal(
			t,
			re_vfs.StatusOK,
			d.VirtualReadDir(3, re_vfs.AttributesMaskInodeNumber, reporter))
	})
}
