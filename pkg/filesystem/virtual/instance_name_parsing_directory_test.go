package virtual_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/buildbarn/bb-clientd/internal/mock"
	cd_vfs "github.com/buildbarn/bb-clientd/pkg/filesystem/virtual"
	re_vfs "github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func instanceNameParsingDirectoryExpectCreate(t *testing.T, ctrl *gomock.Controller, handleAllocator *mock.MockStatelessHandleAllocator, expectedIdentifier []byte) {
	handleAllocation := mock.NewMockStatelessHandleAllocation(ctrl)
	handleAllocator.EXPECT().New(gomock.Any()).DoAndReturn(func(id io.WriterTo) re_vfs.StatelessHandleAllocation {
		actualIdentifier := bytes.NewBuffer(nil)
		n, err := id.WriteTo(actualIdentifier)
		require.NoError(t, err)
		require.Equal(t, int64(len(expectedIdentifier)), n)
		require.Equal(t, expectedIdentifier, actualIdentifier.Bytes())
		return handleAllocation
	})
	handleAllocation.EXPECT().AsStatelessDirectory(gomock.Any()).
		DoAndReturn(func(directory re_vfs.Directory) re_vfs.Directory { return directory })
}

func TestInstanceNameParsingDirectory(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	lookupFunc := mock.NewMockInstanceNameLookupFunc(ctrl)
	rootHandleAllocation := mock.NewMockStatelessHandleAllocation(ctrl)
	handleAllocator := mock.NewMockStatelessHandleAllocator(ctrl)
	rootHandleAllocation.EXPECT().AsStatelessAllocator().Return(handleAllocator)
	instanceNameParsingDirectoryExpectCreate(t, ctrl, handleAllocator, []byte("/"))
	d := cd_vfs.NewInstanceNameParsingDirectory(
		rootHandleAllocation,
		map[path.Component]cd_vfs.InstanceNameLookupFunc{
			path.MustNewComponent("blobs"): lookupFunc.Call,
		})
	attributesMask := re_vfs.AttributesMaskChangeID |
		re_vfs.AttributesMaskFileType |
		re_vfs.AttributesMaskLinkCount |
		re_vfs.AttributesMaskPermissions |
		re_vfs.AttributesMaskSizeBytes

	t.Run("EmptyInstanceName", func(t *testing.T) {
		// Parse the empty instance name by just traversing the
		// path "blobs".
		mockChildDirectory := mock.NewMockVirtualDirectory(ctrl)
		lookupFunc.EXPECT().Call(digest.EmptyInstanceName).Return(mockChildDirectory)
		mockChildDirectory.EXPECT().VirtualGetAttributes(
			ctx,
			re_vfs.AttributesMaskInodeNumber,
			gomock.Any(),
		).Do(func(ctx context.Context, requested re_vfs.AttributesMask, out *re_vfs.Attributes) {
			out.SetInodeNumber(123)
		})

		var out re_vfs.Attributes
		child, s := d.VirtualLookup(ctx, path.MustNewComponent("blobs"), re_vfs.AttributesMaskInodeNumber, &out)
		require.Equal(t, re_vfs.StatusOK, s)
		require.Equal(t, re_vfs.DirectoryChild{}.FromDirectory(mockChildDirectory), child)
		require.Equal(t, *(&re_vfs.Attributes{}).SetInodeNumber(123), out)
	})

	t.Run("SingleComponentInstanceName", func(t *testing.T) {
		// Parse the instance name "hello" by traversing the
		// path "hello/blobs".
		instanceNameParsingDirectoryExpectCreate(t, ctrl, handleAllocator, []byte("hello//"))

		var out1 re_vfs.Attributes
		child1, s := d.VirtualLookup(ctx, path.MustNewComponent("hello"), attributesMask, &out1)
		require.Equal(t, re_vfs.StatusOK, s)
		require.Equal(
			t,
			*(&re_vfs.Attributes{}).
				SetChangeID(0).
				SetFileType(filesystem.FileTypeDirectory).
				SetLinkCount(re_vfs.ImplicitDirectoryLinkCount).
				SetPermissions(re_vfs.PermissionsExecute).
				SetSizeBytes(0),
			out1)

		mockChildDirectory := mock.NewMockVirtualDirectory(ctrl)
		lookupFunc.EXPECT().Call(digest.MustNewInstanceName("hello")).Return(mockChildDirectory)
		mockChildDirectory.EXPECT().VirtualGetAttributes(
			ctx,
			re_vfs.AttributesMaskInodeNumber,
			gomock.Any(),
		).Do(func(ctx context.Context, requested re_vfs.AttributesMask, out *re_vfs.Attributes) {
			out.SetInodeNumber(123)
		})

		childDirectory1, _ := child1.GetPair()
		var out2 re_vfs.Attributes
		child2, s := childDirectory1.VirtualLookup(ctx, path.MustNewComponent("blobs"), re_vfs.AttributesMaskInodeNumber, &out2)
		require.Equal(t, re_vfs.StatusOK, s)
		require.Equal(t, re_vfs.DirectoryChild{}.FromDirectory(mockChildDirectory), child2)
		require.Equal(t, *(&re_vfs.Attributes{}).SetInodeNumber(123), out2)
	})

	t.Run("DoubleComponentInstanceName", func(t *testing.T) {
		// Parse the instance name "hello/world" by traversing
		// the path "hello/world/blobs".
		instanceNameParsingDirectoryExpectCreate(t, ctrl, handleAllocator, []byte("hello//"))

		var out1 re_vfs.Attributes
		child1, s := d.VirtualLookup(ctx, path.MustNewComponent("hello"), attributesMask, &out1)
		require.Equal(t, re_vfs.StatusOK, s)
		require.Equal(
			t,
			*(&re_vfs.Attributes{}).
				SetChangeID(0).
				SetFileType(filesystem.FileTypeDirectory).
				SetLinkCount(re_vfs.ImplicitDirectoryLinkCount).
				SetPermissions(re_vfs.PermissionsExecute).
				SetSizeBytes(0),
			out1)

		instanceNameParsingDirectoryExpectCreate(t, ctrl, handleAllocator, []byte("hello/world//"))

		childDirectory1, _ := child1.GetPair()
		var out2 re_vfs.Attributes
		child2, s := childDirectory1.VirtualLookup(ctx, path.MustNewComponent("world"), attributesMask, &out2)
		require.Equal(t, re_vfs.StatusOK, s)
		require.Equal(
			t,
			*(&re_vfs.Attributes{}).
				SetChangeID(0).
				SetFileType(filesystem.FileTypeDirectory).
				SetLinkCount(re_vfs.ImplicitDirectoryLinkCount).
				SetPermissions(re_vfs.PermissionsExecute).
				SetSizeBytes(0),
			out2)

		mockChildDirectory := mock.NewMockVirtualDirectory(ctrl)
		lookupFunc.EXPECT().Call(digest.MustNewInstanceName("hello/world")).Return(mockChildDirectory)
		mockChildDirectory.EXPECT().VirtualGetAttributes(
			ctx,
			re_vfs.AttributesMaskInodeNumber,
			gomock.Any(),
		).Do(func(ctx context.Context, requested re_vfs.AttributesMask, out *re_vfs.Attributes) {
			out.SetInodeNumber(123)
		})

		childDirectory2, _ := child2.GetPair()
		var out3 re_vfs.Attributes
		child3, s := childDirectory2.VirtualLookup(ctx, path.MustNewComponent("blobs"), re_vfs.AttributesMaskInodeNumber, &out3)
		require.Equal(t, re_vfs.StatusOK, s)
		require.Equal(t, re_vfs.DirectoryChild{}.FromDirectory(mockChildDirectory), child3)
		require.Equal(t, *(&re_vfs.Attributes{}).SetInodeNumber(123), out3)
	})

	t.Run("InvalidInstanceName", func(t *testing.T) {
		// Parse the instance name "operations" by traversing
		// the path "operations/blobs". This should return an
		// error, due to "operations" being a reserved pathname
		// component.
		instanceNameParsingDirectoryExpectCreate(t, ctrl, handleAllocator, []byte("operations//"))

		var out1 re_vfs.Attributes
		child1, s := d.VirtualLookup(ctx, path.MustNewComponent("operations"), attributesMask, &out1)
		require.Equal(t, re_vfs.StatusOK, s)
		require.Equal(
			t,
			*(&re_vfs.Attributes{}).
				SetChangeID(0).
				SetFileType(filesystem.FileTypeDirectory).
				SetLinkCount(re_vfs.ImplicitDirectoryLinkCount).
				SetPermissions(re_vfs.PermissionsExecute).
				SetSizeBytes(0),
			out1)

		childDirectory1, _ := child1.GetPair()
		var out2 re_vfs.Attributes
		_, s = childDirectory1.VirtualLookup(ctx, path.MustNewComponent("blobs"), 0, &out2)
		require.Equal(t, re_vfs.StatusErrNoEnt, s)
	})
}
