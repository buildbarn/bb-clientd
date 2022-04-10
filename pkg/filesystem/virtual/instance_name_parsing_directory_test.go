package virtual_test

import (
	"bytes"
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
	ctrl := gomock.NewController(t)

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
			re_vfs.AttributesMaskInodeNumber,
			gomock.Any(),
		).Do(func(requested re_vfs.AttributesMask, out *re_vfs.Attributes) {
			out.SetInodeNumber(123)
		})

		var out re_vfs.Attributes
		childDirectory, childFile, s := d.VirtualLookup(path.MustNewComponent("blobs"), re_vfs.AttributesMaskInodeNumber, &out)
		require.Equal(t, re_vfs.StatusOK, s)
		require.Equal(t, mockChildDirectory, childDirectory)
		require.Nil(t, childFile)
		require.Equal(t, *(&re_vfs.Attributes{}).SetInodeNumber(123), out)
	})

	t.Run("SingleComponentInstanceName", func(t *testing.T) {
		// Parse the instance name "hello" by traversing the
		// path "hello/blobs".
		instanceNameParsingDirectoryExpectCreate(t, ctrl, handleAllocator, []byte("hello//"))

		var out1 re_vfs.Attributes
		childDirectory1, childFile1, s := d.VirtualLookup(path.MustNewComponent("hello"), attributesMask, &out1)
		require.Equal(t, re_vfs.StatusOK, s)
		require.Nil(t, childFile1)
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
			re_vfs.AttributesMaskInodeNumber,
			gomock.Any(),
		).Do(func(requested re_vfs.AttributesMask, out *re_vfs.Attributes) {
			out.SetInodeNumber(123)
		})

		var out2 re_vfs.Attributes
		childDirectory2, childFile2, s := childDirectory1.VirtualLookup(path.MustNewComponent("blobs"), re_vfs.AttributesMaskInodeNumber, &out2)
		require.Equal(t, re_vfs.StatusOK, s)
		require.Equal(t, mockChildDirectory, childDirectory2)
		require.Nil(t, childFile2)
		require.Equal(t, *(&re_vfs.Attributes{}).SetInodeNumber(123), out2)
	})

	t.Run("DoubleComponentInstanceName", func(t *testing.T) {
		// Parse the instance name "hello/world" by traversing
		// the path "hello/world/blobs".
		instanceNameParsingDirectoryExpectCreate(t, ctrl, handleAllocator, []byte("hello//"))

		var out1 re_vfs.Attributes
		childDirectory1, childFile1, s := d.VirtualLookup(path.MustNewComponent("hello"), attributesMask, &out1)
		require.Equal(t, re_vfs.StatusOK, s)
		require.Nil(t, childFile1)
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

		var out2 re_vfs.Attributes
		childDirectory2, childFile2, s := childDirectory1.VirtualLookup(path.MustNewComponent("world"), attributesMask, &out2)
		require.Equal(t, re_vfs.StatusOK, s)
		require.Nil(t, childFile2)
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
			re_vfs.AttributesMaskInodeNumber,
			gomock.Any(),
		).Do(func(requested re_vfs.AttributesMask, out *re_vfs.Attributes) {
			out.SetInodeNumber(123)
		})

		var out3 re_vfs.Attributes
		childDirectory3, childFile3, s := childDirectory2.VirtualLookup(path.MustNewComponent("blobs"), re_vfs.AttributesMaskInodeNumber, &out3)
		require.Equal(t, re_vfs.StatusOK, s)
		require.Equal(t, mockChildDirectory, childDirectory3)
		require.Nil(t, childFile3)
		require.Equal(t, *(&re_vfs.Attributes{}).SetInodeNumber(123), out3)
	})

	t.Run("InvalidInstanceName", func(t *testing.T) {
		// Parse the instance name "operations" by traversing
		// the path "operations/blobs". This should return an
		// error, due to "operations" being a reserved pathname
		// component.
		instanceNameParsingDirectoryExpectCreate(t, ctrl, handleAllocator, []byte("operations//"))

		var out1 re_vfs.Attributes
		childDirectory1, childFile1, s := d.VirtualLookup(path.MustNewComponent("operations"), attributesMask, &out1)
		require.Equal(t, re_vfs.StatusOK, s)
		require.Nil(t, childFile1)
		require.Equal(
			t,
			*(&re_vfs.Attributes{}).
				SetChangeID(0).
				SetFileType(filesystem.FileTypeDirectory).
				SetLinkCount(re_vfs.ImplicitDirectoryLinkCount).
				SetPermissions(re_vfs.PermissionsExecute).
				SetSizeBytes(0),
			out1)

		var out2 re_vfs.Attributes
		_, _, s = childDirectory1.VirtualLookup(path.MustNewComponent("blobs"), 0, &out2)
		require.Equal(t, re_vfs.StatusErrNoEnt, s)
	})
}
