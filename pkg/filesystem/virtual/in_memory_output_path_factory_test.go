package virtual_test

import (
	"sort"
	"testing"
	"time"

	"github.com/buildbarn/bb-clientd/internal/mock"
	cd_vfs "github.com/buildbarn/bb-clientd/pkg/filesystem/virtual"
	re_vfs "github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestInMemoryOutputPathFactory(t *testing.T) {
	ctrl := gomock.NewController(t)

	filePool := mock.NewMockFilePool(ctrl)
	symlinkFactory := mock.NewMockSymlinkFactory(ctrl)
	handleAllocator := mock.NewMockStatefulHandleAllocator(ctrl)
	clock := mock.NewMockClock(ctrl)
	outputPathFactory := cd_vfs.NewInMemoryOutputPathFactory(filePool, symlinkFactory, handleAllocator, sort.Sort, clock)

	// StartInitialBuild() should create a new in-memory directory.
	handleAllocation := mock.NewMockStatefulHandleAllocation(ctrl)
	handleAllocator.EXPECT().New().Return(handleAllocation)
	directoryHandle := mock.NewMockStatefulDirectoryHandle(ctrl)
	handleAllocation.EXPECT().AsStatefulDirectory(gomock.Any()).Return(directoryHandle)
	clock.EXPECT().Now().Return(time.Unix(1000, 0))

	casFileFactory := mock.NewMockCASFileFactory(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	outputPath := outputPathFactory.StartInitialBuild(
		path.MustNewComponent("my-output-path"),
		casFileFactory,
		digest.MustNewInstanceName("default-scheduler"),
		errorLogger)

	// The last data modification time on the root directory
	// should be the same as provided above.
	directoryHandle.EXPECT().GetAttributes(re_vfs.AttributesMaskLastDataModificationTime, gomock.Any())

	var attributes re_vfs.Attributes
	outputPath.VirtualGetAttributes(re_vfs.AttributesMaskLastDataModificationTime, &attributes)
	lastDataModificationTime, ok := attributes.GetLastDataModificationTime()
	require.True(t, ok)
	require.Equal(t, time.Unix(1000, 0), lastDataModificationTime)
}
