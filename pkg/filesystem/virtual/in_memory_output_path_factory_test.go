package virtual_test

import (
	"context"
	"sort"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-clientd/internal/mock"
	cd_vfs "github.com/buildbarn/bb-clientd/pkg/filesystem/virtual"
	re_vfs "github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/stretchr/testify/require"

	"go.uber.org/mock/gomock"
)

func TestInMemoryOutputPathFactory(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

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
		digest.MustNewFunction("default-scheduler", remoteexecution.DigestFunction_SHA256),
		errorLogger)

	// The last data modification time on the root directory
	// should be the same as provided above.
	directoryHandle.EXPECT().GetAttributes(re_vfs.AttributesMaskLastDataModificationTime, gomock.Any())

	var attributes re_vfs.Attributes
	outputPath.VirtualGetAttributes(ctx, re_vfs.AttributesMaskLastDataModificationTime, &attributes)
	lastDataModificationTime, ok := attributes.GetLastDataModificationTime()
	require.True(t, ok)
	require.Equal(t, time.Unix(1000, 0), lastDataModificationTime)
}
