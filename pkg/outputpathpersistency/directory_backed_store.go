package outputpathpersistency

import (
	"io"
	"syscall"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/outputpathpersistency"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type directoryBackedStore struct {
	directory                 filesystem.Directory
	maximumStateFileSizeBytes int64
}

// NewDirectoryBackedStore creates a store for persisting the contents
// of output paths that is backed by a directory.
func NewDirectoryBackedStore(directory filesystem.Directory, maximumStateFileSizeBytes int64) Store {
	return &directoryBackedStore{
		directory:                 directory,
		maximumStateFileSizeBytes: maximumStateFileSizeBytes,
	}
}

func (s *directoryBackedStore) Read(outputBaseID path.Component) (ReadCloser, *outputpathpersistency.RootDirectory, error) {
	f, err := s.directory.OpenRead(outputBaseID)
	if err != nil {
		return nil, nil, util.StatusWrap(err, "Failed to open state file")
	}
	reader, rootDirectory, err := NewFileReader(f, s.maximumStateFileSizeBytes)
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	return struct {
		Reader
		io.Closer
	}{
		Reader: reader,
		Closer: f,
	}, rootDirectory, err
}

func getTemporaryName(outputBaseID path.Component) (path.Component, error) {
	outputBaseIDStr := outputBaseID.String()
	temporaryName, ok := path.NewComponent(outputBaseIDStr + ".tmp")
	if !ok {
		return temporaryName, status.Errorf(codes.InvalidArgument, "Cannot obtain a temporary filename for output base ID %#v", outputBaseIDStr)
	}
	return temporaryName, nil
}

func (s *directoryBackedStore) Write(outputBaseID path.Component) (WriteCloser, error) {
	temporaryName, err := getTemporaryName(outputBaseID)
	if err != nil {
		return nil, err
	}
	if err := s.directory.Remove(temporaryName); err != nil && err != syscall.ENOENT {
		return nil, util.StatusWrap(err, "Failed to remove persistent state temporary file")
	}
	temporaryFile, err := s.directory.OpenWrite(temporaryName, filesystem.CreateExcl(0o666))
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to create persistent state temporary file")
	}
	return &directoryBackedWriter{
		directory:     s.directory,
		outputBaseID:  outputBaseID,
		temporaryName: temporaryName,
		temporaryFile: temporaryFile,
		writer:        NewFileWriter(temporaryFile),
	}, nil
}

func (s *directoryBackedStore) Clean(outputBaseID path.Component) error {
	temporaryName, err := getTemporaryName(outputBaseID)
	if err != nil {
		return err
	}

	if err := s.directory.Remove(outputBaseID); err != nil && err != syscall.ENOENT {
		return util.StatusWrap(err, "Failed to remove persistent state file")
	}
	if err := s.directory.Remove(temporaryName); err != nil && err != syscall.ENOENT {
		return util.StatusWrap(err, "Failed to remove persistent state temporary file")
	}
	return nil
}

type directoryBackedWriter struct {
	directory     filesystem.Directory
	outputBaseID  path.Component
	temporaryName path.Component
	temporaryFile filesystem.FileWriter
	writer        Writer
}

func (w *directoryBackedWriter) WriteDirectory(directory *outputpathpersistency.Directory) (*outputpathpersistency.FileRegion, error) {
	return w.writer.WriteDirectory(directory)
}

func (w *directoryBackedWriter) Finalize(rootDirectory *outputpathpersistency.RootDirectory) error {
	if err := w.writer.Finalize(rootDirectory); err != nil {
		w.Close()
		return err
	}
	if err := w.temporaryFile.Sync(); err != nil {
		w.Close()
		return util.StatusWrap(err, "Failed to synchronize contents of persistent state temporary file")
	}
	if err := w.temporaryFile.Close(); err != nil {
		w.directory.Remove(w.temporaryName)
		return util.StatusWrap(err, "Failed to close persistent state temporary file")
	}
	if err := w.directory.Rename(w.temporaryName, w.directory, w.outputBaseID); err != nil {
		w.directory.Remove(w.temporaryName)
		return util.StatusWrap(err, "Failed to rename persistent state temporary file")
	}
	return nil
}

func (w *directoryBackedWriter) Close() error {
	err1 := w.temporaryFile.Close()
	err2 := w.directory.Remove(w.temporaryName)
	if err1 != nil {
		return util.StatusWrap(err1, "Failed to close persistent state temporary file")
	}
	if err2 != nil && err2 != syscall.ENOENT {
		return util.StatusWrap(err1, "Failed to remove persistent state temporary file")
	}
	return nil
}
