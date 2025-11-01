package outputpathpersistency

import (
	"bufio"
	"encoding/binary"
	"io"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/outputpathpersistency"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// offsetWriter implements an io.Writer on top of an io.WriterAt by
// adding a write cursor.
type offsetWriter struct {
	w           io.WriterAt
	offsetBytes int64
}

func (w *offsetWriter) Write(p []byte) (n int, err error) {
	n, err = w.w.WriteAt(p, w.offsetBytes)
	w.offsetBytes += int64(n)
	return n, err
}

type fileWriter struct {
	rawWriter      io.WriterAt
	bufferedWriter *bufio.Writer
	offsetBytes    int64
}

// NewFileWriter creates a writer for output path state files. Against
// this handle zero or more WriteDirectory() calls should be performed,
// followed by one call to Finalize().
func NewFileWriter(rawWriter io.WriterAt) Writer {
	return &fileWriter{
		rawWriter: rawWriter,
		bufferedWriter: bufio.NewWriterSize(&offsetWriter{
			w:           rawWriter,
			offsetBytes: headerSizeBytes,
		}, 1024*1024),
		offsetBytes: headerSizeBytes,
	}
}

func (w *fileWriter) writeMessage(m proto.Message) (*outputpathpersistency.FileRegion, error) {
	data, err := proto.Marshal(m)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if len(data) == 0 {
		// Zero-sized messages don't need to be stored explicitly.
		return nil, nil
	}
	offsetBytes := w.offsetBytes
	n, err := w.bufferedWriter.Write(data)
	w.offsetBytes += int64(n)
	if err != nil {
		return nil, err
	}
	return &outputpathpersistency.FileRegion{
		OffsetBytes: offsetBytes,
		SizeBytes:   int32(len(data)),
	}, nil
}

// WriteDirectory writes a single directory into the output path state
// file. This method returns a FileRegion, which may be embedded into
// other Directory objects to refer to a directory.
func (w *fileWriter) WriteDirectory(directory *outputpathpersistency.Directory) (*outputpathpersistency.FileRegion, error) {
	return w.writeMessage(directory)
}

// Finalize an output path state file by writing a root directory to it
// and updating the file's header.
func (w *fileWriter) Finalize(rootDirectory *outputpathpersistency.RootDirectory) error {
	// Store the final directory.
	fileRegion, err := w.writeMessage(rootDirectory)
	if err != nil {
		return err
	}
	if err := w.bufferedWriter.Flush(); err != nil {
		return err
	}

	// Update the header.
	var header [headerSizeBytes]byte
	copy(header[:], headerMagic)
	binary.LittleEndian.PutUint64(header[4:], uint64(fileRegion.GetOffsetBytes()))
	binary.LittleEndian.PutUint32(header[4+8:], uint32(fileRegion.GetSizeBytes()))
	_, err = w.rawWriter.WriteAt(header[:], 0)
	return err
}
