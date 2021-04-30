package outputpathpersistency

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/outputpathpersistency"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// offsetReader implements an io.Reader on top of an io.ReaderAt by
// adding a read cursor.
type offsetReader struct {
	r      io.ReaderAt
	offset int64
}

func (r *offsetReader) Read(p []byte) (n int, err error) {
	n, err = r.r.ReadAt(p, r.offset)
	r.offset += int64(n)
	return
}

type fileReader struct {
	rawReader          io.ReaderAt
	maximumOffsetBytes int64
}

// NewFileReader reads the header and the root directory from an output
// path state file. In addition to the contents of the root directory, a
// Reader is returned that may be used to access child directories.
func NewFileReader(rawReader io.ReaderAt, maximumStateFileSizeBytes int64) (Reader, *outputpathpersistency.RootDirectory, error) {
	// Read the header from the file and validate it.
	var header [headerSizeBytes]byte
	if _, err := io.ReadFull(&offsetReader{r: rawReader}, header[:]); err != nil {
		return nil, nil, util.StatusWrap(err, "Failed to read header")
	}
	if !bytes.Equal(header[:4], headerMagic) {
		return nil, nil, status.Error(codes.InvalidArgument, "Header contains invalid magic")
	}

	// Read the root directory message that is stored at the offset
	// embedded in the header.
	r := fileReader{
		rawReader:          rawReader,
		maximumOffsetBytes: maximumStateFileSizeBytes,
	}
	var rootDirectory outputpathpersistency.RootDirectory
	childReader, err := r.readMessage(int64(binary.LittleEndian.Uint64(header[4:])), int32(binary.LittleEndian.Uint32(header[4+8:])), &rootDirectory)
	if err != nil {
		return nil, nil, util.StatusWrap(err, "Failed to read root directory")
	}
	return childReader, &rootDirectory, nil
}

func (r *fileReader) ReadDirectory(fileRegion *outputpathpersistency.FileRegion) (Reader, *outputpathpersistency.Directory, error) {
	if fileRegion == nil {
		// Empty subdirectories are not stored explicitly.
		return &fileReader{}, &outputpathpersistency.Directory{}, nil
	}

	var directory outputpathpersistency.Directory
	childReader, err := r.readMessage(fileRegion.OffsetBytes, fileRegion.SizeBytes, &directory)
	if err != nil {
		return nil, nil, err
	}
	return childReader, &directory, nil
}

func (r *fileReader) readMessage(offsetBytes int64, sizeBytes int32, m proto.Message) (Reader, error) {
	// Perform bounds checking on the offset and size of the message
	// to make sure state files don't contain cyclic references.
	// It's only permitted to refer to messages that are stored
	// previously.
	endBytes := offsetBytes + int64(sizeBytes)
	if offsetBytes < headerSizeBytes || offsetBytes > r.maximumOffsetBytes || sizeBytes <= 0 || endBytes > r.maximumOffsetBytes {
		return nil, status.Errorf(codes.InvalidArgument, "Message at offset [%d, %d) does not lie in permitted range [%d, %d)", offsetBytes, endBytes, headerSizeBytes, r.maximumOffsetBytes)
	}
	data := make([]byte, sizeBytes)
	if _, err := io.ReadFull(&offsetReader{r: r.rawReader, offset: offsetBytes}, data); err != nil {
		return nil, err
	}
	if err := proto.Unmarshal(data, m); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &fileReader{
		rawReader:          r.rawReader,
		maximumOffsetBytes: offsetBytes,
	}, nil
}
