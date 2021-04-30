package outputpathpersistency

// The size of the header that is prepended to an output path state
// file. It consists of a four-byte magic, followed by an offset and
// size of the RootDirectory message.
const headerSizeBytes = 4 + 8 + 4

// The magic of output path state files. This value should be changed
// every time we make an incompatible change to the file format. Because
// files with an invalid magic are effectively ignored, this will cause
// a smooth transition from one format to another.
var headerMagic = []byte{0xfa, 0x12, 0xa4, 0xa5}
