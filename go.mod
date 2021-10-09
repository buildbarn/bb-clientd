module github.com/buildbarn/bb-clientd

go 1.16

replace github.com/gordonklaus/ineffassign => github.com/gordonklaus/ineffassign v0.0.0-20201223204552-cba2d2a1d5d9

require (
	github.com/bazelbuild/remote-apis v0.0.0-20211004185116-636121a32fa7
	github.com/buildbarn/bb-remote-execution v0.0.0-20211009082456-49b2d95ca693
	github.com/buildbarn/bb-storage v0.0.0-20211009063419-74e925917e4c
	github.com/hanwen/go-fuse/v2 v2.1.0
	google.golang.org/genproto v0.0.0-20211008145708-270636b82663
	google.golang.org/grpc v1.41.0
	google.golang.org/protobuf v1.27.1
)
