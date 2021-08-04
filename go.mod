module github.com/buildbarn/bb-clientd

go 1.16

replace github.com/gordonklaus/ineffassign => github.com/gordonklaus/ineffassign v0.0.0-20201223204552-cba2d2a1d5d9

require (
	github.com/bazelbuild/remote-apis v0.0.0-20210718193713-0ecef08215cf
	github.com/buildbarn/bb-remote-execution v0.0.0-20210804170224-1659f80f7d2a
	github.com/buildbarn/bb-storage v0.0.0-20210804150025-5b6c01e8a7fc
	github.com/golang/protobuf v1.5.2
	github.com/hanwen/go-fuse/v2 v2.1.0
	google.golang.org/genproto v0.0.0-20210803142424-70bd63adacf2
	google.golang.org/grpc v1.39.0
	google.golang.org/protobuf v1.27.1
)
