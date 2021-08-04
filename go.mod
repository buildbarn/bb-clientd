module github.com/buildbarn/bb-clientd

go 1.16

replace github.com/gordonklaus/ineffassign => github.com/gordonklaus/ineffassign v0.0.0-20201223204552-cba2d2a1d5d9

require (
	github.com/bazelbuild/remote-apis v0.0.0-20210718193713-0ecef08215cf
	github.com/buildbarn/bb-remote-execution v0.0.0-20210804080527-465570d18854
	github.com/buildbarn/bb-storage v0.0.0-20210804073654-6536dcb16de6
	github.com/golang/protobuf v1.5.2
	github.com/hanwen/go-fuse/v2 v2.1.0
	google.golang.org/genproto v0.0.0-20210803142424-70bd63adacf2
	google.golang.org/grpc v1.39.0
	google.golang.org/protobuf v1.27.1
)
