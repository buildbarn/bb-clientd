module github.com/buildbarn/bb-clientd

go 1.15

require (
	github.com/bazelbuild/remote-apis v0.0.0-20201209220655-9e72daff42c9
	github.com/buildbarn/bb-remote-execution v0.0.0-20210207110010-0079051e8739
	github.com/buildbarn/bb-storage v0.0.0-20210207101039-9507e33a5caf
	github.com/golang/protobuf v1.4.3
	github.com/hanwen/go-fuse/v2 v2.0.3
	google.golang.org/genproto v0.0.0-20210207032614-bba0dbe2a9ea
	google.golang.org/grpc v1.35.0
)

replace github.com/golang/mock => github.com/golang/mock v1.4.4-0.20201026142858-99aa9272d551

replace github.com/gordonklaus/ineffassign => github.com/gordonklaus/ineffassign v0.0.0-20201223204552-cba2d2a1d5d9
