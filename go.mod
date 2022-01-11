module github.com/buildbarn/bb-clientd

go 1.16

replace github.com/gordonklaus/ineffassign => github.com/gordonklaus/ineffassign v0.0.0-20201223204552-cba2d2a1d5d9

require (
	github.com/bazelbuild/remote-apis v0.0.0-20211004185116-636121a32fa7
	github.com/buildbarn/bb-remote-execution v0.0.0-20211222101503-592ecb371dfd
	github.com/buildbarn/bb-storage v0.0.0-20220110215627-b01d6cf6af02
	github.com/hanwen/go-fuse/v2 v2.1.0
	golang.org/x/sys v0.0.0-20220110181412-a018aaa089fe
	google.golang.org/genproto v0.0.0-20220107163113-42d7afdf6368
	google.golang.org/grpc v1.43.0
	google.golang.org/protobuf v1.27.1
)
