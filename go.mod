module github.com/buildbarn/bb-clientd

go 1.16

replace github.com/gordonklaus/ineffassign => github.com/gordonklaus/ineffassign v0.0.0-20201223204552-cba2d2a1d5d9

require (
	github.com/bazelbuild/remote-apis v0.0.0-20211004185116-636121a32fa7
	github.com/buildbarn/bb-remote-execution v0.0.0-20211205210515-1decd60e9ed2
	github.com/buildbarn/bb-storage v0.0.0-20211205205823-634fb8ef62e0
	github.com/hanwen/go-fuse/v2 v2.1.0
	google.golang.org/genproto v0.0.0-20211207154714-918901c715cf
	google.golang.org/grpc v1.42.0
	google.golang.org/protobuf v1.27.1
)
