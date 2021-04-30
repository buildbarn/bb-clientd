module github.com/buildbarn/bb-clientd

go 1.15

require (
	github.com/bazelbuild/remote-apis v0.0.0-20210309154856-0943dc4e70e1
	github.com/buildbarn/bb-remote-execution v0.0.0-20210430150052-ed1e2f4f8ecf
	github.com/buildbarn/bb-storage v0.0.0-20210430141345-6b1bb480c012
	github.com/golang/protobuf v1.5.1
	github.com/hanwen/go-fuse/v2 v2.1.0
	google.golang.org/genproto v0.0.0-20210310155132-4ce2db91004e
	google.golang.org/grpc v1.36.0
)

replace github.com/golang/mock => github.com/golang/mock v1.4.4-0.20201026142858-99aa9272d551

replace github.com/gordonklaus/ineffassign => github.com/gordonklaus/ineffassign v0.0.0-20201223204552-cba2d2a1d5d9
