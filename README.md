# The Buildbarn client daemon [![Build status](https://github.com/buildbarn/bb-clientd/workflows/master/badge.svg)](https://github.com/buildbarn/bb-clientd/actions) [![PkgGoDev](https://pkg.go.dev/badge/github.com/buildbarn/bb-clientd)](https://pkg.go.dev/github.com/buildbarn/bb-clientd) [![Go Report Card](https://goreportcard.com/badge/github.com/buildbarn/bb-clientd)](https://goreportcard.com/report/github.com/buildbarn/bb-clientd)

Whereas most of the other Buildbarn repositories provide server-side
infrastructure, this repository provides a tool that's intended to be
run on your personal system: the Buildbarn client daemon, bb\_clientd
for short. The goal of bb\_clientd is twofold:

1. To provide insight in, and access to the data stored by
   [Remote Execution](https://github.com/bazelbuild/remote-apis) services.
2. To speed up builds that use Remote Execution.

Even though this project is developed as part of the Buildbarn project,
and also reuses many of Buildbarn's components, bb\_clientd is expected
to be compatible with other implementations of the Remote Execution API.

This document will explain how to set up bb\_clientd and demonstrate its
features.

## Obtaining bb\_clientd

There are various ways in which a copy of bb\_clientd may be obtained:

- By running `bazel run //cmd/bb_clientd` inside this repository.
- By downloading [a precompiled binary built through GitHub Actions](https://github.com/buildbarn/bb-clientd/actions?query=workflow%3Amaster).
- By downloading [a container image from Docker Hub](https://hub.docker.com/r/buildbarn/bb-clientd).

bb\_clientd is only available for Linux and macOS for the time being.
Keep in mind that running bb\_clientd inside a Docker container may
require it to be privileged, as bb\_clientd creates a FUSE mount.

## Running bb\_clientd

After carefully reviewing the contents of
[configs/bb\_clientd.jsonnet](configs/bb_clientd.jsonnet) and making
changes where needed, you may run the following commands to launch
bb\_clientd:

```sh
umount ~/bb_clientd; fusermount -u ~/bb_clientd
mkdir -p ~/.cache/bb_clientd/cas/persistent_state ~/bb_clientd
bazel run //cmd/bb_clientd $(bazel info workspace)/configs/bb_clientd.jsonnet
```

You may validate that bb\_clientd is running by inspecting the top-level
directory of its FUSE mount:

```
$ ls -l ~/bb_clientd
total 0
d--x--x--x  1 root  wheel  0 Jan  1  2000 cas
dr-xr-xr-x  2 root  wheel  0 Jan  1  2000 outputs
drwxrwxrwx  1 root  wheel  0 Jan  1  2000 scratch
```

## Using bb\_clientd...

### ... as a proxy for gRPC requests

Bazel's way of communicating with the Remote Execution service may
sometimes be inefficient:

- Even though Bazel uses hashes as part of its state, it is not content
  addressed. This means that it may frequently download files
  redundantly. For example, in case a rebuild of an action yields files
  with the same contents, Bazel will still delete the old files and
  download them once again.

- Bazel does not cache the results of FindMissingBlobs() calls, meaning
  that it often looks up digests that it already requested recently.

bb\_clientd solves this by adding local caching. This cache is used for
all traffic passing through a single instance of bb\_clientd, meaning
that you will even see speedups when building multiple checkouts of the
same project, after running `bazel clean`, switching between clusters,
etc.

If you normally run Bazel to build with remote execution as follows:

```
bazel build --remote_executor mycluster-prod.example.com --remote_instance_name hello [more options]
```

You can let it use bb\_clientd by running it like this instead:

```
bazel build --remote_executor unix:${HOME}/.cache/bb_clientd/grpc --remote_instance_name mycluster-prod.example.com/hello [more options]
```

Notice how the hostname of the cluster has become a prefix of
`--remote_instance_name`. This is because bb\_clientd's example
configuration provides support for routing requests to multiple
clusters. It uses the instance name to determine to which cluster
traffic needs to be routed.

Note that you need Bazel 3.5.0 for this to work, as older versions of
Bazel [don't allow you to provide UNIX socket endpoints](https://github.com/bazelbuild/bazel/pull/11722).

### ... as a tool for exploring the Content Addressable Storage

When using the default configuration file, bb\_clientd will memorize the
credentials that were attached to any RPCs that were forwarded to the
Remote Execution service. This means that bb\_clientd can give you
direct access to your cluster's Content Addressable Storage, at least
until your token expires or bb\_clientd restarts.

The FUSE file system provided by bb\_clientd automatically generates a
"blobs" directory for every instance name provided:

```
$ ls -l ~/bb_clientd/cas/mycluster-prod.example.com/hello/blobs
total 0
d--x--x--x  1 root  wheel  0 Jan  1  2000 directory
d--x--x--x  1 root  wheel  0 Jan  1  2000 executable
d--x--x--x  1 root  wheel  0 Jan  1  2000 file
d--x--x--x  1 root  wheel  0 Jan  1  2000 tree
```

You can use it to obtain the contents of individual files:

```
$ file -b ~/bb_clientd/cas/mycluster-prod.example.com/hello/blobs/file/22184d8f153d9e28ae827297dbe0c3459554abb384d7b5c9dc292e6c8f596882-799796
C++ source, UTF-8 Unicode (with BOM) text, with very long lines
```

Or view the contents of entire directories:

```
$ cd ~/bb_clientd/cas/mycluster-prod.example.com/hello/blobs/directory/aa58f5f12edcd4695af98aed13cf87c79bd537e5bfc2c0d21fb0b6e695c94ce9-175
$ find . -ls
 7826520175049250699  0 dr-xr-xr-x    1 root  root     0 Jan  1  2000 .
17991803894754190639  0 dr-xr-xr-x    1 root  root     0 Jan  1  2000 ./runfiles
18404615058361292775  0 -r-xr-xr-x 9999 root  root  8340 Jan  1  2000 ./runfiles/runfiles.h
 7642484527095133707  0 -r-xr-xr-x 9999 root  root   744 Jan  1  2000 ./grep-includes.sh
```

All of the content accessed through the "cas" directory is lazy-loading,
and is cached locally.

### ... as a playground

The FUSE file system also provides a "scratch" directory that you can
(mostly) use like an ordinary directory on your system. Because it
resides on the same file system as the "cas" directory shown above, it
is possible to create hard links to files that are backed by the Content
Addressable Storage. This may be useful when trying to reproduce
problems locally, without needing to download all of the files locally.

```
$ ln ~/bb_clientd/cas/mycluster-prod.example.com/hello/blobs/file/22184d8f153d9e28ae827297dbe0c3459554abb384d7b5c9dc292e6c8f596882-799796 ~/bb_clientd/scratch/foo.cpp
$ ls -l ~/bb_clientd/scratch/foo.cpp
-r--r--r-- 9999 root root 799796 Jan  1  2000 ~/bb_clientd/scratch/foo.cpp
```

It is also possible to directly write data into the file system. The
backing store for this is configured through the "filePool" option in
the bb\_clientd configuration file. Keep in mind that none of the data
written into the "scratch" directory is persisted across restarts of
bb\_clientd!

### ... to perform Remote Builds without the Bytes

Bazel 0.25 and later provide a feature named ["Remote Builds without the Bytes"](https://blog.bazel.build/2019/05/07/builds-without-bytes.html).
By enabling flags such as `--remote_download_minimal`, Bazel will no
longer download output files of build actions and store them in
`bazel-out/`. Though this speeds up builds significantly, there are two
disadvantages:

- You can no longer access output files. This may be acceptable if you
  only want to know whether a build succeeds (e.g., CI presubmit
  checks), but it cannot be used universally.
- It doesn't provide fast incremental builds. For each of the files that
  is normally downloaded, Bazel creates an in-memory reference to the
  remote instance of the file. This information is discarded at the end
  of the build, meaning that Bazel can't continue where it left off.

To solve these issues, bb\_clientd implements a gRPC API named the
Remote Output Service. Bazel can use this API to store the entire
`bazel-out/` directory inside the FUSE file system. Every time Bazel
needs to download a file from the Remote Execution service, it calls
into a `BatchCreate()` gRPC method. bb\_clientd implements this method
by creating lazy-loading files, just like the ones in the "cas"
directory. This means that you can enjoy the performance improvements of
"Remote Builds without the Bytes", but not with the restrictions that it
currently imposes.

The Remote Output Service can be used by building a copy of Bazel that
includes [upstream PR#12823](https://github.com/bazelbuild/bazel/pull/12823).
You may then run Bazel with the following additional command line flags:

```
--remote_output_service unix:${HOME}/.cache/bb_clientd/grpc --remote_output_service_output_path_prefix ${HOME}/bb_clientd/outputs
```

We hope that upstream versions of Bazel ship with integrated support for
the Remote Output Service going forward.
