local workflows_template = import 'tools/github_workflows/workflows_template.libsonnet';

workflows_template.getWorkflows(
  ['bb_clientd'],
  ['bb_clientd:bb_clientd'],
)
{
  // TODO: Should we integrate this into the workflows template?
  'main.yaml'+: {
    jobs+: {
      build_and_test+: {
        steps+: [
          {
            name: 'linux_amd64: build bb_clientd.deb',
            run: 'bazel build --stamp --platforms=@rules_go//go/toolchain:linux_amd64 //:bb_clientd_deb',
            'if': "matrix.host.cross_compile || matrix.host.platform_name == 'linux_amd64'",
          },
          {
            name: 'linux_amd64: copy bb_clientd.deb',
            run: 'rm -f bb_clientd.deb && cp bazel-bin/bb_clientd_deb.deb bb_clientd.deb',
            'if': 'matrix.host.upload',
          },
          {
            name: 'linux_amd64: upload bb_clientd.deb',
            uses: 'actions/upload-artifact@v4',
            with: {
              name: 'bb_clientd.linux_amd64.deb',
              path: 'bb_clientd.deb',
            },
            'if': 'matrix.host.upload',
          },
        ],
      },
    },
  },
}
