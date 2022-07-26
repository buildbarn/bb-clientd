#!/bin/sh

set -eu

# Clean up stale FUSE mounts from previous invocations.
fusermount -u ~/bb_clientd || true

# Remove UNIX socket, so that builds will not attept to send gRPC traffic.
rm -f ~/.cache/bb_clientd/grpc

if [ "$1" = "start" ]; then
  # Create directories that are used by bb_clientd.
  mkdir -p \
      ~/.cache/bb_clientd/ac/persistent_state \
      ~/.cache/bb_clientd/cas/persistent_state \
      ~/.cache/bb_clientd/outputs \
      ~/bb_clientd

  # Discard logs of the previous invocation.
  rm -f ~/.cache/bb_clientd/log

  # Use either the user provided or system-wide configuration file, based
  # on whether the former is present.
  config_file=/usr/lib/bb_clientd/bb_clientd.jsonnet
  personal_config_file="${HOME}/.config/bb_clientd.jsonnet"
  if [ -f "${personal_config_file}" ]; then
    ln -sf "${config_file}" "${HOME}/.config/bb_clientd_defaults.jsonnet"
    config_file="${personal_config_file}"
  fi

  OS=$(uname) exec /usr/bin/bb_clientd "${config_file}"
fi
