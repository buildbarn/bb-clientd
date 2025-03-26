#!/bin/sh

set -eu

# Clean up stale FUSE mounts from previous invocations.
fusermount -u "${HOME}/bb_clientd" || true

# Remove UNIX socket, so that builds will not attept to send gRPC traffic.
export XDG_CACHE_HOME="${XDG_CACHE_HOME:-${HOME}/.cache}"
rm -f "${XDG_CACHE_HOME}/bb_clientd/grpc"

if [ "$1" = "start" ]; then
  # Create directories that are used by bb_clientd.
  mkdir -p \
      "${XDG_CACHE_HOME}/bb_clientd/ac/persistent_state" \
      "${XDG_CACHE_HOME}/bb_clientd/cas/persistent_state" \
      "${XDG_CACHE_HOME}/bb_clientd/outputs" \
      "${HOME}/bb_clientd"

  # Discard logs of the previous invocation.
  rm -f "${XDG_CACHE_HOME}/bb_clientd/log"

  # Use either the user provided or system-wide configuration file, based
  # on whether the former is present.
  config_file=/usr/lib/bb_clientd/bb_clientd.jsonnet
  personal_config_file="${HOME}/.config/bb_clientd/bb_clientd.jsonnet"

  # TODO: 2025-04-08 Remove migration code after a year.
  old_personal_config_file="${HOME}/.config/bb_clientd.jsonnet"
  if [ -f "$old_personal_config_file" ]; then
    echo "Found ${old_personal_config_file}, moving it to ${personal_config_file}"
    mkdir -p "${HOME}/.config/bb_clientd"
    mv "$old_personal_config_file" "$personal_config_file"
  fi
  rm -f "${HOME}/.config/bb_clientd_defaults.jsonnet"

  if [ -f "${personal_config_file}" ]; then
    ln -sf "${config_file}" "${HOME}/.config/bb_clientd/bb_clientd_defaults.jsonnet"
    config_file="${personal_config_file}"
  fi

  OS=$(uname) exec /usr/bin/bb_clientd "${config_file}"
fi
