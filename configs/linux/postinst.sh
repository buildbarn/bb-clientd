#!/bin/sh -eu

# bb_clientd's FUSE mount need to be created with the 'allow_other'
# option set, as containers are unable to access the mount without it.
if ! grep -q '^user_allow_other$' /etc/fuse.conf; then
  echo user_allow_other >> /etc/fuse.conf
fi

# Pick up changes to /etc/logind.conf.
systemctl restart systemd-logind.service

# Pick up changes to the systemd service.
systemctl daemon-reload

# Multiple users may have enabled bb_clientd as a systemd user service.
# Force a restart of all of these instances to ensure they do not run an
# outdated copy of the executable.
pkill bb_clientd || true
