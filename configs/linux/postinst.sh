#!/bin/sh -eu

# bb_clientd's FUSE mount need to be created with the 'allow_other'
# option set, as containers are unable to access the mount without it.
if ! grep -q '^user_allow_other$' /etc/fuse.conf; then
  echo user_allow_other >> /etc/fuse.conf
fi

# Pick up changes to /etc/logind.conf.
if [ -z "$(loginctl --no-legend list-sessions)" ]; then
  deb-systemd-invoke --system restart systemd-logind.service
else
  # Restarting systemd-logind.service effectively kills user sessions and the
  # users' desktop environment, so let the user get a hint about reboot
  # instead.
  if [ "$#" -ge 2 ] && [ "$1" = "configure" ]; then
    old_version=$2
  else
    old_version=""
  fi
  # In the future, the old version can determine if reboot is required:
  # if dpkg --compare-versions "$old_version" lt "20250312T094712Z-2b641c6"; then
  if [ -z "$old_version" ]; then
    if [ -x /usr/share/update-notifier/notify-reboot-required ]; then
      /usr/share/update-notifier/notify-reboot-required
    fi
    if [ -e /var/run/reboot-required ]; then
      cat /var/run/reboot-required
    fi
    echo "Please reboot to apply changes to /etc/logind.conf"
  fi
fi

# Pick up changes to the systemd service.
deb-systemd-invoke --system daemon-reload

# Multiple users may have enabled bb_clientd as a systemd user service.
# Force a restart of all of these instances to ensure they do not run an
# outdated copy of the executable.
pkill bb_clientd || true
