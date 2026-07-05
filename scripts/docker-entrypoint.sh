#!/bin/sh
set -e

# Fix ownership of the audit data volume on first run, but skip the
# recursive chown if it's already owned by appuser — AUDIT_DATA_ROOT can
# hold multi-GB / thousands-of-files audit archives, and re-scanning the
# whole tree on every container restart delays the healthcheck for no
# reason once ownership has already been fixed once.
if [ -d /audit_data ]; then
    current_owner=$(stat -c '%U' /audit_data 2>/dev/null || echo "")
    if [ "$current_owner" != "appuser" ]; then
        chown -R appuser:appuser /audit_data
    fi
fi

exec gosu appuser "$@"
