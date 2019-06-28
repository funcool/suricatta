#!/usr/bin/env zsh
set -ex
sudo pg_ctlcluster 9.6 main start

exec "$@"
