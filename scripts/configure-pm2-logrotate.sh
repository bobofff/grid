#!/usr/bin/env sh
# Configure PM2's official log-rotation module for this machine.  The module
# applies to every PM2 process, including grid-bot and grid-dashboard.
set -eu

if ! pm2 describe pm2-logrotate >/dev/null 2>&1; then
  pm2 install pm2-logrotate
fi

# Rotate at local midnight even when a file is quiet, and also cap noisy failure
# loops before they can consume the disk.  Rotated names receive the date suffix.
pm2 set pm2-logrotate:rotateInterval '0 0 * * *'
pm2 set pm2-logrotate:TZ Asia/Shanghai
pm2 set pm2-logrotate:dateFormat YYYY-MM-DD
pm2 set pm2-logrotate:max_size 20M
pm2 set pm2-logrotate:retain 14
pm2 set pm2-logrotate:compress true
pm2 set pm2-logrotate:rotateModule true

echo 'PM2 logs rotate daily (and at 20M), retain 14 compressed archives.'
