"""Hourly diagnostic files with a 24-hour retention boundary."""

import logging
from logging.handlers import TimedRotatingFileHandler
import os
import re
import time


def prune_logs(root, now=None):
    cutoff = (time.time() if now is None else now) - 86400
    for name in os.listdir(root):
        if not re.fullmatch(r"(?:proxy|routing)\.log\.\d{4}-\d{2}-\d{2}_\d{2}", name):
            continue
        path = os.path.join(root, name)
        if not os.path.islink(path) and os.path.isfile(path) and os.stat(path).st_mtime <= cutoff:
            os.unlink(path)


def handler(root, role):
    path = os.path.join(root, "proxy.log" if role == "serving" else "routing.log")
    fd = os.open(path, os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o600)
    os.close(fd)
    log = TimedRotatingFileHandler(path, when="H", interval=1, backupCount=24,
                                   utc=True, encoding="utf-8", delay=True)
    log.setFormatter(logging.Formatter("%(message)s"))
    return log


def release_bootstrap_stdio():
    """Managed daemons log through rotating owners; release inherited file inodes."""
    if os.environ.get("AZURE_PROXY_MANAGED_LOG"):
        fd = os.open(os.devnull, os.O_WRONLY)
        try:
            os.dup2(fd, 1)
            os.dup2(fd, 2)
        finally:
            os.close(fd)
