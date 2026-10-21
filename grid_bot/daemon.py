"""Start the grid runner in its own session so it outlives the shell that launched it.

A job started from a shell shares that shell's process group.  When the terminal -
or an agent runner, or CI - tears that group down, the runner is killed outright:
no traceback, no journal entry, and a live grid left behind with nobody to replace
a fill.  That is exactly how the run on 2026-10-15 died, mid-grid, with `state.json`
still reading RUNNING.  `os.setsid()` moves the bot into a new session so only an
explicit signal stops it.

    python -m grid_bot.daemon            # start detached and print the PID
    python -m grid_bot.daemon --status   # report whether it is running
    python -m grid_bot.daemon --stop     # SIGTERM it, preserving exchange state
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import signal
import sys
import time

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_LOG = PROJECT_ROOT / "runtime" / "runner_stdout.log"
DEFAULT_PIDFILE = PROJECT_ROOT / "runtime" / "runner.pid"

# Give the process enough time to journal a recoverable shutdown before declaring it stuck.
STOP_TIMEOUT_SECONDS = 90.0


def is_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def running_pid(pidfile: Path) -> int | None:
    """Return the PID from `pidfile` if that process is still alive."""
    try:
        pid = int(pidfile.read_text().strip())
    except (OSError, ValueError):
        return None
    return pid if is_running(pid) else None


def spawn(log_path: Path, pidfile: Path) -> int:
    """Double-fork into a new session, then exec the runner. Returns the new PID."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    pidfile.parent.mkdir(parents=True, exist_ok=True)
    read_fd, write_fd = os.pipe()

    if os.fork() > 0:
        # The launching shell gets its prompt back as soon as the bot reports in.
        os.close(write_fd)
        with os.fdopen(read_fd) as report:
            return int(report.read().strip() or 0)

    os.close(read_fd)
    os.setsid()  # a new session, so the launcher's process group can no longer reach us
    if os.fork() > 0:
        os._exit(0)  # the session leader exits, so the bot never acquires a terminal

    os.write(write_fd, str(os.getpid()).encode())
    os.close(write_fd)

    handle = os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    os.dup2(os.open(os.devnull, os.O_RDONLY), 0)
    os.dup2(handle, 1)
    os.dup2(handle, 2)
    os.chdir(PROJECT_ROOT)  # `runtime/...` paths in .env are relative to the project
    pidfile.write_text(str(os.getpid()))
    os.execv(sys.executable, [sys.executable, "-m", "grid_bot.main"])
    raise AssertionError("os.execv returned")  # unreachable


def stop(pidfile: Path, timeout: float = STOP_TIMEOUT_SECONDS) -> int:
    pid = running_pid(pidfile)
    if pid is None:
        print("not running")
        return 1
    # SIGTERM, not SIGKILL: the runner journals a recoverable pause before it goes.
    os.kill(pid, signal.SIGTERM)
    deadline = time.time() + timeout
    while time.time() < deadline and is_running(pid):
        time.sleep(0.2)
    if is_running(pid):
        print(f"pid {pid} is still running after {timeout:.0f}s; it did not stop cleanly")
        return 1
    pidfile.unlink(missing_ok=True)
    print(f"stopped pid {pid}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--log", type=Path, default=DEFAULT_LOG,
                        help="where the detached runner writes stdout and stderr")
    parser.add_argument("--pidfile", type=Path, default=DEFAULT_PIDFILE)
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--status", action="store_true", help="report whether the bot is running")
    action.add_argument("--stop", action="store_true", help="ask the bot to pause and exit")
    args = parser.parse_args(argv)

    if args.status:
        pid = running_pid(args.pidfile)
        print(f"running (pid {pid})" if pid else "not running")
        return 0 if pid else 1

    if args.stop:
        return stop(args.pidfile)

    pid = running_pid(args.pidfile)
    if pid is not None:
        print(f"already running (pid {pid}); use --stop first")
        return 1

    new_pid = spawn(args.log, args.pidfile)
    print(f"started pid {new_pid}")
    print(f"log     {args.log}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
