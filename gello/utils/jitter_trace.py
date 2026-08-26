"""Read-only wall-clock instrumentation for the GELLO->xArm pipeline.

Phase 4 task 2026-08-20-001 (teleop jitter characterization, see
docs/exec-plans/PHASE4_moveItServo.md "Current data pipeline"). Adds
five timestamped taps (T1, T1b, T2-T4) with zero control-flow impact
on the tapped code paths. T1b is a HOST-approved addition beyond the
original 4 spec'd sites: T1 (DynamixelDriver.get_joints()) timestamps
when the 100Hz poll asks for leader ticks, not when the background
reader thread (_read_joint_states) actually samples the Dynamixel
bus — those run at decoupled rates, so T1 alone can't give the real
sample-timing the step-3 filter design needs. T1b taps the bus read
directly, inside _read_joint_states, immediately after each fresh
sample is committed to self._joint_angles.

Disabled by default (no-op, near-zero overhead) so normal teleop is
unaffected. Enable for a capture session with:

    GELLO_JITTER_TRACE=1

Each OS process gets its own CSV under GELLO_JITTER_TRACE_DIR
(default: <gello_software>/logs/jitter_trace/, already gitignored via
the repo's `*logs*` pattern), named by pid so the 100Hz leader process
(run_env.py) and the 50Hz follower process (launch_nodes.py) never
write to the same file concurrently. All timestamps are epoch seconds
from time.time() on the host clock, so rows from both files can be
merged/aligned offline by wall_clock_ts.
"""

import atexit
import csv
import os
import queue
import threading
import time
from typing import Optional, Sequence

_ENABLED = os.environ.get("GELLO_JITTER_TRACE") == "1"
_DEFAULT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "logs",
    "jitter_trace",
)
_OUT_DIR = os.environ.get("GELLO_JITTER_TRACE_DIR", _DEFAULT_DIR)
_MAX_JOINTS = 7
_SENTINEL = object()


class _TraceWriter:
    def __init__(self, out_dir: str):
        os.makedirs(out_dir, exist_ok=True)
        path = os.path.join(out_dir, f"trace_pid{os.getpid()}.csv")
        self._q: "queue.Queue" = queue.Queue()
        self._fh = open(path, "w", newline="")
        self._writer = csv.writer(self._fh)
        self._writer.writerow(
            ["tap_id", "wall_clock_ts"] + [f"j{i}" for i in range(_MAX_JOINTS)]
        )
        self._fh.flush()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        atexit.register(self._flush_and_close)

    def log(self, tap_id: str, ts: float, joints: Sequence[float]) -> None:
        self._q.put_nowait((tap_id, ts, tuple(float(v) for v in joints)))

    def _run(self) -> None:
        while True:
            item = self._q.get()
            if item is _SENTINEL:
                break
            tap_id, ts, joints = item
            row = [tap_id, repr(ts)] + [repr(v) for v in joints]
            row += [""] * (_MAX_JOINTS - len(joints))
            self._writer.writerow(row)
            if self._q.empty():
                self._fh.flush()
        self._fh.flush()
        self._fh.close()

    def _flush_and_close(self) -> None:
        # Drains any still-queued taps (e.g. from a Ctrl-C stop at the end
        # of an S2 capture session) before the process exits.
        self._q.put(_SENTINEL)
        self._thread.join(timeout=2.0)


_writer: Optional[_TraceWriter] = _TraceWriter(_OUT_DIR) if _ENABLED else None


def tap(tap_id: str, joints: Sequence[float]) -> None:
    """Buffer one (tap_id, wall_clock_ts, joints) record; no-op unless enabled.

    Non-blocking: puts onto an in-memory queue drained by a background
    thread. Never does file I/O on the caller's thread.
    """
    if _writer is None:
        return
    _writer.log(tap_id, time.time(), joints)
