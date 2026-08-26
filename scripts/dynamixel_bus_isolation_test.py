"""Isolated DynamixelDriver bus test — leader Dynamixel chain only.

Diagnostic script for tasks/active.md task 2026-08-25-001 (RS-485/FTDI
bus dropouts on the GELLO leader). Instantiates DynamixelDriver
directly against the leader's 7 servo IDs with nothing else running —
no XArmRobot, no ZMQ, no run_env.py/launch_nodes.py — to separate two
hypotheses for the dropouts seen in the full pipeline:

  (a) the RS-485/FTDI bus is flaky on its own, independent of anything
      else, or
  (b) something in the fuller pipeline (follower's 50Hz thread, ZMQ
      traffic, container scheduling) is starving the reader thread and
      causing txRxPacket() timeouts.

If dropouts still occur in this isolated script, that points at (a).
If they don't, that points at (b).

This script never calls set_joints()/set_torque_mode(True), so it
never commands the servos — safe to run "idle" (leader powered, nobody
touching it) as the first pass, which also rules out cable-flex/motion
as a cause if dropouts still occur. The leader can still be backdriven
by hand for a motion-condition second pass since torque stays disabled
throughout.

Captures two independent failure modes of the driver's background
reader thread (gello.dynamixel.driver.DynamixelDriver._read_joint_states),
without modifying driver.py (task 2026-08-25-001 guard g1 — diagnostics
only):
  - Per-attempt comm failures: intercepts stdout for the existing
    "warning, comm failed: {code}" print (driver.py:467).
  - Silent thread death: driver.py:482/497 raise an uncaught
    RuntimeError when txRxPacket() succeeds but a servo's data is
    missing from the response (partial/corrupt read) — this kills the
    daemon reader thread with no warning, and get_joints() then keeps
    returning the same frozen array forever, which looks identical to
    a healthy read unless the thread itself is watched. This script
    polls driver._reading_thread.is_alive() every loop and flags the
    exact moment it goes False.

Usage:
    python dynamixel_bus_isolation_test.py [--port PORT] [--duration SECONDS]
    python dynamixel_bus_isolation_test.py --ids 1,2,3,4,5,6   # leave-one-out (excludes gripper id 7)
    python dynamixel_bus_isolation_test.py --mode per-id       # per-servo attribution, see below

Output: CSV at gello_software/logs/bus_isolation/bus_isolation_<unix_ts>.csv
with columns: wall_clock_ts, elapsed_s, status, dxl_comm_result,
thread_alive, j1..j7 (raw ticks on success, blank otherwise). A summary
(OK/FAIL counts, thread-death event, failure codes, dropout cluster
durations) prints at the end — deliberately does not editorialize about
root cause; that's for results.md after cross-referencing both failure
modes and, if needed, the motion-condition run.

--mode per-id: GroupSyncRead's txRxPacket() result is a whole-transaction
verdict — when it fails, the SDK gives no indication of which of the 7
IDs' response was missing, since all 7 are read in one instruction/7
status-packet exchange over the shared half-duplex bus. This mode
instead reads each ID individually via PacketHandler.read4ByteTxRx(),
which returns a (value, comm_result, dxl_error) tuple per ID — direct
per-servo attribution, at the cost of a slower sweep rate (7 round trips
instead of 1 bulk transaction). Output is a separate CSV
(bus_isolation_per_id_<unix_ts>.csv, columns wall_clock_ts, elapsed_s,
dxl_id, status, dxl_comm_result, dxl_error, position) with a per-ID
OK/COMM_FAIL/HW_ERROR breakdown in the summary — if failures concentrate
on one ID (e.g. 7, the gripper servo — also the ID that hit the -3001
connect-time flake fixed by 2026-08-18-001), that ID's connector is the
lead. --ids also applies in this mode.
"""

import argparse
import csv
import re
import sys
import time
from pathlib import Path

from gello.dynamixel.driver import ADDR_PRESENT_POSITION, DynamixelDriver

# Leader's FTDI device and servo config, from
# gello/agents/gello_agent.py PORT_CONFIG_MAP (xArm Lite 6 entry).
DEFAULT_PORT = "/dev/serial/by-id/usb-FTDI_USB__-__Serial_Converter_FTB8HQU5-if00-port0"
LEADER_IDS = (1, 2, 3, 4, 5, 6, 7)  # joints 1-6 + gripper (id 7)
BAUDRATE = 57600


def _parse_ids(s):
    return tuple(int(x) for x in s.split(","))

_FAIL_RE = re.compile(r"warning, comm failed: (-?\d+)")


class _FailureScanningStdout:
    """Passes writes through to the real stdout unchanged, while also
    scanning for the driver's 'warning, comm failed' line so each
    failure gets a timestamped record without modifying driver.py."""

    def __init__(self, real_stdout, on_failure):
        self._real = real_stdout
        self._on_failure = on_failure

    def write(self, s):
        self._real.write(s)
        for line in s.splitlines():
            m = _FAIL_RE.search(line)
            if m:
                self._on_failure(int(m.group(1)))
        return len(s)

    def flush(self):
        self._real.flush()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", default=DEFAULT_PORT)
    parser.add_argument("--duration", type=float, default=120.0, help="seconds to run")
    parser.add_argument(
        "--poll-hz",
        type=float,
        default=100.0,
        help="rate to sample get_joints() at, matching run_env.py's leader poll rate "
        "(groupsync mode) or target sweep rate (per-id mode, likely unreachable)",
    )
    parser.add_argument(
        "--mode",
        choices=["groupsync", "per-id"],
        default="groupsync",
        help="groupsync (default): matches production driver.py, one bulk transaction "
        "for all ids. per-id: individual reads, one comm_result/dxl_error per servo — "
        "see module docstring.",
    )
    parser.add_argument(
        "--ids",
        type=_parse_ids,
        default=LEADER_IDS,
        help="comma-separated servo ids, e.g. 1,2,3,4,5,6 to exclude the gripper (id 7) "
        "for a leave-one-out comparison. Default: all 7 leader ids.",
    )
    args = parser.parse_args()

    out_dir = Path(__file__).resolve().parent.parent / "logs" / "bus_isolation"
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.mode == "per-id":
        out_path = out_dir / f"bus_isolation_per_id_{int(time.time())}.csv"
        run_per_id(args, out_path)
        return

    out_path = out_dir / f"bus_isolation_{int(time.time())}.csv"

    rows = []
    start = time.time()

    def on_failure(comm_result):
        rows.append(
            {
                "wall_clock_ts": time.time(),
                "elapsed_s": time.time() - start,
                "status": "FAIL",
                "dxl_comm_result": comm_result,
                "joints": None,
                "thread_alive": "",
            }
        )

    real_stdout = sys.stdout
    sys.stdout = _FailureScanningStdout(real_stdout, on_failure)

    driver = None
    try:
        print(
            f"Connecting DynamixelDriver on {args.port}, ids={args.ids} "
            "(no XArmRobot/ZMQ in this process)..."
        )
        driver = DynamixelDriver(
            args.ids, port=args.port, baudrate=BAUDRATE, use_fake_fallback=False
        )
        print(
            "Connected. Running idle read loop — this script never commands "
            "motion; move the leader by hand if testing under the motion "
            "condition."
        )
        print(f"Duration: {args.duration}s, poll rate: {args.poll_hz}Hz. Ctrl-C to stop early.")

        period = 1.0 / args.poll_hz
        thread_was_alive = True
        while time.time() - start < args.duration:
            loop_start = time.time()
            # get_joints() does no bus I/O itself — it returns whatever the
            # background reader thread (_read_joint_states) last committed.
            # That thread has an uncaught-RuntimeError path (driver.py:482,
            # 497) on a partial/corrupt group-sync response, which kills it
            # silently: get_joints() then keeps returning the same frozen
            # array forever, indistinguishable from a healthy read unless
            # we watch the thread itself.
            alive = driver._reading_thread.is_alive()
            joints = driver.get_joints()
            rows.append(
                {
                    "wall_clock_ts": time.time(),
                    "elapsed_s": time.time() - start,
                    "status": "OK",
                    "dxl_comm_result": "",
                    "joints": list(joints),
                    "thread_alive": alive,
                }
            )
            if thread_was_alive and not alive:
                print(
                    f"*** READER THREAD DIED at ~{time.time() - start:.2f}s elapsed "
                    "(uncaught exception, see stderr above) — remaining rows are "
                    "stale/frozen values, not live reads. ***"
                )
                rows.append(
                    {
                        "wall_clock_ts": time.time(),
                        "elapsed_s": time.time() - start,
                        "status": "THREAD_DEAD",
                        "dxl_comm_result": "",
                        "joints": None,
                        "thread_alive": False,
                    }
                )
            thread_was_alive = alive
            sleep_left = period - (time.time() - loop_start)
            if sleep_left > 0:
                time.sleep(sleep_left)
    except KeyboardInterrupt:
        print("Interrupted by user.")
    finally:
        if driver is not None:
            driver.close()
        sys.stdout = real_stdout

    _write_csv(out_path, rows)
    _summarize(rows)


def _write_csv(path, rows):
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["wall_clock_ts", "elapsed_s", "status", "dxl_comm_result", "thread_alive"]
            + [f"j{i}" for i in range(1, 8)]
        )
        for r in rows:
            joints = r["joints"] or []
            joints_padded = list(joints) + [""] * (7 - len(joints))
            writer.writerow(
                [
                    repr(r["wall_clock_ts"]),
                    f"{r['elapsed_s']:.3f}",
                    r["status"],
                    r["dxl_comm_result"],
                    r.get("thread_alive", ""),
                ]
                + [repr(v) if v != "" else "" for v in joints_padded]
            )
    print(f"\nWrote {len(rows)} rows to {path}")


def _summarize(rows):
    fails = [r for r in rows if r["status"] == "FAIL"]
    oks = [r for r in rows if r["status"] == "OK"]
    died = [r for r in rows if r["status"] == "THREAD_DEAD"]
    print(f"Summary: {len(oks)} OK reads, {len(fails)} FAIL (comm failed) events.")
    if died:
        print(
            f"*** Reader thread died at {died[0]['elapsed_s']:.2f}s elapsed — "
            "everything after that point is a frozen last-known value, not a "
            "live read. This is a silent-death event, distinct from a "
            "comm-failed dropout. Check stderr for the traceback. ***"
        )
    if not fails and not died:
        print(
            "No 'comm failed' prints and the reader thread stayed alive for "
            "the whole run. This only rules out txRxPacket-level comm "
            "failures at idle — it does not by itself prove the pipeline is "
            "the cause of the full-session dropouts; that needs the "
            "motion-condition run and/or the in-pipeline failure-path tap "
            "from 2026-08-25-001."
        )
        return

    if not fails:
        return

    by_code = {}
    for r in fails:
        by_code[r["dxl_comm_result"]] = by_code.get(r["dxl_comm_result"], 0) + 1
    print(f"Failure codes: {by_code}")

    # Cluster consecutive failures (< 1s apart) into dropout windows.
    clusters = []
    cluster_start = None  # (wall_clock_ts, elapsed_s)
    prev = None
    for r in fails:
        cur = (r["wall_clock_ts"], r["elapsed_s"])
        if prev is None or cur[0] - prev[0] > 1.0:
            if cluster_start is not None:
                clusters.append((cluster_start, prev))
            cluster_start = cur
        prev = cur
    clusters.append((cluster_start, prev))

    total_gap = sum(end[0] - begin[0] for begin, end in clusters)
    print(f"Dropout clusters: {len(clusters)}, total dead time within clusters: {total_gap:.2f}s")
    for begin, end in clusters:
        print(f"  cluster: starts at {begin[1]:.2f}s elapsed, duration {end[0] - begin[0]:.2f}s")


def run_per_id(args, out_path):
    """Per-servo attribution: individual read4ByteTxRx() calls instead of
    GroupSyncRead, so each read carries its own (comm_result, dxl_error)
    instead of one whole-transaction verdict. See module docstring."""
    from dynamixel_sdk.packet_handler import PacketHandler
    from dynamixel_sdk.port_handler import PortHandler
    from dynamixel_sdk.robotis_def import COMM_SUCCESS

    port_handler = PortHandler(args.port)
    packet_handler = PacketHandler(2.0)

    print(f"Per-ID mode: individual reads over ids={args.ids} on {args.port}.")
    if not port_handler.openPort():
        raise RuntimeError(f"Failed to open port {args.port}")
    if not port_handler.setBaudRate(BAUDRATE):
        raise RuntimeError(f"Failed to set baudrate {BAUDRATE}")
    # Same RS-485 direction-line / FTDI latency-timer settle as driver.py:287.
    time.sleep(0.1)

    print(
        f"Connected. Duration: {args.duration}s, target sweep rate: {args.poll_hz}Hz "
        "(actual will likely be lower — 7x the round trips of one bulk read). "
        "This mode never commands motion; move the leader by hand to test under "
        "the motion condition. Ctrl-C to stop early."
    )

    rows = []
    start = time.time()
    period = 1.0 / args.poll_hz
    try:
        while time.time() - start < args.duration:
            sweep_start = time.time()
            for dxl_id in args.ids:
                position, comm_result, dxl_error = packet_handler.read4ByteTxRx(
                    port_handler, dxl_id, ADDR_PRESENT_POSITION
                )
                if comm_result != COMM_SUCCESS:
                    status = "COMM_FAIL"
                elif dxl_error != 0:
                    status = "HW_ERROR"
                else:
                    status = "OK"
                rows.append(
                    {
                        "wall_clock_ts": time.time(),
                        "elapsed_s": time.time() - start,
                        "dxl_id": dxl_id,
                        "status": status,
                        "dxl_comm_result": "" if status == "OK" else comm_result,
                        "dxl_error": "" if comm_result != COMM_SUCCESS else dxl_error,
                        "position": position if status == "OK" else "",
                    }
                )
            sleep_left = period - (time.time() - sweep_start)
            if sleep_left > 0:
                time.sleep(sleep_left)
    except KeyboardInterrupt:
        print("Interrupted by user.")
    finally:
        port_handler.closePort()

    _write_per_id_csv(out_path, rows)
    _summarize_per_id(rows, args.ids)


def _write_per_id_csv(path, rows):
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["wall_clock_ts", "elapsed_s", "dxl_id", "status", "dxl_comm_result", "dxl_error", "position"]
        )
        for r in rows:
            writer.writerow(
                [
                    repr(r["wall_clock_ts"]),
                    f"{r['elapsed_s']:.3f}",
                    r["dxl_id"],
                    r["status"],
                    r["dxl_comm_result"],
                    r["dxl_error"],
                    repr(r["position"]) if r["position"] != "" else "",
                ]
            )
    print(f"\nWrote {len(rows)} rows to {path}")


def _summarize_per_id(rows, ids):
    print("Per-ID summary (this is what attributes a bad connector to a specific servo):")
    for dxl_id in ids:
        id_rows = [r for r in rows if r["dxl_id"] == dxl_id]
        ok = sum(1 for r in id_rows if r["status"] == "OK")
        comm_fail = sum(1 for r in id_rows if r["status"] == "COMM_FAIL")
        hw_error = sum(1 for r in id_rows if r["status"] == "HW_ERROR")
        total = len(id_rows)
        rate = 100.0 * (comm_fail + hw_error) / total if total else 0.0
        print(
            f"  id {dxl_id}: {ok} OK, {comm_fail} COMM_FAIL, {hw_error} HW_ERROR "
            f"out of {total} ({rate:.1f}% failed)"
        )
    print(
        "If one id's failure rate is far above the others, that servo's "
        "connector/cable is the lead. If they're all similar, the shared "
        "bus/FTDI adapter itself is more likely than any single connector."
    )


if __name__ == "__main__":
    main()
