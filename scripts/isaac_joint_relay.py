"""ZMQ -> ROS 2 relay: mirrors GELLO's commanded xArm joint targets into
Isaac Sim's /isaac_joint_commands topic, and (2026-08-30-001) the real arm's
actual joint feedback into /xarm/joint_states.

Phase 5 Step 1 (tasks/active.md task 2026-08-29-003, Part B) added the first
tap; 2026-08-30-001 (Phase 6 prerequisite) added the second, alongside it,
without modifying the first (g2). Runs as a standalone process under
container_2_ros's SYSTEM Python 3.10 (where rclpy is natively installed) --
NOT under the conda env GELLO's teleop processes run in, and NOT a
colcon-built ament package (single-purpose bridge, same precedent as
dynamixel_bus_isolation_test.py). Keeps GELLO's dependency footprint (conda
env, Python 3.11) free of any rclpy import (guard g3).

Source /opt/ros/humble/setup.bash before running:
    source /opt/ros/humble/setup.bash
    python3 scripts/isaac_joint_relay.py

Subscribes to two ZMQ PUB sockets XArmRobot._set_position() publishes on
(gello/robots/xarm_robot.py):
  - ISAAC_RELAY_PORT=6002: commanded joint target, unconditional (fires even
    with no physical arm) -> /isaac_joint_commands. Unchanged from
    2026-08-29-003.
  - ARM_FEEDBACK_RELAY_PORT=6003: real arm's actual joint feedback
    (self.last_state.joints(), sourced from get_servo_angle()), gated on a
    physical arm being connected -> /xarm/joint_states.
Each received joint vector is the plain 6-element arm-only array -- matches
lite6_isaac.urdf exactly (add_gripper:=false), so no slicing or reshaping
needed. A zmq.Poller multiplexes both sockets so a quiet stream on one port
cannot delay delivery on the other (a second sequential blocking recv() per
loop iteration would risk exactly that).
"""

import pickle

import rclpy
import zmq
from rclpy.node import Node
from sensor_msgs.msg import JointState

ISAAC_RELAY_PORT = 6002
ARM_FEEDBACK_RELAY_PORT = 6003
JOINT_NAMES = ["joint1", "joint2", "joint3", "joint4", "joint5", "joint6"]


class IsaacJointRelay(Node):
    def __init__(self):
        super().__init__("isaac_joint_relay")
        self._command_publisher = self.create_publisher(
            JointState, "/isaac_joint_commands", 10
        )
        self._feedback_publisher = self.create_publisher(
            JointState, "/xarm/joint_states", 10
        )

        self._zmq_context = zmq.Context()

        self._command_socket = self._zmq_context.socket(zmq.SUB)
        self._command_socket.connect(f"tcp://127.0.0.1:{ISAAC_RELAY_PORT}")
        self._command_socket.setsockopt(zmq.SUBSCRIBE, b"")

        self._feedback_socket = self._zmq_context.socket(zmq.SUB)
        self._feedback_socket.connect(f"tcp://127.0.0.1:{ARM_FEEDBACK_RELAY_PORT}")
        self._feedback_socket.setsockopt(zmq.SUBSCRIBE, b"")

        self._poller = zmq.Poller()
        self._poller.register(self._command_socket, zmq.POLLIN)
        self._poller.register(self._feedback_socket, zmq.POLLIN)

    def _publish(self, publisher, message: bytes) -> None:
        joints = pickle.loads(message)
        msg = JointState()
        msg.name = JOINT_NAMES
        msg.position = [float(j) for j in joints]
        publisher.publish(msg)

    def spin(self):
        while rclpy.ok():
            events = dict(self._poller.poll(timeout=1000))  # ms
            if self._command_socket in events:
                self._publish(self._command_publisher, self._command_socket.recv())
            if self._feedback_socket in events:
                self._publish(self._feedback_publisher, self._feedback_socket.recv())


def main():
    rclpy.init()
    node = IsaacJointRelay()
    try:
        node.spin()
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()
