"""ZMQ -> ROS 2 relay: mirrors GELLO's commanded xArm joint targets into
Isaac Sim's /isaac_joint_commands topic.

Phase 5 Step 1 (tasks/active.md task 2026-08-29-003, Part B). Runs as a
standalone process under container_2_ros's SYSTEM Python 3.10 (where rclpy
is natively installed) -- NOT under the conda env GELLO's teleop processes
run in, and NOT a colcon-built ament package (single-purpose bridge, same
precedent as dynamixel_bus_isolation_test.py). Keeps GELLO's dependency
footprint (conda env, Python 3.11) free of any rclpy import (guard g3).

Source /opt/ros/humble/setup.bash before running:
    source /opt/ros/humble/setup.bash
    python3 scripts/isaac_joint_relay.py

Subscribes to the ZMQ PUB socket XArmRobot._set_position() publishes on
(gello/robots/xarm_robot.py, ISAAC_RELAY_PORT=6002; a dedicated port,
separate from the existing ZMQ REQ/REP robot-control channel on 6001, so
the two sockets never collide inside the follower process). Each received
joint vector is the plain 6-element arm-only array XArmRobot already uses
for real hardware commands -- matches lite6_isaac.urdf exactly
(add_gripper:=false), so no slicing or reshaping needed.
"""

import pickle

import rclpy
import zmq
from rclpy.node import Node
from sensor_msgs.msg import JointState

ISAAC_RELAY_PORT = 6002
JOINT_NAMES = ["joint1", "joint2", "joint3", "joint4", "joint5", "joint6"]


class IsaacJointRelay(Node):
    def __init__(self):
        super().__init__("isaac_joint_relay")
        self._publisher = self.create_publisher(
            JointState, "/isaac_joint_commands", 10
        )
        self._zmq_context = zmq.Context()
        self._zmq_socket = self._zmq_context.socket(zmq.SUB)
        self._zmq_socket.connect(f"tcp://127.0.0.1:{ISAAC_RELAY_PORT}")
        self._zmq_socket.setsockopt(zmq.SUBSCRIBE, b"")
        self._zmq_socket.setsockopt(zmq.RCVTIMEO, 1000)  # ms

    def spin(self):
        while rclpy.ok():
            try:
                message = self._zmq_socket.recv()
            except zmq.Again:
                continue
            joints = pickle.loads(message)

            msg = JointState()
            msg.name = JOINT_NAMES
            msg.position = [float(j) for j in joints]
            self._publisher.publish(msg)


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
