import logging
import os
import subprocess
import time
from datetime import datetime

from net import send_message
from node import Node

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def _ssh(node_name, command):
    return subprocess.run(["ssh", node_name, command])


def _scp(node_name, remote_path, local_path):
    return subprocess.run(["scp", f"{node_name}:{remote_path}", local_path])


def collect_logs(node_names, log_dir="experiment"):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = os.path.join(log_dir, timestamp)
    os.makedirs(run_dir, exist_ok=True)
    for name in node_names:
        local_path = os.path.join(run_dir, f"{name}.log")
        _scp(name, "/tmp/node.log", local_path)
        
def launch_nodes(node_names):
    for name in node_names:
        _ssh(name, "nohup python -u /app/src/start.py "
            f"--name {name} --host 0.0.0.0 --port 8000 "
            "> /tmp/node.log 2>&1 &")

def stop_docker_nodes(node_names):
    for name in node_names:
        _ssh(name, "pkill -9 -f 'python'")


def main():
    node_names = [f"node{i}" for i in range(1, 13)]
    stop_docker_nodes(node_names)

    launch_nodes(node_names)
    time.sleep(5.0)

    stop_docker_nodes(node_names)
    collect_logs(node_names)


if __name__ == "__main__":
    main()
