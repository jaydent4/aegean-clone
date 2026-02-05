import logging
import os
import subprocess
import time
from datetime import datetime

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def collect_logs(node_names, client_names, results_dir="experiment/results"):
    logger.info("Collecting logs (%d nodes, %d clients)", len(node_names), len(client_names))
    def _scp(node_name, remote_path, local_path):
        return subprocess.run(["scp", f"{node_name}:{remote_path}", local_path])

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = os.path.join(results_dir, timestamp)

    logs_dir = os.path.join(run_dir, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    for name in node_names:
        local_path = os.path.join(logs_dir, f"{name}.log")
        _scp(name, "/tmp/node.log", local_path)

    traces_dir = os.path.join(run_dir, "traces")
    os.makedirs(traces_dir, exist_ok=True)
    for name in client_names:
        local_path = os.path.join(traces_dir, f"{name}.jsonl")
        _scp(name, "/tmp/client_result.jsonl", local_path)
    logger.info("Log collection complete: %s", run_dir)

def launch_nodes(node_names):
    logger.info("Launching %d nodes", len(node_names))
    for name in node_names:
        subprocess.run(["ssh", name, "sh", "-lc",
                        "'cd /app/src && "
                        f"nohup go run . --name {name} --host 0.0.0.0 --port 8000 "
                        "> /tmp/node.log 2>&1 &'"])

def stop_docker_nodes(node_names):
    logger.info("Stopping %d nodes", len(node_names))
    for name in node_names:
        subprocess.run(["ssh", name, "pkill -9 -f 'go'"])


def main():
    node_names = [f"node{i}" for i in range(1, 16)]
    client_names = [f"node{i}" for i in range(1, 4)]
    logger.info("Experiment starting")
    stop_docker_nodes(node_names)

    launch_nodes(node_names)
    logger.info("Waiting for nodes to run")
    time.sleep(5.0)

    stop_docker_nodes(node_names)
    collect_logs(node_names, client_names)
    logger.info("Experiment complete")


if __name__ == "__main__":
    main()
