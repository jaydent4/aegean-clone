from .node import Node
from common.net import send_message
import time, threading, logging
from collections import defaultdict

logger = logging.getLogger(__name__)

class Client(Node):
    def __init__(self, name, host, port, next):
        super().__init__(name, host, port)
        self.next = next
        self.completed_requests = set()

    def start(self):
        client_thread = threading.Thread(target=self.client_workflow, args=(), daemon=True)
        client_thread.start()
        super().start()

    def client_workflow(self):
        # Wait for other nodes to be turned on. TODO: improvable
        time.sleep(2)

        for request_id in range(1, 11):
            timestamp = time.time()

            request = {
                'request_id': request_id,
                'timestamp': timestamp,
                'sender': self.name,
                'op': 'spin_write_read',
                'op_payload': {
                    'spin_time': 0.1,
                    'write_key': '1',
                    'write_value': f'value_{request_id}',
                    'read_key': '1',
                },
            }

            logger.info(f"Client {self.name} sending request {request_id} to {self.next}")

            for next_node in self.next:
                try:
                    response = send_message(next_node, '8000', request)
                    logger.debug(f"Ack from shim {next_node}: {response}")
                except Exception as e:
                    logger.error(f"Failed to send to {next_node}: {e}")

    def handle_message(self, payload):
        logger.debug(f"Handler called on {self.name} with payload: {payload}")

        request_id = payload.get('request_id')
        response = payload.get('response', {})

        # with self.lock:
        if request_id in self.completed_requests:
            logger.debug(f"Client {self.name}: Ignoring duplicate response for {request_id}")
            return {'status': 'already_completed'}

        logger.info(f"Client {self.name}: Received response for request {request_id}: {response}")

        # In CFT mode with single exec pipeline, one response is sufficient
        # TODO: In full BFT mode, would wait for f+1 matching responses
        self.completed_requests.add(request_id)

        logger.info(f"Client {self.name}: Request {request_id} completed with: {response}")

        return {'status': 'response_received', 'request_id': request_id}