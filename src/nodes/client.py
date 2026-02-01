from .node import Node
from common.net import send_message
import time, threading, logging

logger = logging.getLogger(__name__)

class Client(Node):
    def __init__(self, name, host, port, next):
        super().__init__(name, host, port)
        self.next = next

    def start(self):
        client_thread = threading.Thread(target=self.client_workflow, args=(), daemon=True)
        client_thread.start()
        super().start()

    def client_workflow(self):
        time.sleep(2)

        for request_id in range(1, 2):
            timestamp = time.time()

            request = {
                'request_id': request_id,
                'timestamp': timestamp,
                'sender': self.name,
                'op': 'read',
                'op_payload': {'key': '1'},
            }

            logger.info(f"Client {self.name} sending request {request_id} to {self.next}")

            responses = []
            for next_node in self.next:
                try:
                    response = send_message(next_node, '8000', request)
                    responses.append({'node': next_node, 'response': response})
                    logger.debug(f"Response from {next_node}: {response}")
                except Exception as e:
                    logger.error(f"Failed to send to {next_node}: {e}")
                    responses.append({'node': next_node, 'error': str(e)})

    def handle_message(self, payload):
        logger.debug(f"Handler called on {self.name} with payload: {payload}")
        # TODO: Analyze results