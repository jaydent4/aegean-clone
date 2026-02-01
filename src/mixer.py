from node import Node
from net import send_message
from quorum import QuorumHelper
import time, threading, logging

logger = logging.getLogger(__name__)

class Mixer(Node):
    def __init__(self, name, host, port, next):
        super().__init__(name, host, port)
        self.next = next

    def start(self):
        super().start()

    def handle_message(self, payload):
        logger.debug(f"Handler called on {self.name} with payload: {payload}")

        # TODO: this mixer is a placeholder
        send_message(self.next, '8000', payload)
        return {'status': 'ok'}

        