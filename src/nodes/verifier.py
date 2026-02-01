from .node import Node
from common.net import send_message
from common.quorum import QuorumHelper
import time, threading, logging

logger = logging.getLogger(__name__)

class Verifier(Node):
    def __init__(self, name, host, port, next):
        super().__init__(name, host, port)
        self.next = next

    def start(self):
        super().start()

    def handle_message(self, payload):
        logger.debug(f"Handler called on {self.name} with payload: {payload}")
        