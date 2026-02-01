from node import Node
from net import send_message
from quorum import QuorumHelper
import logging

logger = logging.getLogger(__name__)

class Shim(Node):
    def __init__(self, name, host, port, next):
        super().__init__(name, host, port)
        self.next = next
        self.quorum_helper = QuorumHelper(2)  # TODO: Replace hard-coded value with formula

    def start(self):
        super().start()

    def handle_message(self, payload):
        logger.debug(f"Handler called on {self.name} with payload: {payload}")

        request_id = payload.get('request_id')
        sender = payload.get('sender')

        if self.quorum_helper.add(request_id, sender):
            send_message(self.next, '8000', payload)
            return {'status': 'forwarded_to_mid_execs'}
        return {'status': 'waiting_for_quorum'}