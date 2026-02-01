from .node import Node
from common.net import send_message
from common.quorum import QuorumHelper
import time, threading, logging

logger = logging.getLogger(__name__)

class Exec(Node):
    def __init__(self, name, host, port, verifiers):
        super().__init__(name, host, port)
        self.verifiers = verifiers
        self.kv_store = {'1': '111'}

    def start(self):
        super().start()

    def handle_message(self, payload):
        logger.debug(f"Handler called on {self.name} with payload: {payload}")

        # TODO: handle rollback message, handle state transfer message

        # Application logic
        for request in payload:
            request_id = request.get('request_id')
            sender = request.get('sender')
            op = request.get('op')
            op_payload = request.get('op_payload')

            if op == 'read':
                value = self.kv_store[op_payload['key']]
                # TODO: Send response message to server-shim/client?
            else:
                raise ValueError(f"Unknown op: {op}")
            
        # Send hash to verifiers
        # TODO: Use pymerkle to compute hash
        hash = 'Placeholder hash'
        for verifier in self.verifiers:
            # TODO: This message is more complicated than this
            send_message(verifier, '8000', hash)

    
        