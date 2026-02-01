from .node import Node
from common.net import send_message
from common.quorum import QuorumHelper
import logging

logger = logging.getLogger(__name__)

class Shim(Node):
    def __init__(self, name, host, port, next, clients):
        super().__init__(name, host, port)
        self.next = next
        self.clients = clients
        self.quorum_helper = QuorumHelper(2)  # TODO: Replace hard-coded value with formula

    def start(self):
        super().start()

    def handle_message(self, payload):
        logger.debug(f"Handler called on {self.name} with payload: {payload}")

        msg_type = payload.get('type', 'request')
        if msg_type == 'response':
            # Handle response from exec - broadcast to all clients that sent the request
            request_id = payload.get('request_id')
            response_data = payload.get('response')

            logger.info(f"{self.name}: Broadcasting response for request {request_id} "
                    f"to {len(self.clients)} clients: {self.clients}")

            for client in self.clients:
                try:
                    client_response = {
                        'type': 'response',
                        'request_id': request_id,
                        'response': response_data
                    }
                    send_message(client, '8000', client_response)
                    logger.debug(f"{self.name}: Sent response to client {client}")
                except Exception as e:
                    logger.error(f"{self.name}: Failed to send response to {client}: {e}")

            return {'status': 'response_broadcast', 'recipients': list(self.clients)}
        else:
            # Handle incoming client request - wait for quorum then forward
            request_id = payload.get('request_id')
            sender = payload.get('sender')

            if self.quorum_helper.add(request_id, sender):
                send_message(self.next, '8000', payload)
                return {'status': 'forwarded_to_mid_execs'}
            return {'status': 'waiting_for_quorum'}