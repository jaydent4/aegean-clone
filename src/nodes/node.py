import logging
from http.server import ThreadingHTTPServer

from common.net import make_handler

logger = logging.getLogger(__name__)


class Node:
    def __init__(self, name, host, port):
        self.name = name
        self.host = host
        self.port = port
        self._server = None

    def start(self):
        if self._server is not None:
            logger.debug(f"Node {self.name} already running")
            return

        logger.info(f"Starting node {self.name} on {self.host}:{self.port}")
        handler = make_handler(self.handle_message)
        self._server = ThreadingHTTPServer((self.host, self.port), handler)
        self._server.serve_forever()

    def handle_message(self, payload):
        raise NotImplementedError

