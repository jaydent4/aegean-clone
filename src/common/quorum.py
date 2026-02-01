import logging
from collections import defaultdict

logger = logging.getLogger(__name__)

class QuorumHelper:
    def __init__(self, quorum_size):
        self.quorum_size = quorum_size
        # request_id -> set of senders
        self.pending = defaultdict(set)
        # request_ids that have already reached quorum
        self.completed = set()

    def add(self, request_id, sender):
        """Add a request. Returns True if quorum just reached, False otherwise."""
        if request_id in self.completed:
            logger.debug(f"Ignoring duplicate request {request_id} from {sender}")
            return False

        self.pending[request_id].add(sender)

        if len(self.pending[request_id]) >= self.quorum_size:
            self.completed.add(request_id)
            del self.pending[request_id]
            logger.debug(f"Quorum reached for request {request_id}")
            return True

        logger.debug(f"Request {request_id}: {len(self.pending[request_id])}/{self.quorum_size}")
        return False