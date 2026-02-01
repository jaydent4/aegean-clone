from .node import Node
from common.net import send_message
import time, threading, logging
from collections import defaultdict

logger = logging.getLogger(__name__)

class Verifier(Node):
    def __init__(self, name, host, port, execs):
        super().__init__(name, host, port)
        self.execs = execs if isinstance(execs, (list, tuple)) else [execs]

        # TODO: replace hard-coded values with formulas
        # Fault tolerance parameters (simplified: u=1, r=0 for CFT)
        self.u = 1  # max failures for liveness
        self.r = 0  # max commission failures for safety

        # Quorum sizes
        self.exec_quorum = max(self.u, self.r) + 1  # tokens needed to preprepare
        self.verify_quorum = 2 * self.u + self.r + 1  # for commit (in full impl)

        # State tracking per sequence number
        # seq_num -> { token -> set(exec_ids) }
        self.tokens = defaultdict(lambda: defaultdict(set))
        # seq_num -> committed token (or None)
        self.committed = {}
        # seq_num -> prev_hash from tokens
        self.prev_hashes = {}

        self.lock = threading.Lock()

    def start(self):
        super().start()

    # Check if we have enough matching tokens to reach agreement
    def _check_agreement(self, seq_num):
        token_counts = self.tokens[seq_num]

        # Find token with most support
        best_token = None
        best_count = 0
        for token, exec_ids in token_counts.items():
            if len(exec_ids) > best_count:
                best_count = len(exec_ids)
                best_token = token

        total_responses = sum(len(ids) for ids in token_counts.values())

        # If we have quorum of matching tokens -> commit
        if best_count >= self.exec_quorum:
            return 'commit', best_token

        # If we've heard from all execs and no quorum -> rollback
        if total_responses >= len(self.execs):
            logger.warning(f"Verifier {self.name}: Divergence detected for seq {seq_num}")
            return 'rollback', best_token

        return None, None

    def _send_verify_response(self, seq_num, decision, token):
        """Send commit/rollback decision to all execution replicas."""
        response = {
            'type': 'verify_response',
            'seq_num': seq_num,
            'decision': decision,
            'token': token,
            'view_changed': decision == 'rollback'
        }

        for exec_node in self.execs:
            try:
                send_message(exec_node, '8000', response)
            except Exception as e:
                logger.error(f"Failed to send to exec {exec_node}: {e}")

    def handle_message(self, payload):
        logger.debug(f"Handler called on {self.name} with payload: {payload}")

        seq_num = payload.get('seq_num')
        token = payload.get('token')
        prev_hash = payload.get('prev_hash')
        exec_id = payload.get('exec_id')

        # Validate prev_hash matches what we expect (if we have committed seq_num-1)
        if seq_num > 1:
            prev_committed = self.committed.get(seq_num - 1)
            if prev_committed and prev_hash != prev_committed:
                logger.warning(f"Verifier {self.name}: Invalid prev_hash from {exec_id}")
                return {'status': 'invalid_prev_hash'}

        with self.lock:
            # Already committed this seq_num?
            if seq_num in self.committed:
                return {'status': 'already_committed', 'token': self.committed[seq_num]}

            # Record this token
            self.tokens[seq_num][token].add(exec_id)
            self.prev_hashes[seq_num] = prev_hash

            logger.info(f"Verifier {self.name}: seq={seq_num}, token={token[:16]}... "
                       f"from {exec_id}, count={len(self.tokens[seq_num][token])}")

            # Check if we can reach agreement
            decision, agreed_token = self._check_agreement(seq_num)

            if decision == 'commit':
                self.committed[seq_num] = agreed_token
                logger.info(f"Verifier {self.name}: COMMIT seq={seq_num}")
                self._send_verify_response(seq_num, 'commit', agreed_token)
                # Cleanup
                del self.tokens[seq_num]
                return {'status': 'committed', 'token': agreed_token}

            elif decision == 'rollback':
                logger.info(f"Verifier {self.name}: ROLLBACK seq={seq_num}")
                self._send_verify_response(seq_num, 'rollback', agreed_token)
                # Cleanup
                del self.tokens[seq_num]
                return {'status': 'rollback'}

        return {'status': 'waiting', 'count': len(self.tokens[seq_num][token])}

        