from .node import Node
from common.net import send_message
import time, logging, hashlib, json, copy

logger = logging.getLogger(__name__)

class Exec(Node):
    def __init__(self, name, host, port, verifiers, shim):
        super().__init__(name, host, port)
        self.verifiers = verifiers
        self.shim = shim
        self.kv_store = {'1': '111'}

        # State management for rollback
        self.stable_state = copy.deepcopy(self.kv_store)
        self.stable_seq_num = 0
        self.prev_hash = '0' * 64

        # Pending responses (held until commit)
        self.pending_responses = {}

        # Sequential execution flag (set after rollback)
        self.force_sequential = False

    def start(self):
        super().start()

    # TODO: Merkle tree
    # Compute Merkle-tree-style hash of state and outputs
    def _compute_state_hash(self, state, outputs, prev_hash, seq_num):
        data = {
            'seq_num': seq_num,
            'prev_hash': prev_hash,
            'state': state,
            'outputs': outputs
        }
        return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()

    # Execute a single request and return the response
    def _execute_request(self, request, nd_seed, nd_timestamp):
        request_id = request.get('request_id')
        sender = request.get('sender')
        op = request.get('op')
        op_payload = request.get('op_payload', {})

        response = {'request_id': request_id, 'sender': sender}

        if op == 'read':
            key = op_payload.get('key')
            value = self.kv_store.get(key)
            response['value'] = value
            response['status'] = 'ok' if value is not None else 'not_found'

        elif op == 'write':
            key = op_payload.get('key')
            value = op_payload.get('value')
            self.kv_store[key] = value
            response['status'] = 'ok'

        elif op == 'read_write':
            read_key = op_payload.get('read_key')
            write_key = op_payload.get('write_key')
            write_value = op_payload.get('write_value')
            response['read_value'] = self.kv_store.get(read_key)
            self.kv_store[write_key] = write_value
            response['status'] = 'ok'

        else:
            response['status'] = 'error'
            response['error'] = f'Unknown op: {op}'

        return response
        
    def handle_message(self, payload):
        logger.debug(f"Handler called on {self.name} with payload: {payload}")

        msg_type = payload.get('type', 'batch')

        if msg_type == 'verify_response':
            return self._handle_verify_response(payload)
        elif msg_type == 'batch':
            return self._handle_batch(payload)
        else:
            raise ValueError(f'Unknown message type: {msg_type}')

    def _handle_batch(self, payload):
        seq_num = payload.get('seq_num', 0)
        parallel_batches = payload.get('parallel_batches', [])
        nd_seed = payload.get('nd_seed', 0)
        nd_timestamp = payload.get('nd_timestamp', time.time())

        logger.info(f"{self.name}: Executing batch {seq_num} with {len(parallel_batches)} parallelBatches)")

        # Execute all parallelBatches and collect outputs
        outputs = []
        for pb_idx, parallel_batch in enumerate(parallel_batches):
            pb_outputs = []
            # TODO: In prototype, execute sequentially within parallelBatch
            # (Real impl would use threading for parallel execution)
            for request in parallel_batch:
                output = self._execute_request(request, nd_seed, nd_timestamp)
                pb_outputs.append(output)
            outputs.extend(pb_outputs)

        # Compute token (hash of state + outputs)
        logger.debug(f'kv_store: {self.kv_store}')
        logger.debug(f'outputs: {outputs}')
        logger.debug(f'prev_hash: {self.prev_hash}')
        logger.debug(f'seq_num: {seq_num}')
        token = self._compute_state_hash(
            self.kv_store, outputs, self.prev_hash, seq_num
        )

        self.pending_responses[seq_num] = {
            'outputs': outputs,
            'state': copy.deepcopy(self.kv_store),
            'token': token,
        }

        # Send VERIFY message to all verifiers
        verify_msg = {
            'type': 'verify',
            'seq_num': seq_num,
            'token': token,
            'prev_hash': self.prev_hash,
            'exec_id': self.name
        }

        for verifier in self.verifiers:
            try:
                send_message(verifier, '8000', verify_msg)
            except Exception as e:
                logger.error(f"Failed to send to verifier {verifier}: {e}")

        return {'status': 'executed', 'seq_num': seq_num, 'token': token}

    # Handle verification response from verifier
    def _handle_verify_response(self, payload):
        decision = payload.get('decision')
        seq_num = payload.get('seq_num')
        agreed_token = payload.get('token')
        # view_changed used for view change protocol (not fully implemented)
        _ = payload.get('view_changed', False)

        pending = self.pending_responses.get(seq_num)
        if not pending:
            return {'status': 'no_pending_for_seq'}

        if decision == 'commit':
            if pending['token'] == agreed_token:
                # Mark state as stable and release responses
                logger.info(f"{self.name}: Committing seq_num {seq_num}")
                self.stable_state = copy.deepcopy(pending['state'])
                self.stable_seq_num = seq_num
                self.force_sequential = False

                # Send responses back to the server-shim for broadcasting to clients
                for output in pending['outputs']:
                    request_id = output.get('request_id')
                    response_msg = {
                        'type': 'response',
                        'request_id': request_id,
                        'response': output
                    }
                    send_message(self.shim, '8000', response_msg)
                    logger.debug(f"{self.name}: Sent response for request {request_id} to shim {self.shim}")
                
                self.prev_hash = agreed_token
            else:
                # Our state diverged - need state transfer
                logger.warning(f"{self.name}: State diverged, need state transfer")
                # TODO: Request state transfer from other replicas

        elif decision == 'rollback':
            logger.info(f"{self.name}: Rolling back to seq_num {self.stable_seq_num}")
            self.kv_store = copy.deepcopy(self.stable_state)
            self.force_sequential = True
            logger.info(f"{self.name}: Will execute next batch sequentially")

        # Cleanup
        if seq_num in self.pending_responses:
            del self.pending_responses[seq_num]

        return {'status': 'processed', 'decision': decision}
