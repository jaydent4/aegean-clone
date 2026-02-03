from .node import Node
from common.net import send_message
import time, logging, hashlib, json, copy

logger = logging.getLogger(__name__)

class Exec(Node):
    # TODO: request pipelining, parallel pipelining
    def __init__(self, name, host, port, verifiers, shim, peers):
        super().__init__(name, host, port)
        self.verifiers = verifiers
        self.shim = shim
        self.peers = peers
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
        op = request.get('op')
        op_payload = request.get('op_payload', {})

        response = {'request_id': request_id}

        if op == 'spin_write_read':
            spin_time = op_payload.get('spin_time', 0)
            write_key = op_payload.get('write_key')
            write_value = op_payload.get('write_value')
            read_key = op_payload.get('read_key')

            # Spin for the given time
            if spin_time > 0:
                time.sleep(spin_time)

            # Write to key
            self.kv_store[write_key] = write_value

            # Read from key
            read_value = self.kv_store.get(read_key)
            response['read_value'] = read_value
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
        elif msg_type == 'state_transfer_request':
            return self._handle_state_transfer_request(payload)
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
        for parallel_batch in parallel_batches:
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
                # TODO: rollback? (I guess we need to introduce parallel pipelining first)
                # Our state diverged - need state transfer from a replica with correct state
                logger.warning(f"{self.name}: State diverged at seq_num {seq_num}, "
                              f"requesting state transfer")

                # Request state transfer from a peer replica
                success = self._request_state_transfer()

                if success:
                    logger.info(f"{self.name}: State transfer successful for seq_num {seq_num}")
                else:
                    # If state transfer fails, fall back to rollback
                    logger.error(f"{self.name}: State transfer failed, rolling back")
                    self.kv_store = copy.deepcopy(self.stable_state)
                    self.force_sequential = True

        elif decision == 'rollback':
            logger.info(f"{self.name}: Rolling back to seq_num {self.stable_seq_num}")
            self.kv_store = copy.deepcopy(self.stable_state)
            self.force_sequential = True
            logger.info(f"{self.name}: Will execute next batch sequentially")

        # Cleanup
        if seq_num in self.pending_responses:
            del self.pending_responses[seq_num]

        return {'status': 'processed', 'decision': decision}

    # TODO: should state transfer be async? Meaning that should state transfer request
    # spin and wait for a response before processing other requests
    # TODO: after state transfer, do we send back client the response?
    # Request state transfer from a replica that has the correct state
    def _request_state_transfer(self):
        for source_exec in self.peers:
            try:
                logger.info(f"{self.name}: Requesting state transfer from {source_exec}")

                request_msg = {
                    'type': 'state_transfer_request',
                    'requesting_exec': self.name
                }

                response = send_message(source_exec, '8000', request_msg)

                if response and response.get('status') == 'ok':
                    transferred_state = response.get('state')
                    transferred_stable_seq_num = response.get('stable_seq_num')
                    transferred_prev_hash = response.get('prev_hash')

                    # Only apply if the provided stable seq num is higher than ours
                    if transferred_stable_seq_num <= self.stable_seq_num:
                        logger.warning(f"{self.name}: Received stable_seq_num {transferred_stable_seq_num} "
                                     f"from {source_exec} is not higher than ours ({self.stable_seq_num}), "
                                     f"trying another replica")
                        continue

                    # Apply the transferred state
                    self.kv_store = copy.deepcopy(transferred_state)
                    self.stable_state = copy.deepcopy(transferred_state)
                    self.stable_seq_num = transferred_stable_seq_num
                    self.prev_hash = transferred_prev_hash
                    self.force_sequential = False

                    logger.info(f"{self.name}: Successfully applied state transfer from {source_exec}, "
                               f"now at stable_seq_num {self.stable_seq_num}")

                    return True

                else:
                    logger.warning(f"{self.name}: State transfer from {source_exec} failed: {response}")

            except Exception as e:
                logger.error(f"{self.name}: Error requesting state transfer from {source_exec}: {e}")

        return False

    # Handle incoming state transfer request from a diverged replica
    def _handle_state_transfer_request(self, payload):
        requesting_exec = payload.get('requesting_exec')

        logger.info(f"{self.name}: Received state transfer request from {requesting_exec}, "
                   f"providing stable state at seq_num {self.stable_seq_num}")

        return {
            'status': 'ok',
            'state': self.stable_state,
            'stable_seq_num': self.stable_seq_num,
            'prev_hash': self.prev_hash
        }
