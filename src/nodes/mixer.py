from .node import Node
from common.net import send_message
import time, threading, logging
from collections import defaultdict

logger = logging.getLogger(__name__)

class Mixer(Node):
    def __init__(self, name, host, port, next, shim):
        super().__init__(name, host, port)
        self.next = next
        self.shim = shim  # Origin shim for response routing
        self.batch = []
        self.batch_size = 10
        self.batch_timeout = 0.1
        self.seq_num = 0
        self.lock = threading.Lock()
        self.last_batch_time = time.time()

    def start(self):
        flusher = threading.Thread(target=self._batch_flusher, daemon=True)
        flusher.start()
        super().start()

    # Periodically flush batch if timeout reached
    # TODO: different mixers may have different timings of flushing, this may send different parallelBatches to exec, causing divergence
    def _batch_flusher(self):
        while True:
            time.sleep(self.batch_timeout)
            with self.lock:
                if self.batch and (time.time() - self.last_batch_time) >= self.batch_timeout:
                    self._flush_batch()

    # Extract read/write keys from request for conflict detection
    # keys are based on the objects the request will access
    def _get_keys(self, request):
        op = request.get('op', '')
        payload = request.get('op_payload', {})

        read_keys = set()
        write_keys = set()

        if op == 'read':
            read_keys.add(payload.get('key', ''))
        elif op == 'write':
            write_keys.add(payload.get('key', ''))
        elif op == 'read_write':
            read_keys.add(payload.get('read_key', ''))
            write_keys.add(payload.get('write_key', ''))

        return read_keys, write_keys

    # Check for read/write or write/write conflicts
    def _has_conflict(self, req_read, req_write, batch_reads, batch_writes):
        # Write-write conflict
        if req_write & batch_writes:
            return True
        # Read-write conflict (either direction)
        if req_write & batch_reads:
            return True
        if req_read & batch_writes:
            return True
        return False

    # Partition batch into parallelBatches of non-conflicting requests
    def _partition_into_parallel_batches(self, batch):
        parallel_batches = []

        for request in batch:
            req_read, req_write = self._get_keys(request)
            placed = False

            # Try to add to existing parallelBatch
            for pb in parallel_batches:
                if not self._has_conflict(req_read, req_write, pb['reads'], pb['writes']):
                    pb['requests'].append(request)
                    pb['reads'] |= req_read
                    pb['writes'] |= req_write
                    placed = True
                    break

            # Create new parallelBatch if conflicts with all existing
            if not placed:
                parallel_batches.append({
                    'requests': [request],
                    'reads': req_read,
                    'writes': req_write
                })

        return [pb['requests'] for pb in parallel_batches]

    def _flush_batch(self):
        if not self.batch:
            return

        batch = self.batch
        self.batch = []
        self.seq_num += 1
        self.last_batch_time = time.time()

        # Partition into parallelBatches
        parallel_batches = self._partition_into_parallel_batches(batch)

        logger.info(f"{self.name}: Batch {self.seq_num} partitioned into "
                   f"{len(parallel_batches)} parallelBatches from {len(batch)} requests")

        # Send to exec with sequence number and nondeterminism data
        message = {
            'type': 'batch',
            'seq_num': self.seq_num,
            'parallel_batches': parallel_batches,
            'nd_seed': int(time.time() * 1000),  # For rand() determinism
            'nd_timestamp': time.time(),         # For gettimeofday() determinism
        }

        send_message(self.next, '8000', message)

    def handle_message(self, payload):
        logger.debug(f"Handler called on {self.name} with payload: {payload}")

        with self.lock:
            self.batch.append(payload)

            if len(self.batch) >= self.batch_size:
                self._flush_batch()

        return {'status': 'batched'}

        