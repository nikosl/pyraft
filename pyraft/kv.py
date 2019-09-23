from concurrent import futures
import threading

import grpc

import kv_pb2
import kv_pb2_grpc
import log_commands


class KV(object):
    def __init__(self):
        self.bucket = {}
        self._lock = threading.Lock()

    def get(self, key):
        with self._lock:
            return self.bucket[key] if key in self.bucket else None

    def delete(self, key):
        with self._lock:
            return self.bucket.pop(key, None)

    def put(self, key, val):
        with self._lock:
            self.bucket[key] = val
            return val


class KVServicer(kv_pb2_grpc.KVServicer):
    def __init__(self, address, store, state):
        super(KVServicer, self).__init__()
        self.address = address
        self.store = store
        self.state = state

    def Get(self, request, context):
        val = self.store.get(request.key)
        return kv_pb2.Data(
            key=request.key,
            value=val,
            error=False,
            timestamp=""
        )

    def Delete(self, request, context):
        self.state.next_state(log_commands.DELETE, request.key, request.value)
        self.store.delete(request.key)
        return kv_pb2.Data(
            key=request.key,
            value="",
            error=False,
            timestamp=""
        )

    def Put(self, request, context):
        self.state.next_state(log_commands.PUT, request.key, request.value)
        self.store.put(request.key, request.value)
        return kv_pb2.Data(
            key=request.key,
            value=request.value,
            error=False,
            timestamp=""
        )

    def GetLeader(self, request, context):
        lid, address = self.state.get_current_leader()
        return kv_pb2.Node(
            id=lid,
            address=address
        )


def serve(kvservicer):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    kv_pb2_grpc.add_KVServicer_to_server(kvservicer, server)
    server.add_insecure_port(kvservicer.address)
    return server
