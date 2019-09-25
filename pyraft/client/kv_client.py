import logging
import argparse
import os

import grpc

from pyraft import kv_pb2
from pyraft import kv_pb2_grpc


def put(stub, key, val):
    response = stub.Put(kv_pb2.Data(key=key, value=val))
    print("client received: {}".format(response))


def get(stub, key):
    response = stub.Get(kv_pb2.Data(key=key))
    print("client received: {}".format(response))


def delete(stub, key):
    response = stub.Delete(kv_pb2.Data(key=key))
    print("client received: {}".format(response))


def get_leader(server):
    n = ""
    with grpc.insecure_channel(target=server,
                               options=[('grpc.enable_retries', 0), ('grpc.keepalive_timeout_ms', 1000)]) as chan:
        lstub = kv_pb2_grpc.KVStub(chan)
        response = lstub.GetLeader(kv_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty())
        print("leader is : {}".format(response))
        if response:
            n = response.address
    return n


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('-b', action="store", dest='seed', help='cluster node ip:port')
    parser.add_argument('-g', dest='gkey', action='store', help='get key value')
    parser.add_argument('-d', dest='dkey', action='store', help='delete key value')
    parser.add_argument('-p', dest='pkey', action='store', help='put key value')
    parser.add_argument('-v', dest='val', action='store', help='value to store')

    args = parser.parse_args()

    if not args.seed:
        parser.print_help()
        os.exit(1)

    logging.basicConfig()

    seed = args.seed

    if os.environ.get('https_proxy'):
        del os.environ['https_proxy']
    if os.environ.get('http_proxy'):
        del os.environ['http_proxy']

    print("Connect to: {}".format(seed))

    leader = get_leader(seed)

    with grpc.insecure_channel(target=seed,
                               options=[('grpc.enable_retries', 0), ('grpc.keepalive_timeout_ms', 1000)]) as channel:
        client = kv_pb2_grpc.KVStub(channel)
        if args.gkey:
            get(client, args.gkey)
        elif args.dkey:
            delete(client, args.dkey)
        elif args.pkey:
            put(client, args.pkey, args.val)
