import logging
import argparse
import os
import sys

import grpc

from pyraft import kv_pb2
from pyraft import kv_pb2_grpc


def put(stub, key, val):
    print("Put {}:{}".format(key, val))
    response = stub.Put(kv_pb2.Data(key=key, value=val))
    print("client received: {}".format(response))


def get(stub, key):
    print("Get {}".format(key))
    response = stub.Get(kv_pb2.Data(key=key))
    print("client received: {}".format(response))


def delete(stub, key):
    print("Delete {}".format(key))
    response = stub.Delete(kv_pb2.Data(key=key))
    print("client received: {}".format(response))


def get_leader(peers):
    for peer in peers:
        try:
            with grpc.insecure_channel(target=peer,
                                       options=[('grpc.enable_retries', 0),
                                                ('grpc.keepalive_timeout_ms', 1000)]) as chan:
                lstub = kv_pb2_grpc.KVStub(chan)
                response = lstub.GetLeader(kv_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty())
                print("leader is : {}".format(response))
                if response:
                    return response.address
        except Exception as e:
            print("Error {}: {}.".format(peer, e.details()))
    return ""


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('-nodes', dest='nodes', action="store", help='cluster nodes [ip:port,...]', required=True)
    parser.add_argument('-get', dest='get_kv', action='store', help='get key value')
    parser.add_argument('-delete', dest='del_kv', action='store', help='delete key value')
    parser.add_argument('-put', dest='put_kv', nargs=2, action='store', help='put key value')

    args = parser.parse_args()

    logging.basicConfig()

    nodes = args.nodes

    if os.environ.get('https_proxy'):
        del os.environ['https_proxy']
    if os.environ.get('http_proxy'):
        del os.environ['http_proxy']

    print("Using: {}".format(nodes))

    seed = [nodes] if ',' not in nodes else nodes.split(",")

    leader = get_leader(seed)
    if not leader:
        print("No available node.")
        sys.exit(-1)

    print("Connect to: {}".format(leader))
    with grpc.insecure_channel(target=leader,
                               options=[('grpc.enable_retries', 0), ('grpc.keepalive_timeout_ms', 1000)]) as channel:
        client = kv_pb2_grpc.KVStub(channel)
        if args.get_kv:
            get(client, args.get_kv)
        elif args.del_kv:
            delete(client, args.del_kv)
        elif args.put_kv:
            put(client, args.put_kv[0], args.put_kv[1])
