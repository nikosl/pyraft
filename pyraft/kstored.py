import os
import time

import kv
import state
import threading

_ONE_DAY_IN_SECONDS = 60 * 60 * 24

MY_ADDRESS = os.getenv('ADDRESS', 'localhost:8005')
MY_ID = os.getenv('MYID', '1')
PEERS = os.getenv("PEERS", "1=127.0.0.1:7005")
EXTADDRESS = os.getenv('EXTADDRESS', '1=127.0.0.1:8005')
STATE = os.getenv('STATE', '0')

if __name__ == '__main__':
    if os.environ.get('https_proxy'):
        del os.environ['https_proxy']
    if os.environ.get('http_proxy'):
        del os.environ['http_proxy']

    print("Starting at {}...".format(MY_ADDRESS))
    print("PEERS {} MYID {} STATE {}".format(PEERS, MY_ID, STATE))
    store = kv.KV()
    peers = {}
    if PEERS:
        p = PEERS.split(',')
        ext_peers = EXTADDRESS.split(',')
        for peer in p:
            peer_id, addr = peer.split('=')
            ext_addr = ""
            for ext in ext_peers:
                ext_peer_id, _ext_addr = ext.split('=')
                if ext_peer_id == peer_id:
                    ext_addr = _ext_addr
            peers[int(peer_id)] = state.Peer(
                peer_id,
                addr,
                ext_addr
            )

    st = state.State(int(MY_ID), peers, int(STATE))
    kvserver = kv.KVServicer(MY_ADDRESS, store, st)
    server = kv.serve(kvserver)
    threading.Thread(target=st.run).start()
    threading.Thread(target=server.start).start()

    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)
