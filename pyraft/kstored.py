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
    print("Starting at {}...".format(MY_ADDRESS))
    print("PEERS {} MYID {} STATE {}".format(PEERS, MY_ID, STATE))
    store = kv.KV()
    peers = {}
    if PEERS:
        p = PEERS.split(',')
        for peer in p:
            peer_id, addr = peer.split('=')
            peers[int(peer_id)] = state.Peer(
                peer_id,
                addr,
                ""
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
