import os
import time

import kv
import state
import threading

_ONE_DAY_IN_SECONDS = 60 * 60 * 24

MY_ADDRESS = os.getenv('ADDRESS', 'localhost:8005')
MY_ID = os.getenv('MYID', '1')

if __name__ == '__main__':
    print("Starting at {}...".format(MY_ADDRESS))
    store = kv.KV()
    st = state.State(int(MY_ID), {})
    kvserver = kv.KVServicer(MY_ADDRESS, store)
    server = kv.serve(kvserver)
    st.register("PUT", kvserver.put)
    st.register("GET", kvserver.get)
    st.register("DELETE", kvserver.delete)
    st.register("KEYS", kvserver.keys)
    threading.Thread(target=st.run).start()
    threading.Thread(target=server.start).start()
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)
