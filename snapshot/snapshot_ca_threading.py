#!/usr/bin/python

from epics import ca
import time
import json
import sys
import os
import signal
from multiprocessing import Process, Manager


def get(d, pv_name, size, start, pid):
    ch = ca.create_channel(pv_name, connect=False, auto_cb=False)
    if ca.connect_channel(ch, timeout=1.0):
        d[pv_name] = ca.get(ch, wait=True)
    else:
        d[pv_name] = 'not connected'
    if len(d) == size:
        print int(round((time.time() - start) * 1000))
        os.kill(pid, signal.SIGTERM)


if len(sys.argv) == 1:
    config = open('./config/pv_list.json', 'r')
elif len(sys.argv) == 2:
    if str(sys.argv[1]) == 'clean':
        config = open('./config/pv_list_clean.json', 'r')
    else:
        config = open('./config/pv_list.json', 'r')
else:
    sys.exit('at most two arguments allowed.')

pv_list = json.loads(config.read())
size = len(pv_list)
manager = Manager()
d = manager.dict()
start = time.time()
pid = os.getpid()

for pv_name in pv_list:
    p = Process(target=get, args=(d, pv_name, size, start, pid))
    p.start()

time.sleep(30)
