#!/usr/bin/python

from epics import ca
import time
import json
import sys

if len(sys.argv) == 1:
    config = open('./config/pv_list.json', 'r')
elif len(sys.argv) == 2:
    if str(sys.argv[1]) == 'clean':
        config = open('./config/pv_list_clean.json', 'r')
    else:
        config = open('./config/pv_list.json', 'r')
else:
    sys.exit('at most two arguments allowed.')

result = {}
pv_list = json.loads(config.read())
start = time.time()
for pv_name in pv_list:
    ch = ca.create_channel(pv_name, connect=False, auto_cb=False)
    result[pv_name] = [ch, None, None]
for pv_name, data in result.items():
    result[pv_name][1] = ca.connect_channel(data[0])
ca.poll()
for pv_name, data in result.items():
    if result[pv_name][1]:
        ca.get(data[0], wait=False)

ca.poll()
for pv_name, data in result.items():
    if result[pv_name][1]:
        val = ca.get_complete(data[0])
        result[pv_name][2] = val
    else:
        result[pv_name][2] = 'not connected'

duration = time.time() - start
print int(round(duration * 1000))
