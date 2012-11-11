#!/usr/bin/python

import epics
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
    pv = epics.PV(pv_name)
    result[pv_name] = pv.get(count=None, as_string=False, as_numpy=True, timeout=1.0, use_monitor=False)
duration = time.time() - start
print int(round(duration * 1000))
