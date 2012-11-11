#!/usr/bin/python

from epics import caget
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
for pv in pv_list:
    result[pv] = caget(pv, timeout=1)
duration = time.time() - start
print int(round(duration * 1000))
