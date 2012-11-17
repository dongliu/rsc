rsc
===

This repository contains the programs used in the evaluation of "RESTful Service Composition"

xrd
---

An embedded jetty service for file transfer.


snapshot
--------

The implementations of a simple PV snapshot application.
- snapshot.js is a node.js implementation based on ca.js in ./lib
- snapshot.py is a python implementation based on caget() of pyepics
- snapshot_pv.py is based on PV class of pyepics
- snapshot_ca.py is based on ca module of pyepics
- snapshot_pv_threading.py is a multi-threading version of snapshot_pv.py
- snapshot_ca_threading.py is a multi-threading version of snapshot_ca.py

For the details of pyepics and pv vs ca in pyepics, please check the [pyepics document](http://cars.uchicago.edu/software/python/pyepics3/advanced.html#strategies-for-connecting-to-a-large-number-of-pvs).