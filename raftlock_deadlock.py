#!/bin/python3
# coding: utf-8

# info: reproduce raftLock_ deadlock on storage
# commit: 650776b

import os
import time
import random
import signal
from subprocess import check_output

import pdb
import sys
import time
import random
import string
from traceback import print_tb
from nebula2 import meta
from nebula2.mclient import MetaCache
from nebula2.gclient.net import ConnectionPool
from nebula2.Config import Config

meta_host = '192.168.15.11'
# meta_host = '192.168.8.53'
meta_port = '9559'
graph_host = '192.168.15.11'
# graph_host = '192.168.8.53'
graph_port = '9669'
space = 'ttos_3p3r'
user = 'root'
passwd = 'nebula'

meta_cache = MetaCache([(meta_host, meta_port)], 50000)
config = Config()
config.max_connection_pool_size = 10
conn_pool = ConnectionPool()
ok = conn_pool.init([(graph_host, graph_port)], config)
session = conn_pool.get_session(user, passwd)

def get_leader():
    ret = session.execute('show hosts')
    # pdb.set_trace()
    ports = ret.column_values('Port')
    leader_count = ret.column_values('Leader count')
    leader_ports = zip(ports, leader_count)
    # pdb.set_trace()
    # leader_ports = leader_ports[:len(leader_ports)-1]
    for (port, count) in leader_ports:
        # pdb.set_trace()
        # print(port, count)
        # cmd = "lsof -i :47583 | grep 47583 | awk '{print $2}'"
        if count.as_int() > 0 and port.is_int():
            print(port.as_int())
            _port = port.as_int()
            cmd = "lsof -P -i :{} | grep {} | grep LISTEN | awk '{}'".format(_port, _port, '{print $2}')
            print('cmd: {}'.format(cmd))
            # cmd = "lsof -i :{} | grep {}".format(_port, _port)
            # print('cmd: {}'.format(cmd))
            pid = int(check_output([cmd], shell=True))
            print('leader pid: {}'.format(pid))

            return pid

    return None

# get_leader()
# process_name = 'nebula-storaged'
# pid_list = check_output(['pidof', '-c', '/data/src/wwl/nebula/build/bin', process_name])
# pids = pid_list.split()
# pids = [int(pid) for pid in pids]
# print(pids)
# random.seed(int(time.time()))
#
while True:
    # victim = random.choice(pids)
    leader_pid = get_leader()
    victim = leader_pid
    # print("stopping {}".format(victim))
    # # import pdb; pdb.set_trace()
    if not victim:
        print("failed getting storage leader")
        # time.sleep(0.01)
        continue

    os.kill(victim, signal.SIGSTOP)
    time.sleep(6)
    os.kill(victim, signal.SIGCONT)
    print('resuming {}'.format(victim))
    time.sleep(0.6)
    # time.sleep(0.6)
    # time.sleep(3)
    # time.sleep(0.5)

