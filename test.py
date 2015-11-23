#!/usr/bin/env python

from __future__ import print_function

import argparse
import logging
import os
import random
import string

from kazoo import client


logging.basicConfig()


LOG = logging.getLogger(__name__)


def get_random_str(size):
    random.seed(os.urandom(128))
    return "".join([random.choice(string.letters) for _ in range(size)])


def main():
    parser = argparse.ArgumentParser(prog="zookeeper_bench")
    parser.add_argument("--servers", type=str, nargs="+",
                        help="list of ZK servers")
    parser.add_argument("--threads", type=int, default=1,
                        help="count of thrads (default: 1)")
    parser.add_argument("--message-count", type=int, default=1000,
                        help="count of znode per thread (default: 1000)")
    parser.add_argument("--znode-size", type=int, default=1000,
                        help="size of znode in bytes (default: 1000)")
    parser.add_argument("--watchers", type=int, default=0,
                        help="count of watchers per znode (default: 0)")
    args = parser.parse_args()

    children = []
    pid = -1
    for x in range(args.threads):
        pid = os.fork()
        if pid == 0:
            print("Start child process %d" % os.getpid())
            break
        else:
            children.append(pid)

    if pid == 0:
        run_bench(args.servers, args.message_count, args.znode_size,
                  args.watchers)
    else:
        for child in children:
            pid, exit_code = os.waitpid(child, 0)
            print("Child process %d exited with code %d" % (pid, exit_code))


def watcher(event):
    return 1 + 2


def get_znode_path(path, number):
    return "%s/data-%d" % (path, number)


def run_bench(zk_hosts, message_count, znode_size, watchers):
    zk = client.KazooClient(hosts=",".join(zk_hosts))
    zk.start()
    path = "/my/path-%s" % get_random_str(8)
    zk.ensure_path(path)

    # create znode
    for x in range(message_count):
        data_path = get_znode_path(path, x)
        zk.create(data_path, str(x) * znode_size)

    # get znode
    for x in range(message_count):
        data_path = get_znode_path(path, x)
        zk.get(data_path)

    # update znode
    for x in range(message_count):
        data_path = get_znode_path(path, x)
        zk.set(data_path, str(x + 1))

    # create watchers
    if watchers > 0:
        for x in range(message_count):
            data_path = get_znode_path(path, x)
            for w in range(watchers):
                zk.get_children(data_path, watch=watcher)

    # call watchers
    if watchers > 0:
        for x in range(message_count):
            data_path = get_znode_path(path, x)
            zk.set(data_path, str(x + 2))

    # delete znodes
    zk.delete(path, recursive=True)
    zk.stop()


if __name__ == "__main__":
    main()