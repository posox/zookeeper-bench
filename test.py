#!/usr/bin/env python

from __future__ import division
from __future__ import print_function

import argparse
import csv
import datetime
import functools
import json
import logging
import os
import random
import string
import sys
import time
import traceback
import uuid

from kazoo import client
import posix_ipc


logging.basicConfig()

SEM_NAME_START = "zk-bench-start-sem-%s" % uuid.uuid4()
QUEUE_RESULT_NAME = "/zk-bench-result-%s" % uuid.uuid4()

STATUS_SUCCESS = "success"
STATUS_FAILED = "failed"


def msg(msg):
    print("%s [%d]: %s" % (str(datetime.datetime.now()), os.getpid(), msg))


def get_random_str(size):
    random.seed(os.urandom(128))
    return "".join([random.choice(string.letters) for _ in range(size)])


def timeit(fct):

    @functools.wraps(fct)
    def wrapper(*args, **kwargs):
        st = time.time()
        fct(*args, **kwargs)
        return time.time() - st

    return wrapper


def create_sem(name, *args, **kwargs):
    for _ in range(2):
        try:
            return posix_ipc.Semaphore(name, *args, **kwargs)
        except posix_ipc.ExistentialError:
            sem = posix_ipc.Semaphore(name)
            sem.unlink()
            sem.close()

    raise RuntimeError("Create semaphore failed")


def parse_args():
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
    parser.add_argument("--output", type=str,
                        help="specify a file to benchmark output")
    parser.add_argument("--format", type=str, default="json",
                        choices=["json", "csv"],
                        help="format of benchmark result")
    return parser.parse_args()


def main():
    args = parse_args()

    start_sem = create_sem(SEM_NAME_START, posix_ipc.O_CREX, initial_value=1)
    start_sem.acquire()

    children = []
    pid = -1
    for _ in range(args.threads):
        pid = os.fork()
        if pid == 0:
            msg("start process")
            run_bench(args.servers, args.message_count, args.znode_size,
                      args.watchers)
            break
        else:
            children.append(pid)

    if pid > 0:
        time.sleep(args.threads // 500)  # waiting start children
        start_sem.release()

        results = collect_result(args.threads)

        failed = 0
        for child in children:
            pid, exit_code = os.waitpid(child, 0)
            print("Child process %d exited with code %d" % (pid, exit_code))
            if exit_code != 0:
                failed += 1

        start_sem.unlink()
        start_sem.close()
        print("Total failed tasks: %d" % failed)
        write_results(results, args.output, args.format)


def collect_result(threads):
    queue = posix_ipc.MessageQueue(QUEUE_RESULT_NAME, flags=posix_ipc.O_CREAT)
    result = {}
    result["failed"] = 0

    for _ in range(threads):
        msg, priority = queue.receive()
        msg = json.loads(msg.decode("UTF-8"))

        if msg["status"] == STATUS_FAILED:
            result["failed"] += 1
            continue

        for k, v in msg["data"].items():
            result.setdefault(k, [])
            result[k].append(float(v))

    queue.unlink()
    queue.close()
    return result


def write_results(results, output, format):
    out = sys.stdout
    if output:
        out = open(output, "w")

    del results["failed"]

    if format == "json":
        json.dump(results, out, indent=4, sort_keys=True)
    elif format == "csv":
        keys = sorted(results.keys())
        writer = csv.DictWriter(out, fieldnames=keys)
        writer.writeheader()
        for idx in range(len(results[results.keys()[0]])):
            values = {}
            for key in results:
                values[key] = results[key][idx]
            writer.writerow(values)

    out.close()


def get_znode_path(path, number):
    return "%s/data-%d" % (path, number)


def run_bench(zk_hosts, message_count, znode_size, watchers):
    success = True
    try:
        zk = client.KazooClient(hosts=",".join(zk_hosts), timeout=150)
        zk.retry(zk.start)
        path = "/my/path-%s" % get_random_str(8)
        zk.retry(zk.ensure_path, path)

        msg("connected to zk")
        with posix_ipc.Semaphore(SEM_NAME_START):
            pass

        msg("bench started")

        metrics = []

        metrics.append(check_create_znode(zk, path, znode_size, message_count))
        metrics.append(check_get_znode(zk, path, message_count))
        metrics.append(check_update_znode(zk, path, message_count))
        metrics.append(check_watchers_create(zk, path, message_count, watchers))
        metrics.append(check_watchers_call(zk, path, message_count, watchers))
        metrics.append(check_delete_znode(zk, path))

        zk.retry(zk.stop)

    except Exception as e:
        success = False
        trace = traceback.format_exc(e)

    results = {}

    if success:
        keys = ("create_znode", "get_znode", "update_znode",
                "create_watcher", "call_watcher", "delete_znode")
        results["data"] = dict(
            map(lambda v: (v[0], "%.5f" % v[1]), zip(keys, metrics)))
        results["status"] = STATUS_SUCCESS
    else:
        results["status"] = STATUS_FAILED
        results["error"] = trace

    queue = posix_ipc.MessageQueue(QUEUE_RESULT_NAME, flags=posix_ipc.O_CREAT)
    queue.send(json.dumps(results).encode("ASCII"))
    queue.close()


@timeit
def check_create_znode(zk, root, message_size, message_count):
    data = "m" * message_size
    for i in range(message_count):
        data_path = get_znode_path(root, i)
        zk.retry(zk.create, data_path, data)
    zk.retry(zk.sync, root)


@timeit
def check_get_znode(zk, root, message_count):
    for i in range(message_count):
        data_path = get_znode_path(root, i)
        zk.retry(zk.get, data_path)
    zk.retry(zk.sync, root)


@timeit
def check_update_znode(zk, root, message_count):
    for i in range(message_count):
        data_path = get_znode_path(root, i)
        zk.retry(zk.set, data_path, str(i))
    zk.retry(zk.sync, data_path)


@timeit
def check_watchers_create(zk, root, message_count, watchers_count):
    if watchers_count < 1:
        return

    for i in range(message_count):
        data_path = get_znode_path(root, i)
        for j in range(watchers_count):
            zk.retry(zk.get_children, data_path, watch=(lambda event: 1 + 2))
    zk.retry(zk.sync, root)


@timeit
def check_watchers_call(zk, root, message_count, watchers_count):
    if watchers_count < 1:
        return

    for i in range(message_count):
        data_path = get_znode_path(root, i)
        zk.retry(zk.set, data_path, str(i + 1))
    zk.retry(zk.sync, root)


@timeit
def check_delete_znode(zk, root):
    zk.retry(zk.delete, root, recursive=True)
    zk.retry(zk.sync, root)


if __name__ == "__main__":
    main()
