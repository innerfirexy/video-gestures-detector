import multiprocessing as mp
import queue
import json
import sys
import os
import glob
import time
import itertools
import numpy as np
import argparse
from typing import List
from utils import DownloadTask, ParseTask



# Create argument parser
def create_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--batch_mode', '-bm', action='store_true', help='batch mode requires dlib installed with CUDA support. See this post for installation: https://gist.github.com/nguyenhoan1988/ed92d58054b985a1b45a521fcf8fa781')
    parser.add_argument('--batch_size', '-bs', type=int, default=128)
    parser.add_argument('--delete_after_done', action='store_true')
    parser.add_argument('--sample_interval', '-si', type=int, default=24)
    parser.add_argument('--log_file', type=str, default=None)
    return parser


def parse_task_worker(tasks_assigned, tasks_done, tasks_failed, pid):
    while True:
        try:
            task = tasks_assigned.get() # Using get_nowait() will cause problem
        except queue.Empty:
            break
        else:
            task.execute(pid)
            if task.done:
                tasks_done.put(task)
            else:
                tasks_failed.put(task)
    return True


def log_writer(log_file: str, parse_tasks_done: Queue):
    with open(log_file, 'w') as f:
        while True:
            task = parse_tasks_done.get()
            if task == 'done':
                break
            f.write(task.get_log_str())
            f.flush()



def main():
    pass


def test_dl(args):
    # t1 = DownloadTask(download_path='./tmp', url='https://www.youtube.com/watch?v=Z3l3ST7z7ps', task_type='download')
    # ret = t1.execute()
    # print(ret)

    play_list_file = 'play_lists/Life Academy_Loving on Purpose.md'
    dl_tasks_assigned = Queue()
    dl_tasks_done = Queue()
    dl_tasks_failed = Queue()
    tasks = _load_tasks_from_playlist_file(play_list_file, args)
    for t in tasks:
        dl_tasks_assigned.put(t)
    num_tasks_total = len(tasks)

    num_dl_workers = 2
    download_processes = []
    for _ in range(num_dl_workers):
        p = mp.Process(target = download_task_worker, args=(dl_tasks_assigned, dl_tasks_done, dl_tasks_failed))
        download_processes.append(p)
        p.start()

    while True:
        time.sleep(1)
        num_tasks_done = dl_tasks_done.qsize()
        num_tasks_failed = dl_tasks_failed.qsize()
        num_tasks_remain = num_tasks_total - num_tasks_done - num_tasks_failed
        sys.stdout.write(f'\r Remaining tasks #: {num_tasks_remain} | Done: {num_tasks_done} | Failed: {num_tasks_failed}')
        sys.stdout.flush()

        if num_tasks_remain == 0:
            print('='*12)
            print('All download tasks done')
            break
    
    for p in download_processes:
        p.join()


def test_parse(args):
    parse_tasks_assigned = Queue(ctx=mp.get_context())
    parse_tasks_done = Queue(ctx=mp.get_context())
    parse_tasks_failed = Queue(ctx=mp.get_context())

    num_dl_workers = 2
    download_processes = []
    for _ in range(num_dl_workers):
        p = mp.Process(target = download_task_worker, args=(dl_tasks_assigned, dl_tasks_done, dl_tasks_failed, parse_tasks_assigned))
        download_processes.append(p)
        p.start()
    
    # Let the download task workers run for a while
    while True:
        time.sleep(1)
        if not parse_tasks_assigned.empty():
            break
    # print(f'parse tasks assigned: {parse_tasks_assigned.qsize()}')

    num_parse_workers = 2
    parse_processes = []
    for i in range(num_parse_workers):
        p = mp.Process(target = parse_task_worker, args=(parse_tasks_assigned, parse_tasks_done, parse_tasks_failed, i+1))
        parse_processes.append(p)
        p.start()
    
    if args.log_file:
        p_log = mp.Process(target = log_writer, args=(args.log_file, parse_tasks_done))
        p_log.start()

    while True:
        time.sleep(1)
        num_dl_tasks_done = dl_tasks_done.qsize()
        num_dl_tasks_failed = dl_tasks_failed.qsize()
        num_dl_tasks_remain = num_dl_tasks - num_dl_tasks_done - num_dl_tasks_failed
        num_parse_tasks_total = num_dl_tasks - num_dl_tasks_failed
        num_parse_tasks_done = parse_tasks_done.qsize()
        num_parse_tasks_failed = parse_tasks_failed.qsize()
        num_parse_tasks_remain = num_parse_tasks_total - num_parse_tasks_done - num_parse_tasks_failed
        sys.stdout.write(f'\r dl tasks remain: {num_dl_tasks_remain}, done: {num_dl_tasks_done}, failed: {num_dl_tasks_failed} | parse tasks remain: {num_parse_tasks_remain}')
        sys.stdout.flush()

        if num_parse_tasks_remain == 0:
            parse_tasks_done.put('done')
            print('\n' + '='*12)
            print('All tasks done')
            break
    
    for p in download_processes:
        p.join()
    for p in parse_processes:
        p.join()
    if args.log_file:
        p_log.join()


if __name__ == '__main__':
    parser = create_parser()
    args = parser.parse_args()
    test_parse(args)
