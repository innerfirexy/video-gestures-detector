import json
import sys
import os
import glob
import itertools
from utils import Task, DownloadTask

# Fix mp.Queue.qsize() problem on MacOS
import platform
if platform.system() == 'Darwin':
    from FixedQueue import Queue
else:
    from multiprocessing.queues import Queue


def download_task_worker(tasks_assigned, tasks_done, tasks_failed, next_tasks_assigned=None):
    while True:
        try:
            task = tasks_assigned.get()
        except queue.Empty:
            break
        else:
            # do the task
            task.execute()
            if task.done:
                tasks_done.put(task)
                if next_tasks_assigned is not None:
                    # Create parse task and add it to the queue
                    next_task = task.create_next_task()
                    next_tasks_assigned.put(next_task)
            else:
                tasks_failed.put(task)
    return True


def _load_tasks_from_playlist_file(play_list_file: str, args) -> List[Task]:
    video_urls = []
    with open(play_list_file, 'r') as f:
        for line in f:
            line = line.strip()
            if len(line) == 0 or line.startswith('#'):
                continue
            if line.startswith('{'):
                json_obj = json.loads(line)
                if json_obj['duration'] is not None: # duration is null for private videos
                    video_urls.append(json_obj['url'])
    tasks = []
    for url in video_urls:
        tasks.append(DownloadTask(download_path='./tmp', url=url, args=args))
    return tasks

def load_tasks_from_folder(play_list_folder: str, args) -> List[Task]:
    loaded_tasks = []
    play_list_files = glob.glob(os.path.join(play_list_folder, '*.md'))
    for pl_file in play_list_files:
        tasks = _load_tasks_from_playlist_file(pl_file, args)
        loaded_tasks.append(tasks)
    loaded_tasks = list(itertools.chain.from_iterable(loaded_tasks))
    return loaded_tasks


def main(args):
    play_list_folder = 'play_lists/'
    dl_tasks = load_tasks_from_folder(play_list_folder, args)

    dl_tasks_assigned = Queue(ctx=mp.get_context())
    dl_tasks_done = Queue(ctx=mp.get_context())
    dl_tasks_failed = Queue(ctx=mp.get_context())

    for t in dl_tasks:
        dl_tasks_assigned.put(t)
    num_dl_tasks = len(dl_tasks)