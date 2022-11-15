import multiprocessing as mp
import queue
import subprocess
import json
import sys
import time
from typing import List

# Fix mp.Queue.qsize() problem on MacOS
import platform
if platform.system() == 'Darwin':
    from FixedQueue import Queue
else:
    from multiprocessing.queues import Queue


class Task:
    def __init__(self, task_type: str, url: str):
        self.task_type = task_type
        self.url = url
        self.done = False
        self.status = None

    def set_done(self):
        self.done = True


class DownloadTask(Task):
    command = 'yt-dlp'

    def __init__(self, download_path: str, **kwargs):
        super(DownloadTask, self).__init__(**kwargs)
        self.download_path = download_path
        self.n_trials = 0
    
    def execute(self):
        ret = subprocess.run([DownloadTask.command, self.url, '--paths', self.download_path, 
        '--output', '%(id)s.%(ext)s', '--format', 'mp4', '--write-auto-subs',
        '--quiet'])

        if ret.returncode == 0:
            self.set_done()
            self.status = 'success'
        else:
            self.status = 'failure'
        self.n_trials += 1

        return ret


class ParseTask(Task):
    def __init__(self, output_path: str, **kwargs):
        super(ParseTask, self).__init__(**kwargs)
        self.output_path = output_path


def download_worker(tasks_assigned, tasks_done, tasks_fail):
    while True:
        try:
            task = tasks_assigned.get_nowait()
        except queue.Empty:
            break
        else:
            # do the task
            task.execute()
            if task.done:
                tasks_done.put(task)
            else:
                tasks_fail.put(task)

    return True


def parse_worker(tasks_assigned, tasks_done):
    while True:
        try:
           task = tasks_assigned.get_nowait()
        except queue.Empty:
            break
        else:
            # do the task
            task.set_done()
            tasks_done.put(task)
    return True


def init_download_tasks(play_list_file: str) -> List[Task]:
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
        tasks.append(DownloadTask(download_path='./tmp', url=url, task_type='download'))
    
    return tasks


def main():
    dl_tasks_assigned = Queue()
    dl_tasks_done = Queue()
    dl_tasks_fail = Queue()

    parse_tasks_assigned = Queue()
    parse_tasks_done = Queue()

    download_tasks = init_download_tasks()
    for t in download_tasks:
        dl_tasks_assigned.put(t)


def test_dl():
    # t1 = DownloadTask(download_path='./tmp', url='https://www.youtube.com/watch?v=Z3l3ST7z7ps', task_type='download')
    # ret = t1.execute()
    # print(ret)

    play_list_file = 'play_lists/Life Academy_Loving on Purpose.md'
    dl_tasks_assigned = Queue()
    dl_tasks_done = Queue()
    dl_tasks_failed = Queue()
    tasks = init_download_tasks(play_list_file)
    for t in tasks:
        dl_tasks_assigned.put(t)
    num_tasks_total = len(tasks)

    num_dl_workers = 2
    download_processes = []
    for _ in range(num_dl_workers):
        p = mp.Process(target = download_worker, args=(dl_tasks_assigned, dl_tasks_done, dl_tasks_failed))
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


def test_parse():
    pass


if __name__ == '__main__':
    test_dl()