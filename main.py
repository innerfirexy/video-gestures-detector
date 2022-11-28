import multiprocessing as mp
import queue
import subprocess
import json
import sys
import os
import time
import cv2
import face_recognition
import numpy as np
import argparse
import re
from typing import List

# Fix mp.Queue.qsize() problem on MacOS
import platform
if platform.system() == 'Darwin':
    from FixedQueue import Queue
else:
    from multiprocessing.queues import Queue

# Create argument parser
def create_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--batch_mode', '-bm', action='store_true')
    parser.add_argument('--batch_size', '-bs', type=int, default=128)
    parser.add_argument('--delete_after_done', action='store_true')
    parser.add_argument('--sample_interval', '-si', type=int, default=24)
    parser.add_argument('--log_file', type=str)
    return parser


class Task:
    def __init__(self, task_type: str, url: str):
        self.task_type = task_type
        self.url = url
        self.done = False
        self.status = None
        self.video_id = re.search(r'(?<=\?v\=).+', url).group(0)
    def set_done(self):
        self.done = True


class DownloadTask(Task):
    command = 'yt-dlp'
    def __init__(self, download_path: str, args, **kwargs):
        super(DownloadTask, self).__init__(**kwargs)
        self.download_path = download_path
        self.n_trials = 0
        self.args = args

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

    def create_next_task(self):
        input_video = os.path.join(self.download_path, self.video_id + '.mp4')
        parse_task = ParseTask(self, input_path=input_video, sample_interval=self.args.sample_interval,
                               batch_mode=self.args.batch_mode, batch_size=self.args.batch_size,
                               delete_after_done=self.args.delete_after_done, url=self.url)
        return parse_task


class ParseTask(Task):
    def __init__(self, input_path: str, sample_interval: int, batch_mode: bool, batch_size: int,
                 delete_after_done: bool, **kwargs):
        """
        :param input_path: Path of input video file
        :param sample_interval: Interval of sampling the input video, measured by number of frames
        :param delete_after_done: If True, delete the input video after the parsing is successfully done
        :param kwargs:
        """
        super(ParseTask, self).__init__(**kwargs)
        self.input_file = input_path
        self.sample_interval = sample_interval
        self.batch_mode = batch_mode
        self.batch_size = batch_size
        self.delete_after_done = delete_after_done
        self.parse_result = None
    def execute(self):
        try:
            video_capture = cv2.VideoCapture(self.input_path)
            total_frame_count = int(video_capture.get(cv2.CAP_PROP_FRAME_COUNT))
        except Exception:
            self.status = 'failure'
            return 1
        else:
            if self.batch_mode:
                # Use CUDA for faster processing
                # https://github.com/ageitgey/face_recognition/blob/master/examples/find_faces_in_batches.py
                frames = []
                current_frame_index = -1
                frame_indices = []
                number_of_faces_in_frames = []
                while video_capture.isOpened():
                    ret, frame = video_capture.read()
                    if not ret:
                        break
                    current_frame_index += 1
                    if (current_frame_index + 1) % self.sample_interval == 0:
                        frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                        frames.append(frame)
                        frame_indices.append(current_frame_index)
                    if len(frames) == self.batch_size \
                            or current_frame_index == total_frame_count - 1: # Last frame
                        batch_of_face_locations = face_recognition.batch_face_locations(frames, number_of_times_to_upsample=0)
                        for face_locations in batch_of_face_locations:
                            num_faces = len(face_locations)
                            number_of_faces_in_frames.append(num_faces)
                        frames = []
                self.status = 'success'
                self.set_done()
                self.parse_result = (frame_indices, number_of_faces_in_frames)
            else:
                # Process frames one by one
                # https://github.com/ageitgey/face_recognition/blob/master/examples/find_faces_in_picture.py
                pass

            if self.delete_after_done:
                pass

            return 0


def task_worker(tasks_assigned, tasks_done, tasks_fail, next_tasks_assigned=None):
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
                if next_tasks_assigned is not None:
                    # Create parse task and add it to the queue
                    next_task = task.create_next_task()
                    next_tasks_assigned.put(next_task)
            else:
                tasks_fail.put(task)
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
        p = mp.Process(target = task_worker, args=(dl_tasks_assigned, dl_tasks_done, dl_tasks_failed))
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
    play_list_file = 'play_lists/Life Academy_Loving on Purpose.md'
    dl_tasks_assigned = Queue()
    dl_tasks_done = Queue()
    dl_tasks_failed = Queue()
    tasks = init_download_tasks(play_list_file)
    for t in tasks:
        dl_tasks_assigned.put(t)
    num_dl_tasks = len(tasks)

    num_dl_workers = 2
    download_processes = []
    for _ in range(num_dl_workers):
        p = mp.Process(target = task_worker, args=(dl_tasks_assigned, dl_tasks_done, dl_tasks_failed))
        download_processes.append(p)
        p.start()

    num_parse_workers = 2
    parse_processes = []
    for _ in range(num_parse_workers):
        pass

    while True:
        # dispatch download
        pass


if __name__ == '__main__':
    test_dl()
