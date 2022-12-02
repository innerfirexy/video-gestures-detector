import multiprocessing as mp
import queue
import subprocess
import json
import sys
import os
import glob
import time
import itertools
import pickle
import cv2
import face_recognition
import numpy as np
from tqdm import tqdm
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
    parser.add_argument('--log_file', type=str, default=None)
    return parser


class Task:
    def __init__(self):
        self.done = False
        self.status = None
    def set_done(self):
        self.done = True


class DownloadTask(Task):
    command = 'yt-dlp'
    def __init__(self, download_path: str, url: str, args):
        super(DownloadTask, self).__init__()
        self.download_path = download_path
        self.url = url
        self.args = args
        self.video_id = re.search(r'(?<=\?v\=).+', url).group(0)
        self.downloaded_video_file = os.path.join(self.download_path, self.video_id + '.mp4')
        self.n_trials = 0

    def execute(self):
        if os.path.exists(self.downloaded_video_file):
            self.set_done()
            self.status = 'success'
            return 
        # Run download command
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
        try:
            parse_task = ParseTask(input_file=self.downloaded_video_file, sample_interval=self.args.sample_interval, 
            batch_mode=self.args.batch_mode, batch_size=self.args.batch_size, delete_after_done=self.args.delete_after_done, url=self.url)
        except Exception:
            print('Error in creating task for {}'.format(self.downloaded_video_file))
            raise
        return parse_task


class ParseTask(Task):
    def __init__(self, input_file: str, sample_interval: int, batch_mode: bool, batch_size: int,
                 delete_after_done: bool, url: str):
        """
        :param input_file: Path of input video file
        :param sample_interval: Interval of sampling the input video, measured by number of frames
        :param delete_after_done: If True, delete the input video after the parsing is successfully done
        :param kwargs:
        """
        super(ParseTask, self).__init__()
        self.input_file = input_file
        self.sample_interval = sample_interval
        self.batch_mode = batch_mode
        self.batch_size = batch_size
        self.delete_after_done = delete_after_done
        self.parse_result = None
        self.url = url
        self._init_output_path(input_file)
    
    def _init_output_path(self, input_file: str):
        output_filename, _ = os.path.splitext(input_file)
        self.output_path = output_filename + '.pkl'
    
    def _delete_input_file(self):
        if os.path.exists(self.input_file):
            os.remove(self.input_file)
    
    def _save_parse_result(self):
        pickle.dump(self.parse_result, open(self.output_path, 'w'))

    def execute(self, pid:int=None):
        # print(f'Worker-{pid} started parsing {self.input_file}')
        try:
            video_capture = cv2.VideoCapture(self.input_file)
        except Exception:
            self.status = 'failure'
            return 1
        else:
            total_frame_count = int(video_capture.get(cv2.CAP_PROP_FRAME_COUNT))
            self.total_frame_count = total_frame_count
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
                # https://github.com/ageitgey/face_recognition/blob/master/examples/facerec_from_video_file.py
                current_frame_index = -1
                frame_indices = []
                number_of_faces_in_frames = []
                pbar = tqdm(total=total_frame_count//self.sample_interval, desc=f'Worker-{pid}', position=pid, leave=False)
                while video_capture.isOpened():
                    ret, frame = video_capture.read()
                    if not ret:
                        break
                    current_frame_index += 1
                    if (current_frame_index + 1) % self.sample_interval == 0:
                        frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                        face_locations = face_recognition.face_locations(frame)
                        num_faces = len(face_locations)
                        number_of_faces_in_frames.append(num_faces)
                        pbar.update(1)
                self.status = 'success'
                self.set_done()
                self.parse_result = (frame_indices, number_of_faces_in_frames)
                self._save_parse_result()
                pbar.close()

            video_capture.release()
            # Delete video if needed
            if self.delete_after_done:
                self._delete_input_file()
            return 0


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
    # play_list_file = 'play_lists/Life Academy_Loving on Purpose.md'
    # dl_tasks = _load_tasks_from_playlist_file(play_list_file, args=args)
    play_list_folder = 'play_lists/'
    dl_tasks = load_tasks_from_folder(play_list_folder, args)

    dl_tasks_assigned = Queue()
    dl_tasks_done = Queue()
    dl_tasks_failed = Queue()

    for t in dl_tasks:
        dl_tasks_assigned.put(t)
    num_dl_tasks = len(dl_tasks)

    parse_tasks_assigned = Queue()
    parse_tasks_done = Queue()
    parse_tasks_failed = Queue()

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

    while True:
        time.sleep(1)
        num_dl_tasks_done = dl_tasks_done.qsize()
        num_dl_tasks_failed = dl_tasks_failed.qsize()
        num_parse_tasks_done = parse_tasks_done.qsize()
        num_parse_tasks_failed = parse_tasks_failed.qsize()

        num_dl_tasks_remain = num_dl_tasks - num_dl_tasks_done - num_dl_tasks_failed
        num_parse_tasks_remain = (num_dl_tasks - num_dl_tasks_failed) - num_parse_tasks_done - num_parse_tasks_failed
        sys.stdout.write(f'\r dl tasks remain: {num_dl_tasks_remain}, done: {num_dl_tasks_done}, failed: {num_dl_tasks_failed} | parse tasks remain: {num_parse_tasks_remain}, done: {num_parse_tasks_done}, failed: {num_parse_tasks_failed}')
        sys.stdout.flush()

        if num_parse_tasks_remain == 0:
            print('='*12)
            print('All download tasks done')
            break
    
    for p in download_processes:
        p.join()
    for p in parse_processes:
        p.join()


if __name__ == '__main__':
    parser = create_parser()
    args = parser.parse_args()
    test_parse(args)
