import multiprocessing as mp
import queue


class Task:
    def __init__(task_type: str, url: str):
        self.task_type = task_type
        self.url = url
        self.done = False
        self.status = None

    def set_done():
        self.done = True

class DownloadTask(Task):
    def __init__(self, download_path:str, **kwargs):
        super(DownloadTask, self).__init__(**kwargs)
        self.download_path = download_path

class ParseTask(Task):
    def __init__(self, output_path: str, **kwargs):
        super(ParseTask, self).__init__(**kwargs)
        self.output_path = output_path


def download_worker(tasks_assigned, tasks_done):
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


def init_download_tasks(input_path: str) -> List[Task]:
    pass


def main():
    dl_tasks_assigned = mp.Queue()
    dl_tasks_done = mp.Queue()

    parse_tasks_assigned = mp.Queue()
    parse_tasks_done = mp.Queue()

    download_tasks = init_download_tasks(input_path)
    for t in download_tasks:
        dl_tasks_assigned.put(t)

    pass