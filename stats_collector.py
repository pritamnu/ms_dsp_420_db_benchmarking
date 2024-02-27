import functools
import psutil
import pandas as pd
import time
import os
import threading
import docker
import queue
import subprocess


REPORT_DIR = "report"
DB_DIR = os.path.join(REPORT_DIR, "%s")


class StatCollectorThread(threading.Thread):
    """Thread class with a stop() method. The thread itself has to check
    regularly for the stopped() condition."""

    def __init__(self, *args, **kwargs):
        self._stop_event = threading.Event()
        self.process = args[0]
        self.stats = args[2]
        self.test = args[1]
        super(StatCollectorThread, self).__init__(**kwargs)

    def stop(self):
        self._stop_event.set()

    def run(self, *args, **kwargs):
        stats = []
        while True:
            if self.stopped():
                print("Exiting %s" % self.ident)
                self.stats.put(stats)
                return
            with self.process.oneshot():
                io_data = self.process.io_counters()
                kpis = {
                    "pid": self.process.pid,
                    "epoch": time.time(),
                    "test": self.test,
                    "name": self.process.name(),
                    "cpu_per": self.process.cpu_percent(),
                    "mem_per": self.process.memory_percent(),
                    "read_count": io_data.read_count,
                    "write_count": io_data.write_count,
                    "read_bytes": io_data.read_bytes,
                    "write_bytes": io_data.write_bytes,
                    "other_count": io_data.other_count,
                    "other_bytes": io_data.other_bytes
                }
                stats.append(kpis)
            time.sleep(0.1)

    def stopped(self):
        return self._stop_event.is_set()

class ContainerStatCollector(threading.Thread):
    def __init__(self, *args, **kwargs):
        self._stop_event = threading.Event()
        self.client = args[0]
        self.db = args[1]
        self.test = args[2]
        self.stats = {
            "cpu_usage": [],
            "memory_usage": [],
            # "block_io": [],
        }
        super(ContainerStatCollector, self).__init__(**kwargs)

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()

    def run(self):
        stats = []
        while True:
            # cmd = "docker stats self.client.id --no-stream"
            if self.stopped():
                # print(stats)
                print("Exiting container stats collection {}".format(self.client.id))
                test_report = os.path.join(DB_DIR % self.db, self.test + '.csv')
                os.makedirs(DB_DIR % self.db, exist_ok=True)
                # if not os.path.exists(DB_DIR % self.db):
                os.makedirs(DB_DIR % self.db, exist_ok=True)
                columns = None
                data = []
                for stat in stats:
                    columns = [col.strip() for col in stat.split('\n')[0].split('  ') if col]
                    print(columns)
                    data.append([data.strip() for data in stat.split('\n')[1].split('  ') if data])
                    print(data)
                pd.DataFrame(data, columns=columns).to_csv(test_report, index=False)
                return
            ps = subprocess.Popen(["docker", "stats", self.client.id, "--no-stream"], stdout=subprocess.PIPE, text=True)
            out, _ = ps.communicate()
            stats.append(out)
            # if self.stopped():
            #     print("Exiting container stats collection {}".format(self.client.id))
            #     test_report = os.path.join(DB_DIR % self.db, self.test + '.csv')
            #     # if not os.path.exists(DB_DIR % self.db):
            #     os.makedirs(DB_DIR % self.db, exist_ok=True)
            #     print(self.stats)
            #     pd.DataFrame(self.stats).to_csv(test_report, index=False)
            #     return
            # stats = self.client.stats(stream=False)
            # # cpu stats
            # print(stats)
            # # cpu_usage = stats['cpu_stats']['cpu_usage']['total_usage']- stats['precpu_stats']['cpu_usage']['total_usage']
            # # cpu_system = stats['cpu_stats']['system_cpu_usage'] - stats['precpu_stats']['system_cpu_usage']
            # # cpu_per = (cpu_usage / cpu_system) * (stats["cpu_stats"]["online_cpus"]) / 100
            # self.stats["cpu_usage"].append(stats['cpu_stats']['cpu_usage']['total_usage'])
            # self.stats["memory_usage"].append(stats['memory_stats']['usage'])



# def find_procs_by_name(name):
#     """Return a list of processes matching 'name'."""
#     ls = []
#     for p in psutil.process_iter(["name", "exe", "cmdline"]):
#         if name == p.info['name'] or \
#                 p.info['exe'] and os.path.basename(p.info['exe']) == name or \
#                 p.info['cmdline'] and p.info['cmdline'][0] == name:
#             ls.append(p)
#     return ls


def collect_stats(func):
    functools.wraps(func)

    def wrapper(*args, **kwargs):
        db_obj = args[0]
        test_name = func.__name__
        client = docker.from_env()
        container = client.containers.list(all=False)[0]
        print("Started collecting stats for container {}".format(container.id))
        # stats = container.stats(stream=False)
        th = ContainerStatCollector(container,
                               db_obj.process_name,
                               test_name)
        th.start()
        time.sleep(5)
        start_time = time.time()
        func(*args, **kwargs)
        exec_time = time.time() - start_time
        time.sleep(5)
        th.stop()
        th.join()
        test_report = os.path.join(DB_DIR % db_obj.process_name, test_name + '.csv')
        df = pd.read_csv(test_report)

        # process the columns
        df['exec_time'] = exec_time
        rename_map = {"CONTAINER ID": "container_id",
                      "NAME": "container_name",
                      "CPU %": "cpu",
                      "MEM %": "mem",
                      "PIDS": "pid_count"
                     }
        df = df.rename(columns=rename_map)
        df.cpu = df.cpu.apply(lambda x: x.strip('%'))
        df.mem = df.mem.apply(lambda x: x.strip('%'))
        df.to_csv(test_report, index=False)

    return wrapper

# NOTE: For local process based status collection
# def collect_stats(func):
#     functools.wraps(func)
#
#     def wrapper(*args, **kwargs):
#         db_obj = args[0]
#         test_name = func.__name__
#         proc_lst = find_procs_by_name(db_obj.process_name)
#         thread_lst = []
#         print("Starting stats collection for %s process" % len(proc_lst))
#         for proc in proc_lst:
#             th_data = {
#                 'id': proc.pid,
#                 'test': test_name,
#                 'name': proc.name,
#                 'th': None,
#                 'stats': queue.Queue()
#             }
#             th = StatCollectorThread(proc, th_data['test'], th_data['stats'])
#             th_data['th'] = th
#             th.start()
#             thread_lst.append(th_data)
#         print("Started stats collection with: %s" % thread_lst)
#         start = time.time()
#         func(*args, **kwargs)
#         exec_time = time.time() - start
#         time.sleep(exec_time)  # Measure the status till 1 min after the query is over
#         print("Stopping stats collection")
#         for th in thread_lst:
#             th['th'].stop()
#             th['th'].join()
#         final_stats = pd.DataFrame()
#         for th in thread_lst:
#             th_stats = th['stats'].get()
#             print("stats collected by thread: ", th['id'], len(th_stats))
#             print('-' * 25)
#             th_report = pd.DataFrame(th_stats)
#             th_report['exec_time'] = exec_time
#             final_stats = pd.concat([final_stats, th_report], ignore_index=True)
#         test_report = os.path.join(DB_DIR % db_obj.__class__.__name__, test_name + '.csv')
#         if not os.path.exists(DB_DIR % db_obj.__class__.__name__):
#             os.makedirs(DB_DIR % db_obj.__class__.__name__)
#         print("Saving results of test [%s] to file [%s]" % (test_name, test_report))
#         final_stats.to_csv(test_report, index=False)
#     return wrapper
