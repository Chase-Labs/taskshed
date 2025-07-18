import asyncio
import resource
from time import perf_counter

from ._benchmark_observer import BenchmarkObserver

COMPLETED = 0
MAX_CONCURRECY = 0
CURRENT_CONCURRENCY = 0
START_TIMESTAMP = None


class HighConcurrencyObserver(BenchmarkObserver):
    def __init__(self, num_tasks: int, cpu_start: float, mem_start: float):
        self.num_tasks = num_tasks
        self.cpu_start = cpu_start
        self.mem_start = mem_start
        self.completed = 0
        self.current_concurrency = 0
        self.max_concurrency = 0
        self.start_timestamp = None

    async def callback(self):
        if self.start_timestamp is None:
            self.start_timestamp = perf_counter()

        self.current_concurrency += 1
        self.max_concurrency = max(self.max_concurrency, self.current_concurrency)
        # global COMPLETED, MAX_CONCURRECY, CURRENT_CONCURRENCY, START_TIMESTAMP
        # if START_TIMESTAMP is None:
        #     START_TIMESTAMP = perf_counter()

        # CURRENT_CONCURRENCY += 1
        # MAX_CONCURRECY = max(MAX_CONCURRECY, CURRENT_CONCURRENCY)

        # ---------------------------------------- simulate work
        await asyncio.sleep(0.5)
        await asyncio.sleep(0.1)

        list(range(1000))

        await asyncio.sleep(0.2)
        await asyncio.sleep(0.3)

        list(range(1000))

        await asyncio.sleep(0.1)

        # ---------------------------------------- end of work
        self.current_concurrency -= 1
        self.completed += 1
        if self.completed >= self.num_tasks:
            # CURRENT_CONCURRENCY -= 1
            # COMPLETED += 1
            # if COMPLETED >= self.num_tasks:
            usage = resource.getrusage(resource.RUSAGE_SELF)
            self.cpu_time = usage.ru_utime + usage.ru_stime - self.cpu_start
            self.mem_kb = usage.ru_maxrss - self.mem_start
            self.print_results()
            asyncio.get_running_loop().stop()

    def print_results(self):
        # print(f"\nTotal duration: {perf_counter() - START_TIMESTAMP:.5f}s")
        # print(f"Total tasks completed: {COMPLETED}")
        # print(f"Max concurrency observed: {MAX_CONCURRECY}")

        print(f"\nTotal duration: {perf_counter() - self.start_timestamp}s")
        print(f"Total tasks completed: {self.completed}")
        print(f"Max concurrency observed: {self.max_concurrency}")
        print(f"CPU time used: {self.cpu_time:.5f}s")
        print(f"Additional max RSS: {self.mem_kb} KB")
