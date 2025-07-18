import asyncio
from statistics import mean, stdev
from time import perf_counter

from ._benchmark_observer import BenchmarkObserver


class OverheadObserver(BenchmarkObserver):
    def __init__(self, num_tasks: int):
        self.num_tasks = num_tasks
        self.task_ids = set()
        self.overhead_data = []

    async def callback(self, scheduled_task_id: str | int, delay: float):
        t0 = perf_counter()
        await asyncio.sleep(delay)
        self.overhead_data.append((perf_counter() - t0) - delay)
        self.task_ids.add(scheduled_task_id)
        if len(self.overhead_data) >= self.num_tasks:
            self.print_results()
            asyncio.get_running_loop().stop()

    def print_results(self):
        print(
            f"\nCollected {len(self.overhead_data)} samples, from {len(self.task_ids)} tasks."
        )
        print(f"Mean overhead: {mean(self.overhead_data)}")
        print(f"Min overhead: {min(self.overhead_data)}")
        print(f"Max overhead: {max(self.overhead_data)}")
        print(f"Stddev overhead: {stdev(self.overhead_data)}\n")
