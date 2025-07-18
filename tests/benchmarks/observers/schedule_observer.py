import asyncio
from statistics import mean, stdev

from ._benchmark_observer import BenchmarkObserver


class ScheduleObserver(BenchmarkObserver):
    def __init__(self, num_tasks: int) -> None:
        self.num_tasks = num_tasks
        self.durations: list[float] = []

    async def callback(self, *args, **kwargs):
        return None

    def record(self, duration: float) -> None:
        self.durations.append(duration)

    def print_results(self) -> None:
        avg_time = mean(self.durations)
        min_time = min(self.durations)
        max_time = max(self.durations)
        stddev_time = stdev(self.durations)
        print(f"\nScheduled {self.num_tasks}")
        print(f"Average time: {avg_time:.6f}s")
        print(f"Minimum time: {min_time:.6f}s")
        print(f"Maximum time: {max_time:.6f}s")
        print(f"Standard deviation: {stddev_time:.6f}s")
        print(f"Time per task: {avg_time / self.num_tasks:.6f}s\n")
        asyncio.get_running_loop().stop()
