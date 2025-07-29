import asyncio
from datetime import datetime, timezone
from statistics import mean, stdev

from ._benchmark_observer import BenchmarkObserver


class ExecutionLagObserver(BenchmarkObserver):
    def __init__(self, num_tasks: int):
        self.num_tasks = num_tasks
        self.execution_lag = []
        self.task_ids = set()
        self.start_time = datetime.now(timezone.utc)

    async def callback(
        self, scheduled_run_time: datetime, scheduled_task_id: str | int
    ):
        calltime = datetime.now(tz=timezone.utc)
        if isinstance(scheduled_run_time, str):
            scheduled_run_time = datetime.fromisoformat(scheduled_run_time)

        self.execution_lag.append((calltime - scheduled_run_time).total_seconds())
        self.task_ids.add(scheduled_task_id)
        if len(self.execution_lag) >= self.num_tasks:
            self.print_results()
            asyncio.get_running_loop().stop()

    def print_results(self):
        execution_time = datetime.now(tz=timezone.utc) - self.start_time
        print(
            f"\nCollected {len(self.execution_lag)} samples, from {len(self.task_ids)} unqiue tasks."
        )
        print(f"Mean lag {mean(self.execution_lag):.6f}s")
        print(f"Min lag {min(self.execution_lag):.6f}s")
        print(f"Max lag {max(self.execution_lag):.6f}s")
        print(f"Stddev lag {stdev(self.execution_lag):.6f}s")
        print(f"Total execution time: {execution_time.total_seconds():.5f}s\n")
