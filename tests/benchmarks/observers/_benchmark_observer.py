from abc import ABC, abstractmethod


class BenchmarkObserver(ABC):
    """
    A class responsible for:

    1. Providing a callback during benchmark execution
    2. Tracking metrics
    3. Printing results
    """

    @abstractmethod
    async def callback(self, *args, **kwargs): ...

    @abstractmethod
    def print_results(self): ...
