class TaskNotFoundError(Exception):
    """
    Raised when a task is not found in the data store.
    """

    pass


class IncorrectCallbackNameError(Exception):
    """
    Raised when a task's callback is not found in the worker's callback map.
    """

    pass
