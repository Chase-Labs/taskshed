# Workers

Workers are responsible for pulling data from the datastores and executing the task. TaskShed supports the following workers:

* **EventDrivenWorker**: A worker that dynamically adjusts it's polling rate depending on the submitted tasks. This is the reccomended worker as it has the lowest task execution latency - that is the difference between when you scheduled the task to run for, and when it was actually run. However, it depends on the **Scheduler** to notify it about changes.
* **PollingWorker**: A worker that periodically polls the datastore for new tasks to run. The job latency 


??? info "Task Latency"

    