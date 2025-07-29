# Key Conecpts

There are three central and configurable components of TaskShed:

* **Scheduler**: Acts as an API for developers to perform CRUD operations on tasks. It additionally has the responsibility of informing other components of changes (for instance if a new task has been submitted).
* **Datastore**: A data access object that stores the tasks. This could be purely in-memory datastores or a persistant storage such as `Redis` or `SQL`.
* **Worker**: The component responsible for executing the tasks. It pulls due tasks from the datastore and submits them to the event loop.

```mermaid
graph LR
  A[Scheduler] ---> B{Datastore};
  B ---> C[Worker];
  A -...-> |Notifies| C
```

