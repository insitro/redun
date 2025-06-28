```mermaid

graph LR

    Workflow_Orchestrator["Workflow Orchestrator"]

    Scheduler["Scheduler"]

    Task_Definition["Task Definition"]

    Expression_Handling["Expression Handling"]

    Promise_Management["Promise Management"]

    RedunBackendDb["RedunBackendDb"]

    Executors["Executors"]

    Config["Config"]

    File_System["File System"]

    RedunClient["RedunClient"]

    Workflow_Orchestrator -- "Composes" --> Scheduler

    Workflow_Orchestrator -- "Composes" --> Task_Definition

    Workflow_Orchestrator -- "Composes" --> Expression_Handling

    Workflow_Orchestrator -- "Composes" --> Promise_Management

    Workflow_Orchestrator -- "Persists state to" --> RedunBackendDb

    Workflow_Orchestrator -- "Delegates task execution to" --> Executors

    Workflow_Orchestrator -- "Is configured by" --> Config

    Workflow_Orchestrator -- "Interacts with" --> File_System

    Workflow_Orchestrator -- "Initiated and monitored by" --> RedunClient

    Scheduler -- "Part of" --> Workflow_Orchestrator

    Scheduler -- "Orchestrates" --> Task_Definition

    Scheduler -- "Processes" --> Expression_Handling

    Scheduler -- "Manages" --> Promise_Management

    Scheduler -- "Persists state to" --> RedunBackendDb

    Scheduler -- "Delegates execution to" --> Executors

    Scheduler -- "Is configured by" --> Config

    Scheduler -- "Interacts with" --> File_System

    Task_Definition -- "Part of" --> Workflow_Orchestrator

    Task_Definition -- "Managed by" --> Scheduler

    Task_Definition -- "Can be represented as" --> Expression_Handling

    Expression_Handling -- "Part of" --> Workflow_Orchestrator

    Expression_Handling -- "Processed by" --> Scheduler

    Expression_Handling -- "Involves" --> Task_Definition

    Promise_Management -- "Part of" --> Workflow_Orchestrator

    Promise_Management -- "Used by" --> Scheduler

    Promise_Management -- "Related to" --> Task_Definition

    RedunBackendDb -- "Persists state for" --> Workflow_Orchestrator

    RedunBackendDb -- "Persists state for" --> Scheduler

    RedunBackendDb -- "Accessed by" --> RedunClient

    Executors -- "Used by" --> Scheduler

    Executors -- "Interacts with" --> File_System

    Executors -- "Configured by" --> Config

    Config -- "Configures" --> Workflow_Orchestrator

    Config -- "Configures" --> Scheduler

    Config -- "Configures" --> Executors

    Config -- "Accessed by" --> RedunClient

    File_System -- "Used by" --> Workflow_Orchestrator

    File_System -- "Used by" --> Scheduler

    File_System -- "Used by" --> Executors

    File_System -- "Used by" --> RedunClient

    RedunClient -- "Initiates and monitors" --> Workflow_Orchestrator

    RedunClient -- "Accesses workflow state for logging and visualization" --> Scheduler

    RedunClient -- "Initializes and retrieves system configurations" --> Config

    RedunClient -- "Performs database-specific operations" --> RedunBackendDb

    RedunClient -- "Reads and writes workflow-related files" --> File_System

    click Workflow_Orchestrator href "https://github.com/insitro/redun/blob/main/.codeboarding//Workflow_Orchestrator.md" "Details"

```



[![CodeBoarding](https://img.shields.io/badge/Generated%20by-CodeBoarding-9cf?style=flat-square)](https://github.com/CodeBoarding/GeneratedOnBoardings)[![Demo](https://img.shields.io/badge/Try%20our-Demo-blue?style=flat-square)](https://www.codeboarding.org/demo)[![Contact](https://img.shields.io/badge/Contact%20us%20-%20contact@codeboarding.org-lightgrey?style=flat-square)](mailto:contact@codeboarding.org)



## Details



Detailed analysis of the Redun workflow orchestration system, outlining its key components and their interdependencies.



### Workflow Orchestrator [[Expand]](./Workflow_Orchestrator.md)

The core intelligence of Redun, responsible for defining, scheduling, and managing the execution of tasks within a computational graph. It handles dependency resolution, caching logic, and asynchronous operations, acting as the central coordinator for workflow execution. This component is fundamentally composed of the Scheduler, Task Definition, Expression Handling, and Promise Management components.





**Related Classes/Methods**:



- <a href="https://github.com/insitro/redun/redun/scheduler.py#L881-L2298" target="_blank" rel="noopener noreferrer">`redun.scheduler.Scheduler` (881:2298)</a>

- <a href="https://github.com/insitro/redun/redun/task.py#L141-L564" target="_blank" rel="noopener noreferrer">`redun.task.Task` (141:564)</a>

- <a href="https://github.com/insitro/redun/redun/expression.py#L38-L111" target="_blank" rel="noopener noreferrer">`redun.expression.Expression` (38:111)</a>

- <a href="https://github.com/insitro/redun/redun/promise.py#L6-L187" target="_blank" rel="noopener noreferrer">`redun.promise.Promise` (6:187)</a>





### Scheduler

The core engine within the Workflow Orchestrator, specifically responsible for managing the execution of tasks, tracking job states, resolving dependencies, and interacting with various executors. It maintains the overall state of active and historical workflows.





**Related Classes/Methods**:



- <a href="https://github.com/insitro/redun/redun/scheduler.py#L881-L2298" target="_blank" rel="noopener noreferrer">`redun.scheduler.Scheduler` (881:2298)</a>





### Task Definition

Defines the atomic units of work within a Redun workflow. Tasks encapsulate Python functions and their associated metadata, allowing them to be tracked, cached, and executed within the computational graph.





**Related Classes/Methods**:



- <a href="https://github.com/insitro/redun/redun/task.py#L141-L564" target="_blank" rel="noopener noreferrer">`redun.task.Task` (141:564)</a>





### Expression Handling

Manages the representation and evaluation of the computational graph. Expressions define how tasks and values are combined and depend on each other, forming the basis for dependency resolution and execution planning.





**Related Classes/Methods**:



- <a href="https://github.com/insitro/redun/redun/expression.py#L38-L111" target="_blank" rel="noopener noreferrer">`redun.expression.Expression` (38:111)</a>





### Promise Management

Provides a mechanism for handling asynchronous results and deferred computations within the workflow. Promises allow the scheduler to manage dependencies efficiently, enabling parallel execution and proper sequencing of tasks.





**Related Classes/Methods**:



- <a href="https://github.com/insitro/redun/redun/promise.py#L6-L187" target="_blank" rel="noopener noreferrer">`redun.promise.Promise` (6:187)</a>





### RedunBackendDb

The database backend component responsible for persisting Redun's workflow metadata, task results, execution logs, and other internal states. It provides an interface for storing and retrieving this critical information.





**Related Classes/Methods**:



- `redun.backends.db.RedunBackendDb` (1:1)





### Executors

A set of components responsible for executing Redun tasks in various environments (e.g., local, AWS Batch, Docker, Kubernetes). Each executor provides an interface for running tasks and managing their lifecycle within its specific execution context.





**Related Classes/Methods**:



- <a href="https://github.com/insitro/redun/redun/executors/base.py#L12-L73" target="_blank" rel="noopener noreferrer">`redun.executors.base.Executor` (12:73)</a>





### Config

Manages the configuration settings for the entire Redun system. This includes database connection details, executor configurations, and other global parameters that influence Redun's behavior.





**Related Classes/Methods**:



- <a href="https://github.com/insitro/redun/redun/config.py#L38-L150" target="_blank" rel="noopener noreferrer">`redun.config.Config` (38:150)</a>





### File System

An abstraction layer for handling file operations across different storage systems (e.g., local disk, AWS S3, Google Cloud Storage, Azure Blob Storage). It ensures that Redun can seamlessly interact with various data sources and sinks.





**Related Classes/Methods**:



- <a href="https://github.com/insitro/redun/redun/file.py#L210-L397" target="_blank" rel="noopener noreferrer">`redun.file.FileSystem` (210:397)</a>





### RedunClient

The primary command-line interface (CLI) client for Redun. It acts as the main entry point for users to execute various Redun commands, such as running tasks, managing configurations, inspecting workflow history, and interacting with the backend.





**Related Classes/Methods**:



- <a href="https://github.com/insitro/redun/redun/cli.py#L864-L3192" target="_blank" rel="noopener noreferrer">`redun.cli.RedunClient` (864:3192)</a>









### [FAQ](https://github.com/CodeBoarding/GeneratedOnBoardings/tree/main?tab=readme-ov-file#faq)