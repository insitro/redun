# Computational model

Here, we review the computational model of redun and the semantics of workflow execution. These details help to define what is correct behavior when implementing redun.

## The redun Expression Graph language

redun uses expression graphs to represent workflows. We can describe the structure of those graphs using the following [formal grammar](https://en.wikipedia.org/wiki/Formal_grammar):

```
# An expression, e, can be one of these forms:
e = TaskExpression(task_name, args=(e, ...), kwargs={arg_key: e, ...}, options)
  | SchedulerExpression(task_name, args=(e, ...), kwargs={arg_key: e, ...}, options)
  | SimpleExpression(op_name, (e, ...), {e, ...})
  | nested_value
  | concrete_value

# Nested values are container types that redun recurses into to evaluate:
nested_value = [e, ...]
             | {e: e, ...}
             | (e, ...)
             | {e, ...}
             | named_tuple_cls(e, ...)
             | dataclass_cls(e, ...)

options = {key: e, ...}
  where key is an option name

named_tuple_cls = User defined NamedTuple
dataclass_cls = User defined dataclass

task_name = Task name string
arg_key = Task argument name string

op_name = "add"
        | "mult"
        | "div"
        | ... name of any supported Python operator (e.g. `__getitem__`, `__call__`)

concrete_value = Any other python value (e.g. 10, "hello", pd.DataFrame(...))
```

## Evaluation process

Workflow execution is carried out by recursively evaluating Expression Graphs using the following [Graph Reduction](https://en.wikipedia.org/wiki/Graph_reduction) rules:

```
eval(expr, parent_job) =
  Evaluate an expression expr within Job parent_job

exec(func, args, kwargs, options) =
  Execute the function func with arguments (args and kwargs) and execution configuration options

# Main expression evaluations:
eval(TaskExpression(task_name, args, kwargs, expr_options), p) => eval(exec(
    func=f,
    args=eval(args, p),
    kwargs=eval(default_args(t, args, kwargs), options_job) | eval(kwargs, p),
    options=j.exec_options
  ), j)
  where f = function associated with task t with name task_name
        j = Job(expression=TaskExpression(...), task=t, parent_job=p)
        options_job = Job(parent_job=p, context=j.context)
        p = parent job of the TaskExpression or None if this the root Expression
        expr_options = option overrides specific to this TaskExpression
        default_args(t, args, kwargs) = default arguments of task t that are not specified
          by args and kwargs
        j.exec_options, j.context = see next section

eval(SchedulerExpression(task_name, args, kwargs, options), p) => eval(f(scheduler, p, s, *args, **kwargs), p)
  where scheduler = redun scheduler
        s = SchedulerExpression(task_name, args, kwargs, options)
        p = parent job of the SchedulerExpression
        options = configuration to customize the execution environment of f

eval(SimpleExpression(op_name, args, kwargs), p) = op(*args, **kwargs)
  where op = an operation associated with operation name `op_name`, such as
             `add`, `sub`, `__call__`, `__getitem__`, etc.
        p = parent job of the SimpleExpression

eval(ValueExpression(concrete_value), p) => concrete_value
   where p = parent job of the ValueExpression

# Evaluation of nested values:
eval([a, b, ...], p) => [eval(a, p), eval(b, p), ...]
eval({a: b, c: d, ...}, p) => {eval(a, p): eval(b, p), eval(c, p): eval(d, p), ...}
eval((a, b, ...), p) => (eval(a, p), eval(b, p), ...)
eval({a, b, ...}, p) => {eval(a, p), eval(b, p), ...}
eval(named_tuple_cls(a, b, ...), p) => named_tuple_cls(eval(a, p), eval(b, p), ...)
eval(dataclass_cls(a, b, ...), p) => dataclass_cls(eval(a, p), eval(b, p), ...)

# Concrete values evaluate to themselves:
eval(concrete_value, p) => concrete_value
```

Noteworthy design choices:
- We only create new Job `j` for evaluating TaskExpressions. We do not create new Jobs for evaluating the other expression types. This design choice was made to record provenance at an appropriate level of detail.
- We propagate the parent jobs through the evaluation in order to facilitate CallGraph recording and to pass down environment like data to child Jobs.
- When evaluating a `SchedulerExpression`, we pass the positional arguments `args` and keywords arguments `kwargs` unevaluated into `f`. This allows `f` to customize how to evaluate the arguments, if at all.
- Default arguments of a task can be expressions themselves (see `eval(default_args(t, args, kwargs), options_job)`) and they are evaluated prior to passing to the underlying function `f`. This allows chaining extra logic just before task execution

    ```py
    @task
    def my_task(x, y=another_task()):
        # Both x and y will be concrete at this point.
        # ...
    ```


### Task option evaluation and overriding

[Task options](../tasks.md#task-options) allow customization of Job execution (e.g. memory, cpu, executor, caching, etc). These options can be defined in a number of places. In general, more local options definitions override more globally defined options. Here, we define the specific options overriding semantics:

```
# Final options used when executing a Job:
job.exec_options = job.executor_options  # Options defined in the Job's Executor config in `redun.ini`
                 | job.eval_options      

# Evaluated options for the Job. This is needed since job.options might contain expressions.
job.eval_options = eval(job.options, job.parent_job)

# Job options prior to considering executor options.
job.options =
    job.task.options        # Options defined in the @task() decorator
  | job.inherited_options   # Exported options from the parent Job.
  | job.expression.options  # Options defined at call-time using my_task.options(opt=X)(arg1, arg2)
  | scheduler_options       # Options defined by the Scheduler or CLI (e.g. `redun run --no-cache`)

# Options that inherit from the parent Job, if it exists.
job.inherited_options = {
  k: v for k, v in job.parent_job.options.items()
  if k in job.parent_job.exported_options
} if job.parent_job exists else {}

# Jobs export options defined by the union of parent job, task, and expression.
# Here we determine the option names to export.
job.exported_options = (job.parent_job.exported_options if job.parent_job exists else set())
                     U job.expression.exported_options
                     U job.task.exported_options

# Job context inherits from parent Job with overrides from Expression.
job.context =
  job.parent_job.context | job.eval_options["_context_override"]; if job.parent_job exists
  config_context | scheduler_context                            ; otherwise

config_context = Context defined in the `redun.ini` configuration file.
scheduler_context = Context defined on the CLI (`--context x=10`) or Scheduler (`Scheduler.run(expr, context={"x": 10})`)
```

Noteworthy design choices:
- Options can be expressions as well and are evaluated prior to their use (see `eval(job.options, job.parent_job)`). This allows further run-time configuration:

  ```py
  @task(memory=get_memory_task())
  def my_task():
      # ...
  ```

- [Context](../context.md) is passed from parent to child Jobs and is modified by expression options (`update_context()` specifically performs this modification).

  ```py
  @task(memory=get_context("memory"))
  def my_task(x, y=get_context("y")):
      # pass

  result = my_task.update_context(memory=2, y=10)(9)
  ```

  - In order for `get_context("y")` in the default argument to pick up the modification to `y` with `update_context()`, we need to evaluate the options using a special parent job `options_job` that uses the context from `job`.
