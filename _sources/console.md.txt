---
tocpdeth: 3
---

# Console (TUI)

redun has a [Text-based User Interface (TUI)](https://en.wikipedia.org/wiki/Text-based_user_interface) called Console, which provides a convenient way to interactively explore the [data provenance](design.md#provenance-and-call-graphs) recorded by past redun executions.

For example, running the command

```sh
redun console
```

will bring up the redun Console showing all the past Executions recorded in the redun database repo:

<a href="_static/console-executions.svg"><img src="_static/console-executions.svg"></a>

This provides similar information as the `redun log` command, but also supports interactive exploration using keyboard navigation (e.g. arrow keys, cycling focus using tab) and mouse (e.g. clicking, dragging, mouse wheel). This is made possible by the powerful [Textual](https://github.com/textualize/textual/) Python library.

## Console walk-through

To highlight some of the key features of the Console, we'll explore the data provenance recorded by executing several of the [workflow examples](https://github.com/insitro/redun/tree/main/examples) provided in the redun source repo.

### Executions screen

As we showed above, the Console can be started using the command `redun console`. The first screen is a list of the past Executions.

<a href="_static/console-executions.svg"><img src="_static/console-executions.svg"></a>

Let's review some features on this first screen. First, on the bottom, we have a list of actions we can take along with hotkeys for invoking them. For example, pressing the `q` key will quit the Console application. Another important key, the `Escape` key, acts like a web browser back button as we navigate to other screens in the Console app. The `m` hotkey provides a menu of the top-level screens of the Console.

<a href="_static/console-menu.svg"><img src="_static/console-menu.svg"></a>

### Execution screen

Selecting an Execution in the executions list will take us to a screen providing information specific to one Execution. Here, we see the provenance recorded by running the [web scraping example](https://github.com/insitro/redun/tree/main/examples/scraping).

<a href="_static/console-execution.svg"><img src="_static/console-execution.svg"></a>

**Header.** First, notice that the header has changed to `redun console executions/5174a516-dfaf-4b81-978c-b6ec948160a6`. This a valid shell command that can be used to return to this specific screen in the Console app. You can think of it like a URL for your terminal. For example, you could [copy](https://github.com/Textualize/textual/blob/main/FAQ.md#how-can-i-select-and-copy-text-in-a-textual-app) this command and share it with collaborators who have access to the same redun repo.

**Execution details.** Below the header, we have high-level information about the Execution, such as the start time, duration, and original command-line arguments. We also see the *Tags* that redun has automatically attached to the Execution, such as the related github repo, redun version, and user name. Some tags, such as `git_commit`, `git_origin_url`, and `aws_batch_job`, will automatically be rendered as links to Github, AWS Console, etc.

**Job status table.** Next, we see an overview of the Execution's Jobs organized by task and status (e.g. DONE, FAILED, CACHED). In addition to providing a quick overview of the Jobs, it provides Job filtering. Clicking and pressing enter on any cell of the table allows filtering to jobs matching that task and/or status.

<a href="_static/console-execution-filter-jobs.svg"><img src="_static/console-execution-filter-jobs.svg"></a>

**Job tree.** Lastly, on the bottom half of the screen we see the Execution's Job tree. We can see which tasks were executed along with a brief summary of the argument values. The `n` and `p` hotkeys can be used to move through *next* and *previous pages* in the Job tree. Pressing `Enter` on any Job will navigate the to the *Job screen*.

**Filter commands.** You may have noticed that filtering jobs updated the header's command line options, such as `--status` and `--task`. Using the `/` hotkey, these filters can be edited directly. Many of the screens in Console provide this hotkey.

<a href="_static/console-execution-filter.svg"><img src="_static/console-execution-filter.svg"></a>

### Job screen

Selecting a specific job, we navigate to the Job screen.

<a href="_static/console-job.svg"><img src="_static/console-job.svg"></a>

**Header.** Similar to the Execution screen, the header provides basic information of the Job. In particular the `Trackback:` line allows quick navigation to the Execution, parent Job, and child Jobs. Job and Execution-specific tags are also displayed.

**CallNode, arguments, result.** In this section, we see details on the actual arguments provided in this Job and well as the return value. Inspecting these values can be very helpful in the debugging process. Clicking on these values will navigate to the *Value screen*, which provides value-specific details.

**Task.** Lastly, we have a rendering of the original Task for the Job. Clicking on the Task link will navigate to the *Task screen*, where all versions of the Task can be inspected.

**REPL.** One common need when inspecting a job is to more deeply inspect the Job's arguments or result. This can be done by pressing the `r` hotkey, which will navigate to the built-in [Read-Eval-Print-Loop (REPL)](https://en.wikipedia.org/wiki/Read%E2%80%93eval%E2%80%93print_loop) screen. The `r` hotkey is provided on many screens, and pressing it will inject local variables into the REPL specific to the originating screen. In this case, we will inject the `job`, `args`, `kwargs`, and `result` values (see below).

### REPL screen

The REPL screen provides a [Read-Eval-Print-Loop (REPL)](https://en.wikipedia.org/wiki/Read%E2%80%93eval%E2%80%93print_loop) for inspecting data provenance directly in a Python REPL. redun uses [SQLAlchemy](https://www.sqlalchemy.org/) to lazily load Python values from the redun database into memory.

<a href="_static/console-repl.svg"><img src="_static/console-repl.svg"></a>

Values used in the workflow will deserialize from the database using pickle and will default to pickle previews (mock-like objects) for any value with an unknown class (i.e. not currently importable).

**Builtin classes and functions.** There are several classes and functions automatically imported into the local environment to ease inspection. Some of the most helpful ones are summarized during REPL startup, such as `query()` and `console()`. `query()` provides access to the SQLAlchemy ORM for navigating the Call Graph models in the redun database. `console()` provides an easy way to navigating to the screen of a specific model, such as Execution, Job, Task, etc.

## Conclusion

In summary, we have gone through a quick walk-through of the redun Console and how it can be used for inspecting data provenance and debugging workflow executions. There are many more screens that allow inspecting Files, Values, and more that can be found in the main menu (hotkey `m`) as well as navigating between screens via links. Overall, the app acts similar a web app by providing a header that updates with a shell command for navigating directly back to any screen, much like a web page URL.