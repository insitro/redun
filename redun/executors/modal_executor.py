import typing
import queue
import modal
from typing import Optional, Callable
import threading
from redun.config import create_config_section
from redun.executors.base import Executor, register_executor
from redun.scripting import exec_script
from redun.task import get_task_registry

if typing.TYPE_CHECKING:
    from redun.scheduler import Job, Scheduler


def exec_script_func(func: Callable) -> Callable:
    def wrapper(*args, **kwargs):
        return exec_script(func(*args, **kwargs))

    return wrapper


def queue_wrapper(func: Callable) -> Callable:
    def wrapper(id: str, queue: modal.Queue, *args, **kwargs):
        try:
            print("calling func")
            res = func(*args, **kwargs)
            print("putting success")
            queue.put((id, "success"))
            return res
        except Exception:
            print("putting error")
            queue.put((id, "error"))
            raise

    return wrapper


@register_executor("modal")
class ModalExecutor(Executor):
    """
    A redun Executor for running jobs on Modal.
    """

    def __init__(
        self,
        name: str,
        scheduler: Optional["Scheduler"] = None,
        config=None,
    ):
        super().__init__(name, scheduler=scheduler)

        # Parse config.
        if not config:
            config = create_config_section()

        self.finished_setup = False

        self.modal_function_calls = {}

        self.jobs = {}
        self.stopped = False

    def start(self) -> None:
        self.image = modal.Image.debian_slim().pip_install("redun")
        self.modal_app = modal.App(f"redun-modal-job-{self.name}", image=self.image)

        self.functions = {}
        for t in get_task_registry():
            task_options = t.get_task_options()

            if task_options.get("executor") == "modal":
                modal_args = task_options.get("modal_args", {})
                modal_args["image"] = modal_args.get("image", self.image)
                modal_args["name"] = modal_args.get("name", t.fullname)
                modal_args["serialized"] = True

                func = queue_wrapper(exec_script_func(t.func) if t.script else t.func)

                self.functions[t.fullname] = self.modal_app.function(**modal_args)(func)

        self.output_ctx = modal.enable_output()
        self.output_ctx.__enter__()
        self.run_ctx = self.modal_app.run()
        self.run_ctx.__enter__()

        self.queue_ctx = modal.Queue.ephemeral()
        self.queue = self.queue_ctx.__enter__()

        self.result_thread = threading.Thread(target=self.result_thread_func)
        self.result_thread.start()
        self.finished_setup = True

    def result_thread_func(self):
        while not self.stopped:
            try:
                print("getting results")
                res = self.queue.get_many(100, timeout=1)
                print(f"got results {len(res)}")
                for job_id, status in res:
                    job = self.jobs[job_id]
                    try:
                        return_value = self.modal_function_calls[job_id].get()
                    except Exception as e:
                        return_value = e

                    if status == "success":
                        self._scheduler.done_job(job, return_value)
                    else:
                        self._scheduler.reject_job(job, return_value)
            except queue.Empty:
                continue

    def stop(self):
        print("stopping")
        self.stopped = True
        self.result_thread.join()
        self.run_ctx.__exit__(None, None, None)
        self.output_ctx.__exit__(None, None, None)
        self.queue_ctx.__exit__(None, None, None)

    def _submit(self, job: "Job") -> None:
        assert job.args
        if not self.finished_setup:
            self.start()

        args, kwargs = job.args

        try:
            self.jobs[job.id] = job
            modal_exec_func = self.functions[job.task.fullname]
        except KeyError:
            raise ValueError(f"No modal function found for task {job.task.fullname}")

        self.modal_function_calls[job.id] = modal_exec_func.spawn(
            job.id, self.queue, *args, **kwargs
        )

    def submit(self, job: "Job") -> None:
        assert not job.task.script
        self._submit(job)

    def submit_script(self, job: "Job") -> None:
        assert job.task.script
        self._submit(job)

    def scratch_root(self) -> str:
        pass
