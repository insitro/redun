import contextlib
import logging
import os
import random
import sys
import threading
import time
import traceback
import typing
import uuid
from collections import OrderedDict
from functools import wraps
from multiprocessing import Process, Queue
from typing import Any, Callable, Dict, Generator, List, Optional, Tuple

import psycopg2
import psycopg2.errors
import psycopg2.extras
from psycopg2 import sql

from redun.executors.base import Executor, load_task_module, register_executor
from redun.scripting import exec_script, get_task_command
from redun.task import get_task_registry
from redun.utils import pickle_dumps, pickle_loads

if typing.TYPE_CHECKING:
    from redun.scheduler import Job, Scheduler

log = logging.getLogger(__name__)

encode_obj = pickle_dumps
decode_obj = pickle_loads


def _processify(func: Callable) -> Callable:
    """Decorator to run a function as a process.
    Be sure that every argument and the return value
    is *pickable*.
    The created process is joined, so the code does not
    run in parallel.
    """
    # stolen from https://gist.github.com/schlamar/2311116

    def process_func(q, *args, **kwargs):
        try:
            ret = func(*args, **kwargs)
        except Exception:
            ex_type, ex_value, tb = sys.exc_info()
            error = ex_type, ex_value, "".join(traceback.format_tb(tb))
            ret = None
        else:
            error = None

        q.put((ret, error))

    # register original function with different name
    # in sys.modules so it is pickable
    process_func.__name__ = func.__name__ + "__processify_func"
    setattr(sys.modules[__name__], process_func.__name__, process_func)

    @wraps(func)
    def wrapper(*args, **kwargs):
        q = Queue()
        p = Process(target=process_func, args=[q] + list(args), kwargs=kwargs)
        p.start()
        ret, error = q.get()
        p.join()

        if error:
            ex_type, ex_value, tb_str = error
            message = "%s (in subprocess)\n%s" % (ex_value, tb_str)
            raise ex_type(message)

        return ret

    return wrapper


def create_queue_table(opt: Dict, table: str) -> None:
    """Create table to be used as message queue for the executor."""

    _sql = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {table} (
            id           int not null primary key generated always as identity,
            payload      bytea,
            executor_key UUID,
            created_at   timestamptz default now()
        );

        CREATE INDEX IF NOT EXISTS {idx_key}
        ON {table}(executor_key);

        CREATE INDEX IF NOT EXISTS {idx_created}
        ON {table}(created_at);
    """
    ).format(
        table=sql.Identifier(table),
        idx_key=sql.Identifier("idx__" + table + "__executor_key"),
        idx_created=sql.Identifier("idx__" + table + "__created_at"),
    )
    try:
        with get_cursor(opt) as cur:
            cur.connection.autocommit = True
            cur.execute(_sql)
    except psycopg2.errors.UniqueViolation:
        pass


@contextlib.contextmanager
def get_cursor(opt: Dict) -> Generator[psycopg2.cursor, None, None]:
    """Context manager to get a database cursor and close it after use.

    Cursor autocommit is off. If needed, turn it on yourself with
    `cursor.connection.autocommit = True`.

    Args:
        opt: The arguments passed to psycopg2.connect(**opt).

    Yields:
        cursor: Database cursor that can be used while the context is active.
    """
    conn = psycopg2.connect(**opt)
    cur = conn.cursor()
    try:
        yield cur
    finally:
        cur.close()
        conn.close()


def run_worker_single(cur: psycopg2.cursor, queue: str, result_queue: Optional[str] = None) -> int:
    """Fetch a batch of tasks from the queue and run them, optionally returning
    result on different queue.

    The function is fetched, deleted and executed under a single transaction.
    This means that if the worker crashes, the transaction removing the items
    from the queue will be automatically rolled back, so a different worker may
    retry and execute it.

    If the function code itself errors out, the error should be returned in
    place of the result, as part of the result payload. This is handled by
    `submit_encoded_tasks()`.

    Args:
        cur: database cursor to be used
        queue: name of queue table where the tasks are read from
        result_queue: if set, task results are placed on this second queue.

    Returns:
        int: the number of tasks executed in this batch, or 0 if none.
    """

    order = random.choice(["ASC", "DESC"])
    sql_begin = sql.SQL(
        """
        BEGIN;
        DELETE FROM {queue}
        USING (
            SELECT * FROM {queue}
            ORDER BY created_at {order}
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        ) q
        WHERE q.id = {queue}.id RETURNING {queue}.*;
    """
    ).format(queue=sql.Identifier(queue), order=sql.SQL(order))

    cur.execute(sql_begin)
    v = cur.fetchall()
    if v:
        for item in v:
            _pk, payload, executor_key, created_at = item
            rv = decode_and_run(_pk, payload, executor_key)
            if result_queue:
                rv = encode_obj(rv)
                submit_encoded_tasks(cur, result_queue, executor_key, [rv])
        cur.execute("COMMIT;")

    # no result - try later
    return len(v)


@contextlib.contextmanager
def fetch_results(
    cur: psycopg2.cursor, queue: str, executor_key: uuid.UUID, limit: int = 100
) -> Generator[Optional[List], None, None]:
    """Context manager for fetching results from a queue under transaction.

    The transaction that deletes messages from the queue is finalized when the
    context exists normally.

    If the code under this context crashes, the transaction is automatically
    rolled back, and the message that caused the error is put back on the
    queue.

    Args:
        cur: database cursor to be used.
        queue: name of queue table where the tasks are read from.
        executor_key: only results with this key are returned.
        limit (int, optional): Maximum number of rows to fetch in one go

    Yields:
        List[Object]: Yields a single list of results,
            or None if we can't find any.
    """
    sql_begin = sql.SQL(
        """
        BEGIN;
        DELETE FROM {queue}
        USING (
            SELECT * FROM {queue}
            WHERE executor_key = %s
            FOR UPDATE SKIP LOCKED
            LIMIT {limit}
        ) q
        WHERE q.id = {queue}.id RETURNING {queue}.*;
    """
    ).format(queue=sql.Identifier(queue), limit=sql.Literal(limit))

    cur.execute(sql_begin, (executor_key,))
    rows = cur.fetchall()
    if not rows:
        yield None
    else:
        yield [decode_obj(row[1]) for row in rows]
    cur.execute("COMMIT;")


def run_worker_until_empty(
    cur: psycopg2.cursor,
    queue: str,
    result_queue: Optional[str] = None,
    max_tasks: int = 1000,
    max_time: int = 3600,
):
    """Continuously run worker until we're out of messages.

    Args:
        cur: database cursor to be used
        queue: name of queue table where the tasks are read from
        result_queue: if set, task results are placed on this second queue.
    Returns:
        int: number of tasks finished
    """
    finished_tasks = 0
    t0 = time.time()
    while (
        (just_finished := run_worker_single(cur, queue, result_queue)) > 0
        and finished_tasks < max_tasks
        and time.time() - t0 < max_time
    ):
        finished_tasks += just_finished
    return finished_tasks


def decode_and_run(_pk: int, payload: bytes, executor_key: uuid.UUID):
    """Decode a queued payload into function and args, run the function, and
    return the results or errors.

    Args:
        _pk: Primary key of queue entry row.
        payload (bytes): The encoded contents of the task to run. Expected to
            be a pickled dict with fields `func`, `args` and `kwargs`.
        executor_key (UUID): A copy of the key to be saved in the result
            object.

    Returns:
        Dict: Object containing task metadata (under `task_args`) and function
            result (under `result`) or error (under `error`)
    """
    ret_obj = {"_pk": _pk, "size": len(payload), "executor_key": executor_key}
    try:
        obj = decode_obj(payload)
        ret_obj["task_args"] = obj

        func = obj["func"]
        args = obj.get("args", tuple())
        kw = obj.get("kw", dict())
        ret_obj["result"] = func(*args, **kw)
        return ret_obj
    except Exception as e:
        log.error("ERROR in task  id = %s err = %s", _pk, str(e))
        traceback.print_exception(*sys.exc_info())
        ret_obj["error"] = e
        return ret_obj


def encode_run_params(func: Callable, args: Tuple, kw: Dict) -> bytes:
    """Encode object in the format expected by `decode_and_run`.

    Args:
        func (Callable): function to run
        args (Tuple): Positional arguments to pass fo `func`
        kw (Dict): Keyword arguments to pass to `func`

    Returns:
        bytes: A pickled Dict containing keys `func`, `args`, `kw`
    """
    obj: Dict[str, object] = {"func": func}
    obj["args"] = args
    obj["kw"] = kw
    return encode_obj(obj)


def wait_until_notified(
    cur: psycopg2.cursor, queue: str, timeout: int = 60, extra_read_fd: Optional[int] = None
):
    """Use Postgres-specific commands LISTEN, UNLISTEN to hibernate the
    process until there is new data to be read from the table queue. To
    achieve this, we use `select` on the database connection object, to sleep
    until there is new data to be read.

    Inspired by:
    https://gist.github.com/kissgyorgy/beccba1291de962702ea9c237a900c79

    Args:
        cur (Cursor): Database cursor we use to run LISTEN/UNLISTEN
        queue: table queue name
        timeout (int, optional): Max seconds to wait. Defaults to 60.
        extra_read_fd (int, optional): FD which, if set, will be passed to
            `select` alongside the database connection. Can be used to signal
            early return, so this function can return immediately for worker
            shutdown.
    """
    import select

    chan = queue + "_channel"
    sql_listen = sql.SQL("LISTEN {chan}; COMMIT;").format(chan=sql.Identifier(chan))
    sql_unlisten = sql.SQL("UNLISTEN {chan}; COMMIT;").format(chan=sql.Identifier(chan))

    cur.execute(sql_listen)
    conn = cur.connection
    timeout = int(timeout * (0.5 + random.random()))
    if extra_read_fd:
        fds = select.select((conn, extra_read_fd), (), (), timeout)
        if extra_read_fd in fds[0]:
            os.read(extra_read_fd, 1)
    else:
        select.select((conn,), (), (), timeout)
    conn.notifies.clear()
    cur.execute(sql_unlisten)


def run_worker_forever(opt: Dict, queue: str, result_queue: Optional[str] = None):
    """Start and restart worker processes, forever.

    Args:
        opt: The arguments passed to psycopg2.connect(**opt).
        queue: name of queue table where the tasks are read from
        result_queue: if set, task results are placed on this second queue.
    """
    while True:
        try:
            _run_worker_batch(opt, queue, result_queue)
        except Exception as error:
            log.exception(error)
            log.error("error while running worker process: %s", str(error))
            time.sleep(1.0)


@_processify
def _run_worker_batch(
    opt: Dict,
    queue: str,
    result_queue: Optional[str] = None,
    tasks_before_halt: int = 1000,
    time_before_halt: int = 3600,
):
    """Run a single python worker sub-process until it either runs the
    required number of tasks, or for the required amount of time, or it
    crashes or errors out.

    Args:
        opt, queue, result_queue: same as `run_worker_forever`
        tasks_before_halt: function exists after running at least this number
            of tasks.
        time_before_halt: seconds after which the runner will stop, regardless
            of the number of completed tasks.
    """
    t0 = time.time()
    finished_tasks = 0
    with get_cursor(opt) as cur:
        while finished_tasks < tasks_before_halt and time.time() - t0 < time_before_halt:
            finished_tasks += run_worker_until_empty(
                cur, queue, result_queue, tasks_before_halt, time_before_halt
            )
            wait_until_notified(cur, queue)


def submit_encoded_tasks(
    cur: psycopg2.cursor, queue: str, executor_key: uuid.UUID, payloads: List[bytes]
):
    """Inserts some payloads into the queue, then notifies any listeners of
    that queue.

    **WARNING**: This function requires the caller to run
    `cur.execute('commit')` and finish the transaction. This is done so the
    caller can control when their own transaction finishes, without using a
    sub-transaction.

    Args:
        cur (Cursor): Database cursor we use to run INSERT and NOTIFY
        queue: table queue name
        payloads (List[bytes]): A list of the payloads to enqueue.
        executor_key: UUID to be set on the queue column

    Returns:
        List[int]: primary keys of all the inserted rows
    """
    chan = queue + "_channel"
    sql_notify = sql.SQL("NOTIFY {chan};").format(chan=sql.Identifier(chan))
    sql_insert = sql.SQL(
        """
        INSERT INTO {queue} (payload, executor_key)
        VALUES (%s, %s) RETURNING id;
    """
    )

    ids = []
    for payload in payloads:
        cur.execute(sql_insert.format(queue=sql.Identifier(queue)), (payload, executor_key))
        rv = cur.fetchone()
        assert rv
        _id = rv[0]
        ids.append(_id)
    cur.execute(sql_notify)
    return ids


def submit_task(opt: Dict, queue: str, executor_key: uuid.UUID, func: Callable, *args, **kw):
    """Submit a task `func(*args, **kw)` to queue `queue`.

    Args:
        opt: The arguments passed to psycopg2.connect(**opt).
        queue: name of queue table where the tasks are read from.
        executor_key: results can be fetched from the result queue (configured
            on the executor) using this key.
        func, args, kw: the task, executed as `func(*args, **kw)`
    """
    with get_cursor(opt) as cur:
        rv = submit_encoded_tasks(cur, queue, executor_key, [encode_run_params(func, args, kw)])[0]
        cur.execute("commit")
        return rv


def exec_task(
    job_id: int,
    module_name: str,
    task_fullname: str,
    args: Tuple,
    kwargs: dict,
    **extra,
) -> Any:
    """
    Execute a task in the worker process.
    """
    # stolen from local_executor.py
    load_task_module(module_name, task_fullname)
    task = get_task_registry().get(task_fullname)
    return task.func(*args, **kwargs)


def exec_script_task(
    job_id: int,
    module_name: str,
    task_fullname: str,
    args: Tuple,
    kwargs: dict,
    **extra,
) -> bytes:
    """
    Execute a script task from the task registry.
    """
    # stolen from local_executor.py
    load_task_module(module_name, task_fullname)
    task = get_task_registry().get(task_fullname)
    command = get_task_command(task, args, kwargs)
    return exec_script(command)


@register_executor("pg")
class PgExecutor(Executor):
    """Distributed executor using PostgreSQL tables as queues.

    Inspired by:
    https://www.crunchydata.com/blog/message-queuing-using-native-postgresql

    Configuration arguments:
    - `dsn`, `dbname`, `user`, `password`, `host`, `port`
      - passed directly to psycopg2 connection
      - can be different from redun backend database
    - `queue_args`, `queue_results`
      - override table names used as message queues for sending arguments and
        receiving results
    - `scratch_root`
    """

    DEFAULT_QUEUE_ARGS = "pg_executor__args"
    DEFAULT_QUEUE_RESULTS = "pg_executor__results"

    @staticmethod
    def _extract_psycopg2_connect_options(config: Dict):
        return dict(
            (
                (k, v)
                for (k, v) in config.items()
                if k in ["dbname", "user", "password", "host", "port", "dsn"]
            )
        )

    def __init__(
        self,
        name: str,
        scheduler: Optional["Scheduler"] = None,
        config: Optional[Dict] = None,
    ):
        super().__init__(name, scheduler=scheduler)
        config = config or dict()
        name = name or "default"
        log.info("starting worker for executor name=%s config=%s", name, config)
        self._executor_key = uuid.uuid4()
        self._scratch_root = config.get("scratch_root", "/tmp")
        self._queue_args = config.get(
            "queue_args",
            PgExecutor.DEFAULT_QUEUE_ARGS,
        )
        self._queue_results = config.get(
            "queue_results",
            PgExecutor.DEFAULT_QUEUE_RESULTS,
        )
        assert self._queue_args, "queue_args not configured"
        assert self._queue_results, "queue_results not configured"
        self._conn_opt = PgExecutor._extract_psycopg2_connect_options(config)
        assert self._conn_opt is not None, "no psycopg2 connect options given!"

        self._is_running = False
        self._pending_jobs: Dict[str, "Job"] = OrderedDict()
        self._thread: Optional[threading.Thread] = None

        self._thread_signal_read_fd: Optional[int] = None
        self._thread_signal_write_fd: Optional[int] = None

    @staticmethod
    def run_worker(config: Dict):
        """Create queue tables and start a single worker process for this
        executor, then wait for it to finish."""
        conn_opt = PgExecutor._extract_psycopg2_connect_options(config)
        queue_args = config.get(
            "queue_args",
            PgExecutor.DEFAULT_QUEUE_ARGS,
        )
        queue_results = config.get(
            "queue_results",
            PgExecutor.DEFAULT_QUEUE_RESULTS,
        )
        assert queue_args, "queue_args not configured"
        assert queue_results, "queue_results not configured"

        scratch_root = config.get("scratch_root", "/tmp")
        os.makedirs(scratch_root, exist_ok=True)
        psycopg2.extras.register_uuid()
        create_queue_table(conn_opt, queue_args)
        create_queue_table(conn_opt, queue_results)
        run_worker_forever(conn_opt, queue_args, queue_results)

    def stop(self) -> None:
        """
        Stop Executor and monitoring thread,
        and clear out queue tables.
        """
        was_running = self._is_running
        # first, turn off the monitor
        self._is_running = False
        if self._thread_signal_write_fd:
            os.write(self._thread_signal_write_fd, b"x")

        # Try to clear out the current key from the queue tables.
        if was_running:
            # Do args first, to starve the workers, then the results.
            self._clear_queue_table(self._queue_args)
            self._clear_queue_table(self._queue_results)

        if (
            self._thread
            and self._thread.is_alive()
            and threading.get_ident() != self._thread.ident
        ):
            self._thread.join()

        # Run the clear operations again, since the monitor might have done
        # more work since we initially stopped it.
        if was_running:
            # Do args first, to starve the workers, then the results.
            self._clear_queue_table(self._queue_args)
            self._clear_queue_table(self._queue_results)

    def _clear_queue_table(self, queue: str):
        """Clear out all rows from the table that have our executor key."""

        # Since we have no cancellation functionality, in-flight
        # tasks will still have locked rows in the table; so we
        # use `fetch_results()` to remove them one batch at a time.
        with get_cursor(self._conn_opt) as cur:
            empty = False
            total_count = 0
            while not empty:
                with fetch_results(cur, queue, self._executor_key) as results:
                    if results:
                        total_count += len(results)
                    else:
                        empty = True
            if total_count > 0:
                log.warning(
                    "deleted %s pending tasks from queue %s",
                    total_count,
                    queue,
                )
            # Now that we don't have extra rows hanging around, we can
            # do `delete from {table}` and it will wait on all the active
            # transactions.
            sql_delete = sql.SQL(
                """
                DELETE FROM {table} where executor_key = %s;
            """
            ).format(
                table=sql.Identifier(queue),
            )
            cur.execute(sql_delete, (self._executor_key,))
            deleted = cur.rowcount
            if deleted:
                log.warning(
                    "deleted %s running tasks from queue %s",
                    deleted,
                    queue,
                )

    def _start(self) -> None:
        """
        Start monitoring thread. Workers need to be started separately, on
        different processes, using `PgExecutor.run_worker()`.
        """
        if not self._is_running:
            os.makedirs(self._scratch_root, exist_ok=True)
            psycopg2.extras.register_uuid()
            create_queue_table(self._conn_opt, self._queue_args)
            create_queue_table(self._conn_opt, self._queue_results)

            (
                self._thread_signal_read_fd,
                self._thread_signal_write_fd,
            ) = os.pipe()
            self._is_running = True
            self._thread = threading.Thread(
                target=self._monitor,
                daemon=False,
            )
            self._thread.start()

    def _monitor(self) -> None:
        """
        Thread for monitoring task ack. Uses single long-running database
        connection.
        """
        assert self._scheduler

        try:
            with get_cursor(self._conn_opt) as cur:
                while self._is_running:
                    while self._monitor_one(cur):
                        pass
                    if self._is_running:
                        wait_until_notified(
                            cur,
                            self._queue_results,
                            extra_read_fd=self._thread_signal_read_fd,
                        )

        except Exception as error:
            self._scheduler.reject_job(None, error)

        self.stop()

    def _monitor_one(self, cur: psycopg2.cursor):
        """Run a single batch of task monitoring.

        Args:
            cur (Cursor): Database cursor to use for fetching results.

        Returns:
            bool: True if we found something on the queue, meaning the caller
                should immediately run this function again.
        """
        assert self._scheduler

        with fetch_results(cur, self._queue_results, self._executor_key) as results:
            if not results:
                return False

            for result in results:
                assert (
                    result["executor_key"] == self._executor_key
                ), "wrong executor key for result!"
                job_id = result["task_args"]["kw"]["job_id"]
                job = self._pending_jobs.pop(job_id)

                if "error" in result:
                    self._scheduler.reject_job(job, result["error"])
                elif "result" in result:
                    self._scheduler.done_job(job, result["result"])
                else:
                    raise RuntimeError("monitor: invalid result: " + str(result))

            return True

    def _submit(self, exec_func: Callable, job: "Job") -> None:
        assert job.args
        self._start()

        args, kwargs = job.args
        self._pending_jobs[job.id] = job
        submit_task(
            self._conn_opt,
            self._queue_args,
            self._executor_key,
            exec_func,
            job_id=job.id,
            module_name=job.task.load_module,
            task_fullname=job.task.fullname,
            args=args,
            kwargs=kwargs,
        )

    def submit(self, job: "Job") -> None:
        assert not job.task.script
        self._submit(exec_task, job)

    def submit_script(self, job: "Job") -> None:
        assert job.task.script
        self._submit(exec_script_task, job)

    def scratch_root(self) -> str:
        return self._scratch_root
