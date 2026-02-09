import json
import os
import shlex
import shutil
import subprocess
import sys
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from threading import Lock, Thread
from typing import Any, Deque, Dict, Iterable, Iterator, List, Optional, Tuple, cast

import sqlalchemy as sa
from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session, joinedload

from redun.backends.db import Argument, CallNode, Execution, Job, RedunBackendDb, Task, Value
from redun.backends.db.query import REDUN_ERROR_TYPE_NAME
from redun.cli import setup_scheduler
from redun.task import split_task_fullname

DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 200
RUN_LOG_MAX_LINES = 10_000


def _format_datetime(value: Optional[datetime]) -> Optional[str]:
    return value.isoformat() if value else None


def _format_duration_seconds(
    start_time: Optional[datetime], end_time: Optional[datetime]
) -> Optional[float]:
    if not (start_time and end_time):
        return None
    return (end_time - start_time).total_seconds()


def _parse_execution_args(args: Optional[str]) -> List[str]:
    if not args:
        return []
    try:
        values = json.loads(args)
    except (TypeError, json.JSONDecodeError):
        return shlex.split(args)

    if isinstance(values, list):
        return [str(value) for value in values]
    return [str(values)]


def _calc_job_status(
    result_type: Optional[str], end_time: Optional[datetime], cached: bool
) -> str:
    if result_type == REDUN_ERROR_TYPE_NAME:
        return "FAILED"
    if end_time is None:
        return "RUNNING"
    if cached:
        return "CACHED"
    return "DONE"


def _calc_execution_status(job_status: str) -> str:
    if job_status in {"DONE", "CACHED"}:
        return "DONE"
    return job_status


def _serialize_tags(tags: Iterable[Any]) -> List[Dict[str, Any]]:
    return [{"key": tag.key, "value": tag.value_parsed} for tag in tags]


def _normalize_page_size(page_size: int) -> int:
    if page_size < 1:
        return DEFAULT_PAGE_SIZE
    return min(page_size, MAX_PAGE_SIZE)


def _job_status_clause(status: str):
    if status == "RUNNING":
        return Job.end_time.is_(None)
    if status == "CACHED":
        return Job.cached.is_(True)
    if status == "FAILED":
        return Value.type == REDUN_ERROR_TYPE_NAME
    if status == "DONE":
        return sa.and_(
            Job.end_time.is_not(None),
            Job.cached.is_(False),
            sa.or_(Value.type.is_(None), Value.type != REDUN_ERROR_TYPE_NAME),
        )
    raise HTTPException(status_code=400, detail=f"Unsupported status filter: {status}")


def _execution_status_clause(status: str):
    if status == "DONE":
        return sa.or_(_job_status_clause("DONE"), _job_status_clause("CACHED"))
    return _job_status_clause(status)


def _resolve_redun_command() -> List[str]:
    redun_binary = shutil.which("redun")
    if redun_binary:
        return [redun_binary]

    repo_root = Path(__file__).resolve().parents[2]
    script_path = repo_root / "bin" / "redun"
    if script_path.exists():
        return [sys.executable, str(script_path)]

    raise RuntimeError("Cannot find redun executable in PATH or local bin/redun script.")


def _tree_sort_jobs(root_id: str, jobs: List["JobSummary"]) -> List["JobSummary"]:
    job2children: Dict[Optional[str], List[JobSummary]] = defaultdict(list)
    for job in jobs:
        job2children[job.parent_id].append(job)

    job_index = {job.id: job for job in jobs}
    ordered: List[JobSummary] = []

    def walk(current: JobSummary, depth: int) -> None:
        current.depth = depth
        ordered.append(current)
        for child in job2children[current.id]:
            walk(child, depth + 1)

    root = job_index.get(root_id)
    if root:
        walk(root, 0)

    seen_ids = {item.id for item in ordered}
    remaining = [job for job in jobs if job.id not in seen_ids]
    for job in remaining:
        if job.parent_id is None:
            walk(job, 0)

    return ordered


@dataclass
class RunRecord:
    run_id: str
    execution_id: str
    command: List[str]
    cwd: str
    status: str = "PENDING"
    pid: Optional[int] = None
    return_code: Optional[int] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    log_lines: Deque[str] = field(default_factory=lambda: deque(maxlen=RUN_LOG_MAX_LINES))
    process: Optional[subprocess.Popen] = None

    def serialize(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "execution_id": self.execution_id,
            "status": self.status,
            "pid": self.pid,
            "return_code": self.return_code,
            "command": self.command,
            "cwd": self.cwd,
            "created_at": _format_datetime(self.created_at),
            "started_at": _format_datetime(self.started_at),
            "ended_at": _format_datetime(self.ended_at),
        }


class RunManager:
    def __init__(self) -> None:
        self._lock = Lock()
        self._runs: Dict[str, RunRecord] = {}

    def create(self, command: List[str], execution_id: str, cwd: str) -> RunRecord:
        run_id = str(uuid.uuid4())
        record = RunRecord(
            run_id=run_id,
            execution_id=execution_id,
            command=command,
            cwd=cwd,
        )
        with self._lock:
            self._runs[record.run_id] = record
        return record

    def get(self, run_id: str) -> RunRecord:
        with self._lock:
            record = self._runs.get(run_id)
            if not record:
                raise KeyError(run_id)
            return record

    def list(self, limit: int = 20) -> List[RunRecord]:
        with self._lock:
            runs = sorted(self._runs.values(), key=lambda record: record.created_at, reverse=True)
        return runs[:limit]

    def start(self, record: RunRecord) -> None:
        thread = Thread(target=self._run_subprocess, args=(record,), daemon=True)
        thread.start()

    def cancel(self, run_id: str) -> RunRecord:
        record = self.get(run_id)
        process = record.process
        if process and process.poll() is None:
            process.terminate()
            with self._lock:
                record.status = "CANCELLED"
                record.ended_at = datetime.utcnow()
        return record

    def logs(self, run_id: str, offset: int = 0) -> Tuple[List[str], int]:
        record = self.get(run_id)
        lines = list(record.log_lines)
        if offset < 0:
            offset = 0
        return lines[offset:], len(lines)

    def _append_line(self, record: RunRecord, line: str) -> None:
        with self._lock:
            record.log_lines.append(line.rstrip("\n"))

    def _run_subprocess(self, record: RunRecord) -> None:
        try:
            process = subprocess.Popen(
                record.command,
                cwd=record.cwd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                env={**os.environ, "PYTHONUNBUFFERED": "1"},
            )
        except Exception as error:
            with self._lock:
                record.status = "FAILED"
                record.ended_at = datetime.utcnow()
                record.log_lines.append(f"Failed to start process: {error}")
            return

        with self._lock:
            record.process = process
            record.pid = process.pid
            record.status = "RUNNING"
            record.started_at = datetime.utcnow()

        assert process.stdout
        for line in process.stdout:
            self._append_line(record, line)

        return_code = process.wait()
        with self._lock:
            if record.status != "CANCELLED":
                record.status = "SUCCEEDED" if return_code == 0 else "FAILED"
            record.return_code = return_code
            record.ended_at = datetime.utcnow()
            record.process = None


class JobSummary(BaseModel):
    id: str
    execution_id: str
    parent_id: Optional[str]
    task_fullname: Optional[str]
    status: str
    cached: bool
    start_time: Optional[datetime]
    end_time: Optional[datetime]
    duration_seconds: Optional[float]
    depth: int = 0


class RunRequest(BaseModel):
    script: str
    task: str
    argv: List[str] = Field(default_factory=list)
    config: Optional[str] = None
    repo: Optional[str] = None
    execution_id: Optional[str] = None
    no_cache: bool = False
    tags: List[str] = Field(default_factory=list)
    user: Optional[str] = None
    project: Optional[str] = None
    doc: Optional[str] = None
    cwd: Optional[str] = None


class LogResponse(BaseModel):
    offset: int
    next_offset: int
    lines: List[str]


def create_app(
    config_dir: Optional[str] = None,
    repo: str = "default",
    poll_interval_seconds: int = 3,
) -> FastAPI:
    scheduler = setup_scheduler(config_dir=config_dir, repo=repo, migrate_if_local=True)
    backend = cast(RedunBackendDb, scheduler.backend)
    if not backend.session:
        raise RuntimeError("redun backend session is not initialized")

    run_manager = RunManager()
    static_dir = Path(__file__).resolve().parent / "static"
    app = FastAPI(title="redun server", version="0.1.0")

    def get_session() -> Iterator[Session]:
        session = backend.Session()
        try:
            yield session
        finally:
            session.close()

    @app.get("/api/health")
    def health() -> Dict[str, Any]:
        return {"ok": True, "poll_interval_seconds": poll_interval_seconds}

    @app.get("/api/executions")
    def list_executions(
        id_prefix: Optional[str] = None,
        namespace: Optional[str] = None,
        find: Optional[str] = None,
        status: List[str] = Query(default=[]),
        page: int = 1,
        page_size: int = DEFAULT_PAGE_SIZE,
        session: Session = Depends(get_session),
    ) -> Dict[str, Any]:
        page = max(page, 1)
        page_size = _normalize_page_size(page_size)

        query = (
            session.query(Execution, Job, Task.fullname, Value.type)
            .join(Job, Execution.job_id == Job.id)
            .outerjoin(CallNode, Job.call_hash == CallNode.call_hash)
            .outerjoin(Value, CallNode.value_hash == Value.value_hash)
            .join(Task, Job.task_hash == Task.hash)
        )

        if id_prefix:
            query = query.filter(Execution.id.like(id_prefix + "%"))
        if namespace == "":
            query = query.filter(Task.namespace == "")
        elif namespace:
            query = query.filter(Task.namespace.like(namespace + "%"))
        elif find:
            query = query.filter(
                Task.namespace.like("%" + find + "%") | Execution.args.like("%" + find + "%")
            )

        if status:
            clauses = [_execution_status_clause(item.upper()) for item in status]
            query = query.filter(sa.or_(*clauses))

        rows = (
            query.order_by(Job.start_time.desc())
            .offset((page - 1) * page_size)
            .limit(page_size + 1)
            .all()
        )
        has_more = len(rows) > page_size
        rows = rows[:page_size]

        executions = []
        for execution, root_job, task_fullname, result_type in rows:
            job_status = _calc_job_status(result_type, root_job.end_time, root_job.cached)
            executions.append(
                {
                    "id": execution.id,
                    "task_fullname": task_fullname,
                    "status": _calc_execution_status(job_status),
                    "start_time": _format_datetime(root_job.start_time),
                    "end_time": _format_datetime(root_job.end_time),
                    "duration_seconds": _format_duration_seconds(
                        root_job.start_time, root_job.end_time
                    ),
                    "updated_time": _format_datetime(execution.updated_time),
                    "args": _parse_execution_args(execution.args),
                }
            )

        return {
            "executions": executions,
            "page": page,
            "page_size": page_size,
            "has_more": has_more,
        }

    @app.get("/api/executions/{execution_id}")
    def get_execution(execution_id: str, session: Session = Depends(get_session)) -> Dict[str, Any]:
        execution = (
            session.query(Execution)
            .options(joinedload(Execution.job).joinedload(Job.task), joinedload(Execution.tags))
            .filter(Execution.id == execution_id)
            .first()
        )
        if not execution or not execution.job:
            raise HTTPException(status_code=404, detail=f"Unknown execution: {execution_id}")

        root_result_type = (
            session.query(Value.type)
            .select_from(Job)
            .outerjoin(CallNode, Job.call_hash == CallNode.call_hash)
            .outerjoin(Value, CallNode.value_hash == Value.value_hash)
            .filter(Job.id == execution.job.id)
            .scalar()
        )
        job_status = _calc_job_status(root_result_type, execution.job.end_time, execution.job.cached)

        job_rows = (
            session.query(Task.fullname, Job.end_time, Job.cached, Value.type)
            .select_from(Job)
            .join(Task, Job.task_hash == Task.hash)
            .outerjoin(CallNode, Job.call_hash == CallNode.call_hash)
            .outerjoin(Value, CallNode.value_hash == Value.value_hash)
            .filter(Job.execution_id == execution_id)
            .all()
        )
        counts: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        totals = defaultdict(int)
        for task_fullname, end_time, cached, result_type in job_rows:
            status = _calc_job_status(result_type, end_time, cached)
            counts[task_fullname][status] += 1
            counts[task_fullname]["TOTAL"] += 1
            totals[status] += 1
            totals["TOTAL"] += 1

        return {
            "id": execution.id,
            "status": _calc_execution_status(job_status),
            "task_fullname": execution.job.task.fullname if execution.job.task else None,
            "start_time": _format_datetime(execution.job.start_time),
            "updated_time": _format_datetime(execution.updated_time),
            "end_time": _format_datetime(execution.job.end_time),
            "duration_seconds": _format_duration_seconds(
                execution.job.start_time, execution.job.end_time
            ),
            "args": _parse_execution_args(execution.args),
            "tags": _serialize_tags(execution.tags),
            "job_counts": {
                "totals": dict(totals),
                "by_task": {
                    task_fullname: dict(task_counts)
                    for task_fullname, task_counts in counts.items()
                },
            },
        }

    @app.get("/api/executions/{execution_id}/jobs")
    def list_execution_jobs(
        execution_id: str,
        page: int = 1,
        page_size: int = DEFAULT_PAGE_SIZE,
        status: List[str] = Query(default=[]),
        task: List[str] = Query(default=[]),
        focus_job_id: Optional[str] = None,
        session: Session = Depends(get_session),
    ) -> Dict[str, Any]:
        execution = session.get(Execution, execution_id)
        if not execution or not execution.job:
            raise HTTPException(status_code=404, detail=f"Unknown execution: {execution_id}")

        page = max(page, 1)
        page_size = _normalize_page_size(page_size)

        query = (
            session.query(Job, Value.type)
            .outerjoin(CallNode, Job.call_hash == CallNode.call_hash)
            .outerjoin(Value, CallNode.value_hash == Value.value_hash)
            .options(joinedload(Job.task))
            .filter(Job.execution_id == execution_id)
        )

        show_tree = not task and not status and not focus_job_id

        if task:
            task_clauses = []
            for task_fullname in task:
                namespace, name = split_task_fullname(task_fullname)
                task_clauses.append(sa.and_(Task.namespace == namespace, Task.name == name))
            query = query.join(Task, Job.task_hash == Task.hash).filter(sa.or_(*task_clauses))
            show_tree = False

        if status:
            clauses = [_job_status_clause(item.upper()) for item in status]
            query = query.filter(sa.or_(*clauses))
            show_tree = False

        if focus_job_id:
            query = query.filter(sa.or_(Job.id == focus_job_id, Job.parent_id == focus_job_id))
            show_tree = False

        rows = query.order_by(Job.start_time).all()
        jobs = [
            JobSummary(
                id=job.id,
                execution_id=job.execution_id,
                parent_id=job.parent_id,
                task_fullname=job.task.fullname if job.task else None,
                status=_calc_job_status(result_type, job.end_time, job.cached),
                cached=job.cached,
                start_time=job.start_time,
                end_time=job.end_time,
                duration_seconds=_format_duration_seconds(job.start_time, job.end_time),
            )
            for job, result_type in rows
        ]

        if show_tree:
            jobs = _tree_sort_jobs(execution.job.id, jobs)

        total = len(jobs)
        start = (page - 1) * page_size
        end = start + page_size
        page_jobs = jobs[start:end]

        return {
            "jobs": [job.dict() for job in page_jobs],
            "page": page,
            "page_size": page_size,
            "total": total,
            "has_more": end < total,
        }

    @app.get("/api/jobs/{job_id}")
    def get_job(job_id: str, session: Session = Depends(get_session)) -> Dict[str, Any]:
        job = (
            session.query(Job)
            .options(
                joinedload(Job.task),
                joinedload(Job.tags),
                joinedload(Job.call_node)
                .joinedload(CallNode.arguments)
                .joinedload(Argument.value),
                joinedload(Job.call_node).joinedload(CallNode.value),
            )
            .filter(Job.id == job_id)
            .first()
        )
        if not job:
            raise HTTPException(status_code=404, detail=f"Unknown job: {job_id}")

        result_type = (
            session.query(Value.type)
            .select_from(Job)
            .outerjoin(CallNode, Job.call_hash == CallNode.call_hash)
            .outerjoin(Value, CallNode.value_hash == Value.value_hash)
            .filter(Job.id == job_id)
            .scalar()
        )

        arguments = []
        if job.call_node:
            ordered_args = sorted(
                job.call_node.arguments,
                key=lambda arg: (arg.arg_position if arg.arg_position is not None else -1),
            )
            for arg in ordered_args:
                arguments.append(
                    {
                        "name": arg.arg_key if arg.arg_key is not None else arg.arg_position,
                        "is_keyword": arg.arg_key is not None,
                        "preview": repr(arg.value.preview),
                    }
                )

        result_preview = None
        if job.call_node and job.call_node.value:
            result_preview = repr(job.call_node.value.preview)

        return {
            "id": job.id,
            "execution_id": job.execution_id,
            "parent_id": job.parent_id,
            "task_fullname": job.task.fullname if job.task else None,
            "status": _calc_job_status(result_type, job.end_time, job.cached),
            "cached": job.cached,
            "start_time": _format_datetime(job.start_time),
            "end_time": _format_datetime(job.end_time),
            "duration_seconds": _format_duration_seconds(job.start_time, job.end_time),
            "tags": _serialize_tags(job.tags),
            "arguments": arguments,
            "result_preview": result_preview,
        }

    @app.post("/api/runs")
    def create_run(request: RunRequest) -> Dict[str, Any]:
        execution_id = request.execution_id or str(uuid.uuid4())
        command = _resolve_redun_command()
        if request.config:
            command += ["-c", request.config]
        if request.repo:
            command += ["--repo", request.repo]
        command += ["run"]
        if request.no_cache:
            command += ["--no-cache"]
        if request.user:
            command += ["--user", request.user]
        if request.project:
            command += ["--project", request.project]
        if request.doc:
            command += ["--doc", request.doc]
        for tag in request.tags:
            command += ["--tag", tag]
        command += [
            request.script,
            request.task,
            "--execution-id",
            execution_id,
        ]
        command += list(request.argv)

        cwd = request.cwd or os.getcwd()
        record = run_manager.create(command=command, execution_id=execution_id, cwd=cwd)
        run_manager.start(record)
        return record.serialize()

    @app.get("/api/runs")
    def list_runs(limit: int = 20) -> Dict[str, Any]:
        limit = max(1, min(limit, 100))
        return {"runs": [record.serialize() for record in run_manager.list(limit=limit)]}

    @app.get("/api/runs/{run_id}")
    def get_run(run_id: str) -> Dict[str, Any]:
        try:
            return run_manager.get(run_id).serialize()
        except KeyError as error:
            raise HTTPException(status_code=404, detail=f"Unknown run: {run_id}") from error

    @app.get("/api/runs/{run_id}/logs")
    def get_run_logs(run_id: str, offset: int = 0) -> LogResponse:
        try:
            lines, next_offset = run_manager.logs(run_id, offset=offset)
        except KeyError as error:
            raise HTTPException(status_code=404, detail=f"Unknown run: {run_id}") from error

        return LogResponse(offset=offset, next_offset=next_offset, lines=lines)

    @app.post("/api/runs/{run_id}/cancel")
    def cancel_run(run_id: str) -> Dict[str, Any]:
        try:
            record = run_manager.cancel(run_id)
        except KeyError as error:
            raise HTTPException(status_code=404, detail=f"Unknown run: {run_id}") from error
        return record.serialize()

    @app.get("/", include_in_schema=False)
    def serve_index() -> FileResponse:
        return FileResponse(static_dir / "index.html")

    app.mount("/", StaticFiles(directory=static_dir), name="static")
    return app


def create_app_from_env() -> FastAPI:
    config_dir = os.environ.get("REDUN_SERVER_CONFIG") or None
    repo = os.environ.get("REDUN_SERVER_REPO", "default")
    poll_interval = int(os.environ.get("REDUN_SERVER_POLL_INTERVAL", "3"))
    return create_app(config_dir=config_dir, repo=repo, poll_interval_seconds=poll_interval)
