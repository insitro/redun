import os
from typing import Dict, List, Optional, TypeVar

from redun.file import Dir, File
from redun.functools import seq
from redun.task import task

T = TypeVar("T")


@task(namespace="redun", version="1", cache=False)
def debug_task(value: T) -> T:
    """
    Launch pdb debugger during one point of the redun workflow.

    For example, to inspect the lazy value `x` in the workflow below
    we can pass it through `debug_task()`:

    .. code-block:: python

        x = task1()
        x = debug_task(x)
        y = task2(x)
    """
    import pdb

    pdb.set_trace()
    return value


@task(namespace="redun", version="1", config_args=["skip_if_exists"])
def copy_file(src_file: File, dest_path: str, skip_if_exists: bool = False) -> File:
    """
    Copy a File to a new path.

    This task can help in parallelizing file copies and providing data
    provenance.
    """
    if dest_path.endswith("/"):
        # If dest_path is a directory, use the same basename as the src_file.
        dest_path = dest_path + src_file.basename()

    return src_file.copy_to(src_file.classes.File(dest_path), skip_if_exists=skip_if_exists)


@task(namespace="redun", version="1", config_args=["skip_if_exists", "copy_options"])
def copy_files(
    src_files: List[File],
    dest_paths: List[str],
    skip_if_exists: bool = False,
    copy_options: Dict = {},
) -> List[File]:
    """
    Copy multiple Files to new paths.

    File copies will be done in parallel.
    """
    return [
        copy_file.options(**copy_options)(src_file, dest_path, skip_if_exists=skip_if_exists)
        for src_file, dest_path in zip(src_files, dest_paths)
    ]


@task(namespace="redun", version="1", config_args=["skip_if_exists", "copy_options"])
def copy_dir(
    src_dir: Dir, dest_path: str, skip_if_exists: bool = False, copy_options: Dict = {}
) -> Dir:
    """
    Copy a Dir to a new path.

    File copies will be done in parallel.
    """
    src_files = list(src_dir)
    dest_dir = src_dir.classes.Dir(dest_path)
    dest_paths = [dest_dir.file(src_dir.rel_path(src_file.path)).path for src_file in src_files]
    return seq(
        [
            copy_files(
                src_files, dest_paths, skip_if_exists=skip_if_exists, copy_options=copy_options
            ),
            dest_dir,
        ]
    )[1]


@task(namespace="redun", version="1")
def render_template(
    out_path: str, template: File, context: dict, template_dir: Optional[str] = None
) -> File:
    """
    Render a jinja2 template.
    """
    try:
        import jinja2
    except ImportError:
        raise ImportError("jinja2 must be installed to use render_template()")

    if not template_dir:
        # Infer template directory from template File.
        template_dir = template.dirname()

    env = jinja2.Environment(loader=jinja2.FileSystemLoader(template_dir))
    jinja_template = env.get_template(os.path.relpath(template.path, template_dir))

    # Render template.
    out_file = File(out_path)
    out_file.write(jinja_template.render(context))
    return out_file
