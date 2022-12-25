"""Sphinx documentation plugin used to document redun tasks.

Introduction
============

Usage
-----

Add the extension to your :file:`docs/conf.py` configuration module:

.. code-block:: python

    extensions = (...,
                  'redun.sphinx')

If you'd like to change the prefix for tasks in reference documentation
then you can change the ``redun_task_prefix`` configuration value:

.. code-block:: python

    redun_task_prefix = '(task)'  # < default

With the extension installed `autodoc` will automatically find
task decorated objects (e.g. when using the automodule directive)
and generate the correct (as well as add a ``(task)`` prefix),
and you can also refer to the tasks using `:task:proj.tasks.add`
syntax.

Use ``.. autotask::`` to alternatively manually document a task.
"""
# This technique is inspired by:
# https://docs.celeryq.dev/en/latest/_modules/celery/contrib/sphinx.html

from inspect import formatargspec, getfullargspec
from typing import Any, List, Optional

from docutils import nodes  # type: ignore
from sphinx.domains.python import PyFunction
from sphinx.ext.autodoc import FunctionDocumenter
from sphinx.util.docstrings import prepare_docstring
from sphinx.util.inspect import getdoc

from redun.task import SchedulerTask, Task


class TaskDocumenter(FunctionDocumenter):
    """Document redun task definitions."""

    objtype = "task"
    member_order = 11

    @classmethod
    def can_document_member(cls, member, membername, isattr, parent) -> bool:
        # This Documenter documents objects of type Task.
        return isinstance(member, Task) and not isinstance(member, SchedulerTask)

    def format_args(self, **kwargs: Any) -> str:
        """
        Returns a string documenting the task's parameters.
        """
        wrapped = self.object.func
        if wrapped is not None:
            argspec = getfullargspec(wrapped)
            fmt = formatargspec(*argspec)
            fmt = fmt.replace("\\", "\\\\")
            return fmt
        return ""

    def get_doc(self, ignore: int = None) -> List[List[str]]:
        """
        Returns the formatted docstring for the task.
        """
        docstring = getdoc(
            self.object.func,
            self.get_attr,
            self.config.autodoc_inherit_docstrings,
            self.parent,
            self.object_name,
        )
        if docstring:
            tab_width = self.directive.state.document.settings.tab_width
            return [prepare_docstring(docstring, tab_width)]
        return []

    def document_members(self, all_members=False):
        pass

    def check_module(self) -> bool:
        # Normally checks if *self.object* is really defined in the module
        # given by *self.modname*. But since functions decorated with the @task
        # decorator are instances living in the redun.task, we have to check
        # the wrapped function instead.
        if self.object.func.__module__ == self.modname:
            return True
        return super().check_module()


class TaskDirective(PyFunction):
    """Sphinx task directive."""

    def get_signature_prefix(self, sig: str) -> List[nodes.Node]:
        return [nodes.Text(self.env.config.redun_task_prefix)]


class SchedulerTaskDocumenter(TaskDocumenter):
    """Document redun scheduler_task definitions."""

    objtype = "scheduler_task"
    member_order = 11

    @classmethod
    def can_document_member(cls, member, membername, isattr, parent) -> bool:
        # This Documenter documents objects of type Task.
        return isinstance(member, SchedulerTask)

    def format_args(self, **kwargs: Any) -> str:
        """
        Returns a string documenting the task's parameters.
        """
        wrapped = self.object.func
        if wrapped is not None:
            argspec = getfullargspec(wrapped)
            # Remove SchedulerTask-specific internal parameters.
            del argspec.args[:3]

            # Unwrap the Promise return value.
            argspec.annotations["return"] = argspec.annotations["return"].__parameters__[0]

            fmt = formatargspec(*argspec)
            fmt = fmt.replace("\\", "\\\\")
            return fmt
        return ""


class SchedulerTaskDirective(PyFunction):
    """Sphinx scheduler_task directive."""

    def get_signature_prefix(self, sig: str) -> List[nodes.Node]:
        return [nodes.Text(self.env.config.redun_scheduler_task_prefix)]


def autodoc_skip_member_handler(app, what, name, obj, skip, options) -> Optional[bool]:
    """Handler for autodoc-skip-member event."""
    # redun tasks created with the @task decorator have the property
    # that *obj.__doc__* and *obj.__class__.__doc__* are equal, which
    # trips up the logic in sphinx.ext.autodoc that is supposed to
    # suppress repetition of class documentation in an instance of the
    # class. This overrides that behavior.
    if isinstance(obj, Task):
        if skip:
            return False
    return None


def setup(app):
    """Setup Sphinx extension."""
    app.setup_extension("sphinx.ext.autodoc")

    app.add_autodocumenter(TaskDocumenter)
    app.add_directive_to_domain("py", "task", TaskDirective)

    app.add_autodocumenter(SchedulerTaskDocumenter)
    app.add_directive_to_domain("py", "scheduler_task", SchedulerTaskDirective)

    app.add_config_value("redun_task_prefix", "(task)", True)
    app.add_config_value("redun_scheduler_task_prefix", "(scheduler_task)", True)
    app.connect("autodoc-skip-member", autodoc_skip_member_handler)

    return {"parallel_read_safe": True}
