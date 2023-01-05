import sys
from typing import Any, Optional

_current_namespace = ""


def namespace(_namespace=None):
    """
    Set the current task namespace.

    Use None to unset the current namespace.
    """
    global _current_namespace
    _current_namespace = _namespace or ""


def get_current_namespace():
    """
    Returns the current task namespace during import.
    """
    return _current_namespace


def compute_namespace(obj: Any, namespace: Optional[str] = None):
    """Compute the namespace for the provided object.

    Precedence:
    - Explicit namespace provided (note: an empty string is a valid explicit value)
    - Infer it from a `redun_namespace` variable in the same module as func
    - The current namespace, configured with `set_current_namespace`

    WARNING: This computation interacts with the global "current namespace" (see above),
    so results may be context dependent.
    """

    # Determine task namespace.
    if namespace is None:
        namespace = getattr(sys.modules[obj.__module__], "redun_namespace", None)

    if namespace is not None:
        return namespace
    return get_current_namespace()
