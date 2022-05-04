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
