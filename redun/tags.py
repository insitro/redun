import json
import re
from typing import Any, Tuple, Union

from redun.utils import trim_string

# Built-in redun tag keys.
VERSION_KEY = "redun.version"
PROJECT_KEY = "project"
DOC_KEY = "doc"
USER_KEY = "user"

# Matches any value.
ANY_VALUE = object()


def str2literal(value_str: str) -> Union[bool, None]:
    """
    Parse a JSON literal.
    """
    lookup = {
        "true": True,
        "false": False,
        "null": None,
    }
    if value_str in lookup:
        return lookup[value_str]
    else:
        raise ValueError(f"Unknown JSON literal: {value_str}")


def parse_tag_value(value_str: str) -> Any:
    """
    Parse a tag value from the cli arguments.
    """
    # Empty string is short for None.
    if not value_str:
        return None

    # If the first character is a JSON compound type (array, object) or
    # or a string, then parse as normal json.
    if value_str[0] in ("[", "{", '"'):
        try:
            return json.loads(value_str)
        except json.JSONDecodeError as error:
            raise ValueError(f"Invalid tag value: {error}")

    # Try to automatically infer the type of the tag value.
    try:
        return int(value_str)
    except ValueError:
        pass

    try:
        return float(value_str)
    except ValueError:
        pass

    try:
        return str2literal(value_str)
    except ValueError:
        pass

    # Assume string.
    return value_str


def parse_tag_key_value(key_value: str, value_required=True) -> Tuple[str, Any]:
    """
    Parse a tag key=value pair from cli arguments.
    """
    if not key_value:
        raise ValueError("key must be specified.")

    if "=" not in key_value:
        if value_required:
            raise ValueError(f"key=value pair expected: '{key_value}'")
        return (key_value, ANY_VALUE)

    key, value = key_value.split("=", 1)
    if not key:
        raise ValueError(f"key must be specified: '{key_value}'")
    return (key, parse_tag_value(value))


def format_tag_value(value: Any) -> str:
    """
    Format a tag value.
    """
    # Simple strings (no spaces or commas or special values) can be displayed without quotes.
    if (
        isinstance(value, str)
        and not re.match(".*[ ,].*", value)
        and isinstance(parse_tag_value(value), str)
    ):
        return value
    else:
        return json.dumps(value, sort_keys=True)


def format_tag_key_value(key: str, value: Any, max_length: int = 50) -> str:
    """
    Format a tag key value pair.
    """
    key = trim_string(key, max_length=max_length)
    value_str = trim_string(format_tag_value(value), max_length=max_length)
    return f"{key}={value_str}"
