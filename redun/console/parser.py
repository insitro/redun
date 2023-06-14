import argparse
import io
from argparse import Namespace
from typing import List, Optional
from unittest.mock import patch


def parse_args(parser: argparse.ArgumentParser, argv: List[str] = []) -> Namespace:
    """
    Parse an argv into an argparse.Namespace.
    """
    # argparse frequently tries to write to stderr, textual throws an exception
    # if anyone writes to stderr. We use a patch here to intercept writes to stderr.
    stderr = io.StringIO()
    with patch("sys.stderr", stderr):
        args, extra_args = parser.parse_known_args(argv)
    stderr.seek(0)
    return args


def get_parser_action(parser: argparse.ArgumentParser, key: str) -> Optional[argparse.Action]:
    """
    Returns the associated ArgumentParser Action for the given Namespace key.
    """
    for option in parser._option_string_actions.values():
        if option.dest == key:
            return option
    return None


def format_args(parser: argparse.ArgumentParser, args: Namespace) -> List[str]:
    """
    Format a Namespace back into an argv.
    """
    argv = []

    for key, value in args.__dict__.items():
        action = get_parser_action(parser, key)
        if isinstance(action, argparse._StoreAction) and value != action.default:
            argv.extend([action.option_strings[0], str(value)])

        elif isinstance(action, argparse._StoreTrueAction):
            if value:
                argv.append(action.option_strings[0])

        elif isinstance(action, argparse._AppendAction) and value != action.default:
            for val in value:
                argv.extend([action.option_strings[0], str(val)])

    return argv
