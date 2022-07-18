#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""
From:
    https://github.com/flying-sheep/bcode/blob/master/bcoding.py

Main change made here is to explicitly safeguard against encoding bools as they derive from int.
Formatting and linting changes were also applied.

Original License:

Copyright (c) 2013 Phil Schaf

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

bencode/decode library.

bencoding is used in bittorrent files

use the exposed functions to encode/decode them.
"""

from collections.abc import Iterable, Mapping
from io import SEEK_CUR, BytesIO
from string import digits

try:
    str = unicode  # type: ignore
except NameError:
    pass


_TYPE_INT = b"i"
_TYPE_LIST = b"l"
_TYPE_DICT = b"d"
_TYPE_END = b"e"
_TYPE_SEP = b":"
_TYPES_STR = [d.encode() for d in digits]


def assert_btype(byte, typ):
    if not byte == typ:
        raise TypeError(
            "Tried to decode type {!r} with identifier {!r}, got identifier {!r} instead".format(
                TYPES[typ] or "End", typ, byte
            )
        )


############
# Decoding #
############


def _readuntil(f, end=_TYPE_END):
    """Helper function to read bytes until a certain end byte is hit"""
    buf = bytearray()
    byte = f.read(1)
    while byte != end:
        if byte == b"":
            raise ValueError("File ended unexpectedly. Expected end byte {}.".format(end))
        buf += byte
        byte = f.read(1)
    return buf


def _decode_int(f):
    """
    Integer types are normal ascii integers
    Delimited at the start with 'i' and the end with 'e'
    """
    assert_btype(f.read(1), _TYPE_INT)
    return int(_readuntil(f))


def _decode_buffer(f):
    """
    String types are normal (byte)strings
    starting with an integer followed by ':'
    which designates the string’s length.

    Since there’s no way to specify the byte type
    in bencoded files, we have to guess
    """
    strlen = int(_readuntil(f, _TYPE_SEP))
    buf = f.read(strlen)
    if not len(buf) == strlen:
        raise ValueError(
            "string expected to be {} bytes long but the file ended after {} bytes".format(
                strlen, len(buf)
            )
        )
    try:
        return buf.decode()
    except UnicodeDecodeError:
        return buf


def _decode_list(f):
    assert_btype(f.read(1), _TYPE_LIST)
    ret = []
    item = bdecode(f)
    while item is not None:
        ret.append(item)
        item = bdecode(f)
    return ret


def _decode_dict(f):
    assert_btype(f.read(1), _TYPE_DICT)
    ret = {}
    key = bdecode(f)
    while key is not None:
        assert isinstance(key, (str, bytes))
        ret[key] = bdecode(f)
        key = bdecode(f)
    return ret


TYPES = {
    _TYPE_INT: _decode_int,
    _TYPE_LIST: _decode_list,
    _TYPE_DICT: _decode_dict,
    _TYPE_END: None,
    # _TYPE_SEP only appears in strings, not here
}
TYPES.update(
    {byte: _decode_buffer for byte in _TYPES_STR}
)  # b'0': _decode_buffer, b'1': _decode_buffer, …


def bdecode(f_or_data):
    """
    bdecodes data by looking up the type byte,
    and using it to look up the respective decoding function,
    which in turn is used to return the decoded object

    The parameter can be a file opened in bytes mode,
    bytes or a string (the last of which will be decoded)
    """
    if isinstance(f_or_data, str):
        f_or_data = f_or_data.encode()
    if isinstance(f_or_data, bytes):
        f_or_data = BytesIO(f_or_data)

    # TODO: the following line is the only one that needs readahead.
    # peek returns a arbitrary amount of bytes, so we have to slice.
    if f_or_data.seekable():
        first_byte = f_or_data.read(1)
        f_or_data.seek(-1, SEEK_CUR)
    else:
        first_byte = f_or_data.peek(1)[:1]
    btype = TYPES.get(first_byte)
    if btype is not None:
        return btype(f_or_data)
    else:  # Used in dicts and lists to designate an end
        assert_btype(f_or_data.read(1), _TYPE_END)
        return None


############
# Encoding #
############


def _encode_int(integer, f):
    f.write(_TYPE_INT)
    f.write(str(integer).encode())
    f.write(_TYPE_END)


def _encode_buffer(string, f):
    """Writes the bencoded form of the input string or bytes"""
    if isinstance(string, str):
        string = string.encode()
    f.write(str(len(string)).encode())
    f.write(_TYPE_SEP)
    f.write(string)


def _encode_iterable(iterable, f):
    f.write(_TYPE_LIST)
    for item in iterable:
        bencode(item, f)
    f.write(_TYPE_END)


def _encode_mapping(mapping, f):
    """Encodes the mapping items in lexical order (spec)"""
    f.write(_TYPE_DICT)
    for key, value in sorted(mapping.items()):
        _encode_buffer(key, f)
        bencode(value, f)
    f.write(_TYPE_END)


def _bencode_to_file(data, f):
    # bool derives from int but we don't want to allow for encoding them because they would be
    # encoded as an int.
    if isinstance(data, int) and not isinstance(data, bool):
        _encode_int(data, f)
    elif isinstance(data, (str, bytes)):
        _encode_buffer(data, f)
    elif isinstance(data, Mapping):
        _encode_mapping(data, f)
    elif isinstance(data, Iterable):
        _encode_iterable(data, f)
    else:
        raise TypeError(
            "the passed value {} of type {} is not bencodable.".format(data, type(data).__name__)
        )


def bencode(data, f=None):
    """
    Writes a serializable data piece to f
    The order of tests is nonarbitrary,
    as strings and mappings are iterable.

    If f is None, it writes to a byte buffer
    and returns a bytestring
    """
    if f is None:
        f = BytesIO()
        _bencode_to_file(data, f)
        return f.getvalue()
    else:
        _bencode_to_file(data, f)


def main(args=None):
    """Decodes bencoded files to python syntax (like JSON, but with bytes support)"""
    import pprint
    import sys
    from argparse import ArgumentParser, FileType

    parser = ArgumentParser(description=main.__doc__)
    parser.add_argument(
        "infile",
        nargs="?",
        type=FileType("rb"),
        default=sys.stdin.buffer,
        help="bencoded file (e.g. torrent) [Default: stdin]",
    )
    parser.add_argument(
        "outfile",
        nargs="?",
        type=FileType("w"),
        default=sys.stdout,
        help="python-syntax serialization [Default: stdout]",
    )
    args = parser.parse_args(args)

    data = bdecode(args.infile)
    pprint.pprint(data, stream=args.outfile)


if __name__ == "__main__":
    main()
