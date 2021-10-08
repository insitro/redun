"""
From:
    https://github.com/flying-sheep/bcode/blob/master/test.py

Main modification was to remove the __main__ block as we run this as part of our test suite and
not as an isolated test as was done in bcode. A test for the modified encoding behavior was also
added. Formatting and linting changes were also applied.

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
"""

from io import BytesIO

from pytest import raises

from redun.bcoding import bdecode, bencode


def test_bools_fail_encoding():
    """
    Bools are recognized as ints by bcode-derived code so we disallow them.
    """
    with raises(TypeError):
        bencode(True)

    # Make sure our modification did not affect the encoding of ints.
    assert bencode(123) == b"i123e"


# basic en/decoding


def test_stream_decoding():
    with BytesIO(b"d2:hii1ee") as f:
        mapping = bdecode(f)
    assert mapping["hi"] == 1


def test_buffer_decoding():
    assert bdecode(b"3:one") == "one"
    assert bdecode("3:two") == "two"


def test_stream_encoding():
    with BytesIO() as stream:
        bencode({"a": 0}, stream)
        assert stream.getvalue() == b"d1:ai0ee"


def test_buffer_encoding():
    assert bencode(("a", 0)) == b"l1:ai0ee"


# decode incomplete stuff


def test_decode_incomplete_int():
    with raises(ValueError):
        print(bdecode("i1"))


def test_decode_incomplete_buffer():
    with raises(ValueError):
        bdecode("1:")


def test_decode_incomplete_list():
    with raises(TypeError):
        print(bdecode("l"))


def test_decode_incomplete_dict():
    with raises(TypeError):
        print(bdecode("d1:k"))
