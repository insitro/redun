from redun.hashing import hash_struct, hash_tag_bytes


def test_hash_struct():
    """
    Test that we can hash a struct.
    """
    assert hash_struct(["Task", "123"]) == "f02cbd309f1866b7d015617265c233160abdbd10"
    assert hash_struct({"key1": 10, "key2": "123"}) == "15c28723885dd16585e5555596f5e0ba67ef9ce1"


def test_hash_tag_bytes():
    """
    Test that we can hash a tagged byte sequence.
    """
    assert hash_tag_bytes("Value", b"123") == "25aa6da917ca4e568e859df039fb1ca184e65d57"
