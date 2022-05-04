from unittest.mock import Mock

import pytest

from redun.promise import Promise, wait_promises


def test_resolve() -> None:
    """
    Promise constructor should resolve values.
    """
    promise: Promise[int] = Promise(lambda resolve, reject: resolve(10))
    assert promise.value == 10


def test_reject() -> None:
    """
    Promise constructor should reject exceptions.
    """
    error = ValueError("boom")
    promise: Promise[int] = Promise(lambda resolve, reject: reject(error))
    assert promise.error == error


def test_do_resolve() -> None:
    """
    Promises should resolve values after instantiation.
    """
    promise: Promise[int] = Promise()
    promise.do_resolve(10)
    assert promise.value == 10


def test_do_reject() -> None:
    """
    Promises should reject exceptions after instantiation.
    """
    error = ValueError("boom")
    promise: Promise[int] = Promise()
    promise.do_reject(error)
    assert promise.error == error


def test_resolve_first() -> None:
    """
    First resolution counts.
    """
    error = ValueError("boom")
    promise: Promise[int] = Promise()
    promise.do_resolve(10)
    promise.do_resolve(20)
    promise.do_reject(error)
    assert promise.value == 10


def test_resolved_has_no_error() -> None:
    """
    Once resolved, promise is marked as not rejected.
    """
    error = ValueError("boom")
    promise: Promise[int] = Promise()
    promise.do_resolve(10)
    promise.do_reject(error)
    assert promise.value == 10
    with pytest.raises(ValueError):
        promise.error


def test_reject_first() -> None:
    """
    First rejection counts.
    """
    error = ValueError("boom")
    error2 = ValueError("boom2")
    promise: Promise[int] = Promise()
    promise.do_reject(error)
    promise.do_reject(error2)
    promise.do_resolve(10)
    promise.do_resolve(20)
    assert promise.error == error


def test_then_resolve() -> None:
    """
    Promise resolutions should propagate through then().
    """
    mock = Mock()
    promise: Promise[int] = Promise()
    promise.then(mock)
    promise.do_resolve(10)
    mock.assert_called_with(10)


def test_then_reject() -> None:
    """
    Promise rejections should propagate through then().
    """

    def fail(error):
        pass

    mock = Mock(fail)
    promise: Promise[int] = Promise()
    promise.then(None, mock)

    error = ValueError("boom")
    promise.do_reject(error)
    mock.assert_called_with(error)


def test_resolve_then() -> None:
    """
    Already resolved promises should still propagate through then().
    """
    promise: Promise[int] = Promise()
    promise.do_resolve(10)

    mock = Mock()
    promise.then(mock)
    mock.assert_called_with(10)


def test_reject_then() -> None:
    """
    Already rejected promises should still propagate through then().
    """
    promise: Promise[int] = Promise()
    error = ValueError("boom")
    promise.do_reject(error)

    mock = Mock()
    promise.then(None, mock)
    mock.assert_called_with(error)


def test_resolve_multiple_then() -> None:
    """
    Resolutions should fan-out to multiple `then()` calls.
    """
    promise: Promise[int] = Promise()

    mock = Mock()
    promise.then(mock)
    mock2 = Mock()
    promise.then(mock2)

    promise.do_resolve(10)
    mock.assert_called_with(10)
    mock2.assert_called_with(10)


def test_reject_multiple_then() -> None:
    """
    Rejections should fan-out through multiple `then()` calls.
    """
    promise: Promise[int] = Promise()

    mock = Mock()
    promise.then(None, mock)
    mock2 = Mock()
    promise.then(None, mock2)

    error = ValueError("boom")
    promise.do_reject(error)
    mock.assert_called_with(error)
    mock2.assert_called_with(error)


def test_chain_resolve() -> None:
    """
    Resolutions should propagate through chained `then()` calls.
    """
    promise: Promise[int] = Promise()

    mock = Mock()
    promise.then(lambda x: x + 1).then(lambda x: x + 2).then(mock)

    promise.do_resolve(10)
    mock.assert_called_with(13)


def test_chain_reject() -> None:
    """
    Rejections should propagate through chained `then()` calls.
    """
    promise: Promise[int] = Promise()

    mock = Mock()
    promise.then(lambda x: x + 1).then(None, mock)

    error = ValueError("boom")
    promise.do_reject(error)
    mock.assert_called_with(error)


def test_chain_null() -> None:
    """
    Resolutions should propagate through empty `then()` calls.
    """
    promise: Promise[int] = Promise()

    mock = Mock()
    promise.then().then(mock)

    promise.do_resolve(10)
    mock.assert_called_with(10)


def test_nest_resolve() -> None:
    """
    Resolutions of nested promises should propagate.
    """
    promise: Promise[int] = Promise()

    mock = Mock()
    promise.then(lambda x: Promise(lambda resolve, reject: resolve(x + 1))).then(mock)

    promise.do_resolve(10)
    mock.assert_called_with(11)


def test_nest_reject() -> None:
    """
    Rejections of nested promises should propagate.
    """
    promise: Promise[int] = Promise()

    error = ValueError("boom")
    mock = Mock()
    promise.then(lambda x: Promise(lambda resolve, reject: reject(error))).catch(mock)

    promise.do_resolve(10)
    mock.assert_called_with(error)


def test_raise() -> None:
    """
    Raising an exception should reject the Promise.
    """
    error = ValueError("boom")

    def then(result: int) -> None:
        raise error

    def fail(error: Exception) -> int:
        return 100

    def final(result: int) -> None:
        pass

    promise: Promise[int] = Promise()

    mock = Mock(side_effect=fail)
    mock2 = Mock(side_effect=final)
    promise.then(then).catch(mock).then(mock2)

    promise.do_resolve(10)
    mock.assert_called_with(error)
    mock2.assert_called_with(100)


def test_all() -> None:
    """
    Promise.all() should resolve when all child promises resolve.
    """
    promise1: Promise[int] = Promise()
    promise2: Promise[int] = Promise()

    promises = Promise.all([promise1, promise2])

    mock = Mock()
    promises.then(mock)

    promise1.do_resolve(1)
    promise2.do_resolve(2)
    mock.assert_called_with([1, 2])


def test_all_first() -> None:
    """
    Promise.all() should resolve even when all child promises resolve first.
    """
    promise1: Promise[int] = Promise()
    promise2: Promise[int] = Promise()
    promise1.do_resolve(1)
    promise2.do_resolve(2)

    promises = Promise.all([promise1, promise2])

    mock = Mock()
    promises.then(mock)

    mock.assert_called_with([1, 2])


def test_all_reject() -> None:
    """
    Promise.all() should reject when any child promise rejects.
    """
    promise1: Promise[int] = Promise()
    promise2: Promise[int] = Promise()

    promises = Promise.all([promise1, promise2])

    mock = Mock()
    promises.then(None, mock)

    error = ValueError("boom")
    promise1.do_resolve(1)
    promise2.do_reject(error)
    mock.assert_called_with(error)


def test_all_reject_first() -> None:
    """
    Promise.all() should reject even when a child promise rejects first.
    """
    error = ValueError("boom")
    promise1: Promise[int] = Promise()
    promise2: Promise[int] = Promise()
    promise1.do_resolve(1)
    promise2.do_reject(error)

    promises = Promise.all([promise1, promise2])

    mock = Mock()
    promises.then(None, mock)

    mock.assert_called_with(error)


def test_wait_promises() -> None:
    """
    wait_promises() should wait for all subpromises regardless of success or failure.
    """

    def good():
        return Promise(lambda resolve, reject: resolve(10))

    def bad():
        return Promise(lambda resolve, reject: reject(ValueError("boom")))

    top = wait_promises([good(), good(), good()])
    promises = top.value
    assert [promise.value for promise in promises] == [10, 10, 10]

    top = wait_promises([good(), bad(), good()])
    promises = top.value
    assert promises[0].value == 10
    assert isinstance(promises[1].error, ValueError)
    assert promises[2].value == 10

    a: Promise[int] = Promise()
    top = wait_promises([good(), bad(), a])
    a.do_resolve(20)
    promises = top.value
    assert promises[0].value == 10
    assert isinstance(promises[1].error, ValueError)
    assert promises[2].value == 20
