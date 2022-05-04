from typing import Any, Callable, Generic, List, Optional, Sequence, TypeVar, cast

S = TypeVar("S")
T = TypeVar("T")


class Promise(Generic[T]):
    """
    A light-weight single-thread implementation of Promise.

    We specifically are careful to remove references as soon as possible to
    enable garbage collection.

    A Promise can only be resolved or rejected once. Callbacks will be called
    at most once. Callbacks can be added both before and after the resolution.
    """

    def __init__(self, func: Optional[Callable] = None):
        self.is_pending = True
        self.is_fulfilled: Optional[bool] = None
        self.is_rejected: Optional[bool] = None

        self._value: Optional[T] = None
        self._error: Optional[Exception] = None
        self._resolvers: List[Callable[[T], S]] = []
        self._rejectors: List[Callable[[Exception], S]] = []

        if func:
            try:
                func(self.do_resolve, self.do_reject)
            except Exception as error:
                self.do_reject(error)

    @property
    def value(self) -> T:
        """
        Returns final result or error.
        """
        if self.is_pending:
            raise ValueError("Promise is still pending.")
        elif self.is_fulfilled:
            return cast(T, self._value)
        else:
            # This is a backwards compatible fix.
            return cast(T, self._error)

    @property
    def error(self) -> Exception:
        """
        Returns final error.
        """
        if not self.is_rejected:
            raise ValueError("Promise is not rejected.")
        return cast(Exception, self._error)

    def do_resolve(self, result: T) -> T:
        """
        Resolve the promise to a result.
        """
        if not self.is_pending:
            # If promise is not pending, then do nothing.
            return result
        self._value = result
        self.is_fulfilled = True
        self.is_rejected = False
        self.is_pending = False
        self._notify()
        return result

    def do_reject(self, error: Exception) -> Exception:
        """
        Reject the promise with an error.
        """
        if not self.is_pending:
            # If promise is not pending, then do nothing.
            return error
        self._error = error
        self.is_fulfilled = False
        self.is_rejected = True
        self.is_pending = False
        self._notify()
        return error

    def _notify(self) -> None:
        """
        Notify all listeners of the promise.
        """
        if self.is_pending:
            # If promise is still pending, do nothing.
            return
        elif self.is_fulfilled:
            # If promise is resolved, notify new resolvers. Discard rejectors.
            resolvers = self._resolvers
            self._resolvers = []
            self._rejectors.clear()
            for resolver in resolvers:
                resolver(cast(T, self._value))
        else:
            # If promise is rejected, notify new rejectors. Discard resolvers.
            self._resolvers.clear()
            rejectors = self._rejectors
            self._rejectors = []
            for rejector in rejectors:
                rejector(cast(Exception, self._error))

    def then(
        self,
        resolver: Optional[Callable[[T], S]] = None,
        rejector: Optional[Callable[[Exception], S]] = None,
    ) -> "Promise[S]":
        """
        Register callbacks to the promise.
        """
        promise: Promise[S] = Promise()

        def wrap_callback(func):
            def wrapper(result_or_error):
                try:
                    result2 = func(result_or_error)
                    if isinstance(result2, Promise):
                        # A nested promise should propagate to the parent promise.
                        result2.then(promise.do_resolve, promise.do_reject)
                    else:
                        # Propagate the resolved value.
                        promise.do_resolve(result2)
                except Exception as error:
                    # Propagate the rejected exception.
                    promise.do_reject(error)

            return wrapper

        if resolver:
            self._resolvers.append(wrap_callback(resolver))
        else:
            # By default propagate result.
            self._resolvers.append(cast(Callable[[T], S], promise.do_resolve))
        if rejector:
            self._rejectors.append(wrap_callback(rejector))
        else:
            # By default propagate error.
            self._rejectors.append(cast(Callable[[Exception], S], promise.do_reject))

        self._notify()
        return promise

    def catch(self, rejector: Callable[[Exception], S]) -> "Promise[S]":
        """
        Register an error callback.
        """
        return self.then(None, rejector)

    @classmethod
    def all(cls, subpromises: Sequence["Promise[T]"]) -> "Promise[List[T]]":
        """
        Return a promise that waits for all subpromises to resolve.
        """
        promise: Promise[List[T]] = Promise()
        results: List[Optional[T]] = []
        num_done = 0

        def then(i: int, result: T) -> None:
            nonlocal num_done

            results[i] = result
            num_done += 1
            if num_done == len(results):
                # All subpromises are resolved now, so resolve the top-level
                # promise with the final list.
                promise.do_resolve(cast(List[T], results))

        def fail(error: Exception) -> None:
            # As soon as we get a rejection of a subpromise, we reject the
            # top-level promise.
            promise.do_reject(error)

        def make_then(i: int) -> Callable:
            # This function gives us a closure for i.
            return lambda result: then(i, result)

        results = [None] * len(subpromises)
        for i, subpromise in enumerate(subpromises):
            subpromise.then(make_then(i), fail)

        if len(results) == 0:
            # Special case for when we are given no subpromises.
            promise.do_resolve(cast(List[T], results))

        return promise


def wait_promises(subpromises: List[Promise[T]]) -> Promise[List[Promise[T]]]:
    """
    Wait for all promises to finish (either fulfill or reject).
    """
    promise: Promise[List[Promise[T]]] = Promise()
    num_done = 0

    def done(result_or_error: Any) -> None:
        nonlocal num_done
        num_done += 1
        if num_done == len(subpromises):
            promise.do_resolve(subpromises)

    for subpromise in subpromises:
        subpromise.then(done, done)

    if len(subpromises) == 0:
        # Special case for when we are given no subpromises.
        promise.do_resolve([])

    return promise
