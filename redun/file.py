import abc
import glob
import io
import os
import shutil
import sys
import threading
from shlex import quote
from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    Dict,
    Generic,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)
from urllib.parse import urlparse

import boto3
import fsspec
import s3fs
from botocore.exceptions import ClientError

from redun import glue
from redun.hashing import hash_stream, hash_struct
from redun.logging import logger
from redun.value import Value

# Don't require pyspark to be installed locally except for type checking.
if TYPE_CHECKING:
    import pandas
    import pyspark

T = TypeVar("T")

# Thread-local storage is used for thread-specific s3 clients.
_local = threading.local()

# Global singletons for Filesystems.
_proto2filesystem_class: Dict[str, Type["FileSystem"]] = {}


def register_filesystem(cls: Type["FileSystem"]) -> Type["FileSystem"]:
    _proto2filesystem_class[cls.name] = cls
    return cls


def get_proto(url: Optional[str] = None) -> str:
    """
    Returns the protocol for a url.

    For example the protocol for 'http://example.com' is 'http'. Local paths
    '/path/to/my/file' have the protocol 'local'.
    """
    if not url:
        return "local"
    return urlparse(url).scheme or "local"


def get_filesystem_class(
    proto: Optional[str] = None, url: Optional[str] = None
) -> Type["FileSystem"]:
    """
    Returns the corresponding FileSystem class for a given url or protocol.
    """
    if not proto:
        assert url, "Must give url or proto as argument."
        proto = get_proto(url)
    return _proto2filesystem_class[proto]


def list_filesystems() -> List[Type["FileSystem"]]:
    """
    Returns list of supported filesystems.
    """
    return list(_proto2filesystem_class.values())


def open_file(url: str, mode: str = "r", encoding: Optional[str] = None, **kwargs: Any) -> IO:
    """
    Open a file stream.

    Parameters
    ----------
    url : str
        Url or path of file to open.
    mode : str
        Stream mode for reading or writing ('r', 'w', 'b', 'a').
    encoding : str
        Text encoding (e.g. 'utf-8') to use when read or writing.
    **kwargs
        Additional arguments for the underlying file stream. They are Filesystem-specific.
    """
    return get_filesystem(url=url).open(url, mode=mode, encoding=encoding, **kwargs)


def copy_file(src_path: Optional[str], dest_path: Optional[str], recursive: bool = False) -> None:
    """
    Copy a file. Files can be from different filesystems.

    Parameters
    ----------
    src_path : Optional[str]
        Source path to copy from. If None, use stdin.
    dest_path : Optional[str]
        Destination path to copy to. If None, use stdout.
    recursive : bool
        If True, copy a directory tree of files.
    """
    if src_path is not None and dest_path is not None:
        if not recursive:
            File(src_path).copy_to(File(dest_path))
        else:
            Dir(src_path).copy_to(Dir(dest_path))
        return

    if recursive:
        raise ValueError("recursive is not supported with stdin or stdout.")

    infile: Optional[IO] = None
    outfile: Optional[IO] = None
    try:
        if src_path is None:
            infile = sys.stdin.buffer
        else:
            infile = open_file(src_path, "rb")
        if dest_path is None:
            outfile = sys.stdout.buffer
        else:
            outfile = open_file(dest_path, "wb")

        shutil.copyfileobj(infile, outfile)
    finally:
        if infile:
            infile.close()
        if outfile:
            outfile.close()


def glob_file(pattern: str) -> List[str]:
    return get_filesystem(url=pattern).glob(pattern)


_filesystem_instances: Dict[Type, "FileSystem"] = {}


def get_filesystem(proto: Optional[str] = None, url: Optional[str] = None) -> "FileSystem":
    """
    Returns the corresponding FileSystem for a given url or protocol.
    """
    filesystem_class = get_filesystem_class(proto=proto, url=url)
    filesystem = _filesystem_instances.get(filesystem_class)
    if not filesystem:
        filesystem = _filesystem_instances[filesystem_class] = filesystem_class()
    return filesystem


class RedunFileNotFoundError(FileNotFoundError):
    """
    Redun-specific FileNotFoundError.
    """

    def __init__(self, path: str, *args):
        super().__init__(*args)
        self.path = path

    def __str__(self) -> str:
        return f"[Errno {self.errno}] {self.strerror}. {self.path}"


class RedunPermissionError(PermissionError):
    """
    Redun-specific PermissionError.
    """

    def __init__(self, path: str, *args):
        super().__init__(*args)
        self.path = path

    def __str__(self) -> str:
        return f"[Errno {self.errno}] {self.strerror}. {self.path}"


class RedunOSError(OSError):
    """
    Redun-specific OSError.
    """

    def __init__(self, path: str, *args):
        super().__init__(*args)
        self.path = path

    def __str__(self) -> str:
        return f"[Errno {self.errno}] {self.strerror}. {self.path}"


class FileSystem(abc.ABC):
    """
    Base class filesystem access.
    """

    name: str = "base"

    def open(self, path: str, mode: str, encoding: Optional[str] = None, **kwargs: Any) -> IO:
        """
        Open a file stream from the filesystem.

        Parameters
        ----------
        path : str
            Url or path of file to open.
        mode : str
            Stream mode for reading or writing ('r', 'w', 'b', 'a').
        encoding : str
            Text encoding (e.g. 'utf-8') to use when read or writing.
        **kwargs
            Additional arguments for the underlying file stream. They are Filesystem-specific.
        """
        if encoding:
            if "b" in mode:
                raise ValueError("Binary mode 'b' cannot be used with encoding.")
            mode += "b"

        try:
            stream = self._open(path, mode, **kwargs)

        except FileNotFoundError as error:
            # Reraise expected errors with redun-specific subclases that include
            # the path.
            raise RedunFileNotFoundError(path, *error.args) from error

        except PermissionError as error:
            raise RedunPermissionError(path, *error.args) from error

        except OSError as error:
            raise RedunOSError(path, *error.args) from error

        except Exception:
            # Unknown error. Just log the path and reraise it.
            logger.error(f"File.open error: {path}")
            raise

        if encoding:
            # Perform encoding/decoding, if specified.
            stream = io.TextIOWrapper(stream, encoding)

        return stream

    def _open(self, path: str, mode: str, **kwargs: Any) -> IO:
        """
        Private open method for subclasses to implement.
        """
        pass

    def exists(self, path: str) -> bool:
        """
        Returns True if path exists on filesystem.
        """
        pass

    def remove(self, path: str) -> None:
        """
        Delete a path from the filesystem.
        """
        pass

    def touch(
        self, path: str, time: Union[Tuple[int, int], Tuple[float, float], None] = None
    ) -> None:
        """
        Create the path on the filesystem with timestamp.
        """
        assert time is None, "time is not supported."
        self.open(path, "a").close()

    def mkdir(self, path: str) -> None:
        """
        Creates the directory in the filesystem.
        """
        pass

    def rmdir(self, path: str, recursive: bool = False) -> None:
        """
        Removes a directory from the filesystem.
        If `recursive`, removes all contents of the directory.
        Otherwise, raises OSError on non-empty directories
        """
        pass

    def get_hash(self, path: str) -> str:
        """
        Return a hash for the file at path.
        """
        pass

    def copy(self, src_path: str, dest_path: str) -> None:
        """
        Copy a file from src_path to dest_path.
        """
        with open_file(src_path, "rb") as infile, open_file(dest_path, "wb") as outfile:
            shutil.copyfileobj(infile, outfile)

    def shell_copy(
        self,
        src_path: Optional[str],
        dest_path: Optional[str],
        recursive: bool = False,
        as_mount: bool = False,
    ) -> str:
        """
        Returns a shell command for performing a file copy.

        Parameters
        ----------
        src_path : Optional[str]
            Source path to copy from. If None, use stdin.
        dest_path : Optional[str]
            Destination path to copy to. If None, use stdout.
        recursive : bool
            If True, copy a directory tree of files.
        as_mount : bool
            Treat src/dest as mounted.
        """
        if recursive:
            if src_path and dest_path:
                return f"redun fs cp --recursive {quote(src_path)} {quote(dest_path)}"
            else:
                raise ValueError("recursive is not supported with stdin or stdout.")

        src_cmd = (
            get_filesystem(url=src_path).shell_copy(src_path, None, as_mount=as_mount)
            if src_path
            else None
        )
        dest_cmd = (
            get_filesystem(url=dest_path).shell_copy(None, dest_path, as_mount=as_mount)
            if dest_path
            else None
        )

        if src_cmd and dest_cmd:
            # Combine two fs-specific shell commands through a pipe.
            return f"({src_cmd}) | ({dest_cmd})"
        elif src_cmd:
            return src_cmd
        elif dest_cmd:
            return dest_cmd
        else:
            # At least one path must be defined.
            raise ValueError("At least one path must be defined.")

    def glob(self, pattern: str) -> List[str]:
        """
        Returns filenames matching pattern.
        """
        pass

    def isfile(self, path: str) -> bool:
        """
        Returns True if path is a file.
        """
        pass

    def isdir(self, path: str) -> bool:
        """
        Returns True if path is a directory.
        """
        pass

    def filesize(self, path: str) -> int:
        """
        Returns file size of path in bytes.
        """
        pass


@register_filesystem
class LocalFileSystem(FileSystem):
    """
    FileSystem methods for a local POSIX filesystem.
    """

    name = "local"

    def _ensure_dir(self, path: str) -> str:
        """
        Automatically create the directory for path.
        """
        dirname = os.path.dirname(path)
        if dirname and not os.path.exists(dirname):
            os.makedirs(dirname)
        return dirname

    def _open(self, path: str, mode: str, **kwargs: Any) -> IO:
        """
        Open a file stream for path with mode ('r', 'w', 'b').
        """
        # Auto create the directory if needed.
        if "w" in mode or "a" in mode:
            self._ensure_dir(path)

        return open(path, mode, **kwargs)

    def exists(self, path: str) -> bool:
        """
        Returns True if path exists on filesystem.
        """
        return os.path.exists(path)

    def remove(self, path: str) -> None:
        """
        Delete a path from the filesystem.
        """
        try:
            os.remove(path)
        except FileNotFoundError:
            pass

    def touch(
        self, path: str, time: Union[Tuple[int, int], Tuple[float, float], None] = None
    ) -> None:
        """
        Create the path on the filesystem with timestamp.
        """
        if not self.exists(path):
            self.open(path, "w").close()
        else:
            return os.utime(path, time)

    def mkdir(self, path: str) -> None:
        os.makedirs(path, exist_ok=True)

    def rmdir(self, path: str, recursive: bool = False) -> None:
        if self.isdir(path):
            if recursive:
                shutil.rmtree(path)
            else:
                os.rmdir(path)

    def get_hash(self, path: str) -> str:
        """
        Return a hash for the file at path.
        """
        # Perform a fast pseudo-hash of the file using O(1) proprties.
        if self.exists(path):
            stat = os.stat(path)
            mtime = stat.st_mtime
            size = stat.st_size
        else:
            mtime = -1
            size = -1
        return hash_struct(["File", "local", path, size, str(mtime)])

    def copy(self, src_path: str, dest_path: str) -> None:
        """
        Copy a file from src_path to dest_path.
        """
        if get_proto(src_path) == get_proto(dest_path) == "local":
            # Perform copy at filesystem level.
            self._ensure_dir(dest_path)
            shutil.copyfile(src_path, dest_path)
        else:
            # Perform generic copy.
            super().copy(src_path, dest_path)

    def shell_copy(
        self,
        src_path: Optional[str],
        dest_path: Optional[str],
        recursive: bool = False,
        as_mount: bool = False,
    ) -> str:
        """
        Returns a shell command for performing a file copy.

        Parameters
        ----------
        src_path : Optional[str]
            Source path to copy from. If None, use stdin.
        dest_path : Optional[str]
            Destination path to copy to. If None, use stdout.
        recursive : bool
            If True, copy a directory tree of files.
        as_mount : bool
            Treat src/dest as mounted.
        """
        protos = {get_proto(path) for path in [src_path, dest_path] if path}
        if "local" not in protos:
            # Fallback to generic copy strategy.
            return super().shell_copy(src_path, dest_path, recursive=recursive, as_mount=as_mount)

        other_proto = next((proto for proto in protos if proto != "local"), None)
        if other_proto:
            # Let other proto produce the command.
            return get_filesystem(other_proto).shell_copy(
                src_path, dest_path, recursive=recursive, as_mount=as_mount
            )

        if src_path and dest_path:
            if not recursive:
                return f"cp {quote(src_path)} {quote(dest_path)}"
            else:
                return f"cp -r {quote(src_path)} {quote(dest_path)}"
        elif recursive:
            raise ValueError("recursive is not supported with stdin or stdout.")
        elif src_path:
            return f"cat {quote(src_path)}"
        elif dest_path:
            return f"cat > {quote(dest_path)}"
        else:
            raise ValueError("At least one path must be given.")

    def glob(self, pattern: str) -> List[str]:
        """
        Returns filenames matching pattern.
        """
        return glob.glob(pattern, recursive=True)

    def isfile(self, path: str) -> bool:
        """
        Returns True if path is a file.
        """
        return os.path.isfile(path)

    def isdir(self, path: str) -> bool:
        """
        Returns True if path is a directory.
        """
        return os.path.isdir(path)

    def filesize(self, path: str) -> int:
        """
        Returns file size of path in bytes.
        """
        stat = os.stat(path)
        return stat.st_size


class FsspecFileSystem(FileSystem):

    name: str = "fsspec"

    def __init__(self):
        self._fs = None

    @property
    def fs(self):
        if self._fs is None:
            self._fs = fsspec.get_filesystem_class(self.name)()
        return self._fs

    def _ensure_dir(self, path: str) -> str:
        """
        Automatically create the directory for path.
        """
        dirname = os.path.dirname(path)
        self.fs.makedirs(dirname, exist_ok=True)
        return dirname

    def _open(self, path: str, mode: str, **kwargs: Any) -> IO:
        """
        Open a file stream for path with mode ('r', 'w', 'b').
        """
        # Auto create the directory if needed.
        if "w" in mode or "a" in mode:
            self._ensure_dir(path)

        return self.fs.open(path, mode, **kwargs)

    def exists(self, path: str) -> bool:
        """
        Returns True if path exists on filesystem.
        """
        return self.fs.exists(path)

    def remove(self, path: str) -> None:
        """
        Delete a path from the filesystem.
        """
        try:
            self.fs.rm(path)
        except FileNotFoundError:
            pass

    def mkdir(self, path: str) -> None:
        self.fs.makedirs(path, exist_ok=True)

    def rmdir(self, path: str, recursive: bool = False) -> None:
        try:
            self.fs.rm(path, recursive=recursive)
        except FileNotFoundError:
            pass

    def get_hash(self, path: str) -> str:
        """
        Return a hash for the file at path.
        """
        # Perform a fast pseudo-hash of the file using O(1) properties.
        if self.exists(path):
            size = self.fs.stat(path)["size"]
        else:
            size = -1
        return hash_struct(["File", "fsspec", path, str(size)])

    def copy(self, src_path: str, dest_path: str) -> None:
        """
        Copy a file from src_path to dest_path.
        """
        if get_proto(src_path) == get_proto(dest_path):
            # Perform copy at filesystem level.
            self.fs.copy(src_path, dest_path)
        else:
            # Perform generic copy.
            super().copy(src_path, dest_path)

    def shell_copy(
        self,
        src_path: Optional[str],
        dest_path: Optional[str],
        recursive: bool = False,
        as_mount: bool = False,
    ) -> str:
        """
        Returns a shell command for performing a file copy.

        Parameters
        ----------
        src_path : Optional[str]
            Source path to copy from. If None, use stdin.
        dest_path : Optional[str]
            Destination path to copy to. If None, use stdout.
        recursive : bool
            If True, copy a directory tree of files.
        as_mount : bool
            Treat src/dest as mounted.
        """
        if recursive:
            if not src_path or not dest_path:
                raise ValueError("recursive is not supported with stdin or stdout.")
            return f"redun fs cp --recursive {quote(src_path)} {quote(dest_path)}"
        else:
            src_path = src_path or "-"
            dest_path = dest_path or "-"
            return f"redun fs cp {quote(src_path)} {quote(dest_path)}"

    def glob(self, pattern: str) -> List[str]:
        """
        Returns filenames matching pattern.
        """
        return self.fs.glob(pattern, recursive=True)

    def isfile(self, path: str) -> bool:
        """
        Returns True if path is a file.
        """
        return self.fs.isfile(path)

    def isdir(self, path: str) -> bool:
        """
        Returns True if path is a directory.
        """
        return self.fs.isdir(path)

    def filesize(self, path: str) -> int:
        """
        Returns file size of path in bytes.
        """
        return self.fs.stat(path)["size"]


@register_filesystem
class HTTPFileSystem(FsspecFileSystem):
    """
    FileSystem methods for a HTTP urls.
    """

    name = "http"


@register_filesystem
class HTTPSFileSystem(FsspecFileSystem):
    """
    FileSystem methods for a HTTPS urls.
    """

    name = "https"


@register_filesystem
class FTPFileSystem(FsspecFileSystem):
    """
    FileSystem methods for a FTP.
    """

    name = "ftp"


@register_filesystem
class GSFileSystem(FsspecFileSystem):
    """
    FileSystem methods for a Google Cloud Storage.
    """

    name = "gs"

    def shell_copy(
        self,
        src_path: Optional[str],
        dest_path: Optional[str],
        recursive: bool = False,
        as_mount: bool = False,
    ) -> str:
        """
        Returns a shell command for performing a file copy.

        Parameters
        ----------
        src_path : Optional[str]
            Source path to copy from. If None, use stdin.
        dest_path : Optional[str]
            Destination path to copy to. If None, use stdout.
        recursive : bool
            If True, copy a directory tree of files.
        as_mount : bool
            Treat src/dest as mounted.
        """

        def to_mount_directory(path):
            parsed_path = urlparse(path)
            return f"/mnt/disks/{parsed_path.netloc}{parsed_path.path}"

        if as_mount:
            src_proto, dest_proto = get_proto(src_path), get_proto(dest_path)
            if src_proto == "gs":
                src_path = to_mount_directory(src_path)
            if dest_proto == "gs":
                dest_path = to_mount_directory(dest_path)

            # When staging mounted files, soft-link to stage in.

            if dest_path:
                # Ensure dest path exists.
                mk_dest_dir = f"mkdir -p {quote(os.path.dirname(dest_path))}"

            if src_path and dest_path:
                if src_proto == "gs" and dest_proto == "local":
                    return f"cp {quote(src_path)} {quote(dest_path)}"
                if src_proto == "local" and dest_proto == "gs":
                    if not recursive:
                        return f"{mk_dest_dir} && cp {quote(src_path)} {quote(dest_path)}"
                    else:
                        return f"{mk_dest_dir} && cp -r {quote(src_path)} {quote(dest_path)}"
            elif recursive:
                raise ValueError("recursive is not supported with stdin or stdout.")
            elif src_path:
                return f"cat {quote(src_path)}"
            elif dest_path:
                # We use a subshell to make dest_dir so that cat receives stdin.
                return f"$({mk_dest_dir}) cat - > {quote(dest_path)}"
            else:
                raise ValueError("At least one path must be given.")

        protos = {get_proto(path) for path in [src_path, dest_path] if path}

        if "gs" not in protos or not (protos <= {"local", "gs"}):
            # Fallback to generic copy strategy.
            return super().shell_copy(src_path, dest_path, recursive=recursive, as_mount=as_mount)

        if src_path and dest_path:
            if not recursive:
                return f"gsutil cp {quote(src_path)} {quote(dest_path)}"
            else:
                return f"gsutil cp -r {quote(src_path)} {quote(dest_path)}"
        elif recursive:
            raise ValueError("recursive is not supported with stdin or stdout.")
        elif src_path:
            return f"gsutil cp {quote(src_path)} -"
        elif dest_path:
            return f"gsutil cp - {quote(dest_path)}"
        else:
            raise ValueError("At least one path must be given.")

    def glob(self, pattern: str) -> List[str]:
        return ["gs://" + key for key in self.fs.glob(pattern)]


@register_filesystem
class S3FileSystem(FileSystem):
    """
    FileSystem methods for a AWS S3.
    """

    name = "s3"

    @property
    def s3(self) -> s3fs.S3FileSystem:
        # Maintain a client per thread, since s3fs is not thread-safe.
        # Double check pid since fork can clone thread-local storage.
        pid = getattr(_local, "pid", None)
        if pid != os.getpid():
            _local.pid = os.getpid()
            client = None
        else:
            client = getattr(_local, "s3", None)
        if not client:
            client = _local.s3 = s3fs.S3FileSystem(anon=False)
        return client

    @property
    def s3_raw(self) -> Any:
        # Maintain a client per thread, since boto is not thread-safe.
        # Double check pid since fork can clone thread-local storage.
        pid = getattr(_local, "pid", None)
        if pid != os.getpid():
            _local.pid = os.getpid()
            client = None
        else:
            client = getattr(_local, "s3_raw", None)
        if not client:
            # We need to create the client using a session to avoid all clients
            # sharing the same global session.
            # See: https://github.com/boto/boto3/issues/801#issuecomment-440942144
            session = boto3.session.Session()
            client = _local.s3_raw = session.client("s3")
        return client

    def exists(self, path: str) -> bool:
        """
        Returns True if path exists in filesystem.
        """
        try:
            # We call head_object ourselves so that we can avoid getting stale
            # results from the s3fs cache.
            _, _, bucket, key = path.split("/", 3)
            _ = self.s3_raw.head_object(Bucket=bucket, Key=key, **self.s3.req_kw)
            return True
        except ClientError:
            # path might be a directory. To detect if it exists in S3,
            # use list_objects_v2(). Same technique used in dask:
            # https://github.com/dask/s3fs/pull/323/files#diff-1997c4b809971172b55a040ccbb82ea0R562
            prefix = key.rstrip("/") + "/"
            response = self.s3_raw.list_objects_v2(
                Bucket=bucket, Prefix=prefix, Delimiter="/", MaxKeys=1, **self.s3.req_kw
            )
            return response["KeyCount"] > 0

    def remove(self, path: str) -> None:
        try:
            self.s3.rm(path)
        except FileNotFoundError:
            # It it not an error to try to remove a non-existent File.
            pass

    def _open(self, path: str, mode: str, **kwargs: Any) -> IO:
        return self.s3.open(path, mode, **kwargs)

    def mkdir(self, path: str) -> None:
        # s3fs mkdir only creates buckets, so we just touch this key
        if not self.exists(path):
            self.s3.touch(path)

    def rmdir(self, path: str, recursive: bool = False) -> None:
        if self.exists(path):
            self.s3.rm(path, recursive=recursive)

    def get_hash(self, path: str) -> str:
        # Use Etag for quick hashing file.
        try:
            # We call head_object ourselves so that we can avoid getting stale
            # results from the s3fs cache.
            _, _, bucket, key = path.split("/", 3)
            response = self.s3_raw.head_object(Bucket=bucket, Key=key, **self.s3.req_kw)
            etag = response["ETag"]
        except ClientError:
            etag = ""
        return hash_struct(["File", "s3", path, etag])

    def copy(self, src_path: str, dest_path: str) -> None:
        if get_proto(src_path) == get_proto(dest_path) == "s3":
            # Perform copy entirely within S3.
            self.s3.copy(src_path, dest_path)
        else:
            # Perform generic copy.
            super().copy(src_path, dest_path)

    def shell_copy(
        self,
        src_path: Optional[str],
        dest_path: Optional[str],
        recursive: bool = False,
        as_mount: bool = False,
    ) -> str:
        """
        Returns a shell command for performing a file copy.

        Parameters
        ----------
        src_path : Optional[str]
            Source path to copy from. If None, use stdin.
        dest_path : Optional[str]
            Destination path to copy to. If None, use stdout.
        recursive : bool
            If True, copy a directory tree of files.
        as_mount : bool
            Treat src/dest as mounted.
        """
        protos = {get_proto(path) for path in [src_path, dest_path] if path}
        if "s3" not in protos or not (protos <= {"local", "s3"}):
            # Fallback to generic copy strategy.
            return super().shell_copy(src_path, dest_path, recursive=recursive)

        if src_path and dest_path:
            if not recursive:
                return f"aws s3 cp --no-progress {quote(src_path)} {quote(dest_path)}"
            else:
                return f"aws s3 cp --no-progress --recursive {quote(src_path)} {quote(dest_path)}"
        elif recursive:
            raise ValueError("recursive is not supported with stdin or stdout.")
        elif src_path:
            return f"aws s3 cp --no-progress {quote(src_path)} -"
        elif dest_path:
            return f"aws s3 cp --no-progress - {quote(dest_path)}"
        else:
            raise ValueError("At least one path must be given.")

    def glob(self, pattern: str) -> List[str]:
        return ["s3://" + key for key in self.s3.glob(pattern)]

    def isfile(self, path: str) -> bool:
        return self.s3.isfile(path)

    def isdir(self, path: str) -> bool:
        return self.s3.isdir(path)

    def filesize(self, path: str) -> int:
        """
        Returns file size of path in bytes.
        """
        try:
            # We call head_object ourselves so that we can avoid getting stale
            # results from the s3fs cache.
            _, _, bucket, key = path.split("/", 3)
            response = self.s3_raw.head_object(Bucket=bucket, Key=key, **self.s3.req_kw)
            return response["ContentLength"]
        except ClientError as error:
            if error.response["Error"]["Code"] == "404":
                raise FileNotFoundError(path)
            else:
                # Unknown error, reraise it.
                raise


class FileClasses:
    """
    A grouping of related File classes.
    """

    File: "Type[File]"
    FileSet: "Type[FileSet]"
    Dir: "Type[Dir]"
    StagingFile: "Type[StagingFile]"
    StagingDir: "Type[StagingDir]"

    def __getattr__(self, attr: str) -> type:
        # We use this getattr in order to support forward references.
        if attr == "File":
            return File
        elif attr == "FileSet":
            return FileSet
        elif attr == "Dir":
            return Dir
        elif attr == "StagingFile":
            return StagingFile
        elif attr == "StagingDir":
            return StagingDir
        else:
            raise AttributeError(attr)


class File(Value):
    """
    Class for assisting file IO in redun tasks.

    File objects are hashed based on their contents and abstract over storage
    backends such as local disk or cloud object storage.
    """

    type_basename = "File"
    type_name = "redun.File"
    classes = FileClasses()

    def __init__(self, path: str):
        self.filesystem: FileSystem = get_filesystem(url=path)
        self.path: str = path
        self.stream: Optional[IO] = None
        self._hash: Optional[str] = None

    def __repr__(self) -> str:
        return f"{self.type_basename}(path={self.path}, hash={self.hash[:8]})"

    def __getstate__(self) -> dict:
        return {"path": self.path, "hash": self.hash}

    def __setstate__(self, state: dict) -> None:
        self.path = state["path"]
        self._hash = state["hash"]
        self.filesystem = get_filesystem(url=self.path)

    @property
    def hash(self) -> str:
        if not self._hash:
            self._hash = self._calc_hash()
        return self._hash

    def _calc_hash(self) -> str:
        return self.filesystem.get_hash(self.path)

    def get_hash(self, data: Optional[bytes] = None) -> str:
        return self.hash

    def update_hash(self) -> None:
        self._hash = self._calc_hash()

    def exists(self) -> bool:
        return self.filesystem.exists(self.path)

    def remove(self) -> None:
        return self.filesystem.remove(self.path)

    def open(self, mode: str = "r", encoding: Optional[str] = None, **kwargs: Any) -> IO:
        """
        Open a file stream.

        Parameters
        ----------
        mode : str
            Stream mode for reading or writing ('r', 'w', 'b', 'a').
        encoding : str
            Text encoding (e.g. 'utf-8') to use when read or writing.
        **kwargs
            Additional arguments for the underlying file stream. They are Filesystem-specific.
        """

        def close():
            original_close()
            self.update_hash()

            # Restore original close. This way double closing doesn't trigger
            # unnecessary hashing.
            self.stream.close = original_close

        self.stream = self.filesystem.open(self.path, mode, encoding=encoding, **kwargs)

        original_close = self.stream.close
        self.stream.close = close  # type: ignore

        return self.stream

    def touch(self, time: Union[Tuple[int, int], Tuple[float, float], None] = None) -> None:
        self.filesystem.touch(self.path, time)

    def read(self, mode: str = "r", encoding: Optional[str] = None) -> Union[str, bytes]:
        with self.open(mode=mode, encoding=encoding) as infile:
            data = infile.read()
        return data

    def readlines(self, mode: str = "r") -> List[Union[str, bytes]]:
        with self.open(mode=mode) as infile:
            data = infile.readlines()
        return data

    def write(
        self, data: Union[str, bytes], mode: str = "w", encoding: Optional[str] = None
    ) -> None:
        with self.open(mode=mode, encoding=encoding) as out:
            out.write(data)

    def copy_to(self, dest_file: "File", skip_if_exists: bool = False) -> "File":
        if skip_if_exists and dest_file.exists():
            return dest_file

        self.filesystem.copy(self.path, dest_file.path)
        dest_file.update_hash()
        return dest_file

    def shell_copy_to(self, dest_path: Optional[str], as_mount: bool = False) -> str:
        """
        Returns a shell command for copying the file to a destination path.

        Parameters
        ----------
        dest_path : Optional[str]
            Destination path to copy to. If None, use stdout.
        as_mount : Optional[str]
            Copy files from mounted directories.
        """
        return self.filesystem.shell_copy(self.path, dest_path, as_mount=as_mount)

    def isfile(self) -> bool:
        return self.filesystem.isfile(self.path)

    def isdir(self) -> bool:
        return self.filesystem.isdir(self.path)

    def is_valid(self) -> bool:
        if not self._hash:
            self.update_hash()
            return True
        else:
            return self.hash == self._calc_hash()

    def stage(self, local: Optional[str] = None) -> "StagingFile":
        if not local:
            # Assume same basename for local file.
            local = os.path.basename(self.path)
        elif local.endswith("/"):
            # Assume same basename for local file within given directory.
            local = os.path.join(local, os.path.basename(self.path))
        return self.classes.StagingFile(local, self)

    def basename(self) -> str:
        return os.path.basename(self.path)

    def dirname(self) -> str:
        return os.path.dirname(self.path)

    def size(self) -> int:
        return self.filesystem.filesize(self.path)


class FileSet(Value):
    type_basename = "FileSet"
    type_name = "redun.FileSet"
    classes = FileClasses()

    def __init__(self, pattern: str):
        self.pattern = pattern
        self.filesystem: FileSystem = get_filesystem(url=self.pattern)
        self._hash: Optional[str] = None
        self._files: Optional[List[File]] = None

    def __repr__(self) -> str:
        return "FileSet(pattern={pattern}, hash={hash})".format(
            pattern=self.pattern, hash=self.hash
        )

    @property
    def hash(self) -> str:
        if not self._hash:
            self._files = list(self)
            self._hash = self._calc_hash(self._files)
        return self._hash

    def _calc_hash(self, files: Optional[List[File]] = None) -> str:
        if files is None:
            files = list(self)
        return hash_struct([self.type_basename] + sorted(file.hash for file in files))

    def get_hash(self, data: Optional[bytes] = None) -> str:
        return self.hash

    def update_hash(self) -> None:
        self._files = list(self)
        self._hash = self._calc_hash(self._files)

    def __getstate__(self) -> dict:
        return {"pattern": self.pattern, "hash": self.hash}

    def __setstate__(self, state: dict) -> None:
        self.pattern = state["pattern"]
        self._hash = state["hash"]
        self.filesystem = get_filesystem(url=self.pattern)
        self._files = None

    def __iter__(self) -> Iterator[File]:
        for path in glob_file(self.pattern):
            if self.filesystem.isfile(path):
                yield self.classes.File(path)

    def files(self) -> List[File]:
        return list(self)

    def is_valid(self) -> bool:
        if not self._hash:
            self.update_hash()
            return True
        else:
            return self.hash == self._calc_hash()

    def iter_subvalues(self) -> Iterator["Value"]:
        """
        Iterates through the FileSet's subvalues (Files).
        """
        if self._files is not None:
            return iter(self._files)
        else:
            return iter(self)


class Dir(FileSet):
    type_basename = "Dir"
    type_name = "redun.Dir"
    classes = FileClasses()

    def __init__(self, path: str):
        path = path.rstrip("/")
        self.path = path
        pattern = os.path.join(path, "**")
        super().__init__(pattern)

    def __repr__(self) -> str:
        return f"{self.type_basename}(path={self.path}, hash={self.hash[:8]})"

    def __getstate__(self) -> dict:
        return {"path": self.path, "hash": self.hash}

    def __setstate__(self, state: dict) -> None:
        self.path = state["path"]
        super().__setstate__({"pattern": os.path.join(self.path, "**"), "hash": state["hash"]})

    def _calc_hash(self, files: Optional[List[File]] = None) -> str:
        if files is None:
            files = list(self)
        return hash_struct([self.type_basename] + sorted(file.hash for file in files))

    def exists(self) -> bool:
        return self.filesystem.exists(self.path)

    def mkdir(self) -> None:
        self.filesystem.mkdir(self.path)
        self.update_hash()

    def rmdir(self, recursive: bool = False) -> None:
        self.filesystem.rmdir(self.path, recursive)
        self.update_hash()

    def file(self, rel_path: str) -> File:
        return self.classes.File(os.path.join(self.path, rel_path))

    def rel_path(self, path: str) -> str:
        return os.path.relpath(path, self.path)

    def copy_to(self, dest_dir: "Dir", skip_if_exists: bool = False) -> "Dir":
        for src_file in self:
            rel_path = self.rel_path(src_file.path)
            dest_file = dest_dir.file(rel_path)
            src_file.copy_to(dest_file, skip_if_exists=skip_if_exists)
        return dest_dir

    def shell_copy_to(self, dest_path: str, as_mount: bool = False) -> str:
        """
        Returns a shell command for copying the directory to a destination path.

        Parameters
        ----------
        dest_path : str
            Destination path to copy to.
        as_mount : bool
            Treat src/dest as mounted.
        """
        return self.filesystem.shell_copy(self.path, dest_path, recursive=True, as_mount=as_mount)

    def stage(self, local: Optional[str] = None) -> "StagingDir":
        if not local:
            local = os.path.basename(self.path)
        return self.classes.StagingDir(local, self)


class Staging(Value, Generic[T]):
    def __init__(self, local: Union[T, str], remote: Union[T, str]):
        self.local: Any = None
        self.remote: Any = None

    def stage(self) -> T:
        pass

    def unstage(self) -> T:
        pass

    def render_unstage(self, as_mount: bool = False) -> str:
        pass

    def render_stage(self, as_mount: bool = False) -> str:
        pass

    @classmethod
    def parse_arg(cls, raw_type: type, arg: str) -> Any:
        raise NotImplementedError("Argument parsing is implemented for Staging Files and Dirs")


class StagingFile(Staging[File]):
    type_basename = "StagingFile"
    type_name = "redun.StagingFile"
    classes = FileClasses()

    def __init__(self, local: Union[File, str], remote: Union[File, str]):
        if isinstance(local, str):
            self.local = self.classes.File(local)
        else:
            self.local = local

        if isinstance(remote, str):
            self.remote = self.classes.File(remote)
        else:
            self.remote = remote

    def __repr__(self) -> str:
        return f"{self.type_basename}(local={self.local}, remote={self.remote})"

    def __getstate__(self) -> dict:
        return {"local": self.local, "remote": self.remote}

    def __setstate__(self, state: dict) -> None:
        self.local = state["local"]
        self.remote = state["remote"]

    def get_hash(self, data: Optional[bytes] = None) -> str:
        return hash_struct([self.type_name, self.local.path, self.remote.path])

    def stage(self) -> File:
        if self.local.path == self.remote.path:
            # No staging is needed.
            return self.local

        return self.remote.copy_to(self.local)

    def unstage(self) -> File:
        if self.local.path == self.remote.path:
            # No staging is needed.
            return self.remote

        return self.local.copy_to(self.remote)

    def render_unstage(self, as_mount: bool = False) -> str:
        """
        Returns a shell command for unstaging a file.
        """
        if self.local.path == self.remote.path:
            # No staging is needed.
            return ""

        return self.local.shell_copy_to(self.remote.path, as_mount=as_mount)

    def render_stage(self, as_mount: bool = False) -> str:
        """
        Returns a shell command for staging a file.
        """
        if self.local.path == self.remote.path:
            # No staging is needed.
            return ""

        return self.remote.shell_copy_to(self.local.path, as_mount=as_mount)


class StagingDir(Staging[Dir]):
    type_basename = "StagingDir"
    type_name = "redun.StagingDir"
    classes = FileClasses()

    def __init__(self, local: Union[Dir, str], remote: Union[Dir, str]):
        if isinstance(local, str):
            self.local = self.classes.Dir(local)
        else:
            self.local = local

        if isinstance(remote, str):
            self.remote = self.classes.Dir(remote)
        else:
            self.remote = remote

    def __repr__(self) -> str:
        return f"{self.type_basename}(local={self.local}, remote={self.remote})"

    def __getstate__(self) -> dict:
        return {"local": self.local, "remote": self.remote}

    def __setstate__(self, state: dict) -> None:
        self.local = state["local"]
        self.remote = state["remote"]

    def get_hash(self, data: Optional[bytes] = None) -> str:
        return hash_struct([self.type_name, self.local.path, self.remote.path])

    def stage(self) -> Dir:
        if self.local.path == self.remote.path:
            # No staging is needed.
            return self.local

        return self.remote.copy_to(self.local)

    def unstage(self) -> Dir:
        if self.local.path == self.remote.path:
            # No staging is needed.
            return self.remote

        return self.local.copy_to(self.remote)

    def render_unstage(self, as_mount: bool = False) -> str:
        """
        Returns a shell command for unstaging a directory.
        """
        if self.local.path == self.remote.path:
            # No staging is needed.
            return ""

        return self.local.shell_copy_to(self.remote.path, as_mount=as_mount)

    def render_stage(self, as_mount: bool = False) -> str:
        """
        Returns a shell command for staging a directory.
        """
        if self.local.path == self.remote.path:
            # No staging is needed.
            return ""

        return self.remote.shell_copy_to(self.local.path, as_mount=as_mount)


class IFileClasses(FileClasses):
    """
    A grouping of related IFile classes.
    """

    def __getattr__(self, attr: str) -> type:
        # We use this getattr in order to support forward references.
        if attr == "File":
            return IFile
        elif attr == "FileSet":
            return IFileSet
        elif attr == "Dir":
            return IDir
        elif attr == "StagingFile":
            return IStagingFile
        elif attr == "StagingDir":
            return IStagingDir
        else:
            raise AttributeError(attr)


class IFile(File):
    """
    Immutable file.

    This class should be used for files that are write once and then immutable.
    """

    type_basename = "IFile"
    type_name = "redun.IFile"
    classes = IFileClasses()

    def _calc_hash(self) -> str:
        # The hash only depends on the path.
        return hash_struct([self.type_basename, self.path])

    def is_valid(self) -> bool:
        # IFiles are always valid.
        return True


class IFileSet(FileSet):
    type_basename = "IFileSet"
    type_name = "redun.IFileSet"
    classes = IFileClasses()

    def _calc_hash(self, files: Optional[List[File]] = None) -> str:
        return hash_struct([self.type_basename, self.pattern])

    def is_valid(self) -> bool:
        # IFileSets are always valid.
        return True


class IDir(Dir):
    type_basename = "IDir"
    type_name = "redun.IDir"
    classes = IFileClasses()

    def _calc_hash(self, files: Optional[List[File]] = None) -> str:
        # IDir hash only depends on the path.
        return hash_struct([self.type_basename, self.path])


class IStagingFile(StagingFile):
    type_basename = "IStagingFile"
    type_name = "redun.IStagingFile"
    classes = IFileClasses()


class IStagingDir(Staging[Dir]):
    type_basename = "IStagingDir"
    type_name = "redun.IStagingDir"
    classes = IFileClasses()


class ContentFileClasses(FileClasses):
    """
    A grouping of related ContentFile classes.
    """

    def __getattr__(self, attr: str) -> type:
        # We use this getattr in order to support forward references.
        if attr == "File":
            return ContentFile
        elif attr == "FileSet":
            return ContentFileSet
        elif attr == "Dir":
            return ContentDir
        elif attr == "StagingFile":
            return ContentStagingFile
        elif attr == "StagingDir":
            return ContentStagingDir
        else:
            raise AttributeError(attr)


class ContentFile(File):
    """
    Content-based file hashing.
    """

    type_basename = "ContentFile"
    type_name = "redun.ConentFile"
    classes = ContentFileClasses()

    def _calc_hash(self) -> str:
        # Use filesystem.open() to avoid triggering a recursive hash update.
        with self.filesystem.open(self.path, mode="rb") as infile:
            content_hash = hash_stream(infile)
        return hash_struct([self.type_basename, self.path, content_hash])


class ContentFileSet(FileSet):
    type_basename = "ContentFileSet"
    type_name = "redun.CnotentFileSet"
    classes = ContentFileClasses()


class ContentDir(Dir):
    type_basename = "ContentDir"
    type_name = "redun.ContentDir"
    classes = ContentFileClasses()


class ContentStagingFile(StagingFile):
    type_basename = "ContentStagingFile"
    type_name = "redun.ContentStagingFile"
    classes = ContentFileClasses()


class ContentStagingDir(Staging[Dir]):
    type_basename = "ContentStagingDir"
    type_name = "redun.ContentStagingDir"
    classes = ContentFileClasses()


class ShardedS3Dataset(Value):
    """
    A sharded dataset on S3. "Sharded" means a collection of files that when concatenated
    comprise the complete dataset. Several formats are supported but parquet is the best
    tested with redun to date due to the quality of its integration with AWS services
    and because it allows reading of only portions of the dataset.

    The hash of the S3ShardedDataset is just the hash of the sorted list of
    files in the dataset. So changing the files included (such as with
    `recurse=True`), adding or removing files, or doing some kind of dataset
    write operation (which creates new shards) cause the hash of the dataset to
    change. This does not recognize individual shards from being altered by
    other code, however.
    """

    type_name = "redun.ShardedS3Dataset"

    def __init__(self, path: str, format: str = "parquet", recurse: bool = True):
        path = path.rstrip("/")
        self._path = path
        self._recurse = recurse
        self._hash: Optional[str] = None

        if format not in ["avro", "csv", "ion", "grokLog", "json", "orc", "parquet", "xml"]:
            raise ValueError(f"Invalid format {format}")
        self._format = format

        self.filesystem: FileSystem = get_filesystem(url=self.path)
        self._filenames: List[str] = self._gather_files()

    def _gather_files(self) -> List[str]:

        # If recursing, look in subdirectories too.
        files = glob_file(f"{self._path}/*.{self._format}")
        if self.recurse:
            files.extend(glob_file(f"{self._path}/**/*.{self._format}"))

        # Work around differences between fsspec's interpretation of ** on S3 vs.local
        # by removing any duplicate file names from the list.
        return sorted(set(files))

    def update_hash(self) -> None:
        self._hash = self._calc_hash()

    def postprocess(self, postprocess_args) -> "ShardedS3Dataset":
        self.update_hash()
        return self

    @property
    def format(self) -> str:
        return self._format

    @format.setter
    def format(self, value):
        self._format = format
        self._calc_hash()

    @property
    def recurse(self) -> bool:
        return self._recurse

    @recurse.setter
    def recurse(self, value):
        self._recurse = value
        self._calc_hash()

    @property
    def filenames(self) -> List[str]:
        return self._filenames

    @property
    def path(self) -> str:
        return self._path

    @path.setter
    def path(self, value):
        self._path = value
        self._calc_hash()

    def get_hash(self, data: Optional[bytes] = None) -> str:
        return self.hash

    @property
    def hash(self) -> str:
        if not self._hash:
            self._hash = self._calc_hash()
        return self._hash

    def _calc_hash(self) -> str:
        """
        Recalculates the hash of the dataset. We re-gather all the files at
        this time as new files may have been created in the meantime, such as
        writing output from a Spark/Glue job.
        """
        self._filenames = self._gather_files()
        return hash_struct(["ShardedS3Dataset"] + sorted(self._filenames))

    def is_valid(self) -> bool:
        if not self._hash:
            self.update_hash()
            return True
        else:
            return self.hash == self._calc_hash()

    def __repr__(self) -> str:
        return (
            "ShardedS3Dataset(path={path}, format={format}, "
            "recurse={recurse}, hash={hash})".format(
                path=self.path, format=self.format, recurse=self.recurse, hash=self.hash
            )
        )

    def iter_subvalues(self) -> Iterator["Value"]:
        for path in self._filenames:
            yield File(path)

    def __getstate__(self) -> dict:
        return {
            "path": self._path,
            "format": self._format,
            "recurse": self._recurse,
            "hash": self._hash,
            "files": self._filenames,
        }

    def __setstate__(self, state: dict) -> None:
        self._path = state["path"]
        self._format = state["format"]
        self._recurse = state["recurse"]
        self._filenames = state["files"]
        self._hash = state["hash"]

        self.filesystem = get_filesystem(url=self.path)
        assert isinstance(self.filesystem, S3FileSystem)
        self.s3 = self.filesystem.s3

    def load_spark(
        self, validate: bool = False, format_options: Dict[str, Any] = {}
    ) -> "pyspark.sql.DataFrame":
        """
        Loads the ShardedS3Dataset as a Spark DataFrame. Must be running
        in a Spark context.

        Parameters
        ----------
        validate : bool
            If True, will check that dataset has at least 1 row. Requires a count
            operation that can take a few minutes, so set to False for performance.

        format_options : Dict[str, Any]
            Additional options for the data loader. Documented here:
            https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format.html

        Returns
        -------
        pyspark.sql.DataFrame: loaded dataset
        """
        # Do this first so if we're not in a Spark env it raises ValueError.
        context = glue.get_glue_context()

        # Only loading from S3 is supported
        if self.filesystem.name != "s3":
            raise ValueError("load_spark requires a path on S3")

        # Set default options for csv as having a header line.
        f_options = {}
        if self.format == "csv":
            f_options = {"withHeader": True, "optimizePerformance": True}
        f_options.update(format_options)

        # Want to exclude all suffixes other than our desired format.
        # for example this could become "**.[!c][!s][!v]" for csv files.
        # Syntax from here:
        # https://docs.aws.amazon.com/glue/latest/dg/define-crawler.html#crawler-data-stores-exclude
        exclude_expr = "**." + "".join(f"[!{x}]" for x in self.format)

        dataset = context.create_dynamic_frame.from_options(
            connection_type="s3",
            format=self.format,
            format_options=f_options,
            connection_options={
                "paths": [self.path],
                "recurse": self.recurse,
                "exclude": exclude_expr,
            },
        )
        result = dataset.toDF()  # get spark dataframe from returned glue dynamicframe.

        if validate:
            num = result.count()
            if num == 0:
                raise ValueError(
                    f"No {self.format} records loaded from {self.path}. Is there a typo?"
                )

        return result

    def load_pandas(self, max_shards: int = -1) -> "pandas.DataFrame":
        """
        Loads the ShardedS3Dataset as a Pandas DataFrame.

        Parameters
        ----------
        max_shards : int
            Maximum number of shards to load. If -1, will load all of them.

        Returns
        -------
        pandas.DataFrame
            All
        """
        import pandas

        data = self.load_pandas_shards(max_shards)
        return pandas.concat(data)

    def load_pandas_shards(self, max_shards: int = -1) -> List["pandas.DataFrame"]:
        """
        Loads the ShardedS3Dataset as a list of Pandas DataFrames. This is
        deterministic and will load the shards in the same order every time.

        Parameters
        ----------
        max_shards : int
            Maximum number of shards to load. If -1 (default), will load
            all shards.

        Returns
        -------
        List[pandas.DataFrame]
            Loaded shards, one per entry in list. Shards
        """
        import pandas

        # Determine which load function to use
        loader_fns = {
            "csv": pandas.read_csv,
            "parquet": pandas.read_parquet,
            "json": pandas.read_json,
            "orc": pandas.read_orc,
        }
        if self.format not in loader_fns:
            raise ValueError(f"No pandas load function found for '{self.format}'")

        # Update list of files to be as apples-to-apples as spark load only looks at the
        # current list of files at load time.
        self._filenames = self._gather_files()

        if max_shards == -1:
            max_shards = len(self._filenames) + 1

        loader = loader_fns[self.format]
        data = [loader(file) for file in self._filenames[:max_shards]]
        return data

    def save_spark(
        self,
        dataset: Union["pandas.DataFrame", "pyspark.sql.DataFrame"],
        partition_keys: List[str] = [],
        catalog_database: str = "default",
        catalog_table: Optional[str] = None,
        format_options: Dict[str, Any] = {},
    ) -> None:
        """
        Writes a pandas or spark DataFrame to the given path in the given format,
        optionally partitioning on dataset keys. Must be done from a spark environment.

        Parameters
        ----------
        dataset : Union[pandas.DataFrame, pyspark.sql.DataFrame]
            Dataset to save

        partition_keys : List[str]
            Dataset keys to partition on. Each key will be a subdirectory in
            `self.path` containing data for each value of that key. For
            example, partition on the column 'K', will make subdirectores
            'K=1', 'K=2', 'K=3', etc.

        catalog_database : str
            Datacatalog name to write to, if creating a table in the Data Catalog.
            Defaults to 'default'

        catalog_table : Optional[str]
            If present, written data will be available in AWS Data Catalog / Glue / Athena
            with the indicated table name.

        format_options : Dict[str, Any]
            Additional options for the data loader. Documented here:
            https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format.html
        """
        # Do this first so if we're not in a Spark env it raises ValueError.
        context = glue.get_glue_context()

        # Only saving to S3 is supported
        if self.filesystem.name != "s3":
            raise ValueError("save_spark requires a path on S3")

        import pandas
        import pyspark
        from awsglue.dynamicframe import DynamicFrame

        # Set default write options.
        f_options: Dict[str, Any] = {}
        write_fmt = self.format

        # Use glue's parquet implementation as it's compatible and has a more mutable schema.
        if self.format == "parquet":
            f_options = {"compression": "snappy"}
            write_fmt = "glueparquet"

        elif self.format == "csv":
            f_options = {"writeHeader": True}
        f_options.update(format_options)

        writer = context.getSink(
            path=self.path,
            connection_type="s3",
            partitionKeys=partition_keys,
            enableUpdateCatalog=(catalog_table is not None),
            **f_options,
        )
        writer.setFormat(write_fmt)

        if catalog_table is not None:
            writer.setCatalogInfo(catalogDatabase=catalog_database, catalogTableName=catalog_table)

        if isinstance(dataset, pandas.DataFrame):
            spark_session = glue.get_spark_session()
            spark_df = spark_session.createDataFrame(dataset)
        elif isinstance(dataset, pyspark.sql.DataFrame):
            spark_df = dataset
        else:
            raise ValueError("Dataset must be a pandas or spark DataFrame")

        glue_df = DynamicFrame.fromDF(spark_df, context, "")
        writer.writeFrame(glue_df)
        self.update_hash()

    def purge_spark(
        self, remove_older_than: int = 1, manifest_file_path: Optional[str] = None
    ) -> None:
        """
        Recursively removes all files older than `remove_older_than` hours.
        Defaults to 1 hour. Optionally writes removed files to `manifest_file_path/Success.csv`
        """
        context = glue.get_glue_context()
        options: Dict[str, Any] = {"retentionPeriod": remove_older_than}
        if manifest_file_path:
            options["manifestFilePath"] = manifest_file_path

        context.purge_s3_path(self.path, options=options)
        self.update_hash()

    @classmethod
    def from_files(
        cls, files: List[File], format: Optional[str] = None, allow_additional_files: bool = False
    ) -> "ShardedS3Dataset":
        """
        Helper function to create a ShardedS3Dataset from a list of redun Files.
        This can be helpful for provenance tracking from methods that return multiple
        files and the "root directory" is wanted for later use.

        If a dataset cannot be created that contains all the files, such as being in
        different S3 prefixes, a ValueError is raised. If files do not all have the
        same format, a ValueError will be raised.

        Parameters
        ----------
        files : List[File]
            Files that should be included in the dataset.

        format : Optional[str]
            If specified, files are considered to be in this format.

        allow_additional_files : bool
            If True, allows the resulting ShardedS3Dataset to contain files not in
            `files`. If False, a ValueError will be raised if a ShardedS3Dataset cannot
            be constructed.

        Raises
        ------
        ValueError
            If a dataset could not be created from the given files.
        """
        # Check all files are actually on S3.
        if not all(f.filesystem.name == "s3" for f in files):
            raise ValueError("All files must be on S3 to construct a ShardedS3Dataset")

        # All files should exist.
        for f in files:
            if not f.exists():
                raise FileNotFoundError(
                    f"File {f.path} must exist to construct a dataset with it."
                )

        # os.path does not like the "s3://" prefix so we remove it and then add it back.
        if len(files) == 1:
            root_path = f"s3:/{os.path.split(files[0].path.replace('s3://', '/'))[0]}"
        elif len(files) > 1:
            root_path = f"s3:/{os.path.commonpath([f.path.replace('s3://', '/') for f in files])}"
        else:
            raise ValueError("Cannot construct a dataset from an empty list of files.")

        if not format:
            formats = set(os.path.splitext(f.path)[1].replace(".", "") for f in files)
            if not formats or len(formats) > 1:
                raise ValueError(
                    f"Multiple or zero file extensions found: {formats}."
                    " Could not detect format for ShardedS3Dataset."
                )
            format = formats.pop()

        result = ShardedS3Dataset(root_path, format=format, recurse=True)

        # Other files should not be included in the dataset unless specifically requested.
        if not allow_additional_files:
            extras = set(result.filenames) - set(f.path for f in files)
            if extras:
                raise ValueError(
                    f"Additional files {extras} would be added to dataset but "
                    "`allow_additional_files=False`"
                )
        return result

    @classmethod
    def from_data(
        cls,
        dataset: Union["pandas.DataFrame", "pyspark.sql.DataFrame"],
        output_path: str,
        format: str = "parquet",
        partition_keys: List[str] = [],
        catalog_database: str = "default",
        catalog_table: Optional[str] = None,
        format_options: Dict[str, Any] = {},
    ) -> "ShardedS3Dataset":
        """
        Helper function to create a ShardedS3Dataset from an existing DataFrame-like object.

        Parameters
        ----------
        dataset : Union[pandas.DataFrame, pyspark.sql.DataFrame]
            Dataset to save

        output_path : str
            Path on S3 to which data will be saved as multiple files of format `format`.

        format : str
            Format to save the data in. Supported formats are:
            `["avro", "csv", "ion", "grokLog", "json", "orc", "parquet", "xml"]`
            Defaults to parquet.

        partition_keys : List[str]
            Dataset keys to partition on. Each key will be a subdirectory in
            `self.path` containing data for each value of that key. For
            example, partition on the column 'K', will make subdirectores
            'K=1', 'K=2', 'K=3', etc.

        catalog_database : str
            Datacatalog name to write to, if creating a table in the Data Catalog.
            Defaults to 'default'

        catalog_table : Optional[str]
            If present, written data will be available in AWS Data Catalog / Glue / Athena
            with the indicated table name.

        format_options : Dict[str, Any]
            Additional options for the data loader. Documented here:
            https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format.html
        """
        result = ShardedS3Dataset(output_path, format, recurse=False)

        result.save_spark(
            dataset,
            partition_keys=partition_keys,
            catalog_database=catalog_database,
            catalog_table=catalog_table,
            format_options=format_options,
        )
        return result
