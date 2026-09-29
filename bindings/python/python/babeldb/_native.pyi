"""Type stubs of the native module ``babeldb._native`` (re-exported by ``babeldb``)."""

import os
from types import TracebackType
from typing import Iterable, List, Literal, Optional, Tuple, Type, Union

__version__: str

#: What keys, values and scan bounds accept; ``str`` is encoded as UTF-8.
_BytesLike = Union[bytes, bytearray, memoryview, str]
#: ``("put", key, value)`` or ``("delete", key)``.
_BatchOp = Union[
    Tuple[Literal["put"], _BytesLike, _BytesLike],
    Tuple[Literal["delete"], _BytesLike],
]
_Durability = Literal["immediate", "buffered"]
_StrPath = Union[str, "os.PathLike[str]"]

class BabelError(Exception):
    """Base class of every error raised by babeldb (I/O and storage errors
    are raised as ``BabelError`` itself)."""

class ConflictError(BabelError):
    """A write expectation failed (``if_absent=True`` on an existing key, or
    an ``if_revision`` that is not the current revision); nothing was written."""

class ClosedError(BabelError):
    """The database is closed: every method except ``close()`` raises it."""

class InvalidArgumentError(BabelError):
    """An argument was rejected: empty key or key over 4096 bytes, value too
    large, unknown durability, conflicting options, malformed batch operation."""

class Db:
    """An open babeldb database: one directory, opened with fjall + WAL.

    ``Db(path, durability="immediate")`` opens or creates the database in the
    directory ``path`` (its WAL is the file ``<path>.wal`` next to it). Share
    one ``Db`` between threads: every method is thread-safe and releases the
    GIL. A directory can be open only once per process (a second open raises
    ``BabelError``).

    ``durability="immediate"``: every write is durable when the call returns
    (concurrent writers share commits). ``durability="buffered"``: writes are
    visible when the call returns and durable within about 100 ms (``sync()``
    forces it; ``close()`` makes everything durable).

    Keys and values are ``bytes``, ``bytearray``, ``memoryview`` or ``str``
    (UTF-8); values come back as ``bytes``. Keys: 1 to 4096 bytes.
    """

    def __init__(self, path: _StrPath, durability: _Durability = "immediate") -> None: ...
    def put(
        self,
        key: _BytesLike,
        value: _BytesLike,
        *,
        if_absent: bool = False,
        if_revision: Optional[int] = None,
    ) -> int:
        """Store ``value`` under ``key``; return the new revision (an int that
        grows with every write of the database).

        ``if_absent=True`` writes only if the key does not exist;
        ``if_revision=r`` writes only if the key's current revision is ``r``
        (compare-and-set). A failed expectation raises ``ConflictError``;
        combining both raises ``InvalidArgumentError``.
        """
    def get(self, key: _BytesLike) -> Optional[bytes]:
        """The current value of ``key``, or ``None`` if it does not exist."""
    def delete(self, key: _BytesLike, *, if_revision: Optional[int] = None) -> bool:
        """Delete ``key``; return whether a record was deleted (``False`` if it
        did not exist). ``if_revision=r`` deletes only if the current revision
        is ``r``, else raises ``ConflictError`` (also when the key is missing)."""
    def scan(
        self,
        prefix: Optional[_BytesLike] = None,
        *,
        start: Optional[_BytesLike] = None,
        end: Optional[_BytesLike] = None,
        reverse: bool = False,
        limit: int = 0,
    ) -> List[Tuple[bytes, bytes]]:
        """Records in byte-wise key order as ``(key, value)`` tuples.

        ``prefix``: only keys starting with it. ``start`` (inclusive) / ``end``
        (exclusive): a key range; cannot be combined with ``prefix``
        (``InvalidArgumentError``). ``reverse=True``: descending order.
        ``limit``: at most this many records (0 = no limit); with
        ``reverse=True``, the last ones.
        """
    def keys(
        self,
        prefix: Optional[_BytesLike] = None,
        *,
        start: Optional[_BytesLike] = None,
        end: Optional[_BytesLike] = None,
        reverse: bool = False,
        limit: int = 0,
    ) -> List[bytes]:
        """The keys ``scan()`` would return (same arguments), without values."""
    def batch(self, ops: Iterable[_BatchOp]) -> List[Optional[int]]:
        """Apply ``("put", key, value)`` / ``("delete", key)`` operations
        atomically, in order, in one durable commit.

        All or nothing: if any operation is malformed or invalid (e.g. an
        empty key) nothing is written and the error is raised. Durable when it
        returns, in both durability modes. Returns one entry per operation: the
        new revision for a put; for a delete, the revision of the deleted
        record, or ``None`` if the key did not exist.
        """
    def sync(self) -> None:
        """Wait until every write acknowledged before this call is durable
        (buffered mode: forces it now; immediate mode: already the case)."""
    def close(self) -> None:
        """Close: wait for the calls in flight, make every acknowledged write
        durable and release the directory. Idempotent; every other method then
        raises ``ClosedError``."""
    @property
    def closed(self) -> bool:
        """``True`` once ``close()`` was called."""
    @property
    def path(self) -> str:
        """The database directory (absolute)."""
    @property
    def durability(self) -> _Durability:
        """``"immediate"`` or ``"buffered"``."""
    def __enter__(self) -> "Db": ...
    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]] = None,
        exc_value: Optional[BaseException] = None,
        traceback: Optional[TracebackType] = None,
    ) -> bool: ...

def open(path: _StrPath, durability: _Durability = "immediate") -> Db:
    """Open (or create) the database in the directory ``path``; same as
    ``Db(path, durability)``."""

def message_key(channel: int, message_id: int) -> bytes:
    """Chat message key: ``channel`` (u64 big-endian) then ``message_id`` (u64
    big-endian), 16 bytes. Keys sort by channel, then by message id."""

def parse_message_key(key: _BytesLike) -> Optional[Tuple[int, int]]:
    """Inverse of ``message_key``: ``(channel, message_id)``, or ``None`` if
    ``key`` is not 16 bytes long."""

def channel_prefix(channel: int) -> bytes:
    """The 8-byte prefix of every message key of ``channel``: ``scan`` it with
    ``reverse=True, limit=n`` for the ``n`` newest messages."""

def _close_all() -> None:
    """Close every database still open in this process (run at exit)."""
