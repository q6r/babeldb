"""babeldb embedded in Python: a native, in-process key-value store.

Quick start::

    import babeldb

    with babeldb.open("data/app") as db:
        rev = db.put("user:1", b'{"name": "Ana"}')   # durable when it returns
        db.get("user:1")                             # b'{"name": "Ana"}'
        db.scan("user:", reverse=True, limit=10)     # [(key, value), ...]

One ``Db`` per database directory per process; share it between threads
(every method is thread-safe and releases the GIL). See ``help(babeldb.Db)``.
"""

import atexit as _atexit

from ._native import (
    BabelError,
    ClosedError,
    ConflictError,
    Db,
    InvalidArgumentError,
    __version__,
    _close_all,
    channel_prefix,
    message_key,
    open,
    parse_message_key,
)

__all__ = [
    "BabelError",
    "ClosedError",
    "ConflictError",
    "Db",
    "InvalidArgumentError",
    "__version__",
    "channel_prefix",
    "message_key",
    "open",
    "parse_message_key",
]

# Databases still open at interpreter exit are closed (buffered writes made
# durable, directories released).
_atexit.register(_close_all)
