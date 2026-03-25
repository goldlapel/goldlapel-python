import os
import re
import socket
import sys
import threading
from collections import namedtuple, OrderedDict

_DDL_SENTINEL = "__ddl__"

CacheEntry = namedtuple("CacheEntry", ["rows", "description", "tables"])

_TX_START = re.compile(r"^\s*(BEGIN|START\s+TRANSACTION)\b", re.IGNORECASE)
_TX_END = re.compile(r"^\s*(COMMIT|ROLLBACK|END)\b", re.IGNORECASE)

_TABLE_PATTERN = re.compile(
    r"\b(?:FROM|JOIN)\s+(?:ONLY\s+)?(?:(\w+)\.)?(\w+)",
    re.IGNORECASE,
)

_SQL_KEYWORDS = frozenset({
    "select", "from", "where", "and", "or", "not", "in", "exists",
    "between", "like", "is", "null", "true", "false", "as", "on",
    "left", "right", "inner", "outer", "cross", "full", "natural",
    "group", "order", "having", "limit", "offset", "union", "intersect",
    "except", "all", "distinct", "lateral", "values",
})


def _make_key(sql, params):
    if params is None:
        return (sql, None)
    if isinstance(params, dict):
        return (sql, tuple(sorted(params.items())))
    return (sql, tuple(params))


def _detect_write(sql):
    trimmed = sql.strip()
    tokens = trimmed.split()
    if not tokens:
        return None
    first = tokens[0].upper()

    if first == "INSERT":
        if len(tokens) < 3 or tokens[1].upper() != "INTO":
            return None
        return _bare_table(tokens[2])
    elif first == "UPDATE":
        if len(tokens) < 2:
            return None
        return _bare_table(tokens[1])
    elif first == "DELETE":
        if len(tokens) < 3 or tokens[1].upper() != "FROM":
            return None
        return _bare_table(tokens[2])
    elif first == "TRUNCATE":
        if len(tokens) < 2:
            return None
        if tokens[1].upper() == "TABLE":
            if len(tokens) < 3:
                return None
            return _bare_table(tokens[2])
        return _bare_table(tokens[1])
    elif first in ("CREATE", "ALTER", "DROP", "REFRESH", "DO", "CALL"):
        return _DDL_SENTINEL
    elif first == "MERGE":
        if len(tokens) < 3 or tokens[1].upper() != "INTO":
            return None
        return _bare_table(tokens[2])
    elif first == "SELECT":
        saw_into = False
        into_target = None
        for tok in tokens[1:]:
            upper = tok.upper()
            if upper == "INTO" and not saw_into:
                saw_into = True
                continue
            if saw_into and into_target is None:
                if upper in ("TEMPORARY", "TEMP", "UNLOGGED"):
                    continue
                into_target = tok
                continue
            if saw_into and into_target is not None and upper == "FROM":
                return _DDL_SENTINEL
            if upper == "FROM":
                return None
        return None
    elif first == "COPY":
        if len(tokens) < 2:
            return None
        raw = tokens[1]
        if raw.startswith("("):
            return None
        table_part = raw.split("(")[0]
        for tok in tokens[2:]:
            upper = tok.upper()
            if upper == "FROM":
                return _bare_table(table_part)
            if upper == "TO":
                return None
        return None
    elif first == "WITH":
        rest_upper = trimmed[len(tokens[0]):].upper()
        for token in rest_upper.split():
            word = token.lstrip("(")
            if word in ("INSERT", "UPDATE", "DELETE"):
                return _DDL_SENTINEL
        return None

    return None


def _bare_table(raw):
    table = raw.split("(")[0]
    table = table.rsplit(".", 1)[-1]
    return table.lower()


def _extract_tables(sql):
    tables = set()
    for match in _TABLE_PATTERN.finditer(sql):
        table = match.group(2).lower()
        if table not in _SQL_KEYWORDS:
            tables.add(table)
    return tables


class NativeCache:
    _instance = None
    _instance_lock = threading.Lock()

    def __new__(cls):
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._cache = OrderedDict()
        self._table_index = {}
        self._max_entries = int(os.environ.get("GOLDLAPEL_NATIVE_CACHE_SIZE", "32768"))
        self._enabled = os.environ.get("GOLDLAPEL_NATIVE_CACHE", "true").lower() != "false"
        self._lock = threading.Lock()
        self._invalidation_connected = False
        self._invalidation_thread = None
        self._invalidation_stop = threading.Event()
        self._invalidation_port = 0
        self._reconnect_attempt = 0
        self.stats_hits = 0
        self.stats_misses = 0
        self.stats_invalidations = 0
        self._initialized = True

    def get(self, sql, params):
        if not self._enabled or not self._invalidation_connected:
            return None
        try:
            key = _make_key(sql, params)
            hash(key)
        except TypeError:
            return None
        with self._lock:
            entry = self._cache.get(key)
            if entry is not None:
                self._cache.move_to_end(key)
                self.stats_hits += 1
                return entry
            self.stats_misses += 1
            return None

    def put(self, sql, params, rows, description):
        if not self._enabled or not self._invalidation_connected:
            return
        try:
            key = _make_key(sql, params)
            hash(key)
        except TypeError:
            return
        tables = _extract_tables(sql)
        with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
            elif len(self._cache) >= self._max_entries:
                self._evict_one()
            self._cache[key] = CacheEntry(rows, description, tables)
            for table in tables:
                if table not in self._table_index:
                    self._table_index[table] = set()
                self._table_index[table].add(key)

    def invalidate_table(self, table):
        table = table.lower()
        with self._lock:
            keys = self._table_index.pop(table, set())
            for key in keys:
                entry = self._cache.pop(key, None)
                if entry:
                    for other_table in entry.tables:
                        if other_table != table and other_table in self._table_index:
                            self._table_index[other_table].discard(key)
            self.stats_invalidations += len(keys)

    def invalidate_all(self):
        with self._lock:
            count = len(self._cache)
            self._cache.clear()
            self._table_index.clear()
            self.stats_invalidations += count

    def connect_invalidation(self, port):
        if self._invalidation_thread and self._invalidation_thread.is_alive():
            return
        self._invalidation_port = port
        self._invalidation_stop.clear()
        self._reconnect_attempt = 0
        self._invalidation_thread = threading.Thread(
            target=self._invalidation_loop, daemon=True
        )
        self._invalidation_thread.start()

    def stop_invalidation(self):
        self._invalidation_stop.set()
        if self._invalidation_thread:
            self._invalidation_thread.join(timeout=5)
            self._invalidation_thread = None
        self._invalidation_connected = False

    def _invalidation_loop(self):
        port = self._invalidation_port
        sock_path = f"/tmp/goldlapel-{port}.sock"

        while not self._invalidation_stop.is_set():
            sock = None
            try:
                if sys.platform != "win32" and os.path.exists(sock_path):
                    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    sock.connect(sock_path)
                else:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.connect(("127.0.0.1", port))

                sock.settimeout(30.0)
                self._invalidation_connected = True
                self._reconnect_attempt = 0
                buf = b""

                while not self._invalidation_stop.is_set():
                    try:
                        data = sock.recv(4096)
                        if not data:
                            break
                        buf += data
                        while b"\n" in buf:
                            line, buf = buf.split(b"\n", 1)
                            self._process_signal(
                                line.decode("utf-8", errors="replace")
                            )
                    except socket.timeout:
                        break

            except (OSError, ConnectionRefusedError):
                pass
            finally:
                if self._invalidation_connected:
                    self._invalidation_connected = False
                    self.invalidate_all()
                if sock:
                    try:
                        sock.close()
                    except OSError:
                        pass

            wait_secs = min(2 ** self._reconnect_attempt, 15)
            if self._invalidation_stop.wait(wait_secs):
                return
            self._reconnect_attempt += 1

    def _process_signal(self, line):
        if line.startswith("I:"):
            table = line[2:].strip()
            if table == "*":
                self.invalidate_all()
            else:
                self.invalidate_table(table)

    def _evict_one(self):
        if not self._cache:
            return
        lru_key, entry = self._cache.popitem(last=False)
        if entry:
            for table in entry.tables:
                if table in self._table_index:
                    self._table_index[table].discard(lru_key)
                    if not self._table_index[table]:
                        del self._table_index[table]

    @classmethod
    def _reset(cls):
        with cls._instance_lock:
            if cls._instance and cls._instance._invalidation_thread:
                cls._instance.stop_invalidation()
            cls._instance = None
