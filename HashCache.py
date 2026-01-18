# ---------- SQLite-backed hash cache ----------
import logging
import sqlite3
import threading
from pathlib import Path
from typing import Optional, Any

from utils import PrefixLoggerAdapter

# ---------- logging ----------
logger = logging.getLogger(__name__)

# ---------- HashCache ----------
class HashCache:
    """
    SQLite-backed cache stored at <output_dir>/<cache_filename>

    Tables:
      out_cache (destination file metadata cache)
      in_cache (input file metadata cache)
    """
    VALID_TABLES = ["out_cache", "in_cache"]

    def __init__(self,
            output_dir: Path = ".",
            cache_filename: Optional[str] = "hashcache.sqlite",
            disabled: Optional[bool] = False
        ):
        self.output_dir = Path(output_dir)
        self.db_path = self.output_dir / cache_filename
        self._lock = threading.Lock()
        self._conn: Optional[sqlite3.Connection] = None
        self._disabled = bool(disabled)

        self._ensure_db_dir()

    def _ensure_db_dir(self):
        try:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            logger.error(f"Something horrible has happened, {self.db_path.parent} containing DB cannot be created: {e}")
            pass

    def _connect(self):
        # return existing connection
        if self._conn:
            return self._conn
        # attempt to connect to DB and create requisite tables
        try:
            conn = sqlite3.connect(self.db_path, timeout=15)
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            # TODO: test if this actually works
            for _ in self.VALID_TABLES:
                # create table
                conn.execute(str(f"""
                CREATE TABLE IF NOT EXISTS {_} (
                    path TEXT PRIMARY KEY,
                    mtime REAL NOT NULL,
                    size INTEGER NOT NULL,
                        artist TEXT,
                        album TEXT,
                        title TEXT,
                        track_number INTEGER,
                        metadata_hash TEXT,
                    audio_md5_signature TEXT
                );""" ))
                # create index for table
                conn.execute(str(f"CREATE INDEX IF NOT EXISTS {_}_hash_idx ON {_}(audio_md5_signature);"))
                # lets hope that explicitly casting it as a str does the replacement before it attempts to run
            conn.commit()
        except Exception as e:
            logger.error(f"Something horrible has happened during DB connection: {e}")
            conn = None

        # return
        self._conn = conn
        return conn

    def _stat_file(self, _file_path: Path):
        # try to resolve the path
        try:
            _file_path = Path(_file_path).resolve()
        except FileNotFoundError as e:
            logger.error(f"Unable to resolve path, file not found: {e}")
            return None
        except RuntimeError as e:
            logger.error(f"Unable to resolve path, infinite loop: {e}")
            return None
        return (_file_path, _file_path.stat())

    def is_disabled(self):
        if self._disabled:
            # must be true, we are disabled
            logger.debug("Cache is disabled.")
        return self._disabled

    def disable(self):
        self._disabled = True
        return self._disabled

    def enable(self):
        self._disabled = False
        return self._disabled

    def is_table_invalid(self, provided_table: str):
        if provided_table not in self.VALID_TABLES:
            logger.error(f"Provided table: '{provided_table}' is not a valid_table.")
            return True
        return False

    # ---------- cache API ----------
    def get_track_metadata_if_unchanged_mtime_size(self, src: Path, table: str) -> Optional[dict]:
        """
            Return cached Track metadata if exists and if cache entry matches mtime+size; else None.
                Will automatically resolve provided path to an absolute path.
        """
        if self.is_disabled() or self.is_table_invalid(table):
            # HashCache is disabled, or we provided with an invalid table...
            return None

        # preps file path and statistics
        src, st = self._stat_file(src)
        logger.debug(f"Attempting metadata retrieval from cache for {src} from table {table}")

        try:
            conn = self._connect()
            with self._lock:
                sql = f"""
                    SELECT artist, album, title, track_number, audio_md5_signature,
                           mtime, size, metadata_hash
                    FROM {table} WHERE path = ?;
                """
                cur = conn.execute(sql, (str(src),))
                row = cur.fetchone()
            # check for results
            if not row:
                return None
            # unpack results
            artist, album, title, track_number, audio_md5_signature, cached_mtime, cached_size, metahash = row

            # if the modified time and size match the current file information
            if float(cached_mtime) == float(st.st_mtime) and int(cached_size) == int(st.st_size):
                return {
                    "abs_path": src,
                    "artist": artist,
                    "album": album,
                    "title": title,
                    "track_number": track_number,
                    "metadata_hash": metahash,
                    "md5_audsig": audio_md5_signature,
                }
            return None
        except Exception as e:
            logger.error(f"{src}: {e}")
            return None

    def set_from_track_obj(self, src: Path, table: str, track: Any):
        """Set cache entry for src."""
        if self.is_disabled() or self.is_table_invalid(table):
            return False

        # TODO: refactor this so it makes more sense
        src, st = self._stat_file(src)
        # if it returns nothing, we know to fail
        if not st:
            logger.error("Failed to read filesystem information for track: {track}")
            return False
        logger.debug(src)

        try:
            conn = self._connect()
            with self._lock:
                sql = f"""
                    INSERT INTO {table}
                        (path, mtime, size, metadata_hash,
                         artist, album, title, track_number, audio_md5_signature)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(path) DO UPDATE SET
                        mtime = excluded.mtime,
                        size = excluded.size,
                        metadata_hash = excluded.metadata_hash,
                        artist = excluded.artist,
                        album = excluded.album,
                        title = excluded.title,
                        track_number = excluded.track_number,
                        audio_md5_signature = excluded.audio_md5_signature;
                """
                conn.execute(
                    sql,
                    (
                        str(src),
                        float(st.st_mtime),
                        int(st.st_size),
                        track.metadata_hash,
                        track.artist,
                        track.album,
                        track.title,
                        int(track.track_number),
                        track.md5_audsig,
                    ),
                )
                conn.commit()
                logger.debug(f"SET: Successfully set track info table '{table}' for {src}")
                return True
        except Exception as e:
            logger.debug(f"ERROR: SET: Error setting track information in table '{table}' for {src}: {e}")
        return False

    def remove(self, src: Path, table: str):
        """Remove cache entry for if present."""
        if self.is_disabled() or self.is_table_invalid(table):
            return False

        try:
            conn = self._connect()
            with self._lock:
                sql = f"DELETE FROM {table} WHERE path = ?;"
                conn.execute(str(sql), (str(src),))
                conn.commit()
            return True
        except Exception as e:
            logger.debug(f"REMOVE: Error removing track information in table '{table}' for {src}: {e}")
        return False

    def close(self):
        try:
            if self._conn:
                self._conn.close()
                self._conn = None
        except Exception as e:
            logger.error(f"Cannot seem to close DB connection: {e}")
            pass

