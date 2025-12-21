# ---------- SQLite-backed hash cache ----------
import logging
import sqlite3
import threading
from pathlib import Path
from typing import Optional

# ---------- logging ----------
logger = logging.getLogger("music_copysync")


# ---------- HashCache ----------
class HashCache:
    """
    SQLite-backed cache stored at <output_dir>/<cache_filename>

    Tables:
      out_cache (destination file metadata cache)
      in_cache (input file metadata cache)
    """

    def __init__(self, output_dir: Path, cache_filename: str, use_input_cache: bool = True):
        self.output_dir = Path(output_dir)
        self.db_path = self.output_dir / cache_filename
        self._lock = threading.Lock()
        self._conn: Optional[sqlite3.Connection] = None
        self.use_input_cache = bool(use_input_cache)
        self._ensure_db_dir()

    def _ensure_db_dir(self):
        try:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass

    def _connect(self):
        if self._conn:
            return self._conn
        conn = sqlite3.connect(self.db_path, timeout=15)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")

        # destination file cache table
        conn.execute(
            """CREATE TABLE IF NOT EXISTS out_cache (
                path TEXT PRIMARY KEY,
                mtime REAL NOT NULL,
                size INTEGER NOT NULL,
                metadata_hash TEXT,
                artist TEXT,
                album TEXT,
                title TEXT,
                track_number INTEGER,
                audio_md5_signature TEXT
            );"""
        )

        conn.execute(
            "CREATE INDEX IF NOT EXISTS out_cache_hash_idx ON out_cache(audio_md5_signature);"
        )

        # input cache table
        conn.execute(
            """CREATE TABLE IF NOT EXISTS in_cache (
                path TEXT PRIMARY KEY,
                mtime REAL NOT NULL,
                size INTEGER NOT NULL,
                metadata_hash TEXT,
                artist TEXT,
                album TEXT,
                title TEXT,
                track_number INTEGER,
                audio_md5_signature TEXT
            );"""
        )

        conn.execute(
            "CREATE INDEX IF NOT EXISTS in_cache_hash_idx ON in_cache(audio_md5_signature);"
        )

        conn.commit()
        self._conn = conn
        return conn

    # ---------- cache API ----------
    def get_track_metadata_if_valid(self, src: Path, table: str) -> Optional[dict]:
        """
        Return cached Track metadata if path exists and if cache entry matches mtime+size; else None.
        """
        if not self.use_input_cache or table not in ["in_cache", "out_cache"]:
            return None
        try:
            st = src.stat()
        except FileNotFoundError:
            return None

        try:
            conn = self._connect()
            with self._lock:
                sql = f"""
                        SELECT artist, album, title, track_number, audio_md5_signature,
                               mtime, size, metadata_hash
                        FROM {table}
                        WHERE path = ?;
                    """
                cur = conn.execute(sql, (str(src),))
                row = cur.fetchone()
            if not row:
                return None
            artist, album, title, track_number, audio_md5_signature, cached_mtime, cached_size, cached_metahash = row

            if float(cached_mtime) == float(st.st_mtime) and int(cached_size) == int(st.st_size):
                return {
                    "artist": artist,
                    "album": album,
                    "title": title,
                    "track_number": track_number,
                    "audio_md5_signature": audio_md5_signature,
                }
            return None
        except Exception as e:
            logger.debug(f"HashCache.get_track_metadata_if_valid DB error for {src}: {e}")
            return None

    def set(self, src: Path, table: str, track):
        """Set or update cache entry for src."""
        if table not in ["in_cache", "out_cache"]:
            return None
        try:
            st = src.stat()
        except FileNotFoundError:
            return None

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
                        track.audio_md5_signature,
                    ),
                )
                conn.commit()
        except Exception as e:
            logger.debug(f"HashCache.set_input_cached DB error for {src}: {e}")

    def remove(self, src: Path, table: str):
        """Remove cache entry for if present."""
        if table not in ["in_cache", "out_cache"]:
            return None
        try:
            conn = self._connect()
            with self._lock:
                sql = f"DELETE FROM {table} WHERE path = ?;"
                conn.execute(sql, (str(src),))
                conn.commit()
        except Exception as e:
            logger.debug(f"HashCache.remove DB error for {src}: {e}")


    def close(self):
        try:
            if self._conn:
                self._conn.close()
                self._conn = None
        except Exception:
            pass
