# ---------- Track dataclass ----------
import logging
from dataclasses import dataclass
from pathlib import Path

from HashCache import HashCache
from utils import sanitize_for_path, compute_list_str_hash

# external libs
try:
    from mutagen.flac import FLAC
except Exception:
    print("ERROR: missing dependency 'mutagen'. Install with: pip install mutagen")
    raise

# ---------- logging ----------
logger = logging.getLogger("music_copysync")


# ---------- Track ----------
@dataclass
class Track:
    src_path: Path
    artist: str
    album: str
    title: str
    track_number: int
    audio_md5_signature: str  # embedded md5 hash in flac
    metadata_hash: str

    @classmethod
    def from_file(cls, path: Path, hash_cache: HashCache, table: str) -> "Track":
        """
        Create Track from path. Use cache (if enabled) to avoid re-reading metadata from disk
        and re-hashing when mtime+size are unchanged.

        Will always write the results back to the cache if available.
        """
        if not path.exists():
            raise FileNotFoundError(path)

        # : _dflts
        artist = ["Unknown Artist"]
        album = ["Unknown Album"]
        title = [path.stem]
        track_number = [0]
        audio_md5_signature = ["0x{unknown signature}"]

        # Try cache
        cache_hit = hash_cache.get_track_metadata_if_valid(path, table) if hash_cache else None

        # somefuckeryishappening with the cache, i dont get it
        # cache_hit = None

        if cache_hit:
            artist = cache_hit["artist"] or artist[0]
            album = cache_hit["album"] or album[0]
            title = cache_hit["title"] or title[0]
            track_number = int(cache_hit["track_number"]) if cache_hit["track_number"] is not None else 0
            audio_md5_signature = cache_hit["audio_md5_signature"] or audio_md5_signature[0]
        else:
            try:
                audio = FLAC(path)
                # results are always wrapped in a list, so always unwrap the first result
                # : priority >>> artists_tag, album_artist, artist, _dflt
                if audio.get("ARTISTS"):
                    artist = audio.get("ARTISTS")[0]
                    if ";" in artist:
                        artist = artist.split(";")[0]
                else:
                    artist = audio.get("albumartist", audio.get("artist", artist))[0]
                # : priority >>> album, _dflt
                album = audio.get("album", album)[0]
                # : priority >>> title, _dflt
                title = audio.get("title", title)[0]
                # : priority >>> track_number, _dflt
                tn = audio.get("tracknumber", track_number)[0]
                # : priority >>> md5_signature, _dflt
                # type is seemingly guaranteed by StreamInfo
                int_md5_signature = audio.info.md5_signature if audio.info.md5_signature else None
                audio_md5_signature = hex(int_md5_signature) if int_md5_signature else audio_md5_signature[0]

                # attempt cast
                try:
                    track_number = int(tn)
                except ValueError as e:
                    logger.error(f"Fallback: failed casting track number for {path}: {e}")
                    track_number = 0
            except Exception as e:
                logger.debug(f"Failed reading FLAC metadata for {path}: {e}")

        metadata = [
            artist,
            album,
            title,
            str(track_number),
            audio_md5_signature
        ]

        tr = cls(
            src_path=path,
            artist=artist,
            album=album,
            title=title,
            track_number=track_number,
            audio_md5_signature=audio_md5_signature,
            metadata_hash=compute_list_str_hash(metadata)
        )

        # Update cache regardless of cache_hit status
        # -> slightly inefficient, but will ensure that tables are up-to-date
        if hash_cache:
            try:
                hash_cache.set(path, table, tr)
            except Exception as e:
                logger.error(f"Something bad has happened: {e}")
                pass

        return tr

    def audio_signature_tag(self, length: int = 4) -> str:
        return self.audio_md5_signature[:length]

    def expected_output_path(self, base_output: Path) -> Path:
        safe_artist = sanitize_for_path(self.artist)
        safe_album = sanitize_for_path(self.album)
        filename = f"{sanitize_for_path(self.title)}-{self.audio_signature_tag()}.flac"
        return base_output / safe_artist / safe_album / filename
