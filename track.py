# ---------- Track dataclass ----------
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict, Union

from HashCache import HashCache
from utils import sanitize_for_path, HashingHelper

# external libs
try:
    from mutagen.flac import FLAC
except Exception:
    print("ERROR: missing dependency 'mutagen'. Install with: pip install mutagen")
    raise

# ---------- logging ----------
logger = logging.getLogger(__name__)


# ---------- Track ----------
@dataclass
class Track:
    # defaults
    abs_path: Path
    title: str
    artist: str
    album: str
    track_number: int
    metadata_hash: str # hash result of above information
    md5_audsig: str # embedded md5 hash in flac


    @classmethod
    def _create_track(cls, track_info: Dict[str, Optional[str]]) -> "Track":
        """
            Responsible for creating Track from the provided information.
        """
        if "abs_path" not in track_info:
            logger.error("Provided track_info dictionary has no abs_path.")
            raise FileNotFoundError

        # should get any available hashing helper
        hasher = HashingHelper()
        DEFAULT_META_HASH_KEYS = ["title", "artist", "album"]

        # : _dflts
        abs_path      = track_info.get("abs_path",      "")
        title         = track_info.get("title",         "Unknown Track")
        artist        = track_info.get("artist",        "Unknown Artist")
        album         = track_info.get("album",         "Unknown Album")
        metadata_hash = track_info.get("metadata_hash", hasher.hash_dict_vals(track_info, DEFAULT_META_HASH_KEYS))
        track_number  = track_info.get("track_number",  -1)
        md5_audsig    = track_info.get("md5_audsig",    "0x{unknown signature}")

        # attempt cast of track_number
        try:
            track_number = int(track_number)
        except ValueError as e:
            logger.error(f"fallback: failed casting track number for {abs_path}: {e}")
            track_number = -1

        return cls(
            abs_path=Path(abs_path),
            artist=artist,
            album=album,
            title=title,
            track_number=track_number,
            md5_audsig=md5_audsig,
            metadata_hash=metadata_hash
        )

    @classmethod
    def from_cache(cls, file_path: Path, hash_cache: HashCache, table: str) -> Optional["Track"]:
        """
        Use cache (if enabled) to avoid re-reading metadata from disk and re-hashing when
        mtime+size are unchanged.

        Will fallback to a clean read if anything goes wrong with the cache.
        """
        result = hash_cache.get_track_metadata_if_unchanged_mtime_size(file_path, table)
        if result:
            logger.debug(f"Cache hit for track: {result['title']}")
            return cls._create_track( result )
        else:
            logger.debug(f"Cache miss: {file_path}")
            return cls.from_file( file_path, hash_cache, table )

    @classmethod
    def from_file(cls, file_path: Union[Path, str], hash_cache: HashCache, table: str) -> Optional["Track"]:
        """
        Create Track from the provided path, if the path is relative, it will attempt to resolve it.
        Will always write the results back to the cache if provided and enabled...
        """
        # try to resolve the path
        try:
            abs_path = Path(file_path).resolve()
        except FileNotFoundError as e:
            logger.error(f"Unable to resolve path, file not found: {e}")
            return None
        except RuntimeError as e:
            logger.error(f"Unable to resolve path, infinite loop: {e}")
            return None

        try:
            audio = FLAC(abs_path)
            # results are always wrapped in a list, so always unwrap the first result
            # : priority >>> first_in_artists_tag, album_artist, artist, _dflt
            if audio.get("ARTISTS"):
                artist = audio.get("ARTISTS")[0]
                if ";" in artist:
                    artist = artist.split(";")[0]
            else:
                artist = audio.get("albumartist", audio.get("artist", [None]))[0]
            logger.debug(f"Determined albumartist to be: {artist} for {abs_path}")
            # : priority >>> album, _dflt
            album = audio.get("album", [None])[0]
            # : priority >>> title, _dflt
            title = audio.get("title", [None])[0]
            # : priority >>> track_number, _dflt
            track_number = audio.get("tracknumber", [None])[0]
            # : priority >>> md5_signature, _dflt
            # type is seemingly guaranteed by StreamInfo
            int_md5_signature = audio.info.md5_signature if audio.info.md5_signature else 0
            audio_md5_signature = hex(int_md5_signature)

            meta_dict = {
                "abs_path": str(abs_path),
                "title": title,
                "artist": artist,
                "album": album,
                "track_number": track_number,
                "": "",
                "": "",
                "md5_audsig": audio_md5_signature,
            }

            tr = cls._create_track(meta_dict)
            # return early if cache is disabled
            if hash_cache.is_disabled():
                return tr
            # otherwise, try to keep the database updated
            if not hash_cache.set_from_track_obj(abs_path, table, tr):
                logger.debug(f"Cache: Failed to save track '{tr.title}'-'{tr.artist}' information to table {table}")
            return tr
        except Exception as e:
            logger.debug(f"Failed reading FLAC metadata for {abs_path}: {e}")
            return None

    def md5_audsig_tag(self, length: int = 4) -> str:
        return self.md5_audsig[:length]

    def expected_output_path(self, base_output: Path) -> Path:
        safe_artist = sanitize_for_path(self.artist)
        safe_album = sanitize_for_path(self.album)
        filename = f"{sanitize_for_path(self.title)}-{self.md5_audsig_tag()}.flac"
        return base_output / safe_artist / safe_album / filename

    def human_readable(self):
        return f"_{self.title}-{self.album}.{self.artist}_"

    # TODO: config different output paths