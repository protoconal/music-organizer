import logging
import os
from pathlib import Path
from typing import Dict, Tuple

from file_sync.HashCache import HashCache
from file_sync.track import Track

# external libs
try:
    from tqdm import tqdm

    TQDM_AVAILABLE = True
except ModuleNotFoundError:
    TQDM_AVAILABLE = False

# -------- logging utils --------
logger = logging.getLogger("music_copysync")


# ---------- scanning helpers ----------

def scan_directory_for_flacs(directory: Path, remove_empty_dir: bool = True) -> set[Path]:
    """
    Returns a set of Path objects pointing to any discovered files ending in FLAC.
      -  remove_empty_dir - will remove any empty directories during its walk.
    """
    logger.info(f"Running discovery for FLACs in {directory}")
    flac_paths = set()

    if not directory.exists():
        return flac_paths

    for root, dirs, files in os.walk(directory):
        p = Path(root)
        if remove_empty_dir and not any(p.iterdir()):
            p.rmdir()
        for f in files:
            if f.lower().endswith(".flac"):
                flac_paths.add(Path(root) / f)

    logger.info(f"Discovered {len(flac_paths)} FLACs in {directory}")
    return flac_paths


def scan_input_dir(input_dir: Path):
    """
    Return a set of all existing FLAC file paths under input_dir (absolute Paths).
    """
    return scan_directory_for_flacs(input_dir)


def scan_output_dir(output_dir: Path, remove_empty_dir: bool) -> set:
    """
    Return a set of all existing FLAC file paths under output_dir (absolute Paths).
    """
    return scan_directory_for_flacs(output_dir, remove_empty_dir)


def build_required_input_maps(input_dir: Path, output_dir: Path, hash_cache: HashCache) -> \
        [Dict[Path, Tuple[Path, Track]]]:
    """
        Return the required maps...
        - audio_signature_map: audio_signature -> expected_path_map
        - expected_path_map: expected_dst_path -> {origin_path, Track}
    """
    table = "in_cache"
    audio_signature_map: Dict[str, Tuple[Path, Track]] = {}
    expected_path_map: Dict[Path, Tuple[Path, Track]] = {}
    input_paths = scan_input_dir(input_dir)

    it = tqdm(input_paths, desc="Scanning input metadata", unit="file") if TQDM_AVAILABLE else input_paths
    for p in it:
        try:
            # track should always update the cache...
            tr = Track.from_file(p, hash_cache, table)
            # determine the expected output_path
            dst = tr.expected_output_path(output_dir)
            # map the expected path, to the real path and object...
            expected_path_map[dst] = (p, tr)
            # map md5 audio hash to expected_path_map
            if tr.audio_md5_signature in audio_signature_map:
                print("\n")
                logger.warning("| ----- --------------- ----- |")
                logger.warning(f"Input audio signature map {tr.audio_md5_signature} already exists; duplicate collision !!!")
                logger.warning(f"Current file: {tr.src_path}")
                logger.warning(f"Existing file: {audio_signature_map[tr.audio_md5_signature]}")
                logger.warning("| ----- --------------- ----- |")
                continue
            audio_signature_map[tr.audio_md5_signature] = expected_path_map[dst]
        except Exception as e:
            logger.error(f"Error processing input file {p}: {e}")

    return audio_signature_map, expected_path_map

def build_required_output_maps(output_dir: Path, hash_cache: HashCache) -> \
        [Dict[Path, Tuple[Path, Track]]]:
    """
        Return the required maps...
        - audio_signature_map: audio_signature -> expected_path_map
        - expected_path_map: expected_dst_path -> {origin_path, Track}
    """
    table = "out_cache"
    audio_signature_map: Dict[str, Tuple[Path, Track]] = {}
    output_paths = scan_input_dir(output_dir)

    it = tqdm(output_paths, desc="Scanning output metadata", unit="file") if TQDM_AVAILABLE else output_paths
    for p in it:
        try:
            # track should always update the cache...
            tr = Track.from_file(p, hash_cache, table)
            # map md5 audio hash to path and object
            if tr.audio_md5_signature in audio_signature_map:
                print("\n")
                logger.warning("| ----- --------------- ----- |")
                logger.warning(f"Output audio signature map {tr.audio_md5_signature} already exists; duplicate collision !!!")
                logger.warning(f"Current file: {tr.src_path}")
                logger.warning(f"Existing file: {audio_signature_map[tr.audio_md5_signature]}")
                logger.warning("| ----- --------------- ----- |")
                continue
            audio_signature_map[tr.audio_md5_signature] = (p, tr)
        except Exception as e:
            logger.error(f"Error processing input file {p}: {e}")

    return audio_signature_map