import logging
import os
from pathlib import Path
from typing import Dict, Tuple, List

from HashCache import HashCache
from track import Track

# external libs
try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ModuleNotFoundError:
    TQDM_AVAILABLE = False

# -------- logging utils --------
logger = logging.getLogger(__name__)

# ---------- scanning helpers ----------

def scan_directory_for_flacs(directory: Path, remove_empty_dir: bool = True) -> set[Path]:
    """
    Returns a set of Path objects pointing to any discovered files ending in FLAC.
      - remove_empty_dir - will remove any empty directories during its walk.
    """
    logger.info(f"Running discovery for FLACs in {directory}")
    flac_paths = set()

    if not directory.exists():
        logger.info(f"Directory {directory} does not exist, creating empty folder.")
        directory.mkdir(exist_ok=True)

    for root, dirs, files in os.walk(directory):
        p = Path(root)
        if remove_empty_dir and not any(p.iterdir()):
            p.rmdir()
        for f in files:
            if f.lower().endswith(".flac"):
                fpath = Path(root) / f
                flac_paths.add(fpath.resolve())

    logger.info(f"Discovered {len(flac_paths)} FLACs in {directory}")
    return flac_paths

def discover_tracks(directory: Path, hash_cache: HashCache, table: str, keep_empty_directories: bool):
    discovered_file_paths = scan_directory_for_flacs(directory, keep_empty_directories)
    # just in case that the MD5s cause collisions, we should append them to lists
    discovered_tracks = []
    sigs_to_tracks = {}
    possible_duplicate_sigs = set()
    # md5_audsig => list[track]
    logger.info("Retrieving metadata...")

    # wrapping it in tqdm
    discovered_file_paths = tqdm(discovered_file_paths,
                                 desc="Scanning/retrieving metadata...",
                                 unit="file") if TQDM_AVAILABLE else discovered_file_paths

    for file_path in discovered_file_paths:
        logger.debug(f"Attempting to retrieve {file_path}")
        # retrieve track
        tr = Track.from_cache(file_path, hash_cache, table)
        discovered_tracks.append(tr)
        # append
        audsig = tr.md5_audsig
        if audsig not in sigs_to_tracks:
            sigs_to_tracks[audsig] = [tr]
        else:
            sigs_to_tracks[audsig].append(tr)
            possible_duplicate_sigs.add(audsig)
            logger.info(f"WARN: Identified duplicate audio file with the same audio_signature. {audsig}")

    if len(possible_duplicate_sigs) > 0:
        logger.info("WARN: Identified duplicate audio files with the same audio signatures.")
        for sig in possible_duplicate_sigs:
            tracks = sigs_to_tracks[sig]
            logger.info("||||||||||||{sig}")
            for tr in tracks:
                logger.info(tr.human_readable())
            logger.info("||||||||||||{sig}")
    return discovered_file_paths, discovered_tracks, sigs_to_tracks