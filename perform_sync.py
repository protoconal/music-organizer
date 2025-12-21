# ---------- main sync logic ----------
import logging
from pathlib import Path
from typing import Dict

from file_sync.HashCache import HashCache
from file_sync.scanner import build_required_input_maps, build_required_output_maps, scan_output_dir
from file_sync.utils import file_move, transactional_copy

# external libs
try:
    from tqdm import tqdm

    TQDM_AVAILABLE = True
except ModuleNotFoundError:
    TQDM_AVAILABLE = False

# ---------- logging ----------
logger = logging.getLogger("music_copysync")


def determine_move_tasks(output_dir, input_audio_signature_map, output_audio_signature_map):
    skippable_destination_paths = set()
    move_tasks = []
    # determine any potential moves by intersecting the input_audio_signatures with the output_audio_signatures
    in_audio_sig = set(input_audio_signature_map.keys())
    out_audio_sig = set(output_audio_signature_map.keys())
    potential_moves = in_audio_sig.intersection(out_audio_sig)
    logger.info(f"Found {len(potential_moves)} movable candidates, determining eligibility")
    it = tqdm(potential_moves, desc="Determining moves...", unit="file") if TQDM_AVAILABLE else potential_moves
    # for every move, check whether the FLAC metadata is the same
    for sig in it:
        in_path, in_track = input_audio_signature_map[sig]
        out_path, out_track = output_audio_signature_map[sig]
        if in_track.metadata_hash == out_track.metadata_hash:
            # they should have the exact same information, therefore, movable candidate
            out_src = out_path
            out_dst = in_track.expected_output_path(output_dir)
            # check their paths, directly
            if str(out_src) != str(out_dst):
                skippable_destination_paths.add(in_path)
                move_tasks.append((out_src, out_dst, in_track))
    logger.info(f"Identified {len(move_tasks)} movable candidates...")
    return move_tasks, skippable_destination_paths


def perform_sync(
        input_dir: Path,
        output_dir: Path,
        dry_run: bool,
        hash_cache: HashCache) -> Dict[str, int]:
    """
    expected_by_dst: dst_path -> (src_path, Track)
    existing_paths: set of Path
    """
    stats = {
        "to_copy": 0,
        "copied": 0,
        "overwritten": 0,
        "skipped": 0,
        "deleted": 0,
        "errors": 0,
    }

    logger.info("| ----- -------------- ----- |")
    logger.info("| ----- stage 1: moves ----- |")
    logger.info("| ----- -------------- ----- |")

    # load all metadata from input
    input_audio_signature_map, expected_path_map = build_required_input_maps(input_dir, output_dir, hash_cache)
    stats["total_in_found"] = len(expected_path_map)

    # load all metadata from output
    output_audio_signature_map = build_required_output_maps(output_dir, hash_cache)
    stats["total_out_found"] = len(output_audio_signature_map)

    # 1. Determine movable files...
    move_tasks, skippable_destination_paths = determine_move_tasks(
        output_dir, input_audio_signature_map, output_audio_signature_map)

    for _ in [input_audio_signature_map[x] for x in
     set(input_audio_signature_map.keys()).difference(set(output_audio_signature_map.keys()))]:
        logger.info(_)

    # 1.5 Perform move tasks...
    it = tqdm(move_tasks, desc="Moving files...", unit="file") if TQDM_AVAILABLE else move_tasks
    for src, dst, tr in it:
        file_move(src, dst)
        # update cache with new information
        hash_cache.remove(src, "out_cache")
        hash_cache.set(dst, "out_cache", tr)
        # update map with new changes
        # - audio_signature_map: audio_signature -> expected_path_map
        # - expected_path_map: expected_dst_path -> {origin_path, Track}
        output_audio_signature_map[tr.audio_md5_signature] = (dst, tr)

    # 2. Determine new copy tasks...
    # - for each expected dst, decide if we need to copy/overwrite
    logger.info("| ----- --------------- ----- |")
    logger.info("| ----- stage 2: copies ----- |")
    logger.info("| ----- --------------- ----- |")

    copy_tasks = []

    items = list(expected_path_map.items())
    it = tqdm(items, desc="Determining required copies...", unit="file") if TQDM_AVAILABLE else items

    for dst, (src, src_tr) in it:
        dst_tr = output_audio_signature_map[src_tr.audio_md5_signature]
        if dst.exists() and \
                dst.is_file() and \
                dst == src_tr.expected_output_path() and \
                src_tr.metadata_hash == dst_tr.metadata_hash:
            continue
        if dst != src_tr.expected_output_path():
            logger.debug(f"[cmp] mismatch file-path: {src} -> {dst}")
        elif src_tr.metadata_hash != dst_tr.metadata_hash:
            logger.debug(f"[cmp] metadata mismatch -> overwrite: {src} -> {dst}")
        else:
            logger.info(f"[copy] dst missing -> new copy: {src} -> {dst}")
        copy_tasks.append((src, dst, src_tr, False))
        stats["to_copy"] += 1

    logger.info(f"Identified {len(copy_tasks)} required copies...")

    it = tqdm(copy_tasks, desc="Copying...", unit="file") if TQDM_AVAILABLE else copy_tasks
    for task in it:
        src, dst, tr, boolean_to = task
        transactional_copy(src, dst, dry_run=dry_run)
        stats["copied"] += 1
        hash_cache.set(dst, "out_cache", tr)

    # Delete any existing file that is not an expected dst
    logger.info("| ----- ----------------- ----- |")
    logger.info("| ----- stage 3: deletion ----- |")
    logger.info("| ----- ----------------- ----- |")
    existing_paths = scan_output_dir(output_dir, True)
    existing_abs_p = set([str(_) for _ in existing_paths])
    predicted_abs_p = set([str(_) for _ in expected_path_map])
    logger.info(f"Updated output dir: {len(existing_paths)} files found...")
    logger.info(f"Expecting: {len(predicted_abs_p)} file...")
    #to_delete = [Path(_) for _ in existing_abs_p if _ not in predicted_abs_p]
    to_delete = [Path(_) for _ in existing_abs_p.difference(predicted_abs_p)]
    missing = [Path(_) for _ in predicted_abs_p.difference(existing_abs_p)]
    for p in to_delete:
        if dry_run:
            logger.info(f"[dry-run] would delete {p}")
        else:
            try:
                p.unlink()
                logger.info(f"DELETED: {p}")
                hash_cache.remove(p, "out_cache")
            except Exception as e:
                logger.error(f"Failed to delete {p}: {e}")
                stats["errors"] += 1
                continue
        stats["deleted"] += 1

    return stats
