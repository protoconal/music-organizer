# ---------- main sync logic ----------
import logging
from pathlib import Path
from typing import Dict, Any, List

from HashCache import HashCache
from scanner import discover_tracks
from scanner import scan_directory_for_flacs
from track import Track
from utils import file_move, transactional_copy

# external libs
try:
    from tqdm import tqdm

    TQDM_AVAILABLE = True
except ModuleNotFoundError:
    TQDM_AVAILABLE = False

# ---------- logging ----------
logger = logging.getLogger(__name__)


def determine_move_tasks(input_map: Dict[str, List[Track]], output_map: Dict[str, List[Track]], output_dir: Path):
    # so to determine if a file is an input is movable,
    # the same md5 hash must exist in both the input and output...
    # and also share the same meta_hashes

    move_tasks = []
    # determine intersection
    in_sigs = set(input_map.keys())
    out_sigs = set(output_map.keys())
    movables = in_sigs.intersection(out_sigs)

    # determine the current states
    for sig in movables:
        sig_movable = True
        new_intr = input_map[sig]
        current_outtr = output_map[sig]

        # ensure that only one track exists per sig
        if len(new_intr) > 1:
            logger.info(f"Detected one or more duplicates for input signatures, {sig} : see {new_intr}")
            sig_movable = False
        if len(current_outtr) > 1:
            logger.info(f"Detected one or more duplicates for output signatures, {sig} : see {current_outtr}")
            sig_movable = False

        # unpack them
        new_intr = new_intr[0]
        current_outtr = current_outtr[0]

        current_file_path = current_outtr.abs_path
        new_file_path = new_intr.expected_output_path(output_dir)

        check_file_path = current_file_path != new_file_path
        check_metadata = new_intr.metadata_hash == current_outtr.metadata_hash
        if check_file_path and check_metadata and sig_movable:
            # track must be movable
            move_tasks.append((current_outtr, current_file_path, new_file_path))
    logger.info(f"Identified {len(move_tasks)} movable candidates...")
    return move_tasks

def perform_sync(
        input_dir: Path,
        output_dir: Path,
        dry_run: bool,
        hash_cache: HashCache,
        config: Dict[str, Any]
) -> Dict[str, int]:
    """
    expected_by_dst: dst_path -> (src_path, Track)
    existing_paths: set of Path
    """

    stats = {
        "movables": 0,
        "to_copy": 0,
        "copied": 0,
        "overwritten": 0,
        "skipped": 0,
        "deleted": 0,
        "errors": 0,
    }

    # scan input for changes

    logger.info("| ----- -------------- ----- |")
    logger.info("| ----- pre-stage 1: scanning current state ----- |")
    logger.info("| ----- -------------- ----- |")

    # must scan input and output directories...

    keep_empty_directories = bool(config.get("keep_empty_directories"))

    discovered_input_files, input_tracks, input_audio_to_sigs = discover_tracks(input_dir, hash_cache, "in_cache", keep_empty_directories)
    discovered_output_files, output_tracks, output_audio_to_sigs = discover_tracks(output_dir, hash_cache, "out_cache", keep_empty_directories)


    logger.info("| ----- -------------- ----- |")
    logger.info("| ----- stage 1: calculating movable items ----- |")
    logger.info("| ----- -------------- ----- |")
    # to calculate the possible movable items is to determine which audio tags are the same
    # with the same metadata

    # this step is only useful if there happens to be no meaningful metadata changes,
    # but somehow items are in the wrong place

    movable_tasks = determine_move_tasks(input_audio_to_sigs, output_audio_to_sigs, output_dir)

    logger.info("| ----- ---------------- ----- |")
    logger.info("| ----- stage 1.5: moves ----- |")
    logger.info("| ----- ---------------- ----- |")


    stats["movables"] = len(movable_tasks)

    movable_tasks = tqdm(movable_tasks, desc="Moving files...", unit="file") if TQDM_AVAILABLE else movable_tasks
    for current_outtr, current_file_path, new_file_path in movable_tasks:
        # move the file
        file_move(current_file_path, new_file_path)
        # retrieve new track information from the file
        new_outtr = Track.from_file(new_file_path, hash_cache, "out_cache")
        hash_cache.remove(current_file_path, "out_cache")
        # replace output_track
        indx = output_tracks.index(current_outtr)
        output_tracks[indx] = new_outtr
        # replace sig
        output_audio_to_sigs[new_outtr.md5_audsig] = new_outtr

    logger.info("| ----- --------------- ----- |")
    logger.info("| ----- stage 2: copies ----- |")
    logger.info("| ----- --------------- ----- |")

    copy_tasks = []

    it_input_tracks = tqdm(input_tracks, desc="Determining required copies...", unit="file") if TQDM_AVAILABLE else input_tracks
    for src_tr in it_input_tracks:
        in_abs_path = src_tr.abs_path
        predicted_abs_path = src_tr.expected_output_path(output_dir)

        # check if it exists
        if predicted_abs_path.exists():
            # we likely scanned it on the entry
            src_tr_audsig = src_tr.md5_audsig
            # let's see if we can find it via audio_sig
            if src_tr_audsig in output_audio_to_sigs:
                matches = output_audio_to_sigs[src_tr_audsig]
                out_tr = None
                for out_tr in matches:
                    match_path = out_tr.abs_path
                    if match_path == predicted_abs_path:
                        stats["skipped"] += 1
                        break
                if out_tr and out_tr.metadata_hash == src_tr.metadata_hash:
                    logger.debug(f"[cmp] up-to-date {out_tr.abs_path} - {out_tr.metadata_hash} : {src_tr.abs_path} - {src_tr.metadata_hash} ")
                    # do nothing and continue on,
                    continue
                # must have failed to find match... overwriting
            # must have failed to find sig in output... overwriting
            logger.info(f"[overwrite] file exists, but cannot be found in output sigs, internal data integrity is compromised")
        else:
            logger.info(f"[missing] will copy ")
        # otherwise, must copy
        copy_tasks.append((in_abs_path, predicted_abs_path, src_tr, False))

    logger.info(f"Identified {len(copy_tasks)} required copies...")

    it_copy_tasks = tqdm(copy_tasks, desc="Copying...", unit="file") if TQDM_AVAILABLE else copy_tasks
    for task in it_copy_tasks:
        src, dst, tr, boolean_to = task
        try:
            transactional_copy(src, dst, dry_run=dry_run)
            stats["copied"] += 1
            hash_cache.set_from_track_obj(dst, "out_cache", tr)
        except Exception as e:
            logger.error(f"{e}")

    # Delete any existing file that is not an expected dst
    logger.info("| ----- ----------------- ----- |")
    logger.info("| ----- stage 3: deletion ----- |")
    logger.info("| ----- ----------------- ----- |")

    output_existing_abs_paths = scan_directory_for_flacs(output_dir, keep_empty_directories)
    predicted_abs_p = set([_.expected_output_path(output_dir) for _ in input_tracks])

    logger.info(f"Updated output dir: {len(output_existing_abs_paths)} files found...")
    logger.info(f"Expecting: {len(predicted_abs_p)} files...")
    #to_delete = [Path(_) for _ in existing_abs_p if _ not in predicted_abs_p]
    to_delete = [Path(_) for _ in output_existing_abs_paths.difference(predicted_abs_p)]
    missing = [Path(_) for _ in predicted_abs_p.difference(output_existing_abs_paths)]
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


    stats["total_inputs_found"] = len(predicted_abs_p)
    stats["total_outputs_found"] = len(output_existing_abs_paths)

    return stats
