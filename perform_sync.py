# ---------- main sync logic ----------
import logging
from pathlib import Path
from typing import Dict, Any, List

from HashCache import HashCache
from scanner import build_required_input_maps, build_required_output_maps, scan_output_dir, discover_tracks
from track import Track
from utils import file_move, transactional_copy

from scanner import scan_directory_for_flacs

# external libs
try:
    from tqdm import tqdm

    TQDM_AVAILABLE = True
except ModuleNotFoundError:
    TQDM_AVAILABLE = False

# ---------- logging ----------
logger = logging.getLogger(__name__)


def old_determine_move_tasks(output_dir, input_audio_signature_map, output_audio_signature_map):
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

def determine_move_tasks(input_map: Dict[str, List[Track]], output_map: Dict[str, Track], output_dir: Path):
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
        if len(input_map) > 1:
            logger.info(f"Detected one or more duplicates for input signatures, {sig} : see {new_intr}")
            sig_movable = False
        if len(output_map) > 1:
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
            move_tasks.append(current_outtr)
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

    # this step is only useful if there happens to be no meaningful metadata changes
    # but somehow items are in the wrong place

    movable_tasks = determine_move_tasks(input_audio_to_sigs, output_audio_to_sigs, output_dir)

    skip = False

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
    #output_audio_signature_map = build_required_output_maps(output_dir, hash_cache)
    #stats["total_out_found"] = len(output_audio_signature_map)

    # 0. Delete any duplicates...



    # 1. Determine movable files...
    #move_tasks, skippable_destination_paths = determine_move_tasks(
    #    output_dir, input_audio_signature_map, output_audio_signature_map)

    #for _ in [input_audio_signature_map[x] for x in
     #set(input_audio_signature_map.keys()).difference(set(output_audio_signature_map.keys()))]:
    #    logger.info(_)

    # 1.5 Perform move tasks...
    #it = tqdm(move_tasks, desc="Moving files...", unit="file") if TQDM_AVAILABLE else move_tasks
    #for src, dst, tr in it:
    #    file_move(src, dst)
        # update cache with new information
    #    hash_cache.remove(src, "out_cache")
    #    hash_cache.set(dst, "out_cache", tr)

    # 2. Determine new copy tasks...
    # - for each expected dst, decide if we need to copy/overwrite
    logger.info("| ----- --------------- ----- |")
    logger.info("| ----- stage 2: copies ----- |")
    logger.info("| ----- --------------- ----- |")

    #if len(move_tasks) > 0:
    #    # reload all metadata from input
    #
    #    stats["total_in_found"] = len(expected_path_map)

    #    # reload all metadata from output
    #    output_audio_signature_map = build_required_output_maps(output_dir, hash_cache)
    #    stats["total_out_found"] = len(output_audio_signature_map)

    copy_tasks = []

    items = list(expected_path_map.items())
    it = tqdm(items, desc="Determining required copies...", unit="file") if TQDM_AVAILABLE else items

    for dst, (src, src_tr) in it:
        # this is some god awful code, but it makes like ugh
        # should rewrite this into a switch statement
        # problem: output sigs may not be correct because it currently cannot handle duplicates...
        # solution: move away from using audio signature matching
        # problem: it's gonna take a bit of re-engineering of the underhood/building...
        # problem: currently, i think theres no guarantee that it will copy properly...
        # im so confused as to why it seems like there are... different ways/ Shan

        check_dst = dst.exists() and dst.is_file()
        check_dst_matches_expected_dst = dst == src_tr.expected_output_path(output_dir)
        #check_src_audio_exists_in_output = src_tr.audio_md5_signature in output_audio_signature_map
        check_src_audio_exists_in_output = check_dst
        check_metadata = False
        if check_src_audio_exists_in_output:
            dst_tr = Track.from_file(dst, None, None)
            check_metadata = src_tr.metadata_hash == dst_tr.metadata_hash

        validity = check_dst and check_dst_matches_expected_dst and check_src_audio_exists_in_output and check_metadata
        if validity:
            # passed all checks
            continue

        # failed one or more checks...
        if not check_dst:
            logger.error(f"[dest] file not found: {src} ")
        if not check_dst_matches_expected_dst:
            logger.debug(f"[cmp] mismatch file-path: {src} -> {dst}")
        #if not check_src_audio_exists_in_output:
        #    logger.warning(f"[sanity]: src file should not exist in dst, but yet, does...")
        if not check_metadata:
            logger.warning(f"[cmp]: metadata mismatch: {src} -> {dst}")

        logger.info(f"[copy] {src} -> {dst}")
        copy_tasks.append((src, dst, src_tr, False))
        stats["to_copy"] += 1

    logger.info(f"Identified {len(copy_tasks)} required copies...")

    it = tqdm(copy_tasks, desc="Copying...", unit="file") if TQDM_AVAILABLE else copy_tasks
    for task in it:
        src, dst, tr, boolean_to = task
        try:
            transactional_copy(src, dst, dry_run=dry_run)
            stats["copied"] += 1
            hash_cache.set(dst, "out_cache", tr)
        except Exception as e:
            logger.error(f"{e}")

    # Delete any existing file that is not an expected dst
    logger.info("| ----- ----------------- ----- |")
    logger.info("| ----- stage 3: deletion ----- |")
    logger.info("| ----- ----------------- ----- |")

    output_audio_signature_map, existing_paths = build_required_output_maps(output_dir, hash_cache)
    #existing_paths = scan_output_dir(output_dir, True)
    existing_abs_p = set([str(_) for _ in existing_paths])
    predicted_abs_p = set([str(_) for _ in expected_path_map])
    logger.info(f"Updated output dir: {len(existing_abs_p)} files found...")
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
