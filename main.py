#!/usr/bin/env python3
"""
full_copy_sync.py — copy-sync FLAC files into a metadata-organized output,
using BLAKE3 for full-file hashing and a SQLite-backed destination-only cache.

Features:
 - Full-file BLAKE3 hashing (fast + secure).
 - SQLite cache stored at <output_dir>/.hashcache.sqlite containing:
     - cache (destination file cache)
     - input_cache (input file metadata + hash)
 - Input-side caching (mtime+size) to speed rescans; can be disabled with --no-input-cache.
 - --full-rebuild : strict verification mode that re-hashes every input and every output,
                    updates caches, reports mismatches, and performs no copying/deleting.
 - Transactional copy mode (atomic replace), threaded copying, dry-run, progress bars.
"""
from __future__ import annotations

import time
from datetime import datetime

from file_sync.HashCache import HashCache
from file_sync.perform_sync import perform_sync
from file_sync.utils import *

# ---------- configuration ----------
CACHE_FILENAME = "hashcache.sqlite"

# ---------- logging ----------
logger = logging.getLogger("music_copysync")

# ---------- CLI / Main ----------
def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Copy-sync FLAC files into metadata-organized output.")
    parser.add_argument("--config", "-c", default="./fullcopy_config.json",
                        help="Path to JSON config file.")
    parser.add_argument("--save-config", action="store_true",
                        help="Save resolved configuration back to --config.")
    parser.add_argument("--input", "-i", default="./input_music",
                        help="Input directory (contains FLAC files).")
    parser.add_argument("--output", "-o", default="./organized_music",
                        help="Output base directory.")
    parser.add_argument("--hash-length", "-l", type=int, default=4,
                        help="Length of suffix (prevents filename collision).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show actions without making changes.")
    parser.add_argument("--verbosity", "-v", type=int, default=1,
                        help="Logging verbosity (0=WARNING,1=INFO,2=DEBUG).")
    parser.add_argument("--log-file", default="full_copy_sync.log",
                        help="Log file path.")
    #parser.add_argument("--no-input-cache", action="store_true",
    #                    help="Disable input-side metadata/hash caching.")
    parser.add_argument("--keep-empty-directories", action="store_true",
                        help="Skip removing empty directories.")
    return parser

def main():
    parser = build_arg_parser()
    args = parser.parse_args()

    config = load_config_file(args.config)
    merged = merge_config_with_args(config, args, parser)

    if args.save_config:
        save_config_file(args.config, merged)

    input_dir = Path(merged.get("input"))
    output_dir = Path(merged.get("output"))
    dry_run = bool(merged.get("dry_run"))
    verbosity = int(merged.get("verbosity"))
    log_file = Path(merged.get("log_file"))
    no_input_cache = bool(merged.get("no_input_cache"))
    keep_empty_directories = bool(merged.get("keep_empty_directories"))

    setup_logging(verbosity, log_file)

    start = time.time()
    logger.info(f"Starting copy-sync at {datetime.now().isoformat()}")
    logger.info(f"Input: {input_dir}  Output: {output_dir}   Dry-run: {dry_run}")

    # Initialize cache (stored inside output_dir)
    hash_cache = HashCache(output_dir, use_input_cache=not no_input_cache, cache_filename=CACHE_FILENAME)

    try:
        # Normal run
        stats = perform_sync(
            input_dir=input_dir,
            output_dir=output_dir,
            dry_run=dry_run,
            hash_cache=hash_cache
        )

        elapsed = time.time() - start

        # Summary
        logger.info("---- Summary ----")
        logger.info(f"Input FLACs scanned: {stats['total_in_found']}")
        logger.info(f"Existing output FLACs found: {stats['total_out_found']}")
        logger.info(f"Planned copies: {stats['to_copy']}")
        logger.info(f"Copied (new): {stats['copied']}")
        logger.info(f"Overwritten: {stats['overwritten']}")
        logger.info(f"Deleted: {stats['deleted']}")
        logger.info(f"Skipped (already up-to-date): {stats['skipped']}")
        logger.info(f"Errors: {stats['errors']}")
        logger.info(f"Elapsed time: {elapsed:.2f}s")
        logger.info("---- End ----")

    finally:
        # Close DB
        try:
            hash_cache.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
