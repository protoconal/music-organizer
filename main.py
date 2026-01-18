#!/usr/bin/env python3
"""
full_copy_sync.py — copy-sync FLAC files into a metadata-organized output,
"""
from __future__ import annotations

import time
from datetime import datetime

from HashCache import HashCache
from perform_sync import perform_sync
from utils import *

# ---------- configuration ----------
CACHE_FILENAME = "hashcache.sqlite"

# ---------- logging ----------
logger = logging.getLogger(__name__)

# ---------- CLI / Main ----------
def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Copy-sync FLAC files into metadata-organized output.")
    parser.add_argument("--config", "-c",
                        default="./fullcopy_config.json",
                        help="Path to JSON config file.")
    parser.add_argument("--save-config",
                        action="store_true",
                        help="Save resolved configuration back to --config.")
    parser.add_argument("--input", "-i",
                        default="./input_music",
                        help="Input directory (contains FLAC files).")
    parser.add_argument("--output", "-o",
                        default="./organized_music",
                        help="Output base directory.")
    parser.add_argument("--hash-length", "-l", type=int,
                        default=4,
                        help="Length of suffix (prevents filename collision).")
    parser.add_argument("--dry-run",
                        action="store_true",
                        help="Show actions without making changes.")
    parser.add_argument("--verbosity", "-v", type=int,
                        default=1,
                        help="Logging verbosity (0=WARNING,1=INFO,2=DEBUG).")
    parser.add_argument("--disable-cache", "-C",
                        action="store_true",
                        help="Will disable the cache.")
    parser.add_argument("--log-file",
                        default="full_copy_sync.log",
                        help="Log file path.")
    parser.add_argument("--skip-input-caching",
                        action="store_true",
                        help="Disable input-side metadata/hash caching.")
    parser.add_argument("--keep-empty-directories",
                        action="store_true",
                        help="Skip removing empty directories.")
    return parser

def main():
    parser = build_arg_parser()
    args = parser.parse_args()

    config = load_config_file(args.config)
    config = merge_config_with_args(config, args, parser)

    if args.save_config:
        save_config_file(args.config, config)

    input_dir = Path(config.get("input"))
    output_dir = Path(config.get("output"))
    dry_run = bool(config.get("dry_run"))
    verbosity = int(config.get("verbosity"))
    log_file = Path(config.get("log_file"))
    disable_cache = bool(config.get("disable_cache"))

    setup_logging(verbosity, log_file)

    start = time.time()
    logger.info(f"Starting copy-sync at {datetime.now().isoformat()}")
    logger.info(f"Input: {input_dir}  Output: {output_dir} Dry-run: {dry_run}")

    # Initialize cache (stored inside output_dir)
    hash_cache = HashCache(output_dir, disabled=disable_cache, cache_filename=CACHE_FILENAME)

    try:
        # Normal run
        stats = perform_sync(
            input_dir=input_dir,
            output_dir=output_dir,
            dry_run=dry_run,
            hash_cache=hash_cache,
            config=config
        )

        elapsed = time.time() - start

        # Summary
        logger.info("---- Summary ----")
        logger.info(f"Input FLACs scanned: {stats['total_inputs_found']}")
        logger.info(f"Output FLACs scanned: {stats['total_outputs_found']}")
        logger.info(f"Planned copies: {stats['to_copy']}")
        logger.info(f"Copied/overwritten : {stats['copied']}")
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
