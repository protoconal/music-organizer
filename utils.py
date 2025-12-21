from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Optional, Dict

# ---------- UTILITIES ----------

# ---------- constants ----------
INVALID_PATH_CHARS = r'\/:*?"<>|.'

# -------- logging utils --------
logger = logging.getLogger("music_copysync")


def setup_logging(verbosity: int, log_file: Optional[Path]):
    level = logging.WARNING
    if verbosity == 1:
        level = logging.INFO
    elif verbosity >= 2:
        level = logging.DEBUG

    handlers = [logging.StreamHandler(sys.stdout)]
    if log_file:
        handlers.append(logging.FileHandler(log_file, mode="a", encoding="utf-8"))

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
        handlers=handlers,
    )


# ---------- fs helpers ----------
def transactional_copy(src: Path, dst: Path, dry_run: bool = False) -> None:
    """
    GPT-generated: Transactional copy using shutil.copy2
    - copy to a temp file in dst dir then os.replace.
    - attempts to guarantee no partial file ever appears at dst.
    """
    dst.parent.mkdir(parents=True, exist_ok=True)

    if dry_run:
        logger.debug(f"[dry-run] would transactional copy2 {src} -> {dst}")
        return

    with tempfile.NamedTemporaryFile(delete=False, dir=dst.parent) as tf:
        tmp_path = Path(tf.name)
    try:
        shutil.copy2(src, tmp_path)
        os.replace(tmp_path, dst)
        logger.debug(f"transactional copy2 complete: {src} -> {dst}")
    except Exception:
        # Cleanup leftover temp file on error
        tmp_path.unlink(missing_ok=True)


def file_move(src: Path, dst: Path, dry_run: bool = False) -> None:
    if dry_run:
        logger.debug(f"[dry-run] would move {src} -> {dst}")
    else:
        try:
            os.replace(src, dst)
        except Exception:
            pass
    return


# ---------- hashing helpers ----------
def compute_file_hash(path: Path, algo: str = "md5", chunk_size: int = 8192) -> str:
    """Compute full-file hash hex digest (lowercase) using provided algo"""
    h = _select_hashing_algo(algo)
    # Source - https://stackoverflow.com/a/59056837
    # Posted by user3064538, modified by community.
    # Retrieved 2025-12-10, License - CC BY-SA 4.0
    with path.open("rb") as f:
        while chunk := f.read(chunk_size):
            h.update(chunk)
    hash_str = h.hexdigest().lower()
    # logger.debug(f"computed {algo} hash for {path}: {hash_str}")
    return hash_str


def compute_list_str_hash(input_buffer: [str], algo: str = "blake3") -> str:
    """Compute string list hash hex digest (lowercase) using provided algo."""
    h = _select_hashing_algo(algo)
    for s in input_buffer:
        h.update(s.encode("utf-8"))
    hash_str = h.hexdigest().lower()
    # logger.debug(f"computed {algo} hash for {input_buffer}: {hash_str}")
    return hash_str


def _select_hashing_algo(algo: str):
    # normalize string, just in case
    algo = algo.lower()
    # default to md5
    h = hashlib.md5()
    # select algorithm
    if algo == "blake3":
        try:
            import blake3
            h = blake3.blake3()
        except ModuleNotFoundError:
            logger.error("non-fatal, 'blake3' not installed. Install with: pip install blake3")
    return h


def sanitize_for_path(s: str, max_len: int = 100) -> str:
    # GPT-generated
    s = "".join(ch for ch in s if ch not in INVALID_PATH_CHARS)
    s = re.sub(r"\s{2,}", " ", s).strip()
    if len(s) > max_len:
        s = s[:max_len]
    # logger.debug(f"sanitized path str: {s}")
    return s


# ---------- cli helpers ----------
def save_config_file(path: str, cfg: Dict):
    """Save JSON config."""
    try:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved merged configuration to {path}")
    except Exception as e:
        logger.error(f"Failed to save config to {path}: {e}")


def load_config_file(path: Optional[str]) -> Dict:
    """Load JSON. Return an object/dict."""
    if not path:
        return {}
    try:
        p = Path(path)
        if not p.exists():
            logger.error(f"Config file not found: {path}")
            return {}
        with p.open("r", encoding="utf-8") as f:
            raw = f.read()
            cfg = json.loads(raw)
            logger.info(f"Loaded config: {path}")
            return dict(cfg)
    except Exception as e:
        logger.error(f"Failed to load config file {path}: {e}")
        return {}


def merge_config_with_args(config: Dict, args: argparse.Namespace, parser: argparse.ArgumentParser) -> Dict:
    """
    Merge config dict with CLI args.
    Behavior: CLI overrides config. If CLI used default for a flag, use config value if present,
    otherwise keep the parser default.
    """
    merged = dict(config)
    for key, value in vars(args).items():
        if key in ("config", "save_config"):
            continue
        default = parser.get_default(key)
        if value != default:
            merged[key] = value
        else:
            merged[key] = config.get(key, default)
    return merged
