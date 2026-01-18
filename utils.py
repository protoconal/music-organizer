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
from typing import Optional, Dict, List

# ---------- UTILITIES ----------

# ---------- constants ----------
INVALID_PATH_CHARS = r'\/:*?"<>|.'

# -------- logging utils --------
logger = logging.getLogger(__name__)


def setup_logging(verbosity: int, log_file: Optional[Path] = None):
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
        format="%(asctime)s [%(name)s] [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
        handlers=handlers,
    )

# https://stackoverflow.com/a/61762820
class PrefixLoggerAdapter(logging.LoggerAdapter):
    """ A logger adapter that adds a prefix to every message """
    def process(self, msg: str, kwargs: dict) -> (str, dict):
        return f'[{self.extra["nickname"]}] ' + msg, kwargs

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
class HashingHelper(object):
    _instance = None
    _initialized = False
    hashing_instance: hashlib
    algorithm: str
    DEBUG: bool

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, algorithm = None):
        if self._initialized:
            return
        self._initialized = True

        self.DEBUG = False

        # select_hashing_provider
        algorithm = "md5" if algorithm is None else algorithm.lower()
        # save name
        self.algorithm = algorithm
        # select default algo
        if algorithm == "md5":
            self.hashing_instance = hashlib.md5()
        elif algorithm == "sha256":
            self.hashing_instance = hashlib.sha256()
        elif algorithm == "blake3":
            try:
                import blake3
                self.hashing_instance = blake3.blake3()
            except ModuleNotFoundError:
                logger.error("non-fatal, 'blake3' not installed. Install with: pip install blake3")

    def _new_hasher_instance(self):
        return self.hashing_instance.copy()

    def _normalize_to_str(self, hashing_provider: hashlib) -> str:
        hash_str = hashing_provider.hexdigest().lower()
        return hash_str

    def hash_file(self, path: Path, chunk_size: int = 8192) -> str:
        """Return a file's hash in string hex digest format normalized to lowercase"""
        h = self._new_hasher_instance()
        # Source - https://stackoverflow.com/a/59056837
        # Posted by user3064538, modified by community.
        # Retrieved 2025-12-10, License - CC BY-SA 4.0
        with path.open("rb") as f:
            while chunk := f.read(chunk_size):
                h.update(chunk)
        hash_str = self._normalize_to_str(h)
        if self.DEBUG:
            logger.debug(f"Computed {self.algorithm} hash for file {path}: {hash_str}")
        return hash_str

    def hash_str_list(self, input_buffer: List[str]) -> str:
        """ Return a hash of stringified items in normalized hexadecimal string format (without 0x)."""
        h = self._new_hasher_instance()
        for s in input_buffer:
            h.update(str(s).encode("utf-8"))
        hash_str = self._normalize_to_str(h)
        if self.DEBUG:
            logger.debug(f"Computed {self.algorithm} hash for {input_buffer}: {hash_str}")
        return hash_str

    def hash_dict_vals(self, input_dict: Dict[str, str], keys: Optional[List[str]]) -> str:
        """ Return the hash of a dictionary's sorted values in normalized hexadecimal string format (without 0x).
            If any keys are provided, the order of the keys is preserved.
        """
        if keys:
            hash_vals = []
            # theoretically, the for_loop should keep them order of the keys
            for k in keys:
                if k not in input_dict:
                    logger.error(f"Provided key: {k} does not exist in input dictionary.")
                hash_vals.append(input_dict.get(k, ""))
        else:
            hash_vals = [str(_) for _ in input_dict.values()]
            hash_vals.sort()
        return self.hash_str_list(hash_vals)

def sanitize_for_path(s: str, max_len: int = 32) -> str:
    # GPT-generated
    s = "".join(ch for ch in s if ch not in INVALID_PATH_CHARS)
    s = re.sub(r"\s{2,}", " ", s).strip()
    # normalize text
    #s = unicodedata.normalize('NFKD', s).encode('ascii', 'ignore').decode('ascii')
    if len(s) > max_len:
        s = s[:max_len].strip()
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
