
# Full Copy Sync (FLAC Organizer) - Jellyfin Folder Organizer

A command-line tool / Python module that **organizes FLAC music libraries** by **copying** `.flac` files from an input directory into a new metadata-based folder structure.

This tool is useful for creating a clean, organized mirror of an existing library without modifying the original files.
**ahem** like the structure required for Jellyfin

**This README was generated with ChatGPT 5.2**

---

## Features

* Copies `.flac` files from input → output
* Builds folder structure using embedded FLAC metadata (e.g., artist/album)
* Adds a short hash suffix to prevent filename collisions
* Supports:
  * JSON config file
  * ~~dry-run mode~~
  * logging + adjustable verbosity
  * caching
  * automatic removal of empty directories

---

## Requirements

* Python 3.x
* mutagen
* tqdm (*optional*)

---

## Usage

### Basic run

```bash
python full_copy_sync.py
```

By default it will:

* read config from `./fullcopy_config.json`
* scan input directory `./test_files/input_dir`
* write organized output to `./test_files/output_dir`
  * you should review the default options found in `./fullcopy_config.json`

---

## CLI Options

### `--config`, `-c`

Path to a JSON config file.

```bash
python full_copy_sync.py --config ./fullcopy_config.json
```

Default:

* `./fullcopy_config.json`

---

### `--save-config`

Save the fully resolved configuration (including defaults and CLI overrides) back to the config file.

```bash
python full_copy_sync.py --save-config
```

This is useful for generating a complete config template.

---

### `--input`, `-i`

Input directory containing `.flac` files.

```bash
python full_copy_sync.py --input "/path/to/music"
```

Default:

* `./input_music`

---

### `--output`, `-o`

Output base directory where organized files will be copied.

```bash
python full_copy_sync.py --output "/path/to/organized"
```

Default:

* `./organized_music`

---

### `--hash-length`, `-l`

Length of a short hash suffix added to filenames to prevent collisions.

Example:

* `Intro.flac`
* `Intro_0xab12.flac`

```bash
python full_copy_sync.py --hash-length 4
```

Default:

* `4`

---

### `--dry-run`

Print actions without copying or modifying anything.

```bash
python full_copy_sync.py --dry-run
```

---

### `--verbosity`, `-v`

Logging verbosity level.

* `0` = WARNING
* `1` = INFO
* `2` = DEBUG

```bash
python full_copy_sync.py --verbosity 2
```

Default:

* `1`

---

### `--disable-cache`, `-C`

Disable caching entirely.

```bash
python full_copy_sync.py --disable-cache
# or
python full_copy_sync.py -C
```

---

### `--log-file`

Path to output log file.

```bash
python full_copy_sync.py --log-file ./logs/full_copy_sync.log
```

Default:

* `full_copy_sync.log`

---

### `--skip-input-caching`

Disable caching on the input side (metadata/hash caching for source FLAC files).

```bash
python full_copy_sync.py --skip-input-caching
```

Use this if:

* you suspect metadata changes
* you want a fully fresh scan each run

---

### `--keep-empty-directories`

Do not remove empty directories during cleanup.

```bash
python full_copy_sync.py --keep-empty-directories
```

---

## Example Commands

### Copy and organize a library

```bash
python full_copy_sync.py -i "/mnt/music_in" -o "/mnt/music_out"
```

### Preview what would happen

```bash
python full_copy_sync.py -i "/mnt/music_in" -o "/mnt/music_out" --dry-run
```

### Debug run (verbose logs)

```bash
python full_copy_sync.py -v 2 --log-file debug.log
```

### Disable caching for maximum correctness

```bash
python full_copy_sync.py -C --skip-input-caching
```

---

## Notes on Organization Structure

The output folder structure is built from FLAC tags such as:

* Artist / Album

---

## TODO:

- implement custom / `%artist%. %title%` formatting strings
  - refactor codebase so that i use a library and i dont implement it myself
- check if multiple of the snippets of code can be just like removed
- run / battle test this,
  - add so much more debugging / logging and implement custom trace levels
- but it should work theoretically.
- write tests.
---
