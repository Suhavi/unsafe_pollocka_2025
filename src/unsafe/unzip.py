"""
Utility for discovering and extracting archive files inside a raw data tree.

* Finds “.zip” and “.7z” archives (ignoring hidden files/folders).
* Recreates the original directory hierarchy under a user‑specified *unzip_dir*.
* If several archives would extract to the same parent directory, a sub‑folder
  named after the archive is created to avoid collisions.
    
    import unsafe.unzip as ununzip

    ununzip.unzip_raw(
        raw_root="data/raw",
        unzip_root="data/unzipped"
    )
"""

# Packages
from __future__ import annotations

import os
from os.path import join
from pathlib import Path
from zipfile import ZipFile
import zipfile_deflate64
import py7zr
from collections import Counter
import logging
import sys
from typing import Union, List


# ----------------------------------------------------------------------
# Logging
# ----------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

# ----------------------------------------------------------------------
# Constants
# ----------------------------------------------------------------------
ZIP_SUFFIXES = (".zip", ".7z")   # <- add more extensions here if needed

# ----------------------------------------------------------------------
# Helper functions
# ----------------------------------------------------------------------
def _is_visible(p: Path) -> bool:
    """Return ``True`` if the file/path is not hidden (does not start with a dot)."""
    return not any(part.startswith(".") for part in p.parts)

def _to_path(p: Union[str, Path]) -> Path:
    """Coerce *p* to a pathlib.Path (handles str, bytes, os.PathLike)."""
    if isinstance(p, Path):
        return p
    # ``os.PathLike`` covers PurePath, pathlib.PathLike objects, etc.
    if isinstance(p, (str, bytes, os.PathLike)):
        return Path(p).expanduser().resolve()
    raise TypeError(f"Expected str | pathlib.Path, got {type(p)!r}")

def _extract_archive(archive_path: Path, dest_dir: Path) -> None:
    """
    Dispatch extraction based on file suffix.

    Supports:
        *.zip* – via :class:`zipfile.ZipFile`
        *.7z*  – via :mod:`py7zr`

    Raises
    ------
    ValueError
        If the suffix is not recognised.
    """
    suffix = archive_path.suffix.lower()
    if suffix == ".zip":
        with ZipFile(archive_path, "r") as zip_ref:
            zip_ref.extractall(dest_dir)
    elif suffix == ".7z":
        with py7zr.SevenZipFile(archive_path, mode="r") as sz:
            sz.extractall(dest_dir)
    else:
        raise ValueError(f"Unsupported archive type: {suffix}")

# ----------------------------------------------------------------------
# Main functionality
# ----------------------------------------------------------------------
def zipped_downloads(fr: Union[str, Path]) -> List[Path]:
    """Return a list of visible *.zip* and *.7z* files under *fr*."""
    fr = _to_path(fr)
    zip_list: List[Path] = []
    for suffix in ZIP_SUFFIXES:
        for path in fr.rglob(f"*{suffix}"):
            if _is_visible(path):
                zip_list.append(path)
    return zip_list


def unzipped_dirs(fr: Union[str, Path], unzip_dir: Union[str, Path]) -> List[Path]:
    """Create the destination directories that mirror the archive layout."""
    fr = _to_path(fr)
    unzip_dir = _to_path(unzip_dir)

    unzip_list: List[Path] = []
    for suffix in ZIP_SUFFIXES:
        for path in fr.rglob(f"*{suffix}"):
            if _is_visible(path):
                zip_root = path.relative_to(fr).parent
                dest = unzip_dir / zip_root
                dest.mkdir(parents=True, exist_ok=True)
                unzip_list.append(dest)
    return unzip_list


def unzip_raw(fr: Union[str, Path], unzip_dir: Union[str, Path]) -> None:
    """
    Extract every archive found under *fr* into a directory tree that
    mirrors the archive’s position inside the overall raw tree.

    Parameters
    ----------
    fr :
        The directory that contains the archives (any sub‑directory of the
        overall ``data/raw`` tree).
    unzip_dir :
        The sibling ``.../unzipped`` directory.  The function determines the
        proper destination for each archive by looking at the path **relative to
        ``unzip_dir.parent``** (the common ``data/raw`` root).

    Example
    -------
    >>> raw_root   = Path("data/raw/external/hazard/gc/response")
    >>> unzip_root = Path("data/raw/unzipped")
    >>> unzip_raw(raw_root, unzip_root)
    # Files from all archives end up in:
    # data/raw/unzipped/external/hazard/gc/response/
    """
    fr = _to_path(fr)
    unzip_dir = _to_path(unzip_dir)

    # Locate all archive files (order is deterministic: rglob walks alphabetically)
    archives = zipped_downloads(fr)

    # Raw root
    raw_base = unzip_dir.parent

    for archive_path in archives:
        # ``archive_path.parent`` is the folder that holds the archive.
        # ``relative_to(raw_base)`` strips the common ``data/raw`` prefix,
        # leaving e.g. ``external/hazard/gc/response``.
        rel_parent = archive_path.parent.relative_to(raw_base)

        # Destination mirrors that relative path under ``unzip_dir``.
        dest_dir = unzip_dir / rel_parent
        dest_dir.mkdir(parents=True, exist_ok=True)

        log.info("Extracting %s → %s", archive_path.name, dest_dir)
        _extract_archive(archive_path, dest_dir)
        log.info("Finished: %s", archive_path.stem)