"""Portable primitives for planned, package-based migrations.

These helpers intentionally know nothing about Modal, rclone credentials, or a
particular source provider. Source and target adapters can share the planning,
path-safety, and retry invariants without sharing provider-specific code.
"""

from .paths import clean_relative_path, ensure_inside, parse_bytes, posix_join, relative_path_under_prefix
from .planning import (
    archive_triplet_paths,
    package_strategy,
    pack_contiguous_members,
    row_belongs_to_worker,
)
from .retries import retry_with_exponential_backoff

__all__ = [
    "archive_triplet_paths",
    "clean_relative_path",
    "ensure_inside",
    "package_strategy",
    "pack_contiguous_members",
    "parse_bytes",
    "posix_join",
    "relative_path_under_prefix",
    "retry_with_exponential_backoff",
    "row_belongs_to_worker",
]
