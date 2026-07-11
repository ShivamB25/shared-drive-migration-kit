"""Safe path and byte-size helpers shared by migration adapters."""

from __future__ import annotations

from pathlib import Path


def parse_bytes(value: str | int) -> int:
    """Parse a decimal or binary byte quantity such as ``512M`` or ``200GiB``."""
    if isinstance(value, int):
        return value
    text = value.strip()
    if not text:
        raise ValueError("empty byte size")
    units = {
        "b": 1,
        "kb": 1000,
        "mb": 1000**2,
        "gb": 1000**3,
        "tb": 1000**4,
        "kib": 1024,
        "mib": 1024**2,
        "gib": 1024**3,
        "tib": 1024**4,
    }
    number = ""
    suffix = ""
    for char in text:
        if char.isdigit() or char == ".":
            number += char
        elif not char.isspace():
            suffix += char.lower()
    if not number:
        raise ValueError(f"invalid byte size: {value}")
    suffix = suffix or "b"
    if suffix not in units:
        raise ValueError(f"unknown byte unit in {value}")
    return int(float(number) * units[suffix])


def clean_relative_path(value: str) -> Path:
    """Normalize an adapter path and reject absolute or parent-traversal paths."""
    value = value.strip().strip("/")
    if not value:
        return Path()
    path = Path(value)
    if path.is_absolute() or ".." in path.parts:
        raise ValueError(f"path must be relative and stay inside the migration root: {value}")
    return path


def ensure_inside(path: Path, root: Path) -> None:
    """Raise if ``path`` resolves outside ``root``."""
    path.resolve().relative_to(root.resolve())


def posix_join(*parts: str) -> str:
    """Join non-empty path fragments into a normalized portable path string."""
    return "/".join(part.strip("/") for part in parts if part and part.strip("/"))


def relative_path_under_prefix(path: str, prefix: str) -> str:
    """Return ``path`` relative to ``prefix`` without assuming the prefix exists."""
    clean_path = path.strip("/")
    clean_prefix = prefix.strip("/")
    if not clean_prefix:
        return clean_path
    if clean_path == clean_prefix:
        return ""
    prefix_with_slash = f"{clean_prefix}/"
    if clean_path.startswith(prefix_with_slash):
        return clean_path[len(prefix_with_slash) :]
    return clean_path
