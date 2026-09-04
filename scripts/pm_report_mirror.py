"""Mirror private weekly report artifacts into the launchd runtime tree."""

from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path
from typing import Iterable


def mirror_report_artifacts(paths: Iterable[Path], target_dir: Path) -> dict[str, object]:
    """Atomically copy generated artifacts to the isolated Control Plane tree."""
    copied: list[str] = []
    errors: list[str] = []
    target_dir = target_dir.expanduser().resolve()
    target_dir.mkdir(parents=True, exist_ok=True)
    for raw_path in paths:
        source = Path(raw_path).expanduser().resolve()
        destination = target_dir / source.name
        try:
            if not source.is_file():
                raise FileNotFoundError(source)
            fd, temporary_name = tempfile.mkstemp(prefix=f".{destination.name}.", dir=str(target_dir))
            temporary = Path(temporary_name)
            try:
                with os.fdopen(fd, "wb") as output, source.open("rb") as input_stream:
                    shutil.copyfileobj(input_stream, output)
                    output.flush()
                    os.fsync(output.fileno())
                os.replace(temporary, destination)
            finally:
                temporary.unlink(missing_ok=True)
            copied.append(str(destination))
        except (OSError, RuntimeError) as exc:
            errors.append(f"{source}: {type(exc).__name__}: {exc}")
    return {
        "status": "ok" if not errors else "partial",
        "target_dir": str(target_dir),
        "copied": copied,
        "errors": errors,
    }


__all__ = ["mirror_report_artifacts"]
