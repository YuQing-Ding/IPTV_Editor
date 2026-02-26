import base64
import json
import time
import zlib

from .i18n import tr

# Custom project format
MAGIC = "IPTVPJ1"
PROJECT_EXT = ".iptvpj"


def _now_ts() -> int:
    return int(time.time())


def save_project_file(path: str, payload: dict):
    """
    Custom format: text file
    Line1: MAGIC
    Line2: base64(zlib(json))
    """
    raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    packed = zlib.compress(raw, level=9)
    b64 = base64.b64encode(packed).decode("ascii")
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(MAGIC + "\n")
        f.write(b64 + "\n")


def load_project_file(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        lines = [ln.rstrip("\n") for ln in f.readlines()]
    if not lines or lines[0].strip() != MAGIC:
        raise ValueError(tr("err_invalid_magic"))
    if len(lines) < 2 or not lines[1].strip():
        raise ValueError(tr("err_corrupted"))
    packed = base64.b64decode(lines[1].strip().encode("ascii"))
    raw = zlib.decompress(packed)
    return json.loads(raw.decode("utf-8"))
