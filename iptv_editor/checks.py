import time
from dataclasses import dataclass
from urllib.parse import urlparse

try:
    import requests
except Exception:
    requests = None  # allow running without requests (limited stream checking)

from PySide6.QtCore import QObject, Signal, QRunnable


@dataclass
class StreamCheckResult:
    ok: bool
    status: str  # OK / FAIL / UNKNOWN
    detail: str
    ms: int


class StreamCheckSignals(QObject):
    finished = Signal(int, object)  # row_index, StreamCheckResult


class StreamCheckTask(QRunnable):
    def __init__(self, row_index: int, url: str, timeout_s: int = 6):
        super().__init__()
        self.row_index = row_index
        self.url = url
        self.timeout_s = timeout_s
        self.signals = StreamCheckSignals()

    def run(self):
        start = time.time()
        url = (self.url or "").strip()
        if not url:
            res = StreamCheckResult(False, "FAIL", "Empty URL", 0)
            self.signals.finished.emit(self.row_index, res)
            return

        # Non-http(s) schemes are hard to validate (udp/rtmp), mark UNKNOWN
        scheme = ""
        try:
            scheme = (urlparse(url).scheme or "").lower()
        except Exception:
            scheme = ""
        if scheme and scheme not in ("http", "https"):
            ms = int((time.time() - start) * 1000)
            res = StreamCheckResult(False, "UNKNOWN", f"Non-HTTP scheme: {scheme}", ms)
            self.signals.finished.emit(self.row_index, res)
            return

        if requests is None:
            ms = int((time.time() - start) * 1000)
            res = StreamCheckResult(False, "UNKNOWN", "requests not installed", ms)
            self.signals.finished.emit(self.row_index, res)
            return

        headers = {
            "User-Agent": "Mozilla/5.0 (Emby-Playlist-Checker)",
            "Accept": "*/*",
            "Connection": "close",
        }

        def _ok(ms_, detail_, status_="OK"):
            return StreamCheckResult(True, status_, detail_, ms_)

        def _fail(ms_, detail_, status_="FAIL"):
            return StreamCheckResult(False, status_, detail_, ms_)

        try:
            # HEAD
            try:
                r = requests.head(url, headers=headers, allow_redirects=True, timeout=(3, self.timeout_s))
                code = r.status_code
                ctype = (r.headers.get("Content-Type") or "").lower()
                if 200 <= code < 400:
                    if "application/vnd.apple.mpegurl" in ctype or "application/x-mpegurl" in ctype:
                        ms = int((time.time() - start) * 1000)
                        self.signals.finished.emit(self.row_index, _ok(ms, f"HEAD {code} {ctype}"))
                        return
                    if ctype.startswith("video/") or "octet-stream" in ctype or "mpeg" in ctype:
                        ms = int((time.time() - start) * 1000)
                        self.signals.finished.emit(self.row_index, _ok(ms, f"HEAD {code} {ctype}"))
                        return
            except Exception:
                pass

            # Range GET (small read)
            h2 = dict(headers)
            h2["Range"] = "bytes=0-2047"
            r2 = requests.get(url, headers=h2, allow_redirects=True, timeout=(3, self.timeout_s), stream=True)
            code2 = r2.status_code
            ctype2 = (r2.headers.get("Content-Type") or "").lower()

            # Read first 2KB
            chunk = b""
            try:
                for part in r2.iter_content(chunk_size=2048):
                    if part:
                        chunk += part
                    break
            except Exception:
                chunk = b""

            text_head = ""
            try:
                text_head = chunk.decode("utf-8", errors="ignore")
            except Exception:
                text_head = ""

            if 200 <= code2 < 400 or code2 == 206:
                if "#EXTM3U" in text_head[:2048]:
                    ms = int((time.time() - start) * 1000)
                    self.signals.finished.emit(self.row_index, _ok(ms, f"GET {code2} looks like M3U8"))
                    return
                if ctype2.startswith("video/") or "mpeg" in ctype2 or "octet-stream" in ctype2:
                    ms = int((time.time() - start) * 1000)
                    self.signals.finished.emit(self.row_index, _ok(ms, f"GET {code2} {ctype2}"))
                    return
                ms = int((time.time() - start) * 1000)
                self.signals.finished.emit(
                    self.row_index,
                    _fail(ms, f"GET {code2} Uncertain Content-Type: {ctype2}", status_="UNKNOWN"),
                )
                return

            ms = int((time.time() - start) * 1000)
            self.signals.finished.emit(self.row_index, _fail(ms, f"HTTP {code2}", status_="FAIL"))

        except Exception as e:
            ms = int((time.time() - start) * 1000)
            self.signals.finished.emit(self.row_index, _fail(ms, f"Error: {e}", status_="FAIL"))


def requests_available() -> bool:
    return requests is not None
