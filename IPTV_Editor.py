#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import re
import json
import base64
import zlib
import time
from dataclasses import dataclass
from typing import List, Tuple, Optional
from urllib.parse import urlparse

try:
    import requests
except Exception:
    requests = None  # 允许无 requests 运行（但检测能力会弱）

from PySide6.QtCore import (
    Qt, QObject, Signal, QRunnable, QThreadPool, QTimer
)
from PySide6.QtGui import QAction, QKeySequence, QIcon, QPixmap
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QTableWidget, QTableWidgetItem, QFileDialog, QMessageBox,
    QPlainTextEdit, QDialog, QLabel, QLineEdit, QFormLayout, QDialogButtonBox,
    QAbstractItemView,QStyle
)

from PySide6.QtNetwork import QNetworkAccessManager, QNetworkRequest, QNetworkReply


# =========================
# 工程文件：自创封装格式
# =========================
MAGIC = "IPTVPJ1"  # 自创格式魔数
PROJECT_EXT = ".iptvpj"


def _now_ts() -> int:
    return int(time.time())


def save_project_file(path: str, payload: dict):
    """
    自创格式：文本文件
    第一行：MAGIC
    第二行：base64(zlib(json))
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
        raise ValueError("不是有效的工程文件（MAGIC 不匹配）")
    if len(lines) < 2 or not lines[1].strip():
        raise ValueError("工程文件损坏（缺少数据段）")
    packed = base64.b64decode(lines[1].strip().encode("ascii"))
    raw = zlib.decompress(packed)
    return json.loads(raw.decode("utf-8"))


# =========================
# M3U 生成/解析
# =========================
def _esc_attr(v: str) -> str:
    if v is None:
        return ""
    return v.replace("\\", "\\\\").replace('"', '\\"').strip()


def _guess_name_from_url(url: str, idx: int) -> str:
    try:
        p = urlparse(url.strip())
        host = (p.hostname or "").lower()
        path = (p.path or "").strip("/")
        if path:
            leaf = path.split("/")[-1]
            leaf = re.sub(r"\.(m3u8|ts|mp4|mkv|flv|aac|mp3)$", "", leaf, flags=re.I)
            leaf = re.sub(r"[^A-Za-z0-9_\-\u4e00-\u9fff]+", " ", leaf).strip()
            if leaf:
                return leaf
        if host:
            return host
    except Exception:
        pass
    return f"Channel {idx:03d}"


def parse_bulk_text(text: str, default_group: str = "IPTV"):
    """
    支持行格式：
      1) 频道名|URL|分组|台标URL
      2) 频道名,URL,分组,台标URL
      3) 仅URL
    忽略空行、# 开头注释
    """
    rows = []
    idx = 1
    for raw in text.splitlines():
        s = raw.strip()
        if not s or s.startswith("#"):
            continue

        if "|" in s:
            parts = [p.strip() for p in s.split("|")]
        elif "," in s:
            parts = [p.strip() for p in s.split(",")]
        else:
            parts = [s]

        parts = [p for p in parts if p != ""]

        if len(parts) == 1:
            url = parts[0]
            name = _guess_name_from_url(url, idx)
            group = default_group
            logo = ""
        else:
            name = parts[0]
            url = parts[1] if len(parts) > 1 else ""
            group = parts[2] if len(parts) > 2 else default_group
            logo = parts[3] if len(parts) > 3 else ""
            if not name:
                name = _guess_name_from_url(url, idx)
            if not group:
                group = default_group

        if url:
            rows.append((name, url, group, logo))
            idx += 1

    return rows


def _decode_text_with_fallback(path: str) -> str:
    with open(path, "rb") as f:
        data = f.read()
    for enc in ("utf-8-sig", "utf-8", "gb18030", "big5", "latin-1"):
        try:
            return data.decode(enc)
        except Exception:
            continue
    return data.decode("utf-8", errors="ignore")


def _parse_m3u_attrs(attr_text: str) -> dict:
    attrs = {}
    if not attr_text:
        return attrs
    for m in re.finditer(r'([A-Za-z0-9_-]+)\s*=\s*"([^"]*)"', attr_text):
        attrs[m.group(1).lower()] = m.group(2).strip()
    for m in re.finditer(r'([A-Za-z0-9_-]+)\s*=\s*([^"\s]+)', attr_text):
        key = m.group(1).lower()
        if key not in attrs:
            attrs[key] = m.group(2).strip()
    return attrs


def parse_m3u_text(text: str, default_group: str = "IPTV"):
    rows = []
    idx = 1
    cur_name = ""
    cur_logo = ""
    cur_group = ""
    for raw in text.splitlines():
        s = raw.strip()
        if not s:
            continue
        if s.upper().startswith("#EXTINF"):
            rest = s[len("#EXTINF:"):].strip()
            if "," in rest:
                attr_part, name_part = rest.split(",", 1)
            else:
                attr_part, name_part = rest, ""
            attr_part = attr_part.strip()
            if attr_part:
                if " " in attr_part:
                    first, remainder = attr_part.split(" ", 1)
                else:
                    first, remainder = attr_part, ""
                if first.lstrip("-").isdigit():
                    attr_part = remainder.strip()
            attrs = _parse_m3u_attrs(attr_part)
            name = name_part.strip() or (attrs.get("tvg-name") or attrs.get("tvg-id") or "").strip()
            cur_name = name
            cur_logo = (attrs.get("tvg-logo") or attrs.get("logo") or "").strip()
            cur_group = (attrs.get("group-title") or attrs.get("group") or "").strip()
            continue
        if s.upper().startswith("#EXTGRP"):
            grp = s.split(":", 1)[1].strip() if ":" in s else ""
            if grp:
                cur_group = grp
            continue
        if s.startswith("#"):
            continue
        url = s
        if url:
            name = cur_name or _guess_name_from_url(url, idx)
            group = cur_group or default_group
            logo = cur_logo or ""
            rows.append((name, url, group, logo))
            idx += 1
        cur_name = ""
        cur_logo = ""
        cur_group = ""
    return rows


def build_m3u(rows: List[Tuple[str, str, str, str]]) -> str:
    """
    rows: (name, url, group, logo)
    输出 Emby 常见可读的 M3U（无 EPG）
    """
    out = ["#EXTM3U"]
    for (name, url, group, logo) in rows:
        name = (name or "").strip()
        url = (url or "").strip()
        group = (group or "").strip()
        logo = (logo or "").strip()
        if not url:
            continue
        if not name:
            name = _guess_name_from_url(url, 1)

        attrs = []
        if logo:
            attrs.append(f'tvg-logo="{_esc_attr(logo)}"')
        if group:
            attrs.append(f'group-title="{_esc_attr(group)}"')

        attr_str = (" " + " ".join(attrs)) if attrs else ""
        out.append(f"#EXTINF:-1{attr_str},{name}")
        out.append(url)
        out.append("")
    return "\n".join(out).rstrip() + "\n"


# =========================
# UI：批量导入对话框
# =========================
class BulkImportDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("批量导入 / 粘贴")
        self.resize(820, 460)

        layout = QVBoxLayout(self)

        tip = QLabel(
            "每行一种频道：\n"
            "  - 仅URL： http://... 或 rtmp://... 等\n"
            "  - 或： 频道名|URL|分组|台标URL\n"
            "  - 或： 频道名,URL,分组,台标URL\n"
            "空行和 # 开头注释会被忽略。"
        )
        tip.setWordWrap(True)
        layout.addWidget(tip)

        self.text = QPlainTextEdit()
        self.text.setPlaceholderText("在这里粘贴你的 IPTV 地址列表…")
        layout.addWidget(self.text, 1)

        form = QFormLayout()
        self.default_group = QLineEdit("IPTV")
        form.addRow("默认分组（缺省时使用）:", self.default_group)
        layout.addLayout(form)

        buttons = QDialogButtonBox(QDialogButtonBox.Cancel | QDialogButtonBox.Ok)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def get_rows(self):
        dg = self.default_group.text().strip() or "IPTV"
        rows = parse_bulk_text(self.text.toPlainText(), default_group=dg)
        return rows


# =========================
# 检测任务（直播源）
# =========================
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

        # 非 http(s) 的源：难以做“有效性”判断（比如 udp/rtmp），标 UNKNOWN
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

        # HTTP 检测策略：
        # 1) HEAD 看状态码/Content-Type
        # 2) 若 HEAD 不行/不可信，GET Range 0-2047 看内容（m3u8）或 content-type（video）
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
                    # 有些源 HEAD 返回 text/html（CDN/鉴权页），继续做 Range GET
            except Exception:
                pass

            # Range GET (小读取)
            h2 = dict(headers)
            h2["Range"] = "bytes=0-2047"
            r2 = requests.get(url, headers=h2, allow_redirects=True, timeout=(3, self.timeout_s), stream=True)
            code2 = r2.status_code
            ctype2 = (r2.headers.get("Content-Type") or "").lower()

            # 读取前 2KB
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
                # m3u8 内容识别
                if "#EXTM3U" in text_head[:2048]:
                    ms = int((time.time() - start) * 1000)
                    self.signals.finished.emit(self.row_index, _ok(ms, f"GET {code2} looks like M3U8"))
                    return
                # 常见视频/ts
                if ctype2.startswith("video/") or "mpeg" in ctype2 or "octet-stream" in ctype2:
                    ms = int((time.time() - start) * 1000)
                    self.signals.finished.emit(self.row_index, _ok(ms, f"GET {code2} {ctype2}"))
                    return
                # 有些源不带 content-type，但还能播：给 UNKNOWN（不直接判死）
                ms = int((time.time() - start) * 1000)
                self.signals.finished.emit(self.row_index, _fail(ms, f"GET {code2} Uncertain Content-Type: {ctype2}", status_="UNKNOWN"))
                return

            ms = int((time.time() - start) * 1000)
            self.signals.finished.emit(self.row_index, _fail(ms, f"HTTP {code2}", status_="FAIL"))

        except Exception as e:
            ms = int((time.time() - start) * 1000)
            self.signals.finished.emit(self.row_index, _fail(ms, f"Error: {e}", status_="FAIL"))


# =========================
# 主窗口
# =========================
class MainWindow(QMainWindow):
    # 0..3 可编辑；4..5 为状态列（只读）
    COL_NAME = 0
    COL_URL = 1
    COL_GROUP = 2
    COL_LOGO = 3
    COL_LOGO_STATUS = 4
    COL_STREAM_STATUS = 5

    COLS = ["频道名", "URL", "分组", "台标URL", "Logo状态", "源状态"]

    def __init__(self):
        super().__init__()
        self.setWindowTitle("IPTV → Emby M3U（排序 / Logo校验 / 源检测 / 工程文件）")
        self.resize(1200, 760)

        self._project_path: Optional[str] = None
        self._dirty = False

        self._net = QNetworkAccessManager(self)
        self._logo_debounce = {}  # row -> QTimer
        self._logo_pending = set()

        self._thread_pool = QThreadPool.globalInstance()
        self._checking = False

        self._icon_ok = self.style().standardIcon(QStyle.SP_DialogApplyButton)
        self._icon_fail = self.style().standardIcon(QStyle.SP_DialogCancelButton)
        self._icon_wait = self.style().standardIcon(QStyle.SP_BrowserReload)


        central = QWidget()
        self.setCentralWidget(central)
        root = QVBoxLayout(central)

        # ---- Buttons row 1
        btn_row = QHBoxLayout()
        self.btn_import = QPushButton("批量导入/粘贴")
        self.btn_import_m3u = QPushButton("导入 M3U")
        self.btn_add = QPushButton("添加一行")
        self.btn_del = QPushButton("删除选中")

        self.btn_up = QPushButton("上移")
        self.btn_down = QPushButton("下移")
        self.btn_top = QPushButton("置顶")
        self.btn_bottom = QPushButton("置底")

        self.btn_autoname = QPushButton("按URL自动生成频道名")

        btn_row.addWidget(self.btn_import)
        btn_row.addWidget(self.btn_import_m3u)
        btn_row.addWidget(self.btn_add)
        btn_row.addWidget(self.btn_del)
        btn_row.addSpacing(12)
        btn_row.addWidget(self.btn_up)
        btn_row.addWidget(self.btn_down)
        btn_row.addWidget(self.btn_top)
        btn_row.addWidget(self.btn_bottom)
        btn_row.addSpacing(12)
        btn_row.addWidget(self.btn_autoname)
        btn_row.addStretch(1)
        root.addLayout(btn_row)

        # ---- Buttons row 2
        btn_row2 = QHBoxLayout()
        self.btn_check_logo_sel = QPushButton("检测Logo(选中)")
        self.btn_check_logo_all = QPushButton("检测Logo(全部)")
        self.btn_check_stream_sel = QPushButton("检测源(选中)")
        self.btn_check_stream_all = QPushButton("检测源(全部)")
        self.btn_copy = QPushButton("复制M3U到剪贴板")
        self.btn_export = QPushButton("导出M3U文件")

        btn_row2.addWidget(self.btn_check_logo_sel)
        btn_row2.addWidget(self.btn_check_logo_all)
        btn_row2.addSpacing(12)
        btn_row2.addWidget(self.btn_check_stream_sel)
        btn_row2.addWidget(self.btn_check_stream_all)
        btn_row2.addStretch(1)
        btn_row2.addWidget(self.btn_copy)
        btn_row2.addWidget(self.btn_export)
        root.addLayout(btn_row2)

        # ---- Table
        self.table = QTableWidget(0, len(self.COLS))
        self.table.setHorizontalHeaderLabels(self.COLS)
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setSelectionMode(QTableWidget.ExtendedSelection)
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.setSortingEnabled(False)

        # 支持拖拽行排序（InternalMove）
        self.table.setDragEnabled(True)
        self.table.setAcceptDrops(True)
        self.table.setDropIndicatorShown(True)
        self.table.setDragDropMode(QAbstractItemView.InternalMove)

        root.addWidget(self.table, 1)

        # ---- Preview
        preview_label = QLabel("M3U 预览（只读）：")
        root.addWidget(preview_label)

        self.preview = QPlainTextEdit()
        self.preview.setReadOnly(True)
        self.preview.setPlaceholderText("这里会显示生成后的 M3U 内容…")
        root.addWidget(self.preview, 1)

        # ---- Events
        self.btn_import.clicked.connect(self.on_import)
        self.btn_import_m3u.clicked.connect(self.on_import_m3u)
        self.btn_add.clicked.connect(self.on_add_row)
        self.btn_del.clicked.connect(self.on_delete_rows)
        self.btn_up.clicked.connect(lambda: self.move_selected(-1))
        self.btn_down.clicked.connect(lambda: self.move_selected(+1))
        self.btn_top.clicked.connect(lambda: self.move_selected_to_edge(top=True))
        self.btn_bottom.clicked.connect(lambda: self.move_selected_to_edge(top=False))
        self.btn_autoname.clicked.connect(self.on_auto_name)

        self.btn_check_logo_sel.clicked.connect(lambda: self.check_logo(selected_only=True))
        self.btn_check_logo_all.clicked.connect(lambda: self.check_logo(selected_only=False))
        self.btn_check_stream_sel.clicked.connect(lambda: self.check_stream(selected_only=True))
        self.btn_check_stream_all.clicked.connect(lambda: self.check_stream(selected_only=False))

        self.btn_export.clicked.connect(self.on_export_m3u)
        self.btn_copy.clicked.connect(self.on_copy_m3u)

        self.table.itemChanged.connect(self.on_item_changed)
        # 行拖拽完成后刷新预览（用定时器粗暴处理）
        self._drag_refresh_timer = QTimer(self)
        self._drag_refresh_timer.setInterval(250)
        self._drag_refresh_timer.setSingleShot(True)
        self.table.model().rowsMoved.connect(lambda *_: self._drag_refresh_timer.start())
        self._drag_refresh_timer.timeout.connect(self.refresh_preview)

        # ---- Menus
        self._build_menus()

        self.refresh_preview()

    # ---------- menus / project ----------
    def _build_menus(self):
        menubar = self.menuBar()

        m_file = menubar.addMenu("File")
        act_new = QAction("New Project", self)
        act_open = QAction("Open Project…", self)
        act_import_m3u = QAction("Import M3U…", self)
        act_save = QAction("Save Project", self)
        act_save_as = QAction("Save Project As…", self)
        act_export = QAction("Export M3U…", self)
        act_quit = QAction("Quit", self)

        act_new.setShortcut(QKeySequence.New)
        act_open.setShortcut(QKeySequence.Open)
        act_import_m3u.setShortcut(QKeySequence("Ctrl+I"))
        act_save.setShortcut(QKeySequence.Save)
        act_save_as.setShortcut(QKeySequence("Ctrl+Shift+S"))
        act_export.setShortcut(QKeySequence("Ctrl+E"))
        act_quit.setShortcut(QKeySequence.Quit)

        act_new.triggered.connect(self.new_project)
        act_open.triggered.connect(self.open_project)
        act_import_m3u.triggered.connect(self.on_import_m3u)
        act_save.triggered.connect(self.save_project)
        act_save_as.triggered.connect(self.save_project_as)
        act_export.triggered.connect(self.on_export_m3u)
        act_quit.triggered.connect(self.close)

        m_file.addAction(act_new)
        m_file.addAction(act_open)
        m_file.addAction(act_import_m3u)
        m_file.addSeparator()
        m_file.addAction(act_save)
        m_file.addAction(act_save_as)
        m_file.addSeparator()
        m_file.addAction(act_export)
        m_file.addSeparator()
        m_file.addAction(act_quit)

    def mark_dirty(self, dirty: bool = True):
        self._dirty = dirty
        title = "IPTV → Emby M3U（排序 / Logo校验 / 源检测 / 工程文件）"
        if self._project_path:
            title += f" - {self._project_path}"
        if self._dirty:
            title += " *"
        self.setWindowTitle(title)

    def new_project(self):
        if self._dirty:
            ret = QMessageBox.question(self, "未保存", "当前工程有改动，确定新建并丢弃改动？")
            if ret != QMessageBox.Yes:
                return
        self.table.blockSignals(True)
        try:
            self.table.setRowCount(0)
        finally:
            self.table.blockSignals(False)
        self._project_path = None
        self.mark_dirty(False)
        self.refresh_preview()

    def _project_payload(self) -> dict:
        rows = []
        for r in range(self.table.rowCount()):
            rows.append({
                "name": self._cell_text(r, self.COL_NAME),
                "url": self._cell_text(r, self.COL_URL),
                "group": self._cell_text(r, self.COL_GROUP),
                "logo": self._cell_text(r, self.COL_LOGO),
            })
        payload = {
            "ver": 1,
            "created": _now_ts(),
            "rows": rows,
            "ui": {
                "col_widths": [self.table.columnWidth(i) for i in range(self.table.columnCount())],
            }
        }
        return payload

    def _load_payload(self, payload: dict):
        rows = payload.get("rows", [])
        self.table.blockSignals(True)
        try:
            self.table.setRowCount(0)
            for row in rows:
                self._append_row(
                    row.get("name", ""),
                    row.get("url", ""),
                    row.get("group", "IPTV"),
                    row.get("logo", "")
                )
            widths = payload.get("ui", {}).get("col_widths", None)
            if widths and len(widths) == self.table.columnCount():
                for i, w in enumerate(widths):
                    try:
                        self.table.setColumnWidth(i, int(w))
                    except Exception:
                        pass
        finally:
            self.table.blockSignals(False)

        self.refresh_preview()
        self.mark_dirty(False)

        # 导入后自动触发 logo 校验（轻量）
        self.check_logo(selected_only=False, auto=True)

    def open_project(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "打开工程文件", "", f"IPTV Project (*{PROJECT_EXT});;All Files (*)"
        )
        if not path:
            return
        try:
            payload = load_project_file(path)
        except Exception as e:
            QMessageBox.critical(self, "打开失败", f"无法读取工程文件：{e}")
            return

        self._project_path = path
        try:
            self._load_payload(payload)
        except Exception as e:
            QMessageBox.critical(self, "加载失败", f"工程内容解析失败：{e}")
            return
        self._project_path = path
        self.mark_dirty(False)

    def save_project(self):
        if not self._project_path:
            self.save_project_as()
            return
        try:
            save_project_file(self._project_path, self._project_payload())
            self.mark_dirty(False)
        except Exception as e:
            QMessageBox.critical(self, "保存失败", f"写入工程文件失败：{e}")

    def save_project_as(self):
        path, _ = QFileDialog.getSaveFileName(
            self, "保存工程文件", f"project{PROJECT_EXT}", f"IPTV Project (*{PROJECT_EXT});;All Files (*)"
        )
        if not path:
            return
        if not path.lower().endswith(PROJECT_EXT):
            path += PROJECT_EXT
        self._project_path = path
        self.save_project()

    # ---------- table helpers ----------
    def _make_item(self, text: str, editable: bool = True) -> QTableWidgetItem:
        it = QTableWidgetItem(text or "")
        flags = it.flags()
        if not editable:
            flags &= ~Qt.ItemIsEditable
        it.setFlags(flags)
        return it

    def _cell_text(self, r: int, c: int) -> str:
        it = self.table.item(r, c)
        return it.text().strip() if it else ""

    def _set_status_cell(self, r: int, c: int, text: str, icon: Optional[QIcon] = None, tooltip: str = ""):
        it = self.table.item(r, c)
        if it is None:
            it = self._make_item("", editable=False)
            self.table.setItem(r, c, it)
        it.setText(text)
        if icon:
            it.setIcon(icon)
        if tooltip:
            it.setToolTip(tooltip)

    def _append_row(self, name="", url="", group="IPTV", logo=""):
        r = self.table.rowCount()
        self.table.insertRow(r)
        self.table.setItem(r, self.COL_NAME, self._make_item(name, editable=True))
        self.table.setItem(r, self.COL_URL, self._make_item(url, editable=True))
        self.table.setItem(r, self.COL_GROUP, self._make_item(group, editable=True))
        self.table.setItem(r, self.COL_LOGO, self._make_item(logo, editable=True))
        self.table.setItem(r, self.COL_LOGO_STATUS, self._make_item("—", editable=False))
        self.table.setItem(r, self.COL_STREAM_STATUS, self._make_item("—", editable=False))

    def selected_rows(self) -> List[int]:
        sel = self.table.selectionModel().selectedRows()
        return sorted({i.row() for i in sel})

    # ---------- core actions ----------
    def on_import(self):
        dlg = BulkImportDialog(self)
        cb = QApplication.clipboard()
        if cb and cb.text().strip():
            dlg.text.setPlainText(cb.text())
        if dlg.exec() == QDialog.Accepted:
            rows = dlg.get_rows()
            if not rows:
                QMessageBox.information(self, "没有可导入内容", "没解析到任何有效行（可能全是空行/注释）。")
                return
            self.table.blockSignals(True)
            try:
                for (name, url, group, logo) in rows:
                    self._append_row(name, url, group, logo)
            finally:
                self.table.blockSignals(False)
            self.refresh_preview()
            self.mark_dirty(True)

            # 导入后自动检测 logo（防止到 Emby 才发现没图）
            self.check_logo(selected_only=False, auto=True)

    def on_import_m3u(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "导入 M3U", "", "M3U Playlist (*.m3u *.m3u8);;All Files (*)"
        )
        if not path:
            return
        try:
            text = _decode_text_with_fallback(path)
            rows = parse_m3u_text(text, default_group="IPTV")
        except Exception as e:
            QMessageBox.critical(self, "导入失败", f"读取 M3U 失败：{e}")
            return
        if not rows:
            QMessageBox.information(self, "导入 M3U", "该 M3U 中未找到可用频道。")
            return

        replace_mode = False
        if self.table.rowCount() > 0:
            msg = QMessageBox(self)
            msg.setWindowTitle("导入 M3U")
            msg.setText("当前列表不为空，选择导入方式：")
            btn_replace = msg.addButton("替换", QMessageBox.AcceptRole)
            btn_append = msg.addButton("追加", QMessageBox.AcceptRole)
            btn_cancel = msg.addButton(QMessageBox.Cancel)
            msg.setDefaultButton(btn_append)
            msg.exec()
            clicked = msg.clickedButton()
            if clicked == btn_cancel:
                return
            replace_mode = (clicked == btn_replace)

        if replace_mode:
            if self._dirty:
                ret = QMessageBox.question(self, "未保存", "当前工程有改动，确定替换并丢弃改动？")
                if ret != QMessageBox.Yes:
                    return
            self.table.blockSignals(True)
            try:
                self.table.setRowCount(0)
            finally:
                self.table.blockSignals(False)

        self.table.blockSignals(True)
        try:
            for (name, url, group, logo) in rows:
                self._append_row(name, url, group, logo)
        finally:
            self.table.blockSignals(False)
        self.refresh_preview()
        self.mark_dirty(True)

        # 导入后自动检测 logo（防止到 Emby 才发现没图）
        self.check_logo(selected_only=False, auto=True)

    def on_add_row(self):
        self._append_row("", "", "IPTV", "")
        self.refresh_preview()
        self.mark_dirty(True)

    def on_delete_rows(self):
        rows = self.selected_rows()
        if not rows:
            return
        for r in reversed(rows):
            self.table.removeRow(r)
        self.refresh_preview()
        self.mark_dirty(True)

    def on_auto_name(self):
        self.table.blockSignals(True)
        try:
            for r in range(self.table.rowCount()):
                url = self._cell_text(r, self.COL_URL)
                name = self._cell_text(r, self.COL_NAME)
                if url and not name:
                    self.table.item(r, self.COL_NAME).setText(_guess_name_from_url(url, r + 1))
        finally:
            self.table.blockSignals(False)
        self.refresh_preview()
        self.mark_dirty(True)

    # ---------- reorder ----------
    def _swap_rows(self, r1: int, r2: int):
        if r1 == r2:
            return
        cols = self.table.columnCount()
        data1 = []
        data2 = []
        for c in range(cols):
            it1 = self.table.item(r1, c)
            it2 = self.table.item(r2, c)
            data1.append((it1.text() if it1 else "", it1.icon() if it1 else QIcon(), it1.toolTip() if it1 else ""))
            data2.append((it2.text() if it2 else "", it2.icon() if it2 else QIcon(), it2.toolTip() if it2 else ""))

        self.table.blockSignals(True)
        try:
            for c in range(cols):
                # 保持可编辑性
                editable = c in (self.COL_NAME, self.COL_URL, self.COL_GROUP, self.COL_LOGO)
                it1 = self._make_item(data2[c][0], editable=editable)
                it1.setIcon(data2[c][1])
                it1.setToolTip(data2[c][2])
                self.table.setItem(r1, c, it1)

                it2 = self._make_item(data1[c][0], editable=editable)
                it2.setIcon(data1[c][1])
                it2.setToolTip(data1[c][2])
                self.table.setItem(r2, c, it2)
        finally:
            self.table.blockSignals(False)

    def move_selected(self, delta: int):
        rows = self.selected_rows()
        if not rows:
            return
        if delta < 0:
            # 上移：从最小行开始处理
            for r in rows:
                if r + delta < 0:
                    continue
                self._swap_rows(r, r + delta)
            new_sel = [max(0, r + delta) for r in rows]
        else:
            # 下移：从最大行开始处理
            for r in reversed(rows):
                if r + delta >= self.table.rowCount():
                    continue
                self._swap_rows(r, r + delta)
            new_sel = [min(self.table.rowCount() - 1, r + delta) for r in rows]

        self.table.clearSelection()
        for r in new_sel:
            self.table.selectRow(r)

        self.refresh_preview()
        self.mark_dirty(True)

    def move_selected_to_edge(self, top: bool):
        rows = self.selected_rows()
        if not rows:
            return
        target = 0 if top else (self.table.rowCount() - 1)
        if top:
            # 逐个交换到上方
            for r in rows:
                cur = r
                while cur > target:
                    self._swap_rows(cur, cur - 1)
                    cur -= 1
                target += 1
        else:
            for r in reversed(rows):
                cur = r
                while cur < target:
                    self._swap_rows(cur, cur + 1)
                    cur += 1
                target -= 1

        self.refresh_preview()
        self.mark_dirty(True)

    # ---------- item changed ----------
    def on_item_changed(self, item: QTableWidgetItem):
        r = item.row()
        c = item.column()

        # 状态列不处理
        if c in (self.COL_LOGO_STATUS, self.COL_STREAM_STATUS):
            return

        self.mark_dirty(True)
        self.refresh_preview()

        # 实时解析 logo：当台标URL变动时，debounce 后自动检测
        if c == self.COL_LOGO:
            self._debounce_logo_check(r)

    # ---------- preview / export ----------
    def get_rows_from_table(self) -> List[Tuple[str, str, str, str]]:
        rows = []
        for r in range(self.table.rowCount()):
            url = self._cell_text(r, self.COL_URL)
            if not url:
                continue
            name = self._cell_text(r, self.COL_NAME)
            group = self._cell_text(r, self.COL_GROUP)
            logo = self._cell_text(r, self.COL_LOGO)
            rows.append((name, url, group, logo))
        return rows

    def refresh_preview(self):
        self.preview.setPlainText(build_m3u(self.get_rows_from_table()))

    def on_copy_m3u(self):
        m3u = self.preview.toPlainText()
        QApplication.clipboard().setText(m3u)
        QMessageBox.information(self, "已复制", "M3U 内容已复制到剪贴板。")

    def on_export_m3u(self):
        m3u = self.preview.toPlainText()
        if not m3u.strip() or m3u.strip() == "#EXTM3U":
            QMessageBox.warning(self, "没有内容", "当前没有有效频道 URL，无法导出。")
            return
        path, _ = QFileDialog.getSaveFileName(
            self, "保存 M3U 文件", "iptv.m3u", "M3U Playlist (*.m3u);;All Files (*)"
        )
        if not path:
            return
        try:
            with open(path, "w", encoding="utf-8", newline="\n") as f:
                f.write(m3u)
            QMessageBox.information(self, "导出成功", f"已保存：\n{path}\n\nEmby：Live TV → M3U Tuner 添加该文件即可。")
        except Exception as e:
            QMessageBox.critical(self, "导出失败", f"写文件失败：{e}")

    # =========================
    # 2) 实时解析 logo
    # =========================
    def _debounce_logo_check(self, row: int):
        # 500ms 内重复编辑只触发一次网络请求
        if row in self._logo_debounce:
            self._logo_debounce[row].stop()

        t = QTimer(self)
        t.setSingleShot(True)
        t.setInterval(500)
        t.timeout.connect(lambda r=row: self._check_logo_row(r))
        self._logo_debounce[row] = t
        t.start()

        self._set_status_cell(row, self.COL_LOGO_STATUS, "Checking…", self._icon_wait, "等待检测…")

    def check_logo(self, selected_only: bool, auto: bool = False):
        rows = self.selected_rows() if selected_only else list(range(self.table.rowCount()))
        if not rows:
            if not auto:
                QMessageBox.information(self, "无可检测项", "没有可检测的行。")
            return
        for r in rows:
            self._check_logo_row(r)

    def _check_logo_row(self, row: int):
        logo_url = self._cell_text(row, self.COL_LOGO)
        if not logo_url:
            self._set_status_cell(row, self.COL_LOGO_STATUS, "—", QIcon(), "未设置台标URL")
            return

        # 防重复
        key = (row, logo_url)
        if key in self._logo_pending:
            return
        self._logo_pending.add(key)

        req = QNetworkRequest(logo_url)
        req.setRawHeader(b"User-Agent", b"Mozilla/5.0 (Logo-Checker)")
        # 只要 logo 可访问就行，不必太严格
        reply = self._net.get(req)

        # 超时
        timeout = QTimer(self)
        timeout.setSingleShot(True)
        timeout.setInterval(5000)
        timeout.timeout.connect(lambda: reply.abort())
        timeout.start()

        def done():
            timeout.stop()
            self._logo_pending.discard(key)

            if reply.error() != QNetworkReply.NoError:
                self._set_status_cell(
                    row, self.COL_LOGO_STATUS, "FAIL", self._icon_fail,
                    f"Network error: {reply.errorString()}"
                )
                reply.deleteLater()
                return

            data = bytes(reply.readAll())
            ctype = (reply.header(QNetworkRequest.ContentTypeHeader) or "")
            ctype_str = str(ctype).lower()

            pix = QPixmap()
            ok = pix.loadFromData(data)
            if ok:
                # 生成小图标
                icon_pix = pix.scaled(24, 24, Qt.KeepAspectRatio, Qt.SmoothTransformation)
                icon = QIcon(icon_pix)
                self._set_status_cell(
                    row, self.COL_LOGO_STATUS, "OK", icon,
                    f"Logo OK, {len(data)} bytes, content-type={ctype_str}"
                )
            else:
                # 有些返回 svg 或 webp/奇怪类型，Qt 可能解不出来，但 Emby 未必不行
                # 这里给 UNKNOWN 让你自己判断
                self._set_status_cell(
                    row, self.COL_LOGO_STATUS, "UNKNOWN", self._icon_wait,
                    f"Downloaded but Qt can't decode. bytes={len(data)} content-type={ctype_str}"
                )

            reply.deleteLater()

        reply.finished.connect(done)
        self._set_status_cell(row, self.COL_LOGO_STATUS, "Checking…", self._icon_wait, "检测中…")

    # =========================
    # 3) 解析直播源是否有效
    # =========================
    def check_stream(self, selected_only: bool):
        rows = self.selected_rows() if selected_only else list(range(self.table.rowCount()))
        if not rows:
            QMessageBox.information(self, "无可检测项", "没有可检测的行。")
            return

        if requests is None:
            QMessageBox.warning(
                self, "缺少依赖",
                "未检测到 requests 库，直播源检测会不可用。\n\n请执行：pip install requests"
            )
            return

        for r in rows:
            url = self._cell_text(r, self.COL_URL)
            if not url:
                self._set_status_cell(r, self.COL_STREAM_STATUS, "—", QIcon(), "Empty URL")
                continue
            self._set_status_cell(r, self.COL_STREAM_STATUS, "Checking…", self._icon_wait, "检测中…")
            task = StreamCheckTask(r, url, timeout_s=6)
            task.signals.finished.connect(self._on_stream_checked)
            self._thread_pool.start(task)

    def _on_stream_checked(self, row_index: int, result: StreamCheckResult):
        if result.status == "OK":
            self._set_status_cell(
                row_index, self.COL_STREAM_STATUS, "OK", self._icon_ok,
                f"{result.detail} ({result.ms}ms)"
            )
        elif result.status == "UNKNOWN":
            self._set_status_cell(
                row_index, self.COL_STREAM_STATUS, "UNKNOWN", self._icon_wait,
                f"{result.detail} ({result.ms}ms)"
            )
        else:
            self._set_status_cell(
                row_index, self.COL_STREAM_STATUS, "FAIL", self._icon_fail,
                f"{result.detail} ({result.ms}ms)"
            )

    # ---------- close event ----------
    def closeEvent(self, event):
        if self._dirty:
            ret = QMessageBox.question(self, "未保存", "工程有改动，是否保存？\nYes=保存  No=不保存  Cancel=取消关闭")
            if ret == QMessageBox.Yes:
                self.save_project()
                if self._dirty:
                    event.ignore()
                    return
            elif ret == QMessageBox.Cancel:
                event.ignore()
                return
        event.accept()


def main():
    app = QApplication(sys.argv)
    w = MainWindow()
    w.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
