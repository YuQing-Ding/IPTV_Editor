from typing import List, Tuple, Optional

from PySide6.QtCore import Qt, QThreadPool, QTimer
from PySide6.QtGui import QAction, QKeySequence, QIcon, QPixmap
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QTableWidget, QTableWidgetItem, QFileDialog, QMessageBox,
    QPlainTextEdit, QLabel, QAbstractItemView, QStyle, QComboBox, QDialog
)
from PySide6.QtNetwork import QNetworkAccessManager, QNetworkRequest, QNetworkReply

from .checks import StreamCheckTask, StreamCheckResult, requests_available
from .dialogs import BulkImportDialog
from .i18n import tr, get_lang_list, get_current_lang, set_language
from .m3u import _decode_text_with_fallback, _guess_name_from_url, build_m3u, parse_m3u_text
from .project import PROJECT_EXT, _now_ts, load_project_file, save_project_file


class MainWindow(QMainWindow):
    # 0..3 editable; 4..5 status columns (read-only)
    COL_NAME = 0
    COL_URL = 1
    COL_GROUP = 2
    COL_LOGO = 3
    COL_LOGO_STATUS = 4
    COL_STREAM_STATUS = 5

    NUM_COLS = 6

    def __init__(self):
        super().__init__()
        self.setWindowTitle(tr("window_title"))
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
        self.btn_import = QPushButton(tr("btn_import"))
        self.btn_import_m3u = QPushButton(tr("btn_import_m3u"))
        self.btn_add = QPushButton(tr("btn_add"))
        self.btn_del = QPushButton(tr("btn_del"))

        self.btn_up = QPushButton(tr("btn_up"))
        self.btn_down = QPushButton(tr("btn_down"))
        self.btn_top = QPushButton(tr("btn_top"))
        self.btn_bottom = QPushButton(tr("btn_bottom"))

        self.btn_autoname = QPushButton(tr("btn_autoname"))

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
        self.btn_check_logo_sel = QPushButton(tr("btn_check_logo_sel"))
        self.btn_check_logo_all = QPushButton(tr("btn_check_logo_all"))
        self.btn_check_stream_sel = QPushButton(tr("btn_check_stream_sel"))
        self.btn_check_stream_all = QPushButton(tr("btn_check_stream_all"))
        self.btn_copy = QPushButton(tr("btn_copy"))
        self.btn_export = QPushButton(tr("btn_export"))

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
        self.table = QTableWidget(0, self.NUM_COLS)
        self._update_table_headers()
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setSelectionMode(QTableWidget.ExtendedSelection)
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.setSortingEnabled(False)

        # Enable row drag-sort (InternalMove)
        self.table.setDragEnabled(True)
        self.table.setAcceptDrops(True)
        self.table.setDropIndicatorShown(True)
        self.table.setDragDropMode(QAbstractItemView.InternalMove)

        root.addWidget(self.table, 1)

        # ---- Preview
        self._preview_label = QLabel(tr("preview_label"))
        root.addWidget(self._preview_label)

        self.preview = QPlainTextEdit()
        self.preview.setReadOnly(True)
        self.preview.setPlaceholderText(tr("preview_placeholder"))
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
        # Refresh preview after drag moves (timer debounce)
        self._drag_refresh_timer = QTimer(self)
        self._drag_refresh_timer.setInterval(250)
        self._drag_refresh_timer.setSingleShot(True)
        self.table.model().rowsMoved.connect(lambda *_: self._drag_refresh_timer.start())
        self._drag_refresh_timer.timeout.connect(self.refresh_preview)

        # ---- Menus
        self._build_menus()

        # ---- Language selector (top-right corner of menu bar)
        self._lang_combo = QComboBox()
        self._lang_combo.setMinimumWidth(100)
        langs = get_lang_list()
        for code, display_name in langs:
            self._lang_combo.addItem(display_name, code)
        # Set current language
        current = get_current_lang()
        for i, (code, _) in enumerate(langs):
            if code == current:
                self._lang_combo.setCurrentIndex(i)
                break
        self._lang_combo.currentIndexChanged.connect(self._on_lang_changed)
        self.menuBar().setCornerWidget(self._lang_combo, Qt.TopRightCorner)

        self.refresh_preview()

    def _update_table_headers(self):
        headers = [
            tr("col_name"), tr("col_url"), tr("col_group"),
            tr("col_logo"), tr("col_logo_status"), tr("col_stream_status"),
        ]
        self.table.setHorizontalHeaderLabels(headers)

    # ---------- language switching ----------
    def _on_lang_changed(self, index):
        code = self._lang_combo.itemData(index)
        if code and set_language(code):
            self._retranslate_ui()

    def _retranslate_ui(self):
        # Window title
        self.mark_dirty(self._dirty)

        # Buttons row 1
        self.btn_import.setText(tr("btn_import"))
        self.btn_import_m3u.setText(tr("btn_import_m3u"))
        self.btn_add.setText(tr("btn_add"))
        self.btn_del.setText(tr("btn_del"))
        self.btn_up.setText(tr("btn_up"))
        self.btn_down.setText(tr("btn_down"))
        self.btn_top.setText(tr("btn_top"))
        self.btn_bottom.setText(tr("btn_bottom"))
        self.btn_autoname.setText(tr("btn_autoname"))

        # Buttons row 2
        self.btn_check_logo_sel.setText(tr("btn_check_logo_sel"))
        self.btn_check_logo_all.setText(tr("btn_check_logo_all"))
        self.btn_check_stream_sel.setText(tr("btn_check_stream_sel"))
        self.btn_check_stream_all.setText(tr("btn_check_stream_all"))
        self.btn_copy.setText(tr("btn_copy"))
        self.btn_export.setText(tr("btn_export"))

        # Table headers
        self._update_table_headers()

        # Preview
        self._preview_label.setText(tr("preview_label"))
        self.preview.setPlaceholderText(tr("preview_placeholder"))

        # Menus
        self._menu_file.setTitle(tr("menu_file"))
        self._act_new.setText(tr("act_new"))
        self._act_open.setText(tr("act_open"))
        self._act_import_m3u.setText(tr("act_import_m3u"))
        self._act_save.setText(tr("act_save"))
        self._act_save_as.setText(tr("act_save_as"))
        self._act_export.setText(tr("act_export"))
        self._act_quit.setText(tr("act_quit"))

    # ---------- menus / project ----------
    def _build_menus(self):
        menubar = self.menuBar()

        self._menu_file = menubar.addMenu(tr("menu_file"))
        self._act_new = QAction(tr("act_new"), self)
        self._act_open = QAction(tr("act_open"), self)
        self._act_import_m3u = QAction(tr("act_import_m3u"), self)
        self._act_save = QAction(tr("act_save"), self)
        self._act_save_as = QAction(tr("act_save_as"), self)
        self._act_export = QAction(tr("act_export"), self)
        self._act_quit = QAction(tr("act_quit"), self)

        self._act_new.setShortcut(QKeySequence.New)
        self._act_open.setShortcut(QKeySequence.Open)
        self._act_import_m3u.setShortcut(QKeySequence("Ctrl+I"))
        self._act_save.setShortcut(QKeySequence.Save)
        self._act_save_as.setShortcut(QKeySequence("Ctrl+Shift+S"))
        self._act_export.setShortcut(QKeySequence("Ctrl+E"))
        self._act_quit.setShortcut(QKeySequence.Quit)

        self._act_new.triggered.connect(self.new_project)
        self._act_open.triggered.connect(self.open_project)
        self._act_import_m3u.triggered.connect(self.on_import_m3u)
        self._act_save.triggered.connect(self.save_project)
        self._act_save_as.triggered.connect(self.save_project_as)
        self._act_export.triggered.connect(self.on_export_m3u)
        self._act_quit.triggered.connect(self.close)

        self._menu_file.addAction(self._act_new)
        self._menu_file.addAction(self._act_open)
        self._menu_file.addAction(self._act_import_m3u)
        self._menu_file.addSeparator()
        self._menu_file.addAction(self._act_save)
        self._menu_file.addAction(self._act_save_as)
        self._menu_file.addSeparator()
        self._menu_file.addAction(self._act_export)
        self._menu_file.addSeparator()
        self._menu_file.addAction(self._act_quit)

    def mark_dirty(self, dirty: bool = True):
        self._dirty = dirty
        title = tr("window_title")
        if self._project_path:
            title += f" - {self._project_path}"
        if self._dirty:
            title += " *"
        self.setWindowTitle(title)

    def new_project(self):
        if self._dirty:
            ret = QMessageBox.question(self, tr("msg_unsaved"), tr("msg_new_confirm"))
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

        # Auto logo check after import (lightweight)
        self.check_logo(selected_only=False, auto=True)

    def open_project(self):
        path, _ = QFileDialog.getOpenFileName(
            self, tr("msg_open_project"), "", f"IPTV Project (*{PROJECT_EXT});;All Files (*)"
        )
        if not path:
            return
        try:
            payload = load_project_file(path)
        except Exception as e:
            QMessageBox.critical(self, tr("msg_open_fail"), tr("msg_open_fail_detail").format(e))
            return

        self._project_path = path
        try:
            self._load_payload(payload)
        except Exception as e:
            QMessageBox.critical(self, tr("msg_load_fail"), tr("msg_load_fail_detail").format(e))
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
            QMessageBox.critical(self, tr("msg_save_fail"), tr("msg_save_fail_detail").format(e))

    def save_project_as(self):
        path, _ = QFileDialog.getSaveFileName(
            self, tr("msg_save_project"), f"project{PROJECT_EXT}", f"IPTV Project (*{PROJECT_EXT});;All Files (*)"
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
                QMessageBox.information(self, tr("msg_no_import"), tr("msg_no_import_detail"))
                return
            self.table.blockSignals(True)
            try:
                for (name, url, group, logo) in rows:
                    self._append_row(name, url, group, logo)
            finally:
                self.table.blockSignals(False)
            self.refresh_preview()
            self.mark_dirty(True)

            # Auto logo check after import
            self.check_logo(selected_only=False, auto=True)

    def on_import_m3u(self):
        path, _ = QFileDialog.getOpenFileName(
            self, tr("msg_import_m3u"), "", "M3U Playlist (*.m3u *.m3u8);;All Files (*)"
        )
        if not path:
            return
        try:
            text = _decode_text_with_fallback(path)
            rows = parse_m3u_text(text, default_group="IPTV")
        except Exception as e:
            QMessageBox.critical(self, tr("msg_import_m3u_fail"), tr("msg_import_m3u_fail_detail").format(e))
            return
        if not rows:
            QMessageBox.information(self, tr("msg_import_m3u"), tr("msg_no_channel"))
            return

        replace_mode = False
        if self.table.rowCount() > 0:
            msg = QMessageBox(self)
            msg.setWindowTitle(tr("msg_import_m3u"))
            msg.setText(tr("msg_import_mode"))
            btn_replace = msg.addButton(tr("msg_replace"), QMessageBox.AcceptRole)
            btn_append = msg.addButton(tr("msg_append"), QMessageBox.AcceptRole)
            btn_cancel = msg.addButton(QMessageBox.Cancel)
            msg.setDefaultButton(btn_append)
            msg.exec()
            clicked = msg.clickedButton()
            if clicked == btn_cancel:
                return
            replace_mode = (clicked == btn_replace)

        if replace_mode:
            if self._dirty:
                ret = QMessageBox.question(self, tr("msg_unsaved"), tr("msg_replace_confirm"))
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

        # Auto logo check after import
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
                # Keep editability
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
            # Move up: start from smallest row
            for r in rows:
                if r + delta < 0:
                    continue
                self._swap_rows(r, r + delta)
            new_sel = [max(0, r + delta) for r in rows]
        else:
            # Move down: start from largest row
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

        # Skip status columns
        if c in (self.COL_LOGO_STATUS, self.COL_STREAM_STATUS):
            return

        self.mark_dirty(True)
        self.refresh_preview()

        # Debounce logo check on logo URL change
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
        QMessageBox.information(self, tr("msg_copied"), tr("msg_copied_detail"))

    def on_export_m3u(self):
        m3u = self.preview.toPlainText()
        if not m3u.strip() or m3u.strip() == "#EXTM3U":
            QMessageBox.warning(self, tr("msg_no_content"), tr("msg_no_content_detail"))
            return
        path, _ = QFileDialog.getSaveFileName(
            self, tr("msg_save_m3u"), "iptv.m3u", "M3U Playlist (*.m3u);;All Files (*)"
        )
        if not path:
            return
        try:
            with open(path, "w", encoding="utf-8", newline="\n") as f:
                f.write(m3u)
            QMessageBox.information(self, tr("msg_export_ok"), tr("msg_export_ok_detail").format(path))
        except Exception as e:
            QMessageBox.critical(self, tr("msg_export_fail"), tr("msg_export_fail_detail").format(e))

    # =========================
    # 2) live logo checking
    # =========================
    def _debounce_logo_check(self, row: int):
        # Only trigger once for rapid edits
        if row in self._logo_debounce:
            self._logo_debounce[row].stop()

        t = QTimer(self)
        t.setSingleShot(True)
        t.setInterval(500)
        t.timeout.connect(lambda r=row: self._check_logo_row(r))
        self._logo_debounce[row] = t
        t.start()

        self._set_status_cell(row, self.COL_LOGO_STATUS, tr("status_checking"), self._icon_wait, tr("status_waiting"))

    def check_logo(self, selected_only: bool, auto: bool = False):
        rows = self.selected_rows() if selected_only else list(range(self.table.rowCount()))
        if not rows:
            if not auto:
                QMessageBox.information(self, tr("msg_no_check"), tr("msg_no_check_detail"))
            return
        for r in rows:
            self._check_logo_row(r)

    def _check_logo_row(self, row: int):
        logo_url = self._cell_text(row, self.COL_LOGO)
        if not logo_url:
            self._set_status_cell(row, self.COL_LOGO_STATUS, "—", QIcon(), tr("status_no_logo"))
            return

        # Deduplicate
        key = (row, logo_url)
        if key in self._logo_pending:
            return
        self._logo_pending.add(key)

        req = QNetworkRequest(logo_url)
        req.setRawHeader(b"User-Agent", b"Mozilla/5.0 (Logo-Checker)")
        # Only need accessibility check
        reply = self._net.get(req)

        # Timeout
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
                # Generate small icon
                icon_pix = pix.scaled(24, 24, Qt.KeepAspectRatio, Qt.SmoothTransformation)
                icon = QIcon(icon_pix)
                self._set_status_cell(
                    row, self.COL_LOGO_STATUS, "OK", icon,
                    f"Logo OK, {len(data)} bytes, content-type={ctype_str}"
                )
            else:
                # Some SVG/WebP may not decode in Qt; mark UNKNOWN
                self._set_status_cell(
                    row, self.COL_LOGO_STATUS, "UNKNOWN", self._icon_wait,
                    f"Downloaded but Qt can't decode. bytes={len(data)} content-type={ctype_str}"
                )

            reply.deleteLater()

        reply.finished.connect(done)
        self._set_status_cell(row, self.COL_LOGO_STATUS, tr("status_checking"), self._icon_wait, tr("status_detecting"))

    # =========================
    # 3) check stream validity
    # =========================
    def check_stream(self, selected_only: bool):
        rows = self.selected_rows() if selected_only else list(range(self.table.rowCount()))
        if not rows:
            QMessageBox.information(self, tr("msg_no_check"), tr("msg_no_check_detail"))
            return

        if not requests_available():
            QMessageBox.warning(self, tr("msg_missing_dep"), tr("msg_missing_dep_detail"))
            return

        for r in rows:
            url = self._cell_text(r, self.COL_URL)
            if not url:
                self._set_status_cell(r, self.COL_STREAM_STATUS, "—", QIcon(), "Empty URL")
                continue
            self._set_status_cell(r, self.COL_STREAM_STATUS, tr("status_checking"), self._icon_wait, tr("status_detecting"))
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
            ret = QMessageBox.question(self, tr("msg_unsaved"), tr("msg_close_save"))
            if ret == QMessageBox.Yes:
                self.save_project()
                if self._dirty:
                    event.ignore()
                    return
            elif ret == QMessageBox.Cancel:
                event.ignore()
                return
        event.accept()
