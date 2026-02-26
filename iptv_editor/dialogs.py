from PySide6.QtWidgets import QDialog, QVBoxLayout, QLabel, QPlainTextEdit, QFormLayout, QLineEdit, QDialogButtonBox

from .i18n import tr
from .m3u import parse_bulk_text


class BulkImportDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle(tr("bulk_title"))
        self.resize(820, 460)

        layout = QVBoxLayout(self)

        tip = QLabel(tr("bulk_tip"))
        tip.setWordWrap(True)
        layout.addWidget(tip)

        self.text = QPlainTextEdit()
        self.text.setPlaceholderText(tr("bulk_placeholder"))
        layout.addWidget(self.text, 1)

        form = QFormLayout()
        self.default_group = QLineEdit("IPTV")
        form.addRow(tr("bulk_default_group"), self.default_group)
        layout.addLayout(form)

        buttons = QDialogButtonBox(QDialogButtonBox.Cancel | QDialogButtonBox.Ok)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def get_rows(self):
        dg = self.default_group.text().strip() or "IPTV"
        rows = parse_bulk_text(self.text.toPlainText(), default_group=dg)
        return rows
