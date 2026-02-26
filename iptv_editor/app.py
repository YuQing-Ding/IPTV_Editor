import sys

from PySide6.QtWidgets import QApplication

from .i18n import init as init_i18n
from .main_window import MainWindow


def main():
    init_i18n()

    app = QApplication(sys.argv)
    w = MainWindow()
    w.show()
    sys.exit(app.exec())
