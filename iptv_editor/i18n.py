from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple
import json

DEFAULT_LANG = "zh_CN"
PREFERRED_ORDER = ("zh_CN", "zh_TW", "en", "ja")


@dataclass(frozen=True)
class LanguagePack:
    code: str
    name: str
    strings: Dict[str, str]


class I18nManager:
    def __init__(self, base_dir: str | Path | None = None, locales_dir: str | Path | None = None):
        if base_dir is None:
            base_dir = Path(__file__).resolve().parent.parent
        if locales_dir is None:
            locales_dir = Path(__file__).resolve().parent / "locales"

        self._base_dir = Path(base_dir)
        self._locales_dir = Path(locales_dir)
        self._config_path = self._base_dir / ".iptv_editor_lang"

        self._packs = self._load_packs()
        self._current_lang = DEFAULT_LANG
        self.load_pref()

    def _load_packs(self) -> Dict[str, LanguagePack]:
        packs: Dict[str, LanguagePack] = {}
        if not self._locales_dir.exists():
            return packs
        for path in sorted(self._locales_dir.glob("*.json")):
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            meta = data.get("_meta", {}) if isinstance(data, dict) else {}
            name = meta.get("name") or meta.get("display_name") or path.stem
            strings = {k: v for k, v in data.items() if not k.startswith("_")}
            packs[path.stem] = LanguagePack(code=path.stem, name=name, strings=strings)
        return packs

    def lang_list(self) -> List[Tuple[str, str]]:
        if not self._packs:
            return [(DEFAULT_LANG, DEFAULT_LANG)]
        ordered: List[str] = []
        for code in PREFERRED_ORDER:
            if code in self._packs:
                ordered.append(code)
        for code in sorted(self._packs.keys()):
            if code not in ordered:
                ordered.append(code)
        return [(code, self._packs[code].name) for code in ordered]

    def current_lang(self) -> str:
        return self._current_lang

    def tr(self, key: str) -> str:
        pack = self._packs.get(self._current_lang) or self._packs.get(DEFAULT_LANG)
        if not pack:
            return key
        return pack.strings.get(key, key)

    def set_language(self, code: str, persist: bool = True) -> bool:
        if code in self._packs:
            self._current_lang = code
            if persist:
                self.save_pref(code)
            return True
        return False

    def load_pref(self) -> None:
        try:
            lang = self._config_path.read_text(encoding="utf-8").strip()
        except Exception:
            return
        if lang in self._packs:
            self._current_lang = lang

    def save_pref(self, code: str) -> None:
        try:
            self._config_path.write_text(code, encoding="utf-8")
        except Exception:
            pass


_manager: I18nManager | None = None


def init(base_dir: str | Path | None = None) -> I18nManager:
    global _manager
    _manager = I18nManager(base_dir=base_dir)
    return _manager


def _get_manager() -> I18nManager:
    global _manager
    if _manager is None:
        _manager = I18nManager()
    return _manager


def tr(key: str) -> str:
    return _get_manager().tr(key)


def get_lang_list() -> List[Tuple[str, str]]:
    return _get_manager().lang_list()


def get_current_lang() -> str:
    return _get_manager().current_lang()


def set_language(code: str, persist: bool = True) -> bool:
    return _get_manager().set_language(code, persist=persist)
