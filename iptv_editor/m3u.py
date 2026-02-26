import re
from typing import List, Tuple
from urllib.parse import urlparse


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
    Support line formats:
      1) name|URL|group|logo
      2) name,URL,group,logo
      3) URL only
    Ignore blank lines and lines starting with #.
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
    Output Emby-friendly M3U (no EPG).
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
