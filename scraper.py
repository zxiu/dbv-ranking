#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scraper.py
只负责：建立稳定会话、处理 CookieWall、抓取 HTML、解析 table.ruler
产出：rows(List[Dict])、header_keys(List[str])、caption(str)、rank_week(str)
特性：
- 解析 RankWeek (RL-Woche: WW-YYYY -> YYYY-WW)
- 解析 PreviousRank（上一期名次）与 RankChange = PreviousRank - Rank
- 解析 PlayerId（来自 Spieler 列 <a href> 的 player=... 参数）
- 默认丢弃空白 UI 列 Flag；支持 keep_flag
- 默认不输出 *_raw 列；支持 keep_raw=True 输出
"""

import re
import time
import unicodedata
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse, urljoin, parse_qsl

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup, Tag

# 网络 & 重试
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; DBV-Ranking-Parser/1.0; +https://example.org)",
    "Accept": "text/html,application/xhtml+xml",
}
REQUEST_TIMEOUT = 30
REQUEST_SLEEP = 0.9  # 翻页间隔
MAX_SSL_RETRIES = 3

# 固定 raw 字段清单（用于 CSV 字段顺序）
FIXED_RAW_FIELDS = ["RankChange_raw", "Rank_raw", "BirthYear_raw", "Points_raw", "Tournaments_raw"]


# =============== 工具函数 ===============
def normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


def set_query_param(url: str, **params) -> str:
    """在原 URL 上更新/添加查询参数（保持其它参数不变）"""
    parts = urlparse(url)
    q = parse_qs(parts.query)
    for k, v in params.items():
        q[str(k)] = [str(v)]
    new_query = urlencode({k: v[0] for k, v in q.items()})
    return urlunparse((parts.scheme, parts.netloc, parts.path, parts.params, new_query, parts.fragment))


def slugify_for_filename(text: str) -> str:
    """把 caption 清洗为安全文件名片段"""
    if not text:
        return "UnknownCategory"
    t = unicodedata.normalize("NFKD", text)
    t = t.encode("ascii", "ignore").decode("ascii")
    t = re.sub(r"\s+", "-", t)
    t = re.sub(r"[^A-Za-z0-9_-]+", "-", t)
    t = re.sub(r"-{2,}", "-", t).strip("-_")
    return t or "UnknownCategory"


def create_session(no_proxy: bool = False) -> requests.Session:
    """稳定 Session：重试/退避、可禁用系统代理、关闭连接复用"""
    s = requests.Session()
    if no_proxy:
        s.trust_env = False

    retry = Retry(
        total=5, connect=5, read=5,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET", "POST"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=16, pool_maxsize=32)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({**HEADERS, "Connection": "close"})
    return s


# =============== CookieWall ===============
def _is_cookie_wall(html: str) -> bool:
    soup = BeautifulSoup(html, "html.parser")
    form = soup.find("form", attrs={"action": re.compile(r"/cookiewall/Save$", re.I)})
    return form is not None


def _extract_return_url_from_cookiewall(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    form = soup.find("form", attrs={"action": re.compile(r"/cookiewall/Save$", re.I)})
    if not form:
        raise RuntimeError("CookieWall 表单未找到")
    ret = form.find("input", {"name": "ReturnUrl"})
    if not ret or not ret.get("value"):
        raise RuntimeError("CookieWall ReturnUrl 未找到")
    return ret["value"]


def _accept_cookies(sess: requests.Session, base: str, html: str, accept_all: bool = True) -> str:
    return_url = _extract_return_url_from_cookiewall(html)
    post_url = urljoin(base, "/cookiewall/Save")
    data = {"ReturnUrl": return_url, "SettingsOpen": "false"}
    if accept_all:
        data["CookiePurposes"] = ["1", "2", "4", "16"]

    r = sess.post(post_url, headers=sess.headers, data=data, allow_redirects=True, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    final_url = urljoin(base, return_url)
    r2 = sess.get(final_url, headers=sess.headers, timeout=REQUEST_TIMEOUT)
    r2.raise_for_status()
    return r2.text


def fetch_html_with_cookiewall(url: str, sess: Optional[requests.Session] = None) -> str:
    """GET 页面；若命中 CookieWall，则自动提交并重取；随机 SSL 错误重建会话重试"""
    owns = False
    s = sess
    if s is None:
        s = create_session()
        owns = True

    try:
        ssl_attempt = 0
        while True:
            try:
                r = s.get(url, headers=s.headers, timeout=REQUEST_TIMEOUT, allow_redirects=True)
                r.raise_for_status()
                html = r.text
                if _is_cookie_wall(html):
                    base = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
                    try:
                        html = _accept_cookies(s, base, html, accept_all=True)
                    except RuntimeError:
                        return html
                return html
            except requests.exceptions.SSLError:
                ssl_attempt += 1
                if ssl_attempt >= MAX_SSL_RETRIES:
                    raise
                time.sleep(0.6 * ssl_attempt)
                if owns:
                    s.close()
                    s = create_session()
                continue
    finally:
        if owns:
            s.close()


# =============== RankWeek / Caption ===============
def parse_rank_week(html: str) -> Optional[str]:
    """
    RL-Woche: <span class="rankingdate">(WW-YYYY)</span> 或 <select class="publication"><option selected>WW-YYYY</option>
    返回 YYYY-WW
    """
    soup = BeautifulSoup(html, "html.parser")

    nd = soup.select_one(".rankingdate")
    if nd:
        raw = nd.get_text(strip=True).strip("()")
        m = re.match(r"(\d{1,2})-(\d{4})$", raw)
        if m:
            ww, yyyy = m.group(1).zfill(2), m.group(2)
            return f"{yyyy}-{ww}"

    opt = soup.select_one("select.publication option[selected]")
    if opt:
        raw = opt.get_text(strip=True)
        m = re.match(r"(\d{1,2})-(\d{4})$", raw)
        if m:
            ww, yyyy = m.group(1).zfill(2), m.group(2)
            return f"{yyyy}-{ww}"

    chosen_span = soup.select_one("#cphPage_cphPage_cphPage_dlPublication_chosen a.chosen-single span")
    if chosen_span:
        raw = chosen_span.get_text(strip=True)
        m = re.match(r"(\d{1,2})-(\d{4})$", raw)
        if m:
            ww, yyyy = m.group(1).zfill(2), m.group(2)
            return f"{yyyy}-{ww}"

    return None


def extract_caption(html_or_table: str) -> Optional[str]:
    soup = BeautifulSoup(html_or_table, "html.parser")
    table = soup.find("table", class_="ruler")
    if not table:
        table = soup.find("table", attrs={"class": re.compile(r"\bruler\b")})
    if not table:
        return None
    cap = table.find("caption")
    return normalize_ws(cap.get_text(" ", strip=True)) if cap else None


# =============== 表格解析 ===============
def _expand_header_cells(th: Tag) -> List[str]:
    """展开带 colspan 的表头单元格；空白表头命名为 'Flag'。"""
    text = normalize_ws(th.get_text(" ", strip=True))
    if text == "":
        text = "Flag"
    colspan = int(th.get("colspan", 1))
    if colspan <= 1:
        return [text]
    out = [text]
    for i in range(2, colspan + 1):
        out.append(f"{text}#{i}")
    return out


def _extract_cell_text(td: Tag) -> str:
    """
    - 若 class 含 rank_equal / rank_up / rank_down：优先从 title 里取 'Previous rank: X' 的 X；
    - 否则优先取 <a> 文本，最后退回纯文本。
    """
    if td is None:
        return ""
    classes = td.get("class") or []
    if any(c in classes for c in ("rank_equal", "rank_up", "rank_down")):
        m = re.search(r"Previous rank:\s*(\d+)", td.get("title") or "")
        if m:
            return m.group(1)
    a = td.find("a")
    return normalize_ws(a.get_text(" ", strip=True) if a else td.get_text(" ", strip=True))


def _extract_player_id_from_td(td: Tag) -> Optional[int]:
    """从 Spieler 列 <a href="player.aspx?...&player=3423713"> 解析 PlayerId"""
    if td is None:
        return None
    a = td.find("a")
    if not a or not a.get("href"):
        return None
    href = a["href"]
    # href 可能是相对路径，直接解析查询参数
    try:
        q = dict(parse_qsl(urlparse(href).query))
        val = q.get("player")
        if val and re.fullmatch(r"\d+", val):
            return int(val)
    except Exception:
        return None
    return None


def parse_ruler_table(
    html_or_table: str,
    keep_flag: bool = False,
    rank_week: Optional[str] = None,
    keep_raw: bool = False,  # 默认不保留 *_raw
) -> Tuple[List[Dict], List[str]]:
    """
    解析 <table class="ruler">：
    - 映射列：Rang -> Rank；Rang#2 -> RankChange（初始装“上一名次X”，稍后转为 X-Rank）
    - 跳过分页行：<td class='noruler'>
    - 默认丢弃 Flag 列，可 keep_flag=True 保留
    - 增补列：PreviousRank（上一期名次）、PlayerId（来自 Spieler 链接）、RankWeek（最后）
    - 数值字段转 int；可选保留 *_raw
    返回 (rows, header_keys)
    """
    soup = BeautifulSoup(html_or_table, "html.parser")
    table = soup.find("table", class_="ruler")
    if not table:
        table = soup.find("table", attrs={"class": re.compile(r"\bruler\b")})
    if not table:
        raise ValueError("未找到 <table class='ruler'>")

    tbody = table.find("tbody") or table
    header_tr = tbody.find("tr")
    raw_headers: List[str] = []
    for th in header_tr.find_all("th"):
        raw_headers.extend(_expand_header_cells(th))

    # 规范化列名
    canon = {
        "rang": "Rank",
        "rang#2": "RankChange",
        "spieler": "Player",
        "spieler/in": "Player",
        "gjahr": "BirthYear",
        "geburtsjahr": "BirthYear",
        "punkte": "Points",
        "region": "Region",
        "verein": "Club",
        "turniere": "Tournaments",
        "flag": "Flag",
    }
    header_keys = [canon.get(h.lower(), h) for h in raw_headers]

    # 记录 Flag 的原始索引（此时未插入任何虚拟列）
    drop_flag = (not keep_flag) and ("Flag" in header_keys)
    flag_index = header_keys.index("Flag") if drop_flag else None

    # 从 header 去掉 Flag
    if drop_flag:
        header_keys = [h for h in header_keys if h != "Flag"]

    # 在 RankChange 之后插入 PreviousRank（虚拟列，不参与 zip）
    if "PreviousRank" not in header_keys:
        if "RankChange" in header_keys:
            idx = header_keys.index("RankChange") + 1
            header_keys.insert(idx, "PreviousRank")
        else:
            header_keys.insert(1, "PreviousRank")

    # 在 Player 之后插入 PlayerId（虚拟列，不参与 zip）
    if "PlayerId" not in header_keys:
        if "Player" in header_keys:
            idx = header_keys.index("Player") + 1
            header_keys.insert(idx, "PlayerId")
        else:
            header_keys.append("PlayerId")

    rows: List[Dict] = []
    for tr in tbody.find_all("tr"):
        if tr.find("th"):
            continue
        if tr.find("td", class_="noruler"):
            continue

        tds = tr.find_all("td")
        if not tds:
            continue

        values = [_extract_cell_text(td) for td in tds]

        # 删除“空白 UI 列”对应的值（用原始 flag_index，避免错删 Player）
        if drop_flag and flag_index is not None and flag_index < len(values):
            del values[flag_index]

        # 构造 zip 对齐的表头（排除虚拟列）
        headers_for_zip = [h for h in header_keys if h not in ("PreviousRank", "PlayerId", "RankWeek")]

        if len(values) < len(headers_for_zip):
            values += [""] * (len(headers_for_zip) - len(values))
        if len(values) > len(headers_for_zip):
            values = values[: len(headers_for_zip)]

        rec = dict(zip(headers_for_zip, values))

        # 抓 PlayerId
        try:
            pid = _extract_player_id_from_td(tds[3])  # 注意：原始第4列是 Spieler
        except Exception:
            pid = None
        rec["PlayerId"] = pid if pid is not None else ""

        # 转数值；此时 RankChange 仍是“上一名次X”
        for f in ("Rank", "BirthYear", "Points", "Tournaments", "RankChange"):
            if f in rec:
                if keep_raw:
                    rec[f + "_raw"] = rec[f]
                num = re.sub(r"[^\d\-]", "", str(rec[f]))
                if num:
                    try:
                        rec[f] = int(num)
                    except ValueError:
                        pass

        # 计算 PreviousRank 与 RankChange
        prev_rank_int = None
        try:
            raw_prev = rec.get("RankChange_raw") if keep_raw else rec.get("RankChange")
            if isinstance(rec.get("RankChange"), int) and not keep_raw:
                prev_rank_int = rec["RankChange"]  # 不保留 raw 时，这里暂存的是“上一名次X”
            else:
                m = re.search(r"\d+", str(raw_prev)) if raw_prev is not None else None
                if m:
                    prev_rank_int = int(m.group())
        except Exception:
            prev_rank_int = None

        curr_rank_int = rec.get("Rank") if isinstance(rec.get("Rank"), int) else None
        if prev_rank_int is not None and curr_rank_int is not None:
            rec["PreviousRank"] = prev_rank_int
            rec["RankChange"] = prev_rank_int - curr_rank_int
        else:
            rec["PreviousRank"] = ""
            rec["RankChange"] = 0

        # RankWeek
        rec["RankWeek"] = rank_week or ""

        rows.append(rec)

    # 确保 RankWeek 在表头
    if "RankWeek" not in header_keys:
        header_keys.append("RankWeek")

    return rows, header_keys


# =============== 首页解析辅助（供 main 使用） ===============
def fetch_first_page_info(url: str, page_size: int, no_proxy: bool = False) -> Tuple[str, str, List[Dict], List[str]]:
    """
    拉取起始页（保留 URL 其它查询参数），返回 (rank_week, caption, rows, header_keys)
    """
    with create_session(no_proxy=no_proxy) as sess:
        url1 = set_query_param(url, ps=page_size)
        # 不强求 p=1（允许用户传 p=K 起步）；只是统一 page_size
        html = fetch_html_with_cookiewall(url1, sess=sess)
        rank_week = parse_rank_week(html) or "UnknownWeek"
        caption = extract_caption(html) or "UnknownCategory"
        rows, header_keys = parse_ruler_table(html, keep_flag=False, rank_week=rank_week, keep_raw=False)
        return rank_week, caption, rows, header_keys