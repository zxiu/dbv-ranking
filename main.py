#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
main.py
CLI 编排：
1) 抓第一页：拿 rank_week / caption / header_keys / 第一页 rows
2) 决定 CSV 输出路径：output-csv/<rank_week>/dbv_rankings_<rank_week>_<caption>[_first_N].csv
3) 打开 CSV & SQLite sink
4) 逐页抓取并同步写入（--max-rank 优先；--stream 流式）
"""

import os
import re
import time
import argparse
from typing import List, Dict, Optional

from scraper import (
    create_session, fetch_html_with_cookiewall, parse_ruler_table,
    parse_rank_week, extract_caption, set_query_param, slugify_for_filename,
    REQUEST_SLEEP, FIXED_RAW_FIELDS
)
from sinks.csv_sink import CSVWriterSink
from sinks.sqlite_sink import SQLiteSink

# -------- CSV 字段顺序计算：常规列 -> (*_raw 可选) -> RankWeek 最后 --------
def compute_fieldnames(header_keys: List[str], keep_raw: bool) -> List[str]:
    base_headers = [k for k in header_keys if k != "RankWeek"]
    seen = set(base_headers)
    if keep_raw:
        for k in FIXED_RAW_FIELDS:
            if k not in seen:
                base_headers.append(k)
                seen.add(k)
    if "RankWeek" in header_keys:
        base_headers.append("RankWeek")
    return base_headers


def main():
    ap = argparse.ArgumentParser(
        description="DBV 排名抓取并导出 CSV/SQLite（含 RankChange、PreviousRank、PlayerId、RankWeek）"
    )
    ap.add_argument("--url", required=True, help="起始 URL，例如 https://dbv.turnier.de/ranking/category.aspx?id=47428&category=3440&p=1&ps=100")
    ap.add_argument("--page-size", type=int, default=100, help="每页条数 ps（默认 100）")
    ap.add_argument("--start-page", type=int, default=1, help="起始页 p（默认 1）")
    ap.add_argument("--max-pages", type=int, default=None, help="最多抓取的页数上限；若与 --max-rank 同给，将被忽略")
    ap.add_argument("--max-rank", dest="max_rank", type=int, default=None, help="仅抓取/输出前 N 名（优先于 --max-pages）")
    ap.add_argument("--output", default="", help="输出 CSV 文件名（默认自动命名；始终写入 output-csv/<rank-week>/ 目录）")
    ap.add_argument("--keep-flag", action="store_true", help="保留空白 UI 列 'Flag'（默认不保留）")
    ap.add_argument("--no-proxy", action="store_true", help="禁用系统代理环境变量（排查代理导致的 TLS 问题）")
    ap.add_argument("--stream", action="store_true", help="启用流式写入：解析一页立刻写入 CSV/SQLite")
    # raw 审计开关：默认不输出 *_raw；需要时 --with-raw
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--no-raw", dest="keep_raw", action="store_false", help="不输出任何 *_raw 列（默认）")
    g.add_argument("--with-raw", dest="keep_raw", action="store_true", help="输出 *_raw 列以便审计/回溯")
    ap.set_defaults(keep_raw=False)

    # SQLite 相关
    ap.add_argument("--to-sqlite", action="store_true", help="同时写入 SQLite（默认关闭）")
    ap.add_argument("--db-path", default="db/rankings.sqlite", help="SQLite 路径（默认 db/rankings.sqlite）")

    args = ap.parse_args()

    if (args.max_pages is not None) and (args.max_rank is not None):
        print("[提示] 同时给了 --max-pages 与 --max-rank；将以 --max-rank 为准，忽略 --max-pages。")
    use_max_pages = (args.max_pages is not None) and (args.max_rank is None)

    # 起始页
    page = args.start_page
    with create_session(no_proxy=args.no_proxy) as sess:
        url_first = set_query_param(set_query_param(args.url, p=page), ps=args.page_size)
        print(f"[抓取] 第 {page} 页: {url_first}")
        html = fetch_html_with_cookiewall(url_first, sess=sess)

        # 基础信息
        rank_week = parse_rank_week(html) or "UnknownWeek"
        caption_text = extract_caption(html) or "UnknownCategory"

        # 解析第一页
        rows, header_keys = parse_ruler_table(html, keep_flag=args.keep_flag, rank_week=rank_week, keep_raw=args.keep_raw)
        # 字段顺序
        fieldnames = compute_fieldnames(header_keys, keep_raw=args.keep_raw)

        # CSV 文件路径（统一 output-csv/<rank_week>/）
        base_dir = os.path.join("output-csv", rank_week)
        if args.output.strip():
            filename = os.path.basename(args.output.strip())
        else:
            caption_slug = slugify_for_filename(caption_text)
            if args.max_rank is not None:
                filename = f"dbv_rankings_{rank_week}_{caption_slug}_first_{args.max_rank}.csv"
            else:
                filename = f"dbv_rankings_{rank_week}_{caption_slug}.csv"
        out_csv = os.path.join(base_dir, filename)
        print(f"[命名] RankWeek='{rank_week}', caption='{caption_text}' → CSV：{out_csv}")

        # 打开 sinks
        csv_sink = CSVWriterSink(out_csv, fieldnames)
        sqlite_sink = SQLiteSink(args.db_path) if args.to_sqlite else None

        with csv_sink as csv_out, (sqlite_sink if sqlite_sink else open(os.devnull, "w")) as maybe_sql:
            if sqlite_sink:
                print(f"[SQLite] 写入数据库：{args.db_path}")
                sqlite_sink.ensure_schema()

            def sink_write(page_rows: List[Dict]):
                if not page_rows:
                    return
                csv_out.write_many(page_rows)
                if sqlite_sink:
                    sqlite_sink.write_many(page_rows, caption=caption_text, rank_week=rank_week)

            # 写第一页
            if args.max_rank is not None:
                to_write = [r for r in rows if isinstance(r.get("Rank"), int) and r["Rank"] <= args.max_rank]
            else:
                to_write = rows
            print(f"  ↳ 第 {page} 页解析 {len(rows)} 条，写入 {len(to_write)} 条")
            sink_write(to_write)

            pages_done = 1
            written = len(to_write)
            last_count = len(rows)

            # 终止条件（第一页后）
            if args.max_rank is not None and written >= args.max_rank:
                print(f"[完成] 已写前 {written} 条（达到 --max-rank） → {out_csv}")
                return
            if last_count < args.page_size:
                if args.max_rank is not None and written < args.max_rank:
                    print(f"[提示] 实际仅有 {written} 条；少于 --max-rank={args.max_rank}。")
                print(f"[完成] 共 {pages_done} 页，已写入 {out_csv}")
                return
            if use_max_pages and pages_done >= args.max_pages:
                print(f"[停止] 已达到 --max-pages={args.max_pages} 上限。")
                print(f"[完成] 已写入 {out_csv}")
                return

            # 后续页循环
            while True:
                page += 1
                url = set_query_param(set_query_param(args.url, p=page), ps=args.page_size)
                print(f"[抓取] 第 {page} 页: {url}")
                html = fetch_html_with_cookiewall(url, sess=sess)
                rows, _ = parse_ruler_table(html, keep_flag=args.keep_flag, rank_week=rank_week, keep_raw=args.keep_raw)

                if args.stream:
                    if args.max_rank is not None:
                        # 只写 rank <= max_rank；一旦页内都超过范围则停止
                        to_write = []
                        for r in rows:
                            rk = r.get("Rank")
                            if isinstance(rk, int) and rk <= args.max_rank:
                                to_write.append(r)
                            else:
                                break
                        print(f"  ↳ 解析 {len(rows)} 条，写入 {len(to_write)} 条（流式）")
                        sink_write(to_write)
                        written += len(to_write)
                        if written >= args.max_rank:
                            print(f"[完成] 已写前 {written} 条（达到 --max-rank） → {out_csv}")
                            break
                        if len(to_write) == 0:
                            print("[停止] 本页 Rank 全部超过 --max-rank，停止抓取后续页。")
                            break
                    else:
                        print(f"  ↳ 解析 {len(rows)} 条（流式写入）")
                        sink_write(rows)
                else:
                    # 非流式：同样直接写（为了避免内存占用）；如果你坚持聚合，可改为 all_rows.extend(rows)
                    if args.max_rank is not None:
                        to_write = []
                        for r in rows:
                            rk = r.get("Rank")
                            if isinstance(rk, int) and rk <= args.max_rank:
                                to_write.append(r)
                            else:
                                break
                        print(f"  ↳ 解析 {len(rows)} 条，写入 {len(to_write)} 条")
                        sink_write(to_write)
                        written += len(to_write)
                        if written >= args.max_rank:
                            print(f"[完成] 已写前 {written} 条（达到 --max-rank） → {out_csv}")
                            break
                    else:
                        print(f"  ↳ 解析 {len(rows)} 条")
                        sink_write(rows)

                pages_done += 1
                if len(rows) < args.page_size:
                    print("  ↳ 本页少于 page_size，推断为最后一页，停止。")
                    break
                if use_max_pages and pages_done >= args.max_pages:
                    print(f"[停止] 已达到 --max-pages={args.max_pages} 上限。")
                    break

                time.sleep(REQUEST_SLEEP)

        print(f"[完成] 共 {pages_done} 页，已写入 {out_csv}")
        if args.max_rank is not None and written < args.max_rank:
            print(f"[提示] 实际仅有 {written} 条；少于 --max-rank={args.max_rank}。")


if __name__ == "__main__":
    main()
