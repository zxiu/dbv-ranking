# DBV Ranking Scraper

抓取德国羽协（dbv.turnier.de）项目的排名列表（`table.ruler`），并**同时**导出为 CSV 与（可选）写入 SQLite。  
支持 CookieWall、稳健重试、流式写入、`RankChange`/`PreviousRank`、`PlayerId`、`RankWeek` 自动入库与命名。

## 目录结构

```
project/
  main.py             # CLI：编排抓取 → CSV/SQLite 同步写
  scraper.py          # 抓取+解析，只返回 rows / header / caption / rank_week
  sinks/
    csv_sink.py       # 写 CSV
    sqlite_sink.py    # 写 SQLite（主键：RankWeek, Caption, PlayerId）
  output-csv/         # 运行后生成（按 RankWeek 分目录）
  db/                 # SQLite 数据库位置（默认 db/rankings.sqlite）
```

> 建议在 `sinks/` 目录下放一个空的 `__init__.py`，以方便某些环境的导入。

## 环境要求

- Python 3.9+
- 依赖：
  - `requests`
  - `beautifulsoup4`
  - （可选）`lxml`（安装后 `bs4` 会更快，但不是必需）

安装（示例）：
```bash
pip install -U requests beautifulsoup4 lxml
```

## 快速开始

抓取并导出 CSV，同时写入 SQLite，示例只抓前 250 名并**流式写入**：
```bash
python main.py   --url "https://dbv.turnier.de/ranking/category.aspx?id=47428&category=3440&p=1&ps=100"   --max-rank 250   --stream   --to-sqlite   --db-path db/rankings.sqlite
```

仅写 CSV，默认不含 `*_raw` 列（更干净）：
```bash
python main.py --url "https://dbv.turnier.de/ranking/category.aspx?id=47428&category=3440&p=1&ps=100"
```

启用审计模式，额外输出 `*_raw` 列：
```bash
python main.py --url "..." --with-raw
```

指定 CSV 文件名（仍会放在 `output-csv/<RankWeek>/` 下）：
```bash
python main.py --url "..." --output custom.csv
```

## CLI 选项

| 选项 | 说明 |
|---|---|
| `--url` | **必填**。起始页面 URL（可带 `p`/`ps`，程序会按 `--page-size` 统一 `ps`） |
| `--page-size` | 每页条数（默认 `100`） |
| `--start-page` | 起始页 `p`（默认 `1`） |
| `--max-pages` | 最多抓取页数上限（若与 `--max-rank` 同给，以 `--max-rank` 为准） |
| `--max-rank` | 只抓前 N 名（**优先**于 `--max-pages`；CSV 文件名会带 `_first_<N>`） |
| `--output` | 输出 CSV 文件名（默认自动命名：`dbv_rankings_<RankWeek>_<Caption>[_first_N].csv`） |
| `--keep-flag` | 保留 table 里的空白 UI 列 `Flag`（默认丢弃） |
| `--no-proxy` | 禁用系统代理环境变量（排查 TLS/代理导致的问题） |
| `--stream` | **流式**写入：解析一页立即写入 CSV/SQLite，几乎不占内存 |
| `--no-raw` / `--with-raw` | 是否输出 `*_raw` 列（默认 `--no-raw`） |
| `--to-sqlite` | 同时写入 SQLite（默认关闭） |
| `--db-path` | SQLite 路径（默认 `db/rankings.sqlite`） |

## 输出内容

### CSV
- 路径：`output-csv/<RankWeek>/dbv_rankings_<RankWeek>_<Caption>[_first_N].csv`
- 列（顺序）：常规列 → （可选 `*_raw`）→ **`RankWeek` 永远最后**
- 主要字段：
  - `Rank`：当前名次  
  - `RankChange`：`PreviousRank - Rank`（正=上升，负=下降，不变=0）  
  - `PreviousRank`：上一期名次（来自单元格 `title="Previous rank: X"`）  
  - `Player`：球员姓名  
  - `PlayerId`：从 `Spieler` 列链接中的 `player=...` 解析  
  - `BirthYear`、`Points`、`Region`、`Club`、`Tournaments`  
  - `RankWeek`：`YYYY-WW`（如 `2025-33`）

> 审计模式（`--with-raw`）下会额外包含：`RankChange_raw / Rank_raw / BirthYear_raw / Points_raw / Tournaments_raw`。

### SQLite

- 默认库：`db/rankings.sqlite`
- 表：`rankings`
- 主键：`(RankWeek, Caption, PlayerId)`
- 结构：
  ```sql
  CREATE TABLE IF NOT EXISTS rankings (
    Rank         INTEGER,
    RankChange   INTEGER,
    PreviousRank INTEGER,
    Player       TEXT,
    PlayerId     INTEGER,
    BirthYear    INTEGER,
    Points       INTEGER,
    Region       TEXT,
    Club         TEXT,
    Tournaments  INTEGER,
    RankWeek     TEXT,
    Caption      TEXT,
    PRIMARY KEY (RankWeek, Caption, PlayerId)
  );
  ```
- 冲突（同主键）时自动 **UPSERT** 更新指标列。

## 解析规则摘要

- 表格：`<table class="ruler">`
- 列头映射：`Rang` → `Rank`；`Rang#2`（`colspan=2` 展开）作为 `RankChange` 的**占位**，最终会被替换为 `PreviousRank` 与真实的 `RankChange`
- `PlayerId`：来自球员列 `<a href="player.aspx?...&player=XXXX">` 的 `player` 参数
- `RankWeek`：从 `RL-Woche: WW-YYYY` 解析为 `YYYY-WW`；用于文件夹与 SQLite 主键

## 注意与建议

- **礼貌抓取**：默认有 `REQUEST_SLEEP≈0.9s` 的翻页间隔，必要时适当增大。
- **CookieWall**：自动提交 `/cookiewall/Save`，若失败会返回原页（通常只需重试）。
- **SSL 问题**：若偶发 `SSLError`，脚本会重建 Session 重试；如果你在有代理的环境，试试 `--no-proxy`。
- **唯一性**：数据库主键用了 `PlayerId`，避免同名冲突；如果目标站结构未来变动，请关注 `scraper.py` 的 `PlayerId` 解析函数。

## 开发与扩展

- 想加更多目标（如 Parquet / DuckDB / Postgres）：仿照 `sinks/` 新增一个 sink 类并在 `main.py` 注册即可。
- 想把 `RankWeek`/`Caption` 也写进 CSV 文件名（或目录层级）已实现：`output-csv/<RankWeek>/...`。
- 性能：默认流式写入已极低内存占用；如要聚合操作，可在 `main.py` 中调整。

## 许可证

请选择并添加一个开源许可证（例如 MIT、Apache-2.0 等），并在仓库根目录放置 `LICENSE` 文件。
