# DBV Ranking Scraper

抓取德国羽协（dbv.turnier.de）项目的排名列表（`table.ruler`），并**同时**导出为 CSV 与（可选）写入 SQLite。  
支持 CookieWall、稳健重试、流式写入、`RankChange`/`PreviousRank`、`PlayerId`、`RankWeek` 自动入库与命名。

## 目录结构

