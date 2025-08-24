# sinks/sqlite_sink.py
import os
import sqlite3
from typing import Iterable, Dict, Sequence, Optional

DDL = """
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
CREATE INDEX IF NOT EXISTS idx_rankings_week_caption_rank
  ON rankings (RankWeek, Caption, Rank);
"""

UPSERT = """
INSERT INTO rankings
  (Rank, RankChange, PreviousRank, Player, PlayerId, BirthYear, Points, Region, Club, Tournaments, RankWeek, Caption)
VALUES
  (:Rank, :RankChange, :PreviousRank, :Player, :PlayerId, :BirthYear, :Points, :Region, :Club, :Tournaments, :RankWeek, :Caption)
ON CONFLICT(RankWeek, Caption, PlayerId) DO UPDATE SET
  Rank         = excluded.Rank,
  RankChange   = excluded.RankChange,
  PreviousRank = excluded.PreviousRank,
  Player       = excluded.Player,
  BirthYear    = excluded.BirthYear,
  Points       = excluded.Points,
  Region       = excluded.Region,
  Club         = excluded.Club,
  Tournaments  = excluded.Tournaments;
"""

ALLOWED_KEYS = {
    "Rank", "RankChange", "PreviousRank", "Player", "PlayerId",
    "BirthYear", "Points", "Region", "Club", "Tournaments", "RankWeek", "Caption"
}

class SQLiteSink:
    """
    用法：
      with SQLiteSink("db/rankings.sqlite") as sink:
          sink.ensure_schema()
          sink.write_many(rows, caption, rank_week)
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        self.conn: Optional[sqlite3.Connection] = None

    def __enter__(self):
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self.conn.execute("PRAGMA foreign_keys=ON;")
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.conn is not None:
            if exc is None:
                self.conn.commit()
            else:
                self.conn.rollback()
            self.conn.close()
            self.conn = None

    def ensure_schema(self):
        assert self.conn is not None
        cur = self.conn.cursor()
        for stmt in DDL.strip().split(";\n"):
            if stmt.strip():
                cur.execute(stmt)
        self.conn.commit()

    def _adapt_row(self, row: Dict, caption: str, rank_week: str) -> Dict:
        # 过滤掉 *_raw 等非数据库列，并补齐 Caption/RankWeek
        out = {k: row.get(k) for k in ALLOWED_KEYS if k in row}
        out["Caption"] = caption
        out["RankWeek"] = rank_week
        return out

    def write_many(self, rows: Sequence[Dict], caption: str, rank_week: str):
        if not rows:
            return
        assert self.conn is not None
        adapted = [self._adapt_row(r, caption, rank_week) for r in rows]
        self.conn.executemany(UPSERT, adapted)
        self.conn.commit()
