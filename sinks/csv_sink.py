# sinks/csv_sink.py
import os
import csv
from typing import List, Dict

class CSVWriterSink:
    """
    with CSVWriterSink(out_csv, fieldnames) as sink:
        sink.write_many(rows)
    """
    def __init__(self, out_csv: str, fieldnames: List[str]):
        self.out_csv = out_csv
        self.fieldnames = fieldnames
        self._f = None
        self._w = None

    def __enter__(self):
        os.makedirs(os.path.dirname(self.out_csv) or ".", exist_ok=True)
        self._f = open(self.out_csv, "w", newline="", encoding="utf-8")
        self._w = csv.DictWriter(self._f, fieldnames=self.fieldnames)
        self._w.writeheader()
        return self

    def __exit__(self, exc_type, exc, tb):
        if self._f:
            self._f.close()
            self._f = None
            self._w = None

    def write_many(self, rows: List[Dict]):
        for r in rows:
            self._w.writerow(r)