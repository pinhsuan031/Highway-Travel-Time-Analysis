#!/usr/bin/env python3
"""
將高公局 TDCS M04A 原始資料 (raw/M04A/YYYYMMDD/HH/*.csv)
依月份合併為 dataset/csv_1/year=YYYY/month=MM/data.csv
並套用門架 (GantryFrom, GantryTo) 配對修正。

用法:
    python3 partition_m04a.py [RAW_DIR] [OUT_DIR]
預設 RAW_DIR = raw/M04A, OUT_DIR = dataset/csv_1
"""

import csv
import sys
from pathlib import Path
from collections import defaultdict

RAW_DIR = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("../raw/M04A/")
OUT_DIR = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("../dataset/csv_1")

# (GantryFrom, GantryTo) -> (新GantryFrom, 新GantryTo)
GANTRY_FIX = {
    ("01F0147S", "01F0155S"): ("01F0147S", "01F0153S"),
    ("01F0155N", "01F0147N"): ("01F0153N", "01F0147N"),
    ("01F0155S", "01F0182S"): ("01F0153S", "01F0182S"),
    ("01F0213N", "01F0155N"): ("01F0213N", "01F0153N"),
    ("01F0339N", "01F0293N"): ("01F0340N", "01F0293N"),
    ("01F0339N", "01H0271N"): ("01F0340N", "01H0271N"),
    ("01F0376N", "01F0339N"): ("01F0376N", "01F0340N"),
    ("01F0467S", "01F0511S"): ("01F0467S", "01F0492S"),
    ("01F0511N", "01F0467N"): ("01F0511N", "01F0492N"),
    ("01F0578S", "01F0616S"): ("01F0578S", "01F0633S"),
    ("01F0616N", "01F0584N"): ("01F0633N", "01F0584N"),
    ("01F2425S", "01F2483S"): ("01F2425S", "01F2472S"),
    ("01F2483N", "01F2425N"): ("01F2472N", "01F2425N"),
    ("01F2483S", "01F2514S"): ("01F2472S", "01F2514S"),
    ("01F2514N", "01F2483N"): ("01F2514N", "01F2472N"),
    ("01F3460S", "01F3525S"): ("01F3460S", "01F3535S"),
    ("01F3525N", "01F3460N"): ("01F3535N", "01F3460N"),
    ("01F3525S", "01F3561S"): ("01F3535S", "01F3561S"),
    ("01F3559N", "01F3525N"): ("01F3559N", "01F3535N"),
    ("01H0447N", "01F0339N"): ("01H0447N", "01F0340N"),
    ("03F0337S", "03F0394S"): ("03F0337S", "03F0385S"),
    ("03F0394N", "03F0338N"): ("03F0385N", "03F0338N"),
    ("03F0394S", "03F0447S"): ("03F0385S", "03F0447S"),
    ("03F0447N", "03F0394N"): ("03F0447N", "03F0385N"),
    ("03F0698S", "03F0783S"): ("03F0698S", "03F0746S"),
    ("03F0783N", "03F0698N"): ("03F0783N", "03F0746N"),
}


def main():
    if not RAW_DIR.is_dir():
        sys.exit(f"找不到原始資料夾: {RAW_DIR}")

    # 依 YYYYMM 分組日期資料夾
    months = defaultdict(list)  # "YYYYMM" -> [日期資料夾...]
    for day_dir in sorted(RAW_DIR.iterdir()):
        name = day_dir.name
        if day_dir.is_dir() and len(name) == 8 and name.isdigit():
            months[name[:6]].append(day_dir)

    if not months:
        sys.exit(f"{RAW_DIR} 底下沒有 YYYYMMDD 格式的資料夾")

    total_rows = 0
    total_fixed = 0

    for ym in sorted(months):
        year, month = ym[:4], ym[4:6]
        out_path = OUT_DIR / f"year={year}" / f"month={month}" / "data.csv"
        out_path.parent.mkdir(parents=True, exist_ok=True)

        rows = 0
        fixed = 0
        with open(out_path, "w", newline="", encoding="utf-8") as fout:
            writer = csv.writer(fout)
            for day_dir in sorted(months[ym]):
                # 小時資料夾 00-23，若某小時缺少就跳過
                for hour_dir in sorted(p for p in day_dir.iterdir() if p.is_dir()):
                    for csv_file in sorted(hour_dir.glob("*.csv")):
                        with open(csv_file, newline="", encoding="utf-8") as fin:
                            for row in csv.reader(fin):
                                if len(row) < 6:
                                    continue
                                key = (row[1], row[2])
                                if key in GANTRY_FIX:
                                    row[1], row[2] = GANTRY_FIX[key]
                                    fixed += 1
                                writer.writerow(row)
                                rows += 1
        total_rows += rows
        total_fixed += fixed
        print(f"{year}-{month}: {rows:>12,} 列 (修正 {fixed:,} 列) -> {out_path}")

    print(f"\n完成。總計 {total_rows:,} 列，門架修正 {total_fixed:,} 列。")


if __name__ == "__main__":
    main()
