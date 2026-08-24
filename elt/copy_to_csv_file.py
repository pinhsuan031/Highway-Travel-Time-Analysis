#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
copy_existing_m04a_csv_home.py

用途：
把 ~/dataset/M04A/{pair_id}/ 底下「實際存在」的 2021~2025 CSV
複製到 ~/dataset/csv/M04A/{pair_id}/

重點：
1. 只複製實際存在的 2021~2025 CSV
2. 不會顯示缺少年份，因為缺少年份不是錯誤
3. 不會刪除目標資料夾
4. 不會動到原本的 2026 CSV
5. 只有「有來源檔案，但複製失敗」才會在 terminal 顯示
6. 預設來源與目標都在家目錄 ~/dataset 底下
"""

import argparse
import shutil
from pathlib import Path
from datetime import datetime


def parse_years(years_text: str) -> set[str]:
    years_text = years_text.strip()

    if "-" in years_text:
        start, end = years_text.split("-", 1)
        return {str(y) for y in range(int(start), int(end) + 1)}

    return {y.strip() for y in years_text.split(",") if y.strip()}


def is_target_year_csv(csv_file: Path, years: set[str]) -> bool:
    return (
        csv_file.is_file()
        and csv_file.suffix.lower() == ".csv"
        and len(csv_file.name) >= 5
        and csv_file.name[:4] in years
        and csv_file.name[4] == "_"
    )


def copy_existing_csv_files(
    src_root: Path,
    dst_root: Path,
    years: set[str],
    dry_run: bool = False,
    skip_existing: bool = False,
    show_success: bool = False,
) -> None:
    src_root = src_root.expanduser().resolve()
    dst_root = dst_root.expanduser().resolve()

    if not src_root.exists():
        raise FileNotFoundError(f"來源資料夾不存在: {src_root}")

    pair_dirs = sorted([p for p in src_root.iterdir() if p.is_dir()])

    total_pair_dirs = len(pair_dirs)
    found_csv_count = 0
    copied_count = 0
    skipped_count = 0
    error_count = 0
    no_target_csv_folder_count = 0

    failed_records = []

    print("=" * 70)
    print("M04A Existing CSV Copy")
    print(f"開始時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"來源根目錄: {src_root}")
    print(f"目標根目錄: {dst_root}")
    print(f"只複製這些年份: {sorted(years)}")
    print(f"來源門架資料夾數量: {total_pair_dirs}")
    print(f"Dry run: {dry_run}")
    print(f"Skip existing: {skip_existing}")
    print("=" * 70)

    # 先測試目標根目錄能不能建立
    if not dry_run:
        try:
            dst_root.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print("\n[錯誤] 目標根目錄無法建立或無法寫入")
            print(f"目標根目錄: {dst_root}")
            print(f"錯誤類型: {type(e).__name__}")
            print(f"錯誤原因: {e}")
            return

    for idx, src_pair_dir in enumerate(pair_dirs, start=1):
        pair_id = src_pair_dir.name
        dst_pair_dir = dst_root / pair_id

        if idx % 50 == 0 or idx == 1 or idx == total_pair_dirs:
            print(f"[進度] {idx}/{total_pair_dirs}，目前處理: {pair_id}")

        matched_csv_files = sorted([
            csv_file for csv_file in src_pair_dir.glob("*.csv")
            if is_target_year_csv(csv_file, years)
        ])

        if not matched_csv_files:
            no_target_csv_folder_count += 1
            continue

        for src_file in matched_csv_files:
            found_csv_count += 1
            dst_file = dst_pair_dir / src_file.name

            if dst_file.exists() and skip_existing:
                skipped_count += 1
                continue

            try:
                if dry_run:
                    print(f"[DRY RUN] copy: {src_file} -> {dst_file}")
                    copied_count += 1
                    continue

                dst_pair_dir.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src_file, dst_file)

                # 複製後確認目標檔案存在，且檔案大小一致
                if not dst_file.exists():
                    raise RuntimeError("複製後目標檔案不存在")

                if src_file.stat().st_size != dst_file.stat().st_size:
                    raise RuntimeError(
                        f"複製後檔案大小不一致，來源大小={src_file.stat().st_size}, 目標大小={dst_file.stat().st_size}"
                    )

                copied_count += 1

                if show_success:
                    print(f"[成功] {src_file} -> {dst_file}")

            except Exception as e:
                error_count += 1
                failed_records.append({
                    "source": str(src_file),
                    "destination": str(dst_file),
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                })

    print("=" * 70)
    print("執行完成")
    print(f"結束時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"來源門架資料夾數量: {total_pair_dirs}")
    print(f"找到可複製的 2021~2025 CSV 數量: {found_csv_count}")
    print(f"成功複製檔案數: {copied_count}")
    print(f"略過既有檔案數: {skipped_count}")
    print(f"沒有 2021~2025 CSV 的資料夾數: {no_target_csv_folder_count}")
    print(f"複製失敗檔案數: {error_count}")
    print("=" * 70)

    if failed_records:
        print("\n沒有成功複製的 CSV：")
        for row in failed_records[:50]:
            print(f"來源: {row['source']}")
            print(f"目標: {row['destination']}")
            print(f"錯誤類型: {row['error_type']}")
            print(f"錯誤原因: {row['error_message']}")
            print("-" * 70)

        if len(failed_records) > 50:
            print(f"... 還有 {len(failed_records) - 50} 筆失敗，畫面只顯示前 50 筆。")
    else:
        print("\n沒有複製失敗的 CSV。")


def main():
    parser = argparse.ArgumentParser(
        description="Copy existing M04A 2021~2025 CSV files from home dataset to home dataset/csv."
    )

    parser.add_argument(
        "--src-root",
        default="~/dataset/M04A",
        help="來源根目錄，預設: ~/dataset/M04A",
    )

    parser.add_argument(
        "--dst-root",
        default="~/dataset/csv/M04A",
        help="目標根目錄，預設: ~/dataset/csv/M04A",
    )

    parser.add_argument(
        "--years",
        default="2021-2025",
        help="要複製的年份，預設: 2021-2025，也可寫 2021,2022,2023",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="只顯示會複製哪些檔案，不真的執行複製",
    )

    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="如果目標端已經有同名 CSV，就略過不覆蓋",
    )

    parser.add_argument(
        "--show-success",
        action="store_true",
        help="顯示每一個成功複製的檔案；檔案很多時不建議開啟",
    )

    args = parser.parse_args()

    years = parse_years(args.years)

    copy_existing_csv_files(
        src_root=Path(args.src_root),
        dst_root=Path(args.dst_root),
        years=years,
        dry_run=args.dry_run,
        skip_existing=args.skip_existing,
        show_success=args.show_success,
    )


if __name__ == "__main__":
    main()
