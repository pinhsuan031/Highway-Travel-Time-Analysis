from pathlib import Path
import argparse
import time
import csv as std_csv

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq


# 你的整理後 CSV 是 5 欄：
# Date, Time, TravelTime, TravelDistance, TrafficVolume
M04A_COLUMNS = [
    "Date",
    "Time",
    "TravelTime",
    "TravelDistance",
    "TrafficVolume",
]

M04A_TYPES = {
    "Date": pa.string(),
    "Time": pa.string(),
    "TravelTime": pa.int32(),
    "TravelDistance": pa.int32(),
    "TrafficVolume": pa.int32(),
}


def format_time(seconds):
    minutes = seconds / 60
    if minutes < 1:
        return f"{seconds:.2f} 秒"
    return f"{minutes:.2f} 分鐘"


def read_first_row(csv_path):
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = std_csv.reader(f)
        for row in reader:
            if row:
                return row
    return []


def has_header(row):
    if not row:
        return False

    first_cell = row[0].strip().lower()

    return first_cell in {
        "date",
        "time",
        "travel_time",
        "traveltime",
        "traffic_volume",
        "trafficvolume",
    }


def get_read_options(csv_path):
    first_row = read_first_row(csv_path)

    if not first_row:
        raise ValueError("空檔案，沒有任何資料列")

    if len(first_row) != 5:
        raise ValueError(
            f"欄位數不是5欄，目前偵測到 {len(first_row)} 欄：{first_row}"
        )

    skip_rows = 1 if has_header(first_row) else 0

    read_options = pacsv.ReadOptions(
        column_names=M04A_COLUMNS,
        skip_rows=skip_rows,
        use_threads=True,
    )

    convert_options = pacsv.ConvertOptions(
        column_types=M04A_TYPES
    )

    return read_options, convert_options


def main():
    parser = argparse.ArgumentParser(
        description="把 ~/dataset/csv/M04A/*/*.csv 轉成 ~/dataset/parquet/*/*.parquet"
    )

    parser.add_argument(
        "--input-root",
        default="~/dataset/csv/M04A",
        help="CSV 根目錄，預設 ~/dataset/csv/M04A",
    )

    parser.add_argument(
        "--output-root",
        default="~/dataset/parquet",
        help="Parquet 輸出根目錄，預設 ~/dataset/parquet",
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="如果 parquet 已存在，是否重新轉檔",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="只顯示會轉哪些檔案，不真的寫出 parquet",
    )

    args = parser.parse_args()

    input_root = Path(args.input_root).expanduser().resolve()
    output_root = Path(args.output_root).expanduser().resolve()

    if not input_root.exists():
        print(f"找不到輸入資料夾：{input_root}")
        return

    output_root.mkdir(parents=True, exist_ok=True)

    gantry_folders = sorted(
        p for p in input_root.iterdir()
        if p.is_dir()
    )

    total_folders = len(gantry_folders)

    total_csv = 0
    total_converted = 0
    total_skipped = 0
    total_failed = 0
    failed_files = []

    start_time = time.perf_counter()

    print("========== M04A CSV to Parquet ==========", flush=True)
    print(f"輸入資料夾: {input_root}", flush=True)
    print(f"輸出資料夾: {output_root}", flush=True)
    print(f"輸入格式: {input_root}/*/*.csv", flush=True)
    print(f"輸出格式: {output_root}/*/*.parquet", flush=True)
    print("CSV欄位格式: 5欄 Date, Time, TravelTime, TravelDistance, TrafficVolume", flush=True)
    print(f"門架資料夾數量: {total_folders}", flush=True)
    print(f"Dry run: {args.dry_run}", flush=True)
    print("=========================================", flush=True)

    for folder_index, folder_path in enumerate(gantry_folders, start=1):
        folder_start_time = time.perf_counter()

        csv_files = sorted(folder_path.glob("*.csv"))
        folder_csv_count = len(csv_files)
        folder_converted = 0
        folder_skipped = 0
        folder_failed = 0

        folder_name = folder_path.name
        output_folder = output_root / folder_name

        if not args.dry_run:
            output_folder.mkdir(parents=True, exist_ok=True)

        for csv_path in csv_files:
            parquet_path = output_folder / csv_path.with_suffix(".parquet").name

            total_csv += 1

            if parquet_path.exists() and not args.overwrite:
                folder_skipped += 1
                total_skipped += 1
                continue

            if args.dry_run:
                folder_converted += 1
                total_converted += 1
                continue

            try:
                read_options, convert_options = get_read_options(csv_path)

                table = pacsv.read_csv(
                    str(csv_path),
                    read_options=read_options,
                    convert_options=convert_options,
                )

                pq.write_table(
                    table,
                    str(parquet_path),
                    compression="snappy",
                )

                folder_converted += 1
                total_converted += 1

            except Exception as e:
                folder_failed += 1
                total_failed += 1
                failed_files.append((str(csv_path), str(e)))

        folder_elapsed = time.perf_counter() - folder_start_time

        print(
            f"[{folder_index}/{total_folders}] {folder_name} 完成 | "
            f"CSV: {folder_csv_count} | "
            f"轉檔: {folder_converted} | "
            f"略過: {folder_skipped} | "
            f"失敗: {folder_failed} | "
            f"本資料夾: {format_time(folder_elapsed)}",
            flush=True,
        )

    total_elapsed = time.perf_counter() - start_time

    print("========== 全部完成 ==========", flush=True)
    print(f"門架資料夾總數: {total_folders}", flush=True)
    print(f"CSV 總數: {total_csv}", flush=True)
    print(f"成功轉檔: {total_converted}", flush=True)
    print(f"已存在略過: {total_skipped}", flush=True)
    print(f"失敗數量: {total_failed}", flush=True)
    print(f"總花費時間: {format_time(total_elapsed)}", flush=True)

    if failed_files:
        print("========== 失敗檔案 ==========", flush=True)
        for path, error in failed_files:
            print(path, flush=True)
            print(f"  錯誤原因: {error}", flush=True)

    print("================================", flush=True)


if __name__ == "__main__":
    main()
