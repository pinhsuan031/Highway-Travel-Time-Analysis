import argparse, os, sys, time, statistics, json
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

# ============ 路徑設定 (相對於本 .py 檔所在位置，移動環境不會壞) ============
SCRIPT_DIR = Path(__file__).resolve().parent.parent
print(SCRIPT_DIR)
ELT_DIR = SCRIPT_DIR /  "elt"                # gantry.csv / network_structure.csv
DATASET_DIR = SCRIPT_DIR /  "dataset" / "csv_1"  # partition_m04a.py 的輸出

# ============ 資料範圍與參數 ============
DATE_START = datetime(2021, 6, 22)
DATE_END = datetime(2026, 5, 4)
VEHICLE_TYPE = 31          # 小客車
CHUNK_SIZE = 2_000_000     # 每月大檔分塊讀取的列數

# 取得起點到終點會經過的所有門架
def get_route_sequence(origin, destination):
    gantry_file =  SCRIPT_DIR / "metadata" / "gantry.csv"
    network_file = SCRIPT_DIR / "metadata" / "network_structure.csv"
    print(gantry_file, network_file)
    # 檢查檔案是否存在
    if not gantry_file.exists() or not network_file.exists():
        print(f"錯誤：找不到必要的 CSV 檔案。請確定 {ELT_DIR} 目錄下有 gantry.csv 與 network_structure.csv")
        sys.exit(1)

    # 讀取 gantry.csv 將使用者輸入的起點與終點轉換為門架代號
    gantry_df = pd.read_csv(gantry_file, header=None, names=['name', 'gantry_id'], dtype={'gantry_id': str})

    orig_row = gantry_df[gantry_df['name'] == origin]
    dest_row = gantry_df[gantry_df['name'] == destination]

    if orig_row.empty or dest_row.empty:
        print("錯誤：無法在 gantry.csv 中找到對應的起點或終點，請檢查輸入名稱。")
        sys.exit(1)

    # 取得基礎門架代號
    orig_base_id = orig_row['gantry_id'].values[0]
    dest_base_id = dest_row['gantry_id'].values[0]

    # 藉由門架代號後四碼判斷南北向
    orig_mileage = int(orig_base_id[3:])
    dest_mileage = int(dest_base_id[3:])

    # 由小到大為南下，由大到小為北上
    if orig_mileage < dest_mileage:
        direction = 'S'
    elif orig_mileage > dest_mileage:
        direction = 'N'
    else:
        print("錯誤：起點與終點相同。")
        sys.exit(1)

    # 組合出帶有方向尾綴的門架代號
    actual_orig_id = f"{orig_base_id}{direction}"
    actual_dest_id = f"{dest_base_id}{direction}"

    print(f"[*] 起點門架: {actual_orig_id}")
    print(f"[*] 終點門架: {actual_dest_id}")

    # 利用 network_structure.csv 找出起點到終點會經過的所有門架
    network_df = pd.read_csv(network_file)
    orig_seq_data = network_df[network_df['gantry_id'] == actual_orig_id]
    dest_seq_data = network_df[network_df['gantry_id'] == actual_dest_id]

    if orig_seq_data.empty or dest_seq_data.empty:
        print("錯誤：無法在 network_structure.csv 中找到對應的門架結構。")
        sys.exit(1)

    orig_seq_id = orig_seq_data['id'].values[0]
    dest_seq_id = dest_seq_data['id'].values[0]

    route_df = network_df[(network_df['id'] >= orig_seq_id) & (network_df['id'] <= dest_seq_id)]
    route_list = route_df['gantry_id'].tolist()

    # 回傳所有門架代號
    return route_list

# 取得符合使用者輸入星期的日期
def get_matching_dates(weekday_str):
    weekday_map = {'Monday': 0, 'Tuesday': 1, 'Wednesday': 2, 'Thursday': 3, 'Friday': 4, 'Saturday': 5, 'Sunday': 6}
    target_weekday = weekday_map.get(weekday_str)

    matching_dates = []
    current_date = DATE_START

    while current_date <= DATE_END:
        if current_date.weekday() == target_weekday:
            date_str = f"{current_date.year}/{current_date.month}/{current_date.day}"
            matching_dates.append(date_str)
        current_date += timedelta(days=1)

    # 回傳所有日期
    return matching_dates


# ============ 月份快取 ============
# csv_1 是「每月一個大檔」，因此改成按 (年,月) 快取：
# 每月的大檔只讀一次，讀取時就過濾出「本次路徑會用到的門架配對」+「小客車(31)」，
# 建成 dict[(GantryFrom, GantryTo, "YYYY/MM/DD HH:MM")] = TravelTime，查詢 O(1)。
# 只保留最近 2 個月在記憶體 (跨午夜的行程可能會查到下個月)。
MONTH_CACHE = {}     # (year, month) -> lookup dict
CACHE_ORDER = []     # LRU
MAX_CACHED_MONTHS = 2
ROUTE_PAIRS = set()  # 本次查詢路徑上的 (GantryFrom, GantryTo)，載入前設定

def load_month(year, month):
    key = (year, month)
    if key in MONTH_CACHE:
        return MONTH_CACHE[key]

    file_path = DATASET_DIR / f"year={year}" / f"month={month:02d}" / "data.csv"
    lookup = {}

    if not file_path.exists():
        print(f"找不到檔案 {file_path}")
    else:
        t0 = time.time()
        froms = {p[0] for p in ROUTE_PAIRS}
        # 分塊讀取，避免 19M 列整檔進記憶體；只留必要欄位
        reader = pd.read_csv(
            file_path, header=None,
            names=['ts', 'gfrom', 'gto', 'type', 'duration', 'volume'],
            usecols=['ts', 'gfrom', 'gto', 'type', 'duration'],
            dtype={'ts': str, 'gfrom': str, 'gto': str, 'type': 'int16', 'duration': 'int32'},
            chunksize=CHUNK_SIZE,
        )
        for chunk in reader:
            # 先用車種與起點門架快速縮小範圍，再精確比對配對
            c = chunk[(chunk['type'] == VEHICLE_TYPE) & (chunk['gfrom'].isin(froms))]
            if c.empty:
                continue
            for ts, gf, gt, dur in zip(c['ts'], c['gfrom'], c['gto'], c['duration']):
                # duration <= 0 代表該時段無車通過，視為無資料，讓後續往下一個5分鐘重試
                if (gf, gt) in ROUTE_PAIRS and dur > 0:
                    lookup[(gf, gt, ts)] = dur
        print(f"[*] 已載入 {year}-{month:02d} ({len(lookup):,} 筆路徑相關資料, {time.time()-t0:.1f} 秒)")

    # LRU: 最多保留 MAX_CACHED_MONTHS 個月
    MONTH_CACHE[key] = lookup
    CACHE_ORDER.append(key)
    while len(CACHE_ORDER) > MAX_CACHED_MONTHS:
        old = CACHE_ORDER.pop(0)
        MONTH_CACHE.pop(old, None)

    return lookup

def get_travel_duration(start_g, end_g, search_dt):
    lookup = load_month(search_dt.year, search_dt.month)
    # csv_1 的 TimeStamp 是零補齊格式，例如 "2021/06/25 03:10"
    ts = search_dt.strftime("%Y/%m/%d %H:%M")
    return lookup.get((start_g, end_g, ts))

# 計算每個日期的總旅行時間
def calculate_travel_times(matching_dates, route_gantries, depart_time_str):
    valid_results = []

    print("[*] 開始載入歷史資料並計算旅行時間")

    # 依 (年,月) 將日期分組排序，讓同一個月的大檔只需載入一次
    def ym_of(date_str):
        y, m, _ = map(int, date_str.split('/'))
        return (y, m)
    sorted_dates = sorted(matching_dates, key=lambda s: tuple(map(int, s.split('/'))))

    for date_str in sorted_dates:
        year, month, day = map(int, date_str.split('/'))
        depart_hour, depart_minute = map(int, depart_time_str.split(':'))
        # 將分鐘數無條件捨去至最接近的 5 分鐘倍數
        depart_minute = (depart_minute // 5) * 5
        journey_start_dt = datetime(year, month, day, depart_hour, depart_minute)

        total_seconds = 0
        skip_date = False
        segments_data = {}  # 用來儲存這一天內，各個路段的秒數

        for i in range(len(route_gantries) - 1):
            start_g = route_gantries[i]
            end_g = route_gantries[i + 1]
            segment_name = f"{start_g}_{end_g}"

            current_interval_offset = (total_seconds // 300) * 300
            found_data = False

            for attempt in range(13):
                search_dt = journey_start_dt + timedelta(seconds=current_interval_offset) + timedelta(minutes=5 * attempt)

                duration = get_travel_duration(start_g, end_g, search_dt)

                if duration is not None:
                    # 紀錄單一路段花費的秒數
                    segments_data[segment_name] = int(duration)
                    total_seconds += duration
                    found_data = True
                    break

            if not found_data:
                skip_date = True
                break

        if not skip_date:
            valid_results.append((date_str, total_seconds, segments_data))

    print(f"[*] 計算完畢，共取得 {len(valid_results)} 天的完整資料。")
    return valid_results

def main():
    # 紀錄程式開始時間
    start_time = time.time()

    parser = argparse.ArgumentParser(description="國道旅行時間計算程式")
    parser.add_argument("origin", help="起點 (例如: 台北)")
    parser.add_argument("destination", help="終點 (例如: 台中)")
    parser.add_argument("weekday", help="星期 (例如: 星期一)")
    parser.add_argument("time", help="出發時間 (例如: 14:00)")

    args = parser.parse_args()

    print(f"====================================")
    print(f" 查詢條件:{args.origin} -> {args.destination} | {args.weekday} | 出發時間 {args.time}")
    print(f"====================================")

    # 1. 獲取門架路徑
    route_gantries = get_route_sequence(args.origin, args.destination)

    # 設定本次查詢會用到的門架配對，供讀檔時過濾
    global ROUTE_PAIRS
    ROUTE_PAIRS = set(zip(route_gantries, route_gantries[1:]))

    # 2. 獲取符合條件的日期清單
    if "/" in args.weekday:
        matched_dates = [d.strip() for d in args.weekday.split(",")]
    else:
        matched_dates = get_matching_dates(args.weekday)

    # 3 & 4. 讀取並計算所有路段總旅行時間
    travel_time_results = calculate_travel_times(matched_dates, route_gantries, args.time)

    # 5. 計算中位數並輸出最終結果
    if travel_time_results:
        # 取出所有成功的總秒數
        all_seconds = [result[1] for result in travel_time_results]

        # 計算中位數並換算成分鐘 (四捨五入到小數點後 1 位)
        median_seconds = statistics.median(all_seconds)
        median_minutes = round(median_seconds / 60, 1)

        print(f"\n================ 最終結果 ================")
        print(f"預估總旅行時間 (中位數):{median_minutes} 分鐘")

        # 從所有結果中，找出總秒數最接近中位數的那一天記錄，能應對天數為偶數時，中位數代表兩天平均值的狀況
        median_record = min(travel_time_results, key=lambda x: abs(x[1] - median_seconds))
        median_date = median_record[0]
        median_segments = median_record[2]

        # 僅將該中位數日期的路段資料整理成 JSON
        json_data = {
            median_date: median_segments
        }

        print("\n================ 中位數日期詳細路段數據 (JSON) ================")
        print(json.dumps(json_data, indent=4, ensure_ascii=False))
        print(f"==========================================================================")

    else:
        print("\n[!] 警告：未能成功計算出任何一天的旅行時間，請檢查 CSV 資料是否齊全。")

    # 紀錄程式結束時間並結算
    end_time = time.time()
    execution_time = round(end_time - start_time, 2)
    print(f"\n程式執行完畢，總耗時：{execution_time} 秒")

if __name__ == "__main__":
    main()