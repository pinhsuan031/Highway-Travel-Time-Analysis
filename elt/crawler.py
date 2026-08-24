import re
import sys
import time
import tarfile
import shutil
from pathlib import Path

import urllib3
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from requests.exceptions import HTTPError
from requests.exceptions import HTTPError, ConnectionError, Timeout, RequestException
from http.client import RemoteDisconnected


BASE_URL = "https://tisvcloud.freeway.gov.tw/history/TDCS/M04A/"

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "raw" / "M04A"

urllib3.disable_warnings(
    urllib3.exceptions.InsecureRequestWarning
)

session = requests.Session()
session.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 "
            "(Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 "
            "(KHTML, like Gecko) "
            "Chrome/136.0 Safari/537.36"
        )
    }
)

def request_page(url: str, retries: int = 3, delay: int = 10):

    last_error = None

    for attempt in range(1, retries + 1):

        try:

            print(f"[GET] {url} (try {attempt}/{retries})")

            r = session.get(
                url,
                timeout=120,
                verify=False,
            )

            r.raise_for_status()

            return r

        except (
            ConnectionError,
            Timeout,
            RemoteDisconnected,
            RequestException,
        ) as e:

            last_error = e

            print(f"[RETRY {attempt}/{retries}] {e}")

            if attempt < retries:
                print(f"[WAIT] {delay} seconds...")
                time.sleep(delay)

    raise last_error

def find_hour_root(root: Path):

    for d in root.rglob("*"):

        if not d.is_dir():
            continue

        hour_dirs = [
            p.name
            for p in d.iterdir()
            if p.is_dir()
        ]

        if "00" in hour_dirs and "23" in hour_dirs:
            return d

    return None

def get_entries(url: str):

    r = request_page(url)
    soup = BeautifulSoup(r.text, "html.parser")
    entries = []

    rows = soup.select("#indexlist tr.even, #indexlist tr.odd")
    
    for row in rows:
        a = row.select_one(
            "td.indexcolname a"
        )
        if not a:
            continue

        href = a.get("href")

        if href:
            entries.append(href)

    return entries


def download_file(url, target):

    if target.exists() and target.stat().st_size > 0:
        print(f"[SKIP] {target}")
        return False

    target.parent.mkdir(parents=True, exist_ok=True)

    print(f"[DOWNLOAD] {target}")

    r = request_page(url)

    with open(target, "wb") as f:
        f.write(r.content)

    return True

def organize_csv(day_dir: Path):

    for csv in day_dir.glob("*.csv"):

        m = re.search(r"_(\d{2})\d{2}\.csv$", csv.name)

        if not m:
            continue

        hour = m.group(1)

        hour_dir = day_dir / hour
        hour_dir.mkdir(exist_ok=True)

        
        dst = hour_dir / csv.name

        if not dst.exists():
            shutil.move(csv, dst)
        else:
            csv.unlink()

def extract_tar1(tar_path: Path, target_dir: Path):

    print(f"[EXTRACT] {tar_path}")

    with tarfile.open(tar_path, "r:gz") as tar:
        tar.extractall(target_dir)

    # 如果解壓後多了一層同名資料夾，就把內容搬出來
    nested_dir = target_dir / target_dir.name

    if nested_dir.exists() and nested_dir.is_dir():
        print(f"[MOVE] {nested_dir} -> {target_dir}")

        for item in nested_dir.iterdir():
            shutil.move(str(item), target_dir)

        nested_dir.rmdir()

    print("[DONE] Extracted")

    tar_path.unlink()
    print("[DELETE] tar.gz removed")

def extract_tar(tar_path: Path, target_dir: Path):

    print(f"[EXTRACT] {tar_path}")

    with tarfile.open(tar_path, "r:gz") as tar:
        tar.extractall(target_dir)

    hour_root = find_hour_root(target_dir)

    if hour_root and hour_root != target_dir:

        print(f"[MOVE] {hour_root} -> {target_dir}")

        for item in hour_root.iterdir():
            shutil.move(str(item), target_dir)

        shutil.rmtree(hour_root)

    print("[DONE] Extracted")

    tar_path.unlink()

    print("[DELETE] tar.gz removed")

def process_tar(href):

    m = re.search(r"M04A_(\d{8})\.tar\.gz", href)
    if not m:
        return

    date_str = m.group(1)

    day_dir = RAW_DIR / date_str

    day_dir.mkdir(parents=True, exist_ok=True)

    tar_path = day_dir / href

    if download_file(BASE_URL + href, tar_path):
        extract_tar(tar_path, day_dir)
        organize_csv(day_dir)
        time.sleep(40)


def process_day_folder(date_str: str):

    target_dir = RAW_DIR / date_str

    if target_dir.exists():

        csv_count = len(list(target_dir.rglob("*.csv")))

        if csv_count >= 288:

            print(f"[SKIP DAY] {date_str} {csv_count} files" )

            return

    target_dir.mkdir(
        parents=True,
        exist_ok=True
    )

    for hour in range(24):

        hour_url = (
            f"{BASE_URL}"
            f"{date_str}/"
            f"{hour:02d}/"
        )

        try:

            entries = get_entries(
                hour_url
            )

        except Exception as e:

            print(
                f"[ERROR] "
                f"{hour_url} "
                f"{e}"
            )

            continue

        for href in entries:

            if not href.endswith(".csv"):
                continue

            csv_url = ( hour_url + href )

            hour_dir = target_dir / f"{hour:02d}"
            hour_dir.mkdir(parents=True, exist_ok=True)

            target_file = hour_dir / href

            try:

                if download_file(csv_url,target_file):
                    time.sleep(7)

            except Exception as e:

                print(
                    f"[ERROR] "
                    f"{csv_url} "
                    f"{e}"
                )


def extract_date(href: str):

    m = re.search(
        r"(\d{8})",
        href
    )

    if not m:
        return None

    return m.group(1)

def process_date(date_str: str):

    tar_name = f"M04A_{date_str}.tar.gz"

    try:

        process_tar(tar_name)

    except HTTPError:

        print(f"[INFO] {date_str} tar.gz not found, use hour folder.")

        process_day_folder(date_str)

    except Exception as e:

        print(f"[ERROR] {date_str}: {e}")

def update_latest():

    today = datetime.today() - timedelta(days=1)

    consecutive_complete = 0

    for i in range(3650):

        d = today - timedelta(days=i)

        date_str = d.strftime("%Y%m%d")

        day_dir = RAW_DIR / date_str

        csv_count = len(list(day_dir.rglob("*.csv")))

        if csv_count >= 288:

            print(f"[SKIP] {date_str}")

            consecutive_complete += 1

            # if consecutive_complete >= 7:
            #     print("最近 5 天都完整，停止搜尋")
            #     break

            continue

        consecutive_complete = 0

        print(f"[UPDATE] {date_str}")

        process_date(date_str)

def main(start_date: str):

    start_date = (
        start_date
        .replace("-", "")
        .strip()
    )

    entries = get_entries(
        BASE_URL
    )
    started = False
    for href in entries:

        date_str = extract_date(href)

        if not date_str:
            continue

        if date_str == start_date:
            started = True

        if not started:
            continue

        #if date_str < "20180214":
        if date_str < "20260501":
            break

        if href.endswith(".tar.gz"):

            process_tar(href)

        elif href.endswith("/"):

            process_day_folder(date_str)


if __name__ == "__main__":

    if len(sys.argv) == 1:

        update_latest()

    elif len(sys.argv) == 2:

        main(sys.argv[1])

    else:

        print(
            "Usage:\n"
            "python tdcs_crawler.py\n"
            "python tdcs_crawler.py 2026-06-07"
        )