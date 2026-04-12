from datetime import datetime, timezone, timedelta
import os
import re
import time
from urllib.parse import urlencode
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

JST = timezone(timedelta(hours=9))

BASE_URL = "https://demedas.kyotei.club"
RENDER_IMPORT_URL = os.environ.get(
    "RENDER_IMPORT_URL",
    "https://race-candidates-app.onrender.com/api/import_candidates",
).strip()
IMPORT_TOKEN = os.environ.get(
    "IMPORT_TOKEN",
    "abc123456789-super-secret",
).strip()

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

BET_TYPE = "3連単"
BET_AMOUNT = 100

JCD_NAME_MAP = {
    "02": "戸田",
    "03": "江戸川",
    "04": "平和島",
    "05": "多摩川",
    "06": "浜名湖",
    "07": "蒲郡",
    "08": "常滑",
    "09": "津",
    "10": "三国",
    "11": "びわこ",
    "12": "住之江",
    "13": "尼崎",
    "14": "鳴門",
    "15": "丸亀",
    "16": "児島",
    "17": "宮島",
    "20": "若松",
    "21": "芦屋",
    "22": "福岡",
    "23": "唐津",
    "24": "大村",
}

NAME_JCD_MAP = {v: k for k, v in JCD_NAME_MAP.items()}

RATING_PAGE_MAP = {
    "★★★★★": "s5",
}

# --- 通信まわり設定 ---
DEFAULT_TIMEOUT = (8, 20)
OFFICIAL_TIMEOUT = (8, 25)
BEFOREINFO_TIMEOUT = (8, 20)
POST_TIMEOUT = (10, 40)

HTTP_RETRY_COUNT = 3
HTTP_RETRY_SLEEP = 1.2

OFFICIAL_MAX_WORKERS = 4
BEFOREINFO_MAX_WORKERS = 10

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


def log(msg):
    print(msg, flush=True)


def jst_now():
    return datetime.now(JST)


def today_str():
    return jst_now().strftime("%Y%m%d")


def today_text():
    return jst_now().strftime("%Y-%m-%d")


def current_hhmm():
    return jst_now().strftime("%H:%M")


def to_minutes(hhmm):
    h, m = map(int, hhmm.split(":"))
    return h * 60 + m


def is_future_or_now(hhmm):
    try:
        return to_minutes(hhmm) >= to_minutes(current_hhmm())
    except Exception:
        return False


def sleep_retry(attempt):
    time.sleep(HTTP_RETRY_SLEEP * attempt)


def fetch_html(url, timeout=DEFAULT_TIMEOUT, retries=HTTP_RETRY_COUNT):
    last_err = None

    for attempt in range(1, retries + 1):
        try:
            res = SESSION.get(url, timeout=timeout)
            res.raise_for_status()
            res.encoding = res.apparent_encoding
            return res.text
        except Exception as e:
            last_err = e
            log(f"[http_retry] attempt={attempt}/{retries} url={url} err={e}")
            if attempt < retries:
                sleep_retry(attempt)

    raise last_err


def fetch_soup(url):
    html = fetch_html(url)
    return BeautifulSoup(html, "html.parser"), html


def normalize_lines(html):
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n")
    lines = [line.strip() for line in text.splitlines()]
    return [line for line in lines if line]


def build_official_url(jcd, race_no=1):
    return f"https://boatrace.jp/owpc/pc/race/pcexpect?rno={race_no}&jcd={jcd}&hd={today_str()}"


def build_beforeinfo_url(jcd, race_no):
    qs = urlencode({"hd": today_str(), "jcd": jcd, "rno": race_no})
    return f"https://boatrace.jp/owpc/pc/race/beforeinfo?{qs}"


def parse_official_deadlines_from_lines(lines):
    deadlines = {}

    for i, line in enumerate(lines):
        if "締切予定時刻" in line:
            block = " ".join(lines[i:i + 80])
            times = re.findall(r"\d{2}:\d{2}", block)
            if times:
                for idx, t in enumerate(times[:12], start=1):
                    deadlines[idx] = t
                return deadlines

    return deadlines


def parse_official_deadlines_for_jcd(jcd):
    official_url = build_official_url(jcd, race_no=1)
    venue_name = JCD_NAME_MAP.get(jcd, jcd)

    try:
        html = fetch_html(official_url, timeout=OFFICIAL_TIMEOUT, retries=HTTP_RETRY_COUNT)
        lines = normalize_lines(html)
        deadlines = parse_official_deadlines_from_lines(lines)

        if deadlines:
            log(f"[official_deadlines_ok] jcd={jcd} venue={venue_name} count={len(deadlines)}")
            return jcd, deadlines

        log(f"[official_deadlines_empty] jcd={jcd} venue={venue_name}")
        return jcd, {}

    except Exception as e:
        log(f"[official_deadlines_error] jcd={jcd} venue={venue_name} err={e}")
        return jcd, {}


def clean_num(text):
    if text is None:
        return None
    s = str(text).strip().replace("%", "")
    m = re.search(r"\d+(?:\.\d+)?", s)
    if not m:
        return None
    try:
        return float(m.group(0))
    except Exception:
        return None


def parse_beforeinfo_for_key(jcd, race_no):
    beforeinfo_url = build_beforeinfo_url(jcd, race_no)
    venue_name = JCD_NAME_MAP.get(jcd, jcd)

    try:
        html = fetch_html(beforeinfo_url, timeout=BEFOREINFO_TIMEOUT, retries=HTTP_RETRY_COUNT)
    except Exception as e:
        log(f"[beforeinfo_error] jcd={jcd} venue={venue_name} race_no={race_no} err={e}")
        return (jcd, race_no), {
            "exhibition": {"times": [], "ranks": {}},
            "boat_stats": {},
        }

    lines = normalize_lines(html)

    # --- 展示タイム取得 ---
    time_candidates = []
    for line in lines:
        if re.fullmatch(r"\d\.\d{2}", line):
            time_candidates.append(line)

    if len(time_candidates) < 6:
        for line in lines:
            found = re.findall(r"\d\.\d{2}", line)
            for m in found:
                time_candidates.append(m)

    times = time_candidates[:6]
    ranks = {}

    if len(times) == 6:
        float_pairs = []
        for lane, t in enumerate(times, start=1):
            try:
                float_pairs.append((lane, float(t)))
            except Exception:
                float_pairs = []
                break

        if float_pairs:
            sorted_pairs = sorted(float_pairs, key=lambda x: x[1])
            current_rank = 1
            prev_time = None

            for idx, (lane, t) in enumerate(sorted_pairs, start=1):
                if prev_time is None or t != prev_time:
                    current_rank = idx
                ranks[lane] = current_rank
                prev_time = t
        else:
            times = []

    if len(times) != 6:
        times = []
        ranks = {}

    # --- 勝率・モーター・ボート取得 ---
    stats = {}
    for lane in range(1, 7):
        stats[lane] = {
            "class": "",
            "national_win": None,
            "local_win": None,
            "motor2": None,
            "boat2": None,
        }

    lane_positions = []
    for idx, line in enumerate(lines):
        if re.fullmatch(r"[1-6]", line):
            lane_positions.append((idx, int(line)))

    for pos_idx, lane in lane_positions:
        segment = lines[pos_idx:pos_idx + 40]
        joined = " ".join(segment)

        m_class = re.search(r"\b(A1|A2|B1|B2)\b", joined)
        if m_class:
            stats[lane]["class"] = m_class.group(1)

        nums = [clean_num(x) for x in segment]
        nums = [x for x in nums if x is not None]

        win_like = [
