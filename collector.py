from datetime import datetime, timezone, timedelta
import os
import re
import time
from urllib.parse import urlencode
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

# collector_latest_v10_27_more_hit_lines_keep_display_after_settle.py
# v10.26: 保険3点を「公式寄せ」ではなく「別頭・相手抜けカバー」へ調整。

JST = timezone(timedelta(hours=9))

RENDER_IMPORT_URL = os.environ.get(
    "RENDER_IMPORT_URL",
    "https://race-candidates-app.onrender.com/api/import_latest_candidates",
).strip()
IMPORT_TOKEN = os.environ.get(
    "IMPORT_TOKEN",
    "race-token-2026",
).strip()

BASE_MAP_URL = os.environ.get(
    "BASE_MAP_URL",
    "https://race-candidates-app.onrender.com/api/base_map_today",
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

REQUEST_TIMEOUT = (8, 16)
POST_TIMEOUT = 35
MAX_RETRIES = 2
RETRY_SLEEP_SEC = 0.8

OFFICIAL_MAX_WORKERS = 3
BEFOREINFO_MAX_WORKERS = 4
RESULT_MAX_WORKERS = 4
RESULT_PAGE_MAX_WORKERS = 4

ONLY_UPCOMING_HOURS = int(os.environ.get("ONLY_UPCOMING_HOURS", "2"))
SKIP_PAST_RACES = os.environ.get("SKIP_PAST_RACES", "1").strip() == "1"

JCD_NAME_MAP = {
    "01": "桐生", "02": "戸田", "03": "江戸川", "04": "平和島", "05": "多摩川",
    "06": "浜名湖", "07": "蒲郡", "08": "常滑", "09": "津", "10": "三国",
    "11": "びわこ", "12": "住之江", "13": "尼崎", "14": "鳴門", "15": "丸亀",
    "16": "児島", "17": "宮島", "20": "若松", "21": "芦屋", "22": "福岡",
    "23": "唐津", "24": "大村",
}
NAME_JCD_MAP = {v: k for k, v in JCD_NAME_MAP.items()}
RATING_PAGE_MAP = {
    "★★★★★": "s5",
    "★★★★☆": "s4",
    "★★★☆☆": "s3",
    "★★☆☆☆": "s2",
    "★☆☆☆☆": "s1",
}
ALL_OFFICIAL_RATINGS = list(RATING_PAGE_MAP.keys())


def log(msg):
    print(msg, flush=True)


def jst_now():
    return datetime.now(JST)


def jst_now_str():
    return jst_now().strftime("%Y-%m-%d %H:%M:%S JST")


def today_str():
    return jst_now().strftime("%Y%m%d")


def today_text():
    return jst_now().strftime("%Y-%m-%d")


def current_hhmm():
    return jst_now().strftime("%H:%M")


def to_minutes(hhmm):
    h, m = map(int, hhmm.split(":"))
    return h * 60 + m


def is_target_deadline(hhmm):
    if not hhmm:
        return False
    try:
        now_min = to_minutes(current_hhmm())
        target_min = to_minutes(hhmm)
        diff = target_min - now_min
        if SKIP_PAST_RACES and diff < 0:
            return False
        return diff <= ONLY_UPCOMING_HOURS * 60
    except Exception:
        return False


def clamp(v, low, high):
    return max(low, min(high, v))


def is_past_race(time_str):
    if not time_str:
        return False
    try:
        return to_minutes(time_str) < to_minutes(current_hhmm())
    except Exception:
        return False


# 結果・払戻の自動反映用。
# 1Rなど古いレースの「結果は入ったが払戻だけ未反映」も拾えるように長め。
# 実際の取得件数は RESULT_PENDING_LIMIT で絞るので重くなりすぎない。
RESULT_LOOKBACK_MINUTES = int(os.environ.get("RESULT_LOOKBACK_MINUTES", "480"))
RESULT_PENDING_LIMIT = max(16, int(os.environ.get("RESULT_PENDING_LIMIT", "16")))

# DBに誤った公式結果/払戻が入った時だけ使う修復モード。
# 通常CronではデフォルトOFF。誤データ修復だけ PC実行や一時Cron で RESULT_REPAIR_MODE=1 にする。
RESULT_REPAIR_MODE = os.environ.get("RESULT_REPAIR_MODE", "0").strip() == "1"
RESULT_REPAIR_LOOKBACK_MINUTES = int(os.environ.get("RESULT_REPAIR_LOOKBACK_MINUTES", "720"))
RESULT_REPAIR_LIMIT = int(os.environ.get("RESULT_REPAIR_LIMIT", "48"))


def is_recent_past_race(hhmm, lookback_minutes=RESULT_LOOKBACK_MINUTES):
    if not hhmm:
        return False
    try:
        now_min = to_minutes(current_hhmm())
        target_min = to_minutes(hhmm)
        diff = now_min - target_min
        return 0 <= diff <= lookback_minutes
    except Exception:
        return False


def is_settle_pending(base_info):
    if not base_info:
        return False

    result_text = str(base_info.get("result_trifecta_text") or "").strip()
    result_payout = int(base_info.get("result_trifecta_payout") or 0)

    # 3連単の払戻がまだ無いものは、結果文言が入っていても再取得対象に残す。
    # これで「公式結果は入ったが公式払戻だけ未反映」の取りこぼしを次回回収できる。
    if result_payout <= 0:
        return True

    # 結果文言も払戻も入っていれば取得済みとみなす。
    if result_text:
        return False

    return False


def fetch_html(url, timeout=REQUEST_TIMEOUT, max_retries=MAX_RETRIES):
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            res = requests.get(url, headers=HEADERS, timeout=timeout)
            res.raise_for_status()
            res.encoding = res.apparent_encoding
            return res.text
        except Exception as e:
            last_err = e
            if attempt < max_retries:
                log(f"[fetch_retry] attempt={attempt}/{max_retries} url={url} err={e}")
                time.sleep(RETRY_SLEEP_SEC * attempt)
            else:
                log(f"[fetch_failed] url={url} err={e}")
    raise last_err


def fetch_soup(url):
    html = fetch_html(url)
    return BeautifulSoup(html, "html.parser"), html


def normalize_lines(html):
    soup = BeautifulSoup(html, "html.parser")
    return normalize_lines_from_soup(soup)


def normalize_lines_from_soup(soup):
    text = soup.get_text("\n")
    lines = [line.strip() for line in text.splitlines()]
    return [line for line in lines if line]


def build_official_url(jcd, race_no=1):
    return f"https://boatrace.jp/owpc/pc/race/pcexpect?rno={race_no}&jcd={jcd}&hd={today_str()}"


def build_beforeinfo_url(jcd, race_no):
    qs = urlencode({"hd": today_str(), "jcd": jcd, "rno": race_no})
    return f"https://boatrace.jp/owpc/pc/race/beforeinfo?{qs}"


def build_result_url(jcd, race_no):
    return f"https://boatrace.jp/owpc/pc/race/raceresult?rno={race_no}&jcd={jcd}&hd={today_str()}"


def build_resultlist_url(jcd):
    return f"https://boatrace.jp/owpc/pc/race/resultlist?hd={today_str()}&jcd={jcd}"


def row_cells(tr):
    return tr.find_all(["td", "th"], recursive=False)


def parse_race_identity_from_text(text):
    m = re.search(r"(\d{2})\s*([^\s]+)\s*(\d{1,2})R", text)
    if not m:
        return None
    return {"jcd": m.group(1), "venue": m.group(2).strip(), "race_no": int(m.group(3))}


def extract_digits_from_cell(cell):
    digits = []
    for el in cell.find_all(True):
        txt = el.get_text(strip=True)
        if re.fullmatch(r"[1-6]", txt):
            digits.append(txt)
    if len(digits) < 18:
        full_text = cell.get_text(" ", strip=True)
        digits.extend(re.findall(r"\b([1-6])\b", full_text))
    return [d for d in digits if d in {"1", "2", "3", "4", "5", "6"}]


def normalize_triplet(a, b, c):
    if a == b or a == c or b == c:
        return ""
    return f"{a}-{b}-{c}"


def triplets_from_digit_sequence(digits):
    triplets = []
    for i in range(0, len(digits) - 2, 3):
        t = normalize_triplet(digits[i], digits[i + 1], digits[i + 2])
        if t and t not in triplets:
            triplets.append(t)
        if len(triplets) >= 6:
            break
    return triplets


def parse_rating_page_dom(rating_text):
    page = RATING_PAGE_MAP[rating_text]
    url = f"https://demedas.kyotei.club/{page}/{today_str()}.html"
    try:
        soup, _html = fetch_soup(url)
    except Exception as e:
        log(f"[rating_page_dom_error] rating={rating_text} url={url} err={e}")
        return []
    rows = []
    header_idx = {}
    header_found = False
    for tr in soup.find_all("tr"):
        cells = row_cells(tr)
        if not cells:
            continue
        cell_texts = [c.get_text(" ", strip=True) for c in cells]
        joined = " | ".join(cell_texts)
        if (not header_found) and ("会場" in joined and "3連単" in joined and "2連単" in joined):
            for idx, txt in enumerate(cell_texts):
                if "会場" in txt:
                    header_idx["race"] = idx
                elif txt == "3連単" or "3連単" in txt:
                    header_idx["trifecta"] = idx
            if "race" in header_idx and "trifecta" in header_idx:
                header_found = True
            continue
        if not header_found:
            continue
        if len(cells) <= max(header_idx["race"], header_idx["trifecta"]):
            continue
        race_text = cells[header_idx["race"]].get_text(" ", strip=True)
        info = parse_race_identity_from_text(race_text) or parse_race_identity_from_text(tr.get_text(" ", strip=True))
        if not info:
            continue
        triplets = triplets_from_digit_sequence(extract_digits_from_cell(cells[header_idx["trifecta"]]))
        rows.append({
            "venue": info["venue"],
            "jcd": info["jcd"],
            "race_no": info["race_no"],
            "rating": rating_text,
            "selection": " / ".join(triplets[:6]),
        })
    dedup = {}
    for r in rows:
        key = (r["venue"], r["race_no"])
        if key not in dedup:
            dedup[key] = r
    return list(dedup.values())


def parse_rating_page(rating_texts=None):
    rating_texts = list(rating_texts or ALL_OFFICIAL_RATINGS)
    rows = []
    for rating_text in rating_texts:
        rows.extend(parse_rating_page_dom(rating_text))
    dedup = {}
    for r in rows:
        key = (r["venue"], r["race_no"])
        if key not in dedup:
            dedup[key] = r
    merged = list(dedup.values())
    log(f"[rating_page_summary] ratings={','.join(rating_texts)} count={len(merged)}")
    return merged


def parse_official_deadlines_from_html(html):
    lines = normalize_lines(html)
    deadlines = {}
    for i, line in enumerate(lines):
        if "締切予定時刻" in line:
            block = " ".join(lines[i:i + 80])
            times = re.findall(r"\d{2}:\d{2}", block)
            if times:
                for idx, t in enumerate(times[:12], start=1):
                    deadlines[idx] = t
                return deadlines
    all_times = re.findall(r"\b\d{2}:\d{2}\b", " ".join(lines))
    if len(all_times) >= 12:
        for idx, t in enumerate(all_times[:12], start=1):
            deadlines[idx] = t
    return deadlines


def parse_official_deadlines_for_jcd(jcd):
    try:
        html = fetch_html(build_official_url(jcd, race_no=1))
    except Exception as e:
        log(f"[official_deadlines_error] jcd={jcd} err={e}")
        return jcd, {}
    return jcd, parse_official_deadlines_from_html(html)


def parse_single_race_deadline(jcd, race_no):
    url = build_official_url(jcd, race_no=race_no)
    try:
        html = fetch_html(url)
    except Exception:
        return ""
    lines = normalize_lines(html)
    for i, line in enumerate(lines):
        if "締切予定時刻" in line:
            block = " ".join(lines[i:i + 30])
            times = re.findall(r"\d{2}:\d{2}", block)
            if times:
                return times[0]
    all_times = re.findall(r"\b\d{2}:\d{2}\b", " ".join(lines))
    return all_times[0] if all_times else ""


def fill_missing_deadlines(rows, deadlines_cache):
    for row in rows:
        jcd = row["jcd"] or NAME_JCD_MAP.get(row["venue"], "")
        if not jcd:
            continue
        if deadlines_cache.get(jcd, {}).get(row["race_no"], ""):
            continue
        single_deadline = parse_single_race_deadline(jcd, row["race_no"])
        if single_deadline:
            deadlines_cache.setdefault(jcd, {})[row["race_no"]] = single_deadline
    return deadlines_cache


def fetch_deadlines_parallel(jcds):
    results = {}
    with ThreadPoolExecutor(max_workers=OFFICIAL_MAX_WORKERS) as ex:
        futures = [ex.submit(parse_official_deadlines_for_jcd, jcd) for jcd in sorted(jcds)]
        for future in as_completed(futures):
            jcd, deadlines = future.result()
            results[jcd] = deadlines
    return results


def normalize_race_no_value(race_no):
    try:
        return int(str(race_no).replace("R", "").replace("r", "").strip())
    except Exception:
        m = re.search(r"(\d{1,2})", str(race_no or ""))
        return int(m.group(1)) if m else 0


def is_exhibition_time_value(val):
    return isinstance(val, (int, float)) and 6.2 <= float(val) <= 8.5


def extract_exhibition_times_from_table(soup):
    lane_times = {}
    for tr in soup.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        if not cells:
            continue
        texts = [c.get_text(" ", strip=True) for c in cells]
        if not texts:
            continue
        lane = None
        first = texts[0].strip()
        if re.fullmatch(r"[1-6]", first):
            lane = int(first)
        else:
            full = " ".join(texts[:2])
            m_lane = re.search(r"\b([1-6])\b", full)
            if m_lane:
                lane = int(m_lane.group(1))
        if lane is None:
            continue

        vals = []
        for txt in texts:
            for x in re.findall(r"\b\d\.\d{2}\b", txt):
                try:
                    vals.append(float(x))
                except Exception:
                    pass
        vals = [v for v in vals if is_exhibition_time_value(v)]
        if vals:
            lane_times[lane] = min(vals)

    if len(lane_times) == 6:
        return [f"{lane_times[lane]:.2f}" for lane in range(1, 7)]
    return []


def extract_exhibition_times_from_lines(lines):
    lane_times = {}
    lane_positions = [(idx, int(line)) for idx, line in enumerate(lines) if re.fullmatch(r"[1-6]", line)]
    for idx, lane in lane_positions:
        seg = lines[idx: idx + 20]
        vals = []
        for txt in seg:
            for x in re.findall(r"\b\d\.\d{2}\b", txt):
                try:
                    vals.append(float(x))
                except Exception:
                    pass
        vals = [v for v in vals if is_exhibition_time_value(v)]
        if vals:
            lane_times[lane] = min(vals)

    if len(lane_times) == 6:
        return [f"{lane_times[lane]:.2f}" for lane in range(1, 7)]
    return []


def build_exhibition_ranks_from_times(times):
    ranks = {}
    if len(times) != 6:
        return ranks
    float_pairs = []
    for lane, t in enumerate(times, start=1):
        try:
            v = float(t)
        except Exception:
            return {}
        if not is_exhibition_time_value(v):
            return {}
        float_pairs.append((lane, v))
    sorted_pairs = sorted(float_pairs, key=lambda x: x[1])
    current_rank = 1
    prev_time = None
    for idx, (lane, t) in enumerate(sorted_pairs, start=1):
        if prev_time is None or t != prev_time:
            current_rank = idx
        ranks[lane] = current_rank
        prev_time = t
    return ranks


def extract_wind_direction_from_text(text):
    s = str(text or "")
    if not s:
        return ""

    directions = [
        "北北西", "西北西", "西南西", "南南西",
        "南南東", "東南東", "東北東", "北北東",
        "北西", "南西", "南東", "北東",
        "北", "西", "南", "東",
        "無風",
    ]

    compact = re.sub(r"\s+", "", s)

    label_patterns = [
        r"風向[:：]?([北西南東無風]{1,3})",
        r"風向き[:：]?([北西南東無風]{1,3})",
        r"([北西南東無風]{1,3})の風",
        r"([北西南東無風]{1,3})\s*([0-9]+(?:\.[0-9]+)?)\s*m",
    ]

    for pat in label_patterns:
        m = re.search(pat, compact)
        if m:
            cand = m.group(1)
            if cand in directions:
                return cand

    for direction in directions:
        if direction in compact:
            return direction
        if direction in s:
            return direction
    return ""



def parse_weather_info_from_lines(lines):
    joined = " ".join(lines)
    compact = re.sub(r"\s+", "", joined)

    weather = {
        "weather": "",
        "wind_speed": None,
        "wave_height": None,
        "wind_type": "",
        "wind_dir": "",
        "water_state_score": 0.0,
    }

    for word in ["晴", "曇", "雨", "雪"]:
        if word in compact:
            weather["weather"] = word
            break

    weather["wind_dir"] = extract_wind_direction_from_text(compact or joined)

    # 風速はラベル付きだけ拾う。競走水面の 1800m などを誤取得しないようにする。
    wind_patterns = [
        r"風速[:：]?([0-9]+(?:\.[0-9]+)?)",
        r"風速([0-9]+(?:\.[0-9]+)?)m",
        r"風速([0-9]+(?:\.[0-9]+)?)m/s",
        r"風速([0-9]+(?:\.[0-9]+)?)メートル",
    ]
    for pat in wind_patterns:
        m_wind = re.search(pat, compact, re.I)
        if m_wind:
            try:
                v = float(m_wind.group(1))
                if 0 <= v <= 20:
                    weather["wind_speed"] = v
                    break
            except Exception:
                pass

    # 波高もラベル付きだけ拾う。
    wave_patterns = [
        r"波高[:：]?([0-9]+(?:\.[0-9]+)?)",
        r"波高([0-9]+(?:\.[0-9]+)?)cm",
        r"波高([0-9]+(?:\.[0-9]+)?)センチ",
    ]
    for pat in wave_patterns:
        m_wave = re.search(pat, compact, re.I)
        if m_wave:
            try:
                v = float(m_wave.group(1))
                if 0 <= v <= 50:
                    weather["wave_height"] = v
                    break
            except Exception:
                pass

    if any(x in compact for x in ["向い風", "向かい風", "向風", "ホーム向い", "ホーム向かい"]):
        weather["wind_type"] = "向い風"
    elif any(x in compact for x in ["追い風", "追風", "ホーム追い"]):
        weather["wind_type"] = "追い風"
    elif any(x in compact for x in ["横風", "左横風", "右横風", "左横", "右横"]):
        weather["wind_type"] = "横風"

    wind = weather["wind_speed"]
    wave = weather["wave_height"]
    score = 0.0

    if isinstance(wind, (int, float)):
        if wind >= 7:
            score -= 0.18
        elif wind >= 5:
            score -= 0.10
        elif wind <= 1:
            score += 0.05
        elif wind <= 2:
            score += 0.04

    if isinstance(wave, (int, float)):
        if wave >= 7:
            score -= 0.18
        elif wave >= 5:
            score -= 0.10
        elif wave <= 1:
            score += 0.05
        elif wave <= 2:
            score += 0.04

    if weather["wind_type"] == "向い風":
        score -= 0.04
    elif weather["wind_type"] == "追い風":
        score += 0.03
    elif weather["wind_type"] == "横風":
        score -= 0.03

    if weather["wind_dir"] == "無風":
        score += 0.03

    weather["water_state_score"] = round(score, 2)
    return weather



def get_wind_level(weather_info):
    """
    風補正の強さを 0.0〜1.0 で返す。
    風速が取れない時は 0。
    """
    try:
        wind = float((weather_info or {}).get("wind_speed") or 0)
    except Exception:
        wind = 0.0

    if wind >= 8:
        return 1.0
    if wind >= 6:
        return 0.8
    if wind >= 5:
        return 0.65
    if wind >= 4:
        return 0.45
    if wind >= 3:
        return 0.25
    return 0.0

def parse_st_value(text):
    if text is None:
        return None
    s = str(text).strip().upper()
    if not s:
        return None

    s = s.replace("ＳＴ", "").replace("ST", "").strip().replace(" ", "")

    m = re.search(r"[FL]?\s*(\d?\.\d{2})", s)
    if m:
        try:
            v = float(m.group(1))
            if 0.0 <= v <= 1.0:
                return v
        except Exception:
            pass

    m = re.search(r"[FL]?\.(\d{2})", s)
    if m:
        try:
            v = float(f"0.{m.group(1)}")
            if 0.0 <= v <= 1.0:
                return v
        except Exception:
            pass
    return None


def parse_start_display_from_lines(lines):
    empty = {
        "course_order": [],
        "course_map": {},
        "st_map": {},
        "entry_change": False,
        "entry_text": "",
        "pre_move_lanes": [],
        "pulled_back_lanes": [],
        "entry_severity": 0.0,
        "entry_reason_text": "",
    }

    start_idx = None
    for idx, line in enumerate(lines):
        if "スタート展示" in line:
            start_idx = idx
            break
    if start_idx is None:
        return empty

    end_idx = len(lines)
    for idx in range(start_idx + 1, len(lines)):
        if any(marker in lines[idx] for marker in ["水面気象情報", "スタンド", "スマートフォン版へ", "PAGE TOP"]):
            end_idx = idx
            break

    segment_lines = lines[start_idx:end_idx]
    segment = " ".join(segment_lines)

    pairs = []
    pattern = re.compile(r"\b([1-6])\b\s*(?:Image)?\s*([FL]?\s*\d?\.\d{2}|[FL]?\.\d{2})")
    for m in pattern.finditer(segment):
        try:
            lane = int(m.group(1))
        except Exception:
            continue
        st = parse_st_value(m.group(2))
        if st is None:
            continue
        pairs.append((lane, st))

    if len(pairs) < 6:
        for idx, line in enumerate(segment_lines):
            s = str(line).strip()
            lane = None
            if re.fullmatch(r"[1-6]", s):
                lane = int(s)
            else:
                m = re.match(r"^([1-6])(?:\s+Image)?\s+([FL]?\s*\d?\.\d{2}|[FL]?\.\d{2})$", s)
                if m:
                    lane = int(m.group(1))
                    st = parse_st_value(m.group(2))
                    if st is not None:
                        pairs.append((lane, st))
                        continue
            if lane is None:
                continue
            for look in segment_lines[idx + 1: idx + 4]:
                st = parse_st_value(look)
                if st is not None:
                    pairs.append((lane, st))
                    break

    order = []
    st_map = {}
    seen = set()
    for lane, st in pairs:
        if lane in seen:
            continue
        seen.add(lane)
        order.append(lane)
        st_map[lane] = st
        if len(order) >= 6:
            break

    course_map = {lane: idx + 1 for idx, lane in enumerate(order)} if order else {}
    entry_change = bool(order and order != [1, 2, 3, 4, 5, 6])
    pre_move_lanes = sorted([lane for lane, course in course_map.items() if course < lane])
    pulled_back_lanes = sorted([lane for lane, course in course_map.items() if course > lane])

    entry_severity = 0.0
    if entry_change:
        entry_severity += 0.16
    if course_map.get(1, 1) > 1:
        entry_severity += 0.11 * min(3, course_map.get(1, 1) - 1)
    for lane in pre_move_lanes:
        gain = max(1, lane - course_map.get(lane, lane))
        entry_severity += 0.05 * min(3, gain)
        if lane >= 4 and course_map.get(lane, lane) <= 3:
            entry_severity += 0.06
        elif lane >= 5 and course_map.get(lane, lane) <= 4:
            entry_severity += 0.03
    if len(pre_move_lanes) >= 2:
        entry_severity += 0.05
    entry_severity = round(clamp(entry_severity, 0.0, 0.68), 2)

    entry_text = "-".join([str(x) for x in order]) if order else ""
    reason_parts = []
    if entry_change and entry_text:
        reason_parts.append(f"進入:{entry_text}")
    if course_map.get(1, 1) > 1:
        reason_parts.append("1がイン外し")
    for lane in pre_move_lanes[:2]:
        if lane != 1:
            reason_parts.append(f"{lane}前づけ")

    return {
        "course_order": order,
        "course_map": course_map,
        "st_map": st_map,
        "entry_change": entry_change,
        "entry_text": entry_text,
        "pre_move_lanes": pre_move_lanes,
        "pulled_back_lanes": pulled_back_lanes,
        "entry_severity": entry_severity,
        "entry_reason_text": " / ".join(reason_parts[:3]),
    }


def parse_start_info_from_lines(lines):
    start_display = parse_start_display_from_lines(lines)
    st_map = dict(start_display.get("st_map") or {})

    if len(st_map) < 4:
        lane_positions = [(idx, int(line)) for idx, line in enumerate(lines) if re.fullmatch(r"[1-6]", line)]
        for idx, lane in lane_positions:
            seg = lines[idx: idx + 12]
            for txt in seg:
                v = parse_st_value(txt)
                if v is not None:
                    st_map[lane] = v
                    break

        if len(st_map) < 6:
            joined = " ".join(lines)
            pattern = re.compile(r"([1-6])\s*([FL]?\s*\d?\.\d{2}|[FL]?\.\d{2})")
            for m in pattern.finditer(joined):
                try:
                    lane = int(m.group(1))
                except Exception:
                    continue
                v = parse_st_value(m.group(2))
                if v is not None and lane not in st_map:
                    st_map[lane] = v

    return {
        "st_map": st_map,
        "course_order": start_display.get("course_order", []),
        "course_map": start_display.get("course_map", {}),
        "entry_change": bool(start_display.get("entry_change")),
        "entry_text": str(start_display.get("entry_text") or ""),
        "pre_move_lanes": list(start_display.get("pre_move_lanes", [])),
        "pulled_back_lanes": list(start_display.get("pulled_back_lanes", [])),
        "entry_severity": float(start_display.get("entry_severity", 0) or 0),
        "entry_reason_text": str(start_display.get("entry_reason_text") or ""),
    }


def build_foot_material(exhibition_info, start_info, weather_info=None):
    weather_info = weather_info or {}
    times = exhibition_info.get("times", []) if exhibition_info else []
    ranks = exhibition_info.get("ranks", {}) if exhibition_info else {}
    st_map = start_info.get("st_map", {}) if start_info else {}
    course_order = list(start_info.get("course_order", []) or []) if start_info else []
    course_map = dict(start_info.get("course_map", {}) or {}) if start_info else {}
    entry_change = bool(start_info.get("entry_change")) if start_info else False
    pre_move_lanes = list(start_info.get("pre_move_lanes", []) or []) if start_info else []
    pulled_back_lanes = list(start_info.get("pulled_back_lanes", []) or []) if start_info else []
    entry_severity = float(start_info.get("entry_severity", 0) or 0) if start_info else 0.0
    entry_reason_text = str(start_info.get("entry_reason_text") or "").strip() if start_info else ""

    lane_scores = {lane: 0.0 for lane in range(1, 7)}
    reasons = []
    top_lane_reasons = []

    float_times = []
    for lane, t in enumerate(times, start=1):
        try:
            v = float(t)
            if is_exhibition_time_value(v):
                float_times.append((lane, v))
        except Exception:
            pass

    if len(float_times) == 6:
        sorted_times = sorted(float_times, key=lambda x: x[1])
        fastest_lane, fastest_time = sorted_times[0]
        second_lane, second_time = sorted_times[1]
        slowest_lane, slowest_time = sorted_times[-1]
        spread = slowest_time - fastest_time
        gap12 = second_time - fastest_time

        if spread >= 0.18:
            reasons.append("足:足差あり")
            lane_scores[fastest_lane] += 0.34
            lane_scores[second_lane] += 0.12
            lane_scores[slowest_lane] -= 0.20
        elif spread >= 0.12:
            reasons.append("足:足差ややあり")
            lane_scores[fastest_lane] += 0.24
            lane_scores[second_lane] += 0.08
            lane_scores[slowest_lane] -= 0.14
        elif spread >= 0.08:
            lane_scores[fastest_lane] += 0.14
            lane_scores[slowest_lane] -= 0.08

        if gap12 >= 0.05:
            lane_scores[fastest_lane] += 0.08

        if ranks.get(fastest_lane) == 1:
            lane_scores[fastest_lane] += 0.10

    if len(st_map) >= 4:
        sorted_st = sorted([(lane, v) for lane, v in st_map.items() if isinstance(v, (int, float))], key=lambda x: x[1])
        if len(sorted_st) >= 2:
            best_lane, best_st = sorted_st[0]
            second_lane, second_st = sorted_st[1]
            worst_lane, worst_st = sorted_st[-1]
            st_spread = worst_st - best_st

            if best_st <= 0.10:
                lane_scores[best_lane] += 0.18
                top_lane_reasons.append(f"足:{best_lane}号艇足色良さげ")
            elif best_st <= 0.12:
                lane_scores[best_lane] += 0.12

            if st_spread >= 0.10:
                reasons.append("足:ST気配あり")
                lane_scores[best_lane] += 0.10
                lane_scores[second_lane] += 0.05
                lane_scores[worst_lane] -= 0.12
            elif st_spread >= 0.06:
                lane_scores[best_lane] += 0.06
                lane_scores[worst_lane] -= 0.06

    if ranks:
        for lane in range(1, 7):
            rank = ranks.get(lane)
            if rank == 1:
                lane_scores[lane] += 0.08
            elif rank == 6:
                lane_scores[lane] -= 0.06

    if entry_change and course_map:
        reasons.append("足:進入変化")
        lane1_course = course_map.get(1, 1)
        if lane1_course == 2:
            lane_scores[1] -= 0.18
        elif lane1_course >= 3:
            lane_scores[1] -= 0.28

        course1_lane = course_order[0] if course_order else None
        if course1_lane and course1_lane != 1:
            lane_scores[course1_lane] += 0.12

        for lane in pre_move_lanes:
            course = course_map.get(lane, lane)
            gain = max(1, lane - course)
            bonus = min(0.08 + 0.05 * gain, 0.22)
            lane_scores[lane] += bonus
            if lane >= 4 and course <= 3:
                lane_scores[lane] += 0.05
            if lane != 1 and f"足:{lane}前づけ注意" not in top_lane_reasons:
                top_lane_reasons.append(f"足:{lane}前づけ注意")

        for lane in pulled_back_lanes:
            course = course_map.get(lane, lane)
            loss = max(1, course - lane)
            lane_scores[lane] -= min(0.06 + 0.03 * loss, 0.16)

    water_state_score = float(weather_info.get("water_state_score") or 0)
    if water_state_score < 0:
        if 1 in lane_scores:
            lane_scores[1] += abs(water_state_score) * 0.25
        for lane in [5, 6]:
            lane_scores[lane] -= abs(water_state_score) * 0.18

    sorted_lane_scores = sorted(lane_scores.items(), key=lambda x: x[1], reverse=True)
    best_lane, best_score = sorted_lane_scores[0]
    second_score = sorted_lane_scores[1][1] if len(sorted_lane_scores) >= 2 else 0.0

    if best_score >= 0.22 and (best_score - second_score) >= 0.08:
        if f"足:{best_lane}号艇足色良さげ" not in top_lane_reasons:
            top_lane_reasons.append(f"足:{best_lane}号艇足色良さげ")

    positive_scores = [v for v in lane_scores.values() if v > 0]
    negative_scores = [v for v in lane_scores.values() if v < 0]

    foot_bonus = 0.0
    if positive_scores:
        foot_bonus += min(max(positive_scores), 0.40) * 0.65
    if negative_scores:
        foot_bonus += max(min(negative_scores), -0.20) * 0.25

    if len(float_times) == 6:
        spread = max(v for _, v in float_times) - min(v for _, v in float_times)
        if spread >= 0.12:
            foot_bonus += 0.10
        elif spread >= 0.08:
            foot_bonus += 0.05

    if entry_change:
        foot_bonus += min(0.10, entry_severity * 0.18)

    foot_bonus = round(foot_bonus, 2)
    merged_reasons = reasons[:]
    if entry_reason_text:
        merged_reasons.append(entry_reason_text)
    merged_reasons.extend(top_lane_reasons)
    unique_reasons = list(dict.fromkeys([x for x in merged_reasons if x]))
    reason_text = " / ".join(unique_reasons[:4])

    return {
        "lane_scores": lane_scores,
        "foot_bonus": foot_bonus,
        "reason_text": reason_text,
        "st_map": st_map,
        "course_order": course_order,
        "course_map": course_map,
        "entry_change": entry_change,
        "entry_text": str(start_info.get("entry_text") or "") if start_info else "",
        "pre_move_lanes": pre_move_lanes,
        "pulled_back_lanes": pulled_back_lanes,
        "entry_severity": entry_severity,
        "entry_reason_text": entry_reason_text,
    }


def parse_beforeinfo_for_key(jcd, race_no):
    race_no = normalize_race_no_value(race_no)
    beforeinfo_url = build_beforeinfo_url(jcd, race_no)
    empty_info = {
        "exhibition": {"times": [], "ranks": {}},
        "weather": {
            "weather": "",
            "wind_speed": None,
            "wave_height": None,
            "wind_type": "",
            "wind_dir": "",
            "water_state_score": 0.0,
        },
        "start_info": {
            "st_map": {},
            "course_order": [],
            "course_map": {},
            "entry_change": False,
            "entry_text": "",
            "pre_move_lanes": [],
            "pulled_back_lanes": [],
            "entry_severity": 0.0,
            "entry_reason_text": "",
        },
    }
    try:
        html = fetch_html(beforeinfo_url)
    except Exception as e:
        log(f"[beforeinfo_error] jcd={jcd} race_no={race_no} err={e}")
        return (jcd, race_no), empty_info

    soup = BeautifulSoup(html, "html.parser")
    lines = normalize_lines_from_soup(soup)

    exhibition_times = extract_exhibition_times_from_table(soup)
    if len(exhibition_times) != 6:
        exhibition_times = extract_exhibition_times_from_lines(lines)
    exhibition_ranks = build_exhibition_ranks_from_times(exhibition_times) if exhibition_times else {}

    weather_info = parse_weather_info_from_lines(lines)
    start_info = parse_start_info_from_lines(lines)

    return (jcd, race_no), {
        "exhibition": {
            "times": exhibition_times,
            "ranks": exhibition_ranks,
        },
        "weather": weather_info,
        "start_info": start_info,
    }


def parse_result_triplet_from_text(text):
    s = str(text or "")
    compact = re.sub(r"\s+", "", s)
    patterns = [
        r"3連単[^0-9]*([1-6])[-=]([1-6])[-=]([1-6])",
        r"組番[^0-9]*([1-6])[-=]([1-6])[-=]([1-6])",
        r"\b([1-6])-([1-6])-([1-6])\b",
    ]
    for pat in patterns:
        m = re.search(pat, compact)
        if m:
            a, b, c = m.group(1), m.group(2), m.group(3)
            if len({a, b, c}) == 3:
                return f"{a}-{b}-{c}"
    return ""


def parse_kimarite_from_text(text):
    s = str(text or "")
    words = ["まくり差し", "まくり", "差し", "抜き", "恵まれ", "逃げ"]
    for w in words:
        if w in s:
            return w
    return ""


def parse_payout_from_text(text):
    """
    3連単の払戻を安全寄りに拾う。
    空白除去で「¥660 1人気」→「6601」のように連結する誤取得を避ける。
    """
    s = str(text or "")
    if not s:
        return 0

    normalized = re.sub(r"[ \t\r\f\v]+", " ", s)
    compact = re.sub(r"\s+", "", s)

    def to_int(num_text):
        try:
            return int(str(num_text).replace(",", "").replace(" ", ""))
        except Exception:
            return 0

    def valid_money(val):
        return 100 <= int(val or 0) <= 5000000

    def split_money_and_popularity(digits):
        digits = re.sub(r"\D", "", str(digits or ""))
        if len(digits) < 4:
            return 0
        candidates = []
        for pop_len in (1, 2, 3):
            if len(digits) <= pop_len:
                continue
            money = int(digits[:-pop_len])
            pop = int(digits[-pop_len:])
            if 1 <= pop <= 120 and valid_money(money):
                candidates.append((pop_len, money))
        if not candidates:
            return 0
        candidates.sort(key=lambda x: x[0])
        return candidates[0][1]

    # compactで「¥6601人気」になったケースだけ救済
    for m in re.finditer(r"[¥￥]([0-9]{4,9})人気", compact):
        val = split_money_and_popularity(m.group(1))
        if valid_money(val):
            return val

    # 通貨記号/円つきは空白を残した文字列で見る
    for pat in [
        r"[¥￥]\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]{3,7})(?![0-9])",
        r"([0-9]{1,3}(?:,[0-9]{3})+|[0-9]{3,7})\s*円(?![0-9])",
    ]:
        for m in re.finditer(pat, normalized):
            val = to_int(m.group(1))
            if valid_money(val):
                return val

    # 3連単の行・セルだけを対象にする
    line_candidates = []
    for raw_line in s.splitlines():
        line = re.sub(r"[ \t\r\f\v]+", " ", raw_line).strip()
        if not line:
            continue
        has_context = (
            "3連単" in line
            or "三連単" in line
            or "組番" in line
            or bool(parse_result_triplet_from_text(line))
        )
        if not has_context:
            continue

        for pat in [
            r"[¥￥]\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]{3,7})(?![0-9])",
            r"([0-9]{1,3}(?:,[0-9]{3})+|[0-9]{3,7})\s*円(?![0-9])",
        ]:
            for m in re.finditer(pat, line):
                val = to_int(m.group(1))
                if valid_money(val):
                    line_candidates.append(val)

        tri_match = re.search(r"[1-6]\s*[-=]\s*[1-6]\s*[-=]\s*[1-6](.*)$", line)
        if tri_match:
            tail = tri_match.group(1)
            for m in re.finditer(r"(?:[¥￥]\s*)?([0-9]{1,3}(?:,[0-9]{3})+|[0-9]{3,7})(?:\s*円)?(?!\s*人気)(?![0-9])", tail):
                val = to_int(m.group(1))
                if valid_money(val):
                    line_candidates.append(val)

    if line_candidates:
        return line_candidates[0]

    # 表セル結合の「3連単 1-2-3 660 1人気」型
    if ("3連単" in normalized or "三連単" in normalized or "組番" in normalized or parse_result_triplet_from_text(normalized)):
        m = re.search(
            r"[1-6]\s*[-=]\s*[1-6]\s*[-=]\s*[1-6]\s+(?:[¥￥]\s*)?([0-9]{1,3}(?:,[0-9]{3})+|[0-9]{3,7})(?:\s*円)?(?:\s+[0-9]{1,3}\s*人気)?",
            normalized,
        )
        if m:
            val = to_int(m.group(1))
            if valid_money(val):
                return val

    # 裸の4桁以上は誤取得しやすいので拾わない
    return 0


def parse_result_triplet_from_cells(cell_texts):
    """
    公式結果ページの table cell から3連単を拾う補助。
    例: ["3連単", "1", "4", "2", "780", "2人気"] のように
    号艇が別セルになっている場合に対応する。
    """
    texts = [str(x or "").strip() for x in (cell_texts or [])]
    joined = " ".join(texts)

    tri = parse_result_triplet_from_text(joined)
    if tri:
        return tri

    start_idx = 0
    for i, txt in enumerate(texts):
        if "3連単" in txt or "三連単" in txt:
            start_idx = i + 1
            break

    digits = []
    for txt in texts[start_idx:]:
        found = re.findall(r"\b([1-6])\b", txt)
        for d in found:
            digits.append(d)
            if len(digits) >= 3:
                break
        if len(digits) >= 3:
            break

    if len(digits) >= 3:
        return normalize_triplet(digits[0], digits[1], digits[2])
    return ""


def parse_payout_from_cells(cell_texts):
    """
    公式結果ページの table cell から3連単払戻を拾う補助。
    780円のような3桁払戻や、円表記なしの別セルに対応する。
    人気・艇番・R番号は除外し、3連単行だけを対象にする。
    """
    texts = [str(x or "").strip() for x in (cell_texts or [])]
    if not texts:
        return 0

    joined = " ".join(texts)
    if ("3連単" not in joined) and ("三連単" not in joined) and not parse_result_triplet_from_cells(texts):
        return 0

    candidates = []
    for txt in texts:
        s = str(txt or "").strip()
        if not s:
            continue
        if "人気" in s:
            continue
        s = s.replace("円", "").replace("¥", "").replace("￥", "").replace(",", "").strip()
        if not re.fullmatch(r"\d{3,7}", s):
            continue
        try:
            val = int(s)
        except Exception:
            continue
        if 100 <= val <= 5000000:
            candidates.append(val)

    if candidates:
        return max(candidates)

    return parse_payout_from_text(joined)


def parse_resultlist_for_jcd(jcd):
    url = build_resultlist_url(jcd)
    try:
        html = fetch_html(url, timeout=(6, 12), max_retries=2)
    except Exception as e:
        log(f"[resultlist_error] jcd={jcd} err={e}")
        return jcd, {}

    soup = BeautifulSoup(html, "html.parser")
    lines = normalize_lines_from_soup(soup)
    compact_html = re.sub(r"\s+", "", soup.get_text(" "))

    results = {}

    # 1) まず table/tr 単位で拾う
    current_race_no = None
    for tr in soup.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        if not cells:
            continue

        cell_texts = [c.get_text(" ", strip=True) for c in cells]
        row_text = " ".join(cell_texts)
        compact_row = re.sub(r"\s+", "", row_text)

        m_race = re.search(r"(\d{1,2})R", compact_row)
        if m_race:
            current_race_no = int(m_race.group(1))

        tri = parse_result_triplet_from_cells(cell_texts) or parse_result_triplet_from_text(compact_row)
        if tri and current_race_no:
            try:
                a, b, c = [int(x) for x in tri.split("-")]
            except Exception:
                a = b = c = None
            if a is not None and current_race_no not in results:
                cell_pay = parse_payout_from_cells(cell_texts)
                results[current_race_no] = {
                    "triplet": tri,
                    "head": a,
                    "second": b,
                    "third": c,
                    "kimarite": parse_kimarite_from_text(compact_row),
                    "trifecta_payout": cell_pay or parse_payout_from_text(row_text),
                }
            elif current_race_no in results:
                kim = parse_kimarite_from_text(compact_row)
                if kim and not results[current_race_no].get("kimarite"):
                    results[current_race_no]["kimarite"] = kim

                pay = parse_payout_from_cells(cell_texts) or parse_payout_from_text(row_text)
                if pay and pay > int(results[current_race_no].get("trifecta_payout") or 0):
                    results[current_race_no]["trifecta_payout"] = pay

    # 2) lines ベースで不足分を補完
    current_race_no = None
    for line in lines:
        compact_line = re.sub(r"\s+", "", str(line or ""))
        m_race = re.search(r"(\d{1,2})R", compact_line)
        if m_race:
            current_race_no = int(m_race.group(1))

        tri = parse_result_triplet_from_text(compact_line)
        if tri and current_race_no and current_race_no not in results:
            try:
                a, b, c = [int(x) for x in tri.split("-")]
            except Exception:
                continue
            results[current_race_no] = {
                "triplet": tri,
                "head": a,
                "second": b,
                "third": c,
                "kimarite": parse_kimarite_from_text(compact_line),
                "trifecta_payout": parse_payout_from_text(line),
            }

        kim = parse_kimarite_from_text(compact_line)
        if kim and current_race_no in results and not results[current_race_no].get("kimarite"):
            results[current_race_no]["kimarite"] = kim

        pay = parse_payout_from_text(line)
        if pay and current_race_no in results and pay > int(results[current_race_no].get("trifecta_payout") or 0):
            results[current_race_no]["trifecta_payout"] = pay

    # 3) それでも不足なら HTML 全体からレースごとにざっくり拾う
    for race_no in range(1, 13):
        m = re.search(
            rf"{race_no}R(.*?)(?:{race_no + 1}R|締切予定時刻|進入コース別結果|艇番別結果|$)",
            compact_html,
        )
        if not m:
            continue
        chunk = m.group(1)
        tri = parse_result_triplet_from_text(chunk)
        pay = 0
        kim = parse_kimarite_from_text(chunk)

        if race_no in results:
            if pay and pay > int(results[race_no].get("trifecta_payout") or 0):
                results[race_no]["trifecta_payout"] = pay
            if kim and not results[race_no].get("kimarite"):
                results[race_no]["kimarite"] = kim
            continue

        if not tri:
            continue
        try:
            a, b, c = [int(x) for x in tri.split("-")]
        except Exception:
            continue
        results[race_no] = {
            "triplet": tri,
            "head": a,
            "second": b,
            "third": c,
            "kimarite": kim,
            "trifecta_payout": pay,
        }

    log(f"[resultlist_ok] jcd={jcd} count={len(results)}")
    return jcd, results


def fetch_day_results_parallel(venue_targets):
    results = {}
    jcds = sorted(set(venue_targets.keys()))
    if not jcds:
        return results

    with ThreadPoolExecutor(max_workers=RESULT_MAX_WORKERS) as ex:
        futures = [ex.submit(parse_resultlist_for_jcd, jcd) for jcd in jcds]
        for future in as_completed(futures):
            jcd, venue_results = future.result()
            for race_no, info in (venue_results or {}).items():
                results[(jcd, race_no)] = info
    return results


def parse_raceresult_for_key(jcd, race_no):
    url = build_result_url(jcd, race_no)
    try:
        html = fetch_html(url, timeout=(6, 12), max_retries=2)
    except Exception as e:
        log(f"[raceresult_error] jcd={jcd} race_no={race_no} err={e}")
        return (jcd, race_no), {}

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    lines = normalize_lines_from_soup(soup)

    tri = parse_result_triplet_from_text(text)
    pay = parse_payout_from_text(text)
    kim = parse_kimarite_from_text(text)

    # table cell が分かれている3連単行を優先して確認。
    # 3桁払戻や円なし表記でも拾えるようにする。
    for tr in soup.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        if not cells:
            continue
        cell_texts = [c.get_text(" ", strip=True) for c in cells]
        row_text = " ".join(cell_texts)
        if "3連単" not in row_text and "三連単" not in row_text:
            continue
        cell_tri = parse_result_triplet_from_cells(cell_texts)
        cell_pay = parse_payout_from_cells(cell_texts)
        cell_kim = parse_kimarite_from_text(row_text)
        if cell_tri:
            tri = cell_tri
        if cell_pay > 0:
            pay = cell_pay
        if cell_kim and not kim:
            kim = cell_kim
        if tri and pay > 0:
            break

    if not tri:
        for line in lines:
            tri = parse_result_triplet_from_text(line)
            if tri:
                break

    if pay <= 0:
        for line in lines:
            pay = parse_payout_from_text(line)
            if pay > 0:
                break

    if not kim:
        for line in lines:
            kim = parse_kimarite_from_text(line)
            if kim:
                break

    if not tri and pay <= 0:
        log(f"[raceresult_empty] jcd={jcd} race_no={race_no}")
        return (jcd, race_no), {}

    out = {
        "triplet": tri,
        "kimarite": kim,
        "trifecta_payout": int(pay or 0),
    }
    log(f"[raceresult_ok] jcd={jcd} race_no={race_no} triplet={tri} payout={int(pay or 0)}")
    return (jcd, race_no), out


def fetch_raceresult_parallel(keys):
    results = {}
    keys = sorted(set(keys))
    if not keys:
        return results

    with ThreadPoolExecutor(max_workers=RESULT_PAGE_MAX_WORKERS) as ex:
        futures = [ex.submit(parse_raceresult_for_key, jcd, race_no) for (jcd, race_no) in keys]
        for future in as_completed(futures):
            key, info = future.result()
            if info:
                results[key] = info
    return results


def build_day_trend_bias(jcd, target_race_no, result_cache):

    def empty_bias(sample_size, phase="observe", observe_text=""):
        return {
            "head": {lane: 0.0 for lane in range(1, 7)},
            "second": {lane: 0.0 for lane in range(1, 7)},
            "third": {lane: 0.0 for lane in range(1, 7)},
            "notes": [],
            "trend_text": "",
            "sample_size": sample_size,
            "active": False,
            "phase": phase,
            "observe_text": observe_text,
        }

    target_race_no = normalize_race_no_value(target_race_no)
    prior = []
    for race_no in range(1, target_race_no):
        info = result_cache.get((jcd, race_no))
        if info and info.get("triplet"):
            prior.append(info)

    n = len(prior)
    if n < 5:
        observe_text = f"当日傾向はまだ観察のみ({n}R)" if n > 0 else ""
        return empty_bias(n, phase="observe", observe_text=observe_text)

    head_counts = {lane: 0 for lane in range(1, 7)}
    second_counts = {lane: 0 for lane in range(1, 7)}
    third_counts = {lane: 0 for lane in range(1, 7)}
    kimarite_counts = {}

    for info in prior:
        head_counts[info["head"]] += 1
        second_counts[info["second"]] += 1
        third_counts[info["third"]] += 1
        k = str(info.get("kimarite") or "")
        if k:
            kimarite_counts[k] = kimarite_counts.get(k, 0) + 1

    head1_rate = head_counts[1] / n
    head23_rate = (head_counts[2] + head_counts[3]) / n
    outer_head_rate = (head_counts[4] + head_counts[5] + head_counts[6]) / n
    second2_rate = second_counts[2] / n
    third456_rate = (third_counts[4] + third_counts[5] + third_counts[6]) / n

    kim_total = sum(kimarite_counts.values())
    nige_rate = kimarite_counts.get("逃げ", 0) / kim_total if kim_total else 0.0
    sashi_rate = (kimarite_counts.get("差し", 0) + kimarite_counts.get("まくり差し", 0)) / kim_total if kim_total else 0.0
    makuri_rate = kimarite_counts.get("まくり", 0) / kim_total if kim_total else 0.0

    strong_inside = head1_rate >= 0.64
    strong_inside_confirm = strong_inside and nige_rate >= 0.58

    inside_weak = head1_rate <= 0.30
    attack_style = head23_rate >= 0.38 or sashi_rate >= 0.36
    outer_style = outer_head_rate >= 0.26 or makuri_rate >= 0.24
    inside_weak_confirm = inside_weak and (attack_style or outer_style)

    second2_confirm = second2_rate >= 0.38
    outer3weak_confirm = n >= 6 and third456_rate <= 0.20

    confirm_flags = []
    if strong_inside_confirm:
        confirm_flags.append("inside")
    if inside_weak_confirm:
        confirm_flags.append("anti_inside")
    if attack_style and sashi_rate >= 0.34:
        confirm_flags.append("sashi")
    if outer_style and makuri_rate >= 0.22:
        confirm_flags.append("makuri")
    if outer3weak_confirm:
        confirm_flags.append("outer3weak")

    if n < 8:
        active = len(confirm_flags) >= 2
        phase = "early_guard"
        scale = clamp(0.24 + (n - 5) * 0.08, 0.24, 0.42)
    else:
        active = bool(confirm_flags)
        phase = "active"
        scale = clamp(0.42 + (n - 8) * 0.08, 0.42, 0.82)

    if not active:
        hint_parts = []
        if strong_inside:
            hint_parts.append("1頭気味")
        if inside_weak:
            hint_parts.append("イン弱め気味")
        if attack_style:
            hint_parts.append("差し寄り気味")
        if outer_style:
            hint_parts.append("外注意気味")
        observe_text = f"当日傾向は保留({n}R: {' / '.join(hint_parts[:2])})" if hint_parts else f"当日傾向は保留({n}R)"
        return empty_bias(n, phase=phase, observe_text=observe_text)

    bias = {
        "head": {lane: 0.0 for lane in range(1, 7)},
        "second": {lane: 0.0 for lane in range(1, 7)},
        "third": {lane: 0.0 for lane in range(1, 7)},
        "notes": [],
        "trend_text": "",
        "sample_size": n,
        "active": True,
        "phase": phase,
        "observe_text": "",
    }

    if strong_inside_confirm:
        bias["head"][1] += 0.10 * scale
        bias["second"][1] += 0.03 * scale
        bias["notes"].append("当日1頭寄り")
    elif inside_weak_confirm:
        bias["head"][1] -= 0.08 * scale
        bias["notes"].append("当日イン弱め")

    if attack_style:
        bias["head"][2] += 0.04 * scale
        bias["head"][3] += 0.04 * scale
        bias["second"][2] += 0.03 * scale
        bias["second"][3] += 0.02 * scale
        bias["notes"].append("当日差し/まくり寄り")

    if outer_style:
        for lane in [4, 5, 6]:
            bias["head"][lane] += 0.03 * scale
        bias["head"][1] -= 0.03 * scale
        bias["notes"].append("当日外伸び注意")

    if second2_confirm:
        bias["second"][2] += 0.04 * scale

    if outer3weak_confirm:
        bias["third"][5] -= 0.03 * scale
        bias["third"][6] -= 0.04 * scale
        bias["notes"].append("当日外3着弱め")

    if strong_inside_confirm and nige_rate >= 0.62:
        bias["head"][1] += 0.04 * scale
        bias["notes"].append("決まり手逃げ多め")
    if attack_style and sashi_rate >= 0.36:
        bias["head"][2] += 0.03 * scale
        bias["head"][3] += 0.02 * scale
        bias["second"][2] += 0.02 * scale
        bias["notes"].append("決まり手差し寄り")
    if outer_style and makuri_rate >= 0.24:
        bias["head"][3] += 0.03 * scale
        bias["head"][4] += 0.03 * scale
        bias["third"][1] -= 0.02 * scale
        bias["notes"].append("決まり手まくり寄り")

    bias["notes"] = list(dict.fromkeys(bias["notes"]))[:3]
    bias["trend_text"] = " / ".join(bias["notes"])
    return bias


def fetch_beforeinfo_parallel(keys):
    results = {}
    with ThreadPoolExecutor(max_workers=BEFOREINFO_MAX_WORKERS) as ex:
        futures = [ex.submit(parse_beforeinfo_for_key, jcd, race_no) for (jcd, race_no) in sorted(keys)]
        for future in as_completed(futures):
            key, info = future.result()
            results[key] = info
    return results


def score_to_ai_rating(score):
    try:
        s = float(score or 0)
    except Exception:
        s = 0.0

    if s >= 4.2:
        return "AI★★★★★"
    if s >= 3.0:
        return "AI★★★★☆"
    if s >= 1.9:
        return "AI★★★☆☆"
    if s >= 0.8:
        return "AI★★☆☆☆"
    return "AI★☆☆☆☆"

def score_to_final_rank(score):
    try:
        s = float(score or 0)
    except Exception:
        s = 0.0

    if s >= 3.8:
        return "買い強め"
    if s >= 2.4:
        return "買い"
    if s >= 1.0:
        return "様子見"
    return "見送り寄り"


def count_rank_signals(signal_metrics=None, foot_material=None):
    signal_metrics = signal_metrics or {}
    foot_material = foot_material or {}

    foot_reason_text = str(foot_material.get("reason_text") or "")
    course_map = foot_material.get("course_map", {}) or {}
    entry_severity = float(foot_material.get("entry_severity", 0) or 0)

    exp_spread = float(signal_metrics.get("exp_spread", 0) or 0)
    st_spread = float(signal_metrics.get("st_spread", 0) or 0)
    lane1_time_gap = float(signal_metrics.get("lane1_time_gap", 0) or 0)
    lane1_st = signal_metrics.get("lane1_st")

    has_display_gap = exp_spread >= 0.12
    has_foot_gap = ("足差あり" in foot_reason_text) or ("足差ややあり" in foot_reason_text)
    has_st_sign = ("ST気配あり" in foot_reason_text) or st_spread >= 0.06
    has_entry_change = bool(foot_material.get("entry_change")) and entry_severity >= 0.12

    lane1_weak = False
    if course_map.get(1, 1) > 1:
        lane1_weak = True
    if lane1_time_gap >= 0.07:
        lane1_weak = True
    if isinstance(lane1_st, (int, float)) and float(lane1_st) >= 0.18:
        lane1_weak = True

    direct_count = sum(1 for x in [has_display_gap, has_foot_gap, has_st_sign, has_entry_change] if x)
    return {
        "has_display_gap": has_display_gap,
        "has_foot_gap": has_foot_gap,
        "has_st_sign": has_st_sign,
        "has_entry_change": has_entry_change,
        "lane1_weak": lane1_weak,
        "direct_count": direct_count,
    }


def determine_final_rank(
    base_info,
    final_ai_score,
    signal_metrics=None,
    foot_material=None,
    role_maps=None,
    scenario_material=None,
    base_hold_strength=0.0,
    phase_material=None,
):
    base_info = base_info or {}
    signal_metrics = signal_metrics or {}
    foot_material = foot_material or {}
    role_maps = role_maps or {}
    scenario_material = scenario_material or {}
    phase_material = phase_material or {}

    try:
        score = float(final_ai_score or 0)
    except Exception:
        score = 0.0

    rating = str(base_info.get("rating") or "").strip()
    head_score = role_maps.get("head", {}) or {}
    head_ranked = list(scenario_material.get("head_ranked") or [])
    if not head_ranked and head_score:
        head_ranked = [lane for lane, _ in sorted(head_score.items(), key=lambda x: x[1], reverse=True)]
    if not head_ranked:
        return score_to_final_rank(score)

    top_head_lane = int(head_ranked[0])
    second_head_lane = int(head_ranked[1]) if len(head_ranked) >= 2 else top_head_lane
    top_head_score = float(head_score.get(top_head_lane, 0) or 0)
    second_head_score = float(head_score.get(second_head_lane, 0) or 0)
    head_gap = top_head_score - second_head_score

    signal_strength = float(signal_metrics.get("signal_strength", 0) or 0)
    morning_priority = float(base_hold_strength or 0) >= 0.18 and signal_strength < 0.35
    flags = count_rank_signals(signal_metrics=signal_metrics, foot_material=foot_material)
    direct_count = int(flags["direct_count"])

    top_head_inner = top_head_lane in {1, 2}
    top_head_safe = top_head_lane in {1, 2, 3}
    outer_head_risky = top_head_lane in {4, 5, 6} and head_gap < 0.16
    extreme_outer_head = top_head_lane in {5, 6}

    race_phase = normalize_race_phase_label(
        phase_material.get("race_phase") or base_info.get("race_phase") or ""
    )
    phase_bonus = 0.0
    if race_phase == "準優勝戦":
        phase_bonus = 0.14
    elif race_phase in {"優勝戦", "ドリーム戦"}:
        phase_bonus = 0.10

    strong_score_threshold = 3.35 - phase_bonus
    buy_score_threshold = 2.25 - phase_bonus * 0.5
    watch_score_threshold = 1.00

    # 星4はかなり厳選
    if rating == "★★★★☆":
        if (
            score >= (3.55 - phase_bonus)
            and direct_count >= 3
            and top_head_inner
            and head_gap >= 0.12
            and not morning_priority
            and not outer_head_risky
            and not extreme_outer_head
        ):
            return "買い強め"
        if (
            score >= (2.45 - phase_bonus * 0.4)
            and direct_count >= 2
            and top_head_safe
            and not morning_priority
            and not extreme_outer_head
        ):
            return "買い"
        if (
            score >= 1.35
            and direct_count >= 1
            and top_head_safe
            and not extreme_outer_head
        ):
            return "様子見"
        return "見送り寄り"

    # 星5は直前材料と頭の読みを合わせて判定
    if (
        score >= strong_score_threshold
        and direct_count >= 2
        and top_head_inner
        and head_gap >= 0.10
        and not morning_priority
        and not outer_head_risky
    ):
        return "買い強め"

    if (
        score >= buy_score_threshold
        and (direct_count >= 1 or signal_strength >= 0.42 or flags["lane1_weak"])
        and top_head_safe
        and not (morning_priority and direct_count == 0)
        and not extreme_outer_head
    ):
        return "買い"

    if (
        score >= watch_score_threshold
        and (direct_count >= 1 or top_head_safe or signal_strength >= 0.30)
    ):
        return "様子見"

    return "見送り寄り"

def exhibition_rank_text_from_map(rank_map):
    if not rank_map:
        return ""
    return " / ".join([f"{lane}:{rank_map.get(lane, '-')}" for lane in range(1, 7)])


def fetch_base_map_today():
    if not BASE_MAP_URL:
        raise RuntimeError("BASE_MAP_URL が未設定です")
    if not IMPORT_TOKEN:
        raise RuntimeError("IMPORT_TOKEN が未設定です")

    headers = {"X-IMPORT-TOKEN": IMPORT_TOKEN}
    params = {"race_date": today_text()}

    last_err = None
    for attempt in range(1, 4):
        try:
            log(f"[base_map_try] attempt={attempt} url={BASE_MAP_URL}")
            res = requests.get(BASE_MAP_URL, headers=headers, params=params, timeout=(8, 20))
            res.raise_for_status()
            data = res.json()
            if not data.get("ok"):
                raise RuntimeError(f"base_map api error: {data}")
            base_map = data.get("base_map", {}) or {}
            log(f"[base_map_ok] count={len(base_map)}")
            return base_map
        except Exception as e:
            last_err = e
            log(f"[base_map_retry] attempt={attempt} err={e}")
            if attempt < 3:
                time.sleep(1.5 * attempt)
    raise last_err


def build_venue_bias_map(venue, day_trend_bias=None):
    """
    latest 側では静的な会場傾向は持たせず、
    後半レースではその日の前半結果から作る当日傾向だけを微補正で使う。
    """
    if day_trend_bias:
        return {
            "head": dict(day_trend_bias.get("head", {}) or {lane: 0.0 for lane in range(1, 7)}),
            "second": dict(day_trend_bias.get("second", {}) or {lane: 0.0 for lane in range(1, 7)}),
            "third": dict(day_trend_bias.get("third", {}) or {lane: 0.0 for lane in range(1, 7)}),
            "notes": list(day_trend_bias.get("notes", []) or []),
        }
    return {
        "head": {lane: 0.0 for lane in range(1, 7)},
        "second": {lane: 0.0 for lane in range(1, 7)},
        "third": {lane: 0.0 for lane in range(1, 7)},
        "notes": [],
    }


def compute_lane_scores_map(exhibition_info, weather_info=None, foot_material=None):
    weather_info = weather_info or {}
    foot_material = foot_material or {}
    ranks = exhibition_info.get("ranks", {}) if exhibition_info else {}
    times = exhibition_info.get("times", []) if exhibition_info else []

    lane_scores = {lane: 0.0 for lane in range(1, 7)}

    # 展示順位は使うが、以前より弱めにする
    if ranks:
        for lane in range(1, 7):
            rank = ranks.get(lane)
            if rank is None:
                continue
            if rank == 1:
                lane_scores[lane] += 0.18
            elif rank == 2:
                lane_scores[lane] += 0.10
            elif rank == 3:
                lane_scores[lane] += 0.03
            elif rank == 4:
                lane_scores[lane] -= 0.01
            elif rank == 5:
                lane_scores[lane] -= 0.05
            elif rank == 6:
                lane_scores[lane] -= 0.10

        r1 = ranks.get(1)
        if r1 == 1:
            lane_scores[1] += 0.05
        elif r1 is not None and r1 >= 5:
            lane_scores[1] -= 0.06

    # 展示タイム差を主役にする
    float_times = []
    for lane, t in enumerate(times, start=1):
        try:
            float_times.append((lane, float(t)))
        except Exception:
            pass

    if len(float_times) == 6:
        min_time = min(v for _, v in float_times)
        avg_time = sum(v for _, v in float_times) / 6.0
        sorted_times = sorted(float_times, key=lambda x: x[1])
        spread = max(v for _, v in float_times) - min_time
        gap12 = sorted_times[1][1] - sorted_times[0][1]

        for lane, v in float_times:
            diff_from_min = v - min_time
            diff_from_avg = v - avg_time
            if diff_from_min <= 0.00:
                lane_scores[lane] += 0.18
            elif diff_from_min <= 0.03:
                lane_scores[lane] += 0.10
            elif diff_from_min >= 0.10:
                lane_scores[lane] -= 0.14
            elif diff_from_min >= 0.06:
                lane_scores[lane] -= 0.08

            if diff_from_avg <= -0.05:
                lane_scores[lane] += 0.06
            elif diff_from_avg >= 0.05:
                lane_scores[lane] -= 0.06

        if spread >= 0.18:
            fastest_lane = sorted_times[0][0]
            lane_scores[fastest_lane] += 0.08
        elif spread >= 0.12:
            fastest_lane = sorted_times[0][0]
            lane_scores[fastest_lane] += 0.05

        if gap12 >= 0.05:
            fastest_lane = sorted_times[0][0]
            lane_scores[fastest_lane] += 0.04

    water_state_score = float(weather_info.get("water_state_score") or 0)
    if water_state_score != 0:
        lane_scores[1] += water_state_score * 0.8
        for lane in [4, 5, 6]:
            lane_scores[lane] -= water_state_score * 0.25

    wind_type = str(weather_info.get("wind_type") or "")
    wind_level = get_wind_level(weather_info)
    try:
        wind_speed = float(weather_info.get("wind_speed") or 0)
    except Exception:
        wind_speed = 0.0

    if wind_level > 0:
        if wind_type == "向い風":
            lane_scores[1] -= 0.06 * wind_level
            lane_scores[2] += 0.05 * wind_level
            lane_scores[3] += 0.03 * wind_level
            lane_scores[5] += 0.01 * wind_level
            lane_scores[6] += 0.02 * wind_level
        elif wind_type == "追い風":
            lane_scores[1] += 0.08 * wind_level
            lane_scores[2] += 0.02 * wind_level
            lane_scores[4] -= 0.02 * wind_level
            lane_scores[5] -= 0.04 * wind_level
            lane_scores[6] -= 0.06 * wind_level
        elif wind_type == "横風":
            lane_scores[1] -= 0.01 * wind_level
            lane_scores[2] += 0.02 * wind_level
            lane_scores[3] += 0.02 * wind_level
            lane_scores[4] -= 0.03 * wind_level
            lane_scores[5] -= 0.05 * wind_level
            lane_scores[6] -= 0.06 * wind_level

    if wind_speed >= 7:
        lane_scores[2] += 0.02
        lane_scores[3] += 0.02
        lane_scores[5] -= 0.03
        lane_scores[6] -= 0.05

    foot_lane_scores = foot_material.get("lane_scores", {})
    if foot_lane_scores:
        for lane in range(1, 7):
            lane_scores[lane] += float(foot_lane_scores.get(lane, 0) or 0) * 0.90

    return lane_scores

def build_lane_score_text(exhibition_info, weather_info=None, foot_material=None):
    lane_scores = compute_lane_scores_map(exhibition_info, weather_info, foot_material)
    return " / ".join([f"{lane}:{lane_scores[lane]:.2f}" for lane in range(1, 7)])


def calculate_latest_signal_metrics(exhibition_info, foot_material=None):
    foot_material = foot_material or {}
    ranks = exhibition_info.get("ranks", {}) if exhibition_info else {}
    times = exhibition_info.get("times", []) if exhibition_info else []
    st_map = foot_material.get("st_map", {}) or {}
    entry_change = bool(foot_material.get("entry_change"))
    entry_text = str(foot_material.get("entry_text") or "")
    entry_severity = float(foot_material.get("entry_severity", 0) or 0)
    course_map = foot_material.get("course_map", {}) or {}

    exp_spread = 0.0
    exp_gap12 = 0.0
    lane1_time_gap = 0.0
    st_spread = 0.0
    st_gap12 = 0.0
    lane1_st = None

    float_times = []
    for lane, t in enumerate(times, start=1):
        try:
            v = float(t)
        except Exception:
            continue
        if is_exhibition_time_value(v):
            float_times.append((lane, v))

    if len(float_times) == 6:
        sorted_times = sorted(float_times, key=lambda x: x[1])
        exp_spread = round(sorted_times[-1][1] - sorted_times[0][1], 3)
        exp_gap12 = round(sorted_times[1][1] - sorted_times[0][1], 3)
        lane1_time = next((v for lane, v in float_times if lane == 1), None)
        if lane1_time is not None:
            lane1_time_gap = round(lane1_time - sorted_times[0][1], 3)

    valid_st = sorted(
        [(lane, float(v)) for lane, v in st_map.items() if isinstance(v, (int, float))],
        key=lambda x: x[1],
    )
    if len(valid_st) >= 4:
        st_spread = round(valid_st[-1][1] - valid_st[0][1], 3)
        if len(valid_st) >= 2:
            st_gap12 = round(valid_st[1][1] - valid_st[0][1], 3)
        lane1_st = st_map.get(1)

    signal_strength = 0.0
    if exp_spread >= 0.18:
        signal_strength += 0.34
    elif exp_spread >= 0.14:
        signal_strength += 0.26
    elif exp_spread >= 0.10:
        signal_strength += 0.18
    elif exp_spread >= 0.07:
        signal_strength += 0.08

    if exp_gap12 >= 0.05:
        signal_strength += 0.10
    elif exp_gap12 >= 0.03:
        signal_strength += 0.05

    if lane1_time_gap >= 0.10:
        signal_strength += 0.09
    elif lane1_time_gap >= 0.07:
        signal_strength += 0.05

    if st_spread >= 0.12:
        signal_strength += 0.30
    elif st_spread >= 0.09:
        signal_strength += 0.22
    elif st_spread >= 0.06:
        signal_strength += 0.12
    elif st_spread >= 0.04:
        signal_strength += 0.06

    if st_gap12 >= 0.04:
        signal_strength += 0.08
    elif st_gap12 >= 0.02:
        signal_strength += 0.04

    rank1 = ranks.get(1)
    if rank1 in {1, 6}:
        signal_strength += 0.05
    elif rank1 in {2, 5}:
        signal_strength += 0.02

    if entry_change:
        signal_strength += min(0.42, entry_severity * 0.82)
        if course_map.get(1, 1) > 1:
            signal_strength += 0.06

    signal_strength = round(clamp(signal_strength, 0.0, 0.98), 2)
    latest_push = round(clamp(0.30 + signal_strength * 0.78, 0.28, 1.0), 2)

    if entry_change and signal_strength >= 0.62:
        signal_text = "進入変化ありで直前重視"
    elif entry_change and signal_strength >= 0.34:
        signal_text = "進入変化ありで少し動かす"
    elif signal_strength >= 0.62:
        signal_text = "直前差大で直前重視"
    elif signal_strength >= 0.34:
        signal_text = "直前差中で少し動かす"
    else:
        signal_text = "直前差小で朝寄り"

    return {
        "exp_spread": exp_spread,
        "exp_gap12": exp_gap12,
        "lane1_time_gap": lane1_time_gap,
        "st_spread": st_spread,
        "st_gap12": st_gap12,
        "lane1_st": lane1_st,
        "entry_change": entry_change,
        "entry_text": entry_text,
        "entry_severity": entry_severity,
        "signal_strength": signal_strength,
        "latest_push": latest_push,
        "signal_text": signal_text,
    }


def analyze_latest(base_ai_score, exhibition_info, weather_info=None, foot_material=None, signal_metrics=None):
    score = float(base_ai_score or 0)
    reasons = []
    weather_info = weather_info or {}
    foot_material = foot_material or {}
    signal_metrics = signal_metrics or calculate_latest_signal_metrics(exhibition_info, foot_material)

    ranks = exhibition_info.get("ranks", {}) if exhibition_info else {}
    times = exhibition_info.get("times", []) if exhibition_info else []
    latest_push = float(signal_metrics.get("latest_push", 0.5) or 0.5)
    course_map = foot_material.get("course_map", {}) or {}
    entry_change = bool(foot_material.get("entry_change"))
    entry_text = str(foot_material.get("entry_text") or "")
    pre_move_lanes = foot_material.get("pre_move_lanes", []) or []

    # 順位は補助、タイム差を主役にする
    if ranks and 1 in ranks:
        r1 = ranks[1]
        if r1 == 1:
            score += 0.24 * latest_push
            reasons.append("1号艇の展示順位が1位")
        elif r1 <= 2:
            score += 0.12 * latest_push
            reasons.append("1号艇の展示順位が上位")
        elif r1 >= 5:
            score -= 0.18 * latest_push
            reasons.append("1号艇の展示順位が下位")

    if times:
        float_times = []
        for t in times:
            try:
                float_times.append(float(t))
            except Exception:
                pass
        if len(float_times) == 6:
            sorted_times = sorted(float_times)
            spread = max(float_times) - min(float_times)
            gap12 = sorted_times[1] - sorted_times[0]
            lane1_time = float_times[0]
            lane1_gap = lane1_time - sorted_times[0]

            if spread >= 0.18:
                score += 0.18 * latest_push
                reasons.append("展示差あり")
            elif spread >= 0.14:
                score += 0.12 * latest_push
                reasons.append("展示差あり")
            elif spread >= 0.10:
                score += 0.08 * latest_push
                reasons.append("展示差ややあり")

            if gap12 >= 0.05:
                score += 0.04 * latest_push

            if lane1_gap <= 0.02:
                score += 0.10 * latest_push
            elif lane1_gap <= 0.05:
                score += 0.05 * latest_push
            elif lane1_gap >= 0.12:
                score -= 0.16 * latest_push
            elif lane1_gap >= 0.08:
                score -= 0.10 * latest_push

            if spread >= 0.10:
                reasons.append("展示タイム差を反映")

    if entry_change:
        reasons.append(f"進入:{entry_text}") if entry_text else reasons.append("進入変化あり")
        lane1_course = course_map.get(1, 1)
        if lane1_course == 2:
            score -= 0.12 * (0.70 + latest_push * 0.40)
            reasons.append("1がイン外し")
        elif lane1_course >= 3:
            score -= 0.22 * (0.72 + latest_push * 0.42)
            reasons.append("1が大きくイン外し")
        if len(pre_move_lanes) >= 2 or any(lane >= 5 for lane in pre_move_lanes):
            score -= 0.05 * latest_push
            reasons.append("前づけで波乱含み")

    wind = weather_info.get("wind_speed")
    wave = weather_info.get("wave_height")
    wind_dir = str(weather_info.get("wind_dir") or "")
    water_state_score = float(weather_info.get("water_state_score") or 0)
    if water_state_score != 0:
        score += water_state_score * (0.62 + latest_push * 0.28)
        reasons.append("気象安定" if water_state_score > 0 else "気象荒れ気味")
    if weather_info.get("wind_type") == "向い風":
        reasons.append("向い風")
    elif weather_info.get("wind_type") == "追い風":
        reasons.append("追い風")
    elif weather_info.get("wind_type") == "横風":
        reasons.append("横風")
    if wind_dir:
        reasons.append(f"風向{wind_dir}")
    if isinstance(wind, (int, float)) and wind >= 7:
        reasons.append(f"風速{wind:g}m")
    if isinstance(wave, (int, float)) and wave >= 5:
        reasons.append(f"波高{wave:g}cm")

    foot_bonus = float(foot_material.get("foot_bonus", 0) or 0)
    if foot_bonus != 0:
        score += foot_bonus * (0.58 + latest_push * 0.42)

    foot_reason_text = str(foot_material.get("reason_text") or "").strip()
    if foot_reason_text:
        reasons.extend([x.strip() for x in foot_reason_text.split(" / ") if x.strip()])

    return {
        "raw_final_ai_score": round(score, 2),
        "final_ai_score": round(score, 2),
        "final_ai_rating": score_to_ai_rating(score),
        "latest_reason_text": " / ".join(dict.fromkeys(reasons[:10])),
    }

def selection_triplets(selection):
    if not selection:
        return []
    return [x.strip() for x in str(selection).split(" / ") if x.strip()]


def parse_selection_weight_map(base_selection):
    triplets = selection_triplets(base_selection)
    weight_map = {}
    for idx, tri in enumerate(triplets):
        weight_map[tri] = max(0.35, 1.0 - idx * 0.09)
    return weight_map


def calc_base_hold_strength(base_info):
    base_info = base_info or {}
    strength = 0.0
    try:
        base_score = float(base_info.get("base_ai_score", 0) or 0)
    except Exception:
        base_score = 0.0

    rating = str(base_info.get("rating") or "").strip()
    reason_text = str(base_info.get("base_reason_text") or "").strip()

    if base_score >= 2.8:
        strength += 0.24
    elif base_score >= 2.4:
        strength += 0.18
    elif base_score >= 2.0:
        strength += 0.12
    elif base_score >= 1.6:
        strength += 0.06


    reason_bonus = 0.0
    for word, bonus in [
        ("B2でも地力上位", 0.10),
        ("地力上位", 0.08),
        ("当地巧者", 0.07),
        ("地元水面", 0.05),
        ("選手力上位", 0.04),
        ("級別傾向強い", 0.03),
    ]:
        if word in reason_text:
            reason_bonus += bonus
    strength += min(0.20, reason_bonus)

    return round(clamp(strength, 0.0, 0.52), 2)


def normalize_race_phase_label(text):
    s = str(text or "").strip()
    if not s:
        return ""
    if "優勝戦" in s:
        return "優勝戦"
    if "準優勝戦" in s or "準優" in s:
        return "準優勝戦"
    if "ドリーム戦" in s or "ドリーム" in s:
        return "ドリーム戦"
    if "予選" in s:
        return "予選"
    if "一般戦" in s:
        return "一般戦"
    return ""


def build_phase_material(base_info, race_no=0):
    base_info = base_info or {}
    series_day = int(base_info.get("series_day") or 0)
    race_phase = normalize_race_phase_label(base_info.get("race_phase") or "")
    material = {
        "series_day": series_day,
        "race_phase": race_phase,
        "head": {lane: 0.0 for lane in range(1, 7)},
        "second": {lane: 0.0 for lane in range(1, 7)},
        "third": {lane: 0.0 for lane in range(1, 7)},
        "score_adjust": 0.0,
        "reason_text": "",
    }

    if race_phase == "準優勝戦":
        material["head"].update({1: 0.16, 2: 0.05, 3: 0.02, 4: -0.04, 5: -0.08, 6: -0.10})
        material["second"].update({2: 0.05, 3: 0.03, 4: 0.01})
        material["third"].update({4: 0.02, 5: 0.03, 6: 0.03})
        material["score_adjust"] = 0.10
        material["reason_text"] = "準優で1頭やや重視"
    elif race_phase == "優勝戦":
        material["head"].update({1: 0.12, 2: 0.04, 3: 0.02, 4: -0.03, 5: -0.06, 6: -0.08})
        material["second"].update({2: 0.04, 3: 0.03, 4: 0.01})
        material["third"].update({4: 0.01, 5: 0.02, 6: 0.02})
        material["score_adjust"] = 0.08
        material["reason_text"] = "優勝戦でやや堅め"
    elif race_phase == "ドリーム戦":
        material["head"].update({1: 0.08, 2: 0.03, 3: 0.01, 5: -0.02, 6: -0.03})
        material["second"].update({2: 0.03, 3: 0.02})
        material["third"].update({4: 0.01, 5: 0.01, 6: 0.01})
        material["score_adjust"] = 0.04
        material["reason_text"] = "ドリームでやや堅め"
    elif series_day == 1:
        material["head"].update({1: -0.03, 2: 0.01, 3: 0.02, 4: 0.01})
        material["second"].update({2: 0.02, 3: 0.01, 4: 0.02})
        material["third"].update({4: 0.03, 5: 0.04, 6: 0.04})
        material["score_adjust"] = -0.04
        material["reason_text"] = "初日で少し波乱寄り"
    elif series_day == 2:
        material["head"].update({1: 0.03, 2: 0.01, 5: -0.01, 6: -0.02})
        material["second"].update({2: 0.02, 3: 0.01})
        material["third"].update({5: -0.01, 6: -0.02})
        material["score_adjust"] = 0.03
        material["reason_text"] = "2日目でやや本命寄り"

    return material


def apply_phase_material_to_role_maps(role_maps, phase_material=None):
    phase_material = phase_material or {}
    if not role_maps:
        return role_maps

    adjusted = {
        "head": dict(role_maps.get("head", {}) or {}),
        "second": dict(role_maps.get("second", {}) or {}),
        "third": dict(role_maps.get("third", {}) or {}),
        "lane": dict(role_maps.get("lane", {}) or {}),
        "venue_notes": list(role_maps.get("venue_notes", []) or []),
    }

    for lane in range(1, 7):
        adjusted["head"][lane] = float(adjusted["head"].get(lane, 0) or 0) + float(phase_material.get("head", {}).get(lane, 0) or 0)
        adjusted["second"][lane] = float(adjusted["second"].get(lane, 0) or 0) + float(phase_material.get("second", {}).get(lane, 0) or 0)
        adjusted["third"][lane] = float(adjusted["third"].get(lane, 0) or 0) + float(phase_material.get("third", {}).get(lane, 0) or 0)

    reason_text = str(phase_material.get("reason_text") or "").strip()
    if reason_text and reason_text not in adjusted["venue_notes"]:
        adjusted["venue_notes"].append(reason_text)
    return adjusted


def stabilize_final_ai_score(base_ai_score, raw_final_ai_score, base_hold_strength, scenario_factor, signal_metrics=None):
    try:
        base_ai_score = float(base_ai_score or 0)
    except Exception:
        base_ai_score = 0.0
    try:
        raw_final_ai_score = float(raw_final_ai_score or 0)
    except Exception:
        raw_final_ai_score = base_ai_score

    signal_strength = float((signal_metrics or {}).get("signal_strength", 0) or 0)
    delta = raw_final_ai_score - base_ai_score
    reflect_factor = 0.36 + signal_strength * 0.46 + (float(scenario_factor or 1.0) - 0.45) * 0.16 - float(base_hold_strength or 0) * 0.26
    reflect_factor = clamp(reflect_factor, 0.26, 0.84)

    up_cap = 0.48 + signal_strength * 0.82 - float(base_hold_strength or 0) * 0.12 + max(0.0, float(scenario_factor or 1.0) - 0.78) * 0.12
    down_cap_mag = 0.52 + signal_strength * 0.86 - float(base_hold_strength or 0) * 0.10 + max(0.0, float(scenario_factor or 1.0) - 0.78) * 0.10
    delta = clamp(delta * reflect_factor, -down_cap_mag, up_cap)
    return round(base_ai_score + delta, 2)


def build_role_score_maps(venue, exhibition_info, weather_info=None, foot_material=None, day_trend_bias=None):
    lane_score_map = compute_lane_scores_map(exhibition_info, weather_info, foot_material)
    ranks = exhibition_info.get("ranks", {}) if exhibition_info else {}
    times = exhibition_info.get("times", []) if exhibition_info else []
    st_map = (foot_material or {}).get("st_map", {}) or {}

    head_score = {lane: float(lane_score_map.get(lane, 0) or 0) for lane in range(1, 7)}
    second_score = {lane: float(lane_score_map.get(lane, 0) or 0) * 0.90 for lane in range(1, 7)}
    third_score = {lane: float(lane_score_map.get(lane, 0) or 0) * 0.78 for lane in range(1, 7)}

    # v10.2: 1号艇頭残し / 2号艇2着残しを少し戻す
    head_score[1] += 0.24
    second_score[1] += 0.07
    third_score[1] -= 0.01

    head_score[2] += 0.07
    second_score[2] += 0.16
    third_score[2] += 0.04

    head_score[3] += 0.10
    second_score[3] += 0.08
    third_score[3] += 0.06
    head_score[4] += 0.00
    second_score[4] += 0.02
    third_score[4] += 0.06
    head_score[5] -= 0.04
    second_score[5] -= 0.01
    third_score[5] += 0.06
    head_score[6] -= 0.06
    second_score[6] -= 0.01
    third_score[6] += 0.08

    venue_bias = build_venue_bias_map(venue, day_trend_bias=day_trend_bias)
    for lane in range(1, 7):
        head_score[lane] += float(venue_bias["head"].get(lane, 0) or 0)
        second_score[lane] += float(venue_bias["second"].get(lane, 0) or 0)
        third_score[lane] += float(venue_bias["third"].get(lane, 0) or 0)

    course_order = (foot_material or {}).get("course_order", []) or []
    course_map = (foot_material or {}).get("course_map", {}) or {}
    pre_move_lanes = (foot_material or {}).get("pre_move_lanes", []) or []
    pulled_back_lanes = (foot_material or {}).get("pulled_back_lanes", []) or []
    if course_map:
        course1_lane = course_order[0] if course_order else None
        if course1_lane and course1_lane != 1:
            head_score[course1_lane] += 0.18
            second_score[course1_lane] += 0.08
            third_score[course1_lane] += 0.03

        if course_map.get(1, 1) > 1:
            lane1_course = course_map.get(1, 1)
            head_score[1] -= 0.16 if lane1_course == 2 else 0.28
            second_score[1] -= 0.06 if lane1_course == 2 else 0.12
            third_score[1] += 0.02

        for lane in pre_move_lanes:
            course = course_map.get(lane, lane)
            gain = max(1, lane - course)
            if lane != 1:
                head_score[lane] += min(0.10 + 0.05 * gain, 0.24)
                second_score[lane] += min(0.08 + 0.04 * gain, 0.18)
                third_score[lane] += min(0.04 + 0.03 * gain, 0.12)
            if lane >= 4 and course <= 3:
                head_score[lane] += 0.06
                second_score[lane] += 0.04

        for lane in pulled_back_lanes:
            course = course_map.get(lane, lane)
            loss = max(1, course - lane)
            head_score[lane] -= min(0.06 + 0.04 * loss, 0.18)
            second_score[lane] -= min(0.03 + 0.03 * loss, 0.10)

    if ranks:
        for lane in range(1, 7):
            rank = ranks.get(lane)
            if rank is None:
                continue
            if rank == 1:
                head_score[lane] += 0.09
                second_score[lane] += 0.05
            elif rank == 2:
                head_score[lane] += 0.05
                second_score[lane] += 0.05
                third_score[lane] += 0.02
            elif rank == 3:
                head_score[lane] += 0.02
                second_score[lane] += 0.03
                third_score[lane] += 0.03
            elif rank == 4:
                third_score[lane] += 0.01
            elif rank == 5:
                head_score[lane] -= 0.04
                second_score[lane] -= 0.015
            elif rank == 6:
                head_score[lane] -= 0.07
                second_score[lane] -= 0.03
                third_score[lane] -= 0.02

        # 順位だけでは頭を強くしすぎない
        rank1 = ranks.get(1)
        if rank1 is not None:
            if rank1 <= 3:
                head_score[1] += 0.02
                second_score[1] += 0.01
            elif rank1 == 4:
                head_score[1] += 0.005

        rank2 = ranks.get(2)
        if rank2 is not None:
            if rank2 <= 3:
                second_score[2] += 0.025
            elif rank2 == 4:
                second_score[2] += 0.01

    float_times = []
    for lane, t in enumerate(times, start=1):
        try:
            float_times.append((lane, float(t)))
        except Exception:
            pass

    if len(float_times) == 6:
        min_time = min(v for _, v in float_times)
        avg_time = sum(v for _, v in float_times) / 6.0
        spread = max(v for _, v in float_times) - min_time
        sorted_times = sorted(float_times, key=lambda x: x[1])
        gap12 = sorted_times[1][1] - sorted_times[0][1]

        for lane, v in float_times:
            diff_min = v - min_time
            diff_avg = v - avg_time

            if diff_min <= 0.00:
                head_score[lane] += 0.13
                second_score[lane] += 0.07
            elif diff_min <= 0.03:
                head_score[lane] += 0.07
                second_score[lane] += 0.04
                third_score[lane] += 0.02
            elif diff_min >= 0.10:
                head_score[lane] -= 0.12
                second_score[lane] -= 0.05
            elif diff_min >= 0.06:
                head_score[lane] -= 0.06
                second_score[lane] -= 0.025

            if diff_avg <= -0.04:
                third_score[lane] += 0.03
            elif diff_avg >= 0.05:
                third_score[lane] -= 0.03

        if spread >= 0.18:
            fastest_lane = sorted_times[0][0]
            head_score[fastest_lane] += 0.05
        elif spread >= 0.12:
            fastest_lane = sorted_times[0][0]
            head_score[fastest_lane] += 0.03

        if gap12 >= 0.05:
            fastest_lane = sorted_times[0][0]
            head_score[fastest_lane] += 0.03
            second_score[fastest_lane] += 0.02

        lane1_time = next((v for lane, v in float_times if lane == 1), None)
        if lane1_time is not None:
            diff1 = lane1_time - min_time
            if diff1 <= 0.03:
                head_score[1] += 0.04
            elif diff1 <= 0.05:
                head_score[1] += 0.02
            elif diff1 >= 0.12:
                head_score[1] -= 0.06
            elif diff1 >= 0.08:
                head_score[1] -= 0.03

        lane2_time = next((v for lane, v in float_times if lane == 2), None)
        if lane2_time is not None:
            diff2 = lane2_time - min_time
            if diff2 <= 0.03:
                second_score[2] += 0.04
            elif diff2 <= 0.05:
                second_score[2] += 0.02
            elif diff2 >= 0.12:
                second_score[2] -= 0.04

    if len(st_map) >= 4:
        sorted_st = sorted(
            [(lane, v) for lane, v in st_map.items() if isinstance(v, (int, float))],
            key=lambda x: x[1]
        )
        best_lane = sorted_st[0][0]
        second_lane = sorted_st[1][0]
        worst_lane = sorted_st[-1][0]
        best_st = sorted_st[0][1]
        worst_st = sorted_st[-1][1]
        spread = worst_st - best_st

        if best_st <= 0.10:
            head_score[best_lane] += 0.16
            second_score[best_lane] += 0.08
        elif best_st <= 0.12:
            head_score[best_lane] += 0.10
            second_score[best_lane] += 0.06

        if spread >= 0.10:
            head_score[best_lane] += 0.06
            second_score[second_lane] += 0.06
            third_score[second_lane] += 0.03
            head_score[worst_lane] -= 0.06

        # v10.2: 1のSTが極端に悪くなければ頭残し
        st1 = st_map.get(1)
        if isinstance(st1, (int, float)):
            if st1 <= 0.14:
                head_score[1] += 0.05
            elif st1 <= 0.17:
                head_score[1] += 0.02

        st2 = st_map.get(2)
        if isinstance(st2, (int, float)):
            if st2 <= 0.14:
                second_score[2] += 0.05
            elif st2 <= 0.17:
                second_score[2] += 0.02

    wind_type = str((weather_info or {}).get("wind_type") or "")
    wind_level = get_wind_level(weather_info or {})
    try:
        wind_speed = float((weather_info or {}).get("wind_speed") or 0)
    except Exception:
        wind_speed = 0.0

    if wind_level > 0:
        if wind_type == "向い風":
            head_score[1] -= 0.08 * wind_level
            head_score[2] += 0.05 * wind_level
            head_score[3] += 0.04 * wind_level
            second_score[2] += 0.06 * wind_level
            second_score[3] += 0.04 * wind_level
            third_score[5] += 0.03 * wind_level
            third_score[6] += 0.04 * wind_level
        elif wind_type == "追い風":
            head_score[1] += 0.10 * wind_level
            second_score[1] += 0.03 * wind_level
            second_score[2] += 0.03 * wind_level
            head_score[4] -= 0.02 * wind_level
            head_score[5] -= 0.05 * wind_level
            head_score[6] -= 0.08 * wind_level
        elif wind_type == "横風":
            head_score[4] -= 0.04 * wind_level
            head_score[5] -= 0.07 * wind_level
            head_score[6] -= 0.10 * wind_level
            second_score[2] += 0.04 * wind_level
            second_score[3] += 0.04 * wind_level
            third_score[1] += 0.02 * wind_level
            third_score[2] += 0.03 * wind_level

    if wind_speed >= 7:
        head_score[2] += 0.03
        head_score[3] += 0.03
        second_score[2] += 0.03
        second_score[3] += 0.02
        head_score[5] -= 0.04
        head_score[6] -= 0.07
        second_score[5] -= 0.03
        second_score[6] -= 0.04
        third_score[5] -= 0.01
        third_score[6] -= 0.02

    for lane in [3, 4, 5, 6]:
        if lane_score_map.get(lane, 0) >= 0.12:
            head_score[lane] += 0.05
            third_score[lane] += 0.03

    return {
        "lane": lane_score_map,
        "head": head_score,
        "second": second_score,
        "third": third_score,
        "venue_notes": venue_bias.get("notes", []),
    }


def build_pref_bonus_map(lanes, values):
    out = {}
    for lane, val in zip(lanes, values):
        if lane is None:
            continue
        out[int(lane)] = float(val)
    return out


def get_exhibition_time_by_lane(exhibition_info, lane):
    times = (exhibition_info or {}).get("times", []) or []
    idx = int(lane) - 1
    if idx < 0 or idx >= len(times):
        return None
    try:
        v = float(times[idx])
    except Exception:
        return None
    return v if is_exhibition_time_value(v) else None


def calc_fastest_gap_by_lane(exhibition_info, lane):
    target = get_exhibition_time_by_lane(exhibition_info, lane)
    if target is None:
        return None

    float_times = []
    for i, raw in enumerate((exhibition_info or {}).get("times", []) or [], start=1):
        try:
            v = float(raw)
        except Exception:
            continue
        if is_exhibition_time_value(v):
            float_times.append((i, v))

    if len(float_times) < 2:
        return None

    sorted_times = sorted(float_times, key=lambda x: x[1])
    if sorted_times[0][0] != int(lane):
        return None
    return round(sorted_times[1][1] - sorted_times[0][1], 3)


def should_guard_center_head_shift(exhibition_info, foot_material=None, center_lane=3, escape_weight=0.0):
    foot_material = foot_material or {}
    course_map = foot_material.get("course_map", {}) or {}
    entry_change = bool(foot_material.get("entry_change"))
    entry_severity = float(foot_material.get("entry_severity", 0) or 0)
    st_map = foot_material.get("st_map", {}) or {}

    if int(center_lane) != 3:
        return False
    if float(escape_weight or 0) < 0.42:
        return False
    if course_map.get(1, 1) != 1:
        return False
    if entry_change or entry_severity >= 0.12:
        return False

    lane1_time = get_exhibition_time_by_lane(exhibition_info, 1)
    lane3_time = get_exhibition_time_by_lane(exhibition_info, 3)
    if lane1_time is None or lane3_time is None:
        return False

    ex_diff = abs(lane1_time - lane3_time)
    if ex_diff > 0.05:
        return False

    st1 = st_map.get(1)
    st3 = st_map.get(3)
    if isinstance(st1, (int, float)) and isinstance(st3, (int, float)):
        # 1 がそこまで悪くないなら、3 はまず相手側へ寄せる
        if (float(st1) - float(st3)) > 0.03:
            return False

    return True


def should_loosen_outer_head_attack(exhibition_info, foot_material=None, outer_lane=6):
    foot_material = foot_material or {}
    st_map = foot_material.get("st_map", {}) or {}
    pre_move_lanes = foot_material.get("pre_move_lanes", []) or []
    entry_change = bool(foot_material.get("entry_change"))
    course_map = foot_material.get("course_map", {}) or {}
    ranks = (exhibition_info or {}).get("ranks", {}) or {}

    if int(outer_lane) != 6:
        return False

    # 6 は展示最速だけで頭まで押しすぎない。
    strong_signal = False
    st6 = st_map.get(6)
    if isinstance(st6, (int, float)) and float(st6) <= 0.11:
        strong_signal = True
    if entry_change and (6 in pre_move_lanes or course_map.get(6, 6) < 6):
        strong_signal = True

    fastest_gap = calc_fastest_gap_by_lane(exhibition_info, 6)
    if ranks.get(6) == 1 and fastest_gap is not None and fastest_gap >= 0.05:
        strong_signal = True

    return not strong_signal


def build_turn_scenario_material(venue, exhibition_info, weather_info=None, foot_material=None, role_maps=None, day_trend_bias=None):
    weather_info = weather_info or {}
    foot_material = foot_material or {}
    role_maps = role_maps or build_role_score_maps(venue, exhibition_info, weather_info, foot_material, day_trend_bias=day_trend_bias)

    head_score = role_maps["head"]
    second_score = role_maps["second"]
    third_score = role_maps["third"]
    lane_score_map = role_maps["lane"]
    venue_notes = role_maps.get("venue_notes", [])

    ranks = exhibition_info.get("ranks", {}) if exhibition_info else {}
    st_map = foot_material.get("st_map", {}) or {}
    course_order = foot_material.get("course_order", []) or []
    course_map = foot_material.get("course_map", {}) or {}
    pre_move_lanes = foot_material.get("pre_move_lanes", []) or []
    entry_change = bool(foot_material.get("entry_change"))
    entry_text = str(foot_material.get("entry_text") or "")
    entry_severity = float(foot_material.get("entry_severity", 0) or 0)

    scenarios = []
    head_ranked = [lane for lane, _ in sorted(head_score.items(), key=lambda x: x[1], reverse=True)]
    second_ranked = [lane for lane, _ in sorted(second_score.items(), key=lambda x: x[1], reverse=True)]
    lane_ranked = [lane for lane, _ in sorted(lane_score_map.items(), key=lambda x: x[1], reverse=True)]

    best_head = head_ranked[0]
    second_head = head_ranked[1]
    head_gap = head_score[best_head] - head_score[second_head]

    rank1 = ranks.get(1)
    st1 = st_map.get(1)
    weight_1 = 0.0
    if head_score.get(1, 0) >= 0.08:
        weight_1 += 0.32
    if best_head == 1:
        weight_1 += 0.20
    if rank1 == 1:
        weight_1 += 0.24
    elif rank1 == 2:
        weight_1 += 0.15
    elif rank1 == 3:
        weight_1 += 0.08
    elif rank1 is not None and rank1 >= 5:
        weight_1 -= 0.18
    if isinstance(st1, (int, float)):
        if st1 <= 0.11:
            weight_1 += 0.12
        elif st1 <= 0.14:
            weight_1 += 0.05
        elif st1 >= 0.19:
            weight_1 -= 0.10
    if weather_info.get("water_state_score", 0) > 0:
        weight_1 += 0.04
    if course_map.get(1, 1) == 2:
        weight_1 -= 0.24
    elif course_map.get(1, 1) >= 3:
        weight_1 -= 0.38
    if "大村イン寄り" in venue_notes:
        weight_1 += 0.06
    if "江戸川外警戒" in venue_notes:
        weight_1 -= 0.08
    if head_gap >= 0.16 and best_head == 1:
        weight_1 += 0.06
    weight_1 = clamp(weight_1, 0.0, 1.0)

    if weight_1 >= 0.20:
        scenarios.append({
            "name": "1逃げ本線",
            "head_lane": 1,
            "weight": weight_1,
            "head_bonus": {
                1: 0.25,
                2: 0.03,
                3: 0.02,
            },
            "second_bonus": {
                2: 0.17,
                3: 0.10,
                4: 0.05,
                5: 0.01,
                1: -0.05,
            },
            "third_bonus": {
                2: 0.06,
                3: 0.10,
                4: 0.10,
                5: 0.06,
                6: 0.03,
                1: -0.04,
            },
        })

    rank2 = ranks.get(2)
    st2 = st_map.get(2)
    weight_2 = 0.0
    if head_score.get(2, 0) >= 0.10:
        weight_2 += 0.24
    if best_head == 2:
        weight_2 += 0.18
    if second_score.get(1, 0) >= 0.06:
        weight_2 += 0.10
    if rank2 == 1:
        weight_2 += 0.18
    elif rank2 == 2:
        weight_2 += 0.10
    if rank1 is not None and rank1 >= 4:
        weight_2 += 0.08
    if course_order and course_order[0] == 2:
        weight_2 += 0.16
    elif 2 in pre_move_lanes:
        weight_2 += 0.08
    if isinstance(st2, (int, float)):
        if st2 <= 0.11:
            weight_2 += 0.12
        elif st2 <= 0.14:
            weight_2 += 0.05
        elif st2 >= 0.18:
            weight_2 -= 0.08
    if "大村イン寄り" in venue_notes:
        weight_2 -= 0.05
    weight_2 = clamp(weight_2, 0.0, 1.0)

    if weight_2 >= 0.20:
        scenarios.append({
            "name": "2差し注意",
            "head_lane": 2,
            "weight": weight_2,
            "head_bonus": {
                2: 0.23,
                3: 0.05,
                1: 0.03,
            },
            "second_bonus": {
                1: 0.15,
                3: 0.12,
                4: 0.05,
                5: 0.02,
                2: -0.03,
            },
            "third_bonus": {
                1: 0.08,
                3: 0.11,
                4: 0.09,
                5: 0.05,
                6: 0.03,
            },
        })

    center_candidates = [3, 4]
    center_lane = max(center_candidates, key=lambda lane: head_score.get(lane, -999))
    rank_center = ranks.get(center_lane)
    st_center = st_map.get(center_lane)
    weight_center = 0.0
    if head_score.get(center_lane, 0) >= 0.12:
        weight_center += 0.24
    if best_head == center_lane:
        weight_center += 0.18
    if lane_score_map.get(center_lane, 0) >= 0.12:
        weight_center += 0.10
    if rank_center == 1:
        weight_center += 0.18
    elif rank_center == 2:
        weight_center += 0.10
    if isinstance(st_center, (int, float)):
        if st_center <= 0.11:
            weight_center += 0.12
        elif st_center >= 0.19:
            weight_center -= 0.08
    if center_lane in pre_move_lanes:
        weight_center += 0.12
    if "若松やや波乱" in venue_notes:
        weight_center += 0.04
    if "江戸川外警戒" in venue_notes:
        weight_center += 0.05

    center_head_guard = should_guard_center_head_shift(
        exhibition_info,
        foot_material=foot_material,
        center_lane=center_lane,
        escape_weight=weight_1,
    )
    if center_head_guard:
        weight_center -= 0.14

    weight_center = clamp(weight_center, 0.0, 1.0)

    if weight_center >= 0.24:
        if center_lane == 3:
            if center_head_guard:
                second_pref = [3, 2, 1, 4, 5]
                third_pref = [3, 2, 1, 4, 5]
                name = "3攻め注意(相手寄り)"
                head_bonus = build_pref_bonus_map(
                    [3, 1, 2, 5],
                    [0.14, 0.08, 0.03, 0.01],
                )
                second_bonus = build_pref_bonus_map(
                    second_pref[:5],
                    [0.18, 0.12, 0.08, 0.04, 0.02],
                )
                third_bonus = build_pref_bonus_map(
                    third_pref[:5],
                    [0.14, 0.10, 0.08, 0.05, 0.03],
                )
            else:
                second_pref = [1, 2, 4, 5, 6]
                third_pref = [2, 1, 4, 5, 6]
                name = "3攻め注意"
                head_bonus = build_pref_bonus_map(
                    [center_lane, 2, 1, 5],
                    [0.25, 0.03, 0.02, 0.02],
                )
                second_bonus = build_pref_bonus_map(
                    second_pref[:5],
                    [0.13, 0.11, 0.07, 0.04, 0.02],
                )
                third_bonus = build_pref_bonus_map(
                    third_pref[:5],
                    [0.10, 0.09, 0.08, 0.05, 0.03],
                )
        else:
            second_pref = [2, 3, 1, 5, 6]
            third_pref = [3, 2, 1, 5, 6]
            name = "4攻め注意"
            head_bonus = build_pref_bonus_map(
                [center_lane, 2, 1, 5],
                [0.25, 0.03, 0.02, 0.02],
            )
            second_bonus = build_pref_bonus_map(
                second_pref[:5],
                [0.13, 0.11, 0.07, 0.04, 0.02],
            )
            third_bonus = build_pref_bonus_map(
                third_pref[:5],
                [0.10, 0.09, 0.08, 0.05, 0.03],
            )

        scenarios.append({
            "name": name,
            "head_lane": center_lane,
            "weight": weight_center,
            "head_bonus": head_bonus,
            "second_bonus": second_bonus,
            "third_bonus": third_bonus,
        })

    outer_candidates = [5, 6]
    outer_lane = max(outer_candidates, key=lambda lane: head_score.get(lane, -999))
    rank_outer = ranks.get(outer_lane)
    st_outer = st_map.get(outer_lane)
    weight_outer = 0.0
    if head_score.get(outer_lane, 0) >= 0.12:
        weight_outer += 0.20
    if best_head == outer_lane:
        weight_outer += 0.20
    if lane_score_map.get(outer_lane, 0) >= 0.16:
        weight_outer += 0.14
    if rank_outer == 1:
        weight_outer += 0.18
    elif rank_outer == 2:
        weight_outer += 0.10
    if isinstance(st_outer, (int, float)):
        if st_outer <= 0.10:
            weight_outer += 0.14
        elif st_outer >= 0.18:
            weight_outer -= 0.08
    if outer_lane in pre_move_lanes:
        weight_outer += 0.14
    if "江戸川外警戒" in venue_notes:
        weight_outer += 0.12
    if "若松やや波乱" in venue_notes:
        weight_outer += 0.06
    if "大村イン寄り" in venue_notes:
        weight_outer -= 0.08

    outer_head_guard = should_loosen_outer_head_attack(
        exhibition_info,
        foot_material=foot_material,
        outer_lane=outer_lane,
    )
    if outer_head_guard:
        weight_outer -= 0.16

    weight_outer = clamp(weight_outer, 0.0, 1.0)

    if weight_outer >= 0.24:
        if outer_lane == 5:
            second_pref = [6, 1, 2, 4, 3]
            third_pref = [1, 6, 2, 3, 4]
            name = "5一撃注意"
            head_bonus = build_pref_bonus_map(
                [outer_lane, 6, 1],
                [0.24, 0.04, 0.02],
            )
            second_bonus = build_pref_bonus_map(
                second_pref[:5],
                [0.14, 0.12, 0.09, 0.06, 0.03],
            )
            third_bonus = build_pref_bonus_map(
                third_pref[:5],
                [0.11, 0.10, 0.08, 0.05, 0.03],
            )
        else:
            second_pref = [6, 5, 1, 2, 3]
            third_pref = [6, 1, 5, 2, 3]
            if outer_head_guard:
                name = "6一撃注意(相手寄り)"
                head_bonus = build_pref_bonus_map(
                    [6, 5, 1],
                    [0.14, 0.03, 0.04],
                )
                second_bonus = build_pref_bonus_map(
                    second_pref[:5],
                    [0.18, 0.12, 0.10, 0.06, 0.03],
                )
                third_bonus = build_pref_bonus_map(
                    third_pref[:5],
                    [0.15, 0.10, 0.09, 0.05, 0.03],
                )
            else:
                name = "6一撃注意"
                head_bonus = build_pref_bonus_map(
                    [outer_lane, 5, 1],
                    [0.24, 0.04, 0.02],
                )
                second_bonus = build_pref_bonus_map(
                    [5, 1, 2, 3, 4],
                    [0.14, 0.12, 0.09, 0.06, 0.03],
                )
                third_bonus = build_pref_bonus_map(
                    [1, 5, 2, 3, 4],
                    [0.11, 0.10, 0.08, 0.05, 0.03],
                )

        scenarios.append({
            "name": name,
            "head_lane": outer_lane,
            "weight": weight_outer,
            "head_bonus": head_bonus,
            "second_bonus": second_bonus,
            "third_bonus": third_bonus,
        })

    if course_order:
        course1_lane = course_order[0]
        if course1_lane != 1:
            front_weight = clamp(0.22 + entry_severity * 0.55, 0.0, 0.46)
            if course1_lane == 2:
                second_pref = [1, 3, 4, 5, 6]
                third_pref = [1, 3, 4, 5, 6]
            elif course1_lane == 3:
                second_pref = [1, 2, 4, 5, 6]
                third_pref = [1, 2, 4, 5, 6]
            elif course1_lane == 4:
                second_pref = [1, 2, 3, 5, 6]
                third_pref = [1, 2, 3, 5, 6]
            elif course1_lane == 5:
                second_pref = [1, 2, 3, 4, 6]
                third_pref = [1, 2, 3, 4, 6]
            else:
                second_pref = [1, 2, 3, 4, 5]
                third_pref = [1, 2, 3, 4, 5]

            scenarios.append({
                "name": f"{course1_lane}前づけ注意",
                "head_lane": course1_lane,
                "weight": front_weight,
                "head_bonus": build_pref_bonus_map([course1_lane, 1, 2, 3], [0.24, 0.04, 0.03, 0.02]),
                "second_bonus": build_pref_bonus_map(second_pref[:5], [0.13, 0.11, 0.08, 0.05, 0.03]),
                "third_bonus": build_pref_bonus_map(third_pref[:5], [0.10, 0.09, 0.08, 0.05, 0.03]),
            })

    scenarios = sorted(scenarios, key=lambda x: x["weight"], reverse=True)[:4]
    scenario_names = [f"{s['name']}" for s in scenarios[:2]]
    if entry_change and entry_text:
        scenario_names.append(f"進入:{entry_text}")
    scenario_text = " / ".join(scenario_names[:3])

    return {
        "scenarios": scenarios,
        "scenario_text": scenario_text,
        "best_head_lane": best_head,
        "head_ranked": head_ranked,
        "second_ranked": second_ranked,
        "lane_ranked": lane_ranked,
    }


def scenario_strength_factor(exhibition_info, foot_material=None, signal_metrics=None):
    foot_material = foot_material or {}
    times = exhibition_info.get("times", []) if exhibition_info else []
    ranks = exhibition_info.get("ranks", {}) if exhibition_info else {}
    st_map = foot_material.get("st_map", {}) or {}
    signal_metrics = signal_metrics or calculate_latest_signal_metrics(exhibition_info, foot_material)

    factor = 1.0

    has_times = len(times) == 6
    has_ranks = len(ranks) == 6
    has_st = len(st_map) >= 4

    if not has_times and not has_ranks:
        factor *= 0.55
    elif not has_times or not has_ranks:
        factor *= 0.72

    if not has_st:
        factor *= 0.90

    signal_strength = float(signal_metrics.get("signal_strength", 0) or 0)
    dynamic_factor = 0.78 + signal_strength * 0.34
    factor *= dynamic_factor

    return round(clamp(factor, 0.45, 1.0), 2)


def scenario_bonus_for_triplet(tri, scenario_material, scenario_factor=1.0):
    if not tri or not scenario_material:
        return 0.0

    try:
        a, b, c = [int(x) for x in tri.split("-")]
    except Exception:
        return 0.0

    total = 0.0
    scenarios = scenario_material.get("scenarios", [])

    for sc in scenarios:
        w = float(sc.get("weight", 0) or 0)
        if w <= 0:
            continue

        head_bonus = sc.get("head_bonus", {})
        second_bonus = sc.get("second_bonus", {})
        third_bonus = sc.get("third_bonus", {})

        total += float(head_bonus.get(a, -0.03 if a != sc.get("head_lane") else 0) or 0) * w
        total += float(second_bonus.get(b, 0) or 0) * w
        total += float(third_bonus.get(c, 0) or 0) * w

        if a == sc.get("head_lane") and b == a:
            total -= 0.10 * w
        if a == sc.get("head_lane") and b in second_bonus and c in third_bonus:
            total += 0.03 * w

    total *= float(scenario_factor or 1.0)
    return round(total, 4)


def pick_best_triplet_for_head(scored_rows, head_lane, exclude_triplets=None):
    exclude_triplets = set(exclude_triplets or [])
    for tri, _score in scored_rows:
        if tri in exclude_triplets:
            continue
        if tri.startswith(f"{head_lane}-"):
            return tri
    return ""


def pick_best_triplet_by_condition(scored_rows, condition_fn, exclude_triplets=None):
    exclude_triplets = set(exclude_triplets or [])
    for tri, _score in scored_rows:
        if tri in exclude_triplets:
            continue
        try:
            a, b, c = [int(x) for x in tri.split("-")]
        except Exception:
            continue
        if condition_fn(a, b, c, tri):
            return tri
    return ""


def is_outer_head_too_loose(lane, exhibition_info=None, foot_material=None, lane_score_map=None):
    """
    5・6号艇は、展示だけで頭にしすぎないためのガード。
    強い根拠がある時だけ頭候補として許可する。
    """
    lane = int(lane)
    if lane not in {5, 6}:
        return False

    exhibition_info = exhibition_info or {}
    foot_material = foot_material or {}
    lane_score_map = lane_score_map or {}

    ranks = exhibition_info.get("ranks", {}) or {}
    st_map = foot_material.get("st_map", {}) or {}
    pre_move_lanes = foot_material.get("pre_move_lanes", []) or []
    course_map = foot_material.get("course_map", {}) or {}
    entry_change = bool(foot_material.get("entry_change"))

    lane_score = float(lane_score_map.get(lane, 0) or 0)
    rank = ranks.get(lane)
    st = st_map.get(lane)
    fastest_gap = calc_fastest_gap_by_lane(exhibition_info, lane)

    strong_signal = False

    if lane_score >= 0.30 and rank in {1, 2}:
        strong_signal = True

    if fastest_gap is not None and fastest_gap >= 0.05:
        strong_signal = True

    if isinstance(st, (int, float)) and float(st) <= 0.10:
        strong_signal = True

    if entry_change and (lane in pre_move_lanes or course_map.get(lane, lane) < lane):
        strong_signal = True

    return not strong_signal


def should_keep_lane1_head_core(
    base_info,
    exhibition_info,
    foot_material,
    role_maps,
    signal_metrics,
    base_hold_strength=0.0,
):
    """
    1号艇が大きく悪くない時は、本線3点の中に1頭を残す。
    直前材料で外へ寄せすぎるのを防ぐ。
    """
    base_info = base_info or {}
    exhibition_info = exhibition_info or {}
    foot_material = foot_material or {}
    role_maps = role_maps or {}
    signal_metrics = signal_metrics or {}

    course_map = foot_material.get("course_map", {}) or {}
    if course_map.get(1, 1) != 1:
        return False

    ranks = exhibition_info.get("ranks", {}) or {}
    st_map = foot_material.get("st_map", {}) or {}

    rank1 = ranks.get(1)
    st1 = st_map.get(1)
    lane1_gap = float(signal_metrics.get("lane1_time_gap", 0) or 0)
    signal_strength = float(signal_metrics.get("signal_strength", 0) or 0)

    lane_map = role_maps.get("lane", {}) or {}
    head_map = role_maps.get("head", {}) or {}

    lane1_eval = float(lane_map.get(1, 0) or 0)
    head1 = float(head_map.get(1, 0) or 0)

    class_support = lane_has_class_support(base_info, lane=1)
    reason_support = lane_has_reason_support(base_info, lane=1)
    morning_support = float(base_hold_strength or 0) >= 0.18

    # かなり明確に悪い時は残さない
    if (
        lane1_gap >= 0.14
        and rank1 is not None
        and rank1 >= 5
        and isinstance(st1, (int, float))
        and float(st1) >= 0.18
    ):
        return False

    if signal_strength >= 0.72 and lane1_gap >= 0.10 and head1 < -0.08 and not morning_support:
        return False

    # 残していい条件
    if class_support or reason_support or morning_support:
        return True

    if rank1 in {1, 2, 3, 4}:
        return True

    if isinstance(st1, (int, float)) and float(st1) <= 0.16:
        return True

    if lane1_gap <= 0.05:
        return True

    if head1 >= 0.05 or lane1_eval >= 0.02:
        return True

    return False


def build_core_cover_triplets(
    initial_top,
    scored_rows,
    role_maps,
    scenario_material,
    signal_metrics,
    base_triplets=None,
    official_triplets=None,
    base_info=None,
    exhibition_info=None,
    foot_material=None,
    base_hold_strength=0.0,
):
    """
    AI6点を「本線3点 + 別頭/相手抜けカバー3点」に整える。
    まずAI6点内に結果が入りやすくなるよう、保険3点は頭違い・相手違いを優先する。
    """
    base_triplets = base_triplets or []
    official_triplets = official_triplets or []
    exhibition_info = exhibition_info or {}
    foot_material = foot_material or {}
    signal_metrics = signal_metrics or {}

    score_map = {tri: score for tri, score in scored_rows}
    lane_score_map = role_maps.get("lane", {}) or {}
    head_score = role_maps.get("head", {}) or {}

    head_ranked = [lane for lane, _ in sorted(head_score.items(), key=lambda x: x[1], reverse=True)]
    if not head_ranked:
        return initial_top[:6]

    signal_strength = float(signal_metrics.get("signal_strength", 0) or 0)
    top_head = int(head_ranked[0])
    second_head = int(head_ranked[1]) if len(head_ranked) >= 2 else top_head
    head_gap = float(head_score.get(top_head, 0) or 0) - float(head_score.get(second_head, 0) or 0)

    lane1_core = should_keep_lane1_head_core(
        base_info,
        exhibition_info,
        foot_material,
        role_maps,
        signal_metrics,
        base_hold_strength=base_hold_strength,
    )

    candidate_heads = []

    if lane1_core:
        candidate_heads.append(1)

    candidate_heads.append(top_head)

    for sc in (scenario_material.get("scenarios", []) or [])[:2]:
        try:
            lane = int(sc.get("head_lane") or 0)
            weight = float(sc.get("weight", 0) or 0)
        except Exception:
            continue
        if lane and weight >= 0.24:
            candidate_heads.append(lane)

    if len(head_ranked) >= 2 and (signal_strength >= 0.34 or head_gap <= 0.18):
        candidate_heads.append(second_head)

    # 重複削除
    dedup_heads = []
    for h in candidate_heads:
        if h not in dedup_heads:
            dedup_heads.append(h)

    main = []

    def add_main(tri):
        if tri and tri in score_map and tri not in main:
            main.append(tri)

    # 本線3点：頭候補ごとに一番良い買い目を入れる
    for head in dedup_heads:
        tri = pick_best_triplet_by_condition(
            scored_rows,
            lambda a, b, c, t, head=head: (
                a == head
                and not is_outer_head_too_loose(a, exhibition_info, foot_material, lane_score_map)
            ),
            exclude_triplets=main,
        )
        add_main(tri)
        if len(main) >= 3:
            break

    # 足りなければ、通常スコア上位から補充
    for tri, _score in scored_rows:
        if len(main) >= 3:
            break
        try:
            a, _b, _c = [int(x) for x in tri.split("-")]
        except Exception:
            continue
        if is_outer_head_too_loose(a, exhibition_info, foot_material, lane_score_map):
            continue
        add_main(tri)

    # それでも足りなければ外頭も許可して補充
    for tri, _score in scored_rows:
        if len(main) >= 3:
            break
        add_main(tri)

    main = main[:3]

    selected = main[:]
    cover = []

    def add_cover(tri):
        if not tri:
            return
        if tri not in score_map:
            return
        if tri in selected or tri in cover:
            return
        cover.append(tri)

    # 1) AIの2番手・3番手頭を先に保険側で拾う
    #    v10.27: 先にスワップで埋めると別頭が入らないため、別頭を最優先にする。
    main_heads = []
    for tri in main:
        try:
            h = int(tri.split("-")[0])
        except Exception:
            continue
        if h not in main_heads:
            main_heads.append(h)

    alt_head_added = 0
    for head in head_ranked[:5]:
        if len(cover) >= 3 or alt_head_added >= 2:
            break
        try:
            head = int(head)
        except Exception:
            continue
        if head in main_heads:
            continue
        # 5/6頭は強い根拠が弱い時は無理に頭で拾わない
        if is_outer_head_too_loose(head, exhibition_info, foot_material, lane_score_map):
            continue
        tri = pick_best_triplet_by_condition(
            scored_rows,
            lambda a, b, c, t, head=head: a == head,
            exclude_triplets=selected + cover,
        )
        before = len(cover)
        add_cover(tri)
        if len(cover) > before:
            alt_head_added += 1

    # 2) 展示・足色上位は頭だけでなく2着/3着にも残す
    #    当たり筋を増やすため、外枠の好気配も連絡みとして拾う。
    strong_lanes = [lane for lane, _score in sorted(lane_score_map.items(), key=lambda x: x[1], reverse=True)[:4]]
    strong_line_added = 0
    for lane in strong_lanes:
        if len(cover) >= 3 or strong_line_added >= 2:
            break
        try:
            lane = int(lane)
        except Exception:
            continue
        tri = pick_best_triplet_by_condition(
            scored_rows,
            lambda a, b, c, t, lane=lane: (b == lane or c == lane),
            exclude_triplets=selected + cover,
        )
        before = len(cover)
        add_cover(tri)
        if len(cover) > before:
            strong_line_added += 1

    # 3) 1号艇を完全に消しすぎないため、頭以外でも1を含む筋を1点だけ残す
    #    ただし本線/保険にすでに1が絡んでいれば追加しない。
    has_lane1_any = any('1' in tri.split('-') for tri in selected + cover if tri.count('-') == 2)
    if len(cover) < 3 and not has_lane1_any:
        tri = pick_best_triplet_by_condition(
            scored_rows,
            lambda a, b, c, t: (a == 1 or b == 1 or c == 1),
            exclude_triplets=selected + cover,
        )
        add_cover(tri)

    # 4) 本線の2・3着入れ替えを1〜2点だけ入れる
    #    頭は同じまま相手抜けを拾うが、これだけで保険3点を埋めない。
    swap_added = 0
    for tri in main:
        if len(cover) >= 3 or swap_added >= 2:
            break
        try:
            a, b, c = tri.split("-")
        except Exception:
            continue
        swap = f"{a}-{c}-{b}"
        before = len(cover)
        add_cover(swap)
        if len(cover) > before:
            swap_added += 1

    # 5) 朝/baseの上位も1〜2点残す
    #    直前材料が弱い時の戻し先として使う。
    base_added = 0
    for tri in base_triplets[:4]:
        if len(cover) >= 3 or base_added >= 2:
            break
        before = len(cover)
        add_cover(tri)
        if len(cover) > before:
            base_added += 1

    # 6) 公式は答えではなく安全確認用。
    #    強制採用はせず、AIスコア上位に残っている場合だけ最後の候補として扱う。
    top_score_values = [score for _tri, score in scored_rows[:12]]
    soft_line = min(top_score_values) if top_score_values else None
    for tri in official_triplets[:2]:
        if len(cover) >= 3:
            break
        if soft_line is not None and score_map.get(tri, -999) < soft_line:
            continue
        add_cover(tri)

    # 7) 本線と同じ頭で、2・3着違いを補充
    for head in main_heads:
        for tri, _score in scored_rows:
            if len(cover) >= 3:
                break
            try:
                a, _b, _c = [int(x) for x in tri.split("-")]
            except Exception:
                continue
            if a == head:
                add_cover(tri)
        if len(cover) >= 3:
            break

    # 7) 最後に通常スコア上位で埋める
    for tri, _score in scored_rows:
        if len(cover) >= 3:
            break
        add_cover(tri)

    final = []
    for tri in selected + cover:
        if tri not in final:
            final.append(tri)
        if len(final) >= 6:
            break

    return final


def enforce_head_diversity(top, scored_rows, scenario_material, scenario_factor):
    if not top:
        return top

    head_counts = {}
    for tri in top:
        head = int(tri.split("-")[0])
        head_counts[head] = head_counts.get(head, 0) + 1

    dominant_head, dominant_count = sorted(head_counts.items(), key=lambda x: (x[1], x[0]), reverse=True)[0]
    scenarios = scenario_material.get("scenarios", [])

    if dominant_count <= 4:
        return top

    candidate_heads = []
    for sc in scenarios:
        lane = sc.get("head_lane")
        if lane and lane != dominant_head:
            candidate_heads.append((lane, float(sc.get("weight", 0) or 0)))

    candidate_heads = [lane for lane, w in candidate_heads if w * scenario_factor >= 0.20]

    replaced = top[:]
    for alt_head in candidate_heads[:2]:
        alt_tri = pick_best_triplet_for_head(scored_rows, alt_head, exclude_triplets=replaced)
        if alt_tri:
            replace_idx = -1
            for idx in range(len(replaced) - 1, -1, -1):
                if replaced[idx].startswith(f"{dominant_head}-"):
                    replace_idx = idx
                    break
            if replace_idx >= 0:
                replaced[replace_idx] = alt_tri

    dedup = []
    for tri in replaced:
        if tri not in dedup:
            dedup.append(tri)
        if len(dedup) >= 6:
            break
    return dedup


def add_basic_form_triplets(
    top,
    scored_rows,
    role_maps,
    exhibition_info,
    base_triplets=None,
):
    """
    v10.2:
    1頭基本形と2着2号艇を少し残す。
    強制しすぎず、条件に合う時だけ1点差し替え。
    """
    base_triplets = base_triplets or []
    head_score = role_maps["head"]
    second_score = role_maps["second"]
    lane_score_map = role_maps["lane"]
    ranks = exhibition_info.get("ranks", {}) if exhibition_info else {}

    if not top:
        return top

    # 1号艇が完全に死んでいない条件
    keep_one_head = False
    if head_score.get(1, -999) >= -0.08:
        keep_one_head = True
    if ranks.get(1) in [1, 2, 3, 4]:
        keep_one_head = True
    if lane_score_map.get(1, -999) >= -0.18:
        keep_one_head = True

    # 2号艇が2着候補として死んでいない条件
    keep_two_second = False
    if second_score.get(2, -999) >= -0.05:
        keep_two_second = True
    if ranks.get(2) in [1, 2, 3, 4]:
        keep_two_second = True

    if not (keep_one_head or keep_two_second):
        return top

    current_has_one_head = any(tri.startswith("1-") for tri in top)
    current_has_two_second = any(tri.split("-")[1] == "2" for tri in top if tri.count("-") == 2)

    candidate_basic = []
    for tri, _score in scored_rows:
        try:
            a, b, _c = [int(x) for x in tri.split("-")]
        except Exception:
            continue

        if keep_one_head and a == 1:
            candidate_basic.append(tri)
        elif keep_two_second and b == 2:
            candidate_basic.append(tri)

    # baseの1頭/2着2号艇も優先候補に混ぜる
    for tri in base_triplets:
        try:
            a, b, _c = [int(x) for x in tri.split("-")]
        except Exception:
            continue
        if (keep_one_head and a == 1) or (keep_two_second and b == 2):
            if tri not in candidate_basic:
                candidate_basic.insert(0, tri)

    if not candidate_basic:
        return top

    need_insert = False
    if keep_one_head and not current_has_one_head:
        need_insert = True
    if keep_two_second and not current_has_two_second:
        need_insert = True

    if not need_insert:
        return top

    chosen = ""
    for tri in candidate_basic:
        if tri not in top:
            chosen = tri
            break

    if not chosen:
        return top

    replaced = top[:]
    replace_idx = len(replaced) - 1

    # 末尾から、1頭でも2着2でもないものを優先して落とす
    for idx in range(len(replaced) - 1, -1, -1):
        tri = replaced[idx]
        try:
            a, b, _c = [int(x) for x in tri.split("-")]
        except Exception:
            continue

        if keep_one_head and a == 1:
            continue
        if keep_two_second and b == 2:
            continue
        replace_idx = idx
        break

    replaced[replace_idx] = chosen

    dedup = []
    for tri in replaced:
        if tri not in dedup:
            dedup.append(tri)
        if len(dedup) >= 6:
            break
    return dedup


def ensure_base_triplets_present(top, scored_rows, base_triplets, min_keep=1):
    if not top or not base_triplets or min_keep <= 0:
        return top

    selected = top[:]
    existing = [tri for tri in selected if tri in base_triplets]
    if len(existing) >= min_keep:
        return selected

    needed = min_keep - len(existing)
    candidate_base = []
    score_map = {tri: score for tri, score in scored_rows}
    for tri in base_triplets:
        if tri not in selected and tri in score_map:
            candidate_base.append((tri, score_map[tri]))
    candidate_base.sort(key=lambda x: (x[1], x[0]), reverse=True)

    for tri, _score in candidate_base[:needed]:
        replace_idx = None
        for idx in range(len(selected) - 1, -1, -1):
            if selected[idx] not in base_triplets:
                replace_idx = idx
                break
        if replace_idx is None:
            break
        selected[replace_idx] = tri

    dedup = []
    for tri in selected:
        if tri not in dedup:
            dedup.append(tri)
        if len(dedup) >= 6:
            break
    return dedup


def extract_lane_text_block(raw_text, lane):
    s = str(raw_text or "")
    if not s:
        return ""
    m = re.search(rf"{int(lane)}:(.*?)(?:\s*/\s*[1-6]:|$)", s)
    return m.group(1).strip() if m else ""


def lane_has_class_support(base_info, lane=1):
    lane_text = extract_lane_text_block((base_info or {}).get("class_history_text"), lane)
    if not lane_text:
        return False
    return ("A1" in lane_text) or (lane_text.count("A2") >= 2)


def lane_has_reason_support(base_info, lane=1):
    lane_text = extract_lane_text_block((base_info or {}).get("player_reason_text"), lane)
    if not lane_text:
        return False
    support_words = ["+モータ", "+級別", "+勝率", "+当地", "+近況", "+ST"]
    return any(word in lane_text for word in support_words)


def should_keep_lane1_support(base_info, exhibition_info, foot_material, role_maps, signal_metrics, base_hold_strength=0.0):
    base_info = base_info or {}
    foot_material = foot_material or {}
    role_maps = role_maps or {}
    signal_metrics = signal_metrics or {}

    course_map = foot_material.get("course_map", {}) or {}
    if course_map.get(1, 1) != 1:
        return False

    class_support = lane_has_class_support(base_info, lane=1)
    reason_support = lane_has_reason_support(base_info, lane=1)
    if not class_support and not reason_support and float(base_hold_strength or 0) < 0.18:
        return False

    lane_map = role_maps.get("lane", {}) or {}
    head_map = role_maps.get("head", {}) or {}
    second_map = role_maps.get("second", {}) or {}
    third_map = role_maps.get("third", {}) or {}

    lane1_eval = float(lane_map.get(1, 0) or 0)
    head1 = float(head_map.get(1, 0) or 0)
    second1 = float(second_map.get(1, 0) or 0)
    third1 = float(third_map.get(1, 0) or 0)

    lane1_gap = float(signal_metrics.get("lane1_time_gap", 0) or 0)
    signal_strength = float(signal_metrics.get("signal_strength", 0) or 0)
    st1 = (foot_material.get("st_map", {}) or {}).get(1)

    if lane1_gap >= 0.18 and not (class_support or reason_support):
        return False
    if lane1_eval <= -0.55 and signal_strength >= 0.70 and float(base_hold_strength or 0) < 0.24:
        return False
    if max(head1, second1, third1) <= -0.12 and float(base_hold_strength or 0) < 0.22:
        return False
    if isinstance(st1, (int, float)) and float(st1) >= 0.30 and lane1_gap >= 0.12:
        return False

    return True


def ensure_lane1_support_triplet(
    top,
    scored_rows,
    base_triplets,
    base_info,
    exhibition_info,
    foot_material,
    role_maps,
    signal_metrics,
    base_hold_strength=0.0,
):
    if not top:
        return top

    if any("1" in tri.split("-") for tri in top if tri.count("-") == 2):
        return top

    if not should_keep_lane1_support(
        base_info,
        exhibition_info,
        foot_material,
        role_maps,
        signal_metrics,
        base_hold_strength=base_hold_strength,
    ):
        return top

    score_map = {tri: score for tri, score in scored_rows}
    candidates = []

    for tri in base_triplets or []:
        parts = tri.split("-")
        if len(parts) != 3:
            continue
        if parts[0] == "1":
            continue
        if "1" in parts and tri in score_map:
            bonus = 0.06 if parts[1] == "1" else 0.04
            candidates.append((tri, score_map[tri] + bonus))

    for tri, score in scored_rows:
        parts = tri.split("-")
        if len(parts) != 3:
            continue
        if parts[0] == "1":
            continue
        if "1" in parts:
            bonus = 0.05 if parts[1] == "1" else 0.03
            candidates.append((tri, score + bonus))

    if not candidates:
        return top

    seen = set()
    ranked = []
    for tri, score in sorted(candidates, key=lambda x: (x[1], x[0]), reverse=True):
        if tri in seen:
            continue
        seen.add(tri)
        ranked.append((tri, score))

    chosen = ""
    for tri, _score in ranked:
        if tri not in top:
            chosen = tri
            break
    if not chosen:
        return top

    replaced = top[:]
    replace_idx = len(replaced) - 1
    for idx in range(len(replaced) - 1, -1, -1):
        tri = replaced[idx]
        parts = tri.split("-")
        if len(parts) != 3:
            continue
        if "1" not in parts:
            replace_idx = idx
            break

    replaced[replace_idx] = chosen

    dedup = []
    for tri in replaced:
        if tri not in dedup:
            dedup.append(tri)
        if len(dedup) >= 6:
            break

    log(f"[lane1_support_keep] chosen={chosen} base_hold={base_hold_strength}")
    return dedup


def generate_top_triplets(
    venue,
    base_selection,
    exhibition_info,
    weather_info=None,
    foot_material=None,
    role_maps=None,
    scenario_material=None,
    base_hold_strength=0.0,
    signal_metrics=None,
    base_info=None,
    official_selection="",
):
    role_maps = role_maps or build_role_score_maps(venue, exhibition_info, weather_info, foot_material)
    scenario_material = scenario_material or build_turn_scenario_material(
        venue, exhibition_info, weather_info, foot_material, role_maps
    )

    lane_score_map = role_maps["lane"]
    head_score = role_maps["head"]
    second_score = role_maps["second"]
    third_score = role_maps["third"]

    signal_metrics = signal_metrics or calculate_latest_signal_metrics(exhibition_info, foot_material)
    signal_strength = float(signal_metrics.get("signal_strength", 0) or 0)
    scenario_factor = scenario_strength_factor(exhibition_info, foot_material, signal_metrics=signal_metrics)

    base_weight_map = parse_selection_weight_map(base_selection)
    base_triplets = selection_triplets(base_selection)

    official_triplets = selection_triplets(official_selection)[:2]
    official_weight_map = {}
    for idx, tri in enumerate(official_triplets):
        official_weight_map[tri] = 1.00 if idx == 0 else 0.82

    lane_ranked = [lane for lane, _ in sorted(lane_score_map.items(), key=lambda x: x[1], reverse=True)]

    base_weight_multiplier = clamp(
        0.52 - signal_strength * 0.20 + float(base_hold_strength or 0) * 0.20,
        0.30,
        0.62,
    )
    scenario_bonus_multiplier = clamp(
        0.68 + signal_strength * 0.28 - float(base_hold_strength or 0) * 0.08,
        0.64,
        0.94,
    )

    # 公式上位2点は「そのまま買う」ではなく、AIスコアの微補正として使う
    official_weight_multiplier = clamp(0.24 + signal_strength * 0.08, 0.22, 0.34)

    lane1_core_keep = should_keep_lane1_head_core(
        base_info,
        exhibition_info,
        foot_material,
        role_maps,
        signal_metrics,
        base_hold_strength=base_hold_strength,
    )

    scored = []
    for a in range(1, 7):
        for b in range(1, 7):
            if b == a:
                continue
            for c in range(1, 7):
                if c == a or c == b:
                    continue

                tri = f"{a}-{b}-{c}"
                score = (
                    head_score.get(a, 0) * 1.12
                    + second_score.get(b, 0) * 0.98
                    + third_score.get(c, 0) * 0.82
                )

                score += (lane_score_map.get(a, 0) - lane_score_map.get(b, 0)) * 0.08

                # 5・6頭は強い根拠がない限り少し抑える
                if is_outer_head_too_loose(a, exhibition_info, foot_material, lane_score_map):
                    score -= 0.16 if a == 5 else 0.22

                # 1号艇が大きく悪くない時は、頭候補として少し守る
                if lane1_core_keep and a == 1:
                    score += 0.08

                if third_score.get(c, 0) < -0.22:
                    score -= 0.10
                elif third_score.get(c, 0) > 0.18:
                    score += 0.04

                score += base_weight_map.get(tri, 0) * base_weight_multiplier
                score += official_weight_map.get(tri, 0) * official_weight_multiplier
                score += scenario_bonus_for_triplet(
                    tri,
                    scenario_material,
                    scenario_factor=scenario_factor,
                ) * scenario_bonus_multiplier

                # 公式上位2点と頭・2着が重なる形は少しだけ上げる
                for otri, ow in official_weight_map.items():
                    try:
                        oa, ob, oc = [int(x) for x in otri.split("-")]
                    except Exception:
                        continue

                    if a == oa:
                        score += 0.018 * ow
                    if b == ob:
                        score += 0.012 * ow
                    if c == oc:
                        score += 0.008 * ow
                    if a == oa and b == ob:
                        score += 0.035 * ow

                if head_score.get(a, 0) < -0.18:
                    score -= 0.18

                if a == lane_ranked[0]:
                    score += 0.05
                elif len(lane_ranked) >= 2 and a == lane_ranked[1]:
                    score += 0.03

                scored.append((tri, round(score, 4)))

    scored.sort(key=lambda x: (x[1], x[0]), reverse=True)

    initial_top = []
    for tri, _score in scored:
        if tri not in initial_top:
            initial_top.append(tri)
        if len(initial_top) >= 6:
            break

    # v10.27: AI6点を「本線3点 + 別頭/相手抜けカバー3点」に再構成。保険側の別筋を優先。
    top = build_core_cover_triplets(
        initial_top,
        scored,
        role_maps,
        scenario_material,
        signal_metrics,
        base_triplets=base_triplets,
        official_triplets=official_triplets,
        base_info=base_info,
        exhibition_info=exhibition_info,
        foot_material=foot_material,
        base_hold_strength=base_hold_strength,
    )

    if base_triplets:
        has_base = any(tri in base_triplets[:3] for tri in top)
        if not has_base:
            best_base = base_triplets[0]
            if best_base not in top:
                top = top[:5] + [best_base]

    head_set = {int(t.split("-")[0]) for t in top if "-" in t}
    scenarios = scenario_material.get("scenarios", [])
    if signal_strength >= 0.30 and len(head_set) == 1 and len(scenarios) >= 2:
        alt_head = scenarios[1].get("head_lane")
        alt_weight = float(scenarios[1].get("weight", 0) or 0)
        if alt_head and alt_weight * scenario_factor >= 0.24:
            alt_tri = pick_best_triplet_for_head(scored, alt_head, exclude_triplets=top)
            if alt_tri and alt_tri not in top:
                top = top[:5] + [alt_tri]

    if signal_strength >= 0.30:
        top = enforce_head_diversity(top, scored, scenario_material, scenario_factor)

    top = add_basic_form_triplets(top, scored, role_maps, exhibition_info, base_triplets=base_triplets)

    top = ensure_lane1_support_triplet(
        top,
        scored,
        base_triplets,
        base_info=base_info,
        exhibition_info=exhibition_info,
        foot_material=foot_material,
        role_maps=role_maps,
        signal_metrics=signal_metrics,
        base_hold_strength=base_hold_strength,
    )

    min_base_keep = 1
    if float(base_hold_strength or 0) >= 0.18 or signal_strength <= 0.28:
        min_base_keep = 2
    if (float(base_hold_strength or 0) >= 0.34 and scenario_factor <= 0.80) or signal_strength <= 0.18:
        min_base_keep = 3

    top = ensure_base_triplets_present(top, scored, base_triplets, min_keep=min_base_keep)

    dedup = []
    for tri in top:
        if tri not in dedup:
            dedup.append(tri)
        if len(dedup) >= 6:
            break

    kept_base_count = len([tri for tri in dedup if tri in base_triplets])
    kept_official_count = len([tri for tri in dedup if tri in official_triplets])

    log(
        f"[selection_regen_v10_27_more_hit_lines] venue={venue} "
        f"scenario={scenario_material.get('scenario_text','')} factor={scenario_factor} hold={base_hold_strength} "
        f"base_keep={kept_base_count}/{len(base_triplets[:6]) if base_triplets else 0} "
        f"official_keep={kept_official_count}/{len(official_triplets)} "
        f"base={base_triplets[:3]} official={official_triplets} final={dedup}"
    )

    return " / ".join(dedup)



def normalize_candidate_source(value):
    s = str(value or "").strip()
    if s == "shadow_ai":
        return "shadow_ai"
    return "official_star"


def make_base_map_source_key(venue, race_no, candidate_source="official_star"):
    return f"{str(venue or '').strip()}|{normalize_race_no_value(race_no)}R|{normalize_candidate_source(candidate_source)}"


def make_base_map_legacy_key(venue, race_no):
    return f"{str(venue or '').strip()}|{normalize_race_no_value(race_no)}R"


def parse_base_map_key(key):
    parts = str(key or "").split("|")
    if len(parts) < 2:
        return "", 0, "official_star", False
    venue = parts[0].strip()
    race_no = normalize_race_no_value(parts[1])
    has_source = len(parts) >= 3
    source = normalize_candidate_source(parts[2] if has_source else "official_star")
    return venue, race_no, source, has_source


def get_base_info_for_source(base_map, venue, race_no, candidate_source="official_star"):
    source = normalize_candidate_source(candidate_source)
    info = base_map.get(make_base_map_source_key(venue, race_no, source))
    if info:
        return info
    # 旧app.py互換。公式候補だけは従来キーも見る。
    if source == "official_star":
        return base_map.get(make_base_map_legacy_key(venue, race_no), {}) or {}
    return {}

def build_candidates():
    log("[collector_version] collector_latest_v10_36_shadow_ai_source")
    log(
        f"[light_mode] ONLY_UPCOMING_HOURS={ONLY_UPCOMING_HOURS} "
        f"SKIP_PAST_RACES={SKIP_PAST_RACES} "
        f"RESULT_LOOKBACK_MINUTES={RESULT_LOOKBACK_MINUTES} "
        f"RESULT_PENDING_LIMIT={RESULT_PENDING_LIMIT} "
        f"RESULT_REPAIR_MODE={RESULT_REPAIR_MODE} "
        f"RESULT_REPAIR_LOOKBACK_MINUTES={RESULT_REPAIR_LOOKBACK_MINUTES}"
    )
    log("========== build_candidates start ==========")
    log(f"now={jst_now().strftime('%Y-%m-%d %H:%M:%S JST')}")

    base_map = fetch_base_map_today()

    # 公式★ページは「公式買い目の参考」として使う。
    # 対象レース自体は base_map に保存済みのものを正とする。
    raw_rows = parse_rating_page()
    official_selection_map = {}
    for row in raw_rows:
        venue = str(row.get("venue") or "").strip()
        race_no = normalize_race_no_value(row.get("race_no"))
        if not venue or race_no <= 0:
            continue
        official_selection_map[(venue, race_no)] = {
            "selection": str(row.get("selection") or "").strip(),
            "rating": str(row.get("rating") or "").strip(),
            "jcd": str(row.get("jcd") or NAME_JCD_MAP.get(venue, "")).strip(),
        }

    # base_map は新app.pyでは venue|race_no|candidate_source を返す。
    # 旧app.py互換で venue|race_no だけの場合も official_star として扱う。
    has_source_keys = any(parse_base_map_key(k)[3] for k in base_map.keys())
    row_map = {}
    legacy_skipped = 0

    for base_key, base_info in (base_map or {}).items():
        venue, race_no, candidate_source, has_source = parse_base_map_key(base_key)
        if not venue or race_no <= 0:
            continue

        # 新app.pyでは公式候補に legacy key も返るので、二重処理しない。
        if has_source_keys and not has_source:
            legacy_skipped += 1
            continue

        jcd = NAME_JCD_MAP.get(venue, "")
        if not jcd:
            continue

        official_info = official_selection_map.get((venue, race_no), {})
        selection_from_rating_page = str(official_info.get("selection") or "").strip()
        if not jcd and official_info.get("jcd"):
            jcd = official_info.get("jcd")

        row_key = (venue, race_no, candidate_source)
        if row_key in row_map:
            continue

        row_map[row_key] = {
            "venue": venue,
            "jcd": jcd,
            "race_no": race_no,
            "candidate_source": candidate_source,
            "selection": selection_from_rating_page,
            "rating": str((base_info or {}).get("rating") or official_info.get("rating") or "").strip(),
            "time": str((base_info or {}).get("time") or "").strip(),
        }

    rows = list(row_map.values())
    official_rows_count = sum(1 for r in rows if normalize_candidate_source(r.get("candidate_source")) == "official_star")
    shadow_rows_count = sum(1 for r in rows if normalize_candidate_source(r.get("candidate_source")) == "shadow_ai")
    log(
        f"[base_source_rows] official={official_rows_count} "
        f"shadow_ai={shadow_rows_count} legacy_skipped={legacy_skipped} total={len(rows)}"
    )

    needed_jcds = set()
    for row in rows:
        jcd = row.get("jcd") or NAME_JCD_MAP.get(row["venue"], "")
        if jcd:
            needed_jcds.add(jcd)

    deadlines_cache = fetch_deadlines_parallel(needed_jcds)
    deadlines_cache = fill_missing_deadlines(rows, deadlines_cache)

    latest_rows = []
    settle_rows = []

    for row in rows:
        venue = row["venue"]
        race_no = int(row["race_no"])
        candidate_source = normalize_candidate_source(row.get("candidate_source"))
        jcd = row.get("jcd") or NAME_JCD_MAP.get(venue, "")
        if not jcd:
            continue

        base_info = get_base_info_for_source(base_map, venue, race_no, candidate_source)
        deadline = (
            str(row.get("time") or "").strip()
            or str((base_info or {}).get("time") or "").strip()
            or deadlines_cache.get(jcd, {}).get(race_no, "")
        )
        row["time"] = deadline

        pending_settle = is_settle_pending(base_info)
        repair_target = RESULT_REPAIR_MODE and is_recent_past_race(
            deadline,
            lookback_minutes=RESULT_REPAIR_LOOKBACK_MINUTES,
        )

        if is_target_deadline(deadline):
            latest_rows.append(row)
        elif (pending_settle and is_recent_past_race(deadline)) or repair_target:
            settle_rows.append(row)

    # 締切後の結果反映は、新しく締切を過ぎたレースから優先する。
    # 同一レースに official_star / shadow_ai がある場合も両方更新できるよう、sourceは保持する。
    settle_rows.sort(key=lambda x: to_minutes(x.get("time") or "00:00"), reverse=True)
    effective_pending_limit = RESULT_REPAIR_LIMIT if RESULT_REPAIR_MODE else RESULT_PENDING_LIMIT
    if len(settle_rows) > effective_pending_limit:
        settle_rows = settle_rows[:effective_pending_limit]

    if settle_rows:
        log(
            "[settle_priority] "
            + ", ".join([
                f"{r.get('venue')}#{r.get('race_no')}@{r.get('time')}[{normalize_candidate_source(r.get('candidate_source'))}]"
                for r in settle_rows
            ])
        )

    rows = latest_rows + settle_rows
    latest_official = sum(1 for r in latest_rows if normalize_candidate_source(r.get("candidate_source")) == "official_star")
    latest_shadow = sum(1 for r in latest_rows if normalize_candidate_source(r.get("candidate_source")) == "shadow_ai")
    settle_official = sum(1 for r in settle_rows if normalize_candidate_source(r.get("candidate_source")) == "official_star")
    settle_shadow = sum(1 for r in settle_rows if normalize_candidate_source(r.get("candidate_source")) == "shadow_ai")
    log(
        f"[target_races_summary] latest={len(latest_rows)}(official={latest_official},shadow={latest_shadow}) "
        f"settle_pending={len(settle_rows)}(official={settle_official},shadow={settle_shadow}) total={len(rows)}"
    )

    live_keys = set()
    settle_keys = set()
    for row in latest_rows:
        venue = row["venue"]
        race_no = int(row["race_no"])
        jcd = row.get("jcd") or NAME_JCD_MAP.get(venue, "")
        if jcd:
            live_keys.add((jcd, race_no))
    for row in settle_rows:
        venue = row["venue"]
        race_no = int(row["race_no"])
        jcd = row.get("jcd") or NAME_JCD_MAP.get(venue, "")
        if jcd:
            settle_keys.add((jcd, race_no))

    # 同じレースに複数sourceがあっても、beforeinfo/result取得はレース単位で1回だけ。
    beforeinfo_keys = set(live_keys) | set(settle_keys)
    beforeinfo_cache = fetch_beforeinfo_parallel(beforeinfo_keys) if beforeinfo_keys else {}

    venue_targets = {}
    for jcd, race_no in settle_keys:
        venue_targets[jcd] = max(venue_targets.get(jcd, 0), int(race_no or 0))
    day_result_cache = fetch_day_results_parallel(venue_targets) if venue_targets else {}

    payout_fallback_keys = []
    for key in sorted(settle_keys):
        info = day_result_cache.get(key) or {}
        # 修復モードでは、すでに払戻が入っていても個別公式結果ページで再確認して上書き候補にする。
        if RESULT_REPAIR_MODE:
            payout_fallback_keys.append(key)
        elif str(info.get("triplet") or "").strip() and int(info.get("trifecta_payout") or 0) <= 0:
            payout_fallback_keys.append(key)

    if payout_fallback_keys:
        log(
            "[raceresult_fallback_targets] "
            + ", ".join([f"{JCD_NAME_MAP.get(jcd, jcd)}{race_no}" for jcd, race_no in payout_fallback_keys])
        )
        result_page_cache = fetch_raceresult_parallel(payout_fallback_keys)
        for key, info in result_page_cache.items():
            merged = dict(day_result_cache.get(key) or {})
            if (RESULT_REPAIR_MODE or not str(merged.get("triplet") or "").strip()) and str(info.get("triplet") or "").strip():
                merged["triplet"] = str(info.get("triplet") or "").strip()
            if (RESULT_REPAIR_MODE or int(merged.get("trifecta_payout") or 0) <= 0) and int(info.get("trifecta_payout") or 0) > 0:
                merged["trifecta_payout"] = int(info.get("trifecta_payout") or 0)
            if (RESULT_REPAIR_MODE or not str(merged.get("kimarite") or "").strip()) and str(info.get("kimarite") or "").strip():
                merged["kimarite"] = str(info.get("kimarite") or "").strip()
            day_result_cache[key] = merged

    results = []
    skipped_no_base = 0

    for row in rows:
        venue = row["venue"]
        race_no = int(row["race_no"])
        candidate_source = normalize_candidate_source(row.get("candidate_source"))
        selection_from_rating_page = row.get("selection", "")
        jcd = row.get("jcd") or NAME_JCD_MAP.get(venue, "")
        deadline = row.get("time", "")

        base_info = get_base_info_for_source(base_map, venue, race_no, candidate_source)

        if not base_info:
            skipped_no_base += 1
            log(f"[skip_no_base] key={make_base_map_source_key(venue, race_no, candidate_source)}")
            continue

        result_info = day_result_cache.get((jcd, race_no), {}) if (jcd, race_no) in settle_keys else {}
        is_live_target = (jcd, race_no) in live_keys

        if not is_live_target:
            result_text = str(result_info.get("triplet") or "").strip()
            result_payout = int(result_info.get("trifecta_payout") or 0)
            log(
                f"[settle_candidate] source={candidate_source} "
                f"venue={venue} race_no={race_no} result_text={result_text} payout={result_payout}"
            )
            if not result_text and result_payout <= 0:
                log(f"[settle_skip_empty] source={candidate_source} venue={venue} race_no={race_no}")
                continue

            # 結果だけを送ると app 側の保存処理によって、
            # 展示タイム/展示順位/風/波などの表示が空で上書きされることがある。
            beforeinfo = beforeinfo_cache.get((jcd, race_no), {})
            exhibition_info = beforeinfo.get("exhibition", {"times": [], "ranks": {}})
            weather_info = beforeinfo.get("weather", {})
            start_info = beforeinfo.get("start_info", {"st_map": {}})
            foot_material = build_foot_material(exhibition_info, start_info, weather_info)
            ai_lane_score_text = build_lane_score_text(exhibition_info, weather_info, foot_material)

            candidate = {
                "race_date": today_text(),
                "venue": venue,
                "race_no": f"{race_no}R",
                "candidate_source": candidate_source,
                "time": deadline,
                "exhibition": exhibition_info.get("times", []),
                "exhibition_rank": exhibition_rank_text_from_map(exhibition_info.get("ranks", {})),
                "weather": str(weather_info.get("weather") or "").strip(),
                "wind_speed": weather_info.get("wind_speed"),
                "wave_height": weather_info.get("wave_height"),
                "wind_type": str(weather_info.get("wind_type") or "").strip(),
                "wind_dir": str(weather_info.get("wind_dir") or "").strip(),
                "water_state_score": float(weather_info.get("water_state_score") or 0),
                "ai_lane_score_text": ai_lane_score_text,
                "result_trifecta_text": result_text,
                "result_trifecta_payout": result_payout,
                "result_source_url": build_resultlist_url(jcd),
                "latest_updated_at": jst_now_str(),
            }
            results.append(candidate)
            continue

        base_ai_score = float(base_info.get("base_ai_score", 0) or 0)
        base_ai_selection = str(base_info.get("base_ai_selection") or "").strip() or selection_from_rating_page
        base_reason_text = str(base_info.get("base_reason_text") or "").strip()
        base_hold_strength = calc_base_hold_strength(base_info)
        phase_material = build_phase_material(base_info, race_no=race_no)
        series_day = int(phase_material.get("series_day") or 0)
        race_phase = normalize_race_phase_label(phase_material.get("race_phase") or "")

        beforeinfo = beforeinfo_cache.get((jcd, race_no), {})
        exhibition_info = beforeinfo.get("exhibition", {"times": [], "ranks": {}})
        weather_info = beforeinfo.get("weather", {})
        start_info = beforeinfo.get("start_info", {"st_map": {}})

        foot_material = build_foot_material(exhibition_info, start_info, weather_info)
        signal_metrics = calculate_latest_signal_metrics(exhibition_info, foot_material)
        analyzed = analyze_latest(
            base_ai_score,
            exhibition_info,
            weather_info,
            foot_material,
            signal_metrics=signal_metrics,
        )
        scenario_factor = scenario_strength_factor(
            exhibition_info,
            foot_material,
            signal_metrics=signal_metrics,
        )

        day_trend_bias = build_day_trend_bias(jcd, race_no, day_result_cache)
        role_maps = build_role_score_maps(
            venue,
            exhibition_info,
            weather_info,
            foot_material,
            day_trend_bias=day_trend_bias,
        )
        role_maps = apply_phase_material_to_role_maps(role_maps, phase_material=phase_material)
        scenario_material = build_turn_scenario_material(
            venue,
            exhibition_info,
            weather_info,
            foot_material,
            role_maps,
            day_trend_bias=day_trend_bias,
        )

        latest_reason_parts = []
        if candidate_source == "shadow_ai":
            latest_reason_parts.append("裏AI候補")
        if base_reason_text:
            latest_reason_parts.append(f"朝:{base_reason_text}")
        if analyzed["latest_reason_text"]:
            latest_reason_parts.append(f"直前:{analyzed['latest_reason_text']}")
        if signal_metrics.get("signal_text"):
            latest_reason_parts.append(signal_metrics["signal_text"])
        if base_hold_strength >= 0.18 and float(signal_metrics.get("signal_strength", 0) or 0) < 0.35:
            latest_reason_parts.append("朝評価をやや優先")
        if scenario_material.get("scenario_text"):
            latest_reason_parts.append(f"隊形:{scenario_material['scenario_text']}")
        if phase_material.get("reason_text"):
            latest_reason_parts.append(f"開催:{phase_material['reason_text']}")

        final_ai_selection = generate_top_triplets(
            venue,
            base_ai_selection,
            exhibition_info,
            weather_info,
            foot_material,
            role_maps=role_maps,
            scenario_material=scenario_material,
            base_hold_strength=base_hold_strength,
            signal_metrics=signal_metrics,
            base_info=base_info,
            official_selection=selection_from_rating_page,
        )
        final_ai_score = stabilize_final_ai_score(
            base_ai_score,
            analyzed["raw_final_ai_score"],
            base_hold_strength,
            scenario_factor,
            signal_metrics=signal_metrics,
        )
        final_ai_score = round(final_ai_score + float(phase_material.get("score_adjust", 0) or 0), 2)
        ai_lane_score_text = build_lane_score_text(exhibition_info, weather_info, foot_material)

        candidate = {
            "race_date": today_text(),
            "venue": venue,
            "race_no": f"{race_no}R",
            "candidate_source": candidate_source,
            "time": deadline,
            "exhibition": exhibition_info.get("times", []),
            "exhibition_rank": exhibition_rank_text_from_map(exhibition_info.get("ranks", {})),
            "weather": str(weather_info.get("weather") or "").strip(),
            "wind_speed": weather_info.get("wind_speed"),
            "wave_height": weather_info.get("wave_height"),
            "wind_type": str(weather_info.get("wind_type") or "").strip(),
            "wind_dir": str(weather_info.get("wind_dir") or "").strip(),
            "water_state_score": float(weather_info.get("water_state_score") or 0),
            "day_trend_text": "",
            "day_trend_sample": 0,
            "series_day": series_day,
            "race_phase": race_phase,
            "ai_lane_score_text": ai_lane_score_text,
            "rating": str(base_info.get("rating") or row.get("rating") or ""),
            "final_ai_score": final_ai_score,
            "final_ai_rating": score_to_ai_rating(final_ai_score),
            "final_ai_selection": final_ai_selection,
            "final_rank": determine_final_rank(
                base_info,
                final_ai_score,
                signal_metrics=signal_metrics,
                foot_material=foot_material,
                role_maps=role_maps,
                scenario_material=scenario_material,
                base_hold_strength=base_hold_strength,
                phase_material=phase_material,
            ),
            "latest_reason_text": " / ".join(latest_reason_parts[:10]),
            "latest_updated_at": jst_now_str(),
        }
        results.append(candidate)

    results.sort(
        key=lambda x: (
            to_minutes(x["time"]) if x["time"] else 9999,
            x["venue"],
            int(str(x["race_no"]).replace("R", "")),
            normalize_candidate_source(x.get("candidate_source")),
        )
    )

    final_official = sum(1 for x in results if normalize_candidate_source(x.get("candidate_source")) == "official_star")
    final_shadow = sum(1 for x in results if normalize_candidate_source(x.get("candidate_source")) == "shadow_ai")
    log(f"[skip_no_base_summary] count={skipped_no_base}")
    log(f"build_candidates final_count={len(results)} official={final_official} shadow_ai={final_shadow}")
    log("========== build_candidates end ==========")
    return results

def send_to_render(races):
    if not RENDER_IMPORT_URL:
        raise RuntimeError("RENDER_IMPORT_URL が未設定です")
    if not IMPORT_TOKEN:
        raise RuntimeError("IMPORT_TOKEN が未設定です")

    headers = {"Content-Type": "application/json", "X-IMPORT-TOKEN": IMPORT_TOKEN}
    payload = {"races": races}

    last_err = None
    for attempt in range(1, 6):
        try:
            log(f"[render_post_try] attempt={attempt} url={RENDER_IMPORT_URL} races={len(races)}")
            health_url = RENDER_IMPORT_URL.replace("/api/import_latest_candidates", "/healthz")
            try:
                hr = requests.get(health_url, headers=HEADERS, timeout=(5, 10))
                log(f"[render_health] attempt={attempt} status={hr.status_code}")
            except Exception as he:
                log(f"[render_health_err] attempt={attempt} err={he}")

            res = requests.post(RENDER_IMPORT_URL, headers=headers, json=payload, timeout=POST_TIMEOUT)
            print("status_code =", res.status_code)
            print("response =", res.text)

            if res.status_code in (502, 503, 504):
                raise requests.exceptions.HTTPError(
                    f"{res.status_code} Server Error: {res.text}",
                    response=res,
                )

            res.raise_for_status()
            log("[render_post_ok]")
            return
        except Exception as e:
            last_err = e
            log(f"[render_post_retry] attempt={attempt} err={e}")
            if attempt < 5:
                time.sleep(8 * attempt)
            else:
                log(f"[render_post_failed] err={e}")

    raise last_err


def main():
    races = build_candidates()
    if not races:
        print("候補が0件でした")
        return
    send_to_render(races)


if __name__ == "__main__":
    main()
