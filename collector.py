from datetime import datetime, timezone, timedelta
import os
import re
import time
from urllib.parse import urlencode
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

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

OFFICIAL_MAX_WORKERS = 4
BEFOREINFO_MAX_WORKERS = 6

ONLY_UPCOMING_HOURS = int(os.environ.get("ONLY_UPCOMING_HOURS", "6"))
SKIP_PAST_RACES = os.environ.get("SKIP_PAST_RACES", "1").strip() == "1"

JCD_NAME_MAP = {
    "01": "桐生", "02": "戸田", "03": "江戸川", "04": "平和島", "05": "多摩川",
    "06": "浜名湖", "07": "蒲郡", "08": "常滑", "09": "津", "10": "三国",
    "11": "びわこ", "12": "住之江", "13": "尼崎", "14": "鳴門", "15": "丸亀",
    "16": "児島", "17": "宮島", "20": "若松", "21": "芦屋", "22": "福岡",
    "23": "唐津", "24": "大村",
}
NAME_JCD_MAP = {v: k for k, v in JCD_NAME_MAP.items()}
RATING_PAGE_MAP = {"★★★★★": "s5"}


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
    text = soup.get_text("\n")
    lines = [line.strip() for line in text.splitlines()]
    return [line for line in lines if line]


def build_official_url(jcd, race_no=1):
    return f"https://boatrace.jp/owpc/pc/race/pcexpect?rno={race_no}&jcd={jcd}&hd={today_str()}"


def build_beforeinfo_url(jcd, race_no):
    qs = urlencode({"hd": today_str(), "jcd": jcd, "rno": race_no})
    return f"https://boatrace.jp/owpc/pc/race/beforeinfo?{qs}"


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
            "selection": " / ".join(triplets[:6]),
        })
    dedup = {}
    for r in rows:
        key = (r["venue"], r["race_no"])
        if key not in dedup:
            dedup[key] = r
    return list(dedup.values())


def parse_rating_page():
    rows = parse_rating_page_dom("★★★★★")
    log(f"[rating_page_summary] count={len(rows)}")
    return rows


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


def parse_weather_info_from_lines(lines):
    joined = " ".join(lines)
    weather = {
        "weather": "",
        "wind_speed": None,
        "wave_height": None,
        "wind_type": "",
        "water_state_score": 0.0,
    }

    for word in ["晴", "曇", "雨", "雪"]:
        if word in joined:
            weather["weather"] = word
            break

    m_wind = re.search(r"風速\s*([0-9]+(?:\.[0-9]+)?)", joined)
    if m_wind:
        try:
            weather["wind_speed"] = float(m_wind.group(1))
        except Exception:
            pass

    m_wave = re.search(r"波高\s*([0-9]+(?:\.[0-9]+)?)", joined)
    if m_wave:
        try:
            weather["wave_height"] = float(m_wave.group(1))
        except Exception:
            pass

    if "向い風" in joined:
        weather["wind_type"] = "向い風"
    elif "追い風" in joined:
        weather["wind_type"] = "追い風"
    elif "横風" in joined:
        weather["wind_type"] = "横風"

    wind = weather["wind_speed"]
    wave = weather["wave_height"]
    score = 0.0
    if isinstance(wind, (int, float)):
        if wind >= 7:
            score -= 0.18
        elif wind >= 5:
            score -= 0.10
        elif wind <= 2:
            score += 0.04
    if isinstance(wave, (int, float)):
        if wave >= 7:
            score -= 0.18
        elif wave >= 5:
            score -= 0.10
        elif wave <= 2:
            score += 0.04
    if weather["wind_type"] == "向い風":
        score -= 0.04
    elif weather["wind_type"] == "追い風":
        score += 0.03
    weather["water_state_score"] = round(score, 2)
    return weather


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


def parse_start_info_from_lines(lines):
    st_map = {}

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

    return {"st_map": st_map}


def build_foot_material(exhibition_info, start_info, weather_info=None):
    weather_info = weather_info or {}
    times = exhibition_info.get("times", []) if exhibition_info else []
    ranks = exhibition_info.get("ranks", {}) if exhibition_info else {}
    st_map = start_info.get("st_map", {}) if start_info else {}

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

    foot_bonus = round(foot_bonus, 2)
    reason_text = " / ".join((reasons + top_lane_reasons)[:3])

    return {
        "lane_scores": lane_scores,
        "foot_bonus": foot_bonus,
        "reason_text": reason_text,
        "st_map": st_map,
    }


def parse_beforeinfo_for_key(jcd, race_no):
    race_no = normalize_race_no_value(race_no)
    beforeinfo_url = build_beforeinfo_url(jcd, race_no)
    empty = {
        "exhibition": {"times": [], "ranks": {}},
        "weather": {},
        "start_info": {"st_map": {}},
    }
    try:
        html = fetch_html(beforeinfo_url)
    except Exception as e:
        log(f"[beforeinfo_error] jcd={jcd} race_no={race_no} err={e}")
        return (jcd, race_no), empty

    soup = BeautifulSoup(html, "html.parser")
    lines = normalize_lines(html)
    times = extract_exhibition_times_from_table(soup)
    if len(times) != 6:
        times = extract_exhibition_times_from_lines(lines)
    ranks = build_exhibition_ranks_from_times(times)
    weather = parse_weather_info_from_lines(lines)
    start_info = parse_start_info_from_lines(lines)

    return (
        (jcd, race_no),
        {
            "exhibition": {"times": times, "ranks": ranks},
            "weather": weather,
            "start_info": start_info,
        },
    )


def fetch_beforeinfo_parallel(keys):
    results = {}
    with ThreadPoolExecutor(max_workers=BEFOREINFO_MAX_WORKERS) as ex:
        futures = [ex.submit(parse_beforeinfo_for_key, jcd, race_no) for (jcd, race_no) in sorted(keys)]
        for future in as_completed(futures):
            key, info = future.result()
            results[key] = info
    return results


def score_to_ai_rating(score):
    if score >= 2.0:
        return "AI★★★★★"
    if score >= 1.2:
        return "AI★★★★☆"
    if score >= 0.5:
        return "AI★★★☆☆"
    if score >= -0.2:
        return "AI★★☆☆☆"
    return "AI★☆☆☆☆"


def score_to_final_rank(score):
    if score >= 2.0:
        return "買い強め"
    if score >= 1.0:
        return "買い"
    if score >= 0:
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


def build_venue_bias_map(venue):
    """
    latest 側では会場傾向を持たせない。
    会場の土台評価は collector_base.py 側で反映し、
    ここでは展示・ST・風波など直前要素だけで補正する。
    """
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

    if ranks:
        for lane in range(1, 7):
            rank = ranks.get(lane)
            if rank is None:
                continue
            if rank == 1:
                lane_scores[lane] += 0.40
            elif rank == 2:
                lane_scores[lane] += 0.22
            elif rank == 3:
                lane_scores[lane] += 0.08
            elif rank == 4:
                lane_scores[lane] -= 0.04
            elif rank == 5:
                lane_scores[lane] -= 0.14
            elif rank == 6:
                lane_scores[lane] -= 0.24

        r1 = ranks.get(1)
        if r1 == 1:
            lane_scores[1] += 0.20
        elif r1 is not None and r1 >= 5:
            lane_scores[1] -= 0.20

    float_times = []
    for lane, t in enumerate(times, start=1):
        try:
            float_times.append((lane, float(t)))
        except Exception:
            pass

    if len(float_times) == 6:
        min_time = min(v for _, v in float_times)
        avg_time = sum(v for _, v in float_times) / 6.0
        for lane, v in float_times:
            diff_from_min = v - min_time
            diff_from_avg = v - avg_time
            if diff_from_min <= 0.00:
                lane_scores[lane] += 0.26
            elif diff_from_min <= 0.03:
                lane_scores[lane] += 0.12
            elif diff_from_min >= 0.10:
                lane_scores[lane] -= 0.22
            elif diff_from_min >= 0.06:
                lane_scores[lane] -= 0.10

            if diff_from_avg <= -0.05:
                lane_scores[lane] += 0.08
            elif diff_from_avg >= 0.05:
                lane_scores[lane] -= 0.08

        spread = max(v for _, v in float_times) - min_time
        if spread >= 0.12:
            fastest_lane = min(float_times, key=lambda x: x[1])[0]
            lane_scores[fastest_lane] += 0.06

    water_state_score = float(weather_info.get("water_state_score") or 0)
    if water_state_score != 0:
        lane_scores[1] += water_state_score * 0.8
        for lane in [4, 5, 6]:
            lane_scores[lane] -= water_state_score * 0.25

    foot_lane_scores = foot_material.get("lane_scores", {})
    if foot_lane_scores:
        for lane in range(1, 7):
            lane_scores[lane] += float(foot_lane_scores.get(lane, 0) or 0) * 0.90

    return lane_scores


def build_lane_score_text(exhibition_info, weather_info=None, foot_material=None):
    lane_scores = compute_lane_scores_map(exhibition_info, weather_info, foot_material)
    return " / ".join([f"{lane}:{lane_scores[lane]:.2f}" for lane in range(1, 7)])


def analyze_latest(base_ai_score, exhibition_info, weather_info=None, foot_material=None):
    score = float(base_ai_score or 0)
    reasons = []
    weather_info = weather_info or {}
    foot_material = foot_material or {}

    ranks = exhibition_info.get("ranks", {}) if exhibition_info else {}
    times = exhibition_info.get("times", []) if exhibition_info else []

    if ranks:
        if 1 in ranks:
            r1 = ranks[1]
            if r1 == 1:
                score += 0.8
                reasons.append("1号艇の展示順位が1位")
            elif r1 <= 2:
                score += 0.4
                reasons.append("1号艇の展示順位が上位")
            elif r1 >= 5:
                score -= 0.6
                reasons.append("1号艇の展示順位が下位")
        if sorted(ranks.items(), key=lambda x: x[1])[:3]:
            reasons.append("展示上位を反映")

    if times:
        float_times = []
        for t in times:
            try:
                float_times.append(float(t))
            except Exception:
                pass
        if len(float_times) == 6:
            spread = max(float_times) - min(float_times)
            if spread >= 0.18:
                score += 0.25
                reasons.append("展示差あり")
            elif spread >= 0.12:
                score += 0.12
                reasons.append("展示差ややあり")

    wind = weather_info.get("wind_speed")
    wave = weather_info.get("wave_height")
    water_state_score = float(weather_info.get("water_state_score") or 0)
    if water_state_score != 0:
        score += water_state_score
        reasons.append("気象安定" if water_state_score > 0 else "気象荒れ気味")
    if weather_info.get("wind_type") == "向い風":
        reasons.append("向い風")
    elif weather_info.get("wind_type") == "追い風":
        reasons.append("追い風")
    if isinstance(wind, (int, float)) and wind >= 7:
        reasons.append(f"風速{wind:g}m")
    if isinstance(wave, (int, float)) and wave >= 5:
        reasons.append(f"波高{wave:g}cm")

    foot_bonus = float(foot_material.get("foot_bonus", 0) or 0)
    if foot_bonus != 0:
        score += foot_bonus

    foot_reason_text = str(foot_material.get("reason_text") or "").strip()
    if foot_reason_text:
        reasons.extend([x.strip() for x in foot_reason_text.split(" / ") if x.strip()])

    return {
        "final_ai_score": round(score, 2),
        "final_ai_rating": score_to_ai_rating(score),
        "latest_reason_text": " / ".join(reasons[:8]),
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


def build_role_score_maps(venue, exhibition_info, weather_info=None, foot_material=None):
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

    venue_bias = build_venue_bias_map(venue)
    for lane in range(1, 7):
        head_score[lane] += float(venue_bias["head"].get(lane, 0) or 0)
        second_score[lane] += float(venue_bias["second"].get(lane, 0) or 0)
        third_score[lane] += float(venue_bias["third"].get(lane, 0) or 0)

    if ranks:
        for lane in range(1, 7):
            rank = ranks.get(lane)
            if rank is None:
                continue
            if rank == 1:
                head_score[lane] += 0.24
                second_score[lane] += 0.12
            elif rank == 2:
                head_score[lane] += 0.14
                second_score[lane] += 0.14
                third_score[lane] += 0.05
            elif rank == 3:
                head_score[lane] += 0.06
                second_score[lane] += 0.08
                third_score[lane] += 0.08
            elif rank == 4:
                third_score[lane] += 0.05
            elif rank == 5:
                head_score[lane] -= 0.10
                second_score[lane] -= 0.04
            elif rank == 6:
                head_score[lane] -= 0.18
                second_score[lane] -= 0.08
                third_score[lane] -= 0.03

        # v10.2: 1が完全に死んでない時は少し残す
        rank1 = ranks.get(1)
        if rank1 is not None:
            if rank1 <= 3:
                head_score[1] += 0.08
                second_score[1] += 0.03
            elif rank1 == 4:
                head_score[1] += 0.02

        # v10.2: 2号艇は2着の基本形を少し戻す
        rank2 = ranks.get(2)
        if rank2 is not None:
            if rank2 <= 3:
                second_score[2] += 0.08
            elif rank2 == 4:
                second_score[2] += 0.03

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

        for lane, v in float_times:
            diff_min = v - min_time
            diff_avg = v - avg_time

            if diff_min <= 0.00:
                head_score[lane] += 0.16
                second_score[lane] += 0.08
            elif diff_min <= 0.03:
                head_score[lane] += 0.08
                second_score[lane] += 0.06
                third_score[lane] += 0.02
            elif diff_min >= 0.10:
                head_score[lane] -= 0.14
                second_score[lane] -= 0.06
            elif diff_min >= 0.06:
                head_score[lane] -= 0.06
                second_score[lane] -= 0.03

            if diff_avg <= -0.04:
                third_score[lane] += 0.04
            elif diff_avg >= 0.05:
                third_score[lane] -= 0.04

        if spread >= 0.12:
            fastest_lane = min(float_times, key=lambda x: x[1])[0]
            head_score[fastest_lane] += 0.05

        # v10.2: 1が展示で大崩れしていなければ少し保険
        lane1_time = next((v for lane, v in float_times if lane == 1), None)
        if lane1_time is not None:
            diff1 = lane1_time - min_time
            if diff1 <= 0.05:
                head_score[1] += 0.05
            elif diff1 <= 0.08:
                head_score[1] += 0.02

        lane2_time = next((v for lane, v in float_times if lane == 2), None)
        if lane2_time is not None:
            diff2 = lane2_time - min_time
            if diff2 <= 0.05:
                second_score[2] += 0.05
            elif diff2 <= 0.08:
                second_score[2] += 0.02

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


def build_turn_scenario_material(venue, exhibition_info, weather_info=None, foot_material=None, role_maps=None):
    weather_info = weather_info or {}
    foot_material = foot_material or {}
    role_maps = role_maps or build_role_score_maps(venue, exhibition_info, weather_info, foot_material)

    head_score = role_maps["head"]
    second_score = role_maps["second"]
    third_score = role_maps["third"]
    lane_score_map = role_maps["lane"]
    venue_notes = role_maps.get("venue_notes", [])

    ranks = exhibition_info.get("ranks", {}) if exhibition_info else {}
    st_map = foot_material.get("st_map", {}) or {}

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
    if "若松やや波乱" in venue_notes:
        weight_center += 0.04
    if "江戸川外警戒" in venue_notes:
        weight_center += 0.05
    weight_center = clamp(weight_center, 0.0, 1.0)

    if weight_center >= 0.24:
        if center_lane == 3:
            second_pref = [1, 2, 4, 5, 6]
            third_pref = [2, 1, 4, 5, 6]
            name = "3攻め注意"
        else:
            second_pref = [2, 3, 1, 5, 6]
            third_pref = [3, 2, 1, 5, 6]
            name = "4攻め注意"

        scenarios.append({
            "name": name,
            "head_lane": center_lane,
            "weight": weight_center,
            "head_bonus": build_pref_bonus_map(
                [center_lane, 2, 1, 5],
                [0.25, 0.03, 0.02, 0.02],
            ),
            "second_bonus": build_pref_bonus_map(
                second_pref[:5],
                [0.13, 0.11, 0.07, 0.04, 0.02],
            ),
            "third_bonus": build_pref_bonus_map(
                third_pref[:5],
                [0.10, 0.09, 0.08, 0.05, 0.03],
            ),
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
    if "江戸川外警戒" in venue_notes:
        weight_outer += 0.12
    if "若松やや波乱" in venue_notes:
        weight_outer += 0.06
    if "大村イン寄り" in venue_notes:
        weight_outer -= 0.08
    weight_outer = clamp(weight_outer, 0.0, 1.0)

    if weight_outer >= 0.24:
        if outer_lane == 5:
            second_pref = [6, 1, 2, 4, 3]
            third_pref = [1, 6, 2, 3, 4]
            name = "5一撃注意"
        else:
            second_pref = [5, 1, 2, 3, 4]
            third_pref = [1, 5, 2, 3, 4]
            name = "6一撃注意"

        scenarios.append({
            "name": name,
            "head_lane": outer_lane,
            "weight": weight_outer,
            "head_bonus": build_pref_bonus_map(
                [outer_lane, 5 if outer_lane == 6 else 6, 1],
                [0.24, 0.04, 0.02],
            ),
            "second_bonus": build_pref_bonus_map(
                second_pref[:5],
                [0.14, 0.12, 0.09, 0.06, 0.03],
            ),
            "third_bonus": build_pref_bonus_map(
                third_pref[:5],
                [0.11, 0.10, 0.08, 0.05, 0.03],
            ),
        })

    scenarios = sorted(scenarios, key=lambda x: x["weight"], reverse=True)[:4]
    scenario_text = " / ".join([f"{s['name']}" for s in scenarios[:2]])

    return {
        "scenarios": scenarios,
        "scenario_text": scenario_text,
        "best_head_lane": best_head,
        "head_ranked": head_ranked,
        "second_ranked": second_ranked,
        "lane_ranked": lane_ranked,
    }


def scenario_strength_factor(exhibition_info, foot_material=None):
    foot_material = foot_material or {}
    times = exhibition_info.get("times", []) if exhibition_info else []
    ranks = exhibition_info.get("ranks", {}) if exhibition_info else {}
    st_map = foot_material.get("st_map", {}) or {}

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


def generate_top_triplets(
    venue,
    base_selection,
    exhibition_info,
    weather_info=None,
    foot_material=None,
    role_maps=None,
    scenario_material=None,
):
    role_maps = role_maps or build_role_score_maps(venue, exhibition_info, weather_info, foot_material)
    scenario_material = scenario_material or build_turn_scenario_material(
        venue, exhibition_info, weather_info, foot_material, role_maps
    )

    lane_score_map = role_maps["lane"]
    head_score = role_maps["head"]
    second_score = role_maps["second"]
    third_score = role_maps["third"]

    scenario_factor = scenario_strength_factor(exhibition_info, foot_material)

    base_weight_map = parse_selection_weight_map(base_selection)
    base_triplets = selection_triplets(base_selection)
    lane_ranked = [lane for lane, _ in sorted(lane_score_map.items(), key=lambda x: x[1], reverse=True)]

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

                if third_score.get(c, 0) < -0.22:
                    score -= 0.10
                elif third_score.get(c, 0) > 0.18:
                    score += 0.04

                score += base_weight_map.get(tri, 0) * 0.32
                score += scenario_bonus_for_triplet(tri, scenario_material, scenario_factor=scenario_factor)

                if head_score.get(a, 0) < -0.18:
                    score -= 0.18

                if a == lane_ranked[0]:
                    score += 0.05
                elif a == lane_ranked[1]:
                    score += 0.03

                scored.append((tri, round(score, 4)))

    scored.sort(key=lambda x: (x[1], x[0]), reverse=True)

    top = []
    for tri, _score in scored:
        if tri not in top:
            top.append(tri)
        if len(top) >= 6:
            break

    if base_triplets:
        has_base = any(tri in base_triplets[:3] for tri in top)
        if not has_base:
            best_base = base_triplets[0]
            if best_base not in top:
                top = top[:5] + [best_base]

    head_set = {int(t.split("-")[0]) for t in top if "-" in t}
    scenarios = scenario_material.get("scenarios", [])
    if len(head_set) == 1 and len(scenarios) >= 2:
        alt_head = scenarios[1].get("head_lane")
        alt_weight = float(scenarios[1].get("weight", 0) or 0)
        if alt_head and alt_weight * scenario_factor >= 0.24:
            alt_tri = pick_best_triplet_for_head(scored, alt_head, exclude_triplets=top)
            if alt_tri and alt_tri not in top:
                top = top[:5] + [alt_tri]

    top = enforce_head_diversity(top, scored, scenario_material, scenario_factor)
    top = add_basic_form_triplets(top, scored, role_maps, exhibition_info, base_triplets=base_triplets)

    dedup = []
    for tri in top:
        if tri not in dedup:
            dedup.append(tri)
        if len(dedup) >= 6:
            break

    log(
        f"[selection_regen_v10_2] venue={venue} "
        f"scenario={scenario_material.get('scenario_text','')} factor={scenario_factor} "
        f"base={base_triplets[:3]} final={dedup}"
    )
    return " / ".join(dedup)


def build_candidates():
    log("[collector_version] collector_latest_weather_v10_3_no_latest_venue_bias")
    log(f"[light_mode] ONLY_UPCOMING_HOURS={ONLY_UPCOMING_HOURS} SKIP_PAST_RACES={SKIP_PAST_RACES}")
    log("========== build_candidates start ==========")
    log(f"now={jst_now().strftime('%Y-%m-%d %H:%M:%S JST')}")

    base_map = fetch_base_map_today()
    raw_rows = parse_rating_page()

    dedup = {}
    for row in raw_rows:
        key = (row["venue"], row["race_no"])
        if key not in dedup:
            dedup[key] = row
    rows = list(dedup.values())
    log(f"[dedup_summary] count={len(rows)}")

    needed_jcds = set()
    for row in rows:
        jcd = row["jcd"] or NAME_JCD_MAP.get(row["venue"], "")
        if jcd:
            needed_jcds.add(jcd)

    deadlines_cache = fetch_deadlines_parallel(needed_jcds)
    deadlines_cache = fill_missing_deadlines(rows, deadlines_cache)

    filtered_rows = []
    for row in rows:
        venue = row["venue"]
        race_no = row["race_no"]
        jcd = row["jcd"] or NAME_JCD_MAP.get(venue, "")
        if not jcd:
            continue
        deadline = deadlines_cache.get(jcd, {}).get(race_no, "")
        row["time"] = deadline
        if is_target_deadline(deadline):
            filtered_rows.append(row)

    rows = filtered_rows
    log(f"[target_races_summary] count={len(rows)}")

    all_keys = set()
    for row in rows:
        venue = row["venue"]
        race_no = row["race_no"]
        jcd = row["jcd"] or NAME_JCD_MAP.get(venue, "")
        if jcd:
            all_keys.add((jcd, race_no))

    beforeinfo_cache = fetch_beforeinfo_parallel(all_keys) if all_keys else {}

    results = []
    skipped_no_base = 0

    for row in rows:
        venue = row["venue"]
        race_no = row["race_no"]
        selection_from_rating_page = row.get("selection", "")
        jcd = row["jcd"] or NAME_JCD_MAP.get(venue, "")
        deadline = row.get("time", "")

        race_key = f"{venue}|{race_no}R"
        base_info = base_map.get(race_key)

        if not base_info:
            skipped_no_base += 1
            log(f"[skip_no_base] key={race_key}")
            continue

        base_ai_score = float(base_info.get("base_ai_score", 0) or 0)
        base_ai_selection = str(base_info.get("base_ai_selection") or "").strip() or selection_from_rating_page
        base_reason_text = str(base_info.get("base_reason_text") or "").strip()

        beforeinfo = beforeinfo_cache.get((jcd, race_no), {})
        exhibition_info = beforeinfo.get("exhibition", {"times": [], "ranks": {}})
        weather_info = beforeinfo.get("weather", {})
        start_info = beforeinfo.get("start_info", {"st_map": {}})

        foot_material = build_foot_material(exhibition_info, start_info, weather_info)
        analyzed = analyze_latest(base_ai_score, exhibition_info, weather_info, foot_material)

        role_maps = build_role_score_maps(venue, exhibition_info, weather_info, foot_material)
        scenario_material = build_turn_scenario_material(
            venue,
            exhibition_info,
            weather_info,
            foot_material,
            role_maps,
        )

        latest_reason_parts = []
        if base_reason_text:
            latest_reason_parts.append(f"朝:{base_reason_text}")
        if analyzed["latest_reason_text"]:
            latest_reason_parts.append(f"直前:{analyzed['latest_reason_text']}")
        if scenario_material.get("scenario_text"):
            latest_reason_parts.append(f"隊形:{scenario_material['scenario_text']}")

        final_ai_selection = generate_top_triplets(
            venue,
            base_ai_selection,
            exhibition_info,
            weather_info,
            foot_material,
            role_maps=role_maps,
            scenario_material=scenario_material,
        )
        final_ai_score = analyzed["final_ai_score"]
        ai_lane_score_text = build_lane_score_text(exhibition_info, weather_info, foot_material)

        candidate = {
            "race_date": today_text(),
            "venue": venue,
            "race_no": f"{race_no}R",
            "time": deadline,
            "exhibition": exhibition_info.get("times", []),
            "exhibition_rank": exhibition_rank_text_from_map(exhibition_info.get("ranks", {})),
            "ai_lane_score_text": ai_lane_score_text,
            "final_ai_score": final_ai_score,
            "final_ai_rating": score_to_ai_rating(final_ai_score),
            "final_ai_selection": final_ai_selection,
            "final_rank": score_to_final_rank(final_ai_score),
            "latest_reason_text": " / ".join(latest_reason_parts[:10]),
            "latest_updated_at": jst_now_str(),
        }
        results.append(candidate)

    results.sort(
        key=lambda x: (
            to_minutes(x["time"]) if x["time"] else 9999,
            x["venue"],
            int(str(x["race_no"]).replace("R", "")),
        )
    )

    log(f"[skip_no_base_summary] count={skipped_no_base}")
    log(f"build_candidates final_count={len(results)}")
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
