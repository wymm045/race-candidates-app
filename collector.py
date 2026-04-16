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

    weather_words = ["晴", "曇", "雨", "雪"]
    for word in weather_words:
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

    s = s.replace("ＳＴ", "").replace("ST", "").strip()
    s = s.replace(" ", "")

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


def build_generated_triplets_from_lane_scores(lane_score_map):
    sorted_lanes = [lane for lane, _ in sorted(lane_score_map.items(), key=lambda x: x[1], reverse=True)]
    if len(sorted_lanes) < 4:
        return []

    a, b, c, d = sorted_lanes[:4]

    candidates = [
        normalize_triplet(a, b, c),
        normalize_triplet(a, b, d),
        normalize_triplet(a, c, b),
        normalize_triplet(a, c, d),
        normalize_triplet(b, a, c),
        normalize_triplet(b, a, d),
        normalize_triplet(a, d, b),
        normalize_triplet(c, a, b),
    ]

    out = []
    for tri in candidates:
        if tri and tri not in out:
            out.append(tri)
    return out


def inject_promoted_triplets(triplets, lane_score_map):
    if not lane_score_map or len(lane_score_map) < 3:
        return triplets[:6]

    ranked = sorted(lane_score_map.items(), key=lambda x: x[1], reverse=True)
    top_lane, top_score = ranked[0]
    second_lane, second_score = ranked[1]

    include_count = 0
    head_count = 0
    for tri in triplets:
        parts = tri.split("-")
        if len(parts) != 3:
            continue
        try:
            a, b, c = map(int, parts)
        except Exception:
            continue
        if top_lane in (a, b, c):
            include_count += 1
        if a == top_lane:
            head_count += 1

    if top_score < 0.55:
        return triplets[:6]

    if (top_score - second_score) < 0.18:
        return triplets[:6]

    generated = build_generated_triplets_from_lane_scores(lane_score_map)

    promoted = []
    for tri in generated:
        parts = tri.split("-")
        if len(parts) != 3:
            continue
        try:
            a, b, c = map(int, parts)
        except Exception:
            continue
        if a == top_lane or b == top_lane:
            promoted.append(tri)

    new_list = list(triplets)

    if include_count == 0:
        for tri in promoted[:2]:
            if tri not in new_list:
                new_list.insert(0, tri)
    elif head_count == 0:
        for tri in promoted[:1]:
            if tri not in new_list:
                new_list.insert(0, tri)

    dedup = []
    for tri in new_list:
        if tri not in dedup:
            dedup.append(tri)
        if len(dedup) >= 6:
            break
    return dedup


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

        top3 = sorted(ranks.items(), key=lambda x: x[1])[:3]
        if top3:
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
        if water_state_score > 0:
            reasons.append("気象安定")
        else:
            reasons.append("気象荒れ気味")
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


def rebuild_final_selection(base_selection, exhibition_info, weather_info=None, foot_material=None):
    triplets = selection_triplets(base_selection)
    if not triplets:
        return ""

    ranks = exhibition_info.get("ranks", {}) if exhibition_info else {}
    foot_material = foot_material or {}
    foot_lane_scores = foot_material.get("lane_scores", {}) or {}

    lane_score_map = compute_lane_scores_map(exhibition_info, weather_info, foot_material)
    triplets = inject_promoted_triplets(triplets, lane_score_map)

    if not ranks and not foot_lane_scores:
        return " / ".join(triplets[:6])

    def triplet_score(tri):
        parts = tri.split("-")
        if len(parts) != 3:
            return -999
        try:
            a, b, c = map(int, parts)
        except Exception:
            return -999

        rank_score = (
            -(ranks.get(a, 9) * 1.3 + ranks.get(b, 9) * 0.9 + ranks.get(c, 9) * 0.6)
            if ranks else 0
        )
        foot_score = (
            float(foot_lane_scores.get(a, 0) or 0) * 1.2
            + float(foot_lane_scores.get(b, 0) or 0) * 0.8
            + float(foot_lane_scores.get(c, 0) or 0) * 0.5
        )
        lane_total_score = (
            float(lane_score_map.get(a, 0) or 0) * 1.0
            + float(lane_score_map.get(b, 0) or 0) * 0.7
            + float(lane_score_map.get(c, 0) or 0) * 0.45
        )
        return rank_score + foot_score + lane_total_score

    triplets = sorted(triplets, key=lambda tri: (triplet_score(tri), tri), reverse=True)

    dedup = []
    for tri in triplets:
        if tri not in dedup:
            dedup.append(tri)
        if len(dedup) >= 6:
            break
    return " / ".join(dedup)


def build_candidates():
    log("[collector_version] collector_latest_weather_v5_promote_top_lane")
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

        latest_reason_parts = []
        if base_reason_text:
            latest_reason_parts.append(f"朝:{base_reason_text}")
        if analyzed["latest_reason_text"]:
            latest_reason_parts.append(f"直前:{analyzed['latest_reason_text']}")

        final_ai_selection = rebuild_final_selection(
            base_ai_selection,
            exhibition_info,
            weather_info,
            foot_material,
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
