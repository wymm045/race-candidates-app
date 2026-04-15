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

TILT_ALERT_ENABLED = os.environ.get("TILT_ALERT_ENABLED", "1").strip() == "1"
TILT_ALERT_OUTER_LANES = set(int(x) for x in os.environ.get("TILT_ALERT_OUTER_LANES", "3,4,5,6").split(",") if x.strip().isdigit())
TILT_ALERT_RANK_BORDER = int(os.environ.get("TILT_ALERT_RANK_BORDER", "2"))
TILT_ALERT_INNER_DIFF = float(os.environ.get("TILT_ALERT_INNER_DIFF", "0.06"))
TILT_ALERT_STRONG_BONUS = float(os.environ.get("TILT_ALERT_STRONG_BONUS", "0.34"))
TILT_ALERT_MID_BONUS = float(os.environ.get("TILT_ALERT_MID_BONUS", "0.18"))



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


def parse_beforeinfo_for_key(jcd, race_no):
    race_no = normalize_race_no_value(race_no)
    beforeinfo_url = build_beforeinfo_url(jcd, race_no)
    empty = {"exhibition": {"times": [], "ranks": {}}}
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
    return (jcd, race_no), {"exhibition": {"times": times, "ranks": ranks}}


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




def detect_tilt_jump_alert(exhibition_info):
    result = {
        "level": "none",
        "target_lane": None,
        "bonus": 0.0,
        "reasons": [],
    }
    if not TILT_ALERT_ENABLED:
        return result

    ranks = exhibition_info.get("ranks", {}) if exhibition_info else {}
    times = exhibition_info.get("times", []) if exhibition_info else []
    if not ranks or len(times) != 6:
        return result

    float_times = {}
    for lane, t in enumerate(times, start=1):
        try:
            float_times[lane] = float(t)
        except Exception:
            return result

    candidates = []
    for lane in sorted(TILT_ALERT_OUTER_LANES):
        if lane not in float_times:
            continue
        rank = ranks.get(lane, 9)
        inner_lanes = [x for x in range(1, lane) if x in float_times]
        if not inner_lanes:
            continue
        inner_best = min(float_times[x] for x in inner_lanes)
        diff = round(inner_best - float_times[lane], 3)
        score = 0.0
        reasons = []
        if rank <= TILT_ALERT_RANK_BORDER:
            score += 1.0
            reasons.append(f"{lane}号艇の展示順位が上位")
        if diff >= TILT_ALERT_INNER_DIFF:
            score += 1.0
            reasons.append(f"{lane}号艇が内側より展示良化")
        elif diff >= max(TILT_ALERT_INNER_DIFF - 0.02, 0.03):
            score += 0.5
            reasons.append(f"{lane}号艇が内側よりやや展示良化")
        if rank == 1 and diff >= TILT_ALERT_INNER_DIFF:
            score += 0.5
            reasons.append(f"{lane}号艇の外攻め気配")
        if score > 0:
            candidates.append((score, lane, diff, reasons))

    if not candidates:
        return result

    candidates.sort(key=lambda x: (-x[0], -x[2], x[1]))
    score, lane, diff, reasons = candidates[0]
    result["target_lane"] = lane
    result["reasons"] = reasons[:3]
    if score >= 2.0:
        result["level"] = "strong"
        result["bonus"] = TILT_ALERT_STRONG_BONUS
    else:
        result["level"] = "mid"
        result["bonus"] = TILT_ALERT_MID_BONUS
    return result


def rebuild_final_selection_with_tilt(base_selection, exhibition_info, tilt_result):
    triplets = selection_triplets(base_selection)
    if not triplets:
        return ""

    target_lane = tilt_result.get("target_lane")
    if not target_lane:
        return rebuild_final_selection(base_selection, exhibition_info)

    ranks = exhibition_info.get("ranks", {}) if exhibition_info else {}

    def tri_score(tri):
        parts = tri.split("-")
        if len(parts) != 3:
            return -999
        try:
            a, b, c = map(int, parts)
        except Exception:
            return -999
        score = 0.0
        if a == target_lane:
            score += 2.3
        elif b == target_lane:
            score += 1.0
        elif c == target_lane:
            score += 0.45
        score -= ranks.get(a, 9) * 1.25
        score -= ranks.get(b, 9) * 0.85
        score -= ranks.get(c, 9) * 0.55
        return score

    sorted_tris = sorted(triplets, key=lambda tri: (tri_score(tri), tri), reverse=True)
    dedup = []
    for tri in sorted_tris:
        if tri not in dedup:
            dedup.append(tri)
        if len(dedup) >= 6:
            break
    return " / ".join(dedup)

def analyze_latest(base_ai_score, exhibition_info):
    score = float(base_ai_score or 0)
    reasons = []

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

    return {
        "final_ai_score": round(score, 2),
        "final_ai_rating": score_to_ai_rating(score),
        "latest_reason_text": " / ".join(reasons[:6]),
    }


def selection_triplets(selection):
    if not selection:
        return []
    return [x.strip() for x in str(selection).split(" / ") if x.strip()]


def rebuild_final_selection(base_selection, exhibition_info):
    triplets = selection_triplets(base_selection)
    if not triplets:
        return ""

    ranks = exhibition_info.get("ranks", {}) if exhibition_info else {}
    if not ranks:
        return " / ".join(triplets[:6])

    def triplet_score(tri):
        parts = tri.split("-")
        if len(parts) != 3:
            return -999
        try:
            a, b, c = map(int, parts)
        except Exception:
            return -999
        return -(ranks.get(a, 9) * 1.3 + ranks.get(b, 9) * 0.9 + ranks.get(c, 9) * 0.6)

    triplets = sorted(triplets, key=lambda tri: (triplet_score(tri), tri), reverse=True)
    dedup = []
    for tri in triplets:
        if tri not in dedup:
            dedup.append(tri)
        if len(dedup) >= 6:
            break
    return " / ".join(dedup)


def build_candidates():
    log("[collector_version] collector_latest_tilt_v3")
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

        analyzed = analyze_latest(base_ai_score, exhibition_info)
        tilt_result = detect_tilt_jump_alert(exhibition_info)

        final_ai_score = analyzed["final_ai_score"] + float(tilt_result.get("bonus", 0) or 0)
        final_ai_score = round(final_ai_score, 2)

        latest_reason_parts = []
        if base_reason_text:
            latest_reason_parts.append(f"朝:{base_reason_text}")
        if analyzed["latest_reason_text"]:
            latest_reason_parts.append(f"直前:{analyzed['latest_reason_text']}")
        if tilt_result.get("level") == "strong":
            latest_reason_parts.append(f"チルト警戒:{tilt_result['target_lane']}号艇外攻め強め")
        elif tilt_result.get("level") == "mid":
            latest_reason_parts.append(f"チルト警戒:{tilt_result['target_lane']}号艇外攻め注意")

        final_ai_selection = rebuild_final_selection_with_tilt(base_ai_selection, exhibition_info, tilt_result)

        candidate = {
            "race_date": today_text(),
            "venue": venue,
            "race_no": f"{race_no}R",
            "time": deadline,
            "exhibition": exhibition_info.get("times", []),
            "exhibition_rank": exhibition_rank_text_from_map(exhibition_info.get("ranks", {})),
            "final_ai_score": final_ai_score,
            "final_ai_rating": score_to_ai_rating(final_ai_score),
            "final_ai_selection": final_ai_selection,
            "final_rank": score_to_final_rank(final_ai_score),
            "latest_reason_text": " / ".join(latest_reason_parts[:8]),
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
