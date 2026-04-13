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
    "race-token-2026",
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

REQUEST_TIMEOUT = (10, 20)
POST_TIMEOUT = 40
MAX_RETRIES = 3
RETRY_SLEEP_SEC = 1.2

OFFICIAL_MAX_WORKERS = 6
BEFOREINFO_MAX_WORKERS = 12
RACELIST_MAX_WORKERS = 6
USE_RACELIST = True

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

RACELIST_VENUE_SLUG_MAP = {
    "01": "kiryu",
    "03": "edogawa",
    "04": "heiwajima",
    "06": "hamanako",
    "14": "naruto",
    "15": "marugame",
    "16": "kojima",
    "17": "miyajima",
    "22": "fukuoka",
    "23": "karatsu",
    "24": "omura",
}

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


def today_text_dashless():
    return jst_now().strftime("%Y%m%d")


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


def fetch_html(url, timeout=REQUEST_TIMEOUT, max_retries=MAX_RETRIES):
    last_err = None

    for attempt in range(1, max_retries + 1):
        try:
            res = SESSION.get(url, timeout=timeout)
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


def build_racelist_url(jcd):
    slug = RACELIST_VENUE_SLUG_MAP.get(jcd)
    if not slug:
        return ""
    return f"https://kyotei.sakura.ne.jp/racelist-{slug}-{today_text_dashless()}.html"


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


def parse_single_race_deadline(jcd, race_no):
    url = build_official_url(jcd, race_no=race_no)
    try:
        html = fetch_html(url)
    except Exception as e:
        log(f"[official_single_error] jcd={jcd} race_no={race_no} err={e}")
        return ""

    lines = normalize_lines(html)

    for i, line in enumerate(lines):
        if "締切予定時刻" in line:
            block = " ".join(lines[i:i + 30])
            times = re.findall(r"\d{2}:\d{2}", block)
            if times:
                return times[0]

    all_times = re.findall(r"\b\d{2}:\d{2}\b", " ".join(lines))
    if all_times:
        return all_times[0]

    return ""


def parse_official_deadlines_for_jcd(jcd):
    official_url = build_official_url(jcd, race_no=1)
    venue = JCD_NAME_MAP.get(jcd, jcd)

    try:
        html = fetch_html(official_url)
    except Exception as e:
        log(f"[official_deadlines_error] jcd={jcd} venue={venue} err={e}")
        return jcd, {}

    deadlines = parse_official_deadlines_from_html(html)
    if deadlines:
        log(f"[official_deadlines_ok] jcd={jcd} venue={venue} count={len(deadlines)}")
    else:
        log(f"[official_deadlines_empty] jcd={jcd} venue={venue}")

    return jcd, deadlines


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


def normalize_weather_text(text):
    if not text:
        return ""
    text = text.strip()
    if "晴" in text:
        return "晴"
    if "曇" in text:
        return "曇"
    if "雨" in text:
        return "雨"
    if "雪" in text:
        return "雪"
    return text


def attrs_text(el):
    vals = []
    for key in ("alt", "title", "aria-label", "data-label"):
        v = el.get(key)
        if v:
            vals.append(str(v))
    return " ".join(vals)


def detect_wind_direction_from_text(text):
    if not text:
        return ""

    checks = [
        "向かい風",
        "追い風",
        "左横風",
        "右横風",
        "横風",
        "左追い風",
        "右追い風",
        "左向かい風",
        "右向かい風",
    ]
    for k in checks:
        if k in text:
            return k
    return ""


def classify_wind_type(direction_text):
    if not direction_text:
        return ""

    if "向かい風" in direction_text:
        return "headwind"
    if "追い風" in direction_text:
        return "tailwind"
    if "横風" in direction_text:
        return "crosswind"
    return ""


def parse_environment_info(html, soup, lines):
    joined = " ".join(lines)
    env = {
        "weather": "",
        "wind_speed": None,
        "wave_height": None,
        "water_temp": None,
        "air_temp": None,
        "wind_direction": "",
        "wind_type": "",
        "stabilizer": False,
    }

    m_air = re.search(r"気温\s*([0-9]+(?:\.[0-9]+)?)℃", joined)
    if m_air:
        env["air_temp"] = float(m_air.group(1))

    m_weather = re.search(r"(晴れ?|曇り?|雨|雪)", joined)
    if m_weather:
        env["weather"] = normalize_weather_text(m_weather.group(1))

    m_wind = re.search(r"風速\s*([0-9]+(?:\.[0-9]+)?)m", joined)
    if m_wind:
        env["wind_speed"] = float(m_wind.group(1))

    m_water = re.search(r"水温\s*([0-9]+(?:\.[0-9]+)?)℃", joined)
    if m_water:
        env["water_temp"] = float(m_water.group(1))

    m_wave = re.search(r"波高\s*([0-9]+(?:\.[0-9]+)?)cm", joined)
    if m_wave:
        env["wave_height"] = float(m_wave.group(1))

    if "安定板使用" in joined or "安定板" in joined:
        env["stabilizer"] = True

    direction = detect_wind_direction_from_text(joined)

    if not direction:
        for el in soup.find_all(True):
            meta = attrs_text(el)
            direction = detect_wind_direction_from_text(meta)
            if direction:
                break

    if not direction:
        for el in soup.find_all("img"):
            src = (el.get("src") or "") + " " + (el.get("data-src") or "")
            direction = detect_wind_direction_from_text(src)
            if direction:
                break

    env["wind_direction"] = direction
    env["wind_type"] = classify_wind_type(direction)

    return env


def summarize_environment_for_log(env):
    parts = []
    if env.get("weather"):
        parts.append(f"weather={env['weather']}")
    if env.get("wind_speed") is not None:
        parts.append(f"wind={env['wind_speed']}m")
    if env.get("wind_direction"):
        parts.append(f"dir={env['wind_direction']}")
    else:
        parts.append("dir=-")
    if env.get("wave_height") is not None:
        parts.append(f"wave={env['wave_height']}cm")
    if env.get("stabilizer"):
        parts.append("stb=Y")
    else:
        parts.append("stb=N")
    return " ".join(parts)


def parse_beforeinfo_for_key(jcd, race_no):
    beforeinfo_url = build_beforeinfo_url(jcd, race_no)

    try:
        html = fetch_html(beforeinfo_url)
    except Exception as e:
        log(f"[beforeinfo_error] jcd={jcd} race_no={race_no} err={e}")
        return (jcd, race_no), {
            "exhibition": {"times": [], "ranks": {}},
            "boat_stats": {},
            "environment": {
                "weather": "",
                "wind_speed": None,
                "wave_height": None,
                "water_temp": None,
                "air_temp": None,
                "wind_direction": "",
                "wind_type": "",
                "stabilizer": False,
            },
        }

    soup = BeautifulSoup(html, "html.parser")
    lines = normalize_lines(html)
    environment = parse_environment_info(html, soup, lines)

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

        win_like = [x for x in nums if 0 <= x <= 10]
        rate_like = [x for x in nums if 0 <= x <= 100]

        if len(win_like) >= 1 and stats[lane]["national_win"] is None:
            stats[lane]["national_win"] = win_like[0]
        if len(win_like) >= 2 and stats[lane]["local_win"] is None:
            stats[lane]["local_win"] = win_like[1]

        if len(rate_like) >= 2:
            stats[lane]["motor2"] = rate_like[-2]
            stats[lane]["boat2"] = rate_like[-1]
        elif len(rate_like) == 1:
            stats[lane]["motor2"] = rate_like[-1]

    for lane in range(1, 7):
        s = stats[lane]
        if s["national_win"] is not None and s["national_win"] > 10:
            s["national_win"] = None
        if s["local_win"] is not None and s["local_win"] > 10:
            s["local_win"] = None
        if s["motor2"] is not None and s["motor2"] > 100:
            s["motor2"] = None
        if s["boat2"] is not None and s["boat2"] > 100:
            s["boat2"] = None

    log(
        f"[beforeinfo_env] jcd={jcd} race_no={race_no} "
        f"times={len(times)} ranks={len(ranks)} {summarize_environment_for_log(environment)}"
    )

    return (jcd, race_no), {
        "exhibition": {"times": times, "ranks": ranks},
        "boat_stats": stats,
        "environment": environment,
    }


def parse_racelist_page_all_races(jcd):
    url = build_racelist_url(jcd)
    venue = JCD_NAME_MAP.get(jcd, jcd)

    if not url:
        return {}

    try:
        html = fetch_html(url)
    except Exception as e:
        log(f"[racelist_page_error] jcd={jcd} venue={venue} err={e}")
        return {}

    lines = normalize_lines(html)
    result = {}

    for race_no in range(1, 13):
        lane_map = parse_racelist_race_from_lines(lines, race_no, jcd, venue)
        if lane_map:
            result[race_no] = lane_map

    log(f"[racelist_summary] jcd={jcd} venue={venue} races={len(result)}")
    return result


def parse_racelist_race_from_lines(lines, race_no, jcd, venue):
    race_label = f"{race_no} R"

    race_idx = None
    for i, line in enumerate(lines):
        if str(line).strip() == race_label:
            race_idx = i
            break

    if race_idx is None:
        log(f"[racelist_race_not_found] jcd={jcd} venue={venue} race_no={race_no}")
        return {}

    next_race_idx = len(lines)
    for i in range(race_idx + 1, len(lines)):
        s = str(lines[i]).strip()
        if re.fullmatch(r"(1[0-2]|[1-9]) R", s):
            next_race_idx = i
            break

    block = lines[race_idx:next_race_idx]
    joined = " | ".join(block)

    if "級" not in joined or "(過去3期)" not in joined:
        log(f"[racelist_race_no_grade_block] jcd={jcd} venue={venue} race_no={race_no}")
        return {}

    grade_idx = None
    for i, line in enumerate(block):
        if str(line).strip() == "級":
            grade_idx = i
            break

    if grade_idx is None:
        log(f"[racelist_race_grade_missing] jcd={jcd} venue={venue} race_no={race_no}")
        return {}

    tokens = []
    stop_words = {
        "能力(前期)",
        "今期 F|L数",
        "全国 勝率",
        "2連対率",
        "3連対率",
        "(6ヶ月)",
        "当地 勝率",
    }

    for s in block[grade_idx + 1:]:
        s = str(s).strip()

        if s in stop_words:
            break

        if s == "(過去3期)":
            continue

        if re.fullmatch(r"(A1|A2|B1|B2)", s):
            tokens.append(("current", s))
            continue

        if re.fullmatch(r"(A1|A2|B1|B2|-)\s+(A1|A2|B1|B2|-)\s+(A1|A2|B1|B2|-)", s):
            tokens.append(("history", s))
            continue

    if len(tokens) < 12:
        log(
            f"[racelist_race_short] jcd={jcd} venue={venue} race_no={race_no} "
            f"tokens={len(tokens)}"
        )
        return {}

    lane_map = {}
    lane = 1
    i = 0

    while i + 1 < len(tokens) and lane <= 6:
        kind1, val1 = tokens[i]
        kind2, val2 = tokens[i + 1]

        if kind1 == "current" and kind2 == "history":
            h_parts = val2.split()
            prev1 = h_parts[0] if len(h_parts) >= 1 and h_parts[0] != "-" else ""
            prev2 = h_parts[1] if len(h_parts) >= 2 and h_parts[1] != "-" else ""
            prev3 = h_parts[2] if len(h_parts) >= 3 and h_parts[2] != "-" else ""

            lane_map[lane] = {
                "current_class": val1,
                "prev1_class": prev1,
                "prev2_class": prev2,
                "prev3_class": prev3,
            }
            lane += 1
            i += 2
        else:
            i += 1

    if len(lane_map) != 6:
        log(
            f"[racelist_race_lane_short] jcd={jcd} venue={venue} race_no={race_no} "
            f"lanes={len(lane_map)}"
        )
        return {}

    log(
        f"[racelist_race_ok] jcd={jcd} venue={venue} race_no={race_no} "
        f"sample={lane_map.get(1, {})}"
    )
    return lane_map


def parse_racelist_for_jcd(jcd):
    venue = JCD_NAME_MAP.get(jcd, jcd)

    if jcd not in RACELIST_VENUE_SLUG_MAP:
        log(f"[racelist_skip] jcd={jcd} venue={venue}")
        return jcd, {}

    result = parse_racelist_page_all_races(jcd)
    return jcd, result


def normalize_triplet(a, b, c):
    if a == b or a == c or b == c:
        return ""
    return f"{a}-{b}-{c}"


def selection_triplets(selection):
    if not selection:
        return []
    return [x.strip() for x in str(selection).split(" / ") if x.strip()]


def avg_stat(stats_list, key):
    vals = []
    for s in stats_list:
        v = s.get(key)
        if isinstance(v, (int, float)):
            vals.append(float(v))
    if not vals:
        return None
    return sum(vals) / len(vals)


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


def decide_final_rank(official_rating, ai_score):
    if official_rating == "★★★★★" and ai_score >= 1.2:
        return "買い強め"
    if official_rating == "★★★★★" and ai_score >= 0.5:
        return "買い"
    if ai_score >= -0.2:
        return "様子見"
    return "見送り寄り"


def class_point(cls):
    if cls == "A1":
        return 1.00
    if cls == "A2":
        return 0.45
    if cls == "B1":
        return 0.00
    if cls == "B2":
        return -0.60
    return 0.0


def class_history_score(class_history):
    cur = class_history.get("current_class", "")
    prev1 = class_history.get("prev1_class", "")
    prev2 = class_history.get("prev2_class", "")
    prev3 = class_history.get("prev3_class", "")

    score = 0.0
    score += class_point(cur) * 1.0
    score += class_point(prev1) * 0.7
    score += class_point(prev2) * 0.5
    score += class_point(prev3) * 0.35

    pattern = [cur, prev1, prev2, prev3]

    if pattern[:3] == ["A1", "A1", "A1"]:
        score += 0.55
    if pattern[:4] == ["A1", "A1", "A1", "A1"]:
        score += 0.20

    if cur == "A1" and prev1 in {"A2", "B1"}:
        score += 0.12
    if cur == "A2" and prev1 == "B1":
        score += 0.08
    if cur in {"B1", "B2"} and prev1 == "A1":
        score -= 0.12
    if cur == "B2":
        score -= 0.18

    return round(score, 3)


def make_class_history_text(class_history):
    cur = class_history.get("current_class", "")
    prev1 = class_history.get("prev1_class", "")
    prev2 = class_history.get("prev2_class", "")
    prev3 = class_history.get("prev3_class", "")
    parts = [x for x in [cur, prev1, prev2, prev3] if x]
    return " / ".join(parts)


def generate_lane_ai_scores(exhibition_info, boat_stats, environment, class_history_map):
    scores = {lane: 0.0 for lane in range(1, 7)}
    exhibition_times = exhibition_info.get("times", [])
    exhibition_ranks = exhibition_info.get("ranks", {})

    lane_time_map = {}
    for lane, t in enumerate(exhibition_times, start=1):
        try:
            lane_time_map[lane] = float(t)
        except Exception:
            pass

    for lane in range(1, 7):
        s = boat_stats.get(lane, {})
        ch = class_history_map.get(lane, {})

        national = s.get("national_win")
        local = s.get("local_win")
        motor2 = s.get("motor2")
        boat2 = s.get("boat2")
        cls = s.get("class") or ch.get("current_class") or ""

        if national is not None:
            scores[lane] += (national - 5.3) * 0.36
        if local is not None:
            scores[lane] += (local - 5.1) * 0.20
        if motor2 is not None:
            scores[lane] += (motor2 - 34.0) * 0.022
        if boat2 is not None:
            scores[lane] += (boat2 - 33.0) * 0.010

        scores[lane] += class_history_score(
            {
                "current_class": cls,
                "prev1_class": ch.get("prev1_class", ""),
                "prev2_class": ch.get("prev2_class", ""),
                "prev3_class": ch.get("prev3_class", ""),
            }
        )

        if lane in exhibition_ranks:
            r = exhibition_ranks[lane]
            if r == 1:
                scores[lane] += 0.70
            elif r == 2:
                scores[lane] += 0.45
            elif r == 3:
                scores[lane] += 0.20
            elif r >= 5:
                scores[lane] -= 0.30

        if lane in lane_time_map:
            all_times = list(lane_time_map.values())
            top_time = min(all_times)
            gap = lane_time_map[lane] - top_time
            if gap <= 0.00:
                scores[lane] += 0.35
            elif gap <= 0.03:
                scores[lane] += 0.18
            elif gap >= 0.10:
                scores[lane] -= 0.12

        if lane == 1:
            scores[lane] += 0.22
        elif lane == 2:
            scores[lane] += 0.08
        elif lane >= 5:
            scores[lane] -= 0.06

    env = environment or {}
    wind_speed = env.get("wind_speed")
    wave_height = env.get("wave_height")
    wind_type = env.get("wind_type") or ""
    stabilizer = bool(env.get("stabilizer"))

    if wind_speed is not None:
        if wind_speed >= 4 and wind_type == "headwind":
            scores[1] += 0.28
            scores[2] += 0.08
            scores[4] -= 0.08
            scores[5] -= 0.12
            scores[6] -= 0.15
        elif wind_speed >= 4 and wind_type == "tailwind":
            scores[4] += 0.10
            scores[5] += 0.16
            scores[6] += 0.12
        elif wind_speed >= 4 and wind_type == "crosswind":
            scores[4] -= 0.08
            scores[5] -= 0.10
            scores[6] -= 0.12

        if wind_speed >= 6:
            scores[1] += 0.12
            scores[5] -= 0.10
            scores[6] -= 0.15

    if wave_height is not None:
        if wave_height >= 5:
            scores[1] += 0.10
            scores[4] -= 0.05
            scores[5] -= 0.08
            scores[6] -= 0.10
        if wave_height >= 7:
            scores[1] += 0.08
            scores[5] -= 0.08
            scores[6] -= 0.12

    if stabilizer:
        scores[1] += 0.15
        scores[2] += 0.05
        scores[4] -= 0.08
        scores[5] -= 0.12
        scores[6] -= 0.14

    return scores


def generate_ai_selection(exhibition_info, boat_stats, environment, class_history_map):
    exhibition_times = exhibition_info.get("times", []) or []
    exhibition_ranks = exhibition_info.get("ranks", {}) or {}

    has_exhibition_times = len(exhibition_times) == 6
    has_exhibition_ranks = len(exhibition_ranks) == 6

    if not has_exhibition_times and not has_exhibition_ranks:
        return {
            "ai_selection": "",
            "ai_confidence": "",
            "ai_lane_scores": {},
            "ai_lane_score_text": "",
        }

    lane_scores = generate_lane_ai_scores(
        exhibition_info,
        boat_stats,
        environment,
        class_history_map,
    )

    sorted_lanes = sorted(lane_scores.items(), key=lambda x: (-x[1], x[0]))
    top_lanes = [lane for lane, _ in sorted_lanes]

    first_candidates = top_lanes[:3]
    second_candidates = top_lanes[:4]
    third_candidates = top_lanes[:5]

    triplets = []
    scored_triplets = []

    for a in first_candidates:
        for b in second_candidates:
            for c in third_candidates:
                if len({a, b, c}) < 3:
                    continue
                score = lane_scores[a] * 1.25 + lane_scores[b] * 0.90 + lane_scores[c] * 0.60
                scored_triplets.append((score, normalize_triplet(str(a), str(b), str(c))))

    scored_triplets.sort(key=lambda x: (-x[0], x[1]))

    for score, tri in scored_triplets:
        if tri and tri not in triplets:
            triplets.append(tri)
        if len(triplets) >= 6:
            break

    top_score = sorted_lanes[0][1] if sorted_lanes else 0.0
    second_score = sorted_lanes[1][1] if len(sorted_lanes) >= 2 else top_score
    confidence_gap = round(top_score - second_score, 2)

    if confidence_gap >= 0.8:
        confidence = "A"
    elif confidence_gap >= 0.35:
        confidence = "B"
    else:
        confidence = "C"

    lane_score_text = " / ".join([f"{lane}:{round(score, 2)}" for lane, score in sorted_lanes])

    return {
        "ai_selection": " / ".join(triplets[:6]),
        "ai_confidence": confidence,
        "ai_lane_scores": lane_scores,
        "ai_lane_score_text": lane_score_text,
    }


def extract_digits_from_cell(cell):
    digits = []

    for el in cell.find_all(True):
        txt = el.get_text(strip=True)
        if re.fullmatch(r"[1-6]", txt):
            digits.append(txt)

    if len(digits) < 18:
        full_text = cell.get_text(" ", strip=True)
        for d in re.findall(r"\b([1-6])\b", full_text):
            digits.append(d)

    cleaned = []
    for d in digits:
        if d in {"1", "2", "3", "4", "5", "6"}:
            cleaned.append(d)
    return cleaned


def triplets_from_digit_sequence(digits):
    triplets = []
    for i in range(0, len(digits) - 2, 3):
        a, b, c = digits[i], digits[i + 1], digits[i + 2]
        t = normalize_triplet(a, b, c)
        if not t:
            continue
        if t not in triplets:
            triplets.append(t)
        if len(triplets) >= 6:
            break
    return triplets


def parse_race_identity_from_text(text):
    m = re.search(r"(\d{2})\s*([^\s]+)\s*(\d{1,2})R", text)
    if not m:
        return None
    return {
        "jcd": m.group(1),
        "venue": m.group(2).strip(),
        "race_no": int(m.group(3)),
    }


def row_cells(tr):
    return tr.find_all(["td", "th"], recursive=False)


def parse_rating_page_dom(rating_text):
    page = RATING_PAGE_MAP[rating_text]
    url = f"{BASE_URL}/{page}/{today_str()}.html"

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

        race_cell = cells[header_idx["race"]]
        trifecta_cell = cells[header_idx["trifecta"]]

        race_text = race_cell.get_text(" ", strip=True)
        info = parse_race_identity_from_text(race_text)
        if not info:
            row_text = tr.get_text(" ", strip=True)
            info = parse_race_identity_from_text(row_text)

        if not info:
            continue

        digits = extract_digits_from_cell(trifecta_cell)
        triplets = triplets_from_digit_sequence(digits)

        if len(triplets) < 6:
            continue

        rows.append(
            {
                "venue": info["venue"],
                "jcd": info["jcd"],
                "race_no": info["race_no"],
                "rating": rating_text,
                "selection": " / ".join(triplets[:6]),
            }
        )

    dedup = {}
    for r in rows:
        key = (r["venue"], r["race_no"])
        if key not in dedup:
            dedup[key] = r

    rows = list(dedup.values())
    log(f"[rating_page_summary_dom] {rating_text} count={len(rows)}")
    return rows


def extract_triplets_from_digit_lines(lines, start_idx):
    digits = []
    for i in range(start_idx, min(start_idx + 220, len(lines))):
        if re.fullmatch(r"[1-6]", lines[i]):
            digits.append(lines[i])

    if len(digits) >= 26:
        digits = digits[8:26]

    return triplets_from_digit_sequence(digits)


def parse_triplets_from_lines(lines, start_idx):
    triplets = extract_triplets_from_digit_lines(lines, start_idx)
    if len(triplets) >= 6:
        return " / ".join(triplets[:6])
    return ""


def parse_rating_page_text_fallback(rating_text):
    page = RATING_PAGE_MAP[rating_text]
    url = f"{BASE_URL}/{page}/{today_str()}.html"

    try:
        html = fetch_html(url)
    except Exception as e:
        log(f"[rating_page_fallback_error] rating={rating_text} url={url} err={e}")
        return []

    lines = normalize_lines(html)

    rows = []

    for i, line in enumerate(lines):
        m = re.fullmatch(r"(\d{2})\s+(.+)", line)
        next_line = lines[i + 1] if i + 1 < len(lines) else ""

        if m and re.fullmatch(r"\d{1,2}R", next_line):
            jcd = m.group(1)
            venue = m.group(2).strip()
            race_no = int(next_line[:-1])

            selection = parse_triplets_from_lines(lines, i + 2)
            if not selection:
                continue

            row = {
                "venue": venue,
                "jcd": jcd,
                "race_no": race_no,
                "rating": rating_text,
                "selection": selection,
            }
            rows.append(row)

    log(f"[rating_page_summary_fallback] {rating_text} count={len(rows)}")
    return rows


def parse_rating_page(rating_text):
    try:
        rows_dom = parse_rating_page_dom(rating_text)
    except Exception as e:
        log(f"[rating_page_dom_crash] rating={rating_text} err={e}")
        rows_dom = []

    if rows_dom:
        log(f"[rating_page_summary] {rating_text} count={len(rows_dom)} mode=dom")
        return rows_dom

    try:
        rows_fallback = parse_rating_page_text_fallback(rating_text)
    except Exception as e:
        log(f"[rating_page_fallback_crash] rating={rating_text} err={e}")
        rows_fallback = []

    log(f"[rating_page_summary] {rating_text} count={len(rows_fallback)} mode=fallback")
    return rows_fallback


def analyze_candidate(
    official_rating,
    selection,
    exhibition_info,
    boat_stats=None,
    environment=None,
    class_history_map=None,
):
    score = 0.0
    reasons = []
    details = []

    triplets = selection_triplets(selection)
    heads = []
    seconds = []
    thirds = []

    for t in triplets:
        parts = t.split("-")
        if len(parts) != 3:
            continue
        a, b, c = parts
        try:
            heads.append(int(a))
            seconds.append(int(b))
            thirds.append(int(c))
        except Exception:
            continue

    unique_heads = sorted(set(heads))
    unique_seconds = sorted(set(seconds))
    unique_thirds = sorted(set(thirds))
    all_targets = sorted(set(unique_heads + unique_seconds + unique_thirds))

    if len(unique_heads) == 1 and heads:
        score += 0.9
        reasons.append("1着候補がかなり絞れている")
    elif len(unique_heads) == 2 and heads:
        score += 0.5
        reasons.append("1着候補が比較的絞れている")
    elif len(unique_heads) >= 4:
        score -= 0.6
        reasons.append("1着候補が散っている")

    if len(unique_seconds) <= 3 and seconds:
        score += 0.3
        reasons.append("2着候補が比較的絞れている")
    elif len(unique_seconds) >= 5:
        score -= 0.2
        reasons.append("2着候補が広い")

    if len(unique_thirds) <= 4 and thirds:
        score += 0.2
        reasons.append("3着候補が比較的整理されている")

    if unique_heads and min(unique_heads) >= 4:
        score -= 0.6
        reasons.append("外枠1着中心")

    exhibition_times = exhibition_info.get("times", [])
    exhibition_ranks = exhibition_info.get("ranks", {})

    if exhibition_times:
        details.append("展示あり")

    head_avg_rank = None
    head_time_gap_from_top = None
    exhibition_spread = None

    if exhibition_ranks:
        if 1 in exhibition_ranks:
            r1 = exhibition_ranks[1]
            if r1 == 1:
                score += 0.8
                reasons.append("1号艇の展示順位が1位")
            elif r1 <= 2:
                score += 0.4
                reasons.append("1号艇の展示順位が上位")
            elif r1 >= 5:
                score -= 0.6
                reasons.append("1号艇の展示順位が下位")

        head_ranks = [exhibition_ranks[h] for h in unique_heads if h in exhibition_ranks]
        if head_ranks:
            head_avg_rank = sum(head_ranks) / len(head_ranks)
            details.append(f"1着展示平均{round(head_avg_rank, 2)}位")

            if head_avg_rank <= 1.8:
                score += 1.2
                reasons.append("1着候補の展示順位がかなり良い")
            elif head_avg_rank <= 2.5:
                score += 0.7
                reasons.append("1着候補の展示順位が良い")
            elif head_avg_rank <= 3.2:
                score += 0.3
                reasons.append("1着候補の展示順位がまずまず")
            elif head_avg_rank >= 4.5:
                score -= 1.0
                reasons.append("1着候補の展示順位が悪い")

            if all(x <= 3 for x in head_ranks):
                score += 0.4
                reasons.append("1着候補が展示上位に寄っている")
            elif all(x >= 4 for x in head_ranks):
                score -= 0.5
                reasons.append("1着候補が展示下位に寄っている")

        second_ranks = [exhibition_ranks[s] for s in unique_seconds if s in exhibition_ranks]
        if second_ranks:
            avg_second_rank = sum(second_ranks) / len(second_ranks)
            details.append(f"2着展示平均{round(avg_second_rank, 2)}位")
            if avg_second_rank <= 3.0:
                score += 0.2
                reasons.append("2着候補の展示も悪くない")
            elif avg_second_rank >= 5.0:
                score -= 0.2
                reasons.append("2着候補の展示が弱い")

        third_ranks = [exhibition_ranks[t] for t in unique_thirds if t in exhibition_ranks]
        if third_ranks:
            avg_third_rank = sum(third_ranks) / len(third_ranks)
            details.append(f"3着展示平均{round(avg_third_rank, 2)}位")
            if avg_third_rank <= 3.5:
                score += 0.1
            elif avg_third_rank >= 5.2:
                score -= 0.2
                reasons.append("3着候補の展示が弱い")

        sorted_rank_pairs = sorted(exhibition_ranks.items(), key=lambda x: x[1])
        top3_lanes = [lane for lane, _ in sorted_rank_pairs[:3]]
        included_top3 = sum(1 for lane in top3_lanes if lane in all_targets)
        if included_top3 >= 3:
            score += 0.5
            reasons.append("展示上位3艇が買い目にうまく入っている")
        elif included_top3 == 2:
            score += 0.2
            reasons.append("展示上位艇が買い目にある程度入っている")

        if top3_lanes:
            if any(h == top3_lanes[0] for h in unique_heads):
                score += 0.4
                reasons.append("展示1位の艇が1着候補に入っている")

    if exhibition_times and unique_heads:
        lane_time_map = {}
        for lane, t in enumerate(exhibition_times, start=1):
            try:
                lane_time_map[lane] = float(t)
            except Exception:
                pass

        head_times = [lane_time_map[h] for h in unique_heads if h in lane_time_map]
        all_times = list(lane_time_map.values())

        if head_times and all_times:
            top_time = min(all_times)
            bottom_time = max(all_times)
            exhibition_spread = bottom_time - top_time
            head_avg_time = sum(head_times) / len(head_times)
            head_time_gap_from_top = head_avg_time - top_time

            details.append(f"1着展示タイム平均{round(head_avg_time, 2)}")
            details.append(f"展示差{round(exhibition_spread, 2)}")

            if exhibition_spread >= 0.18:
                if head_time_gap_from_top <= 0.03:
                    score += 0.35
                    reasons.append("展示タイム差が大きく1着候補がかなり優勢")
                elif head_time_gap_from_top <= 0.06:
                    score += 0.2
                    reasons.append("展示タイム差があり1着候補が上位")
                elif head_time_gap_from_top >= 0.12:
                    score -= 0.25
                    reasons.append("展示タイム差がある中で1着候補が遅い")
            elif exhibition_spread >= 0.12:
                if head_time_gap_from_top <= 0.03:
                    score += 0.18
                    reasons.append("展示タイム差の中で1着候補が上位")
                elif head_time_gap_from_top >= 0.10:
                    score -= 0.12
                    reasons.append("展示タイム差の中で1着候補が遅め")

    if boat_stats:
        head_stats = [boat_stats[h] for h in unique_heads if h in boat_stats]
        second_stats = [boat_stats[s] for s in unique_seconds if s in boat_stats]
        third_stats = [boat_stats[t] for t in unique_thirds if t in boat_stats]

        head_national = avg_stat(head_stats, "national_win")
        head_local = avg_stat(head_stats, "local_win")
        head_motor = avg_stat(head_stats, "motor2")
        head_boat = avg_stat(head_stats, "boat2")

        second_national = avg_stat(second_stats, "national_win")
        second_local = avg_stat(second_stats, "local_win")
        second_motor = avg_stat(second_stats, "motor2")

        third_national = avg_stat(third_stats, "national_win")
        third_local = avg_stat(third_stats, "local_win")
        third_motor = avg_stat(third_stats, "motor2")

        if head_national is not None:
            details.append(f"1着全国勝率平均{round(head_national, 2)}")
            if head_national >= 6.2:
                score += 0.9
                reasons.append("1着候補の全国勝率がかなり高い")
            elif head_national >= 5.5:
                score += 0.5
                reasons.append("1着候補の全国勝率が高い")
            elif head_national < 4.8:
                score -= 0.5
                reasons.append("1着候補の全国勝率が低い")

        if head_local is not None:
            details.append(f"1着当地勝率平均{round(head_local, 2)}")
            if head_local >= 6.0:
                score += 0.5
                reasons.append("1着候補の当地勝率が高い")
            elif head_local >= 5.5:
                score += 0.3
                reasons.append("1着候補の当地勝率がまずまず高い")
            elif head_local < 4.8:
                score -= 0.3
                reasons.append("1着候補の当地勝率が低い")

        if head_motor is not None:
            details.append(f"1着モーター2連率平均{round(head_motor, 1)}")
            if head_motor >= 42:
                score += 0.6
                reasons.append("1着候補のモーター気配が良い")
            elif head_motor >= 35:
                score += 0.3
                reasons.append("1着候補のモーターがまずまず")
            elif head_motor < 30:
                score -= 0.3
                reasons.append("1着候補のモーターが弱い")

        if head_boat is not None:
            details.append(f"1着ボート2連率平均{round(head_boat, 1)}")
            if head_boat >= 38:
                score += 0.2
                reasons.append("1着候補のボート気配が良い")
            elif head_boat < 30:
                score -= 0.2
                reasons.append("1着候補のボート気配が弱い")

        if second_national is not None:
            details.append(f"2着全国勝率平均{round(second_national, 2)}")
            if second_national >= 5.4:
                score += 0.25
                reasons.append("2着候補の全国勝率が高い")
            elif second_national >= 4.8:
                score += 0.1
            elif second_national < 4.2:
                score -= 0.2
                reasons.append("2着候補の全国勝率が弱い")

        if second_local is not None:
            details.append(f"2着当地勝率平均{round(second_local, 2)}")
            if second_local >= 5.2:
                score += 0.15
            elif second_local < 4.2:
                score -= 0.1

        if second_motor is not None:
            details.append(f"2着モーター2連率平均{round(second_motor, 1)}")
            if second_motor >= 38:
                score += 0.15
            elif second_motor < 28:
                score -= 0.1

        if third_national is not None:
            details.append(f"3着全国勝率平均{round(third_national, 2)}")
            if third_national >= 5.0:
                score += 0.1
            elif third_national < 4.0:
                score -= 0.1

        if third_local is not None:
            details.append(f"3着当地勝率平均{round(third_local, 2)}")
            if third_local >= 4.8:
                score += 0.08
            elif third_local < 4.0:
                score -= 0.08

        if third_motor is not None:
            details.append(f"3着モーター2連率平均{round(third_motor, 1)}")
            if third_motor >= 35:
                score += 0.08
            elif third_motor < 26:
                score -= 0.08

    class_history_map = class_history_map or {}
    head_histories = [class_history_map[h] for h in unique_heads if h in class_history_map]
    second_histories = [class_history_map[s] for s in unique_seconds if s in class_history_map]

    if head_histories:
        head_class_scores = [class_history_score(x) for x in head_histories]
        avg_head_class = sum(head_class_scores) / len(head_class_scores)
        details.append(f"1着級別3期平均{round(avg_head_class, 2)}")

        if avg_head_class >= 1.2:
            score += 0.65
            reasons.append("1着候補の級別3期傾向がかなり良い")
        elif avg_head_class >= 0.7:
            score += 0.35
            reasons.append("1着候補の級別3期傾向が良い")
        elif avg_head_class <= -0.2:
            score -= 0.35
            reasons.append("1着候補の級別3期傾向が弱い")

        a1_trend_count = sum(1 for x in head_histories if x.get("current_class") == "A1")
        if a1_trend_count >= 2:
            score += 0.25
            reasons.append("1着候補に現A1が複数いる")

    if second_histories:
        second_class_scores = [class_history_score(x) for x in second_histories]
        avg_second_class = sum(second_class_scores) / len(second_class_scores)
        details.append(f"2着級別3期平均{round(avg_second_class, 2)}")
        if avg_second_class >= 0.7:
            score += 0.15
        elif avg_second_class <= -0.2:
            score -= 0.10

    env = environment or {}
    wind_speed = env.get("wind_speed")
    wave_height = env.get("wave_height")
    wind_type = env.get("wind_type") or ""
    wind_direction = env.get("wind_direction") or ""
    weather = env.get("weather") or ""
    stabilizer = bool(env.get("stabilizer"))

    if weather:
        details.append(f"天候{weather}")
    if wind_speed is not None:
        details.append(f"風速{wind_speed:g}m")
    if wind_direction:
        details.append(f"風向{wind_direction}")
    if wave_height is not None:
        details.append(f"波高{wave_height:g}cm")
    if stabilizer:
        details.append("安定板あり")

    outer_heads = [h for h in unique_heads if h >= 4]
    very_outer_heads = [h for h in unique_heads if h >= 5]

    if wind_speed is not None:
        if wind_speed >= 4:
            if wind_type == "headwind":
                if 1 in unique_heads:
                    score += 0.35
                    reasons.append("向かい風で1号艇寄り")
                if 2 in unique_heads and 1 in unique_heads:
                    score += 0.10
                if outer_heads:
                    score -= 0.12
                    reasons.append("向かい風で外頭は少し不利")

            elif wind_type == "tailwind":
                if outer_heads:
                    score += 0.25
                    reasons.append("追い風で外の一撃候補を少し評価")
                if 1 in unique_heads and len(unique_heads) == 1:
                    score -= 0.05

            elif wind_type == "crosswind":
                if outer_heads:
                    score -= 0.12
                    reasons.append("横風で外頭は少し割引")

        if wind_speed >= 6:
            if 1 in unique_heads:
                score += 0.15
                reasons.append("強風で内寄りを少し評価")
            if very_outer_heads:
                score -= 0.20
                reasons.append("強風で大外頭は少し厳しい")

            if head_avg_rank is not None:
                if head_avg_rank <= 2.5:
                    score += 0.20
                    reasons.append("強風なので展示順位上位を強め評価")
                elif head_avg_rank >= 4.0:
                    score -= 0.20
                    reasons.append("強風で展示下位頭は割引")

            if exhibition_spread is not None:
                if exhibition_spread >= 0.12 and head_time_gap_from_top is not None:
                    if head_time_gap_from_top <= 0.03:
                        score += 0.15
                        reasons.append("強風で展示タイム上位を少し強め評価")
                    elif head_time_gap_from_top >= 0.10:
                        score -= 0.12
                        reasons.append("強風で展示タイム遅め頭は割引")

    if wave_height is not None:
        if wave_height >= 5:
            if 1 in unique_heads:
                score += 0.12
            if outer_heads:
                score -= 0.12
                reasons.append("波高高めで外頭を少し割引")

            if head_avg_rank is not None:
                if head_avg_rank <= 2.5:
                    score += 0.12
                    reasons.append("波高高めで展示上位を少し重視")
                elif head_avg_rank >= 4.0:
                    score -= 0.10

        if wave_height >= 7:
            if 1 in unique_heads:
                score += 0.10
            if very_outer_heads:
                score -= 0.15
                reasons.append("波高かなり高めで大外頭をさらに割引")

    if stabilizer:
        if 1 in unique_heads:
            score += 0.18
            reasons.append("安定板ありでイン寄りを少し評価")
        if outer_heads:
            score -= 0.18
            reasons.append("安定板ありで外のまくり頭を少し割引")

        if head_avg_rank is not None and head_avg_rank <= 2.5:
            score += 0.15
            reasons.append("安定板ありで展示上位を少し重視")

    if wind_speed is not None and wind_speed >= 4 and wind_type == "tailwind":
        if outer_heads and head_avg_rank is not None and head_avg_rank <= 2.8:
            score += 0.10
            reasons.append("追い風×展示上位の外頭候補を軽く加点")

    ai_rating = score_to_ai_rating(score)
    final_rank = decide_final_rank(official_rating, score)

    exhibition_rank_text = ""
    if exhibition_ranks:
        exhibition_rank_text = " / ".join(
            [f"{lane}:{exhibition_ranks.get(lane, '-')}" for lane in range(1, 7)]
        )

    ai_detail = " / ".join(details) if details else ""

    return {
        "ai_score": round(score, 2),
        "ai_rating": ai_rating,
        "ai_label": "",
        "final_rank": final_rank,
        "ai_reasons": reasons,
        "exhibition": exhibition_times,
        "exhibition_rank": exhibition_rank_text,
        "motor_rank": "",
        "ai_detail": ai_detail,
    }


def fill_missing_deadlines(rows, deadlines_cache):
    filled = 0

    for row in rows:
        venue = row["venue"]
        race_no = row["race_no"]
        jcd = row["jcd"] or NAME_JCD_MAP.get(venue, "")

        if not jcd:
            continue

        current = deadlines_cache.get(jcd, {}).get(race_no, "")
        if current:
            continue

        single_deadline = parse_single_race_deadline(jcd, race_no)
        if single_deadline:
            deadlines_cache.setdefault(jcd, {})[race_no] = single_deadline
            filled += 1
            log(f"[official_single_ok] jcd={jcd} venue={venue} race_no={race_no} time={single_deadline}")
        else:
            log(f"[official_single_empty] jcd={jcd} venue={venue} race_no={race_no}")

    log(f"[official_single_fill_summary] filled={filled}")
    return deadlines_cache


def fetch_deadlines_parallel(jcds):
    results = {}
    with ThreadPoolExecutor(max_workers=OFFICIAL_MAX_WORKERS) as ex:
        futures = [ex.submit(parse_official_deadlines_for_jcd, jcd) for jcd in sorted(jcds)]
        for future in as_completed(futures):
            jcd, deadlines = future.result()
            results[jcd] = deadlines
    return results


def fetch_beforeinfo_parallel(keys):
    results = {}
    with ThreadPoolExecutor(max_workers=BEFOREINFO_MAX_WORKERS) as ex:
        futures = [ex.submit(parse_beforeinfo_for_key, jcd, race_no) for (jcd, race_no) in sorted(keys)]
        for future in as_completed(futures):
            key, info = future.result()
            results[key] = info
    return results


def fetch_racelist_parallel(jcds):
    results = {}
    supported = [jcd for jcd in sorted(jcds) if jcd in RACELIST_VENUE_SLUG_MAP]

    if not supported:
        log("[racelist_parallel] supported=0")
        return results

    log(f"[racelist_parallel] supported={supported}")

    with ThreadPoolExecutor(max_workers=RACELIST_MAX_WORKERS) as ex:
        futures = [ex.submit(parse_racelist_for_jcd, jcd) for jcd in supported]
        for future in as_completed(futures):
            try:
                jcd, info = future.result()
                results[jcd] = info
            except Exception as e:
                log(f"[racelist_parallel_error] err={e}")

    return results


def log_beforeinfo_summary(beforeinfo_cache, future_keys):
    total = len(future_keys)
    fetched = len(beforeinfo_cache)

    weather_count = 0
    wind_speed_count = 0
    wind_dir_count = 0
    wave_count = 0
    stabilizer_count = 0
    exhibition_time_count = 0
    exhibition_rank_count = 0

    for _key, info in beforeinfo_cache.items():
        env = info.get("environment", {})
        exhibition = info.get("exhibition", {})

        if env.get("weather"):
            weather_count += 1
        if env.get("wind_speed") is not None:
            wind_speed_count += 1
        if env.get("wind_direction"):
            wind_dir_count += 1
        if env.get("wave_height") is not None:
            wave_count += 1
        if env.get("stabilizer"):
            stabilizer_count += 1
        if exhibition.get("times"):
            exhibition_time_count += 1
        if exhibition.get("ranks"):
            exhibition_rank_count += 1

    log(
        "[beforeinfo_summary] "
        f"targets={total} fetched={fetched} "
        f"weather={weather_count} wind_speed={wind_speed_count} "
        f"wind_dir={wind_dir_count} wave={wave_count} "
        f"stabilizer={stabilizer_count} "
        f"ex_times={exhibition_time_count} ex_ranks={exhibition_rank_count}"
    )


def build_candidates():
    log("========== build_candidates start ==========")
    log(f"now={jst_now().strftime('%Y-%m-%d %H:%M:%S JST')}")

    raw_rows = []
    raw_rows.extend(parse_rating_page("★★★★★"))

    dedup = {}
    for row in raw_rows:
        key = (row["venue"], row["race_no"])
        if key not in dedup:
            dedup[key] = row

    rows = list(dedup.values())
    log(f"[dedup_summary] count={len(rows)}")

    valid_rows = []
    for row in rows:
        triplets = selection_triplets(row["selection"])
        if len(triplets) < 6:
            continue

        bad_triplet = False
        for t in triplets:
            parts = t.split("-")
            if len(parts) != 3:
                bad_triplet = True
                break
            a, b, c = parts
            if a == b or a == c or b == c:
                bad_triplet = True
                break

        if bad_triplet:
            continue

        valid_rows.append(row)

    rows = valid_rows
    log(f"[selection_clean_summary] count={len(rows)}")

    needed_jcds = set()
    for row in rows:
        jcd = row["jcd"] or NAME_JCD_MAP.get(row["venue"], "")
        if jcd:
            needed_jcds.add(jcd)

    if USE_RACELIST:
        racelist_cache = fetch_racelist_parallel(needed_jcds)
    else:
        racelist_cache = {}
        log("[racelist_parallel] skipped by USE_RACELIST=False")

    deadlines_cache = fetch_deadlines_parallel(needed_jcds)
    deadlines_cache = fill_missing_deadlines(rows, deadlines_cache)

    filtered_rows = []
    future_keys = set()
    missing_deadline_rows = []

    for row in rows:
        venue = row["venue"]
        race_no = row["race_no"]
        jcd = row["jcd"] or NAME_JCD_MAP.get(venue, "")

        if not jcd:
            continue

        deadline = deadlines_cache.get(jcd, {}).get(race_no, "")
        if not deadline:
            missing_deadline_rows.append(f"{venue}{race_no}R")
            continue

        row["time"] = deadline
        filtered_rows.append(row)

        if is_future_or_now(deadline):
            future_keys.add((jcd, race_no))

    rows = filtered_rows
    log(f"[deadline_filtered_summary] count={len(rows)} future_beforeinfo_count={len(future_keys)}")

    if missing_deadline_rows:
        log(f"[missing_deadline_rows] count={len(missing_deadline_rows)} rows={missing_deadline_rows}")

    beforeinfo_cache = fetch_beforeinfo_parallel(future_keys) if future_keys else {}
    if future_keys:
        log_beforeinfo_summary(beforeinfo_cache, future_keys)

    results = []
    env_detail_count = 0
    wind_speed_used_count = 0
    wave_used_count = 0
    stabilizer_used_count = 0
    class3_rows = 0
    ai_selection_rows = 0

    for row in rows:
        venue = row["venue"]
        race_no = row["race_no"]
        rating = row["rating"]
        selection = row["selection"]
        jcd = row["jcd"] or NAME_JCD_MAP.get(venue, "")
        deadline = row["time"]

        beforeinfo = beforeinfo_cache.get((jcd, race_no), {})
        exhibition_info = beforeinfo.get("exhibition", {"times": [], "ranks": {}})
        boat_stats = beforeinfo.get("boat_stats", {})
        environment = beforeinfo.get("environment", {})
        class_history_map = racelist_cache.get(jcd, {}).get(race_no, {})

        analyzed = analyze_candidate(
            rating,
            selection,
            exhibition_info,
            boat_stats,
            environment,
            class_history_map,
        )

        ai_generated = generate_ai_selection(
            exhibition_info,
            boat_stats,
            environment,
            class_history_map,
        )

        ai_detail_text = analyzed["ai_detail"]
        if "風速" in ai_detail_text:
            wind_speed_used_count += 1
        if "波高" in ai_detail_text:
            wave_used_count += 1
        if "安定板あり" in ai_detail_text:
            stabilizer_used_count += 1
        if "1着級別3期平均" in ai_detail_text:
            class3_rows += 1
        if ai_detail_text:
            env_detail_count += 1
        if ai_generated["ai_selection"]:
            ai_selection_rows += 1

        class_history_text = ""
        if class_history_map:
            class_history_text = " / ".join(
                [
                    f"{lane}:{make_class_history_text(class_history_map.get(lane, {}))}"
                    for lane in range(1, 7)
                    if class_history_map.get(lane)
                ]
            )

        candidate = {
            "race_date": today_text(),
            "time": deadline,
            "venue": venue,
            "race_no": f"{race_no}R",
            "race_no_num": race_no,
            "rating": rating,
            "bet_type": BET_TYPE,
            "selection": selection,
            "amount": BET_AMOUNT,
            "ai_score": analyzed["ai_score"],
            "ai_rating": analyzed["ai_rating"],
            "ai_label": analyzed["ai_label"],
            "final_rank": analyzed["final_rank"],
            "ai_reasons": analyzed["ai_reasons"],
            "exhibition": analyzed["exhibition"],
            "exhibition_rank": analyzed["exhibition_rank"],
            "motor_rank": analyzed["motor_rank"],
            "ai_detail": analyzed["ai_detail"],
            "ai_selection": ai_generated["ai_selection"],
            "ai_confidence": ai_generated["ai_confidence"],
            "ai_lane_score_text": ai_generated["ai_lane_score_text"],
            "class_history_text": class_history_text,
        }
        results.append(candidate)

    log(
        "[ai_detail_summary] "
        f"rows={len(results)} detail_rows={env_detail_count} "
        f"wind_speed_rows={wind_speed_used_count} "
        f"wave_rows={wave_used_count} stabilizer_rows={stabilizer_used_count} "
        f"class3_rows={class3_rows} ai_selection_rows={ai_selection_rows}"
    )

    official_rating_counts = {}
    for r in results:
        official_rating_counts[r["rating"]] = official_rating_counts.get(r["rating"], 0) + 1
    log(f"[summary_official_ratings] {official_rating_counts}")

    venue_counts = {}
    for r in results:
        venue_counts[r["venue"]] = venue_counts.get(r["venue"], 0) + 1
    log(f"[summary_venues] {venue_counts}")

    results.sort(key=lambda x: (to_minutes(x["time"]), x["venue"], x["race_no_num"]))
    log(f"build_candidates final_count={len(results)}")
    log("========== build_candidates end ==========")
    return results


def send_to_render(races):
    if not RENDER_IMPORT_URL:
        raise RuntimeError("RENDER_IMPORT_URL が未設定です")
    if not IMPORT_TOKEN:
        raise RuntimeError("IMPORT_TOKEN が未設定です")

    headers = {
        "Content-Type": "application/json",
        "X-IMPORT-TOKEN": IMPORT_TOKEN,
    }
    payload = {"races": races}

    res = SESSION.post(RENDER_IMPORT_URL, headers=headers, json=payload, timeout=POST_TIMEOUT)
    print("status_code =", res.status_code)
    print("response =", res.text)
    res.raise_for_status()


def main():
    races = build_candidates()
    if not races:
        print("候補が0件でした")
        return

    send_to_render(races)


if __name__ == "__main__":
    main()
