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

REQUEST_TIMEOUT = (10, 20)
POST_TIMEOUT = 40
MAX_RETRIES = 3
RETRY_SLEEP_SEC = 1.2

OFFICIAL_MAX_WORKERS = 6
BEFOREINFO_MAX_WORKERS = 12

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


def parse_beforeinfo_for_key(jcd, race_no):
    beforeinfo_url = build_beforeinfo_url(jcd, race_no)

    try:
        html = fetch_html(beforeinfo_url)
    except Exception as e:
        log(f"[beforeinfo_error] jcd={jcd} race_no={race_no} err={e}")
        return (jcd, race_no), {
            "exhibition": {"times": [], "ranks": {}},
            "boat_stats": {},
        }

    lines = normalize_lines(html)

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

    return (jcd, race_no), {
        "exhibition": {"times": times, "ranks": ranks},
        "boat_stats": stats,
    }


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

    soup, _html = fetch_soup(url)
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

    html = fetch_html(url)
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
    rows_dom = parse_rating_page_dom(rating_text)
    if rows_dom:
        log(f"[rating_page_summary] {rating_text} count={len(rows_dom)} mode=dom")
        return rows_dom

    rows_fallback = parse_rating_page_text_fallback(rating_text)
    log(f"[rating_page_summary] {rating_text} count={len(rows_fallback)} mode=fallback")
    return rows_fallback


def analyze_candidate(official_rating, selection, exhibition_info, boat_stats=None):
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
            avg_rank = sum(head_ranks) / len(head_ranks)
            details.append(f"1着展示平均{round(avg_rank, 2)}位")

            if avg_rank <= 1.8:
                score += 1.2
                reasons.append("1着候補の展示順位がかなり良い")
            elif avg_rank <= 2.5:
                score += 0.7
                reasons.append("1着候補の展示順位が良い")
            elif avg_rank <= 3.2:
                score += 0.3
                reasons.append("1着候補の展示順位がまずまず")
            elif avg_rank >= 4.5:
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
            spread = bottom_time - top_time
            head_avg_time = sum(head_times) / len(head_times)
            head_gap_from_top = head_avg_time - top_time

            details.append(f"1着展示タイム平均{round(head_avg_time, 2)}")
            details.append(f"展示差{round(spread, 2)}")

            if spread >= 0.18:
                if head_gap_from_top <= 0.03:
                    score += 0.35
                    reasons.append("展示タイム差が大きく1着候補がかなり優勢")
                elif head_gap_from_top <= 0.06:
                    score += 0.2
                    reasons.append("展示タイム差があり1着候補が上位")
                elif head_gap_from_top >= 0.12:
                    score -= 0.25
                    reasons.append("展示タイム差がある中で1着候補が遅い")
            elif spread >= 0.12:
                if head_gap_from_top <= 0.03:
                    score += 0.18
                    reasons.append("展示タイム差の中で1着候補が上位")
                elif head_gap_from_top >= 0.10:
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

        a1_count = sum(1 for s in head_stats if s.get("class") == "A1")
        if a1_count >= 2:
            score += 0.6
            reasons.append("1着候補にA1級が複数いる")
        elif a1_count == 1:
            score += 0.3
            reasons.append("1着候補にA1級がいる")

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

    results = []
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

        analyzed = analyze_candidate(rating, selection, exhibition_info, boat_stats)

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
        }
        results.append(candidate)

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
