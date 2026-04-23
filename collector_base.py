
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
    "https://race-candidates-app.onrender.com/api/import_base_candidates",
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


def env_bool(name, default=True):
    raw = os.environ.get(name, "1" if default else "0")
    return str(raw).strip().lower() not in {"0", "false", "no", "off"}


def env_int(name, default):
    try:
        return int(str(os.environ.get(name, default)).strip())
    except Exception:
        return int(default)


def env_float(name, default):
    try:
        return float(str(os.environ.get(name, default)).strip())
    except Exception:
        return float(default)


# 公式★4/★5だけに寄せすぎない検証用。
# app.py 側で show_shadow=1 の時だけ表示される想定。
ENABLE_SHADOW_AI = env_bool("ENABLE_SHADOW_AI", False)
SHADOW_AI_MAX_CANDIDATES = env_int("SHADOW_AI_MAX_CANDIDATES", 24)
SHADOW_AI_MIN_SCORE = env_float("SHADOW_AI_MIN_SCORE", 1.75)
SHADOW_AI_SKIP_OFFICIAL_DUPLICATES = env_bool("SHADOW_AI_SKIP_OFFICIAL_DUPLICATES", True)

# 全レース検証用。画面には通常出さず、CSV分析用に全開催レースのbase AIを保存する。
ENABLE_ALL_RACE_AI = env_bool("ENABLE_ALL_RACE_AI", False)

JCD_NAME_MAP = {
    "01": "桐生",
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
    "18": "徳山",
    "19": "下関",
    "20": "若松",
    "21": "芦屋",
    "22": "福岡",
    "23": "唐津",
    "24": "大村",
}
NAME_JCD_MAP = {v: k for k, v in JCD_NAME_MAP.items()}
RATING_PAGE_MAP = {
    "★★★★★": "s5",
    "★★★★☆": "s4",
    "★★★☆☆": "s3",
    "★★☆☆☆": "s2",
    "★☆☆☆☆": "s1",
}
OFFICIAL_PICKUP_RATINGS = ["★★★★★", "★★★★☆"]
ALL_OFFICIAL_RATINGS = list(RATING_PAGE_MAP.keys())
RACELIST_VENUE_SLUG_MAP = {
    "01": "kiryu",
    "02": "toda",
    "03": "edogawa",
    "04": "heiwajima",
    "05": "tamagawa",
    "06": "hamanako",
    "07": "gamagori",
    "08": "tokoname",
    "09": "tsu",
    "10": "mikuni",
    "11": "biwako",
    "12": "suminoe",
    "13": "amagasaki",
    "14": "naruto",
    "15": "marugame",
    "16": "kojima",
    "17": "miyajima",
    "18": "tokuyama",
    "19": "shimonoseki",
    "20": "wakamatsu",
    "21": "ashiya",
    "22": "fukuoka",
    "23": "karatsu",
    "24": "omura",
}

BRANCH_NAMES = [
    "群馬", "埼玉", "東京", "静岡", "愛知", "三重", "福井", "滋賀", "大阪", "兵庫",
    "徳島", "香川", "岡山", "広島", "山口", "福岡", "佐賀", "長崎",
]

HOME_BRANCH_BY_JCD = {
    "01": "群馬",
    "02": "埼玉",
    "03": "東京",
    "04": "東京",
    "05": "東京",
    "06": "静岡",
    "07": "愛知",
    "08": "愛知",
    "09": "三重",
    "10": "福井",
    "11": "滋賀",
    "12": "大阪",
    "13": "兵庫",
    "14": "徳島",
    "15": "香川",
    "16": "岡山",
    "17": "広島",
    "18": "山口",
    "19": "山口",
    "20": "福岡",
    "21": "福岡",
    "22": "福岡",
    "23": "佐賀",
    "24": "長崎",
}

# A1の中でも別格級として軽く見るレーサー。
# 強い加点ではなく、同格比較で少し残すための補助タグとして使う。
ELITE_A1_RACERS = {
    "峰 竜太",
    "池田 浩二",
    "茅原 悠紀",
    "桐生 順平",
    "馬場 貴也",
    "白井 英治",
    "瓜生 正義",
    "菊地 孝平",
    "平本 真之",
    "磯部 誠",
    "新田 雄史",
    "山口 剛",
    "深谷 知博",
    "濱野谷 憲吾",
    "篠崎 元志",
    "遠藤 エミ",
}

# 級別だけ見ると過小評価しやすい別枠。
# 例: 今期B2でも地力はA1上位級として軽く救済する。
HIDDEN_ELITE_RACERS = {
    "毒島 誠",
}


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


def today_text_dashless():
    return jst_now().strftime("%Y%m%d")


def current_hhmm():
    return jst_now().strftime("%H:%M")


def to_minutes(hhmm):
    h, m = map(int, hhmm.split(":"))
    return h * 60 + m


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


def build_racelist_detail_url(jcd, race_no, scheme="https"):
    slug = RACELIST_VENUE_SLUG_MAP.get(jcd)
    if not slug:
        return ""
    return f"{scheme}://kyotei.sakura.ne.jp/racelist-{slug}-{today_text_dashless()}-{int(race_no)}.html"


def build_info_detail_url(jcd, race_no):
    return f"https://info.kyotei.fun/info-{today_text_dashless()}-{jcd}-{int(race_no)}.html"


def try_fetch_html(url, timeout=REQUEST_TIMEOUT, max_retries=2):
    for attempt in range(1, max_retries + 1):
        try:
            res = requests.get(url, headers=HEADERS, timeout=timeout)
            res.raise_for_status()
            res.encoding = res.apparent_encoding
            return res.text
        except Exception:
            if attempt < max_retries:
                time.sleep(0.6 * attempt)
    return None


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



def safe_float(value, default=0.0):
    try:
        if value is None:
            return float(default)
        s = str(value).strip()
        if s == "":
            return float(default)
        return float(s)
    except Exception:
        return float(default)

def normalize_text_for_class_parse(html):
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text("\n")
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n+", "\n", text)
    return text.strip()

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


def extract_series_day_and_phase_from_text(text, race_no=0):
    s = normalize_text_for_class_parse(text)
    compact = re.sub(r"\s+", "", s)

    series_day = 0
    day_patterns = [
        r"第\s*([1-7])\s*日",
        r"([1-7])日目",
        r"節間\s*([1-7])日目",
    ]
    for pat in day_patterns:
        m = re.search(pat, compact)
        if m:
            try:
                series_day = int(m.group(1))
                break
            except Exception:
                pass

    phase = ""
    phase_patterns = [
        (r"優勝戦", "優勝戦"),
        (r"準優勝戦|準優", "準優勝戦"),
        (r"ドリーム戦|ドリーム", "ドリーム戦"),
        (r"予選", "予選"),
        (r"一般戦", "一般戦"),
    ]
    for pat, label in phase_patterns:
        if re.search(pat, compact):
            phase = label
            break

    race_no = normalize_race_no_value(race_no)
    if not phase and race_no == 12 and re.search(r"ドリーム", compact):
        phase = "ドリーム戦"

    return {
        "series_day": int(series_day or 0),
        "race_phase": normalize_race_phase_label(phase),
    }


def merge_race_meta(primary_meta, fallback_meta=None):
    primary_meta = dict(primary_meta or {})
    fallback_meta = dict(fallback_meta or {})
    return {
        "series_day": int(primary_meta.get("series_day") or fallback_meta.get("series_day") or 0),
        "race_phase": normalize_race_phase_label(primary_meta.get("race_phase") or fallback_meta.get("race_phase") or ""),
    }



def extract_class_block_tokens(text):
    start = text.find("級")
    if start < 0:
        return {}
    end_candidates = []
    for marker in ["能力", "全国", "当地", "モーター", "ボート", "2連率", "勝率", "展示", "ST"]:
        pos = text.find(marker, start + 1)
        if pos > start:
            end_candidates.append(pos)
    block = text[start:min(end_candidates)] if end_candidates else text[start:start + 1200]
    tokens = re.findall(r"\b(A1|A2|B1|B2|-)\b", block)
    if len(tokens) >= 24:
        tokens = tokens[:24]
        rows = {}
        for lane in range(1, 7):
            base = (lane - 1) * 4
            rows[lane] = {
                "current_class": "" if tokens[base] == "-" else tokens[base],
                "prev1_class": "" if tokens[base + 1] == "-" else tokens[base + 1],
                "prev2_class": "" if tokens[base + 2] == "-" else tokens[base + 2],
                "prev3_class": "" if tokens[base + 3] == "-" else tokens[base + 3],
            }
        return rows
    if len(tokens) >= 18:
        tokens = tokens[:18]
        rows = {}
        for lane in range(1, 7):
            base = (lane - 1) * 3
            rows[lane] = {
                "current_class": "" if tokens[base] == "-" else tokens[base],
                "prev1_class": "" if tokens[base + 1] == "-" else tokens[base + 1],
                "prev2_class": "" if tokens[base + 2] == "-" else tokens[base + 2],
                "prev3_class": "",
            }
        return rows
    return {}


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
    return all_times[0] if all_times else ""


def parse_official_deadlines_for_jcd(jcd):
    venue = JCD_NAME_MAP.get(jcd, jcd)
    try:
        html = fetch_html(build_official_url(jcd, race_no=1))
    except Exception as e:
        log(f"[official_deadlines_error] jcd={jcd} venue={venue} err={e}")
        return jcd, {}
    deadlines = parse_official_deadlines_from_html(html)
    log(f"[official_deadlines_ok] jcd={jcd} venue={venue} count={len(deadlines)}") if deadlines else log(f"[official_deadlines_empty] jcd={jcd} venue={venue}")
    return jcd, deadlines


def normalize_player_name(text):
    s = str(text or "").strip()
    s = s.replace("　", " ")
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"\([A-Z0-9]+\)$", "", s).strip()
    s = re.sub(r"^(?:登録番号|支部|年齢|体重)\s*", "", s).strip()
    return s


def normalize_racer_name_key(name):
    return normalize_player_name(name).replace(" ", "")


def _normalized_name_set(names):
    return {normalize_racer_name_key(x) for x in names if str(x or "").strip()}


def is_elite_a1_racer_name(player_name):
    key = normalize_racer_name_key(player_name)
    return bool(key and key in _normalized_name_set(ELITE_A1_RACERS))


def is_hidden_elite_racer_name(player_name):
    key = normalize_racer_name_key(player_name)
    return bool(key and key in _normalized_name_set(HIDDEN_ELITE_RACERS))


def elite_racer_bonus(player_name):
    # 名前補正は強くしすぎない。買い目を歪ませず、同格比較の微差だけに使う。
    if is_hidden_elite_racer_name(player_name):
        return 0.12
    if is_elite_a1_racer_name(player_name):
        return 0.08
    return 0.0


def elite_racer_label(player_name):
    if is_hidden_elite_racer_name(player_name):
        return "実力A1級"
    if is_elite_a1_racer_name(player_name):
        return "別格"
    return ""


def is_probable_player_name(text):
    s = str(text or "").strip()
    if not s:
        return False

    compact = re.sub(r"\s+", "", s)
    if not compact:
        return False

    ng_words = [
        "天候", "風速", "波高", "気温", "水温", "安定板", "水面", "気象", "情報", "時点",
        "展示", "進入", "体重", "調整", "部品", "全国", "当地", "モーター", "ボート", "勝率",
        "連率", "ST", "級別", "能力", "今節", "成績", "平均", "欠場", "事故", "レース", "気配",
        "晴", "雨", "曇", "曇り", "雲り", "くもり", "風", "波",
        "着順", "枠番", "艇番", "枠", "今節成績", "進入コース", "コース",
        "ピストン", "リング", "キャブ", "ギヤ", "ギア", "シャフト", "電気", "本体",
        "整備", "交換", "部品交換", "チルト", "プロペラ", "ペラ", "ボート変更", "モーター変更"
    ]
    if any(word in compact for word in ng_words):
        return False

    if re.search(r"[0-9０-９A-Za-zＡ-Ｚａ-ｚ]", compact):
        return False

    if len(compact) < 2 or len(compact) > 8:
        return False

    return bool(re.fullmatch(r"[一-龯ぁ-んァ-ヶー\s]+", s))


def extract_player_names_from_lines(lines):
    result = {}

    lane_positions = []
    for idx, line in enumerate(lines):
        if re.fullmatch(r"[1-6]", line):
            lane_positions.append((idx, int(line)))

    for pos_idx, lane in lane_positions:
        segment = lines[pos_idx + 1: pos_idx + 6]
        candidates = []

        for i, line in enumerate(segment):
            name = normalize_player_name(line)
            if is_probable_player_name(name):
                candidates.append((i, name))

        if not candidates:
            continue

        name = candidates[0][1]

        if len(candidates) >= 2:
            first_idx, first_name = candidates[0]
            second_idx, second_name = candidates[1]

            first_compact = first_name.replace(" ", "")
            second_compact = second_name.replace(" ", "")

            if (
                second_idx == first_idx + 1
                and " " not in first_name
                and " " not in second_name
                and 1 <= len(first_compact) <= 4
                and 1 <= len(second_compact) <= 4
            ):
                joined = normalize_player_name(first_compact + " " + second_compact)
                if re.fullmatch(r"[一-龯ぁ-んァ-ヶー]{1,6}\s+[一-龯ぁ-んァ-ヶー]{1,6}", joined):
                    name = joined

        if is_probable_player_name(name):
            result[lane] = name

    return result



def extract_branch_from_segment(segment):
    if not segment:
        return ""

    joined = " ".join([str(x or "").strip() for x in segment if str(x or "").strip()])
    if not joined:
        return ""

    branch_hits = []
    for branch in BRANCH_NAMES:
        patterns = [
            rf"{branch}\s*支部",
            rf"支部\s*{branch}",
            rf"(?:^|\s){branch}(?:\s|$)",
        ]
        for pat in patterns:
            if re.search(pat, joined):
                branch_hits.append(branch)
                break

    if len(branch_hits) == 1:
        return branch_hits[0]

    if len(branch_hits) >= 2:
        counts = {}
        for branch in branch_hits:
            counts[branch] = counts.get(branch, 0) + 1
        return sorted(counts.items(), key=lambda x: (-x[1], x[0]))[0][0]

    for line in segment:
        line = str(line or "").strip()
        if line in BRANCH_NAMES:
            return line

    return ""


def extract_branch_map_from_lines(lines):
    result = {}
    lane_positions = [(idx, int(line)) for idx, line in enumerate(lines) if re.fullmatch(r"[1-6]", line)]

    for pos_idx, lane in lane_positions:
        segment = lines[pos_idx: pos_idx + 18]
        branch = extract_branch_from_segment(segment)
        if branch:
            result[lane] = branch

    return result

def sanitize_player_name_map(name_map):
    cleaned = {}
    if not name_map:
        return cleaned

    counts = {}
    normalized_map = {}
    for lane in range(1, 7):
        raw_name = name_map.get(lane)
        name = normalize_player_name(raw_name)
        if not is_probable_player_name(name):
            continue
        normalized_map[lane] = name
        compact = name.replace(" ", "")
        counts[compact] = counts.get(compact, 0) + 1

    for lane, name in normalized_map.items():
        compact = name.replace(" ", "")
        if counts.get(compact, 0) == 1:
            cleaned[lane] = name

    return cleaned


def make_player_names_text(player_names_map):
    return " / ".join([f"{lane}:{player_names_map.get(lane, '')}" for lane in range(1, 7) if player_names_map.get(lane)])


def merge_player_name_maps(primary_map, fallback_map):
    merged = {}
    primary_map = sanitize_player_name_map(primary_map)
    fallback_map = sanitize_player_name_map(fallback_map)

    for lane in range(1, 7):
        name = primary_map.get(lane) or fallback_map.get(lane) or ""
        if name:
            merged[lane] = normalize_player_name(name)
    return merged


def make_player_reason_text(reason_map):
    parts = []
    for lane in range(1, 7):
        items = [str(x).strip() for x in (reason_map.get(lane) or []) if str(x).strip()]
        if items:
            parts.append(f"{lane}:{'|'.join(items)}")
    return " / ".join(parts)


def build_player_reason_map(boat_stats, class_history_map, extra_stats=None, jcd="", player_names_map=None):
    extra_stats = infer_extra_stats(boat_stats, class_history_map, extra_stats)
    reason_map = {}

    for lane in range(1, 7):
        s = boat_stats.get(lane, {}) or {}
        ch = class_history_map.get(lane, {}) or {}
        ex = extra_stats.get(lane, {}) or {}
        player_name = (player_names_map or {}).get(lane, "")

        cls = s.get("class") or ch.get("current_class") or ""
        national = s.get("national_win")
        local = s.get("local_win")
        motor2 = s.get("motor2")
        boat2 = s.get("boat2")
        avg_st = ex.get("avg_st")
        course_rate = ex.get("course_rate")
        course_rate_source = str(ex.get("course_rate_source") or "")
        recent_avg = ex.get("recent_avg")
        recent_top3 = ex.get("recent_top3")
        recent_source = str(ex.get("recent_source") or "")

        class_score = class_history_score({
            "current_class": cls,
            "prev1_class": ch.get("prev1_class", ""),
            "prev2_class": ch.get("prev2_class", ""),
            "prev3_class": ch.get("prev3_class", ""),
        })
        true_strength_score = calc_true_strength_score(s, ch, ex)
        b2_exception_bonus = calc_b2_exception_bonus(s, ch, ex, true_strength_score)
        local_specialist_bonus = calc_local_specialist_bonus(s)
        home_branch_bonus = calc_home_branch_bonus(s, jcd)

        tags = []

        def add_tag(sign, label):
            chip = f"{sign}{label}"
            if chip not in tags:
                tags.append(chip)

        elite_label = elite_racer_label(player_name)
        if elite_label:
            add_tag("+", elite_label)

        if national is not None:
            if true_strength_score >= 1.10 or national >= 6.1:
                add_tag("+", "勝率")
            elif national <= 4.9:
                add_tag("-", "勝率")
        elif local is not None:
            if local >= 6.6:
                add_tag("+", "勝率")
            elif local <= 4.8:
                add_tag("-", "勝率")

        if home_branch_bonus >= 0.05:
            add_tag("+", "地元")
        if local is not None:
            if local_specialist_bonus >= 0.08 or local >= 6.3:
                add_tag("+", "当地")
            elif local_specialist_bonus <= -0.08 or local <= 4.7:
                add_tag("-", "当地")

        motor_score = motor2 if motor2 is not None else None
        if motor_score is not None:
            if motor_score >= 42:
                add_tag("+", "モータ")
            elif motor_score <= 27:
                add_tag("-", "モータ")
        elif boat2 is not None:
            if boat2 >= 46:
                add_tag("+", "ボート")
            elif boat2 <= 25:
                add_tag("-", "ボート")

        if avg_st is not None:
            if avg_st <= 0.140:
                add_tag("+", "ST")
            elif avg_st >= 0.185:
                add_tag("-", "ST")

        if course_rate is not None and course_rate_source == "actual":
            if course_rate >= 66:
                add_tag("+", "コース")
            elif course_rate <= 24:
                add_tag("-", "コース")

        if recent_source == "actual" and (recent_avg is not None or recent_top3 is not None):
            if (recent_avg is not None and recent_avg <= 2.2) or (recent_top3 is not None and recent_top3 >= 76):
                add_tag("+", "近況")
            elif (recent_avg is not None and recent_avg >= 4.2) or (recent_top3 is not None and recent_top3 <= 38):
                add_tag("-", "近況")

        if cls or ch.get("prev1_class") or ch.get("prev2_class") or ch.get("prev3_class"):
            if class_score >= 0.7 or cls in {"A1", "A2"}:
                add_tag("+", "級別")
            elif class_score <= -0.35 or cls == "B2":
                add_tag("-", "級別")

        if b2_exception_bonus >= 0.15:
            add_tag("+", "B2補正")

        reason_map[lane] = tags[:6]

    return reason_map


def extract_course_recent_stats(lines):
    stats = {lane: {"course_rate": None, "course_rate_source": "", "avg_st": None, "recent_avg": None, "recent_top3": None, "recent_source": ""} for lane in range(1, 7)}
    lane_positions = [(idx, int(line)) for idx, line in enumerate(lines) if re.fullmatch(r"[1-6]", line)]
    for idx, lane in lane_positions:
        seg = lines[idx: idx + 80]
        joined = " ".join(seg)
        percents = []
        for x in re.findall(r"\d{1,3}(?:\.\d+)?", joined):
            try:
                val = float(x)
            except Exception:
                continue
            if 0 <= val <= 100:
                percents.append(val)
        st_vals = []
        for x in re.findall(r"(?:平均ST|ST)\s*([0-9]\.[0-9]{2})", joined):
            try:
                st_vals.append(float(x))
            except Exception:
                pass
        if not st_vals:
            for x in re.findall(r"\b0\.\d{2}\b", joined):
                try:
                    st_vals.append(float(x))
                except Exception:
                    pass
        course_candidates = [v for v in percents if 15 <= v <= 100]
        if course_candidates:
            stats[lane]["course_rate"] = max(course_candidates)
            stats[lane]["course_rate_source"] = "actual"
        if st_vals:
            valid_st = [v for v in st_vals if 0.05 <= v <= 0.35]
            if valid_st:
                stats[lane]["avg_st"] = min(valid_st)
        recent_places = []
        for x in re.findall(r"\b([1-6])\b", joined):
            recent_places.append(int(x))
        if len(recent_places) >= 3:
            rp = recent_places[:5]
            stats[lane]["recent_avg"] = round(sum(rp) / len(rp), 2)
            stats[lane]["recent_top3"] = round(sum(1 for v in rp if v <= 3) / len(rp) * 100, 1)
            stats[lane]["recent_source"] = "actual"
    return stats


def normalize_race_no_value(race_no):
    try:
        return int(str(race_no).replace("R", "").replace("r", "").strip())
    except Exception:
        m = re.search(r"(\d{1,2})", str(race_no or ""))
        return int(m.group(1)) if m else 0


def make_race_label(race_no):
    n = normalize_race_no_value(race_no)
    return f"{n}R" if n else f"{race_no}R"


def parse_beforeinfo_for_key(jcd, race_no):
    race_no = normalize_race_no_value(race_no)
    beforeinfo_url = build_beforeinfo_url(jcd, race_no)
    empty_info = {
        "boat_stats": {},
        "player_names": {},
        "branch_map": {},
        "extra_stats": {lane: {"course_rate": None, "avg_st": None, "recent_avg": None, "recent_top3": None} for lane in range(1, 7)},
    }
    try:
        html = fetch_html(beforeinfo_url)
    except Exception as e:
        log(f"[beforeinfo_error] jcd={jcd} race_no={race_no} err={e}")
        return (jcd, race_no), empty_info

    lines = normalize_lines(html)
    stats = {lane: {"class": "", "branch": "", "national_win": None, "local_win": None, "motor2": None, "boat2": None} for lane in range(1, 7)}
    lane_positions = [(idx, int(line)) for idx, line in enumerate(lines) if re.fullmatch(r"[1-6]", line)]
    branch_map = extract_branch_map_from_lines(lines)
    for pos_idx, lane in lane_positions:
        segment = lines[pos_idx:pos_idx + 40]
        joined = " ".join(segment)
        if branch_map.get(lane):
            stats[lane]["branch"] = branch_map.get(lane, "")
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
        if s["national_win"] is not None and (s["national_win"] > 10 or s["national_win"] <= 0):
            s["national_win"] = None
        if s["local_win"] is not None and (s["local_win"] > 10 or s["local_win"] <= 0):
            s["local_win"] = None
        if s["motor2"] is not None and (s["motor2"] > 100 or s["motor2"] <= 0):
            s["motor2"] = None
        if s["boat2"] is not None and (s["boat2"] > 100 or s["boat2"] <= 0):
            s["boat2"] = None

    player_names = extract_player_names_from_lines(lines)
    extra_stats = extract_course_recent_stats(lines)
    return (jcd, race_no), {
        "boat_stats": stats,
        "player_names": player_names,
        "branch_map": branch_map,
        "extra_stats": extra_stats,
    }


def parse_class_tokens_from_cell_text(text):
    cell_text = str(text or "").replace("\xa0", " ")
    tokens = re.findall(r"\b(A1|A2|B1|B2|-)\b", cell_text)
    tokens = tokens[:4]
    while len(tokens) < 4:
        tokens.append("")
    return {
        "current_class": "" if tokens[0] == "-" else tokens[0],
        "prev1_class": "" if tokens[1] == "-" else tokens[1],
        "prev2_class": "" if tokens[2] == "-" else tokens[2],
        "prev3_class": "" if tokens[3] == "-" else tokens[3],
    }


def normalize_name_cell_text(text):
    s = str(text or "").replace("\xa0", " ")
    s = s.replace("詳細", " ")
    s = re.sub(r"\(\d+\)", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def extract_name_from_racelist_cell_text(text):
    raw = normalize_name_cell_text(text)
    if not raw:
        return ""
    m = re.search(r"([一-龯]{1,4}\s*[一-龯ぁ-んァ-ヶー]{1,4})", raw)
    if m:
        candidate = normalize_player_name(m.group(1))
        if is_probable_player_name(candidate):
            return candidate
    return ""


def parse_racelist_table_bundle(html):
    soup = BeautifulSoup(html, "html.parser")
    best_bundle = {"class_history_map": {}, "player_names": {}}

    for table in soup.find_all("table"):
        table_text = table.get_text(" ", strip=True)
        if "級" not in table_text and "名前" not in table_text:
            continue

        lane_headers = {}
        name_map = {}
        class_map = {}

        for tr in table.find_all("tr"):
            cells = tr.find_all(["td", "th"], recursive=False)
            if len(cells) < 7:
                continue

            cell_texts = [c.get_text("\n", strip=True) for c in cells]
            row_label = re.sub(r"\s+", "", cell_texts[0])

            if not lane_headers:
                for idx, txt in enumerate(cell_texts[1:], start=1):
                    m = re.fullmatch(r"\s*([1-6])\s*", txt.replace("\n", " ").strip())
                    if m:
                        lane_headers[int(m.group(1))] = idx

            if "名前" in row_label:
                for lane, col_idx in lane_headers.items():
                    if col_idx < len(cells):
                        name = extract_name_from_racelist_cell_text(cells[col_idx].get_text("\n", strip=True))
                        if name:
                            name_map[lane] = name

            if "級" in row_label:
                for lane, col_idx in lane_headers.items():
                    if col_idx < len(cells):
                        class_map[lane] = parse_class_tokens_from_cell_text(cells[col_idx].get_text(" ", strip=True))

        if len(class_map) >= len(best_bundle["class_history_map"]):
            best_bundle["class_history_map"] = class_map
        if len(name_map) >= len(best_bundle["player_names"]):
            best_bundle["player_names"] = name_map

        if len(best_bundle["class_history_map"]) == 6 and len(best_bundle["player_names"]) == 6:
            break

    return best_bundle


def parse_racelist_race_from_html(html, race_no, jcd, venue):
    bundle = parse_racelist_table_bundle(html)
    class_map = bundle.get("class_history_map", {})
    name_map = bundle.get("player_names", {})
    bundle.update(extract_series_day_and_phase_from_text(html, race_no=race_no))

    if len(class_map) == 6:
        log(
            f"[racelist_race_ok] jcd={jcd} venue={venue} race_no={race_no} "
            f"names={len(name_map)} day={bundle.get('series_day', 0)} phase={bundle.get('race_phase', '')}"
        )
        return bundle

    text = normalize_text_for_class_parse(html)
    fallback_class_map = extract_class_block_tokens(text)
    if len(fallback_class_map) == 6:
        bundle["class_history_map"] = fallback_class_map
        log(
            f"[racelist_race_ok_fallback] jcd={jcd} venue={venue} race_no={race_no} "
            f"names={len(name_map)} day={bundle.get('series_day', 0)} phase={bundle.get('race_phase', '')}"
        )
        return bundle

    return bundle


def parse_racelist_page_all_races(jcd):
    venue = JCD_NAME_MAP.get(jcd, jcd)
    result = {}
    for race_no in range(1, 13):
        url_candidates = [
            build_racelist_detail_url(jcd, race_no, scheme="https"),
            build_racelist_detail_url(jcd, race_no, scheme="http"),
            build_info_detail_url(jcd, race_no),
        ]
        url_candidates = [u for u in url_candidates if u]
        html = None
        used_url = ""
        for url in url_candidates:
            html = try_fetch_html(url)
            if html:
                used_url = url
                break
        if not html:
            continue
        lane_map = parse_racelist_race_from_html(html, race_no, jcd, venue)

        if (
            not lane_map.get("series_day")
            and not lane_map.get("race_phase")
            and used_url != build_info_detail_url(jcd, race_no)
        ):
            info_url = build_info_detail_url(jcd, race_no)
            if info_url:
                info_html = try_fetch_html(info_url)
                if info_html:
                    lane_map.update(merge_race_meta(
                        extract_series_day_and_phase_from_text(info_html, race_no=race_no),
                        lane_map,
                    ))

        class_map = lane_map.get("class_history_map", {})
        name_map = lane_map.get("player_names", {})
        if class_map or name_map or lane_map.get("series_day") or lane_map.get("race_phase"):
            result[race_no] = lane_map
    log(f"[racelist_summary] jcd={jcd} venue={venue} races={len(result)}")
    return result


def parse_racelist_for_jcd(jcd):
    venue = JCD_NAME_MAP.get(jcd, jcd)
    if jcd not in RACELIST_VENUE_SLUG_MAP:
        log(f"[racelist_skip] jcd={jcd} venue={venue}")
        return jcd, {}
    return jcd, parse_racelist_page_all_races(jcd)


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


def parse_race_identity_from_text(text):
    m = re.search(r"(\d{2})\s*([^\s]+)\s*(\d{1,2})R", text)
    if not m:
        return None
    return {"jcd": m.group(1), "venue": m.group(2).strip(), "race_no": int(m.group(3))}


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
        race_text = cells[header_idx["race"]].get_text(" ", strip=True)
        info = parse_race_identity_from_text(race_text) or parse_race_identity_from_text(tr.get_text(" ", strip=True))
        if not info:
            continue
        triplets = triplets_from_digit_sequence(extract_digits_from_cell(cells[header_idx["trifecta"]]))
        if len(triplets) < 1:
            continue
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
    rows = list(dedup.values())
    log(f"[rating_page_summary_dom] {rating_text} count={len(rows)}")
    return rows


def parse_rating_page(rating_text):
    rows = parse_rating_page_dom(rating_text)
    log(f"[rating_page_summary] {rating_text} count={len(rows)} mode=dom")
    return rows


def parse_rating_pages(rating_texts=None):
    rating_texts = list(rating_texts or ALL_OFFICIAL_RATINGS)
    rows = []
    for rating_text in rating_texts:
        rows.extend(parse_rating_page(rating_text))

    dedup = {}
    for r in rows:
        key = (str(r.get("jcd") or "").strip(), int(r.get("race_no") or 0))
        if key not in dedup:
            dedup[key] = r
    merged = list(dedup.values())
    log(f"[rating_pages_summary] ratings={','.join(rating_texts)} count={len(merged)}")
    return merged


def class_point(cls):
    return {"A1": 1.25, "A2": 0.55, "B1": -0.05, "B2": -0.95}.get(cls, 0.0)


def class_history_score(class_history):
    cur = class_history.get("current_class", "")
    prev1 = class_history.get("prev1_class", "")
    prev2 = class_history.get("prev2_class", "")
    prev3 = class_history.get("prev3_class", "")
    score = 0.0
    score += class_point(cur) * 1.20
    score += class_point(prev1) * 0.85
    score += class_point(prev2) * 0.60
    score += class_point(prev3) * 0.45
    pattern = [cur, prev1, prev2, prev3]
    a1_count = sum(1 for x in pattern if x == "A1")
    a2_or_better_count = sum(1 for x in pattern if x in {"A1", "A2"})
    b2_count = sum(1 for x in pattern if x == "B2")
    if pattern[:3] == ["A1", "A1", "A1"]:
        score += 0.70
    if pattern[:4] == ["A1", "A1", "A1", "A1"]:
        score += 0.30
    if a1_count >= 2:
        score += 0.18
    if a2_or_better_count >= 3:
        score += 0.16
    if cur == "A1" and prev1 in {"A2", "B1"}:
        score += 0.15
    if cur == "A2" and prev1 == "B1":
        score += 0.10
    if cur in {"B1", "B2"} and prev1 == "A1":
        score -= 0.16
    if cur == "B2":
        score -= 0.22
    if b2_count >= 2:
        score -= 0.18
    return round(score, 3)


def make_class_history_text(class_history):
    return " / ".join([x for x in [
        class_history.get("current_class", ""),
        class_history.get("prev1_class", ""),
        class_history.get("prev2_class", ""),
        class_history.get("prev3_class", ""),
    ] if x])


def score_to_ai_rating_base(score):
    if score >= 2.6:
        return "AI★★★★★"
    if score >= 1.75:
        return "AI★★★★☆"
    if score >= 0.95:
        return "AI★★★☆☆"
    if score >= 0.15:
        return "AI★★☆☆☆"
    return "AI★☆☆☆☆"


VENUE_STYLE_BIAS = {
    "01": {1: 0.07, 2: 0.02, 3: -0.01, 4: -0.02, 5: -0.03, 6: -0.03, "reason": "インやや強め"},
    "02": {1: 0.03, 2: 0.06, 3: 0.01, 4: -0.01, 5: -0.04, 6: -0.04, "reason": "差しやや警戒"},
    "03": {1: -0.02, 2: 0.03, 3: 0.06, 4: 0.05, 5: -0.02, 6: -0.03, "reason": "攻めやや注意"},
    "04": {1: 0.03, 2: 0.04, 3: 0.06, 4: 0.05, 5: -0.02, 6: -0.03, "reason": "攻めやや注意"},
    "05": {1: 0.06, 2: 0.03, 3: 0.00, 4: -0.01, 5: -0.03, 6: -0.04, "reason": "インやや強め"},
    "06": {1: 0.04, 2: 0.05, 3: 0.02, 4: 0.01, 5: -0.03, 6: -0.04, "reason": "差しやや警戒"},
    "07": {1: 0.06, 2: 0.03, 3: 0.01, 4: -0.01, 5: -0.03, 6: -0.04, "reason": "インやや強め"},
    "08": {1: 0.03, 2: 0.03, 3: 0.03, 4: 0.02, 5: -0.03, 6: -0.04, "reason": "差しやや警戒"},
    "09": {1: 0.06, 2: 0.03, 3: 0.02, 4: -0.01, 5: -0.03, 6: -0.04, "reason": "インやや強め"},
    "10": {1: 0.07, 2: 0.02, 3: 0.01, 4: -0.01, 5: -0.03, 6: -0.04, "reason": "インやや強め"},
    "11": {1: 0.05, 2: 0.04, 3: 0.03, 4: 0.01, 5: -0.03, 6: -0.04, "reason": "差しやや警戒"},
    "12": {1: 0.05, 2: 0.03, 3: 0.04, 4: 0.03, 5: -0.02, 6: -0.03, "reason": "攻めやや注意"},
    "13": {1: 0.06, 2: 0.03, 3: 0.02, 4: 0.01, 5: -0.03, 6: -0.04, "reason": "差しやや警戒"},
    "14": {1: 0.08, 2: 0.02, 3: 0.00, 4: -0.01, 5: -0.04, 6: -0.04, "reason": "インやや強め"},
    "15": {1: 0.05, 2: 0.03, 3: 0.04, 4: 0.03, 5: -0.02, 6: -0.03, "reason": "攻めやや注意"},
    "16": {1: 0.06, 2: 0.03, 3: 0.01, 4: -0.01, 5: -0.03, 6: -0.04, "reason": "インやや強め"},
    "17": {1: 0.05, 2: 0.03, 3: 0.03, 4: 0.02, 5: -0.03, 6: -0.04, "reason": "差しやや警戒"},
    "20": {1: 0.05, 2: 0.05, 3: 0.04, 4: 0.02, 5: -0.02, 6: -0.03, "reason": "差しやや警戒"},
    "21": {1: 0.07, 2: 0.02, 3: 0.01, 4: 0.00, 5: -0.03, 6: -0.04, "reason": "インやや強め"},
    "22": {1: 0.05, 2: 0.04, 3: 0.02, 4: 0.01, 5: -0.03, 6: -0.04, "reason": "差しやや警戒"},
    "23": {1: 0.06, 2: 0.03, 3: 0.03, 4: 0.02, 5: -0.03, 6: -0.04, "reason": "攻めやや注意"},
    "24": {1: 0.07, 2: 0.03, 3: 0.02, 4: 0.00, 5: -0.03, 6: -0.04, "reason": "インやや強め"},
}



def clamp_num(v, low, high):
    return max(low, min(high, v))


def calc_b2_relief_strength(stat_row, class_history):
    stat_row = stat_row or {}
    class_history = class_history or {}

    national = stat_row.get("national_win")
    local = stat_row.get("local_win")
    prev_classes = [
        class_history.get("prev1_class", ""),
        class_history.get("prev2_class", ""),
        class_history.get("prev3_class", ""),
    ]

    relief = 0.0
    if national is not None:
        if national >= 7.0:
            relief += 0.55
        elif national >= 6.4:
            relief += 0.38
        elif national >= 6.0:
            relief += 0.22

    if local is not None:
        if local >= 6.5:
            relief += 0.30
        elif local >= 6.0:
            relief += 0.18

    if "A1" in prev_classes:
        relief += 0.24
    elif "A2" in prev_classes:
        relief += 0.12

    return clamp_num(relief, 0.0, 0.65)


def calc_true_strength_score(stat_row, class_history, extra_row):
    stat_row = stat_row or {}
    class_history = class_history or {}
    extra_row = extra_row or {}

    national = stat_row.get("national_win")
    local = stat_row.get("local_win")
    motor2 = stat_row.get("motor2")
    cls = stat_row.get("class") or class_history.get("current_class") or ""
    avg_st = extra_row.get("avg_st")
    recent_avg = extra_row.get("recent_avg")
    recent_top3 = extra_row.get("recent_top3")

    score = 0.0
    if national is not None:
        score += (national - 5.5) * 0.42
    if local is not None:
        score += (local - 5.3) * 0.20
    if motor2 is not None:
        score += (motor2 - 34.0) * 0.012

    class_score = class_history_score({
        "current_class": cls,
        "prev1_class": class_history.get("prev1_class", ""),
        "prev2_class": class_history.get("prev2_class", ""),
        "prev3_class": class_history.get("prev3_class", ""),
    })
    score += class_score * 0.44

    if avg_st is not None:
        if avg_st <= 0.130:
            score += 0.12
        elif avg_st <= 0.140:
            score += 0.06
        elif avg_st >= 0.185:
            score -= 0.05

    if recent_avg is not None:
        score += clamp_num((3.0 - recent_avg) * 0.12, -0.12, 0.16)

    if recent_top3 is not None:
        score += clamp_num((recent_top3 - 58.0) * 0.008, -0.10, 0.18)

    if cls == "A1":
        score += 0.10
    elif cls == "A2":
        score += 0.04
    elif cls == "B2":
        score -= 0.06

    return round(score, 3)


def calc_b2_exception_bonus(stat_row, class_history, extra_row, true_strength_score):
    stat_row = stat_row or {}
    class_history = class_history or {}
    extra_row = extra_row or {}

    cls = stat_row.get("class") or class_history.get("current_class") or ""
    if cls != "B2":
        return 0.0

    national = stat_row.get("national_win")
    local = stat_row.get("local_win")
    recent_top3 = extra_row.get("recent_top3")

    bonus = 0.0
    if true_strength_score >= 1.45:
        bonus += 0.14
    elif true_strength_score >= 1.10:
        bonus += 0.09

    if national is not None:
        if national >= 7.1:
            bonus += 0.08
        elif national >= 6.4:
            bonus += 0.04

    if local is not None:
        if local >= 6.6:
            bonus += 0.06
        elif local >= 6.1:
            bonus += 0.03

    if recent_top3 is not None and recent_top3 >= 68:
        bonus += 0.04

    prev_classes = [
        class_history.get("prev1_class", ""),
        class_history.get("prev2_class", ""),
        class_history.get("prev3_class", ""),
    ]
    if "A1" in prev_classes:
        bonus += 0.05
    elif "A2" in prev_classes:
        bonus += 0.02

    return round(clamp_num(bonus, 0.0, 0.22), 3)


def calc_local_specialist_bonus(stat_row):
    stat_row = stat_row or {}
    national = stat_row.get("national_win")
    local = stat_row.get("local_win")

    if national is None or local is None:
        return 0.0

    diff = local - national
    bonus = 0.0

    if diff >= 1.0:
        bonus += 0.18
    elif diff >= 0.6:
        bonus += 0.11
    elif diff >= 0.35:
        bonus += 0.05
    elif diff <= -1.0:
        bonus -= 0.10
    elif diff <= -0.6:
        bonus -= 0.06

    if local >= 6.8:
        bonus += 0.07
    elif local >= 6.2:
        bonus += 0.04
    elif local <= 4.8:
        bonus -= 0.03

    return round(clamp_num(bonus, -0.12, 0.24), 3)


def calc_home_branch_bonus(stat_row, jcd):
    stat_row = stat_row or {}
    branch = str(stat_row.get("branch") or "").strip()
    home_branch = HOME_BRANCH_BY_JCD.get(str(jcd or "").zfill(2), "")
    if not branch or not home_branch or branch != home_branch:
        return 0.0

    national = stat_row.get("national_win")
    local = stat_row.get("local_win")

    # 地元補正は残すが、あくまで軽めの補助材料にする
    bonus = 0.04

    if local is not None and national is not None and local >= national + 0.2:
        bonus += 0.02
    if local is not None and local >= 6.2:
        bonus += 0.02
    if national is not None and national <= 5.0:
        bonus -= 0.02
    if local is not None and local <= 5.0:
        bonus -= 0.02

    return round(clamp_num(bonus, 0.0, 0.08), 3)

def infer_extra_stats(boat_stats, class_history_map, extra_stats=None):
    merged = {}
    extra_stats = extra_stats or {}
    for lane in range(1, 7):
        ex = dict(extra_stats.get(lane, {}) or {})
        s = boat_stats.get(lane, {}) or {}
        ch = class_history_map.get(lane, {}) or {}
        cls = s.get("class") or ch.get("current_class") or ""
        national = s.get("national_win")
        local = s.get("local_win")
        motor2 = s.get("motor2")
        b2_relief = calc_b2_relief_strength(s, ch) if cls == "B2" else 0.0

        if ex.get("avg_st") is None:
            inferred_st = 0.17
            if cls == "A1":
                inferred_st -= 0.018
            elif cls == "A2":
                inferred_st -= 0.008
            elif cls == "B2":
                inferred_st += 0.018 * (1.0 - b2_relief)
            if national is not None:
                inferred_st -= max(min((national - 5.5) * 0.012, 0.025), -0.02)
            if local is not None:
                inferred_st -= max(min((local - 5.2) * 0.008, 0.015), -0.012)
            ex["avg_st"] = round(max(0.11, min(0.24, inferred_st)), 3)

        if ex.get("course_rate") is None:
            inferred_course = 46.0
            if lane == 1:
                inferred_course += 21.0
            elif lane == 2:
                inferred_course += 9.0
            elif lane == 3:
                inferred_course += 3.0
            elif lane >= 5:
                inferred_course -= 8.0
            if cls == "A1":
                inferred_course += 6.0
            elif cls == "A2":
                inferred_course += 2.5
            elif cls == "B2":
                inferred_course -= 6.0 * (1.0 - b2_relief)
            if national is not None:
                inferred_course += max(min((national - 5.5) * 4.8, 10.0), -8.0)
            if motor2 is not None:
                inferred_course += max(min((motor2 - 34.0) * 0.42, 7.0), -6.0)
            ex["course_rate"] = round(max(18.0, min(82.0, inferred_course)), 1)
            ex["course_rate_source"] = "inferred"
        elif not ex.get("course_rate_source"):
            ex["course_rate_source"] = "actual"

        if ex.get("recent_avg") is None:
            inferred_recent = 3.1
            if national is not None:
                inferred_recent -= max(min((national - 5.4) * 0.52, 0.9), -0.7)
            if local is not None:
                inferred_recent -= max(min((local - 5.2) * 0.28, 0.45), -0.35)
            if cls == "A1":
                inferred_recent -= 0.28
            elif cls == "B2":
                inferred_recent += 0.42 * (1.0 - b2_relief)
            ex["recent_avg"] = round(max(1.7, min(4.8, inferred_recent)), 2)
            ex["recent_source"] = "inferred"
        elif not ex.get("recent_source"):
            ex["recent_source"] = "actual"

        if ex.get("recent_top3") is None:
            inferred_top3 = 56.0
            if national is not None:
                inferred_top3 += max(min((national - 5.4) * 10.0, 18.0), -14.0)
            if motor2 is not None:
                inferred_top3 += max(min((motor2 - 34.0) * 0.85, 12.0), -10.0)
            if cls == "A1":
                inferred_top3 += 8.0
            elif cls == "B2":
                inferred_top3 -= 10.0 * (1.0 - b2_relief)
            ex["recent_top3"] = round(max(32.0, min(88.0, inferred_top3)), 1)

        merged[lane] = ex
    return merged


def build_base_triplets(sorted_lanes, head_lane, head_adv, second_adv):
    ordered = [lane for lane, _score in sorted_lanes]
    if not ordered:
        return ""
    triplets = []

    if head_lane == 1 and head_adv >= 0.55:
        second_choices = ordered[1:5]
        third_choices = ordered[1:6]
        for b in second_choices:
            for c in third_choices:
                if len({1, b, c}) < 3:
                    continue
                tri = normalize_triplet("1", str(b), str(c))
                if tri and tri not in triplets:
                    triplets.append(tri)
                if len(triplets) >= 6:
                    return " / ".join(triplets)

    elif head_lane in {2, 3, 4} and head_adv >= 0.38:
        follow = [lane for lane in ordered if lane != head_lane]
        for b in follow[:4]:
            for c in follow[:5]:
                if len({head_lane, b, c}) < 3:
                    continue
                tri = normalize_triplet(str(head_lane), str(b), str(c))
                if tri and tri not in triplets:
                    triplets.append(tri)
                if len(triplets) >= 6:
                    return " / ".join(triplets)

    priority_heads = [head_lane] + [lane for lane in ordered[:4] if lane != head_lane]
    for a in priority_heads:
        for b in ordered[:5]:
            for c in ordered[:6]:
                if len({a, b, c}) < 3:
                    continue
                tri = normalize_triplet(str(a), str(b), str(c))
                if tri and tri not in triplets:
                    triplets.append(tri)
                if len(triplets) >= 6:
                    return " / ".join(triplets)
    return " / ".join(triplets[:6])



def selection_triplets_base(selection):
    if not selection:
        return []
    return [x.strip() for x in str(selection).split(" / ") if x.strip()]


def analyze_base_raw_label(raw_score, reason_items=None):
    """
    素材評価用。枠有利・外頭抑制・頭の散り方などの買い目構造補正は入れず、
    勝率/当地/モーター/ST/コース/近況/級別/地元/別格などの素の強さだけを見る。
    """
    reason_items = [str(x).strip() for x in (reason_items or []) if str(x).strip()]
    reason_set = set(reason_items)

    score = 0.0
    try:
        s = float(raw_score or 0)
    except Exception:
        s = 0.0

    if s >= 2.45:
        score += 1.55
    elif s >= 2.00:
        score += 1.05
    elif s >= 1.55:
        score += 0.60
    elif s >= 1.15:
        score += 0.20
    else:
        score -= 0.38

    strong_support_words = [
        "地力上位", "選手力上位", "B2でも地力上位", "別格", "実力A1級",
        "当地巧者", "地元水面", "モーター良好", "ST良好", "コース相性良好",
        "近況良好", "級別傾向強い",
    ]
    support_count = sum(1 for w in strong_support_words if w in reason_set)

    if support_count >= 4:
        score += 1.00
    elif support_count == 3:
        score += 0.72
    elif support_count == 2:
        score += 0.44
    elif support_count == 1:
        score += 0.14
    else:
        score -= 0.18

    if score >= 2.35:
        label = "base素材◎"
    elif score >= 1.10:
        label = "base素材○"
    elif score >= 0.15:
        label = "base素材保留"
    else:
        label = "base素材危険"

    notes = []
    if support_count:
        notes.append(f"支え{support_count}")
    if s >= 2.45:
        notes.append("上位明確")
    elif s <= 1.10:
        notes.append("材料薄め")

    return {
        "label": label,
        "score": round(score, 2),
        "reason_text": "/".join(notes[:2]),
    }


def build_base_head_reasons(head_lane, boat_stats, class_history_map, extra_stats=None, jcd="", player_names_map=None):
    reasons = []
    if not head_lane:
        return reasons
    s = boat_stats.get(head_lane, {}) or {}
    ex = (extra_stats or {}).get(head_lane, {}) or {}
    ch = class_history_map.get(head_lane, {}) or {}
    head_true_strength = calc_true_strength_score(s, ch, ex)
    head_b2_exception = calc_b2_exception_bonus(s, ch, ex, head_true_strength)
    head_local_bonus = calc_local_specialist_bonus(s)
    head_home_bonus = calc_home_branch_bonus(s, jcd)
    head_elite_label = elite_racer_label((player_names_map or {}).get(head_lane, ""))

    if head_elite_label:
        reasons.append(head_elite_label)
    if head_true_strength >= 1.10 or (s.get("national_win") or 0) >= 6.2:
        reasons.append("地力上位")
    elif (s.get("national_win") or 0) >= 6.0:
        reasons.append("選手力上位")
    if head_b2_exception >= 0.12:
        reasons.append("B2でも地力上位")
    if head_local_bonus >= 0.10:
        reasons.append("当地巧者")
    elif (s.get("local_win") or 0) >= 5.8:
        reasons.append("当地相性良好")
    if head_home_bonus >= 0.06:
        reasons.append("地元水面")
    if (s.get("motor2") or 0) >= 42:
        reasons.append("モーター良好")
    if (ex.get("avg_st") or 9) <= 0.140:
        reasons.append("ST良好")
    if (ex.get("course_rate") or 0) >= 60:
        reasons.append("コース相性良好")
    if (ex.get("recent_avg") or 9) <= 2.3 or (ex.get("recent_top3") or 0) >= 72:
        reasons.append("近況良好")
    if class_history_score(ch) >= 1.0:
        reasons.append("級別傾向強い")
    if head_lane == 1:
        reasons.append("1号艇有利")
    elif head_lane in {3, 4}:
        reasons.append("中枠攻め候補")
    return reasons


def analyze_base_quality_label(base_ai_score, base_ai_selection, reason_items=None):
    """
    公式★を使わず、朝baseだけで土台の良し悪しを判定する。
    表示/CSVにすぐ乗せられるよう、labelは base_reason_text の先頭へ付ける想定。
    """
    reason_items = [str(x).strip() for x in (reason_items or []) if str(x).strip()]
    reason_set = set(reason_items)
    triplets = selection_triplets_base(base_ai_selection)

    heads = []
    for tri in triplets:
        parts = str(tri).split("-")
        if len(parts) >= 3 and parts[0].isdigit():
            try:
                heads.append(int(parts[0]))
            except Exception:
                pass

    first_head = heads[0] if heads else 0
    top3_heads = heads[:3]
    unique_top3_heads = len(set(top3_heads)) if top3_heads else 0
    outer_head_count = sum(1 for x in heads[:6] if x in {5, 6})

    score = 0.0
    try:
        s = float(base_ai_score or 0)
    except Exception:
        s = 0.0

    if s >= 2.65:
        score += 1.55
    elif s >= 2.25:
        score += 1.10
    elif s >= 1.75:
        score += 0.60
    elif s >= 1.30:
        score += 0.15
    else:
        score -= 0.45

    strong_support_words = [
        "地力上位", "選手力上位", "B2でも地力上位", "別格", "実力A1級",
        "当地巧者", "地元水面", "モーター良好", "ST良好", "コース相性良好",
        "近況良好", "級別傾向強い", "1号艇有利", "インやや強め",
    ]
    support_count = sum(1 for w in strong_support_words if w in reason_set)

    if support_count >= 4:
        score += 1.00
    elif support_count == 3:
        score += 0.75
    elif support_count == 2:
        score += 0.45
    elif support_count == 1:
        score += 0.12
    else:
        score -= 0.25

    if first_head == 1:
        score += 0.70
        if "1号艇有利" in reason_set and support_count >= 2:
            score += 0.20
    elif first_head == 2:
        score += 0.35
    elif first_head == 3:
        score += 0.18
    elif first_head == 4:
        score -= 0.12
    elif first_head in {5, 6}:
        score -= 0.75
    else:
        score -= 0.50

    if unique_top3_heads == 1:
        score += 0.28
    elif unique_top3_heads == 2:
        score += 0.12
    elif unique_top3_heads >= 3:
        score -= 0.32

    if outer_head_count >= 3:
        score -= 0.58
    elif outer_head_count >= 2:
        score -= 0.34

    if "中枠攻め候補" in reason_set and support_count <= 2:
        score -= 0.22
    if "攻めやや注意" in reason_set and support_count <= 2:
        score -= 0.18

    if not triplets:
        score -= 1.00

    if score >= 2.55:
        label = "base土台◎"
    elif score >= 1.25:
        label = "base土台○"
    elif score >= 0.20:
        label = "base保留"
    else:
        label = "base危険"

    notes = []
    if first_head:
        notes.append(f"頭{first_head}")
    if support_count:
        notes.append(f"支え{support_count}")
    if unique_top3_heads >= 3:
        notes.append("頭散り")
    if outer_head_count >= 2:
        notes.append("外頭多め")

    return {
        "label": label,
        "score": round(score, 2),
        "reason_text": "/".join(notes[:3]),
    }


def generate_base_ai_selection(boat_stats, class_history_map, extra_stats=None, venue="", jcd="", player_names_map=None):
    lane_scores = {lane: 0.0 for lane in range(1, 7)}
    extra_stats = infer_extra_stats(boat_stats, class_history_map, extra_stats)
    venue_bias = VENUE_STYLE_BIAS.get(str(jcd or "").zfill(2), {})

    for lane in range(1, 7):
        s = boat_stats.get(lane, {})
        ch = class_history_map.get(lane, {})
        ex = extra_stats.get(lane, {})
        player_name = (player_names_map or {}).get(lane, "")

        national = s.get("national_win")
        local = s.get("local_win")
        motor2 = s.get("motor2")
        boat2 = s.get("boat2")
        cls = s.get("class") or ch.get("current_class") or ""
        class_score = class_history_score({
            "current_class": cls,
            "prev1_class": ch.get("prev1_class", ""),
            "prev2_class": ch.get("prev2_class", ""),
            "prev3_class": ch.get("prev3_class", ""),
        })
        true_strength_score = calc_true_strength_score(s, ch, ex)
        b2_exception_bonus = calc_b2_exception_bonus(s, ch, ex, true_strength_score)
        local_specialist_bonus = calc_local_specialist_bonus(s)
        home_branch_bonus = calc_home_branch_bonus(s, jcd)
        elite_bonus = elite_racer_bonus(player_name)

        if national is not None:
            lane_scores[lane] += (national - 5.3) * 0.32
        if local is not None:
            lane_scores[lane] += (local - 5.1) * 0.16
        if motor2 is not None:
            lane_scores[lane] += (motor2 - 34.0) * 0.020
        if boat2 is not None:
            lane_scores[lane] += (boat2 - 33.0) * 0.006

        lane_scores[lane] += class_score * 0.96

        if cls == "A1":
            lane_scores[lane] += 0.16
        elif cls == "A2":
            lane_scores[lane] += 0.05
        elif cls == "B2":
            lane_scores[lane] -= max(0.06, 0.18 - b2_exception_bonus)

        lane_scores[lane] += true_strength_score * 0.22
        lane_scores[lane] += local_specialist_bonus
        lane_scores[lane] += home_branch_bonus
        lane_scores[lane] += elite_bonus

        st = ex.get("avg_st")
        cr = ex.get("course_rate")
        cr_source = str(ex.get("course_rate_source") or "")
        recent_avg = ex.get("recent_avg")
        recent_top3 = ex.get("recent_top3")
        recent_source = str(ex.get("recent_source") or "")

        if st is not None:
            if st <= 0.130:
                lane_scores[lane] += 0.14
            elif st <= 0.140:
                lane_scores[lane] += 0.08
            elif st >= 0.185:
                lane_scores[lane] -= 0.10

        if cr is not None:
            if cr_source == "actual":
                if cr >= 72:
                    lane_scores[lane] += 0.24
                elif cr >= 64:
                    lane_scores[lane] += 0.12
                elif cr <= 24:
                    lane_scores[lane] -= 0.12
            else:
                if cr >= 74:
                    lane_scores[lane] += 0.08
                elif cr >= 66:
                    lane_scores[lane] += 0.04
                elif cr <= 22:
                    lane_scores[lane] -= 0.06

        if recent_source == "actual":
            if recent_avg is not None:
                if recent_avg <= 2.15:
                    lane_scores[lane] += 0.14
                elif recent_avg >= 4.1:
                    lane_scores[lane] -= 0.10

            if recent_top3 is not None:
                if recent_top3 >= 78:
                    lane_scores[lane] += 0.10
                elif recent_top3 <= 40:
                    lane_scores[lane] -= 0.08
        else:
            if recent_avg is not None:
                if recent_avg <= 2.0:
                    lane_scores[lane] += 0.05
                elif recent_avg >= 4.3:
                    lane_scores[lane] -= 0.04

            if recent_top3 is not None:
                if recent_top3 >= 82:
                    lane_scores[lane] += 0.04
                elif recent_top3 <= 36:
                    lane_scores[lane] -= 0.03

        if lane == 1:
            lane_scores[lane] += 0.22
        elif lane == 2:
            lane_scores[lane] += 0.08
        elif lane >= 5:
            lane_scores[lane] -= 0.06

        if venue_bias:
            lane_scores[lane] += venue_bias.get(lane, 0.0)

    lane1_cls = boat_stats.get(1, {}).get("class") or class_history_map.get(1, {}).get("current_class")
    lane1_true_strength = calc_true_strength_score(
        boat_stats.get(1, {}) or {},
        class_history_map.get(1, {}) or {},
        extra_stats.get(1, {}) or {},
    )
    if lane1_cls == "A1":
        lane_scores[1] += 0.10
    elif lane1_cls == "B2":
        lane_scores[1] -= max(0.08, 0.20 - calc_b2_exception_bonus(
            boat_stats.get(1, {}) or {},
            class_history_map.get(1, {}) or {},
            extra_stats.get(1, {}) or {},
            lane1_true_strength,
        ))
    elif lane1_cls == "B1":
        lane_scores[1] -= 0.12

    one_cls = boat_stats.get(1, {}).get("class") or class_history_map.get(1, {}).get("current_class") or ""
    for lane in (3, 4):
        cls = boat_stats.get(lane, {}).get("class") or class_history_map.get(lane, {}).get("current_class") or ""
        motor2 = boat_stats.get(lane, {}).get("motor2")
        if cls == "A1" and one_cls in {"A2", "B1", "B2", ""}:
            lane_scores[lane] += 0.08
        if motor2 is not None and motor2 >= 42:
            lane_scores[lane] += 0.10

    raw_lane_scores = dict(lane_scores)

    sorted_lanes = sorted(lane_scores.items(), key=lambda x: (-x[1], x[0]))
    top_lanes = [lane for lane, _ in sorted_lanes]
    top_score = sorted_lanes[0][1] if sorted_lanes else 0.0
    second_score = sorted_lanes[1][1] if len(sorted_lanes) > 1 else top_score - 0.2
    fourth_score = sorted_lanes[3][1] if len(sorted_lanes) > 3 else second_score - 0.2
    head_adv = top_score - second_score
    second_adv = second_score - fourth_score
    base_score = top_score + head_adv * 0.80 + second_adv * 0.30
    if top_lanes and top_lanes[0] == 1 and head_adv >= 0.60:
        base_score += 0.18
    elif top_lanes and top_lanes[0] in {3, 4} and head_adv >= 0.38:
        base_score += 0.12

    ai_rating = score_to_ai_rating_base(base_score)
    base_selection = build_base_triplets(sorted_lanes, top_lanes[0] if top_lanes else 1, head_adv, second_adv)

    reasons = build_base_head_reasons(
        top_lanes[0] if top_lanes else None,
        boat_stats,
        class_history_map,
        extra_stats=extra_stats,
        jcd=jcd,
        player_names_map=player_names_map,
    )

    if venue_bias.get("reason") and head_adv >= 0.30:
        reasons.append(venue_bias["reason"])

    base_reason_text = " / ".join(dict.fromkeys(reasons[:7]))
    base_quality = analyze_base_quality_label(base_score, base_selection, reasons)
    base_quality_label = str(base_quality.get("label") or "").strip()
    if base_quality_label:
        base_reason_text = f"{base_quality_label} / {base_reason_text}" if base_reason_text else base_quality_label

    raw_sorted_lanes = sorted(raw_lane_scores.items(), key=lambda x: (-x[1], x[0]))
    raw_top_lanes = [lane for lane, _ in raw_sorted_lanes]
    raw_top_score = raw_sorted_lanes[0][1] if raw_sorted_lanes else 0.0
    raw_second_score = raw_sorted_lanes[1][1] if len(raw_sorted_lanes) > 1 else raw_top_score - 0.2
    raw_third_score = raw_sorted_lanes[2][1] if len(raw_sorted_lanes) > 2 else raw_second_score - 0.1
    raw_score = raw_top_score + (raw_second_score * 0.42) + (raw_third_score * 0.18)
    raw_reasons = build_base_head_reasons(
        raw_top_lanes[0] if raw_top_lanes else None,
        boat_stats,
        class_history_map,
        extra_stats=extra_stats,
        jcd=jcd,
        player_names_map=player_names_map,
    )
    base_raw = analyze_base_raw_label(raw_score, raw_reasons)
    base_raw_label = str(base_raw.get("label") or "").strip()
    raw_reason_text = " / ".join(dict.fromkeys(raw_reasons[:6]))
    if base_raw_label:
        raw_reason_text = f"{base_raw_label} / {raw_reason_text}" if raw_reason_text else base_raw_label

    return {
        "base_ai_score": round(base_score, 2),
        "base_ai_rating": ai_rating,
        "base_ai_selection": base_selection,
        "base_reason_text": base_reason_text,
        "base_raw_score": round(raw_score, 2),
        "base_raw_label": base_raw_label,
        "base_raw_reason_text": raw_reason_text,
    }


def fill_missing_deadlines(rows, deadlines_cache):
    filled = 0
    for row in rows:
        jcd = row["jcd"] or NAME_JCD_MAP.get(row["venue"], "")
        if not jcd:
            continue
        if deadlines_cache.get(jcd, {}).get(row["race_no"], ""):
            continue
        single_deadline = parse_single_race_deadline(jcd, row["race_no"])
        if single_deadline:
            deadlines_cache.setdefault(jcd, {})[row["race_no"]] = single_deadline
            filled += 1
            log(f"[official_single_ok] jcd={jcd} venue={row['venue']} race_no={row['race_no']} time={single_deadline}")
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
        return results
    with ThreadPoolExecutor(max_workers=RACELIST_MAX_WORKERS) as ex:
        futures = [ex.submit(parse_racelist_for_jcd, jcd) for jcd in supported]
        for future in as_completed(futures):
            try:
                jcd, info = future.result()
                results[jcd] = info
            except Exception as e:
                log(f"[racelist_parallel_error] err={e}")
    return results


def log_beforeinfo_summary(beforeinfo_cache, keys):
    total = len(keys)
    fetched = len(beforeinfo_cache)
    names_count = 0
    extra_count = 0
    for _key, info in beforeinfo_cache.items():
        if info.get("player_names"):
            names_count += 1
        if any(isinstance(info.get("extra_stats", {}).get(lane, {}).get("course_rate"), (int, float)) for lane in range(1, 7)):
            extra_count += 1
    log(f"[beforeinfo_summary] targets={total} fetched={fetched} names={names_count} extras={extra_count}")


def is_shadow_candidate_quality_ok(candidate):
    score = safe_float(candidate.get("base_ai_score", 0), 0)
    rating = str(candidate.get("base_ai_rating") or "").strip()
    selection = str(candidate.get("base_ai_selection") or "").strip()
    if not selection:
        return False
    if rating in {"AI★★★★★", "AI★★★★☆"}:
        return score >= SHADOW_AI_MIN_SCORE
    return False


def make_base_candidate(row, beforeinfo_cache, racelist_cache, candidate_source="official_star"):
    venue = row["venue"]
    race_no = int(row["race_no"])
    rating = str(row.get("rating") or "").strip()
    selection = str(row.get("selection") or "").strip()
    jcd = row.get("jcd") or NAME_JCD_MAP.get(venue, "")

    beforeinfo = beforeinfo_cache.get((jcd, race_no), {})
    boat_stats = beforeinfo.get("boat_stats", {})
    beforeinfo_player_names = beforeinfo.get("player_names", {})
    extra_stats = beforeinfo.get("extra_stats", {})
    racelist_race_info = racelist_cache.get(jcd, {}).get(race_no, {})
    class_history_map = racelist_race_info.get("class_history_map", {})
    racelist_player_names = racelist_race_info.get("player_names", {})
    player_names = merge_player_name_maps(racelist_player_names, beforeinfo_player_names)
    player_reason_map = build_player_reason_map(
        boat_stats,
        class_history_map,
        extra_stats,
        jcd=jcd,
        player_names_map=player_names,
    )
    series_day = int(racelist_race_info.get("series_day") or 0)
    race_phase = normalize_race_phase_label(racelist_race_info.get("race_phase") or "")

    base_ai = generate_base_ai_selection(
        boat_stats,
        class_history_map,
        extra_stats,
        venue=venue,
        jcd=jcd,
        player_names_map=player_names,
    )

    class_history_text = " / ".join(
        [
            f"{lane}:{make_class_history_text(class_history_map.get(lane, {}))}"
            for lane in range(1, 7)
            if class_history_map.get(lane)
        ]
    ) if class_history_map else ""
    player_names_text = make_player_names_text(player_names)

    candidate = {
        "race_date": today_text(),
        "time": row.get("time", ""),
        "venue": venue,
        "race_no": make_race_label(race_no),
        "race_no_num": race_no,
        "candidate_source": candidate_source,
        "rating": rating,
        "bet_type": BET_TYPE,
        "selection": selection,
        "amount": BET_AMOUNT,
        "player_names_text": player_names_text,
        "class_history_text": class_history_text,
        "player_reason_text": make_player_reason_text(player_reason_map),
        "series_day": series_day,
        "race_phase": race_phase,
        "base_ai_score": base_ai["base_ai_score"],
        "base_ai_rating": base_ai["base_ai_rating"],
        "base_ai_selection": base_ai["base_ai_selection"],
        "base_reason_text": base_ai["base_reason_text"],
        "base_raw_score": base_ai.get("base_raw_score", 0),
        "base_raw_label": base_ai.get("base_raw_label", ""),
        "base_raw_reason_text": base_ai.get("base_raw_reason_text", ""),
        "base_updated_at": jst_now_str(),
    }
    return candidate


def build_shadow_seed_rows(active_jcds, deadlines_cache, official_key_set, all_rating_map=None):
    if not ENABLE_SHADOW_AI:
        return []

    all_rating_map = all_rating_map or {}
    seed_rows = []
    for jcd in sorted(active_jcds):
        venue = JCD_NAME_MAP.get(jcd, jcd)
        deadlines = deadlines_cache.get(jcd, {}) or {}
        for race_no in range(1, 13):
            deadline = deadlines.get(race_no, "")
            if not deadline:
                continue
            key = (jcd, race_no)
            if SHADOW_AI_SKIP_OFFICIAL_DUPLICATES and key in official_key_set:
                continue
            rating_info = all_rating_map.get(key, {}) or {}
            seed_rows.append({
                "venue": venue,
                "jcd": jcd,
                "race_no": race_no,
                "rating": str(rating_info.get("rating") or "").strip(),
                "selection": str(rating_info.get("selection") or "").strip(),
                "time": deadline,
            })
    return seed_rows


def build_all_race_seed_rows(active_jcds, deadlines_cache, all_rating_map=None):
    if not ENABLE_ALL_RACE_AI:
        return []

    all_rating_map = all_rating_map or {}
    seed_rows = []
    for jcd in sorted(active_jcds):
        venue = JCD_NAME_MAP.get(jcd, jcd)
        deadlines = deadlines_cache.get(jcd, {}) or {}
        for race_no in range(1, 13):
            deadline = deadlines.get(race_no, "")
            if not deadline:
                continue
            rating_info = all_rating_map.get((jcd, race_no), {}) or {}
            seed_rows.append({
                "venue": venue,
                "jcd": jcd,
                "race_no": race_no,
                "rating": str(rating_info.get("rating") or "").strip(),
                "selection": str(rating_info.get("selection") or "").strip(),
                "time": deadline,
            })
    return seed_rows


def build_candidates():
    log("[collector_version] collector_base_v10_39_base_quality")
    log("========== build_candidates start ==========")
    log(f"now={jst_now().strftime('%Y-%m-%d %H:%M:%S JST')}")
    log("[official_all_config] source=official_all ratings=★1〜★5 one_row_per_race")

    # 公式★1〜★5を全部拾い、1レース1行で保存する。
    # 画面側で「公式候補」「裏AI候補」「全レース検証」を分類するため、
    # collector側では official_star / shadow_ai / all_race_ai に分けない。
    all_rating_rows = parse_rating_pages(ALL_OFFICIAL_RATINGS)

    dedup = {}
    for row in all_rating_rows:
        jcd = row.get("jcd") or NAME_JCD_MAP.get(row.get("venue", ""), "")
        race_no = int(row.get("race_no") or 0)
        if not jcd or race_no <= 0:
            continue
        key = (jcd, race_no)
        if key not in dedup:
            row["jcd"] = jcd
            row["race_no"] = race_no
            row["candidate_source"] = "official_all"
            dedup[key] = row

    official_all_rows = list(dedup.values())
    log(f"[official_all_seed_summary] rating_rows={len(all_rating_rows)} unique={len(official_all_rows)}")

    needed_jcds = set()
    for row in official_all_rows:
        jcd = row.get("jcd") or NAME_JCD_MAP.get(row.get("venue", ""), "")
        if jcd:
            needed_jcds.add(jcd)

    deadlines_cache = fetch_deadlines_parallel(needed_jcds)
    deadlines_cache = fill_missing_deadlines(official_all_rows, deadlines_cache)
    racelist_cache = fetch_racelist_parallel(needed_jcds) if USE_RACELIST else {}

    filtered_rows = []
    for row in official_all_rows:
        venue = row["venue"]
        race_no = int(row["race_no"])
        jcd = row.get("jcd") or NAME_JCD_MAP.get(venue, "")
        if not jcd:
            continue
        deadline = deadlines_cache.get(jcd, {}).get(race_no, "")
        if not deadline:
            continue
        row["time"] = deadline
        row["candidate_source"] = "official_all"
        filtered_rows.append(row)

    all_keys = set()
    for row in filtered_rows:
        jcd = row.get("jcd") or NAME_JCD_MAP.get(row.get("venue", ""), "")
        if jcd:
            all_keys.add((jcd, int(row["race_no"])))

    beforeinfo_cache = fetch_beforeinfo_parallel(all_keys) if all_keys else {}
    if all_keys:
        log_beforeinfo_summary(beforeinfo_cache, all_keys)

    results = []
    for row in filtered_rows:
        candidate = make_base_candidate(
            row,
            beforeinfo_cache,
            racelist_cache,
            candidate_source="official_all",
        )
        results.append(candidate)

    results.sort(key=lambda x: (to_minutes(x["time"]) if x.get("time") else 9999, x["venue"], x["race_no_num"]))
    log(f"build_candidates final_count={len(results)} official_all={len(results)}")
    log("========== build_candidates end ==========")
    return results


def chunk_list(items, size):
    size = max(1, int(size or 1))
    for i in range(0, len(items), size):
        yield items[i:i + size]


def split_races_for_post(races):
    """
    all_race_ai 追加後は送信件数が多くなるため、Render側の500/タイムアウトを避ける目的で分割送信する。
    official_star → shadow_ai → all_race_ai の順で送る。
    """
    order = ["official_all", "official_star", "shadow_ai", "all_race_ai"]
    grouped = {src: [] for src in order}
    others = []

    for race in races:
        src = str((race or {}).get("candidate_source") or "official_all").strip() or "official_all"
        if src in grouped:
            grouped[src].append(race)
        else:
            others.append(race)

    ordered = []
    for src in order:
        ordered.extend(grouped[src])
    ordered.extend(others)
    return ordered


def send_to_render(races):
    if not RENDER_IMPORT_URL:
        raise RuntimeError("RENDER_IMPORT_URL が未設定です")
    if not IMPORT_TOKEN:
        raise RuntimeError("IMPORT_TOKEN が未設定です")

    headers = {"Content-Type": "application/json", "X-IMPORT-TOKEN": IMPORT_TOKEN}

    ordered_races = split_races_for_post(races)
    chunk_size = env_int("RENDER_POST_CHUNK_SIZE", 60)
    chunks = list(chunk_list(ordered_races, chunk_size))
    total_received = 0
    total_inserted = 0
    total_updated = 0

    log(f"[render_post_plan] total={len(ordered_races)} chunk_size={chunk_size} chunks={len(chunks)}")

    for chunk_idx, chunk in enumerate(chunks, start=1):
        payload = {"races": chunk}
        source_counts = {}
        for race in chunk:
            src = str((race or {}).get("candidate_source") or "official_all").strip() or "official_all"
            source_counts[src] = source_counts.get(src, 0) + 1

        last_err = None
        for attempt in range(1, 6):
            try:
                log(
                    f"[render_post_try] chunk={chunk_idx}/{len(chunks)} attempt={attempt} "
                    f"url={RENDER_IMPORT_URL} races={len(chunk)} sources={source_counts}"
                )
                health_url = RENDER_IMPORT_URL.replace("/api/import_base_candidates", "/healthz")
                try:
                    hr = requests.get(health_url, headers=HEADERS, timeout=(5, 10))
                    log(f"[render_health] chunk={chunk_idx}/{len(chunks)} attempt={attempt} status={hr.status_code}")
                except Exception as he:
                    log(f"[render_health_err] chunk={chunk_idx}/{len(chunks)} attempt={attempt} err={he}")

                res = requests.post(RENDER_IMPORT_URL, headers=headers, json=payload, timeout=POST_TIMEOUT)
                print("status_code =", res.status_code)
                print("response =", res.text[:2000])

                if res.status_code in (500, 502, 503, 504):
                    sample = [
                        f"{x.get('candidate_source','')}:{x.get('venue','')}{x.get('race_no','')}"
                        for x in chunk[:8]
                    ]
                    raise requests.exceptions.HTTPError(
                        f"{res.status_code} Server Error chunk={chunk_idx}/{len(chunks)} sample={sample}: {res.text[:500]}",
                        response=res,
                    )

                res.raise_for_status()
                data = {}
                try:
                    data = res.json()
                except Exception:
                    data = {}
                total_received += int(data.get("received") or len(chunk))
                total_inserted += int(data.get("inserted") or 0)
                total_updated += int(data.get("updated") or 0)
                log(f"[render_post_ok] chunk={chunk_idx}/{len(chunks)}")
                break
            except Exception as e:
                last_err = e
                log(f"[render_post_retry] chunk={chunk_idx}/{len(chunks)} attempt={attempt} err={e}")
                if attempt < 5:
                    time.sleep(8 * attempt)
                else:
                    log(f"[render_post_failed] chunk={chunk_idx}/{len(chunks)} err={e}")
                    raise last_err

    log(
        f"[render_post_all_ok] chunks={len(chunks)} received={total_received} "
        f"inserted={total_inserted} updated={total_updated}"
    )
    return


def main():
    races = build_candidates()
    if not races:
        print("候補が0件でした")
        return
    send_to_render(races)


if __name__ == "__main__":
    main()
