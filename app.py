from datetime import datetime, timezone, timedelta
import os
import re
import json
import csv
import io
from urllib.parse import quote

import psycopg2
import psycopg2.extras
from flask import Flask, request, redirect, jsonify, Response

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
EXTERNAL_URL = os.environ.get("EXTERNAL_URL", "").strip()
IMPORT_TOKEN = os.environ.get("IMPORT_TOKEN", "").strip()

JST = timezone(timedelta(hours=9))

AI_RATING_OPTIONS = [
    "",
    "AI★★★★★",
    "AI★★★★☆",
    "AI★★★☆☆",
    "AI★★☆☆☆",
    "AI★☆☆☆☆",
]

OFFICIAL_RATING_FILTER_OPTIONS = [
    ("pickup", "公式★5+★4"),
    ("★★★★★", "公式★5のみ"),
    ("★★★★☆", "公式★4のみ"),
    ("★★★☆☆", "公式★3のみ"),
    ("★★☆☆☆", "公式★2のみ"),
    ("★☆☆☆☆", "公式★1のみ"),
]

CARD_SELECT_COLUMNS = '''
    id,
    race_date,
    time,
    venue,
    race_no,
    race_no_num,
    candidate_source,
    rating,
    bet_type,
    selection,
    amount,
    ai_reasons,
    exhibition,
    exhibition_rank,
    weather,
    wind_speed,
    wave_height,
    wind_type,
    wind_dir,
    water_state_score,
    ai_lane_score_text,
    class_history_text,
    player_names_text,
    player_stat_text,
    player_reason_text,
    ai_score,
    ai_rating,
    ai_selection,
    ai_detail,
    ai_confidence,
    base_ai_score,
    base_ai_rating,
    base_ai_selection,
    base_reason_text,
    base_updated_at,
    final_ai_score,
    final_ai_rating,
    final_ai_selection,
    final_rank,
    latest_reason_text,
    latest_updated_at,
    purchased,
    purchased_selection_text,
    hit,
    payout,
    memo,
    result_trifecta_text,
    result_trifecta_payout,
    result_exacta_text,
    result_exacta_payout,
    result_trio_text,
    result_trio_payout,
    settled_flag,
    settled_at,
    result_source_url,
    imported_at
'''

POINT_COUNT_SQL = """
CASE
    WHEN COALESCE(BTRIM(purchased_selection_text), '') = '' THEN 0
    ELSE COALESCE(array_length(string_to_array(purchased_selection_text, ' / '), 1), 0)
END
"""

AUTO_HIT_SQL = """
CASE
    WHEN COALESCE(BTRIM(result_trifecta_text), '') <> ''
     AND COALESCE(BTRIM(purchased_selection_text), '') <> ''
     AND REPLACE(result_trifecta_text, ' ', '') = ANY(string_to_array(REPLACE(purchased_selection_text, ' ', ''), '/'))
    THEN 1
    ELSE 0
END
"""

AUTO_PAYOUT_SQL = f"""
CASE
    WHEN {AUTO_HIT_SQL} = 1 THEN CAST(ROUND(COALESCE(result_trifecta_payout, 0) * COALESCE(NULLIF(amount, 0), 100) / 100.0) AS INTEGER)
    ELSE 0
END
"""

ALLOWED_GROUP_COLUMNS = {
    "rating": "rating",
    "venue": "venue",
    "ai_rating": "COALESCE(NULLIF(final_ai_rating, ''), NULLIF(base_ai_rating, ''), NULLIF(ai_rating, ''), '')",
    "final_rank": "final_rank",
    "candidate_source": "candidate_source",
}


def log(msg):
    print(f"[DEBUG][{jst_now_str()}] {msg}", flush=True)


def jst_now():
    return datetime.now(JST)


def jst_now_str():
    return jst_now().strftime("%Y-%m-%d %H:%M:%S JST")


def today_text():
    return jst_now().strftime("%Y-%m-%d")


def current_hhmm():
    return jst_now().strftime("%H:%M")


def hhmm_to_minutes(hhmm):
    h, m = map(int, hhmm.split(":"))
    return h * 60 + m


def is_not_started(time_str):
    try:
        return hhmm_to_minutes(time_str) >= hhmm_to_minutes(current_hhmm())
    except Exception:
        return True


def minutes_until_start(time_str):
    try:
        return hhmm_to_minutes(time_str) - hhmm_to_minutes(current_hhmm())
    except Exception:
        return None


def render_countdown_badge(time_str):
    diff = minutes_until_start(time_str)
    if diff is None:
        return '<span class="countdown-badge countdown-normal">時刻不明</span>'
    if diff < 0:
        return '<span class="countdown-badge countdown-closed">締切後</span>'
    if diff <= 10:
        return '<span class="countdown-badge countdown-soon">まもなく締切</span>'
    if diff <= 30:
        return f'<span class="countdown-badge countdown-warning">あと{diff}分</span>'
    return f'<span class="countdown-badge countdown-normal">あと{diff}分</span>'


def db_connect():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL が設定されていません")
    return psycopg2.connect(DATABASE_URL)


def parse_json_array_text(value):
    if not value:
        return []
    if isinstance(value, list):
        return value
    try:
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return parsed
    except Exception:
        pass
    return []


def display_text(value, empty_text="未取得"):
    if value is None:
        return empty_text
    s = str(value).strip()
    if s == "" or s == "-":
        return empty_text
    return s


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


def normalize_candidate_source(value):
    s = str(value or "").strip()
    if s in {"official_star", "shadow_ai", "all_race_ai", "official_all"}:
        return s
    return "official_all"


def candidate_source_label(value):
    source = normalize_candidate_source(value)
    if source == "shadow_ai":
        return "裏AI候補"
    if source == "all_race_ai":
        return "全レース検証"
    if source == "official_all":
        return "公式★1〜5"
    return "公式候補"


def candidate_source_short_label(value):
    source = normalize_candidate_source(value)
    if source == "shadow_ai":
        return "裏AI"
    if source == "all_race_ai":
        return "全検証"
    if source == "official_all":
        return "全星"
    return "公式"


def is_pickup_official_rating(rating):
    return str(rating or "").strip() in {"★★★★★", "★★★★☆"}


def is_low_official_rating(rating):
    return str(rating or "").strip() in {"★★★☆☆", "★★☆☆☆", "★☆☆☆☆"}


def effective_ai_rating_text(row):
    row = row or {}
    return (
        display_text(row.get("final_ai_rating"), "")
        or display_text(row.get("base_ai_rating"), "")
        or display_text(row.get("ai_rating"), "")
        or ""
    )


def is_shadow_like_row(row):
    row = row or {}
    source = normalize_candidate_source(row.get("candidate_source"))
    if source == "shadow_ai":
        return True
    if source != "official_all":
        return False
    return is_low_official_rating(row.get("rating")) and effective_ai_rating_text(row) == "AI★★★★★"


def card_source_badge_html(row):
    row = row or {}
    source = normalize_candidate_source(row.get("candidate_source"))
    if is_shadow_like_row(row):
        return '<span class="source-badge source-badge-shadow">裏AI候補・検証用</span>'
    if source == "all_race_ai":
        return '<span class="source-badge source-badge-all">全レース検証</span>'
    if source == "official_all":
        if is_pickup_official_rating(row.get("rating")):
            return '<span class="source-badge source-badge-official">公式候補</span>'
        return '<span class="source-badge source-badge-all">全レース検証</span>'
    return '<span class="source-badge source-badge-official">公式候補</span>'


def effective_ai_score(row):
    """
    表示・CSV用のAI点数を返す。
    latest反映後は final_ai_score を優先。
    latest前は final_ai_score が 0 のままでも base_ai_score が入っていることがあるため、
    final_rank 等が空なら base_ai_score -> old ai_score の順で見る。
    """
    row = row or {}

    has_latest = any([
        str(row.get("final_rank") or "").strip(),
        str(row.get("latest_updated_at") or "").strip(),
        str(row.get("latest_reason_text") or "").strip(),
    ])

    if has_latest:
        return safe_float(row.get("final_ai_score"), 0)

    # latest前は、0/空を未確定扱いにして base -> old ai の順で見る。
    for key in ["final_ai_score", "base_ai_score", "ai_score"]:
        value = row.get(key)
        if value is None:
            continue
        s = str(value).strip()
        if s == "":
            continue
        try:
            score = float(s)
        except Exception:
            continue
        if abs(score) > 0.000001:
            return score

    return 0.0


def normalize_ai_detail(raw_detail, exhibition_list):
    detail = (raw_detail or "").strip()
    has_exhibition = bool(exhibition_list)
    if has_exhibition and not detail:
        return "展示反映"
    if not detail:
        return "基本補正のみ"
    if detail in ["モーター反映", "展示反映"]:
        return "展示補正なし"
    return detail


def yen(n):
    try:
        return f"{int(n):,}円"
    except Exception:
        return "0円"


def normalize_amount_per_point(value, default=100):
    try:
        v = int(str(value or "").replace("円", "").replace(",", "").strip())
    except Exception:
        v = int(default or 100)

    # 基本は100円/200円。将来増やす時に備えて100円単位だけ許可。
    if 100 <= v <= 5000 and v % 100 == 0:
        return v
    return int(default or 100)


def scale_payout_by_amount(base_payout, amount_per_point):
    try:
        payout = int(base_payout or 0)
    except Exception:
        payout = 0
    amount = normalize_amount_per_point(amount_per_point, 100)
    return int(round(payout * amount / 100.0))


def render_amount_options(current_amount):
    current = normalize_amount_per_point(current_amount, 100)
    options = ""
    for value in [100, 200]:
        selected = "selected" if value == current else ""
        options += f'<option value="{value}" {selected}>{value}円</option>'
    return options


def signed_yen(n):
    try:
        v = int(n)
    except Exception:
        return "0円"
    if v > 0:
        return f"+{v:,}円"
    return f"{v:,}円"


def lane_color_class(lane):
    try:
        lane_num = int(lane)
    except Exception:
        lane_num = 0
    return f"lane-color lane-color-{lane_num}"


def render_lane_badge(lane, suffix=""):
    label = f"{lane}{suffix}" if suffix else str(lane)
    return f'<span class="{lane_color_class(lane)}">{label}</span>'


def render_colored_pick_html(pick_text):
    s = normalize_pick_text(pick_text)
    if not s:
        return ""
    parts = s.split("-")
    html_parts = []
    for idx, part in enumerate(parts):
        if idx > 0:
            html_parts.append('<span class="pick-sep">-</span>')
        lane_text = str(part).strip()
        if lane_text.isdigit():
            html_parts.append(render_lane_badge(int(lane_text)))
        else:
            html_parts.append(f'<span class="pick-plain">{lane_text}</span>')
    return f'<span class="pick-inline">{"".join(html_parts)}</span>'


def percent(n):
    try:
        return f"{float(n):.1f}%"
    except Exception:
        return "0%"


def profit_class(value):
    try:
        v = int(value)
    except Exception:
        v = 0
    if v > 0:
        return "profit-plus"
    if v < 0:
        return "profit-minus"
    return "profit-zero"


def normalize_pick_text(value):
    return str(value or "").replace(" ", "").replace("\n", "").replace("\r", "").strip()


def selection_items(selection_text):
    s = str(selection_text or "").strip()
    if not s:
        return []

    parts = [x for x in re.split(r"\s*/\s*", s) if x.strip()]
    items = []
    for part in parts:
        item = normalize_pick_text(part)
        if item:
            items.append(item)
    return items


def unique_preserve(seq):
    result = []
    seen = set()
    for x in seq:
        if x not in seen:
            seen.add(x)
            result.append(x)
    return result


def get_selected_count_from_text(selection_text):
    return len(selection_items(selection_text))


def get_selected_total_amount(race):
    return normalize_amount_per_point(race.get("amount"), 100) * get_selected_count_from_text(
        race.get("purchased_selection_text", "")
    )


def make_race_key(race_date, venue, race_no, candidate_source="official_star"):
    return (
        str(race_date or "").strip(),
        str(venue or "").strip(),
        str(race_no or "").strip(),
        normalize_candidate_source(candidate_source),
    )


def get_existing_race_map_by_date(race_date):
    ensure_db_initialized()
    conn = db_connect()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        '''
        SELECT
            id, race_date, venue, race_no, time, candidate_source,
            final_ai_score, final_ai_rating, final_ai_selection, final_rank,
            latest_reason_text, latest_updated_at
        FROM races
        WHERE race_date = %s
          AND venue <> 'テスト会場'
        ORDER BY id DESC
        ''',
        (race_date,),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    race_map = {}
    for row in rows:
        key = make_race_key(row['race_date'], row['venue'], row['race_no'], row.get('candidate_source'))
        if key not in race_map:
            race_map[key] = row
    return race_map


def parse_exhibition_rank_map(rank_text):
    result = {}
    s = (rank_text or "").strip()
    if not s:
        return result
    parts = [x.strip() for x in s.split("/") if x.strip()]
    for part in parts:
        if ":" not in part:
            continue
        a, b = part.split(":", 1)
        try:
            result[int(a.strip())] = int(b.strip())
        except Exception:
            continue
    return result


def exhibition_rank_class(rank):
    try:
        r = int(rank)
    except Exception:
        return "ex-rank-box"
    if r == 1:
        return "ex-rank-box ex-rank-1"
    if r == 2:
        return "ex-rank-box ex-rank-2"
    if r == 3:
        return "ex-rank-box ex-rank-3"
    if r >= 5:
        return "ex-rank-box ex-rank-low"
    return "ex-rank-box"


def render_exhibition_rank_boxes(rank_text):
    rank_map = parse_exhibition_rank_map(rank_text)
    if not rank_map:
        return '<div class="ex-rank-empty">未取得</div>'
    boxes = ""
    for lane in range(1, 7):
        rank = rank_map.get(lane)
        rank_display = "-" if rank is None else str(rank)
        boxes += f'''
        <div class="{exhibition_rank_class(rank)}">
          <div class="ex-lane">{render_lane_badge(lane)}</div>
          <div class="ex-rank">{rank_display}</div>
        </div>
        '''
    return f'<div class="ex-rank-grid">{boxes}</div>'


def render_exhibition_time_chips(exhibition_list):
    if not exhibition_list:
        return '<div class="ex-chip-empty">未取得</div>'
    chips = ""
    for i, t in enumerate(exhibition_list, start=1):
        chips += f'''
        <div class="ex-chip">
          <span class="ex-chip-lane">{render_lane_badge(i)}</span>
          <span class="ex-chip-time">{t}</span>
        </div>
        '''
    return f'<div class="ex-chip-wrap">{chips}</div>'


def format_weather_num(value, suffix=""):
    try:
        if value is None or str(value).strip() == "":
            return ""
        v = float(value)
        if v.is_integer():
            return f"{int(v)}{suffix}"
        return f"{v:.1f}{suffix}"
    except Exception:
        return ""


def render_weather_summary_html(weather, wind_speed, wave_height, wind_type, wind_dir, water_state_score=None):
    items = []

    weather_text = str(weather or "").strip()
    wind_type_text = str(wind_type or "").strip()
    wind_dir_text = str(wind_dir or "").strip()
    wind_speed_text = format_weather_num(wind_speed, "m")
    wave_height_text = format_weather_num(wave_height, "cm")

    if weather_text:
        items.append(("weather-chip", weather_text))
    if wind_type_text:
        items.append(("weather-chip weather-chip-windtype", wind_type_text))
    if wind_dir_text:
        items.append(("weather-chip weather-chip-dir", f"風向 {wind_dir_text}"))
    if wind_speed_text:
        items.append(("weather-chip weather-chip-num", f"風速 {wind_speed_text}"))
    if wave_height_text:
        items.append(("weather-chip weather-chip-num", f"波高 {wave_height_text}"))

    try:
        ws = float(water_state_score)
        if ws > 0.08:
            items.append(("weather-chip weather-chip-good", "水面やや安定"))
        elif ws < -0.08:
            items.append(("weather-chip weather-chip-bad", "水面やや荒れ"))
    except Exception:
        pass

    if not items:
        return '<div class="detail-chip-empty">未取得</div>'

    chips = "".join([f'<div class="{cls}">{label}</div>' for cls, label in items])
    return f'<div class="weather-chip-wrap">{chips}</div>'


def parse_player_names_map(player_names_text):
    result = {}
    s = str(player_names_text or "").strip()
    if not s:
        return result
    parts = [x.strip() for x in s.split("/") if x.strip()]
    for part in parts:
        if ":" not in part:
            continue
        lane_part, name = part.split(":", 1)
        try:
            lane = int(lane_part.strip())
        except Exception:
            continue
        player_name = str(name).strip()
        if player_name:
            result[lane] = player_name
    return result


def lane_score_tone_class(score):
    if score >= 0.7:
        return "player-info-chip player-info-chip-good"
    if score <= -0.4:
        return "player-info-chip player-info-chip-bad"
    return "player-info-chip"


def parse_exhibition_time_float(value):
    s = str(value or "").strip()
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def build_exhibition_time_rank_map(exhibition_list):
    valid = []
    for lane, value in enumerate(exhibition_list or [], start=1):
        t = parse_exhibition_time_float(value)
        if t is not None:
            valid.append((lane, t))
    valid.sort(key=lambda x: (x[1], x[0]))
    return {lane: idx for idx, (lane, _t) in enumerate(valid, start=1)}


def parse_lane_chip_text_map(raw_text):
    result = {}
    s = str(raw_text or "").strip()
    if not s:
        return result
    for part in [x.strip() for x in s.split('/') if x.strip()]:
        if ':' not in part:
            continue
        lane_part, body = part.split(':', 1)
        try:
            lane = int(lane_part.strip())
        except Exception:
            continue
        items = []
        for token in re.split(r'[|｜]', body):
            chip = str(token).strip()
            if chip:
                items.append(chip)
        if items:
            result[lane] = items
    return result


def normalize_reason_tag(text):
    s = str(text or "").strip()
    if not s:
        return ""

    tag_rules = [
        (["地力", "全国勝率", "全国2連率", "全国3連率"], "勝率系"),
        (["当地", "当地勝率", "当地2連率", "当地3連率"], "当地"),
        (["モーター", "機力"], "モーター"),
        (["ST", "スタート"], "ST"),
        (["コース", "進入率"], "コース"),
        (["近況", "最近"], "近況"),
        (["級別", "A1", "A2", "B1", "B2"], "級別"),
        (["展示"], "展示"),
        (["進入", "前づけ", "イン外し"], "進入"),
    ]

    for keywords, label in tag_rules:
        if any(k in s for k in keywords):
            return label

    return s


def parse_signed_chip_items(raw_items):
    parsed = []
    for raw in raw_items or []:
        text = str(raw or "").strip()
        if not text:
            continue

        tone = "neutral"
        if text[:1] in {"+", "＋"}:
            tone = "plus"
            text = text[1:].strip()
        elif text[:1] in {"-", "－", "▲"}:
            tone = "minus"
            text = text[1:].strip()

        tag = normalize_reason_tag(text)
        if tag:
            parsed.append((tone, tag))

    return parsed


def build_default_player_evidence_items(lane, exhibition_rank_map, exhibition_list, lane_score_map, latest_reason_text=""):
    items = []

    rank = exhibition_rank_map.get(lane)
    if rank == 1 or rank == 2:
        items.append(("plus", "展示"))
    elif rank is not None and rank >= 5:
        items.append(("minus", "展示"))

    lane_score = lane_score_map.get(lane)
    if lane_score is not None:
        if lane_score >= 0.25:
            items.append(("plus", "展示"))
        elif lane_score <= -0.15:
            items.append(("minus", "展示"))

    latest_text = str(latest_reason_text or "")
    if lane == 1 and ("イン外し" in latest_text):
        items.append(("minus", "進入"))
    elif "前づけ" in latest_text and lane in {3, 4, 5, 6}:
        items.append(("plus", "進入"))

    return items


def render_player_evidence_chips(reason_items, default_items):
    chips = []
    seen = set()

    for tone, item in list(reason_items) + list(default_items):
        if not item:
            continue

        key = item
        if key in seen:
            continue
        seen.add(key)

        if tone == "plus":
            cls = "player-evidence-chip player-evidence-chip-plus"
        elif tone == "minus":
            cls = "player-evidence-chip player-evidence-chip-minus"
        else:
            cls = "player-evidence-chip player-evidence-chip-neutral"

        chips.append(f'<span class="{cls}">{item}</span>')

    if not chips:
        return '<div class="player-rank-empty">未取得</div>'

    return f'<div class="player-evidence-wrap">{"".join(chips)}</div>'


def render_player_rank_summary_html(
    player_names_text,
    class_history_text,
    lane_score_text="",
    exhibition_rank_text="",
    exhibition_list=None,
    player_stat_text="",
    player_reason_text="",
    latest_reason_text="",
):
    player_map = parse_player_names_map(player_names_text)
    class_rows = parse_class_history_rows(class_history_text)
    class_map = {}
    for row in class_rows:
        lane = row.get("lane")
        if lane is None:
            continue
        values = list(row.get("classes", []) or [])[:4]
        while len(values) < 4:
            values.append("")
        class_map[lane] = values

    lane_score_map = {lane: score for lane, score in parse_lane_score_items(lane_score_text)}
    exhibition_rank_map = parse_exhibition_rank_map(exhibition_rank_text)
    exhibition_list = exhibition_list or []
    player_stat_map = parse_lane_chip_text_map(player_stat_text)
    player_reason_map = parse_lane_chip_text_map(player_reason_text)

    has_any = (
        bool(player_map)
        or bool(class_map)
        or bool(player_stat_map)
        or bool(player_reason_map)
        or bool(lane_score_map)
        or bool(exhibition_rank_map)
        or bool(exhibition_list)
    )
    if not has_any:
        return '<div class="player-rank-empty">未取得</div>'

    rows_html = ""
    for lane in range(1, 7):
        name = player_map.get(lane, "未取得")
        class_values = class_map.get(lane, ["", "", "", ""])
        class_labels = ["現", "-1", "-2", "-3"]
        class_chips = ""
        for idx, cls in enumerate(class_values):
            cls_text = cls or "-"
            cls_safe = (cls or "blank").lower()
            current_cls = " current-class-chip" if idx == 0 else ""
            blank_cls = " class-chip-blank" if cls_text == "-" else ""
            class_chips += f'<div class="class-chip class-chip-{cls_safe}{current_cls}{blank_cls}"><span class="class-chip-sub">{class_labels[idx]}</span><span class="class-chip-main">{cls_text}</span></div>'

        stat_items = player_stat_map.get(lane, [])
        reason_items = parse_signed_chip_items(player_reason_map.get(lane, []))
        reason_items.extend(parse_signed_chip_items(stat_items))
        default_items = build_default_player_evidence_items(
            lane,
            exhibition_rank_map,
            exhibition_list,
            lane_score_map,
            latest_reason_text=latest_reason_text,
        )
        evidence_html = render_player_evidence_chips(reason_items, default_items)

        rows_html += f"""
        <div class=\"player-rank-row\">
          <div class=\"player-rank-main-wrap\">
            <div class=\"player-rank-main\">
              <span class=\"player-rank-lane\">{render_lane_badge(lane)}</span>
              <span class=\"player-rank-name\">{name}</span>
            </div>
            <div class=\"player-rank-class-row\">{class_chips}</div>
          </div>
          <div class=\"player-rank-evidence\">{evidence_html}</div>
        </div>
        """

    return f'<div class="player-rank-wrap">{rows_html}</div>'


def parse_class_history_rows(class_history_text):
    rows = []
    s = str(class_history_text or "").strip()
    if not s:
        return rows
    parts = [x.strip() for x in s.split("/") if x.strip()]
    current = None
    classes = []
    for part in parts:
        if ":" in part:
            if current:
                rows.append(current)
            lane_part, cls = part.split(":", 1)
            try:
                lane = int(lane_part.strip())
            except Exception:
                lane = None
            classes = [cls.strip()] if cls.strip() else []
            current = {"lane": lane, "classes": classes}
        else:
            if current and part:
                current["classes"].append(part)
    if current:
        rows.append(current)
    return rows


def parse_lane_score_items(lane_score_text):
    items = []
    s = str(lane_score_text or "").strip()
    if not s:
        return items
    for part in [x.strip() for x in s.split("/") if x.strip()]:
        if ":" not in part:
            continue
        lane, score = part.split(":", 1)
        try:
            lane_num = int(lane.strip())
            score_val = float(score.strip())
        except Exception:
            continue
        items.append((lane_num, score_val))
    return items


def lane_score_class(score):
    if score >= 1.5:
        return "lane-score-chip lane-score-verygood"
    if score >= 0.7:
        return "lane-score-chip lane-score-good"
    if score <= -0.4:
        return "lane-score-chip lane-score-bad"
    return "lane-score-chip"


def render_lane_score_chips(lane_score_text):
    items = parse_lane_score_items(lane_score_text)
    if not items:
        return '<div class="lane-score-empty">未取得</div>'
    items = sorted(items, key=lambda x: (-x[1], x[0]))
    chips = ""
    for lane, score in items:
        chips += f'<div class="{lane_score_class(score)}"><span class="lane-score-lane">{render_lane_badge(lane)}</span><span class="lane-score-value">{score:.2f}</span></div>'
    return f'<div class="lane-score-wrap">{chips}</div>'


def parse_detail_material_list(detail_text):
    s = str(detail_text or "").strip()
    if not s:
        return []
    return [x.strip() for x in s.split("/") if x.strip()]


def render_detail_material_chips(detail_text):
    items = parse_detail_material_list(detail_text)
    if not items:
        return '<div class="detail-chip-empty">未取得</div>'
    chips = "".join([f'<div class="detail-chip">{item}</div>' for item in items])
    return f'<div class="detail-chip-wrap">{chips}</div>'


def final_rank_badge(rank_text):
    s = (rank_text or "").strip()
    if s == "買い強め":
        return '<span class="final-rank final-rank-strong">買い強め</span>'
    if s == "買い":
        return '<span class="final-rank final-rank-buy">買い</span>'
    if s == "様子見":
        return '<span class="final-rank final-rank-watch">様子見</span>'
    if s:
        return f'<span class="final-rank final-rank-skip">{s}</span>'
    return ""


def render_ai_rating_filter_options(current_value):
    html = '<option value="">すべて</option>'
    for value in AI_RATING_OPTIONS:
        if not value:
            continue
        selected = "selected" if value == current_value else ""
        html += f'<option value="{value}" {selected}>{value}</option>'
    return html


def render_official_rating_filter_options(current_value):
    current = current_value or "pickup"
    html = ""
    for value, label in OFFICIAL_RATING_FILTER_OPTIONS:
        selected = "selected" if value == current else ""
        html += f'<option value="{value}" {selected}>{label}</option>'
    return html


def safe_redirect_path(path, default="/"):
    s = str(path or "").strip()
    if not s.startswith("/") or s.startswith("//"):
        return default
    return s


def build_selection_compare_data(official_text, ai_text):
    official_items = selection_items(official_text)
    ai_items = selection_items(ai_text)
    combined = official_items + ai_items
    overlap = sorted(set(official_items) & set(ai_items), key=lambda x: combined.index(x))
    return {
        "official_items": official_items,
        "ai_items": ai_items,
        "overlap": overlap,
    }

def get_triplet_head_lane(triplet):
    try:
        return int(str(triplet or "").split("-")[0])
    except Exception:
        return 0


def is_close_to_official_core(ai_item, official_items):
    ai = normalize_pick_text(ai_item)
    if not ai:
        return False
    if ai in official_items:
        return True
    try:
        a1, b1, _c1 = ai.split("-")
    except Exception:
        return False
    for official in official_items:
        official = normalize_pick_text(official)
        try:
            a2, b2, _c2 = official.split("-")
        except Exception:
            continue
        if a1 == a2 and b1 == b2:
            return True
    return False


def build_bet_guide_data(
    final_rank,
    ai_selection,
    official_selection,
    candidate_source="official_star",
    ai_rating="",
    official_rating="",
):
    rank = str(final_rank or "").strip()
    source = normalize_candidate_source(candidate_source)
    ai_rating_text = str(ai_rating or "").strip()
    official_rating_text = str(official_rating or "").strip()

    ai_items = selection_items(ai_selection)
    # 6点買い運用に戻す。表示上の「本線3点」は買い推奨の中心にしない。
    core_items = ai_items[:6]
    official_top2 = selection_items(official_selection)[:2]

    is_ai5 = ai_rating_text == "AI★★★★★"
    is_shadow_pick_rating = official_rating_text in {"★★★☆☆", "★★☆☆☆", "★☆☆☆☆"}
    # official_all 一本化後は candidate_source は official_all のままなので、
    # 「公式★1〜3 × AI★★★★★」はここで裏AI扱いに変換する。
    # これをしないと、画面上は「買い」でも公式候補ロジックに入り、
    # 「買い」でも正しく裏AI枠の買い方メモに入る。
    if source == "official_all" and is_shadow_pick_rating and is_ai5:
        source = "shadow_ai"
    has_core = len(core_items) >= 3

    conditions = []
    should_buy = False
    recommended_count = 0
    recommended_amount = 100
    title = "見送り推奨"
    tone = "skip"
    recommend_text = "買わずに結果だけ確認"
    memo = "明日は検証優先。条件外は無理に買わない。"
    action_label = "推奨どおり見送り"

    if source == "all_race_ai":
        conditions = [
            ("全レース検証枠", True),
            ("買い対象ではない", True),
            ("CSV分析用", True),
            ("本番購入に混ぜない", True),
        ]
        title = "全レース検証用"
        tone = "skip"
        recommend_text = "買わない"
        memo = "all_race_ai は母集団分析用です。画面に出ても購入対象にしない。"
        action_label = "推奨どおり見送り"
    elif source == "shadow_ai":
        rank_ok = rank in {"買い強め", "買い"}
        conditions = [
            ("裏AI候補", True),
            ("AI★★★★★", is_ai5),
            ("買い強め/買い", rank_ok),
            ("公式★1〜3なら要注目", is_shadow_pick_rating),
        ]

        if not rank:
            title = "直前待ち"
            tone = "watch"
            recommend_text = "展示・風・進入の反映待ち"
            memo = "朝baseだけの裏AI候補です。collector_latest.py 反映後に判定を確認。"
            action_label = "直前待ち（反映なし）"
        elif is_ai5 and rank_ok and has_core:
            should_buy = True
            recommended_count = 6
            recommended_amount = 100
            tone = "buy"
            title = "裏AIの6点買い候補"
            if official_rating_text == "★★★☆☆":
                title = "公式★3×AI★5 要注目"
            recommend_text = "AI6点 × 100円"
            memo = "裏AIは検証寄り。買う場合もAI6点100円まで。"
            action_label = "AI6点を反映"
        else:
            title = "裏AIは検証のみ"
            tone = "watch" if is_ai5 else "skip"
            recommend_text = "買わずに検証"
            memo = "裏AIでもAI★★★★★かつ買い以上でなければ明日は買わない。"
            action_label = "推奨どおり見送り"
    else:
        rank_buy_ok = rank in {"買い強め", "買い"}
        conditions = [
            ("公式候補", True),
            ("AI★★★★★", is_ai5),
            ("最終判定が買い以上", rank_buy_ok),
            ("AI買い目あり", has_core),
        ]

        if not rank:
            title = "直前待ち"
            tone = "watch"
            recommend_text = "展示・風・進入の反映待ち"
            memo = "朝baseだけの候補です。collector_latest.py 反映後に買い判定を確認してください。"
            action_label = "直前待ち（反映なし）"
        elif rank_buy_ok and is_ai5 and has_core:
            should_buy = True
            recommended_count = 6
            recommended_amount = 100
            title = "公式候補の6点買い対象" if rank == "買い強め" else "公式候補の6点買い候補"
            tone = "strong" if rank == "買い強め" else "buy"
            recommend_text = "AI6点 × 100円"
            memo = "3点に絞らず、買うレースを絞ってAI6点で確認。200円にはしない。"
            action_label = "AI6点を反映"
        elif rank == "様子見":
            title = "様子見は買わない"
            tone = "watch"
            recommend_text = "買わずに検証"
            memo = "様子見は取り逃し確認用。明日は買わない。"
            action_label = "推奨どおり見送り"
        else:
            title = "見送り推奨"
            tone = "skip"
            recommend_text = "買わない"
            memo = "見送り寄り、AI★★★★以下、または買い以上でないものは買わない。"
            action_label = "推奨どおり見送り"

    return {
        "ai_core_items": core_items,
        "official_top2": official_top2,
        "conditions": conditions,
        "should_buy": should_buy,
        "recommended_count": recommended_count,
        "recommended_amount": recommended_amount,
        "title": title,
        "tone": tone,
        "recommend_text": recommend_text,
        "memo": memo,
        "action_label": action_label,
    }


def render_bet_guide_html(
    final_rank,
    ai_selection,
    official_selection,
    race_id_key="",
    candidate_source="official_star",
    ai_rating="",
    official_rating="",
):
    guide = build_bet_guide_data(
        final_rank,
        ai_selection,
        official_selection,
        candidate_source=candidate_source,
        ai_rating=ai_rating,
        official_rating=official_rating,
    )
    guide_icon_map = {
        "strong": "🔥",
        "buy": "🎯",
        "watch": "👀",
        "skip": "⏸️",
    }
    guide_icon = guide_icon_map.get(guide.get("tone"), "🎯")
    condition_html = ""
    for label, ok in guide["conditions"]:
        cls = "guide-check guide-check-ok" if ok else "guide-check guide-check-ng"
        mark = "OK" if ok else "NG"
        condition_html += f'<span class="{cls}"><span class="guide-check-mark">{mark}</span>{label}</span>'

    if guide["ai_core_items"]:
        core_html = "".join([f'<span class="guide-pick-chip">{render_colored_pick_html(x)}</span>' for x in guide["ai_core_items"]])
    else:
        core_html = '<span class="selection-chip-empty">AI本線未取得</span>'

    if guide["official_top2"]:
        official_html = "".join([f'<span class="guide-pick-chip guide-pick-official">{render_colored_pick_html(x)}</span>' for x in guide["official_top2"]])
    else:
        official_html = '<span class="selection-chip-empty">公式上位未取得</span>'

    action_label = guide.get("action_label") or ("推奨を反映" if guide["should_buy"] else "推奨どおり見送り")

    return f'''
    <div class="bet-guide-box bet-guide-{guide['tone']}">
      <div class="bet-guide-head">
        <div class="bet-guide-title-wrap">
          <div class="bet-guide-icon">{guide_icon}</div>
          <div>
            <div class="bet-guide-kicker">買い方メモ</div>
            <div class="bet-guide-title">{guide['title']}</div>
          </div>
        </div>
        <div class="bet-guide-recommend">{guide['recommend_text']}</div>
      </div>
      <details class="bet-guide-detail">
        <summary>条件と中身を見る</summary>
        <div class="bet-guide-body">
          <div class="bet-guide-row"><span class="bet-guide-label">AI買い目6点</span><span class="bet-guide-picks">{core_html}</span></div>
          <div class="bet-guide-row"><span class="bet-guide-label">公式上位2点</span><span class="bet-guide-picks">{official_html}</span></div>
          <div class="guide-check-wrap">{condition_html}</div>
          <div class="bet-guide-memo">{guide['memo']}</div>
        </div>
      </details>
      <button type="button" class="quick-select-btn quick-select-recommend" onclick="applyRecommendedBet('{race_id_key}', {guide['recommended_count']}, {guide['recommended_amount']})">{action_label}</button>
    </div>
    '''



def render_selection_column(
    own_items,
    overlap_items,
    source,
    empty_text,
    race_id_key="",
    selected_items=None,
    form_id="",
):
    if not own_items:
        return f'<div class="selection-chip-empty">{empty_text}</div>'

    selected_items = {normalize_pick_text(x) for x in (selected_items or set())}
    overlap_set = set(overlap_items)

    chips = ""
    for idx, item in enumerate(own_items):
        item_clean = normalize_pick_text(item)

        if item_clean in overlap_set:
            chip_kind = "overlap"
        else:
            chip_kind = source

        if source == "official":
            selected_class = " is-selected-view" if item_clean in selected_items else ""
            chips += f'''
            <div class="selection-view-chip selection-view-chip-{chip_kind}{selected_class}">
              <span class="selection-choice-body selection-choice-body-{chip_kind} selection-choice-body-view">{render_colored_pick_html(item_clean)}</span>
            </div>
            '''
            continue

        checked = "checked" if item_clean in selected_items else ""
        item_id = f"cmp-ai-{race_id_key}-{idx}"

        chips += f'''
        <label class="selection-choice-chip selection-choice-chip-{chip_kind}" for="{item_id}">
          <input
            class="selection-choice-input"
            type="checkbox"
            id="{item_id}"
            name="selected_ai"
            value="{item_clean}"
            data-pick-value="{item_clean}"
            data-race-group="{race_id_key}"
            form="{form_id}"
            {checked}
            onchange="syncSelectionValue(this, '{race_id_key}'); updateSelectionSummary('{race_id_key}')"
          >
          <span class="selection-choice-body selection-choice-body-{chip_kind}">{render_colored_pick_html(item_clean)}</span>
        </label>
        '''

    return f'<div class="selection-chip-grid compact-grid">{chips}</div>'


def render_ai_selection_column(
    ai_items,
    overlap_items,
    race_id_key="",
    selected_items=None,
    form_id="",
):
    if not ai_items:
        return '<div class="selection-chip-empty">未取得</div>'

    selected_items = {normalize_pick_text(x) for x in (selected_items or set())}
    overlap_set = set(overlap_items)

    def render_ai_chip(item, idx, role_class):
        item_clean = normalize_pick_text(item)
        chip_kind = "overlap" if item_clean in overlap_set else "ai"
        checked = "checked" if item_clean in selected_items else ""
        item_id = f"cmp-ai-{race_id_key}-{idx}"
        return f"""
        <label class="selection-choice-chip selection-choice-chip-{chip_kind} {role_class}" for="{item_id}">
          <input
            class="selection-choice-input"
            type="checkbox"
            id="{item_id}"
            name="selected_ai"
            value="{item_clean}"
            data-pick-value="{item_clean}"
            data-race-group="{race_id_key}"
            form="{form_id}"
            {checked}
            onchange="syncSelectionValue(this, '{race_id_key}'); updateSelectionSummary('{race_id_key}', false)"
          >
          <span class="selection-choice-body selection-choice-body-{chip_kind}">{render_colored_pick_html(item_clean)}</span>
        </label>
        """

    ai6_items = ai_items[:6]
    ai6_html = "".join([
        render_ai_chip(item, idx, "selection-choice-core" if idx < 3 else "selection-choice-cover")
        for idx, item in enumerate(ai6_items)
    ])

    return f"""
    <div class="ai-selection-block">
      <div class="quick-select-row">
        <button type="button" class="quick-select-btn quick-select-main" onclick="selectTopPicks('{race_id_key}', 6)">AI6点を選択</button>
        <button type="button" class="quick-select-btn quick-select-clear" onclick="clearPickSelection('{race_id_key}')">クリア</button>
      </div>
      <div class="selection-section selection-section-core">
        <div class="selection-section-title">AI6点</div>
        <div class="selection-chip-grid compact-grid">{ai6_html}</div>
      </div>
    </div>
    """


def render_selection_compare_html(r, race_id_key):
    official_text = r.get("selection", "")
    ai_text = r.get("ai_selection", "")
    selected_items = set(selection_items(r.get("purchased_selection_text", "")))
    form_id = f"race-form-{race_id_key}"

    data = build_selection_compare_data(official_text, ai_text)

    ai_html = render_ai_selection_column(
        data["ai_items"],
        data["overlap"],
        race_id_key=race_id_key,
        selected_items=selected_items,
        form_id=form_id,
    )
    official_html = render_selection_column(
        data["official_items"],
        data["overlap"],
        "official",
        "未取得",
        race_id_key=race_id_key,
        selected_items=selected_items,
        form_id=form_id,
    )

    return f'''
    <div class="selection-compare-wrap ai-priority-wrap">
      <div class="selection-compare-col selection-compare-col-ai">
        <div class="selection-col-title selection-col-title-ai">上位AI6点</div>
        {ai_html}
      </div>
      <div class="selection-compare-col selection-compare-col-official">
        <div class="selection-col-title selection-col-title-official">公式買い目（見るだけ）</div>
        {official_html}
      </div>
    </div>
    '''


def render_selected_summary_html(selected_text):
    items = selection_items(selected_text)
    if not items:
        return '<div class="selection-chip-empty">未選択</div>'
    chips = "".join([f'<div class="picked-chip">{render_colored_pick_html(item)}</div>' for item in items])
    return f'<div class="picked-chip-wrap">{chips}</div>'


def get_races_by_date(race_date):
    ensure_db_initialized()
    conn = db_connect()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        f'''
        SELECT {CARD_SELECT_COLUMNS}
        FROM races
        WHERE race_date = %s
          AND venue <> 'テスト会場'
        ORDER BY time ASC, venue ASC, race_no_num ASC, candidate_source ASC, id ASC
        ''',
        (race_date,),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def get_race_by_id(race_id):
    ensure_db_initialized()
    conn = db_connect()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        '''
        SELECT id, time, venue, race_no, candidate_source
        FROM races
        WHERE id = %s
        ''',
        (race_id,),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row


def get_filtered_today_races(show_closed=False, ai_rating_filter="", official_rating_filter="pickup", show_shadow=False, show_all_race=False):
    ensure_db_initialized()

    official_rating_filter = str(official_rating_filter or "pickup").strip() or "pickup"
    official_rating_values = {value for value, _label in OFFICIAL_RATING_FILTER_OPTIONS}
    if official_rating_filter not in official_rating_values:
        official_rating_filter = "pickup"

    where_clauses = [
        "race_date = %s",
        "venue <> 'テスト会場'",
    ]
    params = [today_text()]

    if official_rating_filter == "pickup":
        official_rating_sql = "rating IN (%s, %s)"
        official_rating_params = ["★★★★★", "★★★★☆"]
    else:
        official_rating_sql = "rating = %s"
        official_rating_params = [official_rating_filter]

    source_clauses = []
    if show_all_race:
        # 新方式: official_all が公式★1〜5の全レース母集団。
        # 旧方式の all_race_ai も過去データ互換で残す。
        source_clauses.append("candidate_source IN ('official_all', 'all_race_ai')")
    else:
        # 通常表示: 公式★4/★5など、公式評価フィルターに合うもの。
        source_clauses.append(f"(candidate_source IN ('official_all', 'official_star') AND {official_rating_sql})")
        params.extend(official_rating_params)
        # 裏AI候補は常時表示する。
        # 新方式の裏AI: 公式★1〜3 かつ AI★★★★★。
        source_clauses.append("(candidate_source = 'official_all' AND rating IN ('★★★☆☆','★★☆☆☆','★☆☆☆☆') AND COALESCE(NULLIF(final_ai_rating, ''), NULLIF(base_ai_rating, ''), NULLIF(ai_rating, '')) = 'AI★★★★★')")
        # 旧方式の裏AIも過去データ互換で表示。
        source_clauses.append("candidate_source = 'shadow_ai'")
    where_clauses.append("(" + " OR ".join(source_clauses) + ")")

    if ai_rating_filter:
        where_clauses.append(
            "COALESCE(NULLIF(final_ai_rating, ''), NULLIF(base_ai_rating, ''), NULLIF(ai_rating, '')) = %s"
        )
        params.append(ai_rating_filter)

    if not show_closed:
        where_clauses.append("time >= %s")
        params.append(current_hhmm())

    conn = db_connect()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        f'''
        SELECT {CARD_SELECT_COLUMNS}
        FROM races
        WHERE {' AND '.join(where_clauses)}
        ORDER BY time ASC, venue ASC, race_no_num ASC, candidate_source ASC, id ASC
        ''',
        tuple(params),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def update_race_result(race_id, selected_text, hit, payout, memo, amount_per_point=100):
    ensure_db_initialized()
    selected_text = " / ".join(
        unique_preserve([normalize_pick_text(x) for x in selection_items(selected_text)])
    )
    purchased = 1 if selected_text else 0
    amount_per_point = normalize_amount_per_point(amount_per_point, 100)
    if purchased == 0:
        hit = 0
        payout = 0

    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        "UPDATE races SET purchased = %s, purchased_selection_text = %s, amount = %s, hit = %s, payout = %s, memo = %s WHERE id = %s",
        (purchased, selected_text, amount_per_point, hit, payout, memo, race_id),
    )
    conn.commit()
    cur.close()
    conn.close()
    log(
        f"update_race_result race_id={race_id} purchased={purchased} selected={selected_text} amount={amount_per_point} hit={hit} payout={payout} memo={memo}"
    )


def delete_race(race_id):
    ensure_db_initialized()
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("DELETE FROM races WHERE id = %s", (race_id,))
    deleted = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    log(f"delete_race race_id={race_id} deleted={deleted}")
    return deleted


def delete_races_bulk(race_ids):
    ensure_db_initialized()
    race_ids = [int(x) for x in race_ids if str(x).strip().isdigit()]
    if not race_ids:
        return 0
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("DELETE FROM races WHERE id = ANY(%s)", (race_ids,))
    deleted = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    log(f"delete_races_bulk race_ids={race_ids} deleted={deleted}")
    return deleted


def get_summary_by_date(race_date):
    ensure_db_initialized()
    conn = db_connect()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        f"""
        WITH latest AS (
            SELECT DISTINCT ON (race_date, venue, race_no, candidate_source)
                *
            FROM races
            WHERE race_date = %s
              AND venue <> 'テスト会場'
              AND candidate_source <> 'all_race_ai'
            ORDER BY race_date, venue, race_no, candidate_source, id DESC
        )
        SELECT
            COUNT(*) AS total_rows,
            COALESCE(SUM(CASE WHEN {POINT_COUNT_SQL} > 0 THEN 1 ELSE 0 END), 0) AS total_bets,
            COALESCE(SUM({POINT_COUNT_SQL}), 0) AS total_points,
            COALESCE(SUM(amount * ({POINT_COUNT_SQL})), 0) AS total_investment,
            COALESCE(SUM({AUTO_PAYOUT_SQL}), 0) AS total_payout,
            COALESCE(SUM(CASE WHEN {AUTO_HIT_SQL} = 1 AND {POINT_COUNT_SQL} > 0 THEN 1 ELSE 0 END), 0) AS total_hits,
            COALESCE(MAX(imported_at), '') AS last_imported_at
        FROM latest
        """,
        (race_date,),
    )
    row = cur.fetchone() or {}
    cur.close()
    conn.close()

    total_rows = int(row.get("total_rows") or 0)
    total_bets = int(row.get("total_bets") or 0)
    total_points = int(row.get("total_points") or 0)
    total_investment = int(row.get("total_investment") or 0)
    total_payout = int(row.get("total_payout") or 0)
    total_hits = int(row.get("total_hits") or 0)
    total_profit = total_payout - total_investment
    hit_rate = round((total_hits / total_bets * 100), 1) if total_bets else 0
    roi = round((total_payout / total_investment * 100), 1) if total_investment else 0

    return {
        "total_rows": total_rows,
        "total_bets": total_bets,
        "total_points": total_points,
        "total_investment": total_investment,
        "total_payout": total_payout,
        "total_profit": total_profit,
        "total_hits": total_hits,
        "hit_rate": hit_rate,
        "roi": roi,
        "last_imported_at": str(row.get("last_imported_at") or "").strip(),
    }


def get_group_summary(race_date, group_key):
    ensure_db_initialized()

    group_sql = ALLOWED_GROUP_COLUMNS.get(group_key)
    if not group_sql:
        return []

    conn = db_connect()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        f"""
        WITH latest AS (
            SELECT DISTINCT ON (race_date, venue, race_no, candidate_source)
                *
            FROM races
            WHERE race_date = %s
              AND venue <> 'テスト会場'
              AND candidate_source <> 'all_race_ai'
            ORDER BY race_date, venue, race_no, candidate_source, id DESC
        )
        SELECT
            COALESCE(NULLIF(BTRIM({group_sql}), ''), '(空白)') AS group_name,
            COALESCE(SUM(CASE WHEN {POINT_COUNT_SQL} > 0 THEN 1 ELSE 0 END), 0) AS total_bets,
            COALESCE(SUM(CASE WHEN {AUTO_HIT_SQL} = 1 AND {POINT_COUNT_SQL} > 0 THEN 1 ELSE 0 END), 0) AS total_hits,
            COALESCE(SUM({POINT_COUNT_SQL}), 0) AS total_points,
            COALESCE(SUM(amount * ({POINT_COUNT_SQL})), 0) AS total_investment,
            COALESCE(SUM({AUTO_PAYOUT_SQL}), 0) AS total_payout
        FROM latest
        GROUP BY 1
        ORDER BY 1 ASC
        """,
        (race_date,),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    results = []
    for row in rows:
        total_bets = int(row.get("total_bets") or 0)
        total_hits = int(row.get("total_hits") or 0)
        total_points = int(row.get("total_points") or 0)
        total_investment = int(row.get("total_investment") or 0)
        total_payout = int(row.get("total_payout") or 0)
        total_profit = total_payout - total_investment
        hit_rate = round((total_hits / total_bets * 100), 1) if total_bets else 0
        roi = round((total_payout / total_investment * 100), 1) if total_investment else 0
        results.append(
            {
                "group_name": candidate_source_label(row.get("group_name")) if group_key == "candidate_source" else str(row.get("group_name") or "(空白)"),
                "total_bets": total_bets,
                "total_hits": total_hits,
                "total_points": total_points,
                "total_investment": total_investment,
                "total_payout": total_payout,
                "total_profit": total_profit,
                "hit_rate": hit_rate,
                "roi": roi,
            }
        )
    return results

def get_history_dates():
    ensure_db_initialized()
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT race_date FROM races WHERE venue <> 'テスト会場' ORDER BY race_date DESC"
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [row[0] for row in rows]


def get_history_date_summaries():
    ensure_db_initialized()
    conn = db_connect()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        f"""
        WITH latest AS (
            SELECT DISTINCT ON (race_date, venue, race_no, candidate_source)
                *
            FROM races
            WHERE venue <> 'テスト会場'
              AND candidate_source <> 'all_race_ai'
            ORDER BY race_date, venue, race_no, candidate_source, id DESC
        )
        SELECT
            race_date,
            COUNT(*) AS total_rows,
            COALESCE(SUM(CASE WHEN {POINT_COUNT_SQL} > 0 THEN 1 ELSE 0 END), 0) AS total_bets,
            COALESCE(SUM({POINT_COUNT_SQL}), 0) AS total_points,
            COALESCE(SUM(amount * ({POINT_COUNT_SQL})), 0) AS total_investment,
            COALESCE(SUM({AUTO_PAYOUT_SQL}), 0) AS total_payout,
            COALESCE(SUM(CASE WHEN {AUTO_HIT_SQL} = 1 AND {POINT_COUNT_SQL} > 0 THEN 1 ELSE 0 END), 0) AS total_hits,
            COALESCE(MAX(imported_at), '') AS last_imported_at
        FROM latest
        GROUP BY race_date
        ORDER BY race_date DESC
        """
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    results = []
    for row in rows:
        total_bets = int(row.get("total_bets") or 0)
        total_hits = int(row.get("total_hits") or 0)
        total_points = int(row.get("total_points") or 0)
        total_investment = int(row.get("total_investment") or 0)
        total_payout = int(row.get("total_payout") or 0)
        total_profit = total_payout - total_investment
        hit_rate = round((total_hits / total_bets * 100), 1) if total_bets else 0
        roi = round((total_payout / total_investment * 100), 1) if total_investment else 0
        results.append(
            {
                "race_date": str(row.get("race_date") or "").strip(),
                "summary": {
                    "total_rows": int(row.get("total_rows") or 0),
                    "total_bets": total_bets,
                    "total_points": total_points,
                    "total_investment": total_investment,
                    "total_payout": total_payout,
                    "total_profit": total_profit,
                    "total_hits": total_hits,
                    "hit_rate": hit_rate,
                    "roi": roi,
                    "last_imported_at": str(row.get("last_imported_at") or "").strip(),
                },
            }
        )
    return results

def filter_history_races(rows, venue_filter="", race_no_filter="", purchased_only=False, hit_only=False):
    filtered = list(rows)
    if venue_filter:
        filtered = [r for r in filtered if str(r.get("venue", "")).strip() == venue_filter]
    if race_no_filter:
        filtered = [r for r in filtered if str(r.get("race_no", "")).strip() == race_no_filter]
    if purchased_only:
        filtered = [r for r in filtered if get_selected_count_from_text(r.get("purchased_selection_text", "")) > 0]
    if hit_only:
        filtered = [r for r in filtered if int(r.get("hit") or 0) == 1]
    return filtered


def make_history_filter_options(rows, selected_venue="", selected_race_no=""):
    venues = sorted(set(str(r.get("venue", "")).strip() for r in rows if str(r.get("venue", "")).strip()))
    race_nos = sorted(
        set(str(r.get("race_no", "")).strip() for r in rows if str(r.get("race_no", "")).strip()),
        key=lambda x: int(str(x).replace("R", "")) if str(x).replace("R", "").isdigit() else 999
    )
    venue_options = '<option value="">すべて</option>'
    for venue in venues:
        selected = "selected" if venue == selected_venue else ""
        venue_options += f'<option value="{venue}" {selected}>{venue}</option>'
    race_no_options = '<option value="">すべて</option>'
    for race_no in race_nos:
        selected = "selected" if race_no == selected_race_no else ""
        race_no_options += f'<option value="{race_no}" {selected}>{race_no}</option>'
    return venue_options, race_no_options, venues, race_nos


def extract_base_quality_display_text(*texts):
    joined = " / ".join([str(t or "") for t in texts])
    for label in ["base土台◎", "base土台○", "base保留", "base危険"]:
        if label in joined:
            return label
    return ""


def render_base_quality_badge(*texts):
    label = extract_base_quality_display_text(*texts)
    if not label:
        return ""
    cls = {
        "base土台◎": "base-quality-strong",
        "base土台○": "base-quality-good",
        "base保留": "base-quality-watch",
        "base危険": "base-quality-risk",
    }.get(label, "base-quality-watch")
    return f'<span class="base-quality-badge {cls}">{label}</span>'


def build_card_html(r, is_history=False, race_date=""):
    selected_count = get_selected_count_from_text(r.get("purchased_selection_text", ""))
    selected_total_amount = get_selected_total_amount(r)

    result_trifecta_text = normalize_pick_text(r.get("result_trifecta_text", ""))
    result_trifecta_payout = int(r.get("result_trifecta_payout") or 0)
    amount_per_point = normalize_amount_per_point(r.get("amount"), 100)
    auto_hit = 1 if result_trifecta_text and result_trifecta_text in selection_items(r.get("purchased_selection_text", "")) else 0
    auto_payout = scale_payout_by_amount(result_trifecta_payout, amount_per_point) if auto_hit else 0
    display_hit_value = auto_hit if result_trifecta_text else int(r.get("hit") or 0)
    display_payout_value = auto_payout if result_trifecta_text else int(r.get("payout") or 0)
    checked_hit = "checked" if display_hit_value == 1 else ""
    payout_value = display_payout_value if display_payout_value else ""
    memo_value = r["memo"] if r["memo"] else ""
    auto_profit_value = display_payout_value - selected_total_amount if selected_count > 0 else 0
    settled_flag_value = int(r.get("settled_flag") or 0)
    settled_at_text = str(r.get("settled_at") or "").strip()

    card_class = "card history-edit-card" if is_history else "card"
    if int(r.get("hit") or 0) == 1:
        card_class += " card-hit"
    elif selected_count > 0:
        card_class += " card-purchased"

    source_value = normalize_candidate_source(r.get("candidate_source"))
    if is_shadow_like_row(r):
        card_class += " card-source-shadow"
    elif source_value == "all_race_ai":
        card_class += " card-source-all-race"

    rank_for_class = str(r.get("final_rank") or "").strip()
    if rank_for_class == "買い強め":
        card_class += " card-rank-strong"
    elif rank_for_class == "買い":
        card_class += " card-rank-buy"
    elif rank_for_class == "様子見":
        card_class += " card-rank-watch"
    elif rank_for_class:
        card_class += " card-rank-skip"

    status_parts = []
    if selected_count > 0:
        status_parts.append(f'<span class="status-badge status-badge-saved">購入済み {selected_count}点</span>')
    if display_hit_value == 1:
        status_parts.append('<span class="status-badge status-badge-hit">的中</span>')
    if result_trifecta_text or settled_flag_value == 1:
        status_parts.append('<span class="status-badge countdown-normal">結果反映済み</span>')
    status_html = f'<div class="status-wrap">{"".join(status_parts)}</div>' if status_parts else ""

    ai_reasons = parse_json_array_text(r.get("ai_reasons", "[]"))
    exhibition = parse_json_array_text(r.get("exhibition", "[]"))

    # 折りたたみを開かなくても、直前展示が反映済みか分かるようにする
    exhibition_count = len([x for x in exhibition if str(x or "").strip()])
    exhibition_rank_map_for_status = parse_exhibition_rank_map(r.get("exhibition_rank", ""))
    has_exhibition_status = exhibition_count >= 6 or bool(exhibition_rank_map_for_status)
    has_weather_status = bool(
        str(r.get("weather") or "").strip()
        or str(r.get("wind_type") or "").strip()
        or str(r.get("wind_dir") or "").strip()
        or r.get("wind_speed") not in [None, ""]
        or r.get("wave_height") not in [None, ""]
    )
    has_lane_score_status = bool(str(r.get("ai_lane_score_text") or "").strip())

    if has_exhibition_status:
        exhibition_status_class = "ex-status-ok"
        exhibition_status_title = "展示反映済み"
        exhibition_status_sub = f"展示{exhibition_count}艇 / 順位{'あり' if exhibition_rank_map_for_status else '待ち'}"
    else:
        exhibition_status_class = "ex-status-wait"
        exhibition_status_title = "展示待ち"
        exhibition_status_sub = "直前データ未反映"

    weather_status_text = "水面あり" if has_weather_status else "水面待ち"
    lane_score_status_text = "補正あり" if has_lane_score_status else "補正待ち"

    ai_reason_html = ""
    if ai_reasons and not is_history:
        items = "".join([f"<li>{x}</li>" for x in ai_reasons])
        ai_reason_html = f'<div class="row"><span class="label">補正理由</span><span class="value text-left"><ul class="reason-list">{items}</ul></span></div>'

    race_id_key = f"history-{r['id']}" if is_history else str(r["id"])

    exhibition_time_html = render_exhibition_time_chips(exhibition)
    exhibition_rank_html = render_exhibition_rank_boxes(r.get("exhibition_rank", ""))
    weather_summary_html = render_weather_summary_html(
        r.get("weather", ""),
        r.get("wind_speed"),
        r.get("wave_height"),
        r.get("wind_type", ""),
        r.get("wind_dir", ""),
        r.get("water_state_score"),
    )
    display_ai_rating = (
        display_text(r.get("final_ai_rating"), "")
        or display_text(r.get("base_ai_rating"), "")
        or "AI評価なし"
    )
    display_ai_selection = (
        str(r.get("final_ai_selection") or "").strip()
        or str(r.get("base_ai_selection") or "").strip()
        or ""
    )
    display_ai_detail_text = display_text(r.get("latest_reason_text"), "") or display_text(r.get("base_reason_text"), "")
    render_r = dict(r)
    render_r["ai_selection"] = display_ai_selection
    selection_compare_html = render_selection_compare_html(render_r, race_id_key)
    ai_detail_text = display_ai_detail_text or normalize_ai_detail(r.get("ai_detail"), exhibition)
    ai_score_value = effective_ai_score(r)
    player_rank_summary_html = render_player_rank_summary_html(
        r.get("player_names_text", ""),
        r.get("class_history_text", ""),
        r.get("ai_lane_score_text", ""),
        r.get("exhibition_rank", ""),
        exhibition,
        r.get("player_stat_text", ""),
        r.get("player_reason_text", ""),
        r.get("latest_reason_text", "") or r.get("base_reason_text", ""),
    )
    final_rank_html = final_rank_badge(r.get("final_rank"))
    countdown_html = render_countdown_badge(r["time"]) if not is_history else ""
    selected_summary_html = render_selected_summary_html(r.get("purchased_selection_text", ""))
    form_id = f"race-form-{race_id_key}"
    bet_guide_html = render_bet_guide_html(
        r.get("final_rank"),
        display_ai_selection,
        r.get("selection", ""),
        race_id_key=race_id_key,
        candidate_source=r.get("candidate_source"),
        ai_rating=display_ai_rating,
        official_rating=r.get("rating", ""),
    )

    source_value = normalize_candidate_source(r.get("candidate_source"))
    if is_shadow_like_row(r):
        source_badge_html = '<span class="source-badge source-badge-shadow">裏AI候補・検証用</span>'
    elif source_value == "all_race_ai":
        source_badge_html = '<span class="source-badge source-badge-all-race">全レース検証・買わない</span>'
    else:
        source_badge_html = '<span class="source-badge source-badge-official">公式候補</span>' 

    base_quality_badge_html = render_base_quality_badge(
        r.get("latest_reason_text", ""),
        r.get("base_reason_text", ""),
    )

    top_checkbox = ""
    if is_history:
        top_checkbox = f'''
        <div class="multi-check-wrap">
          <input type="checkbox" class="bulk-checkbox" name="race_ids" value="{r['id']}" form="bulk-delete-form" onchange="updateBulkDeleteCount()">
        </div>
        '''

    history_hidden = f'<input type="hidden" name="redirect_to" value="/history/{race_date}">' if is_history else ''
    action_url = "/update_record" if is_history else "/save"

    delete_form = ""
    if is_history:
        delete_form = f'''
        <form method="post" action="/delete_record" class="delete-form" onsubmit="return confirm('この過去データを削除しますか？');">
          <input type="hidden" name="race_id" value="{r['id']}">
          <input type="hidden" name="redirect_to" value="/history/{race_date}">
          <button type="submit" class="delete-btn">この1件を削除</button>
        </form>
        '''

    return f'''
    <div class="{card_class}" data-race-card-id="{race_id_key}" id="race-card-{r['id']}">
      {top_checkbox}
      <div class="card-top card-top-main">
        <div class="card-top-left">
          <div class="time-line">
            <div class="time">{r['time']}</div>
            {countdown_html}
          </div>
          <div class="race-mainline">
            <span class="race-spot race-spot-main">
              <span class="race-venue">{r['venue']}</span>
              <span class="race-rno">{r['race_no']}</span>
            </span>
          </div>
        </div>
        {status_html}
      </div>

      <div class="badge-row">
        {source_badge_html}
        {base_quality_badge_html}
        <span class="rating">{display_text(r.get('rating'), '公式評価なし')}</span>
        <span class="ai-rating">{display_ai_rating}</span>
        {final_rank_html}
      </div>

      <div class="metric-badge-row">
        <span class="metric-badge"><span class="metric-badge-label">券種</span><span class="metric-badge-value">{r['bet_type']}</span></span>
        <span class="metric-badge"><span class="metric-badge-label">選択点数</span><span class="metric-badge-value" id="selected-count-badge-{race_id_key}">{selected_count}点</span></span>
        <span class="metric-badge metric-badge-strong"><span class="metric-badge-label">購入額</span><span class="metric-badge-value" id="selected-total-badge-{race_id_key}">{yen(selected_total_amount)}</span></span>
        <span class="metric-badge"><span class="metric-badge-label">1点</span><span class="metric-badge-value" id="amount-per-point-badge-{race_id_key}">{yen(amount_per_point)}</span></span>
        <span class="metric-badge metric-badge-score"><span class="metric-badge-label">AI補正点</span><span class="metric-badge-value">{round(ai_score_value, 2)}</span></span>
      </div>

      <div class="ex-status-strip {exhibition_status_class}">
        <div class="ex-status-main">
          <span class="ex-status-dot"></span>
          <span class="ex-status-title">{exhibition_status_title}</span>
          <span class="ex-status-sub">{exhibition_status_sub}</span>
        </div>
        <div class="ex-status-chips">
          <span>{weather_status_text}</span>
          <span>{lane_score_status_text}</span>
        </div>
      </div>

      {bet_guide_html}

      <form id="{form_id}" method="post" action="{action_url}" class="form form-compact-save {'history-form' if is_history else ''}" data-race-id="{race_id_key}" data-amount="{amount_per_point}">
        <input type="hidden" name="race_id" value="{r['id']}">
        <input type="hidden" name="selected_text" id="selected-hidden-{race_id_key}" value="{r.get('purchased_selection_text', '')}">
        {history_hidden}

        <div class="quick-save-panel">
          <div class="quick-save-left">
            <label for="amount-select-{race_id_key}">1点</label>
            <select class="stake-select stake-select-compact" id="amount-select-{race_id_key}" name="amount_per_point" onchange="updateAmountPerPoint('{race_id_key}')">
              {render_amount_options(amount_per_point)}
            </select>
          </div>
          <div class="quick-save-middle">
            <div class="quick-save-count"><span id="selected-count-inline-{race_id_key}">{selected_count}点</span> / <span id="selected-total-inline-{race_id_key}">{yen(selected_total_amount)}</span></div>
            <div class="quick-save-rule">今日ルール: 本買い/準候補はAI6点100円</div>
          </div>
          <button type="submit" class="save-btn save-btn-compact {'half-btn' if is_history else ''}">保存</button>
        </div>

        <div class="info-box info-box-picks-first">
          <div class="row row-selection-highlight"><span class="label">買い目</span><span class="value">{selection_compare_html}</span></div>
          <div class="row row-selected-compact"><span class="label">選択中</span><span class="value"><div id="selected-summary-{race_id_key}">{selected_summary_html}</div></span></div>
          <div class="row result-row-compact"><span class="label">結果/収支</span><span class="value">
            <div class="result-mini-grid">
              <div><span class="mini-label">結果</span><span class="mini-value">{render_colored_pick_html(result_trifecta_text) if result_trifecta_text else '<span class="selection-chip-empty">未反映</span>'}</span></div>
              <div><span class="mini-label">払戻</span><span class="mini-value">{(yen(result_trifecta_payout) + ' /100円') if result_trifecta_payout > 0 else '未反映'}</span></div>
              <div><span class="mini-label">収支</span><span class="mini-value {profit_class(auto_profit_value)}">{signed_yen(auto_profit_value) if selected_count > 0 and result_trifecta_text else '未計算'}</span></div>
            </div>
          </span></div>

          <details class="detail-accordion">
            <summary>展示・水面・選手材料を開く</summary>
            <div class="row"><span class="label">水面気象</span><span class="value">{weather_summary_html}</span></div>
            <div class="row row-player-rank"><span class="label">選手・材料</span><span class="value">{player_rank_summary_html}</span></div>
            <div class="row"><span class="label">展示タイム</span><span class="value">{exhibition_time_html}</span></div>
            <div class="row row-exhibition-rank"><span class="label">展示順位</span><span class="value">{exhibition_rank_html}</span></div>
            {ai_reason_html}
          </details>
        </div>

        <div id="detail-{race_id_key}" class="detail-box detail-box-simple">
          <div class="auto-result-note">
            的中・払戻は公式結果から自動反映されます
          </div>
        </div>
      </form>
      {delete_form}
    </div>
    '''


def build_safe_card_html(r, is_history=False, race_date=""):
    try:
        return build_card_html(r, is_history=is_history, race_date=race_date)
    except Exception as e:
        log(
            "[card_render_error] "
            f"id={r.get('id')} "
            f"venue={r.get('venue')} "
            f"race_no={r.get('race_no')} "
            f"time={r.get('time')} "
            f"err={e}"
        )
        return f'''
        <div class="card">
          <div class="message message-error">
            表示エラー: {r.get("venue", "")} {r.get("race_no", "")}
          </div>
        </div>
        '''




def csv_safe(value):
    if value is None:
        return ""
    if isinstance(value, (list, dict)):
        try:
            return json.dumps(value, ensure_ascii=False)
        except Exception:
            return str(value)
    return str(value)


def build_export_rows(rows):
    export_rows = []
    for r in rows:
        selected_count = get_selected_count_from_text(r.get("purchased_selection_text", ""))
        selected_total_amount = get_selected_total_amount(r)

        result_trifecta_text = normalize_pick_text(r.get("result_trifecta_text", ""))
        result_trifecta_payout = int(r.get("result_trifecta_payout") or 0)
        amount_per_point = normalize_amount_per_point(r.get("amount"), 100)
        auto_hit = 1 if result_trifecta_text and result_trifecta_text in selection_items(r.get("purchased_selection_text", "")) else 0
        auto_payout = scale_payout_by_amount(result_trifecta_payout, amount_per_point) if auto_hit else 0
        display_hit_value = auto_hit if result_trifecta_text else int(r.get("hit") or 0)
        display_payout_value = auto_payout if result_trifecta_text else int(r.get("payout") or 0)
        auto_profit_value = display_payout_value - selected_total_amount if selected_count > 0 else 0

        display_ai_rating = (
            display_text(r.get("final_ai_rating"), "")
            or display_text(r.get("base_ai_rating"), "")
            or "AI評価なし"
        )
        display_ai_selection = (
            str(r.get("final_ai_selection") or "").strip()
            or str(r.get("base_ai_selection") or "").strip()
            or ""
        )
        display_ai_detail_text = display_text(r.get("latest_reason_text"), "") or display_text(r.get("base_reason_text"), "")
        ai_score_value = effective_ai_score(r)

        weather_parts = []
        weather = str(r.get("weather") or "").strip()
        wind_type = str(r.get("wind_type") or "").strip()
        wind_dir = str(r.get("wind_dir") or "").strip()
        wind_speed = r.get("wind_speed")
        wave_height = r.get("wave_height")
        water_state_score = r.get("water_state_score")

        if weather:
            weather_parts.append(weather)
        if wind_type:
            weather_parts.append(wind_type)
        if wind_dir:
            weather_parts.append(wind_dir)
        if wind_speed not in [None, ""]:
            weather_parts.append(f"風速{wind_speed}m")
        if wave_height not in [None, ""]:
            weather_parts.append(f"波高{wave_height}cm")

        export_rows.append({
            "race_date": csv_safe(r.get("race_date")),
            "candidate_source": csv_safe(normalize_candidate_source(r.get("candidate_source"))),
            "candidate_source_label": csv_safe(candidate_source_label(r.get("candidate_source"))),
            "time": csv_safe(r.get("time")),
            "venue": csv_safe(r.get("venue")),
            "race_no": csv_safe(r.get("race_no")),
            "official_rating": csv_safe(r.get("rating")),
            "bet_type": csv_safe(r.get("bet_type")),
            "official_selection": csv_safe(r.get("selection")),
            "amount_per_point": amount_per_point,
            "ai_score": round(ai_score_value, 2),
            "ai_rating": csv_safe(display_ai_rating),
            "ai_selection": csv_safe(display_ai_selection),
            "final_rank": csv_safe(r.get("final_rank")),
            "latest_reason_text": csv_safe(display_ai_detail_text),
            "player_names_text": csv_safe(r.get("player_names_text")),
            "class_history_text": csv_safe(r.get("class_history_text")),
            "player_stat_text": csv_safe(r.get("player_stat_text")),
            "player_reason_text": csv_safe(r.get("player_reason_text")),
            "exhibition_times": csv_safe(parse_json_array_text(r.get("exhibition", "[]"))),
            "exhibition_rank": csv_safe(r.get("exhibition_rank")),
            "ai_lane_score_text": csv_safe(r.get("ai_lane_score_text")),
            "weather_summary": " / ".join(weather_parts),
            "weather": csv_safe(weather),
            "wind_type": csv_safe(wind_type),
            "wind_dir": csv_safe(wind_dir),
            "wind_speed": csv_safe(wind_speed),
            "wave_height": csv_safe(wave_height),
            "water_state_score": csv_safe(water_state_score),
            "selected_count": selected_count,
            "selected_total_amount": selected_total_amount,
            "purchased": int(r.get("purchased") or 0),
            "purchased_selection_text": csv_safe(r.get("purchased_selection_text")),
            "official_result_trifecta": csv_safe(result_trifecta_text),
            "official_result_trifecta_payout": result_trifecta_payout,
            "display_hit": display_hit_value,
            "display_payout": display_payout_value,
            "auto_profit": auto_profit_value,
            "memo": csv_safe(r.get("memo")),
            "settled_flag": int(r.get("settled_flag") or 0),
            "settled_at": csv_safe(r.get("settled_at")),
            "imported_at": csv_safe(r.get("imported_at")),
        })
    return export_rows


def make_csv_response(rows, filename):
    output = io.StringIO()
    export_rows = build_export_rows(rows)
    fieldnames = list(export_rows[0].keys()) if export_rows else [
        "race_date", "candidate_source", "candidate_source_label", "time", "venue", "race_no", "official_rating", "bet_type",
        "official_selection", "amount_per_point", "ai_score", "ai_rating", "ai_selection",
        "final_rank", "latest_reason_text", "player_names_text", "class_history_text",
        "player_stat_text", "player_reason_text", "exhibition_times", "exhibition_rank",
        "ai_lane_score_text", "weather_summary", "weather", "wind_type", "wind_dir",
        "wind_speed", "wave_height", "water_state_score", "selected_count",
        "selected_total_amount", "purchased", "purchased_selection_text",
        "official_result_trifecta", "official_result_trifecta_payout", "display_hit",
        "display_payout", "auto_profit", "memo", "settled_flag", "settled_at", "imported_at"
    ]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for row in export_rows:
        writer.writerow(row)

    csv_text = output.getvalue()
    return Response(
        csv_text,
        mimetype="text/csv; charset=utf-8-sig",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

def render_home(races, summary, message_type="", message_text="", show_closed=False, ai_rating_filter="", official_rating_filter="pickup", show_shadow=False, show_all_race=False):
    updated_str = summary["last_imported_at"] if summary["last_imported_at"] else "未更新"
    if message_text:
        message_class = "message-success" if message_type == "success" else "message-error"
        message_html = f'<div class="message {message_class}">{message_text}</div>'
    else:
        message_html = ""
    checked_show_closed = "checked" if show_closed else ""
    checked_show_shadow = "checked" if show_shadow else ""
    checked_show_all_race = "checked" if show_all_race else ""
    ai_rating_options_html = render_ai_rating_filter_options(ai_rating_filter)
    official_rating_filter = str(official_rating_filter or "pickup").strip() or "pickup"
    official_rating_options_html = render_official_rating_filter_options(official_rating_filter)
    cards_html = ''.join([build_safe_card_html(r) for r in races]) if races else '<div class="empty">条件に合う★4以上候補はありません</div>'
    external_line = f'<div class="sub"><strong>公開URL:</strong> <a href="{EXTERNAL_URL}">{EXTERNAL_URL}</a></div>' if EXTERNAL_URL else ''
    filter_status_text = "締切後も表示中" if show_closed else "締切前のみ表示中"
    filter_shadow_text = "裏AI候補も常時表示"
    filter_all_race_text = "全レース検証も表示中" if show_all_race else "全レース検証は非表示"
    filter_ai_text = ai_rating_filter if ai_rating_filter else "すべて"
    official_label_map = {
        "pickup": "公式★5+★4",
        "★★★★★": "公式★5のみ",
        "★★★★☆": "公式★4のみ",
        "★★★☆☆": "公式★3のみ",
        "★★☆☆☆": "公式★2のみ",
        "★☆☆☆☆": "公式★1のみ",
    }
    filter_official_text = official_label_map.get(official_rating_filter, "公式★5+★4")
    content = f'''
    <div class="app-shell">
      <div class="topbar">
        <div class="brand">
          <div class="brand-logo">🏁</div>
          <div>
            <div class="brand-title">Race Candidates</div>
            <div class="brand-sub">ボートレース買い候補</div>
          </div>
        </div>
        <div class="topbar-status">
          <span class="top-pill">最終取込: {updated_str}</span>
        </div>
      </div>
      <div class="header hero hero-strong">
        <div class="title">今日の買い候補</div>
        <div class="sub">評価：公式★5+★4 / 券種：3連単 / 締切予定時刻が早い順</div>
        <div class="sub">現在の絞り込み: {filter_status_text} / {filter_shadow_text} / {filter_all_race_text} / 公式評価 {filter_official_text} / AI評価 {filter_ai_text}</div>
        {external_line}
        {message_html}
        <div class="daily-rule-panel">
          <div class="daily-rule-main">
            <div class="daily-rule-kicker">今日の買い方</div>
            <div class="daily-rule-title">運用ルール：買うレースを絞ってAI6点100円。買い強め/買いは買える表示に統一。</div>
          </div>
          <div class="daily-rule-steps">
            <span>公式候補 → AI★5×買い以上なら6点</span>
            <span>裏AI → 検証寄り、買うなら6点100円</span>
            <span>全レース検証 → 買わない</span>
          </div>
        </div>
        <form method="get" action="/" class="filter-box">
          <div class="filter-grid">
            <div class="filter-item filter-item-wide">
              <label class="filter-check">
                <input type="checkbox" name="show_closed" value="1" {checked_show_closed}>
                締切後も表示する
              </label>
            </div>
            <div class="filter-item filter-item-wide">
              <div class="filter-static-note">裏AI候補は常時表示</div>
            </div>
            <div class="filter-item filter-item-wide">
              <label class="filter-check">
                <input type="checkbox" name="show_all_race" value="1" {checked_show_all_race}>
                全レース検証も表示
              </label>
            </div>
            <div class="filter-item">
              <label for="official_rating">公式評価で絞る</label>
              <select name="official_rating" id="official_rating">{official_rating_options_html}</select>
            </div>
            <div class="filter-item">
              <label for="ai_rating">AI評価で絞る</label>
              <select name="ai_rating" id="ai_rating">{ai_rating_options_html}</select>
            </div>
            <div class="filter-actions">
              <button type="submit" class="filter-btn">フィルター適用</button>
              <a href="/" class="filter-reset">解除</a>
            </div>
          </div>
        </form>
        <div class="nav nav-app">
          <a href="/" class="nav-card active">今日の候補</a>
          <a href="/stats" class="nav-card">今日の集計</a>
          <a href="/history" class="nav-card">過去データ</a>
          <a href="/export/today.csv" class="nav-card">今日CSV</a>
        </div>
        <div class="summary">
          <div class="summary-box"><div class="summary-label">表示中候補</div><div class="summary-value">{len(races)}</div></div>
          <div class="summary-box"><div class="summary-label">購入レース数</div><div class="summary-value">{summary['total_bets']}</div></div>
          <div class="summary-box"><div class="summary-label">購入点数</div><div class="summary-value">{summary['total_points']}</div></div>
          <div class="summary-box"><div class="summary-label">収支</div><div class="summary-value {profit_class(summary['total_profit'])}">{signed_yen(summary['total_profit'])}</div></div>
        </div>
      </div>
      {cards_html}
    </div>
    '''
    return render_layout("今日の買い候補", content)


def render_stats_page(race_date, summary, by_rating, by_venue, by_ai_rating, by_final_rank, by_candidate_source):
    def make_table(rows):
        if not rows:
            return '<div class="empty">データがありません</div>'
        body = ""
        for r in rows:
            body += f"<tr><td>{r['group_name']}</td><td>{r['total_bets']}</td><td>{r['total_points']}</td><td>{r['total_hits']}</td><td>{yen(r['total_investment'])}</td><td>{yen(r['total_payout'])}</td><td class='{profit_class(r['total_profit'])}'>{signed_yen(r['total_profit'])}</td><td>{percent(r['hit_rate'])}</td><td>{percent(r['roi'])}</td></tr>"
        return f"<div class='table-wrap'><table><thead><tr><th>区分</th><th>購入レース</th><th>購入点数</th><th>的中</th><th>投資</th><th>払戻</th><th>収支</th><th>的中率</th><th>回収率</th></tr></thead><tbody>{body}</tbody></table></div>"

    content = f'''
    <div class="app-shell">
      <div class="topbar">
        <div class="brand">
          <div class="brand-logo">📊</div>
          <div>
            <div class="brand-title">Race Candidates</div>
            <div class="brand-sub">今日の集計</div>
          </div>
        </div>
        <div class="topbar-status"><span class="top-pill">対象日: {race_date}</span></div>
      </div>
      <div class="header hero hero-strong">
        <div class="title">今日の集計</div>
        <div class="sub">対象日: {race_date}</div>
        <div class="sub">最終取込時刻: {summary['last_imported_at'] or '未更新'}</div>
        <div class="nav nav-app"><a href="/" class="nav-card">今日の候補</a><a href="/stats" class="nav-card active">今日の集計</a><a href="/history" class="nav-card">過去データ</a></div>
        <div class="summary six">
          <div class="summary-box"><div class="summary-label">全候補数</div><div class="summary-value">{summary['total_rows']}</div></div>
          <div class="summary-box"><div class="summary-label">購入レース数</div><div class="summary-value">{summary['total_bets']}</div></div>
          <div class="summary-box"><div class="summary-label">購入点数</div><div class="summary-value">{summary['total_points']}</div></div>
          <div class="summary-box"><div class="summary-label">的中数</div><div class="summary-value">{summary['total_hits']}</div></div>
          <div class="summary-box"><div class="summary-label">投資額</div><div class="summary-value">{yen(summary['total_investment'])}</div></div>
          <div class="summary-box"><div class="summary-label">払戻額</div><div class="summary-value">{yen(summary['total_payout'])}</div></div>
        </div>
        <div class="summary" style="margin-top:8px;">
          <div class="summary-box"><div class="summary-label">収支</div><div class="summary-value {profit_class(summary['total_profit'])}">{signed_yen(summary['total_profit'])}</div></div>
          <div class="summary-box"><div class="summary-label">的中率</div><div class="summary-value">{percent(summary['hit_rate'])}</div></div>
          <div class="summary-box"><div class="summary-label">回収率</div><div class="summary-value">{percent(summary['roi'])}</div></div>
          <div class="summary-box"><div class="summary-label">1点あたり平均投資</div><div class="summary-value">{yen(round(summary['total_investment'] / summary['total_points']) if summary['total_points'] else 0)}</div></div>
        </div>
      </div>

      <div class="stats-grid">
        <div>
          <div class="header"><div class="section-title">公式星別集計</div></div>
          {make_table(by_rating)}
        </div>
        <div>
          <div class="header"><div class="section-title">AI補正星別集計</div></div>
          {make_table(by_ai_rating)}
        </div>
      </div>

      <div class="stats-grid">
        <div>
          <div class="header"><div class="section-title">最終判定別集計</div></div>
          {make_table(by_final_rank)}
        </div>
        <div>
          <div class="header"><div class="section-title">候補タイプ別集計</div></div>
          {make_table(by_candidate_source)}
        </div>
      </div>

      <div class="stats-grid">
        <div>
          <div class="header"><div class="section-title">会場別集計</div></div>
          {make_table(by_venue)}
        </div>
      </div>
    </div>
    '''
    return render_layout("今日の集計", content)


def render_history_page(date_summaries):
    if not date_summaries:
        list_html = '<div class="empty">過去データはありません</div>'
    else:
        items = ""
        for item in date_summaries:
            d = item["race_date"]
            s = item["summary"]
            items += f'''<div class="history-item"><div class="history-top"><div class="history-date">{d}</div><a class="history-link" href="/history/{d}">結果を見る</a></div><div class="history-mini"><div class="history-mini-box"><div class="history-mini-label">候補数</div><div class="history-mini-value">{s['total_rows']}</div></div><div class="history-mini-box"><div class="history-mini-label">購入レース</div><div class="history-mini-value">{s['total_bets']}</div></div><div class="history-mini-box"><div class="history-mini-label">購入点数</div><div class="history-mini-value">{s['total_points']}</div></div><div class="history-mini-box"><div class="history-mini-label">収支</div><div class="history-mini-value {profit_class(s['total_profit'])}">{signed_yen(s['total_profit'])}</div></div></div></div>'''
        list_html = f'<div class="header"><div class="history-list">{items}</div></div>'
    return render_layout("過去データ", f'<div class="app-shell"><div class="topbar"><div class="brand"><div class="brand-logo">🗂️</div><div><div class="brand-title">Race Candidates</div><div class="brand-sub">過去データ一覧</div></div></div></div><div class="header hero hero-strong"><div class="title">過去データ</div><div class="nav nav-app"><a href="/" class="nav-card">今日の候補</a><a href="/stats" class="nav-card">今日の集計</a><a href="/history" class="nav-card active">過去データ</a></div></div>{list_html}</div>')


def render_history_detail_page(
    race_date,
    races,
    summary,
    message_type="",
    message_text="",
    venue_filter="",
    race_no_filter="",
    purchased_only=False,
    hit_only=False,
):
    if message_text:
        message_class = "message-success" if message_type == "success" else "message-error"
        message_html = f'<div class="message {message_class}">{message_text}</div>'
    else:
        message_html = ""

    venue_options_html, race_no_options_html, _all_venues, _all_race_nos = make_history_filter_options(
        races,
        selected_venue=venue_filter,
        selected_race_no=race_no_filter,
    )

    filtered_races = filter_history_races(
        races,
        venue_filter=venue_filter,
        race_no_filter=race_no_filter,
        purchased_only=purchased_only,
        hit_only=hit_only,
    )

    checked_purchased = "checked" if purchased_only else ""
    checked_hit = "checked" if hit_only else ""

    jump_items = []
    seen_race_nos = set()
    for r in filtered_races:
        race_no = str(r.get("race_no", "")).strip()
        if race_no and race_no not in seen_race_nos:
            seen_race_nos.add(race_no)
            jump_items.append(f'<a class="jump-chip" href="#race-card-{r["id"]}">{race_no}</a>')
    jump_html = "".join(jump_items) if jump_items else '<span class="jump-empty">ジャンプ候補なし</span>'

    if not filtered_races:
        body = '<div class="empty">条件に合うデータがありません</div>'
    else:
        cards_html = ''.join([build_safe_card_html(r, is_history=True, race_date=race_date) for r in filtered_races])
        body = f'''
        <form id="bulk-delete-form" method="post" action="/delete_records_bulk" onsubmit="return confirmBulkDelete();"><input type="hidden" name="redirect_to" value="/history/{race_date}"></form>
        <div class="header history-filter-box">
          <div class="section-title">絞り込み</div>
          <form method="get" action="/history/{race_date}" class="filter-box">
            <div class="history-filter-grid">
              <div class="filter-item"><label for="venue">会場</label><select name="venue" id="venue">{venue_options_html}</select></div>
              <div class="filter-item"><label for="race_no">R</label><select name="race_no" id="race_no">{race_no_options_html}</select></div>
              <div class="filter-item filter-item-check"><label class="filter-check"><input type="checkbox" name="purchased_only" value="1" {checked_purchased}>購入済みのみ</label></div>
              <div class="filter-item filter-item-check"><label class="filter-check"><input type="checkbox" name="hit_only" value="1" {checked_hit}>的中のみ</label></div>
              <div class="filter-actions"><button type="submit" class="filter-btn">絞り込む</button><a href="/history/{race_date}" class="filter-reset">解除</a></div>
            </div>
          </form>
          <div class="history-filter-meta"><div class="history-filter-count">表示中 {len(filtered_races)} / 全{len(races)}件</div><div class="jump-wrap">{jump_html}</div></div>
        </div>
        <div class="bulk-toolbar"><div class="bulk-toolbar-left"><button type="button" class="toolbar-btn" onclick="toggleAllBulk(true)">全選択</button><button type="button" class="toolbar-btn toolbar-btn-muted" onclick="toggleAllBulk(false)">選択解除</button></div><div class="bulk-toolbar-right"><span class="bulk-count" id="bulk-delete-count">0件選択中</span><button type="submit" class="toolbar-delete-btn" form="bulk-delete-form">選択したものを削除</button></div></div>
        {cards_html}
        '''
    content = f'''
    <div class="app-shell">
      <div class="topbar"><div class="brand"><div class="brand-logo">🧾</div><div><div class="brand-title">Race Candidates</div><div class="brand-sub">過去データ詳細</div></div></div><div class="topbar-status"><span class="top-pill">対象日: {race_date}</span></div></div>
      <div class="header hero hero-strong"><div class="title">過去データ詳細</div><div class="sub">対象日: {race_date}</div><div class="sub">最終取込時刻: {summary['last_imported_at'] or '未更新'}</div>{message_html}<div class="nav nav-app"><a href="/history" class="nav-card">過去データ一覧</a><a href="/" class="nav-card">今日の候補</a><a href="/history/{race_date}" class="nav-card active">この日の詳細</a><a href="/export/history/{race_date}.csv" class="nav-card">この日CSV</a></div><div class="summary"><div class="summary-box"><div class="summary-label">候補数</div><div class="summary-value">{summary['total_rows']}</div></div><div class="summary-box"><div class="summary-label">購入レース</div><div class="summary-value">{summary['total_bets']}</div></div><div class="summary-box"><div class="summary-label">購入点数</div><div class="summary-value">{summary['total_points']}</div></div><div class="summary-box"><div class="summary-label">収支</div><div class="summary-value {profit_class(summary['total_profit'])}">{signed_yen(summary['total_profit'])}</div></div></div></div>
      {body}
    </div>
    '''
    return render_layout("過去データ詳細", content)


def render_layout(title, body_html):
    home_active = "active" if title == "今日の買い候補" else ""
    stats_active = "active" if title == "今日の集計" else ""
    history_active = "active" if title in ["過去データ", "過去データ詳細"] else ""

    css = """
    <style>
      *{box-sizing:border-box}
      html{padding-top:env(safe-area-inset-top,0px);background:#f5f7fb}
      body{
        margin:0;
        background:#f5f7fb;
        font-family:-apple-system,BlinkMacSystemFont,'Helvetica Neue','Yu Gothic',sans-serif;
        color:#222;
        -webkit-text-size-adjust:100%;
      }
      .container{
        max-width:980px;
        margin:0 auto;
        padding:calc(16px + env(safe-area-inset-top,0px)) 12px calc(92px + env(safe-area-inset-bottom,0px));
      }
      .app-shell{display:flex;flex-direction:column;gap:14px}
      .topbar,.header,.card,.history-item{background:#fff;border-radius:16px;padding:14px;box-shadow:0 4px 14px rgba(0,0,0,.06)}
      .hero-strong{background:linear-gradient(180deg,#fff,#f8fbff)}
      .brand{display:flex;align-items:center;gap:10px;min-width:0}
      .brand-logo{font-size:28px;flex:0 0 auto}
      .brand-title{font-weight:700}
      .brand-sub,.sub{font-size:13px;color:#667085}
      .topbar{display:flex;justify-content:space-between;align-items:center;gap:10px}
      .topbar-status{min-width:0;display:flex;justify-content:flex-end}
      .top-pill{background:#eef4ff;color:#2f5bd2;padding:6px 10px;border-radius:999px;font-size:12px;display:inline-block;max-width:100%;white-space:normal;word-break:break-word;line-height:1.4}
      .title{font-size:24px;font-weight:800;margin-bottom:4px}
      .nav{display:flex;gap:8px;flex-wrap:wrap;margin-top:10px}
      .nav-card{background:#eef2ff;color:#334;padding:9px 12px;border-radius:10px;text-decoration:none}
      .nav-card.active{background:#2f5bd2;color:#fff}
      .summary,.summary.six{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-top:12px}
      .summary.six{grid-template-columns:repeat(6,1fr)}
      .summary-box,.history-mini-box{background:#f8fafc;border:1px solid #eaecf0;border-radius:12px;padding:10px}
      .summary-label,.history-mini-label{font-size:12px;color:#667085}
      .summary-value,.history-mini-value{font-size:20px;font-weight:800;margin-top:4px}
      .profit-plus{color:#175cd3}
      .profit-minus{color:#d92d20}
      .profit-zero{color:#344054}
      .filter-box,.info-box{margin-top:10px}
      .filter-grid{display:grid;grid-template-columns:1.1fr 1.1fr 1fr 1fr auto;gap:10px;align-items:end}
      .filter-item label{display:block;font-size:12px;color:#667085;margin-bottom:4px}
      .filter-check{display:flex;align-items:center;gap:8px}
      select,input[type=text],input[type=number]{width:100%;padding:10px;border:1px solid #d0d5dd;border-radius:10px;background:#fff}
      .filter-btn,.save-btn,.toolbar-delete-btn,.delete-btn,.toolbar-btn{border:none;border-radius:10px;padding:10px 14px;font-weight:700;cursor:pointer}
      .filter-btn,.save-btn,.toolbar-delete-btn{background:#2f5bd2;color:#fff}
      .delete-btn,.toolbar-btn-muted{background:#fee4e2;color:#b42318}
      .toolbar-btn{background:#eef2ff;color:#344054}
      .filter-reset{display:inline-flex;align-items:center;justify-content:center;padding:10px 14px;text-decoration:none;background:#f2f4f7;color:#344054;border-radius:10px}
      .card-top-main{display:flex;justify-content:space-between;gap:10px;align-items:flex-start}
      .time-line{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
      .time{font-size:24px;font-weight:800}
      .countdown-badge{display:inline-flex;padding:5px 9px;border-radius:999px;font-size:12px;font-weight:700}
      .countdown-normal{background:#eef2ff;color:#3538cd}
      .countdown-warning{background:#fff4e5;color:#b54708}
      .countdown-soon{background:#ffead5;color:#c4320a}
      .countdown-closed{background:#f2f4f7;color:#475467}
      .race-spot-main{display:inline-flex;gap:8px;align-items:center;padding:8px 12px;border-radius:12px;background:#101828;color:#fff;font-weight:800}
      .race-venue{font-size:22px}
      .race-rno{font-size:22px}
      .status-wrap{display:flex;gap:8px;flex-wrap:wrap}
      .status-badge{padding:7px 10px;border-radius:999px;font-size:12px;font-weight:700}
      .status-badge-saved{background:#ecfdf3;color:#067647}
      .status-badge-hit{background:#fff1f3;color:#c11574}
      .badge-row,.metric-badge-row{display:flex;gap:8px;flex-wrap:wrap;margin-top:12px}
      .rating,.ai-rating,.final-rank,.metric-badge,.source-badge,.base-quality-badge{display:inline-flex;align-items:center;gap:6px;padding:8px 10px;border-radius:999px;font-size:13px;font-weight:700}
      .source-badge-official{background:#f2f4f7;color:#344054}
      .source-badge-shadow{background:#f4ebff;color:#6941c6;border:1px solid #d6bbfb}
      .source-badge-all{background:#f8fafc;color:#475467;border:1px solid #e4e7ec}
      .source-badge-all-race{background:#f8fafc;color:#475467;border:1px dashed #98a2b3}
      .base-quality-strong{background:#ecfdf3;color:#067647;border:1px solid #abefc6}
      .base-quality-good{background:#eff8ff;color:#175cd3;border:1px solid #b2ddff}
      .base-quality-watch{background:#fffaeb;color:#b54708;border:1px solid #fedf89}
      .base-quality-risk{background:#fef3f2;color:#b42318;border:1px solid #fecdca}
      .rating{background:#fff6e5;color:#b54708}
      .ai-rating{background:#eef4ff;color:#175cd3}
      .final-rank-strong{background:#ecfdf3;color:#027a48}
      .final-rank-buy{background:#e0f2fe;color:#0369a1}
      .final-rank-watch{background:#f2f4f7;color:#475467}
      .final-rank-skip{background:#fef3f2;color:#b42318}
      .metric-badge{background:#f8fafc;border:1px solid #eaecf0}
      .metric-badge-strong{background:#eef4ff}
      .metric-badge-score{background:#fff6e5}
      .metric-badge-label{color:#667085}
      .metric-badge-value{font-weight:800}

      .ex-status-strip{margin-top:10px;border:1px solid #e5e7eb;border-radius:16px;padding:10px 12px;display:flex;align-items:center;justify-content:space-between;gap:10px;background:#f8fafc}
      .ex-status-main{display:flex;align-items:center;gap:8px;min-width:0;flex-wrap:wrap}
      .ex-status-dot{width:10px;height:10px;border-radius:999px;background:#94a3b8;box-shadow:0 0 0 4px rgba(148,163,184,.14)}
      .ex-status-title{font-weight:950;font-size:14px;color:#334155}
      .ex-status-sub{font-size:12px;font-weight:800;color:#64748b}
      .ex-status-chips{display:flex;gap:6px;flex-wrap:wrap;justify-content:flex-end}
      .ex-status-chips span{border:1px solid #e2e8f0;background:#fff;border-radius:999px;padding:5px 8px;font-size:12px;font-weight:900;color:#475569;white-space:nowrap}
      .ex-status-ok{background:#ecfdf5;border-color:#bbf7d0}
      .ex-status-ok .ex-status-dot{background:#22c55e;box-shadow:0 0 0 4px rgba(34,197,94,.15)}
      .ex-status-ok .ex-status-title{color:#166534}
      .ex-status-wait{background:#fff7ed;border-color:#fed7aa}
      .ex-status-wait .ex-status-dot{background:#f97316;box-shadow:0 0 0 4px rgba(249,115,22,.16)}
      .ex-status-wait .ex-status-title{color:#9a3412}
      .row{display:grid;grid-template-columns:110px 1fr;gap:10px;align-items:start;padding:10px 0;border-top:1px solid #eaecf0}
      .row:first-child{border-top:none}
      .label{font-weight:700;color:#344054}
      .value{min-width:0}
      .selection-compare-wrap{display:grid;grid-template-columns:1fr 1fr;gap:10px}
      .selection-compare-col{background:#f8fafc;border:1px solid #eaecf0;border-radius:10px;padding:8px}
      .selection-col-title{font-size:12px;color:#667085;margin-bottom:6px;font-weight:700}
      .selection-chip-grid{display:flex;gap:6px;flex-wrap:wrap}
      .ai-selection-block{display:flex;flex-direction:column;gap:10px}
      .quick-select-row{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:2px}
      .quick-select-btn{border:1px solid #d0d5dd;background:#fff;color:#344054;border-radius:999px;padding:7px 10px;font-size:12px;font-weight:800;cursor:pointer}
      .quick-select-main{background:#101828;color:#fff;border-color:#101828}
      .quick-select-clear{background:#f2f4f7;color:#667085}
      .selection-section{border-radius:12px;padding:8px;border:1px solid #eaecf0;background:#fff}
      .selection-section-core{background:#fff7ed;border-color:#fed7aa}
      .selection-section-cover{background:#f8fafc}
      .selection-section-title{font-size:12px;font-weight:900;color:#344054;margin-bottom:7px}
      .selection-choice-core .selection-choice-body{border-width:2px}
      .selection-choice-cover .selection-choice-body{opacity:.92}
      .bet-guide-box{margin-top:12px;border-radius:16px;border:1px solid #eaecf0;padding:12px;background:#fff;box-shadow:0 4px 14px rgba(0,0,0,.04)}
      .bet-guide-strong{background:linear-gradient(180deg,#ecfdf3,#ffffff);border-color:#abefc6}
      .bet-guide-buy{background:linear-gradient(180deg,#eef4ff,#ffffff);border-color:#cfe0ff}
      .bet-guide-watch{background:linear-gradient(180deg,#fff6e5,#ffffff);border-color:#f5deb3}
      .bet-guide-skip{background:linear-gradient(180deg,#f8fafc,#ffffff);border-color:#eaecf0}
      .bet-guide-head{display:flex;align-items:flex-start;justify-content:space-between;gap:12px}
      .bet-guide-kicker{font-size:12px;color:#667085;font-weight:800}
      .bet-guide-title{font-size:18px;font-weight:900;color:#101828;margin-top:2px}
      .bet-guide-recommend{background:#101828;color:#fff;border-radius:999px;padding:8px 12px;font-size:13px;font-weight:900;white-space:nowrap}
      .bet-guide-body{margin-top:10px;display:flex;flex-direction:column;gap:8px}
      .bet-guide-row{display:grid;grid-template-columns:92px 1fr;gap:8px;align-items:center}
      .bet-guide-label{font-size:12px;color:#667085;font-weight:800}
      .bet-guide-picks{display:flex;flex-wrap:wrap;gap:6px}
      .guide-pick-chip{display:inline-flex;align-items:center;border:1px solid #eaecf0;background:#fff;border-radius:999px;padding:5px 7px}
      .guide-pick-official{background:#eef4ff;border-color:#cfe0ff}
      .guide-check-wrap{display:flex;flex-wrap:wrap;gap:6px}
      .guide-check{display:inline-flex;align-items:center;gap:5px;border-radius:999px;padding:6px 9px;font-size:12px;font-weight:800;border:1px solid}
      .guide-check-ok{background:#ecfdf3;border-color:#abefc6;color:#027a48}
      .guide-check-ng{background:#f2f4f7;border-color:#d0d5dd;color:#667085}
      .guide-check-mark{font-size:10px;font-weight:900}
      .bet-guide-memo{font-size:13px;color:#475467;font-weight:700;line-height:1.5}
      .quick-select-recommend{margin-top:10px;background:#101828;color:#fff;width:100%}
      .bet-control-box{margin:0 0 12px;padding:12px;border-radius:14px;background:#f8fafc;border:1px solid #eaecf0}
      .bet-control-title{font-size:13px;font-weight:900;color:#344054;margin-bottom:8px}
      .bet-control-grid{display:grid;grid-template-columns:180px 1fr;gap:12px;align-items:end}
      .bet-control-item label{display:block;font-size:12px;color:#667085;margin-bottom:4px;font-weight:700}
      .stake-select{font-weight:900;background:#fff}
      .bet-control-hint{font-size:12px;color:#667085;line-height:1.45}
      .bet-control-hint strong{color:#101828}
      .selection-choice-chip{display:inline-block;cursor:pointer;user-select:none;-webkit-tap-highlight-color:transparent}
      .selection-view-chip{display:inline-block}
      .selection-choice-body-view{cursor:default}
      .selection-view-chip .selection-choice-body{border-style:solid}
      .selection-view-chip.is-selected-view .selection-choice-body{box-shadow:0 0 0 2px rgba(5,96,58,.08) inset}
      .selection-choice-input{position:absolute;opacity:0;pointer-events:none;width:1px;height:1px}
      .selection-choice-body{display:inline-flex;align-items:center;justify-content:center;padding:8px 10px;border-radius:999px;font-weight:700;border:2px solid #d0d5dd;background:#fff;color:#344054;white-space:nowrap;line-height:1.2;transition:all .15s ease}
      .selection-choice-chip:hover .selection-choice-body{transform:translateY(-1px)}
      .selection-choice-input:focus + .selection-choice-body{outline:2px solid rgba(47,91,210,.18);outline-offset:2px}
      .selection-choice-body-overlap{background:#f3fbf6;border-color:#bfe7cc;color:#5f7a68}
      .selection-choice-input:checked + .selection-choice-body-overlap{background:#dcfae6;border-color:#6fd69a;color:#05603a;box-shadow:0 0 0 2px rgba(5,96,58,.08) inset}
      .selection-choice-input:checked + .selection-choice-body-official{background:#e7f0ff;border-color:#8fb4ff;color:#124fc2;box-shadow:0 0 0 2px rgba(18,79,194,.08) inset}
      .selection-choice-input:checked + .selection-choice-body-ai{background:#fff1db;border-color:#f2b96b;color:#a64b00;box-shadow:0 0 0 2px rgba(166,75,0,.08) inset}
      .lane-color{display:inline-flex;align-items:center;justify-content:center;min-width:24px;height:24px;padding:0 4px;border-radius:2px;font-weight:800;font-size:14px;line-height:1;border:1px solid rgba(0,0,0,.10);box-shadow:inset 0 0 0 1px rgba(255,255,255,.08)}
      .lane-color-1{background:#ffffff;color:#111827;border-color:#d1d5db}
      .lane-color-2{background:#1f2937;color:#ffffff;border-color:#1f2937}
      .lane-color-3{background:#ef4444;color:#ffffff;border-color:#ef4444}
      .lane-color-4{background:#3b82f6;color:#ffffff;border-color:#3b82f6}
      .lane-color-5{background:#fde047;color:#111827;border-color:#eab308}
      .lane-color-6{background:#22c55e;color:#ffffff;border-color:#22c55e}
      .pick-inline .lane-color{min-width:22px;height:22px;font-size:14px;border-radius:2px;padding:0 3px}
      .picked-chip .pick-inline .lane-color,.selection-choice-body .pick-inline .lane-color{min-width:21px;height:21px;font-size:13px}
      .player-chip .lane-color,.class-history-lane .lane-color,.ex-chip-lane .lane-color,.ex-lane .lane-color{min-width:28px;height:24px;font-size:14px;border-radius:2px}
      .ex-chip{display:inline-flex;align-items:center;gap:8px}
      .ex-chip-time{font-weight:700}
      .ex-lane{display:flex;justify-content:center;margin-bottom:4px}
      .pick-inline{display:inline-flex;align-items:center;gap:4px;flex-wrap:nowrap}
      .pick-sep{font-weight:900;color:#667085;font-size:12px;line-height:1}
      .pick-plain{font-weight:800;color:#344054}
      .row-player-rank .label,.row-exhibition-rank .label{padding-top:4px}
      .row-player-rank .value{width:100%}
      .player-rank-wrap{display:flex;flex-direction:column;gap:10px;padding:4px 0}
      .player-rank-row{display:grid;grid-template-columns:minmax(220px,300px) 1fr;gap:16px;align-items:start;padding:10px 0;border-top:1px solid #eaecf0}
      .player-rank-row:first-of-type{border-top:none}
      .player-rank-main-wrap{display:flex;flex-direction:column;gap:8px;min-width:0}
      .player-rank-class-row{display:flex;flex-wrap:wrap;gap:6px;padding-left:34px}
      .player-rank-main{display:flex;align-items:center;gap:10px;min-width:0}
      .player-rank-lane{flex:0 0 auto}
      .player-rank-name{min-width:0;font-weight:800;color:#172033;line-height:1.45;word-break:keep-all;overflow-wrap:anywhere}
      .player-rank-evidence{min-width:0}
      .player-evidence-wrap{display:flex;flex-wrap:wrap;gap:6px;align-items:flex-start}
      .player-evidence-chip{display:inline-flex;align-items:center;padding:5px 9px;border-radius:999px;border:1px solid;font-size:12px;font-weight:700;line-height:1.2;white-space:nowrap}
      .player-evidence-chip-plus{background:#ecfdf3;border-color:#abefc6;color:#027a48}
      .player-evidence-chip-minus{background:#fef3f2;border-color:#fecdca;color:#b42318}
      .player-evidence-chip-neutral{background:#f2f4f7;border-color:#d0d5dd;color:#475467}
      .picked-chip-wrap,.ex-chip-wrap,.lane-score-wrap,.detail-chip-wrap,.weather-chip-wrap{display:flex;gap:8px;flex-wrap:wrap}
      .picked-chip,.ex-chip,.lane-score-chip,.detail-chip,.weather-chip{padding:6px 8px;border-radius:8px;background:#f8fafc;border:1px solid #eaecf0}
      .picked-chip{white-space:nowrap}
      .weather-chip{font-weight:700;color:#344054}
      .weather-chip-windtype{background:#eef4ff;color:#175cd3;border-color:#cfe0ff}
      .weather-chip-dir{background:#f4ebff;color:#6941c6;border-color:#e0d2ff}
      .weather-chip-num{background:#fff6e5;color:#b54708;border-color:#f5deb3}
      .weather-chip-good{background:#ecfdf3;color:#027a48;border-color:#abefc6}
      .weather-chip-bad{background:#fef3f2;color:#b42318;border-color:#fecdca}
      .selection-chip-empty,.ex-chip-empty,.lane-score-empty,.detail-chip-empty,.class-history-empty,.ex-rank-empty,.player-empty,.player-rank-empty{color:#667085}
      .ex-chip-lane,.lane-score-lane{font-weight:800;margin-right:6px;display:inline-flex;align-items:center}
      .lane-score-verygood{background:#ecfdf3}
      .lane-score-good{background:#eef4ff}
      .lane-score-bad{background:#fef3f2}
      .ex-rank-grid{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:6px}
      .ex-rank-box{border:1px solid #eaecf0;background:#f8fafc;border-radius:8px;padding:6px 4px;text-align:center}
      .ex-rank-1{background:#ecfdf3}
      .ex-rank-2{background:#eef4ff}
      .ex-rank-3{background:#fff6e5}
      .ex-rank-low{background:#fef3f2}
      .ex-rank{font-size:14px;font-weight:800;line-height:1.1}
      .class-chip{display:inline-flex;gap:5px;align-items:center;border-radius:10px;padding:6px 8px;border:1px solid #d0d5dd;background:#fff}
      .class-chip-a1{background:#ecfdf3}
      .class-chip-a2{background:#eef4ff}
      .class-chip-b1{background:#fff6e5}
      .class-chip-b2{background:#fef3f2}
      .class-chip-sub{font-size:10px;color:#667085}
      .class-chip-main{font-size:13px;font-weight:800}
      .current-class-chip .class-chip-main{font-size:15px}
      .form{margin-top:12px}
      .detail-box{display:flex;flex-direction:column;gap:10px}
      .detail-box-simple{gap:0}
      .auto-result-note{padding:10px 12px;border-radius:10px;background:#f8fafc;border:1px solid #eaecf0;color:#475467;font-size:13px;font-weight:700}
      .checkline{display:flex;align-items:center;gap:8px;font-weight:700}
      .input-row label{display:block;font-size:12px;color:#667085;margin-bottom:4px}
      .save-btn{width:100%;margin-top:10px}
      .half-btn{width:100%}
      .delete-form{margin-top:8px}
      .message{margin-top:10px;padding:10px 12px;border-radius:10px;font-weight:700}
      .message-success{background:#ecfdf3;color:#027a48}
      .message-error{background:#fef3f2;color:#b42318}
      .empty{background:#fff;border-radius:16px;padding:24px;text-align:center;color:#667085;box-shadow:0 4px 14px rgba(0,0,0,.06)}
      .history-list{display:flex;flex-direction:column;gap:10px}
      .history-top{display:flex;justify-content:space-between;align-items:center;gap:8px}
      .history-date{font-size:20px;font-weight:800}
      .history-link{text-decoration:none;background:#eef4ff;color:#175cd3;padding:8px 10px;border-radius:10px}
      .history-mini{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-top:10px}
      .table-wrap{overflow:auto;background:#fff;border-radius:16px;box-shadow:0 4px 14px rgba(0,0,0,.06)}
      table{width:100%;border-collapse:collapse;background:#fff}
      th,td{padding:10px 12px;border-bottom:1px solid #eaecf0;text-align:left;white-space:nowrap}
      th{background:#f8fafc}
      .stats-grid{display:grid;grid-template-columns:1fr 1fr;gap:14px}
      .section-title{font-size:18px;font-weight:800}
      .bulk-toolbar{display:flex;justify-content:space-between;gap:10px;align-items:center;background:#fff;border-radius:14px;padding:12px;box-shadow:0 4px 14px rgba(0,0,0,.06)}
      .bulk-toolbar-left,.bulk-toolbar-right{display:flex;gap:8px;align-items:center}
      .history-filter-box{padding:16px}
      .history-filter-grid{display:grid;grid-template-columns:1.2fr 180px auto auto auto;gap:12px;align-items:end;margin-top:10px}
      .filter-item-check{display:flex;align-items:end}
      .history-filter-meta{display:flex;justify-content:space-between;align-items:flex-start;gap:12px;margin-top:14px;flex-wrap:wrap}
      .history-filter-count{font-size:13px;font-weight:800;color:#475467}
      .jump-wrap{display:flex;gap:8px;flex-wrap:wrap}
      .jump-chip{display:inline-flex;align-items:center;justify-content:center;min-width:52px;padding:8px 12px;border-radius:999px;text-decoration:none;background:#eef4ff;color:#175cd3;border:1px solid #cdddff;font-weight:800}
      .jump-empty{color:#98a2b3;font-size:13px}
      .card-hit{border-color:#f5c2da;box-shadow:0 14px 38px rgba(193,21,116,.08)}
      .card-purchased{border-color:#bfe3cd;box-shadow:0 14px 38px rgba(6,118,71,.08)}
      .bottom-nav{position:fixed;left:0;right:0;bottom:0;display:grid;grid-template-columns:repeat(3,1fr);background:#fff;border-top:1px solid #eaecf0;padding:8px 10px calc(8px + env(safe-area-inset-bottom,0px));z-index:50}
      .bottom-nav-item{text-decoration:none;color:#667085;display:flex;flex-direction:column;align-items:center;gap:2px;padding:6px 0}
      .bottom-nav-item.active{color:#175cd3;font-weight:800}
      @media (max-width: 760px){
        html{background:#f5f7fb}
        .container{max-width:none;padding:calc(12px + env(safe-area-inset-top,0px)) 10px calc(92px + env(safe-area-inset-bottom,0px));}
        .topbar{flex-direction:column;align-items:flex-start;padding:14px;border-radius:18px;}
        .topbar-status{width:100%;justify-content:flex-start;}
        .top-pill{width:100%;border-radius:12px;}
        .header,.card,.history-item,.bulk-toolbar,.history-filter-box{padding:14px;border-radius:18px;}
        .summary,.summary.six,.history-mini,.stats-grid,.filter-grid,.history-filter-grid,.bet-control-grid{grid-template-columns:1fr;}
        .selection-compare-wrap{grid-template-columns:minmax(0,1.08fr) minmax(0,.92fr);gap:8px;}
        .row{grid-template-columns:1fr;gap:8px;}
        .race-venue,.race-rno{font-size:20px}
        .time{font-size:22px}
        .ex-rank-grid{grid-template-columns:repeat(3,1fr);gap:6px}
        .ex-rank-box{padding:6px 4px}
        .ex-rank{font-size:13px}
        .card-top-main{flex-direction:column;align-items:flex-start}
        .status-wrap{margin-top:2px}
        .history-filter-meta{flex-direction:column;align-items:flex-start}
        .jump-wrap{width:100%}
        .jump-chip{min-width:48px;padding:8px 11px}
        .bulk-toolbar{flex-direction:column;align-items:stretch}
        .bulk-toolbar-left,.bulk-toolbar-right{width:100%;justify-content:space-between;flex-wrap:wrap}
        .player-rank-row{grid-template-columns:1fr;gap:8px;align-items:start;padding:8px 0}
        .player-rank-main{gap:8px}
        .player-rank-name{font-size:14px;line-height:1.4}
        .player-rank-evidence{width:100%}
        .player-evidence-wrap{gap:5px}
        .player-evidence-chip{font-size:11px;padding:5px 8px}
        .lane-color{min-width:24px;height:24px;font-size:14px;border-radius:4px}
        .pick-inline{gap:5px}
        .pick-sep{font-size:13px}
        .bottom-nav{left:0;right:0;transform:none;bottom:0;width:auto;border-radius:0;border-left:none;border-right:none;box-shadow:none;padding:8px 10px calc(8px + env(safe-area-inset-bottom,0px));}
        .bottom-nav-item{border-radius:12px}
        .bottom-nav-item.active{background:none;box-shadow:none;color:#175cd3;}
      }


      /* v10.28 UI polish */
      :root{
        --brand:#175cd3;
        --ink:#101828;
        --muted:#667085;
        --line:#e6eaf2;
        --surface:#ffffff;
        --soft:#f8fafc;
        --good:#027a48;
        --warn:#b54708;
        --bad:#b42318;
      }
      body{
        background:
          radial-gradient(circle at top left, rgba(47,91,210,.10), transparent 28%),
          radial-gradient(circle at top right, rgba(2,122,72,.08), transparent 24%),
          #f5f7fb;
      }
      .topbar{
        position:sticky;
        top:calc(8px + env(safe-area-inset-top,0px));
        z-index:40;
        backdrop-filter:blur(14px);
        background:rgba(255,255,255,.88);
        border:1px solid rgba(230,234,242,.9);
      }
      .brand-logo{
        width:42px;height:42px;border-radius:14px;display:flex;align-items:center;justify-content:center;
        background:linear-gradient(135deg,#eef4ff,#ecfdf3);
        box-shadow:inset 0 0 0 1px rgba(255,255,255,.8);
      }
      .hero-strong{
        border:1px solid rgba(230,234,242,.9);
        background:
          linear-gradient(180deg,rgba(255,255,255,.96),rgba(248,251,255,.94)),
          radial-gradient(circle at top right,rgba(47,91,210,.12),transparent 35%);
      }
      .daily-rule-panel{
        margin-top:12px;
        display:grid;
        grid-template-columns:1.1fr 1.5fr;
        gap:10px;
        align-items:center;
        border:1px solid #dbe7ff;
        background:linear-gradient(135deg,#eef4ff,#ffffff 58%,#ecfdf3);
        border-radius:16px;
        padding:12px;
      }
      .daily-rule-kicker{font-size:12px;font-weight:900;color:#175cd3;letter-spacing:.04em}
      .daily-rule-title{font-size:16px;font-weight:900;color:#101828;margin-top:2px;line-height:1.35}
      .daily-rule-steps{display:flex;gap:8px;flex-wrap:wrap;justify-content:flex-end}
      .daily-rule-steps span{display:inline-flex;border:1px solid #d0d5dd;background:#fff;border-radius:999px;padding:7px 10px;font-size:12px;font-weight:800;color:#344054}
      .card{
        position:relative;
        overflow:hidden;
        border:1px solid rgba(230,234,242,.95);
        box-shadow:0 10px 30px rgba(16,24,40,.07);
        transition:transform .16s ease, box-shadow .16s ease, border-color .16s ease;
      }
      .card:hover{transform:translateY(-1px);box-shadow:0 16px 38px rgba(16,24,40,.10)}
      .card:before{
        content:"";
        position:absolute;left:0;top:0;bottom:0;width:6px;
        background:#d0d5dd;
      }
      .card-rank-strong:before{background:linear-gradient(180deg,#12b76a,#175cd3)}
      .card-rank-buy:before{background:linear-gradient(180deg,#2e90fa,#175cd3)}
      .card-rank-watch:before{background:linear-gradient(180deg,#f79009,#fdb022)}
      .card-rank-skip:before{background:linear-gradient(180deg,#98a2b3,#d0d5dd)}
      .card-source-shadow:before{background:linear-gradient(180deg,#7f56d9,#175cd3)}
      .card-source-shadow{border-color:#d6bbfb}
      .card-source-all-race:before{background:linear-gradient(180deg,#98a2b3,#d0d5dd)}
      .card-source-all-race{border-color:#d0d5dd}
      .card-hit{box-shadow:0 16px 42px rgba(193,21,116,.10);border-color:#f5c2da}
      .card-purchased{box-shadow:0 16px 42px rgba(6,118,71,.10);border-color:#abefc6}
      .card-top-main{padding:4px 0 2px 6px;}
      .race-spot-main{background:linear-gradient(135deg,#101828,#263245);box-shadow:0 8px 18px rgba(16,24,40,.16);}
      .time{letter-spacing:-.03em}
      .badge-row,.metric-badge-row{padding-left:6px}
      .metric-badge{border-color:#e6eaf2;background:rgba(248,250,252,.88);}
      .metric-badge-strong{background:linear-gradient(135deg,#eef4ff,#ffffff);border-color:#cfe0ff;color:#124fc2;}
      .info-box{border:1px solid #edf0f5;background:#fff;border-radius:16px;padding:2px 12px;}
      .row-selection-highlight{background:linear-gradient(180deg,#fbfcff,#ffffff);margin:0 -6px;padding-left:6px;padding-right:6px;border-radius:14px;}
      .selection-compare-wrap{grid-template-columns:minmax(0,1.18fr) minmax(0,.82fr);}
      .selection-compare-col{border-color:#e6eaf2;background:#fff;box-shadow:inset 0 0 0 1px rgba(255,255,255,.7);}
      .selection-compare-col-ai{background:linear-gradient(180deg,#fffaf2,#ffffff);border-color:#f6d6a6;}
      .selection-col-title{display:flex;align-items:center;gap:6px;font-size:13px;color:#344054;}
      .selection-col-title-ai:before{content:"◎";color:#b54708;font-weight:900}
      .selection-col-title-official:before{content:"参";display:inline-flex;align-items:center;justify-content:center;width:18px;height:18px;border-radius:999px;background:#eef4ff;color:#175cd3;font-size:11px;font-weight:900}
      .quick-select-row{position:sticky;top:74px;z-index:8;background:rgba(255,250,242,.88);backdrop-filter:blur(10px);border:1px solid #f6d6a6;padding:8px;border-radius:14px;margin-bottom:10px;}
      .quick-select-btn{box-shadow:0 3px 10px rgba(16,24,40,.06);}
      .quick-select-main{background:linear-gradient(135deg,#101828,#175cd3);border-color:#175cd3;}
      .selection-section{border-radius:14px;padding:9px;}
      .selection-section-core{background:#fff7ea;border:1px solid #f6d6a6;}
      .selection-section-cover{margin-top:8px;background:#f8fafc;border:1px dashed #d0d5dd;}
      .selection-section-title{margin-bottom:7px;font-size:12px;font-weight:900;color:#344054;}
      .selection-section-core .selection-section-title:before{content:"上位 ";color:#b54708}
      .selection-section-cover .selection-section-title:before{content:"追加 ";color:#667085}
      .selection-choice-body{border-radius:14px;box-shadow:0 2px 8px rgba(16,24,40,.04);}
      .selection-choice-core .selection-choice-body{padding:10px 12px;border-color:#f2b96b;background:#fff;}
      .selection-choice-cover .selection-choice-body{background:#ffffff;border-color:#d8dee8;}
      .selection-choice-input:checked + .selection-choice-body{transform:translateY(-1px);}
      .bet-guide-box{padding:14px;border-radius:18px;box-shadow:0 10px 26px rgba(16,24,40,.06);}
      .bet-guide-title-wrap{display:flex;align-items:center;gap:10px}
      .bet-guide-icon{width:38px;height:38px;border-radius:14px;display:flex;align-items:center;justify-content:center;background:#fff;box-shadow:0 6px 14px rgba(16,24,40,.08);font-size:20px;flex:0 0 auto}
      .bet-guide-recommend{background:linear-gradient(135deg,#101828,#175cd3);box-shadow:0 8px 18px rgba(23,92,211,.18);}
      .bet-guide-strong .bet-guide-recommend{background:linear-gradient(135deg,#027a48,#12b76a)}
      .bet-guide-watch .bet-guide-recommend{background:linear-gradient(135deg,#b54708,#f79009)}
      .bet-guide-skip .bet-guide-recommend{background:#667085}
      .guide-check{box-shadow:0 2px 8px rgba(16,24,40,.04)}
      .quick-select-recommend{border-radius:14px;padding:12px 14px;font-size:14px;box-shadow:0 10px 24px rgba(16,24,40,.12);}
      .bet-control-box{background:linear-gradient(180deg,#ffffff,#f8fafc);border-color:#e6eaf2;box-shadow:0 8px 22px rgba(16,24,40,.05);}
      .stake-select{border:2px solid #cfe0ff;background:#fff;font-size:16px;color:#101828}
      .save-btn{background:linear-gradient(135deg,#175cd3,#101828);border-radius:14px;min-height:48px;box-shadow:0 12px 26px rgba(23,92,211,.22);transition:transform .14s ease, box-shadow .14s ease;}
      .save-btn:active{transform:scale(.99);box-shadow:0 6px 16px rgba(23,92,211,.18)}
      .bottom-nav{box-shadow:0 -10px 28px rgba(16,24,40,.08);}
      .bottom-nav-item.active{color:#175cd3;}
      @media (max-width:760px){
        .topbar{top:calc(6px + env(safe-area-inset-top,0px));border-radius:18px}
        .daily-rule-panel{grid-template-columns:1fr}
        .daily-rule-steps{justify-content:flex-start}
        .selection-compare-wrap{grid-template-columns:1fr}
        .quick-select-row{top:82px}
        .metric-badge-row{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px;padding-left:0}

        .ex-status-strip{align-items:flex-start;flex-direction:column;padding:9px 10px;border-radius:14px}
        .ex-status-title{font-size:13px}
        .ex-status-sub,.ex-status-chips span{font-size:11px}
        .metric-badge{justify-content:space-between;border-radius:14px;width:100%}
        .bet-guide-head{flex-direction:column}
        .bet-guide-recommend{width:100%;justify-content:center;text-align:center;border-radius:14px}
        .bet-guide-row{grid-template-columns:1fr;gap:6px}
        .bet-control-grid{grid-template-columns:1fr}
        .save-btn{position:sticky;bottom:calc(76px + env(safe-area-inset-bottom,0px));z-index:20}
        .info-box{padding:2px 10px}
        .card:before{width:5px}
      }

      /* v10.30 compact UI */
      .bet-guide-box{padding:10px 12px;margin-top:10px}
      .bet-guide-head{align-items:center}
      .bet-guide-title{font-size:16px}
      .bet-guide-icon{width:32px;height:32px;border-radius:12px;font-size:17px}
      .bet-guide-recommend{padding:7px 10px;font-size:12px}
      .bet-guide-detail{margin-top:8px}
      .bet-guide-detail summary,.detail-accordion summary{cursor:pointer;list-style:none;display:flex;align-items:center;justify-content:space-between;gap:8px;border:1px solid #e6eaf2;background:#f8fafc;border-radius:12px;padding:8px 10px;font-size:12px;font-weight:900;color:#344054}
      .bet-guide-detail summary::-webkit-details-marker,.detail-accordion summary::-webkit-details-marker{display:none}
      .bet-guide-detail summary:after,.detail-accordion summary:after{content:"開く";font-size:11px;color:#667085;background:#fff;border:1px solid #d0d5dd;border-radius:999px;padding:3px 7px}
      .bet-guide-detail[open] summary:after,.detail-accordion[open] summary:after{content:"閉じる"}
      .quick-select-recommend{width:100%;margin-top:8px;padding:9px 12px}
      .info-box{padding:0 10px}
      .row{padding:8px 0}
      .detail-accordion{border-top:1px solid #eaecf0;padding:8px 0}
      .detail-accordion .row{border-top:1px solid #eef2f6}
      .detail-accordion .row:first-of-type{border-top:none;margin-top:8px}
      .result-mini-grid{display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px}
      .result-mini-grid>div{background:#f8fafc;border:1px solid #eaecf0;border-radius:12px;padding:8px}
      .mini-label{display:block;font-size:11px;color:#667085;font-weight:800;margin-bottom:4px}
      .mini-value{display:block;font-size:13px;font-weight:900;color:#101828}
      .bet-control-box{margin-top:10px;padding:10px}
      .detail-box-simple{display:none}
      @media (max-width:760px){
        .container{padding-left:8px;padding-right:8px}
        .app-shell{gap:10px}
        .topbar,.header,.card,.history-item{padding:10px;border-radius:14px}
        .title{font-size:20px}
        .time{font-size:20px}
        .race-venue,.race-rno{font-size:19px}
        .badge-row{margin-top:8px}
        .metric-badge-row{grid-template-columns:repeat(3,minmax(0,1fr));gap:6px;margin-top:8px}
        .metric-badge{padding:7px 8px;font-size:12px;display:block}
        .metric-badge-label{display:block;font-size:10px}
        .metric-badge-value{display:block;margin-top:2px}
        .bet-guide-box{padding:9px;margin-top:8px;border-radius:14px}
        .bet-guide-kicker{font-size:10px}
        .bet-guide-title{font-size:15px}
        .bet-guide-recommend{font-size:12px;padding:7px 9px}
        .row{grid-template-columns:74px 1fr;gap:8px;padding:7px 0}
        .label{font-size:12px}
        .selection-compare-col{padding:7px}
        .quick-select-row{position:static;padding:6px;margin-bottom:6px}
        .quick-select-btn{padding:6px 9px;font-size:11px}
        .selection-section{padding:7px}
        .selection-choice-core .selection-choice-body,.selection-choice-body{padding:8px 9px}
        .result-mini-grid{grid-template-columns:1fr;gap:6px}
        .result-mini-grid>div{padding:7px 8px}
        .bet-control-grid{grid-template-columns:1fr 1.2fr;gap:8px}
        .bet-control-hint{font-size:11px}
        .save-btn{min-height:44px}
      }

    

      /* v10.31 compact UI fix: iPhoneでヘッダー/保存ボタンがカードに重なる問題を解消 */
      .topbar{position:static;top:auto;z-index:auto;}
      .save-btn{position:static;bottom:auto;z-index:auto;}
      @media (max-width:760px){
        .topbar{position:static;top:auto;z-index:auto;margin-bottom:8px;}
        .save-btn{position:static !important;bottom:auto !important;z-index:auto !important;}
        .bet-control-box{margin-bottom:8px;}
        .bottom-nav{z-index:60;}
      }


      /* v10.35: 買い目と保存を同じ画面に置く */
      .form-compact-save{margin-top:8px}
      .quick-save-panel{display:grid;grid-template-columns:auto 1fr minmax(120px,180px);gap:10px;align-items:center;margin:8px 0 10px;padding:10px;border:1px solid #dbe7ff;background:linear-gradient(180deg,#f8fbff,#ffffff);border-radius:16px;box-shadow:0 8px 22px rgba(16,24,40,.05)}
      .quick-save-left label{display:block;font-size:11px;font-weight:900;color:#667085;margin-bottom:3px}
      .stake-select-compact{min-height:38px;font-size:15px;border-radius:12px;padding:6px 26px 6px 10px}
      .quick-save-count{font-size:14px;font-weight:950;color:#101828;line-height:1.15}
      .quick-save-rule{font-size:11px;font-weight:800;color:#667085;margin-top:2px}
      .save-btn-compact{min-height:42px;margin:0;padding:10px 14px;width:100%}
      .info-box-picks-first{margin-top:0}
      .row-selected-compact{padding-top:6px;padding-bottom:6px}
      .row-selected-compact .selection-chip-empty{padding:5px 8px;font-size:12px}
      .info-box-picks-first .result-row-compact{display:none}
      .selection-compare-wrap{grid-template-columns:minmax(0,1.14fr) minmax(0,.86fr);gap:8px}
      .selection-section-title{font-size:11px;margin-bottom:5px}
      .selection-section{padding:7px}
      .selection-section-cover{margin-top:6px}
      .selection-choice-body{padding:7px 8px;font-size:13px;border-radius:12px}
      .selection-choice-core .selection-choice-body{padding:8px 9px}
      .quick-select-row{padding:6px;margin-bottom:6px;gap:6px}
      .quick-select-btn{padding:6px 8px;font-size:11px}
      .selection-col-title{font-size:12px;margin-bottom:5px}
      @media (max-width:760px){
        .form-compact-save{margin-top:6px}
        .quick-save-panel{grid-template-columns:86px 1fr 96px;gap:7px;padding:8px;border-radius:14px;margin:7px 0 8px}
        .stake-select-compact{min-height:36px;font-size:14px;padding-left:8px}
        .quick-save-count{font-size:13px}
        .quick-save-rule{font-size:10px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
        .save-btn-compact{min-height:38px;border-radius:12px;font-size:14px;padding:8px 10px}
        .selection-compare-wrap{grid-template-columns:minmax(0,1.08fr) minmax(0,.92fr)!important;gap:7px}
        .selection-compare-col{padding:6px;border-radius:12px}
        .selection-col-title{font-size:11px;line-height:1.25}
        .selection-section{padding:6px;border-radius:12px}
        .selection-section-title{font-size:10px;margin-bottom:5px}
        .selection-chip-grid{gap:5px}
        .selection-choice-body{padding:6px 7px;font-size:12px;border-width:1.5px;border-radius:10px}
        .selection-choice-core .selection-choice-body{padding:7px 7px}
        .quick-select-row{gap:5px;padding:5px;margin-bottom:5px}
        .quick-select-btn{padding:5px 7px;font-size:10px}
        .row{grid-template-columns:52px 1fr;gap:6px;padding:6px 0}
        .label{font-size:11px}
        .info-box{padding:0 8px}
        .result-row-compact{display:none}
      }
      @media (max-width:390px){
        .quick-save-panel{grid-template-columns:78px 1fr 86px;gap:6px;padding:7px}
        .quick-save-count{font-size:12px}
        .save-btn-compact{font-size:13px}
        .selection-compare-wrap{grid-template-columns:minmax(0,1.05fr) minmax(0,.95fr)!important;gap:6px}
        .selection-choice-body{font-size:11px;padding:5px 6px}
        .quick-select-btn{font-size:10px;padding:5px 6px}
      }

      /* v10.36: スマホは「保存バー＋買い目」を最優先で上に出す */
      .card{display:flex;flex-direction:column;}
      .card-top-main{order:1;}
      .badge-row{order:2;}
      .metric-badge-row{order:3;}
      .ex-status-strip{order:4;}
      .form-compact-save{order:5;}
      .bet-guide-box{order:6;}
      .delete-form{order:7;}
      @media (max-width:760px){
        .metric-badge-row{display:none!important;}
        .ex-status-strip{margin-top:7px;padding:7px 9px;border-radius:13px;gap:6px;}
        .ex-status-main{gap:6px;}
        .ex-status-dot{width:8px;height:8px;box-shadow:0 0 0 3px rgba(18,183,106,.12);}
        .ex-status-title{font-size:12px;}
        .ex-status-sub{font-size:11px;}
        .ex-status-chips span{font-size:10px;padding:4px 7px;}
        .quick-save-panel{
          position:sticky;
          bottom:calc(74px + env(safe-area-inset-bottom,0px));
          z-index:50;
          margin:7px 0 6px;
          padding:7px;
          box-shadow:0 12px 28px rgba(16,24,40,.14);
          background:rgba(255,255,255,.96);
          backdrop-filter:blur(10px);
        }
        .row-selection-highlight{display:block;padding:0;border-top:none;}
        .row-selection-highlight>.label{display:block;margin:0 0 5px;font-size:11px;}
        .row-selection-highlight>.value{display:block;width:100%;}
        .info-box-picks-first{padding:0 4px;margin-top:0;}
        .selection-compare-wrap{grid-template-columns:minmax(0,1.12fr) minmax(0,.88fr)!important;gap:6px;}
        .selection-compare-col{padding:5px;border-radius:12px;}
        .selection-col-title{font-size:10px;margin-bottom:4px;line-height:1.2;}
        .quick-select-row{padding:4px;gap:4px;margin-bottom:5px;border-radius:12px;}
        .quick-select-btn{font-size:10px;padding:5px 6px;border-radius:10px;}
        .selection-section{padding:5px;border-radius:11px;}
        .selection-section-title{font-size:10px;margin-bottom:4px;}
        .selection-section-cover{display:none;}
        .selection-choice-body{font-size:11px;padding:5px 6px;border-radius:10px;}
        .selection-choice-core .selection-choice-body{padding:6px;}
        .selection-chip-grid{gap:4px;}
        .selection-compare-col-official .selection-view-chip:nth-child(n+3){display:none;}
        .bet-guide-box{margin-top:7px;padding:7px 8px;border-radius:12px;box-shadow:none;}
        .bet-guide-head{flex-direction:row;align-items:center;gap:8px;}
        .bet-guide-title-wrap{gap:6px;}
        .bet-guide-icon{display:none;}
        .bet-guide-kicker{display:none;}
        .bet-guide-title{font-size:13px;line-height:1.25;}
        .bet-guide-recommend{width:auto;min-width:88px;padding:5px 8px;border-radius:999px;font-size:10px;}
        .bet-guide-detail,.quick-select-recommend{display:none;}
        .row-selected-compact{display:none;}
      }
      @media (max-width:390px){
        .selection-choice-body{font-size:10px;padding:5px 5px;}
        .quick-select-btn{font-size:9.5px;padding:5px 5px;}
        .selection-compare-wrap{grid-template-columns:minmax(0,1.15fr) minmax(0,.85fr)!important;}
      }



      /* v10.37: 結果/払戻/収支をカード内に必ず表示する */
      .info-box-picks-first .result-row-compact,
      .result-row-compact{
        display:grid !important;
      }
      .result-row-compact{
        grid-template-columns:110px 1fr !important;
        gap:10px !important;
        align-items:start !important;
      }
      .result-mini-grid{
        display:grid !important;
        grid-template-columns:1fr 1fr 1fr !important;
        gap:8px !important;
        width:100% !important;
      }
      @media (max-width:760px){
        .info-box-picks-first .result-row-compact,
        .result-row-compact{
          display:grid !important;
          grid-template-columns:52px 1fr !important;
          gap:6px !important;
          padding:7px 0 !important;
        }
        .result-mini-grid{
          display:grid !important;
          grid-template-columns:1fr !important;
          gap:6px !important;
        }
      }


      /* v10.47: 保存バーのはみ出し修正 + 公式買い目をAI6点横に全表示 */
      .quick-save-panel{
        width:100%;
        max-width:100%;
        overflow:hidden;
        box-sizing:border-box;
        grid-template-columns:96px minmax(0,1fr) minmax(88px,128px);
      }
      .quick-save-left,.quick-save-middle{min-width:0;}
      .quick-save-left select{width:100%;max-width:100%;}
      .quick-save-count,.quick-save-rule{min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
      .save-btn-compact{width:100%;max-width:100%;white-space:nowrap;overflow:hidden;text-overflow:clip;}
      .selection-compare-col-official .selection-view-chip:nth-child(n+3){display:inline-block!important;}
      .selection-compare-col-official .selection-chip-grid{align-content:flex-start;}
      .selection-col-title-ai:before{content:"◎"!important;color:#b54708;font-weight:900;}
      .selection-col-title-official:before{content:"公"!important;display:inline-flex;align-items:center;justify-content:center;width:18px;height:18px;border-radius:999px;background:#eef4ff;color:#175cd3;font-size:11px;font-weight:900;}
      @media (max-width:760px){
        .quick-save-panel{
          grid-template-columns:76px minmax(0,1fr) 78px!important;
          gap:5px!important;
          padding:6px!important;
        }
        .quick-save-left label{font-size:10px;margin-bottom:2px;}
        .stake-select-compact{min-height:34px!important;font-size:13px!important;padding:5px 18px 5px 7px!important;}
        .quick-save-count{font-size:12px!important;}
        .quick-save-rule{font-size:9.5px!important;}
        .save-btn-compact{min-height:36px!important;font-size:13px!important;padding:7px 8px!important;border-radius:11px!important;}
        .selection-compare-wrap{grid-template-columns:minmax(0,1fr) minmax(0,1fr)!important;align-items:start;}
        .selection-compare-col-official .selection-chip-grid{display:flex!important;gap:4px!important;}
        .selection-compare-col-official .selection-choice-body{font-size:10px!important;padding:5px 5px!important;}
      }
      @media (max-width:390px){
        .quick-save-panel{grid-template-columns:70px minmax(0,1fr) 74px!important;gap:4px!important;}
        .save-btn-compact{font-size:12px!important;padding:7px 6px!important;}
        .quick-save-count{font-size:11px!important;}
        .quick-save-rule{font-size:9px!important;}
      }
</style>
    """

    js = """
    <script>
      function getCardRootByRaceId(raceId){
        return document.querySelector('[data-race-card-id="' + raceId + '"]') || document.querySelector('[data-race-id="' + raceId + '"]');
      }
      function parseSelectionText(text){
        return String(text || '').split(' / ').map(x => String(x || '').replace(/\s+/g, '').trim()).filter(Boolean);
      }
      function getRaceCheckboxes(raceId){
        return Array.from(document.querySelectorAll('input[type="checkbox"][data-pick-value][data-race-group="' + raceId + '"]'));
      }
      function getCheckedValues(raceId){
        return getRaceCheckboxes(raceId).filter(x => x.checked).map(x => (x.getAttribute('data-pick-value') || '').trim()).filter(Boolean);
      }
      function renderColoredPickHtml(value){
        const s = String(value || '').replace(/\s+/g, '').trim();
        if(!s){ return ''; }
        return '<span class="pick-inline">' + s.split('-').map((part, idx) => {
          const sep = idx > 0 ? '<span class="pick-sep">-</span>' : '';
          if(/^\d+$/.test(part)){
            return sep + '<span class="lane-color lane-color-' + part + '">' + part + '</span>';
          }
          return sep + '<span class="pick-plain">' + part + '</span>';
        }).join('') + '</span>';
      }
      function setCheckedValuesFromHidden(raceId){
        const hidden = document.getElementById('selected-hidden-' + raceId);
        if(!hidden){ return []; }
        const values = parseSelectionText(hidden.value);
        const valueSet = new Set(values);
        getRaceCheckboxes(raceId).forEach(el => {
          const pick = (el.getAttribute('data-pick-value') || '').trim();
          el.checked = valueSet.has(pick);
        });
        return values;
      }
      function syncSelectionValue(el, raceId){
        const hidden = document.getElementById('selected-hidden-' + raceId);
        if(!hidden){ return true; }
        const values = getCheckedValues(raceId);
        hidden.value = values.join(' / ');
        return true;
      }
      function getAmountPerPoint(raceId){
        const select = document.getElementById('amount-select-' + raceId);
        if(select){
          const v = parseInt(select.value || '100', 10);
          return isNaN(v) ? 100 : v;
        }
        const formEl = document.querySelector('form[data-race-id="' + raceId + '"]');
        const amount = parseInt((formEl && formEl.getAttribute('data-amount')) || '100', 10);
        return isNaN(amount) ? 100 : amount;
      }
      function updateSelectionSummary(raceId, preserveHiddenWhenEmpty=true){
        const root = getCardRootByRaceId(raceId);
        if(!root){ return; }
        const summaryEl = document.getElementById('selected-summary-' + raceId);
        const countEl = document.getElementById('selected-count-badge-' + raceId);
        const totalEl = document.getElementById('selected-total-badge-' + raceId);
        const countInlineEl = document.getElementById('selected-count-inline-' + raceId);
        const totalInlineEl = document.getElementById('selected-total-inline-' + raceId);
        const amountBadgeEl = document.getElementById('amount-per-point-badge-' + raceId);
        const amountInlineEl = document.getElementById('amount-inline-' + raceId);
        const formEl = document.querySelector('form[data-race-id="' + raceId + '"]');
        const amount = getAmountPerPoint(raceId);
        if(formEl){ formEl.setAttribute('data-amount', String(amount)); }
        const hidden = document.getElementById('selected-hidden-' + raceId);
        let values = getCheckedValues(raceId);
        if(values.length === 0 && hidden && preserveHiddenWhenEmpty){
          values = setCheckedValuesFromHidden(raceId);
        }
        if(summaryEl){
          if(values.length === 0){
            summaryEl.innerHTML = '<div class="selection-chip-empty">未選択</div>';
          }else{
            summaryEl.innerHTML = '<div class="picked-chip-wrap">' + values.map(v => '<div class="picked-chip">' + renderColoredPickHtml(v) + '</div>').join('') + '</div>';
          }
        }
        if(countEl){ countEl.textContent = values.length + '点'; }
        if(totalEl){ totalEl.textContent = (amount * values.length).toLocaleString('ja-JP') + '円'; }
        if(amountBadgeEl){ amountBadgeEl.textContent = amount.toLocaleString('ja-JP') + '円'; }
        if(amountInlineEl){ amountInlineEl.textContent = amount.toLocaleString('ja-JP') + '円'; }
        if(hidden && (values.length > 0 || !preserveHiddenWhenEmpty)){ hidden.value = values.join(' / '); }
      }
      function selectTopPicks(raceId, count){
        const boxes = getRaceCheckboxes(raceId);
        boxes.forEach((el, idx) => { el.checked = idx < count; });
        syncSelectionValue(null, raceId);
        updateSelectionSummary(raceId, false);
      }
      function clearPickSelection(raceId){
        getRaceCheckboxes(raceId).forEach(el => { el.checked = false; });
        const hidden = document.getElementById('selected-hidden-' + raceId);
        if(hidden){ hidden.value = ''; }
        updateSelectionSummary(raceId, false);
      }
      function setAmountPerPoint(raceId, amount){
        const select = document.getElementById('amount-select-' + raceId);
        if(select){ select.value = String(amount || 100); }
        const formEl = document.querySelector('form[data-race-id="' + raceId + '"]');
        if(formEl){ formEl.setAttribute('data-amount', String(amount || 100)); }
      }
      function applyRecommendedBet(raceId, count, amount){
        setAmountPerPoint(raceId, amount || 100);
        if(parseInt(count || 0, 10) <= 0){
          clearPickSelection(raceId);
          return;
        }
        selectTopPicks(raceId, parseInt(count, 10));
        updateSelectionSummary(raceId, false);
      }
      function updateAmountPerPoint(raceId){
        updateSelectionSummary(raceId, false);
      }
      function toggleFormState(raceId){ return true; }
      function updateBulkDeleteCount(){
        const count = document.querySelectorAll('.bulk-checkbox:checked').length;
        const el = document.getElementById('bulk-delete-count');
        if(el){ el.textContent = count + '件選択中'; }
      }
      function toggleAllBulk(checked){
        document.querySelectorAll('.bulk-checkbox').forEach(el => { el.checked = checked; });
        updateBulkDeleteCount();
      }
      function confirmBulkDelete(){
        const count = document.querySelectorAll('.bulk-checkbox:checked').length;
        if(count <= 0){ alert('削除するデータを選んでください'); return false; }
        return confirm(count + '件を削除しますか？');
      }
      document.addEventListener('DOMContentLoaded', function(){
        document.querySelectorAll('form[data-race-id]').forEach(form => {
          const raceId = form.getAttribute('data-race-id');
          setCheckedValuesFromHidden(raceId);
          updateSelectionSummary(raceId, true);
          form.addEventListener('submit', function(){ syncSelectionValue(null, raceId); });
        });
        updateBulkDeleteCount();
      });
    </script>
    """

    bottom_nav_html = f'''
    <nav class="bottom-nav">
      <a href="/" class="bottom-nav-item {home_active}"><span class="bottom-nav-icon">🏁</span><span class="bottom-nav-label">候補</span></a>
      <a href="/stats" class="bottom-nav-item {stats_active}"><span class="bottom-nav-icon">📊</span><span class="bottom-nav-label">集計</span></a>
      <a href="/history" class="bottom-nav-item {history_active}"><span class="bottom-nav-icon">🗂️</span><span class="bottom-nav-label">過去</span></a>
    </nav>
    '''
    return """<!doctype html><html lang=\"ja\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1, viewport-fit=cover\"><title>{}</title>{}</head><body><div class=\"container\">{}</div>{}{}{}</body></html>""".format(title, css, body_html, bottom_nav_html, js, "")


def is_valid_import_token(req):
    sent = req.headers.get("X-IMPORT-TOKEN", "").strip()
    return bool(IMPORT_TOKEN) and sent == IMPORT_TOKEN


def is_valid_read_token(req):
    sent = req.headers.get("X-IMPORT-TOKEN", "").strip()
    return bool(IMPORT_TOKEN) and sent == IMPORT_TOKEN


_db_initialized = False


def ensure_db_initialized():
    global _db_initialized
    if _db_initialized:
        return
    init_db()


def init_db():
    global _db_initialized

    conn = db_connect()
    cur = conn.cursor()

    cur.execute(
        '''
        CREATE TABLE IF NOT EXISTS races (
            id SERIAL PRIMARY KEY,
            race_date TEXT NOT NULL,
            time TEXT NOT NULL,
            venue TEXT NOT NULL,
            race_no TEXT NOT NULL,
            race_no_num INTEGER NOT NULL DEFAULT 0,
            candidate_source TEXT NOT NULL DEFAULT 'official_star',
            rating TEXT NOT NULL DEFAULT '',
            bet_type TEXT NOT NULL DEFAULT '',
            selection TEXT NOT NULL DEFAULT '',
            amount INTEGER NOT NULL DEFAULT 100,

            ai_score DOUBLE PRECISION NOT NULL DEFAULT 0,
            ai_rating TEXT NOT NULL DEFAULT '',
            ai_label TEXT NOT NULL DEFAULT '',
            final_rank TEXT NOT NULL DEFAULT '',
            ai_reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
            exhibition JSONB NOT NULL DEFAULT '[]'::jsonb,
            exhibition_rank TEXT NOT NULL DEFAULT '',
            weather TEXT NOT NULL DEFAULT '',
            wind_speed DOUBLE PRECISION NOT NULL DEFAULT 0,
            wave_height DOUBLE PRECISION NOT NULL DEFAULT 0,
            wind_type TEXT NOT NULL DEFAULT '',
            wind_dir TEXT NOT NULL DEFAULT '',
            water_state_score DOUBLE PRECISION NOT NULL DEFAULT 0,
            day_trend_text TEXT NOT NULL DEFAULT '',
            day_trend_sample INTEGER NOT NULL DEFAULT 0,
            series_day INTEGER NOT NULL DEFAULT 0,
            race_phase TEXT NOT NULL DEFAULT '',
            motor_rank TEXT NOT NULL DEFAULT '',
            ai_detail TEXT NOT NULL DEFAULT '',
            ai_selection TEXT NOT NULL DEFAULT '',
            ai_confidence TEXT NOT NULL DEFAULT '',
            ai_lane_score_text TEXT NOT NULL DEFAULT '',
            class_history_text TEXT NOT NULL DEFAULT '',
            player_names_text TEXT NOT NULL DEFAULT '',
            player_stat_text TEXT NOT NULL DEFAULT '',
            player_reason_text TEXT NOT NULL DEFAULT '',

            base_ai_score DOUBLE PRECISION NOT NULL DEFAULT 0,
            base_ai_rating TEXT NOT NULL DEFAULT '',
            base_ai_selection TEXT NOT NULL DEFAULT '',
            base_reason_text TEXT NOT NULL DEFAULT '',
            base_updated_at TEXT NOT NULL DEFAULT '',

            final_ai_score DOUBLE PRECISION NOT NULL DEFAULT 0,
            final_ai_rating TEXT NOT NULL DEFAULT '',
            final_ai_selection TEXT NOT NULL DEFAULT '',
            latest_reason_text TEXT NOT NULL DEFAULT '',
            latest_updated_at TEXT NOT NULL DEFAULT '',

            purchased INTEGER NOT NULL DEFAULT 0,
            purchased_selection_text TEXT NOT NULL DEFAULT '',
            hit INTEGER NOT NULL DEFAULT 0,
            payout INTEGER NOT NULL DEFAULT 0,
            memo TEXT NOT NULL DEFAULT '',
            result_trifecta_text TEXT NOT NULL DEFAULT '',
            result_trifecta_payout INTEGER NOT NULL DEFAULT 0,
            result_exacta_text TEXT NOT NULL DEFAULT '',
            result_exacta_payout INTEGER NOT NULL DEFAULT 0,
            result_trio_text TEXT NOT NULL DEFAULT '',
            result_trio_payout INTEGER NOT NULL DEFAULT 0,
            settled_flag INTEGER NOT NULL DEFAULT 0,
            settled_at TEXT NOT NULL DEFAULT '',
            result_source_url TEXT NOT NULL DEFAULT '',
            imported_at TEXT NOT NULL DEFAULT ''
        )
        '''
    )

    alter_sqls = [
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS race_no_num INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS candidate_source TEXT NOT NULL DEFAULT 'official_star'",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS ai_score DOUBLE PRECISION NOT NULL DEFAULT 0",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS ai_rating TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS ai_label TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS final_rank TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS ai_reasons JSONB NOT NULL DEFAULT '[]'::jsonb",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS exhibition JSONB NOT NULL DEFAULT '[]'::jsonb",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS exhibition_rank TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS weather TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS wind_speed DOUBLE PRECISION NOT NULL DEFAULT 0",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS wave_height DOUBLE PRECISION NOT NULL DEFAULT 0",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS wind_type TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS wind_dir TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS water_state_score DOUBLE PRECISION NOT NULL DEFAULT 0",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS day_trend_text TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS day_trend_sample INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS series_day INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS race_phase TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS motor_rank TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS ai_detail TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS ai_selection TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS ai_confidence TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS ai_lane_score_text TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS class_history_text TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS player_names_text TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS player_stat_text TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS player_reason_text TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS base_ai_score DOUBLE PRECISION NOT NULL DEFAULT 0",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS base_ai_rating TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS base_ai_selection TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS base_reason_text TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS base_updated_at TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS final_ai_score DOUBLE PRECISION NOT NULL DEFAULT 0",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS final_ai_rating TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS final_ai_selection TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS latest_reason_text TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS latest_updated_at TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS purchased INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS purchased_selection_text TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS hit INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS payout INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS memo TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS result_trifecta_text TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS result_trifecta_payout INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS result_exacta_text TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS result_exacta_payout INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS result_trio_text TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS result_trio_payout INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS settled_flag INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS settled_at TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS result_source_url TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS imported_at TEXT NOT NULL DEFAULT ''",
    ]
    for sql in alter_sqls:
        cur.execute(sql)

    cur.execute("UPDATE races SET candidate_source = 'official_star' WHERE candidate_source IS NULL OR BTRIM(candidate_source) = ''")

    # 旧版では (race_date, venue, race_no, selection) だけでUNIQUE制約を作っていた。
    # 公式候補・裏AI候補・全レース検証候補は同じレース/同じ買い目でも candidate_source が違えば
    # 別データとして保存したいので、旧UNIQUE制約を解除する。
    # 解除しないと shadow_ai / all_race_ai の新規INSERT時に duplicate key で500になる。
    cur.execute("ALTER TABLE races DROP CONSTRAINT IF EXISTS races_race_date_venue_race_no_selection_key")
    cur.execute("DROP INDEX IF EXISTS races_race_date_venue_race_no_selection_key")

    cur.execute("CREATE INDEX IF NOT EXISTS idx_races_race_date ON races (race_date)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_races_race_key ON races (race_date, venue, race_no)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_races_today_view ON races (race_date, rating, time, venue, race_no_num)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_races_source_view ON races (race_date, candidate_source, time, venue, race_no_num)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_races_history_view ON races (race_date, venue, race_no_num, hit)")

    conn.commit()
    cur.close()
    conn.close()
    _db_initialized = True


def upsert_base_candidates(cleaned):
    ensure_db_initialized()
    if not cleaned:
        return {'inserted': 0, 'updated': 0}
    race_date = str(cleaned[0]['race_date']).strip()
    existing_map = get_existing_race_map_by_date(race_date)
    conn = db_connect()
    cur = conn.cursor()
    inserted = 0
    updated = 0

    def as_int(value, default=0):
        try:
            if value is None or str(value).strip() == '':
                return int(default)
            return int(float(value))
        except Exception:
            return int(default)

    for r in cleaned:
        candidate_source_value = normalize_candidate_source(r.get('candidate_source'))
        key = make_race_key(r.get('race_date'), r.get('venue'), r.get('race_no'), candidate_source_value)
        existing = existing_map.get(key)

        common_values = {
            'race_date': str(r.get('race_date') or '').strip(),
            'time': str(r.get('time') or '').strip(),
            'venue': str(r.get('venue') or '').strip(),
            'race_no': str(r.get('race_no') or '').strip(),
            'race_no_num': as_int(r.get('race_no_num'), 0),
            'candidate_source': candidate_source_value,
            'rating': str(r.get('rating') or '').strip(),
            'bet_type': str(r.get('bet_type') or '').strip(),
            'selection': str(r.get('selection') or '').strip(),
            'amount': as_int(r.get('amount'), 100),
            'player_names_text': str(r.get('player_names_text') or '').strip(),
            'class_history_text': str(r.get('class_history_text') or '').strip(),
            'player_stat_text': str(r.get('player_stat_text') or '').strip(),
            'player_reason_text': str(r.get('player_reason_text') or '').strip(),
            'day_trend_text': str(r.get('day_trend_text') or '').strip(),
            'day_trend_sample': as_int(r.get('day_trend_sample'), 0),
            'series_day': as_int(r.get('series_day'), 0),
            'race_phase': str(r.get('race_phase') or '').strip(),
            'base_ai_score': safe_float(r.get('base_ai_score', 0), 0),
            'base_ai_rating': str(r.get('base_ai_rating') or '').strip(),
            'base_ai_selection': str(r.get('base_ai_selection') or '').strip(),
            'base_reason_text': str(r.get('base_reason_text') or '').strip(),
            'base_updated_at': str(r.get('base_updated_at') or '').strip(),
        }

        if existing:
            cur.execute(
                '''
                UPDATE races
                SET
                    time = CASE WHEN COALESCE(%s, '') <> '' THEN %s ELSE time END,
                    race_no_num = %s,
                    candidate_source = %s,
                    rating = %s,
                    bet_type = %s,
                    selection = %s,
                    amount = CASE WHEN COALESCE(purchased, 0) = 1 THEN amount ELSE %s END,
                    player_names_text = CASE WHEN COALESCE(%s, '') <> '' THEN %s ELSE player_names_text END,
                    class_history_text = CASE WHEN COALESCE(%s, '') <> '' THEN %s ELSE class_history_text END,
                    player_stat_text = CASE WHEN COALESCE(%s, '') <> '' THEN %s ELSE player_stat_text END,
                    player_reason_text = CASE WHEN COALESCE(%s, '') <> '' THEN %s ELSE player_reason_text END,
                    day_trend_text = CASE WHEN COALESCE(%s, '') <> '' THEN %s ELSE day_trend_text END,
                    day_trend_sample = CASE WHEN %s > 0 THEN %s ELSE day_trend_sample END,
                    series_day = CASE WHEN %s > 0 THEN %s ELSE series_day END,
                    race_phase = CASE WHEN COALESCE(%s, '') <> '' THEN %s ELSE race_phase END,
                    base_ai_score = %s,
                    base_ai_rating = %s,
                    base_ai_selection = %s,
                    base_reason_text = %s,
                    base_updated_at = %s,
                    imported_at = %s
                WHERE id = %s
                ''',
                (
                    common_values['time'], common_values['time'],
                    common_values['race_no_num'],
                    common_values['candidate_source'],
                    common_values['rating'], common_values['bet_type'], common_values['selection'], common_values['amount'],
                    common_values['player_names_text'], common_values['player_names_text'],
                    common_values['class_history_text'], common_values['class_history_text'],
                    common_values['player_stat_text'], common_values['player_stat_text'],
                    common_values['player_reason_text'], common_values['player_reason_text'],
                    common_values['day_trend_text'], common_values['day_trend_text'],
                    common_values['day_trend_sample'], common_values['day_trend_sample'],
                    common_values['series_day'], common_values['series_day'],
                    common_values['race_phase'], common_values['race_phase'],
                    common_values['base_ai_score'], common_values['base_ai_rating'], common_values['base_ai_selection'],
                    common_values['base_reason_text'], common_values['base_updated_at'],
                    jst_now_str(), existing['id'],
                )
            )
            updated += 1
        else:
            cur.execute(
                '''
                INSERT INTO races (
                    race_date, time, venue, race_no, race_no_num, candidate_source,
                    rating, bet_type, selection, amount,
                    player_names_text, class_history_text, player_stat_text, player_reason_text,
                    day_trend_text, day_trend_sample, series_day, race_phase,
                    base_ai_score, base_ai_rating, base_ai_selection, base_reason_text, base_updated_at,
                    purchased, purchased_selection_text, hit, payout, memo, imported_at
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s
                )
                ''',
                (
                    common_values['race_date'], common_values['time'], common_values['venue'], common_values['race_no'],
                    common_values['race_no_num'], common_values['candidate_source'],
                    common_values['rating'], common_values['bet_type'], common_values['selection'], common_values['amount'],
                    common_values['player_names_text'], common_values['class_history_text'], common_values['player_stat_text'], common_values['player_reason_text'],
                    common_values['day_trend_text'], common_values['day_trend_sample'], common_values['series_day'], common_values['race_phase'],
                    common_values['base_ai_score'], common_values['base_ai_rating'], common_values['base_ai_selection'],
                    common_values['base_reason_text'], common_values['base_updated_at'],
                    0, '', 0, 0, '', jst_now_str(),
                )
            )
            inserted += 1

    conn.commit()
    cur.close()
    conn.close()
    log(f'upsert_base_candidates inserted={inserted} updated={updated}')
    return {'inserted': inserted, 'updated': updated}


def upsert_latest_candidates(cleaned):
    ensure_db_initialized()
    if not cleaned:
        return {'updated': 0, 'skipped': 0}
    race_date = str(cleaned[0]['race_date']).strip()
    existing_map = get_existing_race_map_by_date(race_date)
    conn = db_connect()
    cur = conn.cursor()
    updated = 0
    skipped = 0
    for r in cleaned:
        candidate_source_value = normalize_candidate_source(r.get('candidate_source'))
        key = make_race_key(r.get('race_date'), r.get('venue'), r.get('race_no'), candidate_source_value)
        existing = existing_map.get(key)
        if not existing:
            skipped += 1
            continue

        incoming_time = str(r.get('time') or '').strip()
        current_time = str(existing.get('time') or '').strip()
        effective_time = incoming_time or current_time

        freeze_after_close = False
        if effective_time:
            freeze_after_close = not is_not_started(effective_time)

        final_ai_score_value = safe_float(r.get('final_ai_score', 0), 0)
        final_ai_rating_value = str(r.get('final_ai_rating') or '').strip()
        final_ai_selection_value = str(r.get('final_ai_selection') or '').strip()
        final_rank_value = str(r.get('final_rank') or '').strip()
        latest_reason_text_value = str(r.get('latest_reason_text') or '').strip()
        latest_updated_at_value = str(r.get('latest_updated_at') or '').strip()

        if freeze_after_close and str(existing.get('final_ai_selection') or '').strip():
            final_ai_score_value = safe_float(existing.get('final_ai_score', 0), 0)
            final_ai_rating_value = str(existing.get('final_ai_rating') or '').strip()
            final_ai_selection_value = str(existing.get('final_ai_selection') or '').strip()
            final_rank_value = str(existing.get('final_rank') or '').strip()
            latest_reason_text_value = str(existing.get('latest_reason_text') or '').strip()
            latest_updated_at_value = str(existing.get('latest_updated_at') or '').strip()

        cur.execute(
            '''
            UPDATE races
            SET
                time = CASE WHEN COALESCE(%s, '') <> '' THEN %s ELSE time END,
                exhibition = %s,
                exhibition_rank = %s,
                weather = %s,
                wind_speed = %s,
                wave_height = %s,
                wind_type = %s,
                wind_dir = %s,
                water_state_score = %s,
                ai_lane_score_text = %s,
                player_stat_text = CASE WHEN COALESCE(%s, '') <> '' THEN %s ELSE player_stat_text END,
                player_reason_text = CASE WHEN COALESCE(%s, '') <> '' THEN %s ELSE player_reason_text END,
                final_ai_score = %s,
                final_ai_rating = %s,
                final_ai_selection = %s,
                final_rank = %s,
                latest_reason_text = %s,
                latest_updated_at = %s,
                result_trifecta_text = CASE
                    WHEN COALESCE(%s, '') <> '' THEN %s
                    ELSE result_trifecta_text
                END,
                result_trifecta_payout = CASE
                    WHEN COALESCE(%s, 0) > 0 THEN %s
                    ELSE result_trifecta_payout
                END,
                result_source_url = CASE
                    WHEN COALESCE(%s, '') <> '' THEN %s
                    ELSE result_source_url
                END,
                settled_flag = CASE
                    WHEN COALESCE(%s, '') <> '' THEN 1
                    ELSE settled_flag
                END,
                settled_at = CASE
                    WHEN COALESCE(%s, '') <> '' THEN %s
                    ELSE settled_at
                END,
                imported_at = %s
            WHERE id = %s
            ''',
            (
                incoming_time,
                incoming_time,
                json.dumps(r.get('exhibition', []), ensure_ascii=False),
                str(r.get('exhibition_rank') or '').strip(),
                str(r.get('weather') or '').strip(),
                safe_float(r.get('wind_speed'), 0),
                safe_float(r.get('wave_height'), 0),
                str(r.get('wind_type') or '').strip(),
                str(r.get('wind_dir') or '').strip(),
                safe_float(r.get('water_state_score'), 0),
                str(r.get('ai_lane_score_text') or '').strip(),
                str(r.get('player_stat_text') or '').strip(),
                str(r.get('player_stat_text') or '').strip(),
                str(r.get('player_reason_text') or '').strip(),
                str(r.get('player_reason_text') or '').strip(),
                final_ai_score_value,
                final_ai_rating_value,
                final_ai_selection_value,
                final_rank_value,
                latest_reason_text_value,
                latest_updated_at_value,
                str(r.get('result_trifecta_text') or '').strip(),
                str(r.get('result_trifecta_text') or '').strip(),
                int(r.get('result_trifecta_payout') or 0),
                int(r.get('result_trifecta_payout') or 0),
                str(r.get('result_source_url') or '').strip(),
                str(r.get('result_source_url') or '').strip(),
                str(r.get('result_trifecta_text') or '').strip(),
                str(r.get('result_trifecta_text') or '').strip(),
                jst_now_str(),
                jst_now_str(),
                existing['id'],
            )
        )
        updated += 1

    conn.commit()
    cur.close()
    conn.close()
    log(f'upsert_latest_candidates updated={updated} skipped={skipped}')
    return {'updated': updated, 'skipped': skipped}


@app.route("/healthz")
def healthz():
    return "ok", 200


@app.route("/")
def index():
    show_closed = request.args.get("show_closed", "").strip() == "1"
    show_shadow = True  # 裏AI候補は常時表示
    show_all_race = request.args.get("show_all_race", "").strip() == "1"
    ai_rating_filter = request.args.get("ai_rating", "").strip()
    official_rating_filter = request.args.get("official_rating", "pickup").strip() or "pickup"
    if ai_rating_filter not in AI_RATING_OPTIONS:
        ai_rating_filter = ""
    official_rating_values = {value for value, _label in OFFICIAL_RATING_FILTER_OPTIONS}
    if official_rating_filter not in official_rating_values:
        official_rating_filter = "pickup"
    races = get_filtered_today_races(
        show_closed=show_closed,
        ai_rating_filter=ai_rating_filter,
        official_rating_filter=official_rating_filter,
        show_shadow=show_shadow,
        show_all_race=show_all_race,
    )
    summary = get_summary_by_date(today_text())
    return render_home(
        races,
        summary,
        request.args.get("type", "").strip(),
        request.args.get("msg", "").strip(),
        show_closed=show_closed,
        ai_rating_filter=ai_rating_filter,
        official_rating_filter=official_rating_filter,
        show_shadow=show_shadow,
        show_all_race=show_all_race,
    )


def parse_selected_from_request():
    raw_selected_text = request.form.get("selected_text", "")
    selected_items = [normalize_pick_text(x) for x in selection_items(raw_selected_text)]
    selected_items = [x for x in selected_items if x]
    if selected_items:
        return " / ".join(unique_preserve(selected_items))
    ai = [normalize_pick_text(x) for x in request.form.getlist("selected_ai")]
    ai = [x for x in ai if x]
    return " / ".join(unique_preserve(ai))


@app.route("/save", methods=["POST"])
def save():
    race_id = int(request.form.get("race_id", "0"))
    race = get_race_by_id(race_id)
    if not race:
        return redirect("/?type=error&msg=" + quote("データが見つかりません"))
    selected_text = parse_selected_from_request()
    amount_per_point = normalize_amount_per_point(request.form.get("amount_per_point"), 100)
    hit = 0
    payout = 0
    memo = ""
    update_race_result(race_id, selected_text, hit, payout, memo, amount_per_point=amount_per_point)
    redirect_params = []
    if not is_not_started(race["time"]):
        redirect_params.append("show_closed=1")
    source_for_redirect = normalize_candidate_source(race.get("candidate_source"))
    if source_for_redirect == "shadow_ai":
        redirect_params.append("show_shadow=1")
    elif source_for_redirect == "all_race_ai":
        redirect_params.append("show_all_race=1")
    redirect_base = "/" + (("?" + "&".join(redirect_params)) if redirect_params else "")
    sep = "&" if "?" in redirect_base else "?"
    # 保存後にページ最上部へ戻らないよう、保存したカード位置へ戻す。
    # URLフラグメントはクエリ文字列の後ろに付ける必要がある。
    return redirect(
        redirect_base
        + sep
        + "type=success&msg="
        + quote("保存しました")
        + f"#race-card-{race_id}"
    )


@app.route("/update_record", methods=["POST"])
def update_record():
    race_id = int(request.form.get("race_id", "0"))
    redirect_to = safe_redirect_path(request.form.get("redirect_to", "/history"), "/history")
    race = get_race_by_id(race_id)
    if not race:
        return redirect(redirect_to + ("&" if "?" in redirect_to else "?") + "type=error&msg=" + quote("データが見つかりません"))
    selected_text = parse_selected_from_request()
    amount_per_point = normalize_amount_per_point(request.form.get("amount_per_point"), 100)
    hit = 0
    payout = 0
    memo = ""
    update_race_result(race_id, selected_text, hit, payout, memo, amount_per_point=amount_per_point)
    return redirect(
        redirect_to
        + ("&" if "?" in redirect_to else "?")
        + "type=success&msg="
        + quote("過去データを保存しました")
        + f"#race-card-{race_id}"
    )


@app.route("/delete_record", methods=["POST"])
def delete_record():
    race_id = int(request.form.get("race_id", "0"))
    redirect_to = safe_redirect_path(request.form.get("redirect_to", "/history"), "/history")
    race = get_race_by_id(race_id)
    if not race:
        return redirect(redirect_to + ("&" if "?" in redirect_to else "?") + "type=error&msg=" + quote("削除対象が見つかりません"))
    delete_race(race_id)
    return redirect(redirect_to + ("&" if "?" in redirect_to else "?") + "type=success&msg=" + quote("削除しました"))


@app.route("/delete_records_bulk", methods=["POST"])
def delete_records_bulk():
    redirect_to = safe_redirect_path(request.form.get("redirect_to", "/history"), "/history")
    deleted = delete_races_bulk(request.form.getlist("race_ids"))
    if deleted <= 0:
        return redirect(redirect_to + ("&" if "?" in redirect_to else "?") + "type=error&msg=" + quote("削除するデータを選んでください"))
    return redirect(redirect_to + ("&" if "?" in redirect_to else "?") + "type=success&msg=" + quote(f"{deleted}件削除しました"))


@app.route("/stats")
def stats():
    race_date = today_text()
    return render_stats_page(
        race_date,
        get_summary_by_date(race_date),
        get_group_summary(race_date, "rating"),
        get_group_summary(race_date, "venue"),
        get_group_summary(race_date, "ai_rating"),
        get_group_summary(race_date, "final_rank"),
        get_group_summary(race_date, "candidate_source"),
    )


@app.route("/history")
def history():
    return render_history_page(get_history_date_summaries())


@app.route("/history/<race_date>")
def history_detail(race_date):
    venue_filter = request.args.get("venue", "").strip()
    race_no_filter = request.args.get("race_no", "").strip()
    purchased_only = request.args.get("purchased_only", "").strip() == "1"
    hit_only = request.args.get("hit_only", "").strip() == "1"
    races = get_races_by_date(race_date)
    return render_history_detail_page(
        race_date,
        races,
        get_summary_by_date(race_date),
        request.args.get("type", "").strip(),
        request.args.get("msg", "").strip(),
        venue_filter=venue_filter,
        race_no_filter=race_no_filter,
        purchased_only=purchased_only,
        hit_only=hit_only,
    )


@app.route("/api/import_base_candidates", methods=["POST"])
def import_base_candidates():
    if not is_valid_import_token(request):
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    data = request.get_json(silent=True) or {}
    races = data.get("races", [])
    if not isinstance(races, list):
        return jsonify({"ok": False, "error": "races must be a list"}), 400
    required_keys = {"race_date", "venue", "race_no", "race_no_num", "rating", "bet_type", "selection", "amount"}
    cleaned = []
    for i, r in enumerate(races):
        if not isinstance(r, dict):
            return jsonify({"ok": False, "error": f"row {i} is not dict"}), 400
        missing = required_keys - set(r.keys())
        if missing:
            return jsonify({"ok": False, "error": f"row {i} missing keys: {sorted(list(missing))}"}), 400
        cleaned.append(
            {
                "race_date": str(r.get("race_date") or "").strip(),
                "time": str(r.get("time") or "").strip(),
                "venue": str(r.get("venue") or "").strip(),
                "race_no": str(r.get("race_no") or "").strip(),
                "race_no_num": int(r.get("race_no_num") or 0),
                "candidate_source": normalize_candidate_source(r.get("candidate_source")),
                "rating": str(r.get("rating") or "").strip(),
                "bet_type": str(r.get("bet_type") or "").strip(),
                "selection": str(r.get("selection") or "").strip(),
                "amount": int(r.get("amount") or 100),
                "player_names_text": str(r.get("player_names_text") or "").strip(),
                "class_history_text": str(r.get("class_history_text") or "").strip(),
                "player_stat_text": str(r.get("player_stat_text") or "").strip(),
                "player_reason_text": str(r.get("player_reason_text") or "").strip(),
                "day_trend_text": str(r.get("day_trend_text") or "").strip(),
                "day_trend_sample": int(r.get("day_trend_sample") or 0),
                "series_day": int(r.get("series_day") or 0),
                "race_phase": str(r.get("race_phase") or "").strip(),
                "base_ai_score": safe_float(r.get("base_ai_score", 0), 0),
                "base_ai_rating": str(r.get("base_ai_rating") or "").strip(),
                "base_ai_selection": str(r.get("base_ai_selection") or "").strip(),
                "base_reason_text": str(r.get("base_reason_text") or "").strip(),
                "base_updated_at": str(r.get("base_updated_at") or "").strip(),
            }
        )
    if not cleaned:
        return jsonify({"ok": False, "error": "races is empty"}), 400
    race_dates = sorted(set(r["race_date"] for r in cleaned))
    if len(race_dates) != 1:
        return jsonify({"ok": False, "error": "multiple race_date values are not allowed"}), 400
    try:
        result = upsert_base_candidates(cleaned)
    except Exception as e:
        log(f"[import_base_error] {type(e).__name__}: {e}")
        return jsonify({"ok": False, "error": str(e), "error_type": type(e).__name__, "received": len(cleaned)}), 500
    return jsonify({"ok": True, "received": len(cleaned), "inserted": result["inserted"], "updated": result["updated"], "imported_at": jst_now_str()})


@app.route("/api/base_map_today", methods=["GET"])
def api_base_map_today():
    if not is_valid_read_token(request):
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    race_date = request.args.get("race_date", "").strip() or today_text()
    ensure_db_initialized()
    conn = db_connect()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        '''
        SELECT
            race_date,
            venue,
            race_no,
            candidate_source,
            rating,
            selection,
            series_day,
            race_phase,
            base_ai_score,
            base_ai_rating,
            base_ai_selection,
            base_reason_text,
            final_ai_score,
            final_ai_rating,
            final_ai_selection,
            latest_reason_text
        FROM races
        WHERE race_date = %s
          AND venue <> 'テスト会場'
        ''',
        (race_date,),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    base_map = {}
    for row in rows:
        venue = str(row['venue']).strip()
        race_no = str(row['race_no']).strip()
        source = normalize_candidate_source(row.get("candidate_source"))
        item = {
            "candidate_source": source,
            "rating": str(row.get("rating") or "").strip(),
            "selection": str(row.get("selection") or "").strip(),
            "official_selection": str(row.get("selection") or "").strip(),
            "series_day": int(row.get("series_day") or 0),
            "race_phase": str(row.get("race_phase") or "").strip(),
            "base_ai_score": safe_float(row.get("base_ai_score"), 0),
            "base_ai_rating": str(row.get("base_ai_rating") or "").strip(),
            "base_ai_selection": str(row.get("base_ai_selection") or "").strip(),
            "base_reason_text": str(row.get("base_reason_text") or "").strip(),
            "final_ai_score": safe_float(row.get("final_ai_score"), 0),
            "final_ai_rating": str(row.get("final_ai_rating") or "").strip(),
            "final_ai_selection": str(row.get("final_ai_selection") or "").strip(),
            "latest_reason_text": str(row.get("latest_reason_text") or "").strip(),
        }
        key = f"{venue}|{race_no}|{source}"
        base_map[key] = item
        # 既存の collector_latest.py 互換用。公式候補だけ従来キーも返す。
        if source in {"official_star", "official_all"}:
            legacy_key = f"{venue}|{race_no}"
            base_map[legacy_key] = item
    return jsonify({"ok": True, "race_date": race_date, "count": len(base_map), "base_map": base_map})


@app.route("/api/import_latest_candidates", methods=["POST"])
def import_latest_candidates():
    if not is_valid_import_token(request):
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    data = request.get_json(silent=True) or {}
    races = data.get("races", [])
    if not isinstance(races, list):
        return jsonify({"ok": False, "error": "races must be a list"}), 400
    required_keys = {"race_date", "venue", "race_no"}
    cleaned = []
    for i, r in enumerate(races):
        if not isinstance(r, dict):
            return jsonify({"ok": False, "error": f"row {i} is not dict"}), 400
        missing = required_keys - set(r.keys())
        if missing:
            return jsonify({"ok": False, "error": f"row {i} missing keys: {sorted(list(missing))}"}), 400
        cleaned.append(
            {
                "race_date": str(r.get("race_date") or "").strip(),
                "venue": str(r.get("venue") or "").strip(),
                "race_no": str(r.get("race_no") or "").strip(),
                "candidate_source": normalize_candidate_source(r.get("candidate_source")),
                "time": str(r.get("time") or "").strip(),
                "exhibition": r.get("exhibition", []),
                "exhibition_rank": str(r.get("exhibition_rank") or "").strip(),
                "weather": str(r.get("weather") or "").strip(),
                "wind_speed": safe_float(r.get("wind_speed"), 0),
                "wave_height": safe_float(r.get("wave_height"), 0),
                "wind_type": str(r.get("wind_type") or "").strip(),
                "wind_dir": str(r.get("wind_dir") or "").strip(),
                "water_state_score": safe_float(r.get("water_state_score"), 0),
                "ai_lane_score_text": str(r.get("ai_lane_score_text") or "").strip(),
                "player_stat_text": str(r.get("player_stat_text") or "").strip(),
                "player_reason_text": str(r.get("player_reason_text") or "").strip(),
                "final_ai_score": safe_float(r.get("final_ai_score", 0), 0),
                "final_ai_rating": str(r.get("final_ai_rating") or "").strip(),
                "final_ai_selection": str(r.get("final_ai_selection") or "").strip(),
                "final_rank": str(r.get("final_rank") or "").strip(),
                "latest_reason_text": str(r.get("latest_reason_text") or "").strip(),
                "latest_updated_at": str(r.get("latest_updated_at") or "").strip(),
                "result_trifecta_text": str(r.get("result_trifecta_text") or "").strip(),
                "result_trifecta_payout": int(r.get("result_trifecta_payout") or 0),
                "result_source_url": str(r.get("result_source_url") or "").strip(),
            }
        )
    if not cleaned:
        return jsonify({"ok": False, "error": "races is empty"}), 400
    race_dates = sorted(set(r["race_date"] for r in cleaned))
    if len(race_dates) != 1:
        return jsonify({"ok": False, "error": "multiple race_date values are not allowed"}), 400
    result = upsert_latest_candidates(cleaned)
    return jsonify({"ok": True, "received": len(cleaned), "updated": result["updated"], "skipped": result["skipped"], "imported_at": jst_now_str()})


@app.route("/export/today.csv")
def export_today_csv():
    rows = get_races_by_date(today_text())
    return make_csv_response(rows, f"race_candidates_today_{today_text()}.csv")


@app.route("/export/history/<race_date>.csv")
def export_history_csv(race_date):
    rows = get_races_by_date(race_date)
    return make_csv_response(rows, f"race_candidates_{race_date}.csv")




init_db()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
