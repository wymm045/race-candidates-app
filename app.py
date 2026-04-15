from datetime import datetime, timezone, timedelta
import os
import json
from urllib.parse import quote

import psycopg2
import psycopg2.extras
from flask import Flask, request, redirect, jsonify

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


def is_star5_only(race):
    return str(race.get("rating", "")).strip() == "★★★★★"


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


REASON_TAG_DEFS = [
    ("前づけ警戒", lambda r, joined: "前づけ" in joined),
    ("チルト警戒", lambda r, joined: "チルト" in joined),
    ("まくり警戒", lambda r, joined: "まくり" in joined),
    ("展示反映", lambda r, joined: bool(parse_json_array_text(r.get("exhibition", "[]"))) or "展示" in joined),
    ("外攻め警戒", lambda r, joined: "外攻め" in joined),
    ("イン強め", lambda r, joined: "イン強め" in joined),
    ("差し警戒", lambda r, joined: "差し警戒" in joined),
    ("攻め注意", lambda r, joined: "まくり差し注意" in joined or "攻め" in joined),
]


def get_reason_tag_items(race):
    texts = [
        str(race.get("base_reason_text") or "").strip(),
        str(race.get("latest_reason_text") or "").strip(),
        str(race.get("ai_detail") or "").strip(),
    ]
    joined = " / ".join([x for x in texts if x])
    tags = []
    for label, checker in REASON_TAG_DEFS:
        try:
            if checker(race, joined):
                tags.append(label)
        except Exception:
            continue

    final_rank = str(race.get("final_rank") or "").strip()
    if final_rank == "買い強め":
        tags.append("強気判定")
    elif final_rank == "様子見":
        tags.append("様子見判定")
    elif final_rank == "見送り寄り":
        tags.append("見送り寄り")

    return unique_preserve(tags)


def reason_tag_class(tag):
    if tag in {"前づけ警戒", "外攻め警戒", "見送り寄り"}:
        return "reason-tag-chip reason-tag-warn"
    if tag in {"チルト警戒", "まくり警戒", "展示反映", "差し警戒", "攻め注意"}:
        return "reason-tag-chip reason-tag-info"
    if tag in {"イン強め", "強気判定"}:
        return "reason-tag-chip reason-tag-good"
    if tag == "様子見判定":
        return "reason-tag-chip reason-tag-neutral"
    return "reason-tag-chip"


def render_reason_tag_chips(race):
    tags = get_reason_tag_items(race)
    if not tags:
        return '<div class="detail-chip-empty">タグなし</div>'
    chips = "".join([f'<div class="{reason_tag_class(tag)}">{tag}</div>' for tag in tags])
    return f'<div class="detail-chip-wrap">{chips}</div>'


def summarize_races(rows):
    total_rows = len(rows)
    total_bets = 0
    total_points = 0
    total_hits = 0
    total_investment = 0
    total_payout = 0

    for row in rows:
        selected_count = get_selected_count_from_text(row.get("purchased_selection_text", ""))
        if selected_count <= 0:
            continue
        total_bets += 1
        total_points += selected_count
        total_hits += 1 if int(row.get("hit") or 0) == 1 else 0
        total_investment += int(row.get("amount") or 0) * selected_count
        total_payout += int(row.get("payout") or 0)

    total_profit = total_payout - total_investment
    hit_rate = round((total_hits / total_bets * 100), 1) if total_bets else 0
    roi = round((total_payout / total_investment * 100), 1) if total_investment else 0
    return {
        "total_rows": total_rows,
        "total_bets": total_bets,
        "total_points": total_points,
        "total_hits": total_hits,
        "total_investment": total_investment,
        "total_payout": total_payout,
        "total_profit": total_profit,
        "hit_rate": hit_rate,
        "roi": roi,
    }


def get_reason_tag_summary(rows):
    summary_map = {}
    for row in rows:
        for tag in get_reason_tag_items(row):
            summary_map.setdefault(tag, []).append(row)

    results = []
    for tag, tag_rows in summary_map.items():
        stats = summarize_races(tag_rows)
        results.append(
            {
                "group_name": tag,
                "total_bets": stats["total_bets"],
                "total_hits": stats["total_hits"],
                "total_points": stats["total_points"],
                "total_investment": stats["total_investment"],
                "total_payout": stats["total_payout"],
                "total_profit": stats["total_profit"],
                "hit_rate": stats["hit_rate"],
                "roi": stats["roi"],
            }
        )
    results.sort(key=lambda x: (-x["total_bets"], x["group_name"]))
    return results


def build_tag_overview_html(rows):
    tag_rows = get_reason_tag_summary(rows)
    if not tag_rows:
        return '<div class="detail-chip-empty">タグなし</div>'
    chips = []
    for item in tag_rows[:10]:
        chips.append(
            f'<div class="overview-chip"><span class="overview-chip-name">{item["group_name"]}</span><span class="overview-chip-count">{item["total_bets"]}件</span></div>'
        )
    return f'<div class="overview-chip-wrap">{"".join(chips)}</div>'


def yen(n):
    try:
        return f"{int(n):,}円"
    except Exception:
        return "0円"


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
    return [normalize_pick_text(x) for x in str(selection_text or "").split(" / ") if normalize_pick_text(x)]


def unique_preserve(seq):
    result = []
    seen = set()
    for x in seq:
        if x not in seen:
            seen.add(x)
            result.append(x)
    return result


def merge_selected_items(official_selected, ai_selected):
    return unique_preserve(list(official_selected) + list(ai_selected))


def get_selected_count_from_text(selection_text):
    return len(selection_items(selection_text))


def get_selected_total_amount(race):
    return int(race.get("amount") or 0) * get_selected_count_from_text(
        race.get("purchased_selection_text", "")
    )


def make_race_key(race_date, venue, race_no):
    return (
        str(race_date or "").strip(),
        str(venue or "").strip(),
        str(race_no or "").strip(),
    )


def get_existing_race_map_by_date(race_date):
    ensure_db_initialized()
    conn = db_connect()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        '''
        SELECT *
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
        key = make_race_key(row['race_date'], row['venue'], row['race_no'])
        if key not in race_map:
            race_map[key] = row
    return race_map


def get_saved_state_map_by_race(race_date):
    ensure_db_initialized()
    conn = db_connect()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        '''
        SELECT id, race_date, venue, race_no, purchased, hit, payout, memo, purchased_selection_text
        FROM races
        WHERE race_date = %s
          AND venue <> 'テスト会場'
        ORDER BY
            CASE
                WHEN hit = 1 THEN 4
                WHEN purchased = 1 THEN 3
                WHEN payout > 0 THEN 2
                WHEN COALESCE(memo, '') <> '' THEN 1
                ELSE 0
            END DESC,
            id DESC
        ''',
        (race_date,),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    saved_map = {}
    for row in rows:
        key = (
            str(row['race_date']).strip(),
            str(row['venue']).strip(),
            str(row['race_no']).strip(),
        )
        if key in saved_map:
            continue
        saved_map[key] = {
            'purchased': int(row.get('purchased') or 0),
            'hit': int(row.get('hit') or 0),
            'payout': int(row.get('payout') or 0),
            'memo': str(row.get('memo') or '').strip(),
            'purchased_selection_text': str(row.get('purchased_selection_text') or '').strip(),
        }
    return saved_map


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


def render_player_names_html(player_names_text):
    player_map = parse_player_names_map(player_names_text)
    if not player_map:
        return '<div class="player-empty">未取得</div>'

    items = ""
    for lane in range(1, 7):
        name = player_map.get(lane, "未取得")
        items += f'''
        <div class="player-chip player-chip-{lane}">
          <span class="player-chip-lane">{render_lane_badge(lane)}</span>
          <span class="player-chip-name">{name}</span>
        </div>
        '''
    return f'<div class="player-chip-wrap">{items}</div>'


def render_player_rank_summary_html(player_names_text, class_history_text):
    player_map = parse_player_names_map(player_names_text)
    class_rows = parse_class_history_rows(class_history_text)
    class_map = {}
    for row in class_rows:
        lane = row.get("lane")
        if lane is None:
            continue
        class_map[lane] = row.get("classes", [])

    has_any = bool(player_map) or bool(class_map)
    if not has_any:
        return '<div class="player-rank-empty">未取得</div>'

    rows_html = ""
    for lane in range(1, 7):
        name = player_map.get(lane, "未取得")
        classes = class_map.get(lane, [])
        chips = ""
        if classes:
            for idx, cls in enumerate(classes):
                cls_safe = (cls or "").lower()
                sub = "現" if idx == 0 else f"-{idx}"
                current_cls = " current-class-chip" if idx == 0 else ""
                chips += f'<div class="class-chip class-chip-{cls_safe}{current_cls}"><span class="class-chip-sub">{sub}</span><span class="class-chip-main">{cls}</span></div>'
        else:
            chips = '<div class="class-history-empty">未取得</div>'

        rows_html += f'''
        <div class="player-rank-row">
          <div class="player-rank-main">
            <span class="player-rank-lane">{render_lane_badge(lane)}</span>
            <span class="player-rank-name">{name}</span>
          </div>
          <div class="player-rank-chips">{chips}</div>
        </div>
        '''

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


def render_class_history_blocks(class_history_text):
    rows = parse_class_history_rows(class_history_text)
    if not rows:
        return '<div class="class-history-empty">未取得</div>'

    html = ""
    for row in rows:
        lane = row.get("lane")
        classes = row.get("classes", [])
        chips = ""
        for idx, cls in enumerate(classes):
            cls_safe = (cls or "").lower()
            sub = "現" if idx == 0 else f"-{idx}"
            chips += f'<div class="class-chip class-chip-{cls_safe}"><span class="class-chip-sub">{sub}</span><span class="class-chip-main">{cls}</span></div>'
        html += f'''
        <div class="class-history-row">
          <div class="class-history-lane">{render_lane_badge(lane)}</div>
          <div class="class-history-chips">{chips}</div>
        </div>
        '''
    return f'<div class="class-history-wrap">{html}</div>'


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

    input_name = "selected_official" if source == "official" else "selected_ai"
    prefix = "cmp-off" if source == "official" else "cmp-ai"

    chips = ""
    for idx, item in enumerate(own_items):
        item_clean = normalize_pick_text(item)
        checked = "checked" if item_clean in selected_items else ""
        item_id = f"{prefix}-{race_id_key}-{idx}"

        if item_clean in overlap_set:
            chip_kind = "overlap"
        else:
            chip_kind = source

        chips += f'''
        <label class="selection-choice-chip selection-choice-chip-{chip_kind}" for="{item_id}">
          <input
            class="selection-choice-input"
            type="checkbox"
            id="{item_id}"
            name="{input_name}"
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


def render_selection_compare_html(r, race_id_key):
    official_text = r.get("selection", "")
    ai_text = r.get("ai_selection", "")
    selected_items = set(selection_items(r.get("purchased_selection_text", "")))
    form_id = f"race-form-{race_id_key}"

    data = build_selection_compare_data(official_text, ai_text)

    ai_html = render_selection_column(
        data["ai_items"],
        data["overlap"],
        "ai",
        "未取得",
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
        <div class="selection-col-title selection-col-title-ai">AI買い目</div>
        {ai_html}
      </div>
      <div class="selection-compare-col selection-compare-col-official">
        <div class="selection-col-title selection-col-title-official">参考: 公式買い目</div>
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
        '''
        SELECT *
        FROM races
        WHERE race_date = %s AND venue <> 'テスト会場'
        ORDER BY time ASC, venue ASC, race_no_num ASC
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
    cur.execute("SELECT * FROM races WHERE id = %s", (race_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row


def get_today_races():
    return get_races_by_date(today_text())


def get_filtered_today_races(show_closed=False, ai_rating_filter=""):
    rows = [r for r in get_today_races() if is_star5_only(r)]
    if ai_rating_filter:
        rows = [r for r in rows if str(r.get("ai_rating", "")).strip() == ai_rating_filter]
    if not show_closed:
        rows = [r for r in rows if is_not_started(r["time"])]
    return rows


def update_race_result(race_id, selected_text, hit, payout, memo):
    ensure_db_initialized()
    selected_text = " / ".join(
        unique_preserve([normalize_pick_text(x) for x in selection_items(selected_text)])
    )
    purchased = 1 if selected_text else 0
    if purchased == 0:
        hit = 0
        payout = 0

    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        "UPDATE races SET purchased = %s, purchased_selection_text = %s, hit = %s, payout = %s, memo = %s WHERE id = %s",
        (purchased, selected_text, hit, payout, memo, race_id),
    )
    conn.commit()
    cur.close()
    conn.close()
    log(
        f"update_race_result race_id={race_id} purchased={purchased} selected={selected_text} hit={hit} payout={payout} memo={memo}"
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
        '''
        SELECT
            COUNT(*) AS total_rows,
            COALESCE(SUM(CASE WHEN COALESCE(purchased_selection_text, '') <> '' THEN 1 ELSE 0 END), 0) AS total_bets,
            COALESCE(SUM(CASE WHEN COALESCE(purchased_selection_text, '') <> '' THEN COALESCE(array_length(string_to_array(purchased_selection_text, ' / '), 1), 0) ELSE 0 END), 0) AS total_points,
            COALESCE(SUM(CASE WHEN COALESCE(purchased_selection_text, '') <> '' THEN amount * COALESCE(array_length(string_to_array(purchased_selection_text, ' / '), 1), 0) ELSE 0 END), 0) AS total_investment,
            COALESCE(SUM(CASE WHEN COALESCE(purchased_selection_text, '') <> '' THEN payout ELSE 0 END), 0) AS total_payout,
            COALESCE(SUM(CASE WHEN COALESCE(purchased_selection_text, '') <> '' AND hit = 1 THEN 1 ELSE 0 END), 0) AS total_hits,
            COALESCE(MAX(imported_at), '') AS last_imported_at
        FROM races
        WHERE race_date = %s AND venue <> 'テスト会場'
        ''',
        (race_date,),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    total_rows = row["total_rows"] or 0
    total_bets = row["total_bets"] or 0
    total_points = row["total_points"] or 0
    total_investment = row["total_investment"] or 0
    total_payout = row["total_payout"] or 0
    total_hits = row["total_hits"] or 0
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
        "last_imported_at": row["last_imported_at"] or "",
    }



def get_group_summary(race_date, group_key):
    ensure_db_initialized()
    if group_key not in {"rating", "venue", "ai_rating", "base_ai_rating", "final_rank"}:
        return []
    conn = db_connect()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        f'''
        SELECT
            {group_key} AS group_name,
            COUNT(CASE WHEN COALESCE(purchased_selection_text, '') <> '' THEN 1 END) AS total_bets,
            COALESCE(SUM(CASE WHEN COALESCE(purchased_selection_text, '') <> '' AND hit = 1 THEN 1 ELSE 0 END), 0) AS total_hits,
            COALESCE(SUM(CASE WHEN COALESCE(purchased_selection_text, '') <> '' THEN COALESCE(array_length(string_to_array(purchased_selection_text, ' / '), 1), 0) ELSE 0 END), 0) AS total_points,
            COALESCE(SUM(CASE WHEN COALESCE(purchased_selection_text, '') <> '' THEN amount * COALESCE(array_length(string_to_array(purchased_selection_text, ' / '), 1), 0) ELSE 0 END), 0) AS total_investment,
            COALESCE(SUM(CASE WHEN COALESCE(purchased_selection_text, '') <> '' THEN payout ELSE 0 END), 0) AS total_payout
        FROM races
        WHERE race_date = %s AND venue <> 'テスト会場'
        GROUP BY {group_key}
        ORDER BY {group_key} ASC
        ''',
        (race_date,),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    results = []
    for row in rows:
        total_bets = row["total_bets"] or 0
        total_hits = row["total_hits"] or 0
        total_points = row["total_points"] or 0
        total_investment = row["total_investment"] or 0
        total_payout = row["total_payout"] or 0
        total_profit = total_payout - total_investment
        hit_rate = round((total_hits / total_bets * 100), 1) if total_bets else 0
        roi = round((total_payout / total_investment * 100), 1) if total_investment else 0
        results.append(
            {
                "group_name": row["group_name"] or "(空白)",
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
    cur.execute("SELECT DISTINCT race_date FROM races WHERE venue <> 'テスト会場' ORDER BY race_date DESC")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [row[0] for row in rows]


def get_history_date_summaries():
    return [{"race_date": d, "summary": get_summary_by_date(d)} for d in get_history_dates()]


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


def build_card_html(r, is_history=False, race_date=""):
    checked_hit = "checked" if int(r.get("hit") or 0) == 1 else ""
    payout_value = r["payout"] if r["payout"] else ""
    memo_value = r["memo"] if r["memo"] else ""
    selected_count = get_selected_count_from_text(r.get("purchased_selection_text", ""))
    selected_total_amount = get_selected_total_amount(r)

    card_class = "card history-edit-card" if is_history else "card"
    if int(r.get("hit") or 0) == 1:
        card_class += " card-hit"
    elif selected_count > 0:
        card_class += " card-purchased"

    status_parts = []
    if selected_count > 0:
        status_parts.append(f'<span class="status-badge status-badge-saved">購入済み {selected_count}点</span>')
    if int(r.get("hit") or 0) == 1:
        status_parts.append('<span class="status-badge status-badge-hit">的中</span>')
    status_html = f'<div class="status-wrap">{"".join(status_parts)}</div>' if status_parts else ""

    ai_reasons = parse_json_array_text(r.get("ai_reasons", "[]"))
    exhibition = parse_json_array_text(r.get("exhibition", "[]"))
    ai_reason_html = ""
    if ai_reasons and not is_history:
        items = "".join([f"<li>{x}</li>" for x in ai_reasons])
        ai_reason_html = f'<div class="row"><span class="label">補正理由</span><span class="value text-left"><ul class="reason-list">{items}</ul></span></div>'

    race_id_key = f"history-{r['id']}" if is_history else str(r["id"])

    exhibition_time_html = render_exhibition_time_chips(exhibition)
    exhibition_rank_html = render_exhibition_rank_boxes(r.get("exhibition_rank", ""))
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
    ai_score_value = safe_float(r.get("final_ai_score"), safe_float(r.get("base_ai_score"), safe_float(r.get("ai_score"), 0)))
    ai_confidence_value = ""
    player_rank_summary_html = render_player_rank_summary_html(r.get("player_names_text", ""), r.get("class_history_text", ""))
    lane_score_html = render_lane_score_chips(r.get("ai_lane_score_text", ""))
    detail_material_html = render_detail_material_chips(ai_detail_text)
    reason_tag_html = render_reason_tag_chips(r)
    final_rank_html = final_rank_badge(r.get("final_rank"))
    countdown_html = render_countdown_badge(r["time"]) if not is_history else ""
    selected_summary_html = render_selected_summary_html(r.get("purchased_selection_text", ""))
    form_id = f"race-form-{race_id_key}"

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
        <span class="rating">{display_text(r.get('rating'), '公式評価なし')}</span>
        <span class="ai-rating">{display_ai_rating}</span>
        {final_rank_html}
      </div>

      <div class="metric-badge-row">
        <span class="metric-badge"><span class="metric-badge-label">券種</span><span class="metric-badge-value">{r['bet_type']}</span></span>
        <span class="metric-badge"><span class="metric-badge-label">選択点数</span><span class="metric-badge-value" id="selected-count-badge-{race_id_key}">{selected_count}点</span></span>
        <span class="metric-badge metric-badge-strong"><span class="metric-badge-label">購入額</span><span class="metric-badge-value" id="selected-total-badge-{race_id_key}">{yen(selected_total_amount)}</span></span>
        <span class="metric-badge metric-badge-score"><span class="metric-badge-label">AI補正点</span><span class="metric-badge-value">{round(ai_score_value, 2)}</span></span>
      </div>

      <div class="info-box">
        <div class="row row-selection-highlight"><span class="label">買い目比較</span><span class="value">{selection_compare_html}</span></div>
        <div class="row"><span class="label">選択中</span><span class="value"><div id="selected-summary-{race_id_key}">{selected_summary_html}</div></span></div>
        <div class="row"><span class="label">1点あたり</span><span class="value">{yen(r['amount'])}</span></div>
        <div class="row"><span class="label">AI信頼度</span><span class="value">{ai_confidence_value}</span></div>
        <div class="row"><span class="label">選手・ランク</span><span class="value">{player_rank_summary_html}</span></div>
        <div class="row"><span class="label">展示タイム</span><span class="value">{exhibition_time_html}</span></div>
        <div class="row"><span class="label">展示順位</span><span class="value">{exhibition_rank_html}</span></div>
        <div class="row"><span class="label">AI補正詳細</span><span class="value">{lane_score_html}</span></div>
        <div class="row"><span class="label">補正タグ</span><span class="value">{reason_tag_html}</span></div>
        <div class="row"><span class="label">詳細材料</span><span class="value">{detail_material_html}</span></div>
        {ai_reason_html}
      </div>

      <form id="{form_id}" method="post" action="{action_url}" class="form {'history-form' if is_history else ''}" data-race-id="{race_id_key}" data-amount="{int(r['amount'])}">
        <input type="hidden" name="race_id" value="{r['id']}">
        <input type="hidden" name="selected_text" id="selected-hidden-{race_id_key}" value="{r.get('purchased_selection_text', '')}">
        {history_hidden}

        <div id="detail-{race_id_key}" class="detail-box">
          <label class="checkline">
            <input type="checkbox" id="hit-{race_id_key}" name="hit" value="1" {checked_hit} onchange="toggleFormState('{race_id_key}')">
            的中した
          </label>

          <div class="input-row">
            <label>{'払戻額' if is_history else '払戻額（選んだ買い目全体の合計）'}</label>
            <input type="number" id="payout-{race_id_key}" name="payout" value="{payout_value}" placeholder="例: 870" min="0">
          </div>

          <div class="input-row">
            <label>メモ</label>
            <input type="text" name="memo" value="{memo_value}" placeholder="見送り、締切、様子見など">
          </div>
        </div>

        <button type="submit" class="save-btn {'half-btn' if is_history else ''}">保存</button>
      </form>
      {delete_form}
    </div>
    '''


def render_home(races, summary, message_type="", message_text="", show_closed=False, ai_rating_filter=""):
    updated_str = summary["last_imported_at"] if summary["last_imported_at"] else "未更新"
    if message_text:
        message_class = "message-success" if message_type == "success" else "message-error"
        message_html = f'<div class="message {message_class}">{message_text}</div>'
    else:
        message_html = ""
    checked_show_closed = "checked" if show_closed else ""
    ai_rating_options_html = render_ai_rating_filter_options(ai_rating_filter)
    cards_html = ''.join([build_card_html(r) for r in races]) if races else '<div class="empty">条件に合う★★★★★候補はありません</div>'
    external_line = f'<div class="sub"><strong>公開URL:</strong> <a href="{EXTERNAL_URL}">{EXTERNAL_URL}</a></div>' if EXTERNAL_URL else ''
    filter_status_text = "締切後も表示中" if show_closed else "締切前のみ表示中"
    filter_ai_text = ai_rating_filter if ai_rating_filter else "すべて"
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
        <div class="sub">評価：★★★★★のみ / 券種：3連単 / 締切予定時刻が早い順</div>
        <div class="sub">現在の絞り込み: {filter_status_text} / AI評価 {filter_ai_text}</div>
        {external_line}
        {message_html}
        <form method="get" action="/" class="filter-box">
          <div class="filter-grid">
            <div class="filter-item filter-item-wide">
              <label class="filter-check">
                <input type="checkbox" name="show_closed" value="1" {checked_show_closed}>
                締切後も表示する
              </label>
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
        </div>
        <div class="summary">
          <div class="summary-box"><div class="summary-label">表示中候補</div><div class="summary-value">{len(races)}</div></div>
          <div class="summary-box"><div class="summary-label">購入レース数</div><div class="summary-value">{summary['total_bets']}</div></div>
          <div class="summary-box"><div class="summary-label">購入点数</div><div class="summary-value">{summary['total_points']}</div></div>
          <div class="summary-box"><div class="summary-label">収支</div><div class="summary-value {profit_class(summary['total_profit'])}">{yen(summary['total_profit'])}</div></div>
        </div>
      </div>
      {cards_html}
    </div>
    '''
    return render_layout("今日の買い候補", content)



def render_stats_page(race_date, summary, by_rating, by_venue, by_ai_rating, by_base_ai_rating, by_final_rank, by_reason_tags, tag_overview_html=""):
    def make_table(rows):
        if not rows:
            return '<div class="empty">データがありません</div>'
        body = ""
        for r in rows:
            body += f"<tr><td>{r['group_name']}</td><td>{r['total_bets']}</td><td>{r['total_points']}</td><td>{r['total_hits']}</td><td>{yen(r['total_investment'])}</td><td>{yen(r['total_payout'])}</td><td class='{profit_class(r['total_profit'])}'>{yen(r['total_profit'])}</td><td>{percent(r['hit_rate'])}</td><td>{percent(r['roi'])}</td></tr>"
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
          <div class="summary-box"><div class="summary-label">収支</div><div class="summary-value {profit_class(summary['total_profit'])}">{yen(summary['total_profit'])}</div></div>
          <div class="summary-box"><div class="summary-label">的中率</div><div class="summary-value">{percent(summary['hit_rate'])}</div></div>
          <div class="summary-box"><div class="summary-label">回収率</div><div class="summary-value">{percent(summary['roi'])}</div></div>
          <div class="summary-box"><div class="summary-label">1点あたり平均投資</div><div class="summary-value">{yen(round(summary['total_investment'] / summary['total_points']) if summary['total_points'] else 0)}</div></div>
        </div>
      </div>

      <div class="header">
        <div class="section-title">補正タグの出現状況</div>
        <div class="sub">当日の理由文から自動でタグ化しています</div>
        {tag_overview_html}
      </div>

      <div class="stats-grid">
        <div>
          <div class="header"><div class="section-title">ベースAI星別集計</div></div>
          {make_table(by_base_ai_rating)}
        </div>
        <div>
          <div class="header"><div class="section-title">最終AI星別集計</div></div>
          {make_table(by_ai_rating)}
        </div>
      </div>

      <div class="stats-grid">
        <div>
          <div class="header"><div class="section-title">最終判定別集計</div></div>
          {make_table(by_final_rank)}
        </div>
        <div>
          <div class="header"><div class="section-title">補正タグ別集計</div></div>
          {make_table(by_reason_tags)}
        </div>
      </div>

      <div class="stats-grid">
        <div>
          <div class="header"><div class="section-title">公式星別集計</div></div>
          {make_table(by_rating)}
        </div>
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
            items += f'''<div class="history-item"><div class="history-top"><div class="history-date">{d}</div><a class="history-link" href="/history/{d}">結果を見る</a></div><div class="history-mini"><div class="history-mini-box"><div class="history-mini-label">候補数</div><div class="history-mini-value">{s['total_rows']}</div></div><div class="history-mini-box"><div class="history-mini-label">購入レース</div><div class="history-mini-value">{s['total_bets']}</div></div><div class="history-mini-box"><div class="history-mini-label">購入点数</div><div class="history-mini-value">{s['total_points']}</div></div><div class="history-mini-box"><div class="history-mini-label">収支</div><div class="history-mini-value {profit_class(s['total_profit'])}">{yen(s['total_profit'])}</div></div></div></div>'''
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

    tag_overview_html = build_tag_overview_html(filtered_races)

    if not filtered_races:
        body = '<div class="empty">条件に合うデータがありません</div>'
    else:
        cards_html = ''.join([build_card_html(r, is_history=True, race_date=race_date) for r in filtered_races])
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
        <div class="header">
          <div class="section-title">この条件の補正タグ</div>
          {tag_overview_html}
        </div>
        <div class="bulk-toolbar"><div class="bulk-toolbar-left"><button type="button" class="toolbar-btn" onclick="toggleAllBulk(true)">全選択</button><button type="button" class="toolbar-btn toolbar-btn-muted" onclick="toggleAllBulk(false)">選択解除</button></div><div class="bulk-toolbar-right"><span class="bulk-count" id="bulk-delete-count">0件選択中</span><button type="submit" class="toolbar-delete-btn" form="bulk-delete-form">選択したものを削除</button></div></div>
        {cards_html}
        '''
    content = f'''
    <div class="app-shell">
      <div class="topbar"><div class="brand"><div class="brand-logo">🧾</div><div><div class="brand-title">Race Candidates</div><div class="brand-sub">過去データ詳細</div></div></div><div class="topbar-status"><span class="top-pill">対象日: {race_date}</span></div></div>
      <div class="header hero hero-strong"><div class="title">過去データ詳細</div><div class="sub">対象日: {race_date}</div><div class="sub">最終取込時刻: {summary['last_imported_at'] or '未更新'}</div>{message_html}<div class="nav nav-app"><a href="/history" class="nav-card">過去データ一覧</a><a href="/" class="nav-card">今日の候補</a><a href="/history/{race_date}" class="nav-card active">この日の詳細</a></div><div class="summary"><div class="summary-box"><div class="summary-label">候補数</div><div class="summary-value">{summary['total_rows']}</div></div><div class="summary-box"><div class="summary-label">購入レース</div><div class="summary-value">{summary['total_bets']}</div></div><div class="summary-box"><div class="summary-label">購入点数</div><div class="summary-value">{summary['total_points']}</div></div><div class="summary-box"><div class="summary-label">収支</div><div class="summary-value {profit_class(summary['total_profit'])}">{yen(summary['total_profit'])}</div></div></div></div>
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
      .topbar{
        display:flex;
        justify-content:space-between;
        align-items:center;
        gap:10px;
      }
      .topbar-status{
        min-width:0;
        display:flex;
        justify-content:flex-end;
      }
      .top-pill{
        background:#eef4ff;
        color:#2f5bd2;
        padding:6px 10px;
        border-radius:999px;
        font-size:12px;
        display:inline-block;
        max-width:100%;
        white-space:normal;
        word-break:break-word;
        line-height:1.4;
      }
      .title{font-size:24px;font-weight:800;margin-bottom:4px}
      .nav{display:flex;gap:8px;flex-wrap:wrap;margin-top:10px}
      .nav-card{background:#eef2ff;color:#334;padding:9px 12px;border-radius:10px;text-decoration:none}
      .nav-card.active{background:#2f5bd2;color:#fff}
      .summary,.summary.six{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-top:12px}
      .summary.six{grid-template-columns:repeat(6,1fr)}
      .summary-box,.history-mini-box{background:#f8fafc;border:1px solid #eaecf0;border-radius:12px;padding:10px}
      .summary-label,.history-mini-label{font-size:12px;color:#667085}
      .summary-value,.history-mini-value{font-size:20px;font-weight:800;margin-top:4px}
      .profit-plus{color:#d92d20}
      .profit-minus{color:#175cd3}
      .profit-zero{color:#344054}
      .filter-box,.info-box{margin-top:10px}
      .filter-grid{display:grid;grid-template-columns:1.5fr 1fr auto;gap:10px;align-items:end}
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
      .rating,.ai-rating,.final-rank,.metric-badge{display:inline-flex;align-items:center;gap:6px;padding:8px 10px;border-radius:999px;font-size:13px;font-weight:700}
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
      .row{display:grid;grid-template-columns:110px 1fr;gap:10px;align-items:start;padding:10px 0;border-top:1px solid #eaecf0}
      .row:first-child{border-top:none}
      .label{font-weight:700;color:#344054}
      .value{min-width:0}
      .selection-compare-wrap{display:grid;grid-template-columns:1fr 1fr;gap:10px}
      .selection-compare-col{background:#f8fafc;border:1px solid #eaecf0;border-radius:10px;padding:8px}
      .selection-col-title{font-size:12px;color:#667085;margin-bottom:6px;font-weight:700}
      .selection-chip-grid{display:flex;gap:6px;flex-wrap:wrap}
      .selection-choice-chip{
        display:inline-block;
        cursor:pointer;
        user-select:none;
        -webkit-tap-highlight-color:transparent;
      }
      .selection-choice-input{
        position:absolute;
        opacity:0;
        pointer-events:none;
        width:1px;
        height:1px;
      }
      .selection-choice-body{
        display:inline-flex;
        align-items:center;
        justify-content:center;
        padding:8px 10px;
        border-radius:999px;
        font-weight:700;
        border:2px solid #d0d5dd;
        background:#fff;
        color:#344054;
        white-space:nowrap;
        line-height:1.2;
        transition:all .15s ease;
      }
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
      .ex-lane{display:flex;justify-content:center;margin-bottom:6px}
      .pick-inline{display:inline-flex;align-items:center;gap:4px;flex-wrap:nowrap}
      .pick-sep{font-weight:900;color:#667085;font-size:12px;line-height:1}
      .pick-plain{font-weight:800;color:#344054}
      .player-chip-wrap{display:flex;flex-direction:column;gap:6px}
      .player-chip{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
      .player-chip-lane{flex:0 0 auto}
      .player-chip-name{font-weight:700;color:#172033}
      .player-rank-wrap{display:flex;flex-direction:column;gap:8px}
      .player-rank-row{display:grid;grid-template-columns:minmax(180px,240px) 1fr;gap:10px;align-items:center;padding:6px 0}
      .player-rank-main{display:flex;align-items:center;gap:8px;min-width:0}
      .player-rank-lane{flex:0 0 auto}
      .player-rank-name{font-weight:800;color:#172033;line-height:1.35;word-break:break-word}
      .player-rank-chips{display:flex;gap:5px;flex-wrap:wrap;align-items:center}
      .picked-chip-wrap,.ex-chip-wrap,.lane-score-wrap,.detail-chip-wrap{display:flex;gap:8px;flex-wrap:wrap}
      .picked-chip,.ex-chip,.lane-score-chip,.detail-chip{padding:6px 8px;border-radius:8px;background:#f8fafc;border:1px solid #eaecf0}
      .picked-chip{white-space:nowrap}
      .selection-chip-empty,.ex-chip-empty,.lane-score-empty,.detail-chip-empty,.class-history-empty,.ex-rank-empty,.player-empty,.player-rank-empty{color:#667085}
      .ex-chip-lane,.lane-score-lane{font-weight:800;margin-right:6px;display:inline-flex;align-items:center}
      .lane-score-verygood{background:#ecfdf3}
      .lane-score-good{background:#eef4ff}
      .lane-score-bad{background:#fef3f2}
      .ex-rank-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:8px}
      .ex-rank-box{border:1px solid #eaecf0;background:#f8fafc;border-radius:10px;padding:8px;text-align:center}
      .ex-rank-1{background:#ecfdf3}
      .ex-rank-2{background:#eef4ff}
      .ex-rank-3{background:#fff6e5}
      .ex-rank-low{background:#fef3f2}
      .class-history-wrap{display:flex;flex-direction:column;gap:8px}
      .class-history-row{display:grid;grid-template-columns:60px 1fr;gap:8px;align-items:center}
      .class-history-lane{font-weight:800;display:flex;align-items:center}
      .class-history-chips{display:flex;gap:5px;flex-wrap:wrap}
      .class-chip{display:inline-flex;gap:5px;align-items:center;border-radius:10px;padding:6px 8px;border:1px solid #d0d5dd;background:#fff}
      .class-chip-a1{background:#ecfdf3}
      .class-chip-a2{background:#eef4ff}
      .class-chip-b1{background:#fff6e5}
      .class-chip-b2{background:#fef3f2}
      .class-chip-sub{font-size:11px;color:#667085}
      .class-chip-main{font-weight:800}
      .form{margin-top:12px}
      .detail-box{display:flex;flex-direction:column;gap:10px}
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

.reason-tag-chip{padding:6px 10px;border-radius:999px;border:1px solid #d0d5dd;background:#fff;font-weight:700;font-size:12px}
.reason-tag-good{background:#ecfdf3;color:#067647;border-color:#a6f4c5}
.reason-tag-info{background:#eef4ff;color:#175cd3;border-color:#cdddff}
.reason-tag-warn{background:#fff4e5;color:#b54708;border-color:#f7d79b}
.reason-tag-neutral{background:#f2f4f7;color:#475467;border-color:#d0d5dd}
.overview-chip-wrap{display:flex;gap:8px;flex-wrap:wrap;margin-top:10px}
.overview-chip{display:inline-flex;align-items:center;gap:8px;padding:8px 10px;border-radius:999px;background:#f8fafc;border:1px solid #eaecf0}
.overview-chip-name{font-weight:800;color:#344054}
.overview-chip-count{font-size:12px;color:#667085}

      .bottom-nav{position:fixed;left:0;right:0;bottom:0;display:grid;grid-template-columns:repeat(3,1fr);background:#fff;border-top:1px solid #eaecf0;padding:8px 10px calc(8px + env(safe-area-inset-bottom,0px));z-index:50}
      .bottom-nav-item{text-decoration:none;color:#667085;display:flex;flex-direction:column;align-items:center;gap:2px;padding:6px 0}
      .bottom-nav-item.active{color:#175cd3;font-weight:800}
      .player-rank-row{display:grid;grid-template-columns:minmax(180px, 260px) minmax(360px, 1fr);gap:12px;align-items:center}
      .player-rank-main{min-width:0;display:flex;align-items:center;gap:10px}
      .player-rank-name{min-width:0;font-weight:800;line-height:1.35;word-break:keep-all;overflow-wrap:anywhere}
      .player-rank-chips{display:grid;grid-template-columns:repeat(4, minmax(72px, max-content));gap:8px;min-width:0;justify-content:start}
      .player-rank-chips .class-chip{min-width:72px;padding:8px 12px;white-space:nowrap;justify-content:center}
      @media (max-width: 760px){
        html{background:#f5f7fb}
        .container{max-width:none;padding:calc(12px + env(safe-area-inset-top,0px)) 10px calc(92px + env(safe-area-inset-bottom,0px));}
        .topbar{flex-direction:column;align-items:flex-start;padding:14px;border-radius:18px;}
        .topbar-status{width:100%;justify-content:flex-start;}
        .top-pill{width:100%;border-radius:12px;}
        .header,.card,.history-item,.bulk-toolbar,.history-filter-box{padding:14px;border-radius:18px;}
        .summary,.summary.six,.history-mini,.stats-grid,.filter-grid,.selection-compare-wrap,.history-filter-grid{grid-template-columns:1fr;}
        .row{grid-template-columns:1fr;gap:8px;}
        .race-venue,.race-rno{font-size:20px}
        .time{font-size:22px}
        .ex-rank-grid{grid-template-columns:repeat(2,1fr)}
        .card-top-main{flex-direction:column;align-items:flex-start}
        .status-wrap{margin-top:2px}
        .class-history-row{grid-template-columns:1fr;gap:10px;align-items:start;}
        .class-history-lane{font-size:15px;line-height:1.2;margin-bottom:2px;}
        .class-history-chips{gap:8px}
        .class-chip{padding:6px 8px;min-height:36px;border-radius:10px}
        .class-chip-sub{font-size:12px;font-weight:700;color:#667085;min-width:auto;text-align:center;background:none;padding:0}
        .class-chip-main{font-size:15px;font-weight:900}
        .class-history-row .class-history-chips .class-chip:first-child{padding-left:10px;padding-right:10px;border-width:2px}
        .class-history-row .class-history-chips .class-chip:first-child .class-chip-main{font-size:18px;font-weight:900;letter-spacing:.01em}
        .history-filter-meta{flex-direction:column;align-items:flex-start}
        .jump-wrap{width:100%}
        .jump-chip{min-width:48px;padding:8px 11px}
        .bulk-toolbar{flex-direction:column;align-items:stretch}
        .bulk-toolbar-left,.bulk-toolbar-right{width:100%;justify-content:space-between;flex-wrap:wrap}
        .player-chip{align-items:flex-start}
        .player-chip-name{font-size:14px;line-height:1.45}
        .player-rank-row{grid-template-columns:1fr;gap:8px;align-items:start;padding:4px 0}
        .player-rank-main{gap:8px}
        .player-rank-name{font-size:14px;line-height:1.4}
        .player-rank-chips{gap:6px;flex-wrap:wrap}
        .player-rank-chips .class-chip{min-width:0}
        .current-class-chip .class-chip-main{font-size:18px}
        .lane-color{min-width:24px;height:24px;font-size:14px;border-radius:4px}
        .pick-inline{gap:5px}
        .pick-sep{font-size:13px}
        .bottom-nav{left:0;right:0;transform:none;bottom:0;width:auto;border-radius:0;border-left:none;border-right:none;box-shadow:none;padding:8px 10px calc(8px + env(safe-area-inset-bottom,0px));}
        .bottom-nav-item{border-radius:12px}
        .bottom-nav-item.active{background:none;box-shadow:none;color:#175cd3;}
      }
    </style>
    """

    js = """
    <script>
      function getCardRootByRaceId(raceId){
        return document.querySelector('[data-race-card-id="' + raceId + '"]') || document.querySelector('[data-race-id="' + raceId + '"]');
      }

      function parseSelectionText(text){
        return String(text || '')
          .split(' / ')
          .map(x => String(x || '').replace(/\\s+/g, '').trim())
          .filter(Boolean);
      }

      function getRaceCheckboxes(raceId){
        return Array.from(document.querySelectorAll('input[type="checkbox"][data-pick-value][data-race-group="' + raceId + '"]'));
      }

      function getCheckedValues(raceId){
        return getRaceCheckboxes(raceId)
          .filter(x => x.checked)
          .map(x => (x.getAttribute('data-pick-value') || '').trim())
          .filter(Boolean);
      }

      function renderColoredPickHtml(value){
        const s = String(value || '').replace(/\\s+/g, '').trim();
        if(!s){ return ''; }
        return '<span class="pick-inline">' + s.split('-').map((part, idx) => {
          const sep = idx > 0 ? '<span class="pick-sep">-</span>' : '';
          if(/^\\d+$/.test(part)){
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

      function updateSelectionSummary(raceId, preserveHiddenWhenEmpty=true){
        const root = getCardRootByRaceId(raceId);
        if(!root){ return; }

        const summaryEl = document.getElementById('selected-summary-' + raceId);
        const countEl = document.getElementById('selected-count-badge-' + raceId);
        const totalEl = document.getElementById('selected-total-badge-' + raceId);
        const formEl = document.querySelector('form[data-race-id="' + raceId + '"]');
        const amount = parseInt((formEl && formEl.getAttribute('data-amount')) || '0', 10);
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

        if(countEl){
          countEl.textContent = values.length + '点';
        }

        if(totalEl){
          totalEl.textContent = (amount * values.length).toLocaleString('ja-JP') + '円';
        }

        if(hidden && (values.length > 0 || !preserveHiddenWhenEmpty)){
          hidden.value = values.join(' / ');
        }
      }

      function toggleFormState(raceId){
        return true;
      }

      function updateBulkDeleteCount(){
        const count = document.querySelectorAll('.bulk-checkbox:checked').length;
        const el = document.getElementById('bulk-delete-count');
        if(el){
          el.textContent = count + '件選択中';
        }
      }

      function toggleAllBulk(checked){
        document.querySelectorAll('.bulk-checkbox').forEach(el => {
          el.checked = checked;
        });
        updateBulkDeleteCount();
      }

      function confirmBulkDelete(){
        const count = document.querySelectorAll('.bulk-checkbox:checked').length;
        if(count <= 0){
          alert('削除するデータを選んでください');
          return false;
        }
        return confirm(count + '件を削除しますか？');
      }

      document.addEventListener('DOMContentLoaded', function(){
        document.querySelectorAll('form[data-race-id]').forEach(form => {
          const raceId = form.getAttribute('data-race-id');
          setCheckedValuesFromHidden(raceId);
          updateSelectionSummary(raceId, true);
          form.addEventListener('submit', function(){
            syncSelectionValue(null, raceId);
          });
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
    return """<!doctype html><html lang="ja"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover"><title>{}</title>{}</head><body><div class="container">{}</div>{}{}{}</body></html>""".format(title, css, body_html, bottom_nav_html, js, "")


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
    _db_initialized = True


def init_db():
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
            motor_rank TEXT NOT NULL DEFAULT '',
            ai_detail TEXT NOT NULL DEFAULT '',
            ai_selection TEXT NOT NULL DEFAULT '',
            ai_confidence TEXT NOT NULL DEFAULT '',
            ai_lane_score_text TEXT NOT NULL DEFAULT '',
            class_history_text TEXT NOT NULL DEFAULT '',
            player_names_text TEXT NOT NULL DEFAULT '',

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
            imported_at TEXT NOT NULL DEFAULT ''
        )
        '''
    )

    alter_sqls = [
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS race_no_num INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS ai_score DOUBLE PRECISION NOT NULL DEFAULT 0",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS ai_rating TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS ai_label TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS final_rank TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS ai_reasons JSONB NOT NULL DEFAULT '[]'::jsonb",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS exhibition JSONB NOT NULL DEFAULT '[]'::jsonb",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS exhibition_rank TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS motor_rank TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS ai_detail TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS ai_selection TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS ai_confidence TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS ai_lane_score_text TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS class_history_text TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS player_names_text TEXT NOT NULL DEFAULT ''",

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
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS imported_at TEXT NOT NULL DEFAULT ''",
    ]
    for sql in alter_sqls:
        cur.execute(sql)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_races_race_date ON races (race_date)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_races_race_key ON races (race_date, venue, race_no)")

    conn.commit()
    cur.close()
    conn.close()


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

    for r in cleaned:
        key = make_race_key(r.get('race_date'), r.get('venue'), r.get('race_no'))
        existing = existing_map.get(key)

        if existing:
            cur.execute(
                '''
                UPDATE races
                SET
                    time = CASE
                        WHEN COALESCE(%s, '') <> '' THEN %s
                        ELSE time
                    END,
                    race_no_num = %s,
                    rating = %s,
                    bet_type = %s,
                    selection = %s,
                    amount = %s,
                    player_names_text = %s,
                    class_history_text = %s,
                    base_ai_score = %s,
                    base_ai_rating = %s,
                    base_ai_selection = %s,
                    base_reason_text = %s,
                    base_updated_at = %s,
                    imported_at = %s
                WHERE id = %s
                ''',
                (
                    str(r.get('time') or '').strip(),
                    str(r.get('time') or '').strip(),
                    int(r.get('race_no_num') or 0),
                    str(r.get('rating') or '').strip(),
                    str(r.get('bet_type') or '').strip(),
                    str(r.get('selection') or '').strip(),
                    int(r.get('amount') or 100),
                    str(r.get('player_names_text') or '').strip(),
                    str(r.get('class_history_text') or '').strip(),
                    safe_float(r.get('base_ai_score', 0), 0),
                    str(r.get('base_ai_rating') or '').strip(),
                    str(r.get('base_ai_selection') or '').strip(),
                    str(r.get('base_reason_text') or '').strip(),
                    str(r.get('base_updated_at') or '').strip(),
                    jst_now_str(),
                    existing['id'],
                )
            )
            updated += 1
        else:
            cur.execute(
                '''
                INSERT INTO races (
                    race_date, time, venue, race_no, race_no_num,
                    rating, bet_type, selection, amount,
                    player_names_text, class_history_text,
                    base_ai_score, base_ai_rating, base_ai_selection, base_reason_text, base_updated_at,
                    purchased, purchased_selection_text, hit, payout, memo, imported_at
                )
                VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s
                )
                ''',
                (
                    str(r.get('race_date') or '').strip(),
                    str(r.get('time') or '').strip(),
                    str(r.get('venue') or '').strip(),
                    str(r.get('race_no') or '').strip(),
                    int(r.get('race_no_num') or 0),
                    str(r.get('rating') or '').strip(),
                    str(r.get('bet_type') or '').strip(),
                    str(r.get('selection') or '').strip(),
                    int(r.get('amount') or 100),
                    str(r.get('player_names_text') or '').strip(),
                    str(r.get('class_history_text') or '').strip(),
                    safe_float(r.get('base_ai_score', 0), 0),
                    str(r.get('base_ai_rating') or '').strip(),
                    str(r.get('base_ai_selection') or '').strip(),
                    str(r.get('base_reason_text') or '').strip(),
                    str(r.get('base_updated_at') or '').strip(),
                    0,
                    '',
                    0,
                    0,
                    '',
                    jst_now_str(),
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
        key = make_race_key(r.get('race_date'), r.get('venue'), r.get('race_no'))
        existing = existing_map.get(key)

        if not existing:
            skipped += 1
            continue

        cur.execute(
            '''
            UPDATE races
            SET
                time = CASE
                    WHEN COALESCE(%s, '') <> '' THEN %s
                    ELSE time
                END,
                exhibition = %s,
                exhibition_rank = %s,
                final_ai_score = %s,
                final_ai_rating = %s,
                final_ai_selection = %s,
                final_rank = %s,
                latest_reason_text = %s,
                latest_updated_at = %s,
                imported_at = %s
            WHERE id = %s
            ''',
            (
                str(r.get('time') or '').strip(),
                str(r.get('time') or '').strip(),
                json.dumps(r.get('exhibition', []), ensure_ascii=False),
                str(r.get('exhibition_rank') or '').strip(),
                safe_float(r.get('final_ai_score', 0), 0),
                str(r.get('final_ai_rating') or '').strip(),
                str(r.get('final_ai_selection') or '').strip(),
                str(r.get('final_rank') or '').strip(),
                str(r.get('latest_reason_text') or '').strip(),
                str(r.get('latest_updated_at') or '').strip(),
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
    ai_rating_filter = request.args.get("ai_rating", "").strip()
    if ai_rating_filter not in AI_RATING_OPTIONS:
        ai_rating_filter = ""
    races = get_filtered_today_races(show_closed=show_closed, ai_rating_filter=ai_rating_filter)
    summary = get_summary_by_date(today_text())
    return render_home(
        races,
        summary,
        request.args.get("type", "").strip(),
        request.args.get("msg", "").strip(),
        show_closed=show_closed,
        ai_rating_filter=ai_rating_filter,
    )


def parse_selected_from_request():
    selected_text = normalize_pick_text(request.form.get("selected_text", ""))
    if selected_text:
        return " / ".join(unique_preserve(selection_items(selected_text)))

    official = [normalize_pick_text(x) for x in request.form.getlist("selected_official")]
    ai = [normalize_pick_text(x) for x in request.form.getlist("selected_ai")]
    return " / ".join(merge_selected_items(official, ai))


@app.route("/save", methods=["POST"])
def save():
    race_id = int(request.form.get("race_id", "0"))
    race = get_race_by_id(race_id)
    if not race:
        return redirect("/?type=error&msg=" + quote("データが見つかりません"))

    selected_text = parse_selected_from_request()
    purchased = 1 if selected_text else 0
    hit = 1 if request.form.get("hit") == "1" else 0
    payout_raw = request.form.get("payout", "").strip()
    payout = int(payout_raw) if payout_raw else 0
    memo = request.form.get("memo", "").strip()

    if purchased == 0:
        hit = 0
        payout = 0

    if purchased == 1 and hit == 1 and payout <= 0:
        redirect_base = "/?show_closed=1" if not is_not_started(race["time"]) else "/"
        sep = "&" if "?" in redirect_base else "?"
        return redirect(redirect_base + sep + "type=error&msg=" + quote("的中にした場合は払戻額を入力してください"))

    update_race_result(race_id, selected_text, hit, payout, memo)

    redirect_base = "/?show_closed=1" if not is_not_started(race["time"]) else "/"
    sep = "&" if "?" in redirect_base else "?"
    return redirect(redirect_base + sep + "type=success&msg=" + quote("保存しました"))


@app.route("/update_record", methods=["POST"])
def update_record():
    race_id = int(request.form.get("race_id", "0"))
    redirect_to = safe_redirect_path(request.form.get("redirect_to", "/history"), "/history")
    race = get_race_by_id(race_id)
    if not race:
        return redirect(redirect_to + ("&" if "?" in redirect_to else "?") + "type=error&msg=" + quote("データが見つかりません"))
    selected_text = parse_selected_from_request()
    purchased = 1 if selected_text else 0
    hit = 1 if request.form.get("hit") == "1" else 0
    payout_raw = request.form.get("payout", "").strip()
    payout = int(payout_raw) if payout_raw else 0
    memo = request.form.get("memo", "").strip()
    if purchased == 0:
        hit = 0
        payout = 0
    if purchased == 1 and hit == 1 and payout <= 0:
        return redirect(redirect_to + ("&" if "?" in redirect_to else "?") + "type=error&msg=" + quote("的中にした場合は払戻額を入力してください"))
    update_race_result(race_id, selected_text, hit, payout, memo)
    return redirect(redirect_to + ("&" if "?" in redirect_to else "?") + "type=success&msg=" + quote("過去データを保存しました"))


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
    rows = get_races_by_date(race_date)
    return render_stats_page(
        race_date,
        get_summary_by_date(race_date),
        get_group_summary(race_date, "rating"),
        get_group_summary(race_date, "venue"),
        get_group_summary(race_date, "ai_rating"),
        get_group_summary(race_date, "base_ai_rating"),
        get_group_summary(race_date, "final_rank"),
        get_reason_tag_summary(rows),
        build_tag_overview_html(rows),
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

    required_keys = {
        "race_date", "venue", "race_no", "race_no_num",
        "rating", "bet_type", "selection", "amount"
    }

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
                "rating": str(r.get("rating") or "").strip(),
                "bet_type": str(r.get("bet_type") or "").strip(),
                "selection": str(r.get("selection") or "").strip(),
                "amount": int(r.get("amount") or 100),
                "player_names_text": str(r.get("player_names_text") or "").strip(),
                "class_history_text": str(r.get("class_history_text") or "").strip(),
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

    result = upsert_base_candidates(cleaned)

    return jsonify(
        {
            "ok": True,
            "received": len(cleaned),
            "inserted": result["inserted"],
            "updated": result["updated"],
            "imported_at": jst_now_str(),
        }
    )



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
        key = f"{str(row['venue']).strip()}|{str(row['race_no']).strip()}"
        base_map[key] = {
            "base_ai_score": safe_float(row.get("base_ai_score"), 0),
            "base_ai_rating": str(row.get("base_ai_rating") or "").strip(),
            "base_ai_selection": str(row.get("base_ai_selection") or "").strip(),
            "base_reason_text": str(row.get("base_reason_text") or "").strip(),
            "final_ai_score": safe_float(row.get("final_ai_score"), 0),
            "final_ai_rating": str(row.get("final_ai_rating") or "").strip(),
            "final_ai_selection": str(row.get("final_ai_selection") or "").strip(),
            "latest_reason_text": str(row.get("latest_reason_text") or "").strip(),
        }

    return jsonify({
        "ok": True,
        "race_date": race_date,
        "count": len(base_map),
        "base_map": base_map,
    })


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
                "time": str(r.get("time") or "").strip(),
                "exhibition": r.get("exhibition", []),
                "exhibition_rank": str(r.get("exhibition_rank") or "").strip(),
                "final_ai_score": safe_float(r.get("final_ai_score", 0), 0),
                "final_ai_rating": str(r.get("final_ai_rating") or "").strip(),
                "final_ai_selection": str(r.get("final_ai_selection") or "").strip(),
                "final_rank": str(r.get("final_rank") or "").strip(),
                "latest_reason_text": str(r.get("latest_reason_text") or "").strip(),
                "latest_updated_at": str(r.get("latest_updated_at") or "").strip(),
            }
        )

    if not cleaned:
        return jsonify({"ok": False, "error": "races is empty"}), 400

    race_dates = sorted(set(r["race_date"] for r in cleaned))
    if len(race_dates) != 1:
        return jsonify({"ok": False, "error": "multiple race_date values are not allowed"}), 400

    result = upsert_latest_candidates(cleaned)

    return jsonify(
        {
            "ok": True,
            "received": len(cleaned),
            "updated": result["updated"],
            "skipped": result["skipped"],
            "imported_at": jst_now_str(),
        }
    )




init_db()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)