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


def yen(n):
    try:
        return f"{int(n):,}円"
    except Exception:
        return "0円"


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


def get_saved_state_map_by_race(race_date):
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
            str(row["race_date"]).strip(),
            str(row["venue"]).strip(),
            str(row["race_no"]).strip(),
        )
        if key in saved_map:
            continue
        saved_map[key] = {
            "purchased": int(row.get("purchased") or 0),
            "hit": int(row.get("hit") or 0),
            "payout": int(row.get("payout") or 0),
            "memo": str(row.get("memo") or "").strip(),
            "purchased_selection_text": str(row.get("purchased_selection_text") or "").strip(),
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
          <div class="ex-lane">{lane}号艇</div>
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
          <span class="ex-chip-lane">{i}</span>
          <span class="ex-chip-time">{t}</span>
        </div>
        '''
    return f'<div class="ex-chip-wrap">{chips}</div>'


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
          <div class="class-history-lane">{lane}号艇</div>
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
        chips += f'<div class="{lane_score_class(score)}"><span class="lane-score-lane">{lane}号艇</span><span class="lane-score-value">{score:.2f}</span></div>'
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


def chip_class_for_compare(source):
    if source == "overlap":
        return "selection-choice-chip selection-chip-overlap"
    if source == "official":
        return "selection-choice-chip selection-chip-official"
    return "selection-choice-chip selection-chip-ai"


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
        source_name = "overlap" if item_clean in overlap_set else source
        checked = "checked" if item_clean in selected_items else ""
        item_id = f"{prefix}-{race_id_key}-{idx}"

        chips += f'''
        <label class="{chip_class_for_compare(source_name)}">
          <input
            type="checkbox"
            id="{item_id}"
            name="{input_name}"
            value="{item_clean}"
            data-pick-value="{item_clean}"
            {checked}
            onchange="syncSelectionValue(this, '{race_id_key}'); updateSelectionSummary('{race_id_key}')"
          >
          <span class="selection-choice-text">{item_clean}</span>
        </label>
        '''

    return f'<div class="selection-chip-grid compact-grid">{chips}</div>'


def render_selection_compare_html(r, race_id_key):
    official_text = r.get("selection", "")
    ai_text = r.get("ai_selection", "")
    selected_items = set(selection_items(r.get("purchased_selection_text", "")))

    data = build_selection_compare_data(official_text, ai_text)

    official_html = render_selection_column(
        data["official_items"],
        data["overlap"],
        "official",
        "未取得",
        race_id_key=race_id_key,
        selected_items=selected_items,
    )
    ai_html = render_selection_column(
        data["ai_items"],
        data["overlap"],
        "ai",
        "未取得",
        race_id_key=race_id_key,
        selected_items=selected_items,
    )

    return f'''
    <div class="selection-compare-wrap">
      <div class="selection-compare-col">
        <div class="selection-col-title">公式買い目</div>
        {official_html}
      </div>
      <div class="selection-compare-col">
        <div class="selection-col-title">AI買い目</div>
        {ai_html}
      </div>
    </div>
    '''


def render_selected_summary_html(selected_text):
    items = selection_items(selected_text)
    if not items:
        return '<div class="selection-chip-empty">未選択</div>'
    chips = "".join([f'<div class="picked-chip">{item}</div>' for item in items])
    return f'<div class="picked-chip-wrap">{chips}</div>'


def get_races_by_date(race_date):
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


def delete_today_races():
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("DELETE FROM races WHERE race_date = %s", (today_text(),))
    conn.commit()
    cur.close()
    conn.close()


def update_race_result(race_id, selected_text, hit, payout, memo):
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
    log(f"update_race_result race_id={race_id} purchased={purchased} selected={selected_text} hit={hit} payout={payout} memo={memo}")


def delete_race(race_id):
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
    if group_key not in {"rating", "venue", "ai_rating", "final_rank"}:
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
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT race_date FROM races WHERE venue <> 'テスト会場' ORDER BY race_date DESC")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [row[0] for row in rows]


def get_history_date_summaries():
    return [{"race_date": d, "summary": get_summary_by_date(d)} for d in get_history_dates()]


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
    selection_compare_html = render_selection_compare_html(r, race_id_key)
    ai_detail_text = normalize_ai_detail(r.get("ai_detail"), exhibition)
    ai_score_value = safe_float(r.get("ai_score"), 0)
    ai_confidence_value = display_text(r.get("ai_confidence"), "未取得")
    class_history_html = render_class_history_blocks(r.get("class_history_text", ""))
    lane_score_html = render_lane_score_chips(r.get("ai_lane_score_text", ""))
    detail_material_html = render_detail_material_chips(ai_detail_text)
    final_rank_html = final_rank_badge(r.get("final_rank"))
    countdown_html = render_countdown_badge(r["time"]) if not is_history else ""
    selected_summary_html = render_selected_summary_html(r.get("purchased_selection_text", ""))

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
    <div class="{card_class}">
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
        <span class="ai-rating">{display_text(r.get('ai_rating'), 'AI評価なし')}</span>
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
        <div class="row"><span class="label">3期ランク</span><span class="value">{class_history_html}</span></div>
        <div class="row"><span class="label">展示タイム</span><span class="value">{exhibition_time_html}</span></div>
        <div class="row"><span class="label">展示順位</span><span class="value">{exhibition_rank_html}</span></div>
        <div class="row"><span class="label">AI補正詳細</span><span class="value">{lane_score_html}</span></div>
        <div class="row"><span class="label">詳細材料</span><span class="value">{detail_material_html}</span></div>
        {ai_reason_html}
      </div>

      <form method="post" action="{action_url}" class="form {'history-form' if is_history else ''}" data-race-id="{race_id_key}" data-amount="{int(r['amount'])}">
        <input type="hidden" name="race_id" value="{r['id']}">
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
        <div class="brand"><div class="brand-logo">🏁</div><div><div class="brand-title">Race Candidates</div><div class="brand-sub">ボートレース買い候補</div></div></div>
        <div class="topbar-status"><span class="top-pill">最終取込: {updated_str}</span></div>
      </div>
      <div class="header hero hero-strong">
        <div class="title">今日の買い候補</div>
        <div class="sub">評価：★★★★★のみ / 券種：3連単 / 締切予定時刻が早い順</div>
        <div class="sub">現在の絞り込み: {filter_status_text} / AI評価 {filter_ai_text}</div>
        {external_line}
        {message_html}
        <form method="get" action="/" class="filter-box">
          <div class="filter-grid">
            <div class="filter-item filter-item-wide"><label class="filter-check"><input type="checkbox" name="show_closed" value="1" {checked_show_closed}>締切後も表示する</label></div>
            <div class="filter-item"><label for="ai_rating">AI評価で絞る</label><select name="ai_rating" id="ai_rating">{ai_rating_options_html}</select></div>
            <div class="filter-actions"><button type="submit" class="filter-btn">フィルター適用</button><a href="/" class="filter-reset">解除</a></div>
          </div>
        </form>
        <div class="nav nav-app"><a href="/" class="nav-card active">今日の候補</a><a href="/stats" class="nav-card">今日の集計</a><a href="/history" class="nav-card">過去データ</a></div>
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


def render_stats_page(race_date, summary, by_rating, by_venue, by_ai_rating, by_final_rank):
    def make_table(rows):
        if not rows:
            return '<div class="empty">データがありません</div>'
        body = ""
        for r in rows:
            body += f"<tr><td>{r['group_name']}</td><td>{r['total_bets']}</td><td>{r['total_points']}</td><td>{r['total_hits']}</td><td>{yen(r['total_investment'])}</td><td>{yen(r['total_payout'])}</td><td class='{profit_class(r['total_profit'])}'>{yen(r['total_profit'])}</td><td>{percent(r['hit_rate'])}</td><td>{percent(r['roi'])}</td></tr>"
        return f"<div class='table-wrap'><table><thead><tr><th>区分</th><th>購入レース</th><th>購入点数</th><th>的中</th><th>投資</th><th>払戻</th><th>収支</th><th>的中率</th><th>回収率</th></tr></thead><tbody>{body}</tbody></table></div>"

    content = f'''
    <div class="app-shell">
      <div class="topbar"><div class="brand"><div class="brand-logo">📊</div><div><div class="brand-title">Race Candidates</div><div class="brand-sub">今日の集計</div></div></div><div class="topbar-status"><span class="top-pill">対象日: {race_date}</span></div></div>
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
      <div class="stats-grid"><div><div class="header"><div class="section-title">公式星別集計</div></div>{make_table(by_rating)}</div><div><div class="header"><div class="section-title">AI補正星別集計</div></div>{make_table(by_ai_rating)}</div></div>
      <div class="stats-grid"><div><div class="header"><div class="section-title">最終判定別集計</div></div>{make_table(by_final_rank)}</div><div><div class="header"><div class="section-title">会場別集計</div></div>{make_table(by_venue)}</div></div>
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


def render_history_detail_page(race_date, races, summary, message_type="", message_text=""):
    if message_text:
        message_class = "message-success" if message_type == "success" else "message-error"
        message_html = f'<div class="message {message_class}">{message_text}</div>'
    else:
        message_html = ""
    if not races:
        body = '<div class="empty">データがありません</div>'
    else:
        cards_html = ''.join([build_card_html(r, is_history=True, race_date=race_date) for r in races])
        body = f'''
        <form id="bulk-delete-form" method="post" action="/delete_records_bulk" onsubmit="return confirmBulkDelete();"><input type="hidden" name="redirect_to" value="/history/{race_date}"></form>
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

    bottom_nav_html = f'''
    <nav class="bottom-nav">
      <a href="/" class="bottom-nav-item {home_active}"><span class="bottom-nav-icon">🏁</span><span class="bottom-nav-label">候補</span></a>
      <a href="/stats" class="bottom-nav-item {stats_active}"><span class="bottom-nav-icon">📊</span><span class="bottom-nav-label">集計</span></a>
      <a href="/history" class="bottom-nav-item {history_active}"><span class="bottom-nav-icon">🗂️</span><span class="bottom-nav-label">過去</span></a>
    </nav>
    '''
    return """<!doctype html><html lang=\"ja\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"><title>{}</title></head><body><div class=\"container\">{}</div>{}</body></html>""".format(title, body_html, bottom_nav_html)


def is_valid_import_token(req):
    sent = req.headers.get("X-IMPORT-TOKEN", "").strip()
    return bool(IMPORT_TOKEN) and sent == IMPORT_TOKEN


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


def replace_today_candidates(cleaned):
    if not cleaned:
        return {"inserted": 0, "updated": 0, "deleted": 0}

    race_date = str(cleaned[0]["race_date"]).strip()
    saved_map = get_saved_state_map_by_race(race_date)

    conn = db_connect()
    cur = conn.cursor()

    cur.execute("DELETE FROM races WHERE race_date = %s", (race_date,))
    deleted = cur.rowcount

    inserted = 0
    updated = 0
    imported_at = jst_now_str()

    for r in cleaned:
        key = (
            str(r["race_date"]).strip(),
            str(r["venue"]).strip(),
            str(r["race_no"]).strip(),
        )
        saved = saved_map.get(key, {})

        purchased = int(saved.get("purchased") or 0)
        purchased_selection_text = str(saved.get("purchased_selection_text") or "").strip()
        hit = int(saved.get("hit") or 0)
        payout = int(saved.get("payout") or 0)
        memo = str(saved.get("memo") or "").strip()

        cur.execute(
            '''
            INSERT INTO races (
                race_date, time, venue, race_no, race_no_num,
                rating, bet_type, selection, amount,
                ai_score, ai_rating, ai_label, final_rank,
                ai_reasons, exhibition, exhibition_rank, motor_rank,
                ai_detail, ai_selection, ai_confidence, ai_lane_score_text, class_history_text,
                purchased, purchased_selection_text, hit, payout, memo, imported_at
            )
            VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s
            )
            ''',
            (
                str(r["race_date"]).strip(),
                str(r["time"]).strip(),
                str(r["venue"]).strip(),
                str(r["race_no"]).strip(),
                int(r["race_no_num"]),
                str(r["rating"]).strip(),
                str(r["bet_type"]).strip(),
                str(r["selection"]).strip(),
                int(r["amount"]),
                safe_float(r.get("ai_score", 0), 0),
                str(r.get("ai_rating", "")).strip(),
                str(r.get("ai_label", "")).strip(),
                str(r.get("final_rank", "")).strip(),
                json.dumps(r.get("ai_reasons", []), ensure_ascii=False),
                json.dumps(r.get("exhibition", []), ensure_ascii=False),
                str(r.get("exhibition_rank", "")).strip(),
                str(r.get("motor_rank", "")).strip(),
                str(r.get("ai_detail", "")).strip(),
                str(r.get("ai_selection", "")).strip(),
                str(r.get("ai_confidence", "")).strip(),
                str(r.get("ai_lane_score_text", "")).strip(),
                str(r.get("class_history_text", "")).strip(),
                purchased,
                purchased_selection_text,
                hit,
                payout,
                memo,
                imported_at,
            )
        )

        inserted += 1
        if key in saved_map:
            updated += 1

    conn.commit()
    cur.close()
    conn.close()

    return {
        "inserted": inserted,
        "updated": updated,
        "deleted": deleted,
    }


@app.route("/healthz")
def healthz():
    return "ok", 200


@app.route("/reset_today")
def reset_today():
    delete_today_races()
    return redirect("/")


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
    official = [normalize_pick_text(x) for x in request.form.getlist("selected_official")]
    ai = [normalize_pick_text(x) for x in request.form.getlist("selected_ai")]
    return " / ".join(merge_selected_items(official, ai))


@app.route("/save", methods=["POST"])
def save():
    race_id = int(request.form.get("race_id", "0"))
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
        return redirect("/?type=error&msg=" + quote("的中にした場合は払戻額を入力してください"))
    update_race_result(race_id, selected_text, hit, payout, memo)
    return redirect("/?type=success&msg=" + quote("保存しました"))


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
    return render_stats_page(
        race_date,
        get_summary_by_date(race_date),
        get_group_summary(race_date, "rating"),
        get_group_summary(race_date, "venue"),
        get_group_summary(race_date, "ai_rating"),
        get_group_summary(race_date, "final_rank"),
    )


@app.route("/history")
def history():
    return render_history_page(get_history_date_summaries())


@app.route("/history/<race_date>")
def history_detail(race_date):
    return render_history_detail_page(
        race_date,
        get_races_by_date(race_date),
        get_summary_by_date(race_date),
        request.args.get("type", "").strip(),
        request.args.get("msg", "").strip(),
    )


@app.route("/api/import_candidates", methods=["POST"])
def import_candidates():
    if not is_valid_import_token(request):
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    data = request.get_json(silent=True) or {}
    races = data.get("races", [])
    if not isinstance(races, list):
        return jsonify({"ok": False, "error": "races must be a list"}), 400

    required_keys = {"race_date", "time", "venue", "race_no", "race_no_num", "rating", "bet_type", "selection", "amount"}
    cleaned = []
    for i, r in enumerate(races):
        if not isinstance(r, dict):
            return jsonify({"ok": False, "error": f"row {i} is not dict"}), 400
        missing = required_keys - set(r.keys())
        if missing:
            return jsonify({"ok": False, "error": f"row {i} missing keys: {sorted(list(missing))}"}), 400
        cleaned.append(
            {
                "race_date": str(r["race_date"]).strip(),
                "time": str(r["time"]).strip(),
                "venue": str(r["venue"]).strip(),
                "race_no": str(r["race_no"]).strip(),
                "race_no_num": int(r["race_no_num"]),
                "rating": str(r["rating"]).strip(),
                "bet_type": str(r["bet_type"]).strip(),
                "selection": str(r["selection"]).strip(),
                "amount": int(r["amount"]),
                "ai_score": safe_float(r.get("ai_score", 0), 0),
                "ai_rating": str(r.get("ai_rating", "")).strip(),
                "ai_label": str(r.get("ai_label", "")).strip(),
                "final_rank": str(r.get("final_rank", "")).strip(),
                "ai_reasons": r.get("ai_reasons", []),
                "exhibition": r.get("exhibition", []),
                "exhibition_rank": str(r.get("exhibition_rank", "")).strip(),
                "motor_rank": str(r.get("motor_rank", "")).strip(),
                "ai_detail": str(r.get("ai_detail", "")).strip(),
                "ai_selection": str(r.get("ai_selection", "")).strip(),
                "ai_confidence": str(r.get("ai_confidence", "")).strip(),
                "ai_lane_score_text": str(r.get("ai_lane_score_text", "")).strip(),
                "class_history_text": str(r.get("class_history_text", "")).strip(),
            }
        )
    if not cleaned:
        return jsonify({"ok": False, "error": "races is empty"}), 400

    race_dates = sorted(set(r["race_date"] for r in cleaned))
    if len(race_dates) != 1:
        return jsonify({"ok": False, "error": "multiple race_date values are not allowed"}), 400

    result = replace_today_candidates(cleaned)
    log(f"import api success count={len(cleaned)}")
    return jsonify(
        {
            "ok": True,
            "received": len(cleaned),
            "inserted": result["inserted"],
            "updated": result["updated"],
            "deleted": result["deleted"],
            "imported_at": jst_now_str(),
        }
    )


init_db()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
