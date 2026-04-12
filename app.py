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


def db_connect():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL が設定されていません")
    return psycopg2.connect(DATABASE_URL)


def get_point_count(selection):
    if not selection:
        return 0
    return len([x for x in str(selection).split(" / ") if x.strip()])


def get_total_amount(race):
    return int(race["amount"]) * get_point_count(race["selection"])


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

    if has_exhibition:
        return "展示反映"

    if not detail:
        return "基本補正のみ"

    ng_words = ["モーター反映", "展示反映"]
    if detail in ng_words:
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
            lane = int(a.strip())
            rank = int(b.strip())
            result[lane] = rank
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
        boxes += f"""
        <div class="{exhibition_rank_class(rank)}">
          <div class="ex-lane">{lane}号艇</div>
          <div class="ex-rank">{rank_display}</div>
        </div>
        """
    return f'<div class="ex-rank-grid">{boxes}</div>'


def render_exhibition_time_chips(exhibition_list):
    if not exhibition_list:
        return '<div class="ex-chip-empty">未取得</div>'

    chips = ""
    for i, t in enumerate(exhibition_list, start=1):
        chips += f"""
        <div class="ex-chip">
          <span class="ex-chip-lane">{i}</span>
          <span class="ex-chip-time">{t}</span>
        </div>
        """
    return f'<div class="ex-chip-wrap">{chips}</div>'


def render_selection_chips(selection_text):
    items = [x.strip() for x in str(selection_text or "").split(" / ") if x.strip()]
    if not items:
        return '<div class="selection-chip-empty">未取得</div>'

    chips = ""
    for item in items:
        chips += f'<div class="selection-chip">{item}</div>'
    return f'<div class="selection-chip-grid">{chips}</div>'


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
    if not s.startswith("/"):
        return default
    if s.startswith("//"):
        return default
    return s


def init_db():
    conn = db_connect()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS races (
            id SERIAL PRIMARY KEY,
            race_date TEXT NOT NULL,
            time TEXT NOT NULL,
            venue TEXT NOT NULL,
            race_no TEXT NOT NULL,
            race_no_num INTEGER NOT NULL,
            rating TEXT NOT NULL,
            bet_type TEXT NOT NULL,
            selection TEXT NOT NULL,
            amount INTEGER NOT NULL,
            purchased INTEGER DEFAULT 0,
            hit INTEGER DEFAULT 0,
            payout INTEGER DEFAULT 0,
            memo TEXT DEFAULT '',
            imported_at TEXT DEFAULT '',
            ai_score REAL DEFAULT 0,
            ai_rating TEXT DEFAULT '',
            ai_label TEXT DEFAULT '',
            final_rank TEXT DEFAULT '',
            ai_reasons TEXT DEFAULT '[]',
            exhibition TEXT DEFAULT '[]',
            exhibition_rank TEXT DEFAULT '',
            motor_rank TEXT DEFAULT '',
            ai_detail TEXT DEFAULT '',
            UNIQUE(race_date, venue, race_no, selection)
        )
        """
    )

    alter_sqls = [
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS imported_at TEXT DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS ai_score REAL DEFAULT 0",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS ai_rating TEXT DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS ai_label TEXT DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS final_rank TEXT DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS ai_reasons TEXT DEFAULT '[]'",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS exhibition TEXT DEFAULT '[]'",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS exhibition_rank TEXT DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS motor_rank TEXT DEFAULT ''",
        "ALTER TABLE races ADD COLUMN IF NOT EXISTS ai_detail TEXT DEFAULT ''",
    ]
    for sql in alter_sqls:
        cur.execute(sql)

    conn.commit()
    cur.close()
    conn.close()


def replace_today_candidates(races):
    if not races:
        log("replace_today_candidates: no races")
        return {"inserted": 0, "updated": 0, "deleted": 0}

    race_date = str(races[0]["race_date"]).strip()
    imported_at = jst_now_str()

    conn = db_connect()
    cur = conn.cursor()

    inserted = 0
    updated = 0

    current_keys = set()
    for r in races:
        key = (
            str(r["race_date"]).strip(),
            str(r["venue"]).strip(),
            str(r["race_no"]).strip(),
            str(r["selection"]).strip(),
        )
        current_keys.add(key)

    for r in races:
        cur.execute(
            """
            INSERT INTO races
            (
                race_date, time, venue, race_no, race_no_num, rating, bet_type, selection, amount,
                imported_at, ai_score, ai_rating, ai_label, final_rank, ai_reasons,
                exhibition, exhibition_rank, motor_rank, ai_detail
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (race_date, venue, race_no, selection)
            DO UPDATE SET
                time = EXCLUDED.time,
                race_no_num = EXCLUDED.race_no_num,
                rating = EXCLUDED.rating,
                bet_type = EXCLUDED.bet_type,
                amount = EXCLUDED.amount,
                imported_at = EXCLUDED.imported_at,
                ai_score = EXCLUDED.ai_score,
                ai_rating = EXCLUDED.ai_rating,
                ai_label = EXCLUDED.ai_label,
                final_rank = EXCLUDED.final_rank,
                ai_reasons = EXCLUDED.ai_reasons,
                exhibition = EXCLUDED.exhibition,
                exhibition_rank = EXCLUDED.exhibition_rank,
                motor_rank = EXCLUDED.motor_rank,
                ai_detail = EXCLUDED.ai_detail
            RETURNING xmax = 0 AS inserted_flag
            """,
            (
                r["race_date"],
                r["time"],
                r["venue"],
                r["race_no"],
                r["race_no_num"],
                r["rating"],
                r["bet_type"],
                r["selection"],
                r["amount"],
                imported_at,
                safe_float(r.get("ai_score", 0), 0),
                str(r.get("ai_rating", "")).strip(),
                str(r.get("ai_label", "")).strip(),
                str(r.get("final_rank", "")).strip(),
                json.dumps(r.get("ai_reasons", []), ensure_ascii=False),
                json.dumps(r.get("exhibition", []), ensure_ascii=False),
                str(r.get("exhibition_rank", "")).strip(),
                str(r.get("motor_rank", "")).strip(),
                str(r.get("ai_detail", "")).strip(),
            ),
        )
        row = cur.fetchone()
        if row and row[0]:
            inserted += 1
        else:
            updated += 1

    cur.execute(
        """
        SELECT race_date, venue, race_no, selection
        FROM races
        WHERE race_date = %s
          AND venue <> 'テスト会場'
        """,
        (race_date,),
    )
    existing_rows = cur.fetchall()

    delete_targets = []
    for row in existing_rows:
        key = (row[0], row[1], row[2], row[3])
        if key not in current_keys:
            delete_targets.append(key)

    deleted = 0
    for key in delete_targets:
        cur.execute(
            """
            DELETE FROM races
            WHERE race_date = %s
              AND venue = %s
              AND race_no = %s
              AND selection = %s
            """,
            key,
        )
        deleted += cur.rowcount

    conn.commit()
    cur.close()
    conn.close()

    log(
        f"replace_today_candidates race_date={race_date} inserted={inserted} updated={updated} deleted={deleted}"
    )
    return {"inserted": inserted, "updated": updated, "deleted": deleted}


def get_races_by_date(race_date):
    conn = db_connect()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        """
        SELECT *
        FROM races
        WHERE race_date = %s
          AND venue <> 'テスト会場'
        ORDER BY time ASC, venue ASC, race_no_num ASC
        """,
        (race_date,),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def get_race_by_id(race_id):
    conn = db_connect()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        """
        SELECT *
        FROM races
        WHERE id = %s
        """,
        (race_id,),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row


def get_today_races():
    return get_races_by_date(today_text())


def get_filtered_today_races(show_closed=False, ai_rating_filter=""):
    rows = get_today_races()
    rows = [r for r in rows if is_star5_only(r)]

    if ai_rating_filter:
        rows = [r for r in rows if str(r.get("ai_rating", "")).strip() == ai_rating_filter]

    if not show_closed:
        rows = [r for r in rows if is_not_started(r["time"])]

    return rows


def delete_today_races():
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        DELETE FROM races
        WHERE race_date = %s
        """,
        (today_text(),),
    )
    conn.commit()
    cur.close()
    conn.close()


def update_race_result(race_id, purchased, hit, payout, memo):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE races
        SET purchased = %s, hit = %s, payout = %s, memo = %s
        WHERE id = %s
        """,
        (purchased, hit, payout, memo, race_id),
    )
    conn.commit()
    cur.close()
    conn.close()
    log(
        f"update_race_result race_id={race_id} purchased={purchased} hit={hit} payout={payout} memo={memo}"
    )


def delete_race(race_id):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        DELETE FROM races
        WHERE id = %s
        """,
        (race_id,),
    )
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
    cur.execute(
        """
        DELETE FROM races
        WHERE id = ANY(%s)
        """,
        (race_ids,),
    )
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
        """
        SELECT
            COUNT(*) AS total_rows,
            COALESCE(SUM(CASE WHEN purchased = 1 THEN 1 ELSE 0 END), 0) AS total_bets,
            COALESCE(SUM(
                CASE
                    WHEN purchased = 1 THEN amount * COALESCE(array_length(string_to_array(selection, ' / '), 1), 0)
                    ELSE 0
                END
            ), 0) AS total_investment,
            COALESCE(SUM(CASE WHEN purchased = 1 THEN payout ELSE 0 END), 0) AS total_payout,
            COALESCE(SUM(CASE WHEN purchased = 1 AND hit = 1 THEN 1 ELSE 0 END), 0) AS total_hits,
            COALESCE(MAX(imported_at), '') AS last_imported_at
        FROM races
        WHERE race_date = %s
          AND venue <> 'テスト会場'
        """,
        (race_date,),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()

    total_rows = row["total_rows"] or 0
    total_bets = row["total_bets"] or 0
    total_investment = row["total_investment"] or 0
    total_payout = row["total_payout"] or 0
    total_hits = row["total_hits"] or 0
    last_imported_at = row["last_imported_at"] or ""

    total_profit = total_payout - total_investment
    hit_rate = round((total_hits / total_bets * 100), 1) if total_bets else 0
    roi = round((total_payout / total_investment * 100), 1) if total_investment else 0

    return {
        "total_rows": total_rows,
        "total_bets": total_bets,
        "total_investment": total_investment,
        "total_payout": total_payout,
        "total_profit": total_profit,
        "total_hits": total_hits,
        "hit_rate": hit_rate,
        "roi": roi,
        "last_imported_at": last_imported_at,
    }


def get_group_summary(race_date, group_key):
    if group_key not in {"rating", "venue", "ai_rating", "final_rank"}:
        return []

    conn = db_connect()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    query = f"""
        SELECT
            {group_key} AS group_name,
            COUNT(CASE WHEN purchased = 1 THEN 1 END) AS total_bets,
            COALESCE(SUM(CASE WHEN purchased = 1 AND hit = 1 THEN 1 ELSE 0 END), 0) AS total_hits,
            COALESCE(SUM(
                CASE
                    WHEN purchased = 1 THEN amount * COALESCE(array_length(string_to_array(selection, ' / '), 1), 0)
                    ELSE 0
                END
            ), 0) AS total_investment,
            COALESCE(SUM(CASE WHEN purchased = 1 THEN payout ELSE 0 END), 0) AS total_payout
        FROM races
        WHERE race_date = %s
          AND venue <> 'テスト会場'
        GROUP BY {group_key}
        ORDER BY {group_key} ASC
    """
    cur.execute(query, (race_date,))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    results = []
    for row in rows:
        total_bets = row["total_bets"] or 0
        total_hits = row["total_hits"] or 0
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
    cur.execute(
        """
        SELECT DISTINCT race_date
        FROM races
        WHERE venue <> 'テスト会場'
        ORDER BY race_date DESC
        """
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [row[0] for row in rows]


def get_history_date_summaries():
    dates = get_history_dates()
    results = []
    for d in dates:
        s = get_summary_by_date(d)
        results.append(
            {
                "race_date": d,
                "summary": s,
            }
        )
    return results


def render_home(races, summary, message_type="", message_text="", show_closed=False, ai_rating_filter=""):
    updated_str = summary["last_imported_at"] if summary["last_imported_at"] else "未更新"

    message_html = ""
    if message_text:
        css_class = "message-success" if message_type == "success" else "message-error"
        message_html = f'<div class="message {css_class}">{message_text}</div>'

    checked_show_closed = "checked" if show_closed else ""
    ai_rating_options_html = render_ai_rating_filter_options(ai_rating_filter)

    if not races:
        cards_html = '<div class="empty">条件に合う★★★★★候補はありません</div>'
    else:
        cards_html = ""
        for r in races:
            checked_purchased = "checked" if r["purchased"] == 1 else ""
            checked_hit = "checked" if r["hit"] == 1 else ""
            payout_value = r["payout"] if r["payout"] else ""
            memo_value = r["memo"] if r["memo"] else ""
            point_count = get_point_count(r["selection"])
            total_amount = get_total_amount(r)

            card_class = "card"
            if r["hit"] == 1:
                card_class += " card-hit"
            elif r["purchased"] == 1:
                card_class += " card-purchased"

            status_parts = []
            if r["purchased"] == 1:
                status_parts.append('<span class="status-badge status-badge-saved">購入済み</span>')
            if r["hit"] == 1:
                status_parts.append('<span class="status-badge status-badge-hit">的中</span>')
            if not is_not_started(r["time"]):
                status_parts.append('<span class="status-badge status-badge-closed">締切後</span>')

            status_html = ""
            if status_parts:
                status_html = f'<div class="status-wrap">{"".join(status_parts)}</div>'

            ai_reasons = parse_json_array_text(r.get("ai_reasons", "[]"))
            exhibition = parse_json_array_text(r.get("exhibition", "[]"))

            ai_reason_html = ""
            if ai_reasons:
                items = "".join([f"<li>{x}</li>" for x in ai_reasons])
                ai_reason_html = f"""
                <div class="row">
                  <span class="label">補正理由</span>
                  <span class="value text-left">
                    <ul class="reason-list">{items}</ul>
                  </span>
                </div>
                """

            exhibition_time_html = render_exhibition_time_chips(exhibition)
            exhibition_rank_html = render_exhibition_rank_boxes(r.get("exhibition_rank", ""))
            selection_html = render_selection_chips(r.get("selection", ""))
            ai_detail_text = normalize_ai_detail(r.get("ai_detail"), exhibition)
            ai_score_value = safe_float(r.get("ai_score"), 0)
            final_rank_html = final_rank_badge(r.get("final_rank"))

            cards_html += f"""
            <div class="{card_class}">
              <div class="card-top">
                <div class="time">{r['time']}</div>
                {status_html}
              </div>

              <div class="badge-row">
                <span class="rating">{display_text(r.get('rating'), '公式評価なし')}</span>
                <span class="ai-rating">{display_text(r.get('ai_rating'), 'AI評価なし')}</span>
                {final_rank_html}
              </div>

              <div class="info-box">
                <div class="row"><span class="label">会場・R</span><span class="value">{r['venue']} {r['race_no']}</span></div>
                <div class="row"><span class="label">券種</span><span class="value">{r['bet_type']}</span></div>
                <div class="row"><span class="label">買い目</span><span class="value">{selection_html}</span></div>
                <div class="row"><span class="label">点数</span><span class="value">{point_count}点</span></div>
                <div class="row"><span class="label">1点あたり</span><span class="value">{yen(r['amount'])}</span></div>
                <div class="row"><span class="label">合計金額</span><span class="value total-amount">{yen(total_amount)}</span></div>
                <div class="row"><span class="label">AI補正点</span><span class="value ai-score-value">{round(ai_score_value, 2)}</span></div>
                <div class="row"><span class="label">展示タイム</span><span class="value">{exhibition_time_html}</span></div>
                <div class="row"><span class="label">展示順位</span><span class="value">{exhibition_rank_html}</span></div>
                <div class="row"><span class="label">詳細材料</span><span class="value">{ai_detail_text}</span></div>
                {ai_reason_html}
              </div>

              <form method="post" action="/save" class="form" data-race-id="{r['id']}">
                <input type="hidden" name="race_id" value="{r['id']}">

                <label class="checkline purchase-line">
                  <input
                    type="checkbox"
                    id="purchased-{r['id']}"
                    name="purchased"
                    value="1"
                    {checked_purchased}
                    onchange="toggleFormState('{r['id']}')"
                  >
                  このレースをまとめて買った
                </label>

                <div id="detail-{r['id']}" class="detail-box">
                  <label class="checkline">
                    <input
                      type="checkbox"
                      id="hit-{r['id']}"
                      name="hit"
                      value="1"
                      {checked_hit}
                      onchange="toggleFormState('{r['id']}')"
                    >
                    的中した
                  </label>

                  <div class="input-row">
                    <label>払戻額（レース全体の合計）</label>
                    <input
                      type="number"
                      id="payout-{r['id']}"
                      name="payout"
                      value="{payout_value}"
                      placeholder="例: 870"
                      min="0"
                    >
                  </div>

                  <div class="input-row">
                    <label>メモ</label>
                    <input type="text" name="memo" value="{memo_value}" placeholder="見送り、締切、様子見など">
                  </div>
                </div>

                <button type="submit" class="save-btn">保存</button>
              </form>
            </div>
            """

    external_line = ""
    if EXTERNAL_URL:
        external_line = f'<div class="sub"><strong>公開URL:</strong> <a href="{EXTERNAL_URL}">{EXTERNAL_URL}</a></div>'

    filter_status_text = "締切後も表示中" if show_closed else "締切前のみ表示中"
    filter_ai_text = ai_rating_filter if ai_rating_filter else "すべて"

    content = f"""
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
              <select name="ai_rating" id="ai_rating">
                {ai_rating_options_html}
              </select>
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
          <div class="summary-box">
            <div class="summary-label">表示中候補</div>
            <div class="summary-value">{len(races)}</div>
          </div>
          <div class="summary-box">
            <div class="summary-label">購入数</div>
            <div class="summary-value">{summary['total_bets']}</div>
          </div>
          <div class="summary-box">
            <div class="summary-label">収支</div>
            <div class="summary-value {profit_class(summary['total_profit'])}">{yen(summary['total_profit'])}</div>
          </div>
          <div class="summary-box">
            <div class="summary-label">回収率</div>
            <div class="summary-value">{percent(summary['roi'])}</div>
          </div>
        </div>
      </div>

      {cards_html}
    </div>
    """
    return render_layout("今日の買い候補", content)


def render_stats_page(race_date, summary, by_rating, by_venue, by_ai_rating, by_final_rank):
    def make_table(rows):
        if not rows:
            return '<div class="empty">データがありません</div>'

        body = ""
        for r in rows:
            body += f"""
            <tr>
              <td>{r['group_name']}</td>
              <td>{r['total_bets']}</td>
              <td>{r['total_hits']}</td>
              <td>{yen(r['total_investment'])}</td>
              <td>{yen(r['total_payout'])}</td>
              <td class="{profit_class(r['total_profit'])}">{yen(r['total_profit'])}</td>
              <td>{percent(r['hit_rate'])}</td>
              <td>{percent(r['roi'])}</td>
            </tr>
            """
        return f"""
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>区分</th>
                <th>購入</th>
                <th>的中</th>
                <th>投資</th>
                <th>払戻</th>
                <th>収支</th>
                <th>的中率</th>
                <th>回収率</th>
              </tr>
            </thead>
            <tbody>
              {body}
            </tbody>
          </table>
        </div>
        """

    content = f"""
    <div class="app-shell">
      <div class="topbar">
        <div class="brand">
          <div class="brand-logo">📊</div>
          <div>
            <div class="brand-title">Race Candidates</div>
            <div class="brand-sub">今日の集計</div>
          </div>
        </div>
        <div class="topbar-status">
          <span class="top-pill">対象日: {race_date}</span>
        </div>
      </div>

      <div class="header hero hero-strong">
        <div class="title">今日の集計</div>
        <div class="sub">対象日: {race_date}</div>
        <div class="sub">最終取込時刻: {summary['last_imported_at'] or '未更新'}</div>

        <div class="nav nav-app">
          <a href="/" class="nav-card">今日の候補</a>
          <a href="/stats" class="nav-card active">今日の集計</a>
          <a href="/history" class="nav-card">過去データ</a>
        </div>

        <div class="summary six">
          <div class="summary-box">
            <div class="summary-label">全候補数</div>
            <div class="summary-value">{summary['total_rows']}</div>
          </div>
          <div class="summary-box">
            <div class="summary-label">購入数</div>
            <div class="summary-value">{summary['total_bets']}</div>
          </div>
          <div class="summary-box">
            <div class="summary-label">的中数</div>
            <div class="summary-value">{summary['total_hits']}</div>
          </div>
          <div class="summary-box">
            <div class="summary-label">投資額</div>
            <div class="summary-value">{yen(summary['total_investment'])}</div>
          </div>
          <div class="summary-box">
            <div class="summary-label">払戻額</div>
            <div class="summary-value">{yen(summary['total_payout'])}</div>
          </div>
          <div class="summary-box">
            <div class="summary-label">収支</div>
            <div class="summary-value {profit_class(summary['total_profit'])}">{yen(summary['total_profit'])}</div>
          </div>
        </div>

        <div class="summary" style="margin-top:8px;">
          <div class="summary-box">
            <div class="summary-label">的中率</div>
            <div class="summary-value">{percent(summary['hit_rate'])}</div>
          </div>
          <div class="summary-box">
            <div class="summary-label">回収率</div>
            <div class="summary-value">{percent(summary['roi'])}</div>
          </div>
          <div class="summary-box">
            <div class="summary-label">1件あたり平均投資</div>
            <div class="summary-value">{yen(round(summary['total_investment'] / summary['total_bets']) if summary['total_bets'] else 0)}</div>
          </div>
          <div class="summary-box">
            <div class="summary-label">1件あたり平均払戻</div>
            <div class="summary-value">{yen(round(summary['total_payout'] / summary['total_hits']) if summary['total_hits'] else 0)}</div>
          </div>
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
          <div class="header"><div class="section-title">会場別集計</div></div>
          {make_table(by_venue)}
        </div>
      </div>
    </div>
    """
    return render_layout("今日の集計", content)


def render_history_page(date_summaries):
    if not date_summaries:
        list_html = '<div class="empty">過去データはありません</div>'
    else:
        items = ""
        for item in date_summaries:
            d = item["race_date"]
            s = item["summary"]
            items += f"""
            <div class="history-item">
              <div class="history-top">
                <div class="history-date">{d}</div>
                <a class="history-link" href="/history/{d}">結果を見る</a>
              </div>

              <div class="history-mini">
                <div class="history-mini-box">
                  <div class="history-mini-label">候補数</div>
                  <div class="history-mini-value">{s['total_rows']}</div>
                </div>
                <div class="history-mini-box">
                  <div class="history-mini-label">購入数</div>
                  <div class="history-mini-value">{s['total_bets']}</div>
                </div>
                <div class="history-mini-box">
                  <div class="history-mini-label">収支</div>
                  <div class="history-mini-value {profit_class(s['total_profit'])}">{yen(s['total_profit'])}</div>
                </div>
                <div class="history-mini-box">
                  <div class="history-mini-label">回収率</div>
                  <div class="history-mini-value">{percent(s['roi'])}</div>
                </div>
              </div>
            </div>
            """
        list_html = f'<div class="header"><div class="history-list">{items}</div></div>'

    content = f"""
    <div class="app-shell">
      <div class="topbar">
        <div class="brand">
          <div class="brand-logo">🗂️</div>
          <div>
            <div class="brand-title">Race Candidates</div>
            <div class="brand-sub">過去データ一覧</div>
          </div>
        </div>
      </div>

      <div class="header hero hero-strong">
        <div class="title">過去データ</div>
        <div class="nav nav-app">
          <a href="/" class="nav-card">今日の候補</a>
          <a href="/stats" class="nav-card">今日の集計</a>
          <a href="/history" class="nav-card active">過去データ</a>
        </div>
      </div>

      {list_html}
    </div>
    """
    return render_layout("過去データ", content)


def render_history_detail_page(race_date, races, summary, message_type="", message_text=""):
    message_html = ""
    if message_text:
        css_class = "message-success" if message_type == "success" else "message-error"
        message_html = f'<div class="message {css_class}">{message_text}</div>'

    if not races:
        body = '<div class="empty">データがありません</div>'
    else:
        cards_html = ""
        for r in races:
            checked_purchased = "checked" if r["purchased"] == 1 else ""
            checked_hit = "checked" if r["hit"] == 1 else ""
            payout_value = r["payout"] if r["payout"] else ""
            memo_value = r["memo"] if r["memo"] else ""
            point_count = get_point_count(r["selection"])
            total_amount = get_total_amount(r)

            exhibition = parse_json_array_text(r.get("exhibition", "[]"))
            exhibition_time_html = render_exhibition_time_chips(exhibition)
            exhibition_rank_html = render_exhibition_rank_boxes(r.get("exhibition_rank", ""))
            selection_html = render_selection_chips(r.get("selection", ""))
            ai_detail_text = normalize_ai_detail(r.get("ai_detail"), exhibition)
            ai_score_value = safe_float(r.get("ai_score"), 0)
            final_rank_html = final_rank_badge(r.get("final_rank"))
            redirect_to = f"/history/{race_date}"

            card_class = "card history-edit-card"
            if r["hit"] == 1:
                card_class += " card-hit"
            elif r["purchased"] == 1:
                card_class += " card-purchased"

            status_parts = []
            if r["purchased"] == 1:
                status_parts.append('<span class="status-badge status-badge-saved">購入済み</span>')
            if r["hit"] == 1:
                status_parts.append('<span class="status-badge status-badge-hit">的中</span>')
            status_html = f'<div class="status-wrap">{"".join(status_parts)}</div>' if status_parts else ""

            cards_html += f"""
            <div class="{card_class}">
              <div class="multi-check-wrap">
                <input
                  type="checkbox"
                  class="bulk-checkbox"
                  name="race_ids"
                  value="{r['id']}"
                  form="bulk-delete-form"
                  onchange="updateBulkDeleteCount()"
                >
              </div>

              <div class="card-top">
                <div>
                  <div class="time">{r['time']}</div>
                  <div class="sub history-subline">{r['venue']} {r['race_no']}</div>
                </div>
                {status_html}
              </div>

              <div class="badge-row">
                <span class="rating">{display_text(r.get('rating'), '公式評価なし')}</span>
                <span class="ai-rating">{display_text(r.get('ai_rating'), 'AI評価なし')}</span>
                {final_rank_html}
              </div>

              <div class="info-box">
                <div class="row"><span class="label">券種</span><span class="value">{r['bet_type']}</span></div>
                <div class="row"><span class="label">買い目</span><span class="value">{selection_html}</span></div>
                <div class="row"><span class="label">点数</span><span class="value">{point_count}点</span></div>
                <div class="row"><span class="label">合計金額</span><span class="value total-amount">{yen(total_amount)}</span></div>
                <div class="row"><span class="label">AI補正点</span><span class="value ai-score-value">{round(ai_score_value, 2)}</span></div>
                <div class="row"><span class="label">展示タイム</span><span class="value">{exhibition_time_html}</span></div>
                <div class="row"><span class="label">展示順位</span><span class="value">{exhibition_rank_html}</span></div>
                <div class="row"><span class="label">詳細材料</span><span class="value">{ai_detail_text}</span></div>
              </div>

              <form method="post" action="/update_record" class="form history-form" data-race-id="history-{r['id']}">
                <input type="hidden" name="race_id" value="{r['id']}">
                <input type="hidden" name="redirect_to" value="{redirect_to}">

                <label class="checkline purchase-line">
                  <input
                    type="checkbox"
                    id="purchased-history-{r['id']}"
                    name="purchased"
                    value="1"
                    {checked_purchased}
                    onchange="toggleFormState('history-{r['id']}')"
                  >
                  このレースを買った
                </label>

                <div id="detail-history-{r['id']}" class="detail-box">
                  <label class="checkline">
                    <input
                      type="checkbox"
                      id="hit-history-{r['id']}"
                      name="hit"
                      value="1"
                      {checked_hit}
                      onchange="toggleFormState('history-{r['id']}')"
                    >
                    的中した
                  </label>

                  <div class="input-row">
                    <label>払戻額</label>
                    <input
                      type="number"
                      id="payout-history-{r['id']}"
                      name="payout"
                      value="{payout_value}"
                      placeholder="例: 870"
                      min="0"
                    >
                  </div>

                  <div class="input-row">
                    <label>メモ</label>
                    <input type="text" name="memo" value="{memo_value}" placeholder="自由にメモ">
                  </div>
                </div>

                <div class="action-row">
                  <button type="submit" class="save-btn half-btn">保存</button>
                </div>
              </form>

              <form method="post" action="/delete_record" class="delete-form" onsubmit="return confirm('この過去データを削除しますか？');">
                <input type="hidden" name="race_id" value="{r['id']}">
                <input type="hidden" name="redirect_to" value="{redirect_to}">
                <button type="submit" class="delete-btn">この1件を削除</button>
              </form>
            </div>
            """

        body = f"""
        <form id="bulk-delete-form" method="post" action="/delete_records_bulk" onsubmit="return confirmBulkDelete();">
          <input type="hidden" name="redirect_to" value="/history/{race_date}">
        </form>

        <div class="bulk-toolbar">
          <div class="bulk-toolbar-left">
            <button type="button" class="toolbar-btn" onclick="toggleAllBulk(true)">全選択</button>
            <button type="button" class="toolbar-btn toolbar-btn-muted" onclick="toggleAllBulk(false)">選択解除</button>
          </div>
          <div class="bulk-toolbar-right">
            <span class="bulk-count" id="bulk-delete-count">0件選択中</span>
            <button type="submit" class="toolbar-delete-btn" form="bulk-delete-form">選択したものを削除</button>
          </div>
        </div>

        {cards_html}
        """

    content = f"""
    <div class="app-shell">
      <div class="topbar">
        <div class="brand">
          <div class="brand-logo">🧾</div>
          <div>
            <div class="brand-title">Race Candidates</div>
            <div class="brand-sub">過去データ詳細</div>
          </div>
        </div>
        <div class="topbar-status">
          <span class="top-pill">対象日: {race_date}</span>
        </div>
      </div>

      <div class="header hero hero-strong">
        <div class="title">過去データ詳細</div>
        <div class="sub">対象日: {race_date}</div>
        <div class="sub">最終取込時刻: {summary['last_imported_at'] or '未更新'}</div>
        {message_html}
        <div class="nav nav-app">
          <a href="/history" class="nav-card">過去データ一覧</a>
          <a href="/" class="nav-card">今日の候補</a>
          <a href="/history/{race_date}" class="nav-card active">この日の詳細</a>
        </div>
        <div class="summary">
          <div class="summary-box">
            <div class="summary-label">候補数</div>
            <div class="summary-value">{summary['total_rows']}</div>
          </div>
          <div class="summary-box">
            <div class="summary-label">購入数</div>
            <div class="summary-value">{summary['total_bets']}</div>
          </div>
          <div class="summary-box">
            <div class="summary-label">収支</div>
            <div class="summary-value {profit_class(summary['total_profit'])}">{yen(summary['total_profit'])}</div>
          </div>
          <div class="summary-box">
            <div class="summary-label">回収率</div>
            <div class="summary-value">{percent(summary['roi'])}</div>
          </div>
        </div>
      </div>

      {body}
    </div>
    """
    return render_layout("過去データ詳細", content)


def render_layout(title, body_html):
    home_active = "active" if title == "今日の買い候補" else ""
    stats_active = "active" if title == "今日の集計" else ""
    history_active = "active" if title in ["過去データ", "過去データ詳細"] else ""

    bottom_nav_html = f"""
    <nav class="bottom-nav">
      <a href="/" class="bottom-nav-item {home_active}">
        <span class="bottom-nav-icon">🏁</span>
        <span class="bottom-nav-label">候補</span>
      </a>
      <a href="/stats" class="bottom-nav-item {stats_active}">
        <span class="bottom-nav-icon">📊</span>
        <span class="bottom-nav-label">集計</span>
      </a>
      <a href="/history" class="bottom-nav-item {history_active}">
        <span class="bottom-nav-icon">🗂️</span>
        <span class="bottom-nav-label">過去</span>
      </a>
    </nav>
    """

    return f"""
    <!doctype html>
    <html lang="ja">
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <title>{title}</title>
      <style>
        * {{
          box-sizing: border-box;
        }}

        body {{
          margin: 0;
          background:
            radial-gradient(circle at top left, rgba(59,130,246,0.12), transparent 22%),
            radial-gradient(circle at top right, rgba(14,165,233,0.10), transparent 18%),
            linear-gradient(180deg, #eef4ff 0%, #f7faff 42%, #f3f6fb 100%);
          color: #172033;
          font-family: -apple-system, BlinkMacSystemFont, "Hiragino Sans", "Yu Gothic", sans-serif;
          line-height: 1.5;
        }}

        a {{
          color: #2563eb;
          text-decoration: none;
        }}

        .container {{
          max-width: 1020px;
          margin: 0 auto;
          padding: 16px 16px 92px;
        }}

        .app-shell {{
          display: flex;
          flex-direction: column;
          gap: 16px;
        }}

        .topbar {{
          display: flex;
          justify-content: space-between;
          align-items: center;
          gap: 12px;
          padding: 14px 18px;
          border-radius: 22px;
          background: linear-gradient(135deg, #0f172a 0%, #1d4ed8 52%, #38bdf8 100%);
          color: #ffffff;
          box-shadow: 0 20px 44px rgba(29,78,216,0.22);
        }}

        .brand {{
          display: flex;
          align-items: center;
          gap: 12px;
        }}

        .brand-logo {{
          width: 46px;
          height: 46px;
          border-radius: 14px;
          display: flex;
          align-items: center;
          justify-content: center;
          background: rgba(255,255,255,0.16);
          backdrop-filter: blur(8px);
          font-size: 24px;
        }}

        .brand-title {{
          font-size: 18px;
          font-weight: 900;
          line-height: 1.1;
        }}

        .brand-sub {{
          font-size: 12px;
          opacity: 0.86;
          margin-top: 3px;
        }}

        .topbar-status {{
          display: flex;
          align-items: center;
          gap: 8px;
          flex-wrap: wrap;
        }}

        .top-pill {{
          display: inline-flex;
          align-items: center;
          padding: 8px 12px;
          border-radius: 999px;
          background: rgba(255,255,255,0.16);
          border: 1px solid rgba(255,255,255,0.18);
          font-size: 12px;
          font-weight: 900;
          backdrop-filter: blur(8px);
        }}

        .header {{
          background: rgba(255,255,255,0.88);
          backdrop-filter: blur(10px);
          border: 1px solid rgba(255,255,255,0.68);
          border-radius: 24px;
          padding: 20px;
          box-shadow: 0 14px 36px rgba(15, 23, 42, 0.08);
          margin-bottom: 0;
        }}

        .hero {{
          background:
            linear-gradient(180deg, rgba(255,255,255,0.98), rgba(255,255,255,0.90));
        }}

        .hero-strong {{
          position: relative;
          overflow: hidden;
        }}

        .hero-strong::before {{
          content: "";
          position: absolute;
          inset: 0 0 auto 0;
          height: 5px;
          background: linear-gradient(90deg, #2563eb, #38bdf8, #6366f1);
        }}

        .title {{
          font-size: 30px;
          font-weight: 900;
          margin-bottom: 8px;
          letter-spacing: 0.01em;
          color: #0f172a;
        }}

        .sub {{
          font-size: 13px;
          color: #64748b;
          margin-top: 4px;
        }}

        .history-subline {{
          margin-top: 6px;
        }}

        .filter-box {{
          margin-top: 16px;
          background: linear-gradient(180deg, #ffffff 0%, #f9fbff 100%);
          border: 1px solid #dce7f7;
          border-radius: 18px;
          padding: 14px;
          box-shadow: inset 0 1px 0 rgba(255,255,255,0.9);
        }}

        .filter-grid {{
          display: grid;
          grid-template-columns: 1.3fr 1fr auto;
          gap: 12px;
          align-items: end;
        }}

        .filter-item {{
          display: flex;
          flex-direction: column;
          gap: 6px;
        }}

        .filter-item-wide {{
          justify-content: center;
        }}

        .filter-item label {{
          font-size: 13px;
          color: #64748b;
          font-weight: 800;
        }}

        .filter-check {{
          display: inline-flex;
          align-items: center;
          gap: 8px;
          font-size: 14px;
          color: #0f172a;
          font-weight: 900;
        }}

        select {{
          width: 100%;
          padding: 11px 12px;
          border: 1px solid #cfd8e3;
          border-radius: 12px;
          font-size: 15px;
          background: #ffffff;
          color: #0f172a;
        }}

        select:focus {{
          outline: none;
          border-color: #93c5fd;
          box-shadow: 0 0 0 4px rgba(147,197,253,0.18);
        }}

        .filter-actions {{
          display: flex;
          gap: 8px;
          align-items: center;
          flex-wrap: wrap;
        }}

        .filter-btn {{
          border: none;
          background: linear-gradient(180deg, #2563eb 0%, #1d4ed8 100%);
          color: #ffffff;
          border-radius: 12px;
          padding: 11px 14px;
          font-size: 14px;
          font-weight: 900;
          cursor: pointer;
          white-space: nowrap;
          box-shadow: 0 10px 18px rgba(37,99,235,0.16);
        }}

        .filter-reset {{
          display: inline-flex;
          align-items: center;
          justify-content: center;
          padding: 10px 12px;
          background: #f1f5f9;
          border: 1px solid #dbe3ee;
          border-radius: 12px;
          color: #334155;
          font-size: 14px;
          font-weight: 900;
          white-space: nowrap;
        }}

        .nav {{
          display: flex;
          gap: 10px;
          flex-wrap: wrap;
          margin-top: 16px;
        }}

        .nav-app {{
          gap: 12px;
        }}

        .nav-card {{
          display: inline-flex;
          align-items: center;
          justify-content: center;
          min-width: 120px;
          padding: 12px 15px;
          background: linear-gradient(180deg, #f8fbff 0%, #eef4ff 100%);
          color: #2743b4;
          border-radius: 16px;
          font-size: 14px;
          font-weight: 900;
          border: 1px solid #d7e2ff;
          box-shadow: 0 8px 18px rgba(59,130,246,0.09);
        }}

        .nav-card.active {{
          background: linear-gradient(180deg, #2563eb 0%, #1d4ed8 100%);
          color: #ffffff;
          border-color: #2563eb;
        }}

        .summary {{
          display: grid;
          grid-template-columns: repeat(4, minmax(0, 1fr));
          gap: 12px;
          margin-top: 18px;
        }}

        .summary.six {{
          grid-template-columns: repeat(6, minmax(0, 1fr));
        }}

        .summary-box {{
          background: linear-gradient(180deg, #ffffff 0%, #f9fbff 100%);
          border: 1px solid #dce7f7;
          border-radius: 18px;
          padding: 14px;
          box-shadow:
            inset 0 1px 0 rgba(255,255,255,0.8),
            0 8px 18px rgba(15,23,42,0.04);
        }}

        .summary-label {{
          font-size: 12px;
          color: #64748b;
          margin-bottom: 6px;
        }}

        .summary-value {{
          font-size: 21px;
          font-weight: 900;
        }}

        .profit-plus {{
          color: #16a34a;
        }}

        .profit-minus {{
          color: #dc2626;
        }}

        .profit-zero {{
          color: #334155;
        }}

        .message {{
          margin-top: 12px;
          padding: 12px 14px;
          border-radius: 14px;
          font-size: 14px;
          font-weight: 800;
        }}

        .message-success {{
          background: #ecfdf5;
          color: #166534;
          border: 1px solid #bbf7d0;
        }}

        .message-error {{
          background: #fef2f2;
          color: #991b1b;
          border: 1px solid #fecaca;
        }}

        .empty {{
          background: rgba(255,255,255,0.92);
          border-radius: 20px;
          padding: 18px;
          color: #6b7280;
          box-shadow: 0 10px 28px rgba(15, 23, 42, 0.05);
          border: 1px solid #e5e7eb;
        }}

        .card {{
          background: linear-gradient(180deg, rgba(255,255,255,0.98) 0%, rgba(250,252,255,0.96) 100%);
          border-radius: 24px;
          padding: 18px;
          margin-bottom: 18px;
          box-shadow: 0 18px 42px rgba(15, 23, 42, 0.08);
          border: 1px solid #e4ebf5;
          position: relative;
          overflow: hidden;
        }}

        .card::before {{
          content: "";
          position: absolute;
          left: 0;
          top: 0;
          width: 100%;
          height: 5px;
          background: linear-gradient(90deg, #60a5fa, #818cf8);
          opacity: 0.35;
        }}

        .card-purchased {{
          border-color: #b8d4ff;
          background: linear-gradient(180deg, #fbfdff 0%, #f1f7ff 100%);
        }}

        .card-purchased::before {{
          opacity: 0.85;
        }}

        .card-hit {{
          border-color: #a7f3d0;
          background: linear-gradient(180deg, #f6fffa 0%, #ecfdf5 100%);
        }}

        .card-hit::before {{
          background: linear-gradient(90deg, #22c55e, #34d399);
          opacity: 0.95;
        }}

        .history-edit-card {{
          padding-top: 26px;
        }}

        .multi-check-wrap {{
          position: absolute;
          top: 12px;
          right: 14px;
          z-index: 2;
        }}

        .multi-check-wrap input[type="checkbox"] {{
          width: 20px;
          height: 20px;
        }}

        .card-top {{
          display: flex;
          justify-content: space-between;
          align-items: flex-start;
          gap: 12px;
          margin-bottom: 10px;
        }}

        .time {{
          font-size: 34px;
          font-weight: 900;
          line-height: 1.0;
          letter-spacing: 0.01em;
          color: #13294b;
        }}

        .badge-row {{
          display: flex;
          flex-wrap: wrap;
          gap: 8px;
          margin-bottom: 14px;
        }}

        .rating,
        .ai-rating,
        .final-rank {{
          display: inline-flex;
          align-items: center;
          padding: 7px 12px;
          border-radius: 999px;
          font-size: 12px;
          font-weight: 900;
          letter-spacing: 0.02em;
          border: 1px solid transparent;
        }}

        .rating {{
          background: linear-gradient(180deg, #fff4e8 0%, #ffedd5 100%);
          color: #c2410c;
          border-color: #fed7aa;
        }}

        .ai-rating {{
          background: linear-gradient(180deg, #eef4ff 0%, #dbeafe 100%);
          color: #1d4ed8;
          border-color: #bfdbfe;
        }}

        .final-rank-strong {{
          background: linear-gradient(180deg, #ecfdf5 0%, #dcfce7 100%);
          color: #166534;
          border-color: #bbf7d0;
        }}

        .final-rank-buy {{
          background: linear-gradient(180deg, #eff6ff 0%, #dbeafe 100%);
          color: #1d4ed8;
          border-color: #bfdbfe;
        }}

        .final-rank-watch {{
          background: linear-gradient(180deg, #fffbeb 0%, #fef3c7 100%);
          color: #92400e;
          border-color: #fde68a;
        }}

        .final-rank-skip {{
          background: linear-gradient(180deg, #f8fafc 0%, #f1f5f9 100%);
          color: #475569;
          border-color: #e2e8f0;
        }}

        .info-box {{
          background: linear-gradient(180deg, #ffffff 0%, #f9fbff 100%);
          border: 1px solid #e2e8f0;
          border-radius: 18px;
          padding: 12px 14px;
        }}

        .row {{
          display: grid;
          grid-template-columns: 112px 1fr;
          gap: 12px;
          padding: 10px 0;
          border-bottom: 1px solid #edf2f7;
        }}

        .row:last-child {{
          border-bottom: none;
        }}

        .label {{
          font-size: 13px;
          color: #64748b;
          font-weight: 800;
        }}

        .value {{
          font-size: 14px;
          font-weight: 800;
          white-space: pre-wrap;
          word-break: break-word;
        }}

        .text-left {{
          text-align: left;
        }}

        .selection-chip-grid {{
          display: grid;
          grid-template-columns: repeat(3, minmax(0, 1fr));
          gap: 8px;
          width: 100%;
          max-width: 430px;
        }}

        .selection-chip {{
          display: flex;
          align-items: center;
          justify-content: center;
          min-height: 42px;
          padding: 8px 10px;
          border-radius: 12px;
          background: linear-gradient(180deg, #eef4ff 0%, #dbeafe 100%);
          border: 1px solid #bfdbfe;
          color: #153eaf;
          font-size: 22px;
          font-weight: 900;
          line-height: 1.1;
          letter-spacing: 0.02em;
          box-shadow: inset 0 1px 0 rgba(255,255,255,0.85);
        }}

        .selection-chip-empty {{
          font-size: 13px;
          color: #6b7280;
        }}

        .total-amount {{
          font-size: 16px;
          color: #0f172a;
        }}

        .ai-score-value {{
          font-size: 16px;
          color: #1d4ed8;
        }}

        .reason-list {{
          margin: 0;
          padding-left: 18px;
        }}

        .status-wrap {{
          display: flex;
          gap: 8px;
          flex-wrap: wrap;
          justify-content: flex-end;
        }}

        .status-badge {{
          display: inline-flex;
          align-items: center;
          padding: 7px 11px;
          border-radius: 999px;
          font-size: 12px;
          font-weight: 900;
          box-shadow: 0 4px 10px rgba(15,23,42,0.06);
        }}

        .status-badge-saved {{
          background: linear-gradient(180deg, #eff6ff 0%, #dbeafe 100%);
          color: #1d4ed8;
          border: 1px solid #bfdbfe;
        }}

        .status-badge-hit {{
          background: linear-gradient(180deg, #ecfdf5 0%, #dcfce7 100%);
          color: #166534;
          border: 1px solid #bbf7d0;
        }}

        .status-badge-closed {{
          background: linear-gradient(180deg, #fff1f2 0%, #ffe4e6 100%);
          color: #be123c;
          border: 1px solid #fecdd3;
        }}

        .form {{
          margin-top: 14px;
          background: linear-gradient(180deg, #ffffff 0%, #fbfcff 100%);
          border: 1px solid #e2e8f0;
          border-radius: 16px;
          padding: 14px;
        }}

        .history-form {{
          margin-bottom: 10px;
        }}

        .purchase-line {{
          font-weight: 900;
          color: #0f172a;
        }}

        .checkline {{
          display: flex;
          align-items: center;
          gap: 8px;
          font-size: 14px;
          margin-bottom: 10px;
        }}

        .detail-box {{
          margin-top: 8px;
        }}

        .input-row {{
          margin-top: 10px;
        }}

        .input-row label {{
          display: block;
          font-size: 13px;
          color: #64748b;
          margin-bottom: 6px;
          font-weight: 800;
        }}

        input[type="number"],
        input[type="text"] {{
          width: 100%;
          padding: 11px 12px;
          border: 1px solid #cfd8e3;
          border-radius: 12px;
          font-size: 16px;
          background: #ffffff;
          color: #0f172a;
        }}

        input[type="number"]:focus,
        input[type="text"]:focus {{
          outline: none;
          border-color: #93c5fd;
          box-shadow: 0 0 0 4px rgba(147,197,253,0.18);
        }}

        .action-row {{
          display: flex;
          gap: 10px;
          margin-top: 12px;
        }}

        .save-btn {{
          width: 100%;
          margin-top: 12px;
          border: none;
          background: linear-gradient(180deg, #2563eb 0%, #1d4ed8 100%);
          color: #ffffff;
          border-radius: 14px;
          padding: 13px 14px;
          font-size: 15px;
          font-weight: 900;
          cursor: pointer;
          box-shadow: 0 10px 20px rgba(37,99,235,0.18);
        }}

        .save-btn:hover {{
          opacity: 0.94;
        }}

        .half-btn {{
          margin-top: 0;
        }}

        .delete-form {{
          margin-top: 0;
        }}

        .delete-btn {{
          width: 100%;
          border: none;
          background: linear-gradient(180deg, #ef4444 0%, #dc2626 100%);
          color: #ffffff;
          border-radius: 14px;
          padding: 13px 14px;
          font-size: 15px;
          font-weight: 900;
          cursor: pointer;
          box-shadow: 0 10px 20px rgba(220,38,38,0.14);
        }}

        .delete-btn:hover {{
          opacity: 0.94;
        }}

        .bulk-toolbar {{
          position: sticky;
          top: 10px;
          z-index: 20;
          display: flex;
          justify-content: space-between;
          align-items: center;
          gap: 12px;
          flex-wrap: wrap;
          margin-bottom: 14px;
          padding: 14px 16px;
          border-radius: 18px;
          background: rgba(255,255,255,0.92);
          backdrop-filter: blur(10px);
          border: 1px solid rgba(226,232,240,0.95);
          box-shadow: 0 14px 30px rgba(15,23,42,0.08);
        }}

        .bulk-toolbar-left,
        .bulk-toolbar-right {{
          display: flex;
          align-items: center;
          gap: 10px;
          flex-wrap: wrap;
        }}

        .toolbar-btn {{
          border: none;
          background: linear-gradient(180deg, #eef4ff 0%, #dbeafe 100%);
          color: #1d4ed8;
          border-radius: 12px;
          padding: 10px 13px;
          font-size: 13px;
          font-weight: 900;
          cursor: pointer;
          border: 1px solid #bfdbfe;
        }}

        .toolbar-btn-muted {{
          background: linear-gradient(180deg, #f8fafc 0%, #f1f5f9 100%);
          color: #475569;
          border-color: #e2e8f0;
        }}

        .bulk-count {{
          font-size: 13px;
          font-weight: 900;
          color: #334155;
        }}

        .toolbar-delete-btn {{
          border: none;
          background: linear-gradient(180deg, #ef4444 0%, #dc2626 100%);
          color: #ffffff;
          border-radius: 12px;
          padding: 11px 14px;
          font-size: 13px;
          font-weight: 900;
          cursor: pointer;
          box-shadow: 0 10px 20px rgba(220,38,38,0.14);
        }}

        .table-wrap {{
          overflow-x: auto;
          background: rgba(255,255,255,0.94);
          border-radius: 18px;
          box-shadow: 0 10px 28px rgba(15, 23, 42, 0.06);
          border: 1px solid #e5e7eb;
        }}

        table {{
          width: 100%;
          border-collapse: collapse;
          min-width: 720px;
        }}

        th, td {{
          padding: 12px 10px;
          border-bottom: 1px solid #e5e7eb;
          text-align: left;
          font-size: 14px;
          vertical-align: top;
        }}

        th {{
          background: #f8fafc;
          color: #475569;
          font-weight: 900;
        }}

        .stats-grid {{
          display: grid;
          grid-template-columns: 1fr 1fr;
          gap: 16px;
          margin-bottom: 16px;
        }}

        .section-title {{
          font-size: 18px;
          font-weight: 900;
        }}

        .history-list {{
          display: flex;
          flex-direction: column;
          gap: 12px;
        }}

        .history-item {{
          background: rgba(255,255,255,0.94);
          border: 1px solid #e5e7eb;
          border-radius: 20px;
          padding: 14px;
          box-shadow: 0 10px 24px rgba(15, 23, 42, 0.05);
        }}

        .history-top {{
          display: flex;
          align-items: center;
          justify-content: space-between;
          gap: 12px;
          margin-bottom: 12px;
        }}

        .history-date {{
          font-size: 18px;
          font-weight: 900;
        }}

        .history-link {{
          display: inline-block;
          padding: 9px 12px;
          border-radius: 12px;
          background: linear-gradient(180deg, #eef4ff 0%, #e5edff 100%);
          color: #3730a3;
          font-size: 13px;
          font-weight: 900;
          border: 1px solid #d7e2ff;
        }}

        .history-mini {{
          display: grid;
          grid-template-columns: repeat(4, minmax(0, 1fr));
          gap: 10px;
        }}

        .history-mini-box {{
          background: linear-gradient(180deg, #ffffff 0%, #f9fbff 100%);
          border: 1px solid #e2e8f0;
          border-radius: 14px;
          padding: 10px;
        }}

        .history-mini-label {{
          font-size: 12px;
          color: #64748b;
          margin-bottom: 6px;
        }}

        .history-mini-value {{
          font-size: 16px;
          font-weight: 900;
        }}

        .ex-rank-grid {{
          display: grid;
          grid-template-columns: repeat(6, minmax(0, 1fr));
          gap: 7px;
          width: 100%;
        }}

        .ex-rank-box {{
          border: 1px solid #d7dee8;
          border-radius: 12px;
          background: linear-gradient(180deg, #ffffff 0%, #f8fafc 100%);
          padding: 7px 4px;
          text-align: center;
          min-width: 0;
          box-shadow: inset 0 1px 0 rgba(255,255,255,0.75);
        }}

        .ex-rank-1 {{
          background: linear-gradient(180deg, #ecfdf5 0%, #dcfce7 100%);
          border-color: #86efac;
        }}

        .ex-rank-2 {{
          background: linear-gradient(180deg, #eff6ff 0%, #dbeafe 100%);
          border-color: #93c5fd;
        }}

        .ex-rank-3 {{
          background: linear-gradient(180deg, #fffbeb 0%, #fef3c7 100%);
          border-color: #fcd34d;
        }}

        .ex-rank-low {{
          background: linear-gradient(180deg, #f8fafc 0%, #f1f5f9 100%);
          color: #6b7280;
        }}

        .ex-lane {{
          font-size: 10px;
          color: #64748b;
          font-weight: 800;
          line-height: 1.1;
        }}

        .ex-rank {{
          font-size: 18px;
          font-weight: 900;
          line-height: 1.2;
          margin-top: 3px;
          color: #0f172a;
        }}

        .ex-rank-empty,
        .ex-chip-empty {{
          font-size: 13px;
          color: #6b7280;
        }}

        .ex-chip-wrap {{
          display: flex;
          flex-wrap: wrap;
          gap: 6px;
        }}

        .ex-chip {{
          display: inline-flex;
          align-items: center;
          gap: 6px;
          padding: 6px 9px;
          border-radius: 999px;
          background: linear-gradient(180deg, #ffffff 0%, #f8fafc 100%);
          border: 1px solid #d7dee8;
          font-size: 12px;
          font-weight: 800;
          box-shadow: inset 0 1px 0 rgba(255,255,255,0.8);
        }}

        .ex-chip-lane {{
          display: inline-flex;
          align-items: center;
          justify-content: center;
          width: 20px;
          height: 20px;
          border-radius: 999px;
          background: linear-gradient(180deg, #eff6ff 0%, #dbeafe 100%);
          color: #1d4ed8;
          font-size: 11px;
          font-weight: 900;
          border: 1px solid #bfdbfe;
        }}

        .ex-chip-time {{
          color: #0f172a;
        }}

        .bottom-nav {{
          position: fixed;
          left: 50%;
          bottom: 12px;
          transform: translateX(-50%);
          width: calc(100% - 24px);
          max-width: 560px;
          display: grid;
          grid-template-columns: repeat(3, 1fr);
          gap: 10px;
          padding: 10px;
          border-radius: 22px;
          background: rgba(255,255,255,0.92);
          backdrop-filter: blur(12px);
          border: 1px solid rgba(226,232,240,0.95);
          box-shadow: 0 18px 40px rgba(15,23,42,0.16);
          z-index: 999;
        }}

        .bottom-nav-item {{
          display: flex;
          flex-direction: column;
          align-items: center;
          justify-content: center;
          gap: 4px;
          min-height: 58px;
          border-radius: 16px;
          background: linear-gradient(180deg, #f8fbff 0%, #eef4ff 100%);
          color: #334155;
          font-size: 12px;
          font-weight: 900;
          border: 1px solid #dbe7fb;
          box-shadow: inset 0 1px 0 rgba(255,255,255,0.85);
        }}

        .bottom-nav-item.active {{
          background: linear-gradient(180deg, #2563eb 0%, #1d4ed8 100%);
          color: #ffffff;
          border-color: #2563eb;
          box-shadow: 0 12px 24px rgba(37,99,235,0.24);
        }}

        .bottom-nav-icon {{
          font-size: 18px;
          line-height: 1;
        }}

        .bottom-nav-label {{
          font-size: 12px;
          line-height: 1;
        }}

        @media (max-width: 820px) {{
          .summary,
          .summary.six,
          .history-mini,
          .stats-grid {{
            grid-template-columns: 1fr 1fr;
          }}

          .row {{
            grid-template-columns: 96px 1fr;
          }}

          .time {{
            font-size: 30px;
          }}

          .card-top {{
            flex-direction: column;
            align-items: flex-start;
          }}

          .status-wrap {{
            justify-content: flex-start;
          }}

          .selection-chip-grid {{
            grid-template-columns: repeat(2, minmax(0, 1fr));
            max-width: 320px;
          }}

          .filter-grid {{
            grid-template-columns: 1fr;
            align-items: stretch;
          }}

          .topbar {{
            flex-direction: column;
            align-items: flex-start;
          }}

          .bulk-toolbar {{
            top: 6px;
          }}
        }}

        @media (max-width: 560px) {{
          .container {{
            padding: 12px 12px 92px;
          }}

          .title {{
            font-size: 24px;
          }}

          .summary,
          .summary.six,
          .history-mini,
          .stats-grid {{
            grid-template-columns: 1fr;
          }}

          .row {{
            grid-template-columns: 1fr;
            gap: 4px;
          }}

          .time {{
            font-size: 28px;
          }}

          .ex-rank-grid {{
            grid-template-columns: repeat(3, minmax(0, 1fr));
          }}

          .selection-chip-grid {{
            grid-template-columns: repeat(2, minmax(0, 1fr));
            max-width: 100%;
          }}

          .selection-chip {{
            font-size: 20px;
            min-height: 40px;
          }}

          .bulk-toolbar {{
            flex-direction: column;
            align-items: stretch;
          }}

          .bulk-toolbar-left,
          .bulk-toolbar-right {{
            width: 100%;
            justify-content: space-between;
          }}
        }}
      </style>
    </head>
    <body>
      <div class="container">
        {body_html}
      </div>

      {bottom_nav_html}

      <script>
        function toggleFormState(raceId) {{
          const purchased = document.getElementById(`purchased-${{raceId}}`);
          const hit = document.getElementById(`hit-${{raceId}}`);
          const detail = document.getElementById(`detail-${{raceId}}`);
          const payout = document.getElementById(`payout-${{raceId}}`);

          if (!purchased || !detail) return;

          if (purchased.checked) {{
            detail.style.display = "block";
          }} else {{
            detail.style.display = "none";
            if (hit) hit.checked = false;
            if (payout) payout.value = "";
          }}
        }}

        function updateBulkDeleteCount() {{
          const checked = document.querySelectorAll(".bulk-checkbox:checked").length;
          const target = document.getElementById("bulk-delete-count");
          if (target) {{
            target.textContent = `${{checked}}件選択中`;
          }}
        }}

        function toggleAllBulk(checked) {{
          document.querySelectorAll(".bulk-checkbox").forEach(function(el) {{
            el.checked = checked;
          }});
          updateBulkDeleteCount();
        }}

        function confirmBulkDelete() {{
          const checked = document.querySelectorAll(".bulk-checkbox:checked").length;
          if (checked <= 0) {{
            alert("削除するデータを選んでください");
            return false;
          }}
          return confirm(`${{checked}}件を削除しますか？`);
        }}

        document.addEventListener("DOMContentLoaded", function() {{
          document.querySelectorAll("[data-race-id]").forEach(function(form) {{
            const raceId = form.getAttribute("data-race-id");
            toggleFormState(raceId);
          }});
          updateBulkDeleteCount();
        }});
      </script>
    </body>
    </html>
    """


def is_valid_import_token(req):
    sent = req.headers.get("X-IMPORT-TOKEN", "").strip()
    return bool(IMPORT_TOKEN) and sent == IMPORT_TOKEN


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
    message_type = request.args.get("type", "").strip()
    message_text = request.args.get("msg", "").strip()

    return render_home(
        races,
        summary,
        message_type,
        message_text,
        show_closed=show_closed,
        ai_rating_filter=ai_rating_filter,
    )


@app.route("/save", methods=["POST"])
def save():
    race_id = int(request.form.get("race_id", "0"))
    purchased = 1 if request.form.get("purchased") == "1" else 0
    hit = 1 if request.form.get("hit") == "1" else 0
    payout_raw = request.form.get("payout", "").strip()
    payout = int(payout_raw) if payout_raw else 0
    memo = request.form.get("memo", "").strip()

    if purchased == 0:
        hit = 0
        payout = 0

    if purchased == 1 and hit == 1 and payout <= 0:
        return redirect("/?type=error&msg=" + quote("的中にした場合は払戻額を入力してください"))

    update_race_result(race_id, purchased, hit, payout, memo)
    return redirect("/?type=success&msg=" + quote("保存しました"))


@app.route("/update_record", methods=["POST"])
def update_record():
    race_id = int(request.form.get("race_id", "0"))
    redirect_to = safe_redirect_path(request.form.get("redirect_to", "/history"), "/history")

    race = get_race_by_id(race_id)
    if not race:
        return redirect(redirect_to + ("&" if "?" in redirect_to else "?") + "type=error&msg=" + quote("データが見つかりません"))

    purchased = 1 if request.form.get("purchased") == "1" else 0
    hit = 1 if request.form.get("hit") == "1" else 0
    payout_raw = request.form.get("payout", "").strip()
    payout = int(payout_raw) if payout_raw else 0
    memo = request.form.get("memo", "").strip()

    if purchased == 0:
        hit = 0
        payout = 0

    if purchased == 1 and hit == 1 and payout <= 0:
        return redirect(redirect_to + ("&" if "?" in redirect_to else "?") + "type=error&msg=" + quote("的中にした場合は払戻額を入力してください"))

    update_race_result(race_id, purchased, hit, payout, memo)
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
    race_ids = request.form.getlist("race_ids")

    deleted = delete_races_bulk(race_ids)
    if deleted <= 0:
        return redirect(redirect_to + ("&" if "?" in redirect_to else "?") + "type=error&msg=" + quote("削除するデータを選んでください"))

    return redirect(redirect_to + ("&" if "?" in redirect_to else "?") + "type=success&msg=" + quote(f"{deleted}件削除しました"))


@app.route("/stats")
def stats():
    race_date = today_text()
    summary = get_summary_by_date(race_date)
    by_rating = get_group_summary(race_date, "rating")
    by_venue = get_group_summary(race_date, "venue")
    by_ai_rating = get_group_summary(race_date, "ai_rating")
    by_final_rank = get_group_summary(race_date, "final_rank")
    return render_stats_page(race_date, summary, by_rating, by_venue, by_ai_rating, by_final_rank)


@app.route("/history")
def history():
    date_summaries = get_history_date_summaries()
    return render_history_page(date_summaries)


@app.route("/history/<race_date>")
def history_detail(race_date):
    races = get_races_by_date(race_date)
    summary = get_summary_by_date(race_date)
    message_type = request.args.get("type", "").strip()
    message_text = request.args.get("msg", "").strip()
    return render_history_detail_page(race_date, races, summary, message_type, message_text)


@app.route("/api/import_candidates", methods=["POST"])
def import_candidates():
    if not is_valid_import_token(request):
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    races = data.get("races", [])

    if not isinstance(races, list):
        return jsonify({"ok": False, "error": "races must be a list"}), 400

    required_keys = {
        "race_date",
        "time",
        "venue",
        "race_no",
        "race_no_num",
        "rating",
        "bet_type",
        "selection",
        "amount",
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
