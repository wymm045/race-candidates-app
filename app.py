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


def final_rank_class(rank_text):
    rank = (rank_text or "").strip()
    if rank == "買い強め":
        return "final-rank final-rank-strong"
    if rank == "買い":
        return "final-rank final-rank-buy"
    if rank == "様子見":
        return "final-rank final-rank-watch"
    if rank == "見送り寄り":
        return "final-rank final-rank-skip"
    return "final-rank final-rank-watch"


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
        return {"inserted": 0, "updated": 0}

    race_date = str(races[0]["race_date"]).strip()
    imported_at = jst_now_str()

    conn = db_connect()
    cur = conn.cursor()

    inserted = 0
    updated = 0

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
                float(r.get("ai_score", 0)),
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

    conn.commit()
    cur.close()
    conn.close()

    log(f"replace_today_candidates race_date={race_date} inserted={inserted} updated={updated}")
    return {"inserted": inserted, "updated": updated}


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


def get_today_races():
    return get_races_by_date(today_text())


def get_visible_today_races():
    rows = get_today_races()
    return [r for r in rows if is_not_started(r["time"])]


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


def render_layout(title, content_html):
    return f"""<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta name="apple-mobile-web-app-capable" content="yes">
  <meta name="apple-mobile-web-app-status-bar-style" content="default">
  <meta name="apple-mobile-web-app-title" content="買い候補">
  <title>{title}</title>
  <style>
    body {{
      margin: 0;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: #f5f7fb;
      color: #1f2937;
    }}
    .container {{
      max-width: 960px;
      margin: 0 auto;
      padding: 12px;
    }}
    .header, .card, .table-wrap {{
      background: #fff;
      border-radius: 18px;
      padding: 14px;
      box-shadow: 0 8px 24px rgba(0,0,0,.08);
      margin-bottom: 10px;
    }}
    .card-purchased {{
      background: #eaf3ff;
      border: 2px solid #93c5fd;
    }}
    .card-hit {{
      background: #ecfdf5;
      border: 2px solid #4ade80;
      box-shadow: 0 10px 26px rgba(34,197,94,.15);
    }}
    .title {{
      font-size: 22px;
      font-weight: 700;
      margin: 0 0 8px;
    }}
    .section-title {{
      font-size: 18px;
      font-weight: 700;
      margin: 0;
    }}
    .sub {{
      font-size: 14px;
      color: #6b7280;
      line-height: 1.5;
      margin-bottom: 8px;
      word-break: break-all;
    }}
    .nav {{
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin-top: 10px;
    }}
    .nav a {{
      text-decoration: none;
      background: #111827;
      color: #fff;
      padding: 10px 14px;
      border-radius: 12px;
      font-size: 14px;
      font-weight: 700;
    }}
    .summary {{
      display: grid;
      grid-template-columns: repeat(4,1fr);
      gap: 8px;
      margin-top: 12px;
    }}
    .summary.six {{
      grid-template-columns: repeat(3,1fr);
    }}
    .summary-box {{
      background: #f9fafb;
      border-radius: 12px;
      padding: 10px;
      text-align: center;
    }}
    .summary-label {{
      font-size: 12px;
      color: #6b7280;
      margin-bottom: 2px;
    }}
    .summary-value {{
      font-size: 18px;
      font-weight: 700;
      line-height: 1.2;
    }}
    .time {{
      font-size: 18px;
      font-weight: 700;
      margin-bottom: 8px;
    }}
    .row {{
      display: grid;
      grid-template-columns: 84px 1fr;
      gap: 8px;
      margin: 4px 0;
      font-size: 14px;
      align-items: start;
    }}
    .label {{
      color: #6b7280;
      line-height: 1.45;
    }}
    .value {{
      text-align: right;
      line-height: 1.45;
      font-weight: 500;
    }}
    .selection-value {{
      text-align: right;
      line-height: 1.45;
      word-break: break-word;
      font-weight: 600;
      letter-spacing: .2px;
      white-space: pre-line;
    }}
    .rating {{
      display: inline-block;
      padding: 3px 10px;
      border-radius: 999px;
      background: #fef3c7;
      color: #92400e;
      font-weight: 700;
      font-size: 13px;
      margin-bottom: 8px;
      margin-right: 6px;
    }}
    .ai-rating {{
      display: inline-block;
      padding: 3px 10px;
      border-radius: 999px;
      background: #ede9fe;
      color: #6d28d9;
      font-weight: 700;
      font-size: 13px;
      margin-bottom: 8px;
      margin-right: 6px;
    }}
    .final-rank {{
      display: inline-block;
      padding: 3px 10px;
      border-radius: 999px;
      font-weight: 700;
      font-size: 13px;
      margin-bottom: 8px;
    }}
    .final-rank-strong {{
      background: #dbeafe;
      color: #1d4ed8;
    }}
    .final-rank-buy {{
      background: #ecfeff;
      color: #155e75;
    }}
    .final-rank-watch {{
      background: #f3f4f6;
      color: #4b5563;
    }}
    .final-rank-skip {{
      background: #fee2e2;
      color: #991b1b;
    }}
    .status-wrap {{
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin-top: 8px;
    }}
    .status-badge {{
      display: inline-block;
      padding: 5px 10px;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 700;
    }}
    .status-badge-saved {{
      background: #dbeafe;
      color: #1d4ed8;
    }}
    .status-badge-hit {{
      background: #dcfce7;
      color: #166534;
    }}
    .message {{
      border-radius: 14px;
      padding: 12px 14px;
      margin-bottom: 12px;
      font-size: 14px;
      line-height: 1.5;
    }}
    .message-success {{
      background: #dcfce7;
      color: #166534;
    }}
    .message-error {{
      background: #fee2e2;
      color: #991b1b;
    }}
    .info-box {{
      margin-top: 8px;
      padding-top: 8px;
      border-top: 1px solid #e5e7eb;
      display: grid;
      gap: 1px;
    }}
    .form {{
      margin-top: 10px;
      padding-top: 10px;
      border-top: 1px solid #e5e7eb;
      display: grid;
      gap: 8px;
    }}
    .checkline {{
      font-size: 15px;
      line-height: 1.4;
    }}
    .checkline input {{
      transform: scale(1.08);
      margin-right: 6px;
    }}
    .input-row {{
      display: grid;
      gap: 4px;
    }}
    .input-row label {{
      font-size: 13px;
      color: #6b7280;
    }}
    .input-row input {{
      border: 1px solid #d1d5db;
      border-radius: 10px;
      padding: 10px;
      font-size: 16px;
    }}
    .save-btn {{
      border: none;
      background: #2563eb;
      color: #fff;
      padding: 12px;
      border-radius: 12px;
      font-size: 15px;
      font-weight: 700;
    }}
    .detail-box {{
      background: rgba(255,255,255,.65);
      border-radius: 12px;
      padding: 10px;
      display: grid;
      gap: 8px;
    }}
    .empty {{
      background: #fff;
      border-radius: 18px;
      padding: 24px;
      text-align: center;
      box-shadow: 0 8px 24px rgba(0,0,0,.08);
      color: #6b7280;
    }}
    .reason-list {{
      margin: 6px 0 0;
      padding-left: 18px;
      color: #4b5563;
      font-size: 13px;
      line-height: 1.45;
    }}
    .reason-list li {{
      margin-bottom: 2px;
    }}
    .stats-grid {{
      display: grid;
      grid-template-columns: repeat(2,1fr);
      gap: 10px;
    }}
    .history-list {{
      display: grid;
      gap: 10px;
    }}
    .history-item {{
      display: grid;
      gap: 8px;
      padding: 12px;
      border: 1px solid #e5e7eb;
      border-radius: 12px;
      background: #fff;
    }}
    .history-top {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 10px;
    }}
    .history-date {{
      font-weight: 700;
      font-size: 16px;
    }}
    .history-link {{
      text-decoration: none;
      color: #2563eb;
      font-weight: 700;
    }}
    .history-mini {{
      display: grid;
      grid-template-columns: repeat(4,1fr);
      gap: 8px;
    }}
    .history-mini-box {{
      background: #f9fafb;
      border-radius: 10px;
      padding: 8px;
      text-align: center;
    }}
    .history-mini-label {{
      font-size: 11px;
      color: #6b7280;
      margin-bottom: 2px;
    }}
    .history-mini-value {{
      font-size: 14px;
      font-weight: 700;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
    }}
    th, td {{
      border-bottom: 1px solid #e5e7eb;
      padding: 10px 8px;
      text-align: left;
      vertical-align: top;
    }}
    th {{
      color: #6b7280;
      font-weight: 700;
      background: #f9fafb;
      position: sticky;
      top: 0;
    }}
    .profit-plus {{
      color: #166534;
      font-weight: 700;
    }}
    .profit-minus {{
      color: #991b1b;
      font-weight: 700;
    }}
    .profit-zero {{
      color: #374151;
      font-weight: 700;
    }}
    @media (max-width: 720px) {{
      .container {{
        padding: 10px;
      }}
      .header, .card, .table-wrap {{
        padding: 12px;
        border-radius: 16px;
      }}
      .title {{
        font-size: 20px;
      }}
      .summary {{
        grid-template-columns: repeat(2,1fr);
      }}
      .summary.six {{
        grid-template-columns: repeat(2,1fr);
      }}
      .stats-grid {{
        grid-template-columns: 1fr;
      }}
      .history-mini {{
        grid-template-columns: repeat(2,1fr);
      }}
      table {{
        font-size: 12px;
      }}
      .row {{
        grid-template-columns: 78px 1fr;
        gap: 6px;
        margin: 3px 0;
      }}
      .time {{
        font-size: 16px;
      }}
      .summary-value {{
        font-size: 16px;
      }}
      .selection-value {{
        line-height: 1.3;
      }}
    }}
  </style>
</head>
<body>
  <div class="container">
    {content_html}
  </div>

  <script>
    function toggleFormState(raceId) {{
      const purchased = document.getElementById(`purchased-${{raceId}}`);
      const hit = document.getElementById(`hit-${{raceId}}`);
      const payout = document.getElementById(`payout-${{raceId}}`);
      const detail = document.getElementById(`detail-${{raceId}}`);

      if (!purchased || !hit || !payout || !detail) return;

      if (purchased.checked) {{
        detail.style.display = "grid";
        hit.disabled = false;
        payout.disabled = !hit.checked;
      }} else {{
        detail.style.display = "none";
        hit.checked = false;
        hit.disabled = true;
        payout.value = "";
        payout.disabled = true;
      }}

      if (hit.checked && purchased.checked) {{
        payout.disabled = false;
      }} else {{
        payout.disabled = true;
        if (!hit.checked) {{
          payout.value = "";
        }}
      }}
    }}

    document.addEventListener("DOMContentLoaded", function() {{
      document.querySelectorAll("[data-race-id]").forEach(function(el) {{
        toggleFormState(el.getAttribute("data-race-id"));
      }});
    }});
  </script>
</body>
</html>"""


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


def render_home(races, summary, message_type="", message_text=""):
    updated_str = summary["last_imported_at"] if summary["last_imported_at"] else "未更新"

    message_html = ""
    if message_text:
        css_class = "message-success" if message_type == "success" else "message-error"
        message_html = f'<div class="message {css_class}">{message_text}</div>'

    if not races:
        cards_html = '<div class="empty">締切前の条件に合うレースはありません</div>'
    else:
        cards_html = ""
        for r in races:
            checked_purchased = "checked" if r["purchased"] == 1 else ""
            checked_hit = "checked" if r["hit"] == 1 else ""
            payout_value = r["payout"] if r["payout"] else ""
            memo_value = r["memo"] if r["memo"] else ""
            selection_html = r["selection"].replace(" / ", "\n")
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
                  <span class="value" style="text-align:left;">
                    <ul class="reason-list">{items}</ul>
                  </span>
                </div>
                """

            exhibition_text = " / ".join(exhibition) if exhibition else "未取得"
            exhibition_rank_text = display_text(r.get("exhibition_rank"), "未取得")
            ai_detail_text = normalize_ai_detail(r.get("ai_detail"), exhibition)
            ai_score_text = r.get("ai_score") if r.get("ai_score") is not None else 0

            cards_html += f"""
            <div class="{card_class}">
              <div class="time">{r['time']}</div>
              <div>
                <span class="rating">{r.get('rating') or '公式評価なし'}</span>
                <span class="ai-rating">{display_text(r.get('ai_rating'), 'AI評価なし')}</span>
                <span class="{final_rank_class(r.get('final_rank'))}">{display_text(r.get('final_rank'), '判定なし')}</span>
              </div>

              <div class="info-box">
                <div class="row"><span class="label">会場・R</span><span class="value">{r['venue']} {r['race_no']}</span></div>
                <div class="row"><span class="label">券種</span><span class="value">{r['bet_type']}</span></div>
                <div class="row"><span class="label">買い目</span><span class="selection-value">{selection_html}</span></div>
                <div class="row"><span class="label">点数</span><span class="value">{point_count}点</span></div>
                <div class="row"><span class="label">1点あたり</span><span class="value">{yen(r['amount'])}</span></div>
                <div class="row"><span class="label">合計金額</span><span class="value">{yen(total_amount)}</span></div>
                <div class="row"><span class="label">AI補正点</span><span class="value">{round(float(ai_score_text), 2)}</span></div>
                <div class="row"><span class="label">展示</span><span class="value">{exhibition_text}</span></div>
                <div class="row"><span class="label">展示順位</span><span class="value">{exhibition_rank_text}</span></div>
                <div class="row"><span class="label">詳細材料</span><span class="value">{ai_detail_text}</span></div>
                {ai_reason_html}
              </div>

              {status_html}

              <form method="post" action="/save" class="form" data-race-id="{r['id']}">
                <input type="hidden" name="race_id" value="{r['id']}">

                <label class="checkline">
                  <input
                    type="checkbox"
                    id="purchased-{r['id']}"
                    name="purchased"
                    value="1"
                    {checked_purchased}
                    onchange="toggleFormState('{r['id']}')"
                  >
                  このレースを4点まとめて買った
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

    content = f"""
    <div class="header">
      <div class="title">今日の買い候補</div>
      <div class="sub">評価：★★★★☆・★★★★★ / 券種：2連単 / 1点100円 / 1レース4点 / 締切予定時刻が早い順</div>
      <div class="sub">最終取込時刻: {updated_str}</div>
      {external_line}
      {message_html}

      <div class="nav">
        <a href="/">今日の候補</a>
        <a href="/stats">今日の集計</a>
        <a href="/history">過去データ</a>
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
          <div class="summary-value" class="{profit_class(summary['total_profit'])}">{yen(summary['total_profit'])}</div>
        </div>
        <div class="summary-box">
          <div class="summary-label">回収率</div>
          <div class="summary-value">{percent(summary['roi'])}</div>
        </div>
      </div>
    </div>

    {cards_html}
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
    <div class="header">
      <div class="title">今日の集計</div>
      <div class="sub">対象日: {race_date}</div>
      <div class="sub">ルール: 1点100円 / 1レース4点買い</div>
      <div class="sub">最終取込時刻: {summary['last_imported_at'] or '未更新'}</div>

      <div class="nav">
        <a href="/">今日の候補</a>
        <a href="/history">過去データ</a>
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
    <div class="header">
      <div class="title">過去データ</div>
      <div class="nav">
        <a href="/">今日の候補</a>
        <a href="/stats">今日の集計</a>
      </div>
    </div>
    {list_html}
    """
    return render_layout("過去データ", content)


def render_history_detail_page(race_date, races, summary):
    if not races:
        body = '<div class="empty">データがありません</div>'
    else:
        rows_html = ""
        for r in races:
            point_count = get_point_count(r["selection"])
            total_amount = get_total_amount(r)
            exhibition = parse_json_array_text(r.get("exhibition", "[]"))
            exhibition_text = " / ".join(exhibition) if exhibition else "未取得"

            rows_html += f"""
            <tr>
              <td>{r['time']}</td>
              <td>{r['venue']}</td>
              <td>{r['race_no']}</td>
              <td>{display_text(r.get('rating'), '未設定')}</td>
              <td>{display_text(r.get('ai_rating'), '未設定')}</td>
              <td>{display_text(r.get('final_rank'), '未設定')}</td>
              <td>{r['selection']}</td>
              <td>{exhibition_text}</td>
              <td>{point_count}点 / {yen(total_amount)}</td>
              <td>{'買い' if r['purchased'] == 1 else '見送り'}</td>
              <td>{'的中' if r['hit'] == 1 else '-'}</td>
              <td>{yen(r['payout'])}</td>
              <td>{r['memo'] or ''}</td>
            </tr>
            """
        body = f"""
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>時刻</th>
                <th>会場</th>
                <th>R</th>
                <th>公式</th>
                <th>AI</th>
                <th>判定</th>
                <th>買い目</th>
                <th>展示</th>
                <th>点数/投資</th>
                <th>購入</th>
                <th>的中</th>
                <th>払戻</th>
                <th>メモ</th>
              </tr>
            </thead>
            <tbody>
              {rows_html}
            </tbody>
          </table>
        </div>
        """

    content = f"""
    <div class="header">
      <div class="title">過去データ詳細</div>
      <div class="sub">対象日: {race_date}</div>
      <div class="sub">最終取込時刻: {summary['last_imported_at'] or '未更新'}</div>
      <div class="nav">
        <a href="/history">過去データ一覧</a>
        <a href="/">今日の候補</a>
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
    """
    return render_layout("過去データ詳細", content)


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
    races = get_visible_today_races()
    summary = get_summary_by_date(today_text())
    message_type = request.args.get("type", "").strip()
    message_text = request.args.get("msg", "").strip()
    return render_home(races, summary, message_type, message_text)


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
    return render_history_detail_page(race_date, races, summary)


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
                "ai_score": float(r.get("ai_score", 0)),
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
            "imported_at": jst_now_str(),
        }
    )


init_db()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
