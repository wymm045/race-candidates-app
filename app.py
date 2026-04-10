from datetime import datetime, timezone, timedelta
import os
import re

import psycopg2
import psycopg2.extras
import requests
from bs4 import BeautifulSoup
from flask import Flask, request, redirect

app = Flask(__name__)

BASE_URL = "https://demedas.kyotei24.jp"
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
EXTERNAL_URL = os.environ.get("EXTERNAL_URL", "").strip()

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

VALID_RATINGS = {"★★★★☆", "★★★★★"}
BET_TYPE = "2連単"
BET_AMOUNT = 100

VENUES = [
    ("toda", "戸田"),
    ("hamanako", "浜名湖"),
    ("gamagori", "蒲郡"),
    ("tokoname", "常滑"),
    ("mikuni", "三国"),
    ("biwako", "びわこ"),
    ("marugame", "丸亀"),
    ("miyajima", "宮島"),
    ("karatsu", "唐津"),
    ("omura", "大村"),
    ("heiwajima", "平和島"),
    ("edogawa", "江戸川"),
    ("tamagawa", "多摩川"),
    ("tsu", "津"),
    ("suminoe", "住之江"),
    ("amagasaki", "尼崎"),
    ("naruto", "鳴門"),
    ("ashiya", "芦屋"),
    ("fukuoka", "福岡"),
    ("wakamatsu", "若松"),
]


def log(msg):
    print(f"[DEBUG] {msg}", flush=True)


def jst_now():
    return datetime.now(timezone(timedelta(hours=9)))


def db_connect():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL が設定されていません")
    return psycopg2.connect(DATABASE_URL)


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
            UNIQUE(race_date, venue, race_no, selection)
        )
        """
    )
    conn.commit()
    cur.close()
    conn.close()


def fetch_html(url):
    log(f"fetch_html start: {url}")
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    r.encoding = r.apparent_encoding
    log(f"fetch_html ok: {url} status={r.status_code} len={len(r.text)}")
    return r.text


def today_str():
    return jst_now().strftime("%Y%m%d")


def today_text():
    return jst_now().strftime("%Y-%m-%d")


def now_minutes():
    now = jst_now()
    return now.hour * 60 + now.minute


def to_minutes(hhmm):
    h, m = map(int, hhmm.split(":"))
    return h * 60 + m


def normalize_lines(html):
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n")
    lines = [line.strip() for line in text.splitlines()]
    return [line for line in lines if line]


def parse_official_deadlines(official_url):
    log(f"parse_official_deadlines start: {official_url}")
    html = fetch_html(official_url)
    lines = normalize_lines(html)
    deadlines = {}

    for i, line in enumerate(lines):
        if "締切予定時刻" in line:
            block = " ".join(lines[i:i + 8])
            times = re.findall(r"\d{2}:\d{2}", block)
            if times:
                for idx, t in enumerate(times, start=1):
                    deadlines[idx] = t
                break

    log(f"parse_official_deadlines result: count={len(deadlines)} values={deadlines}")
    return deadlines


def parse_venue_page(venue_slug, venue_name):
    url = f"{BASE_URL}/{venue_slug}/{today_str()}.html"
    log(f"parse_venue_page start: {venue_name} {url}")

    html = fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")
    lines = normalize_lines(html)

    official_url = None
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "boatrace.jp/owpc/pc/race/pcexpect" in href:
            official_url = href
            break

    if official_url:
        log(f"{venue_name} official_url found: {official_url}")
    else:
        log(f"{venue_name} official_url not found")

    rows = []
    i = 0
    while i < len(lines):
        line = lines[i]

        if re.fullmatch(r"\d{1,2}R", line):
            race_no = int(line[:-1])

            rating = lines[i + 1] if i + 1 < len(lines) else ""
            if rating not in {"★★★★★", "★★★★☆", "★★★☆☆", "★★☆☆☆", "★☆☆☆☆"}:
                i += 1
                continue

            confidence_idx = None
            for j in range(i + 1, min(i + 15, len(lines))):
                if "信頼度" in lines[j]:
                    confidence_idx = j
                    break

            if confidence_idx is None:
                log(f"{venue_name} {race_no}R confidence_idx not found")
                i += 1
                continue

            selection = ""
            search_block = lines[confidence_idx + 1: confidence_idx + 12]
            digits = [x for x in search_block if re.fullmatch(r"\d", x)]

            if len(digits) >= 5:
                selection = f"{digits[3]}-{digits[4]}"

            rows.append(
                {
                    "venue": venue_name,
                    "race_no": race_no,
                    "rating": rating,
                    "selection": selection,
                    "official_url": official_url,
                }
            )

        i += 1

    log(f"parse_venue_page result: {venue_name} rows={len(rows)}")
    preview = rows[:3]
    if preview:
        log(f"{venue_name} preview={preview}")
    return rows


def build_candidates():
    results = []
    now = jst_now()
    current_minutes = now_minutes()

    log("========== build_candidates start ==========")
    log(f"jst_now={now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    log(f"today_str={today_str()} today_text={today_text()} current_minutes={current_minutes}")
    log(f"venue_count={len(VENUES)}")

    for venue_slug, venue_name in VENUES:
        log(f"---- venue start: {venue_name} ----")
        try:
            rows = parse_venue_page(venue_slug, venue_name)
        except Exception as e:
            log(f"{venue_name} parse_venue_page error: {e}")
            continue

        if not rows:
            log(f"{venue_name} rows=0")
            continue

        official_url = rows[0].get("official_url")
        if not official_url:
            log(f"{venue_name} official_url missing")
            continue

        try:
            deadlines = parse_official_deadlines(official_url)
        except Exception as e:
            log(f"{venue_name} parse_official_deadlines error: {e}")
            continue

        if not deadlines:
            log(f"{venue_name} deadlines=0")
            continue

        venue_added = 0

        for row in rows:
            race_no = row["race_no"]
            rating = row["rating"]
            selection = row["selection"]
            t = deadlines.get(race_no)

            if rating not in VALID_RATINGS:
                log(f"{venue_name} {race_no}R skip: rating={rating}")
                continue

            if not selection:
                log(f"{venue_name} {race_no}R skip: selection empty")
                continue

            if not t:
                log(f"{venue_name} {race_no}R skip: deadline missing")
                continue

            if to_minutes(t) < current_minutes:
                log(f"{venue_name} {race_no}R skip: past deadline t={t}")
                continue

            candidate = {
                "race_date": today_text(),
                "time": t,
                "venue": row["venue"],
                "race_no": f'{row["race_no"]}R',
                "race_no_num": row["race_no"],
                "rating": row["rating"],
                "bet_type": BET_TYPE,
                "selection": row["selection"],
                "amount": BET_AMOUNT,
            }
            results.append(candidate)
            venue_added += 1
            log(f"{venue_name} {race_no}R add: {candidate}")

        log(f"{venue_name} added_count={venue_added}")

    results.sort(key=lambda x: (to_minutes(x["time"]), x["venue"], x["race_no_num"]))
    log(f"build_candidates final_count={len(results)}")
    if results:
        log(f"build_candidates preview={results[:10]}")
    log("========== build_candidates end ==========")
    return results


def upsert_candidates(races):
    if not races:
        log("upsert_candidates: no races")
        return

    conn = db_connect()
    cur = conn.cursor()

    inserted = 0
    ignored = 0

    for r in races:
        cur.execute(
            """
            INSERT INTO races
            (race_date, time, venue, race_no, race_no_num, rating, bet_type, selection, amount)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (race_date, venue, race_no, selection) DO NOTHING
            RETURNING id
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
            ),
        )
        row = cur.fetchone()
        if row:
            inserted += 1
        else:
            ignored += 1

    conn.commit()
    cur.close()
    conn.close()
    log(f"upsert_candidates inserted={inserted} ignored={ignored}")


def get_races_by_date(race_date):
    conn = db_connect()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        """
        SELECT *
        FROM races
        WHERE race_date = %s
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
    log(f"update_race_result race_id={race_id} purchased={purchased} hit={hit} payout={payout} memo={memo}")


def get_summary_by_date(race_date):
    conn = db_connect()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        """
        SELECT
            COUNT(*) AS total_rows,
            COALESCE(SUM(CASE WHEN purchased = 1 THEN 1 ELSE 0 END), 0) AS total_bets,
            COALESCE(SUM(CASE WHEN purchased = 1 THEN amount ELSE 0 END), 0) AS total_investment,
            COALESCE(SUM(CASE WHEN purchased = 1 THEN payout ELSE 0 END), 0) AS total_payout,
            COALESCE(SUM(CASE WHEN purchased = 1 AND hit = 1 THEN 1 ELSE 0 END), 0) AS total_hits
        FROM races
        WHERE race_date = %s
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
    }


def get_group_summary(race_date, group_key):
    if group_key not in {"rating", "venue"}:
        return []

    conn = db_connect()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    query = f"""
        SELECT
            {group_key} AS group_name,
            COUNT(CASE WHEN purchased = 1 THEN 1 END) AS total_bets,
            COALESCE(SUM(CASE WHEN purchased = 1 AND hit = 1 THEN 1 ELSE 0 END), 0) AS total_hits,
            COALESCE(SUM(CASE WHEN purchased = 1 THEN amount ELSE 0 END), 0) AS total_investment,
            COALESCE(SUM(CASE WHEN purchased = 1 THEN payout ELSE 0 END), 0) AS total_payout
        FROM races
        WHERE race_date = %s
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
                "group_name": row["group_name"],
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
        ORDER BY race_date DESC
        """
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [row[0] for row in rows]


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
      max-width: 900px;
      margin: 0 auto;
      padding: 16px;
    }}
    .header, .card, .table-wrap {{
      background: #fff;
      border-radius: 18px;
      padding: 16px;
      box-shadow: 0 8px 24px rgba(0,0,0,.08);
      margin-bottom: 12px;
    }}
    .title {{
      font-size: 24px;
      font-weight: 700;
      margin: 0 0 8px;
    }}
    .sub {{
      font-size: 14px;
      color: #6b7280;
      line-height: 1.6;
      margin-bottom: 10px;
      word-break: break-all;
    }}
    .nav {{
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      margin-top: 12px;
    }}
    .nav a {{
      text-decoration: none;
      background: #111827;
      color: #fff;
      padding: 10px 14px;
      border-radius: 12px;
      font-size: 14px;
    }}
    .summary {{
      display: grid;
      grid-template-columns: repeat(4,1fr);
      gap: 10px;
      margin-top: 14px;
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
    }}
    .summary-value {{
      font-size: 20px;
      font-weight: 700;
    }}
    .time {{
      font-size: 24px;
      font-weight: 700;
      margin-bottom: 8px;
    }}
    .row {{
      display: flex;
      justify-content: space-between;
      gap: 8px;
      margin: 6px 0;
      font-size: 15px;
    }}
    .label {{
      color: #6b7280;
    }}
    .rating {{
      display: inline-block;
      padding: 4px 10px;
      border-radius: 999px;
      background: #fef3c7;
      color: #92400e;
      font-weight: 700;
      font-size: 14px;
      margin-bottom: 10px;
    }}
    .form {{
      margin-top: 14px;
      padding-top: 12px;
      border-top: 1px solid #e5e7eb;
      display: grid;
      gap: 10px;
    }}
    .checkline {{
      font-size: 15px;
    }}
    .input-row {{
      display: grid;
      gap: 6px;
    }}
    .input-row label {{
      font-size: 14px;
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
    .empty {{
      background: #fff;
      border-radius: 18px;
      padding: 24px;
      text-align: center;
      box-shadow: 0 8px 24px rgba(0,0,0,.08);
      color: #6b7280;
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
    }}
    .history-list {{
      display: grid;
      gap: 10px;
    }}
    .history-item {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 10px;
      padding: 12px;
      border: 1px solid #e5e7eb;
      border-radius: 12px;
    }}
    .history-item a {{
      text-decoration: none;
      color: #2563eb;
      font-weight: 700;
    }}
    @media (max-width: 720px) {{
      .summary {{
        grid-template-columns: repeat(2,1fr);
      }}
      table {{
        font-size: 12px;
      }}
    }}
  </style>
</head>
<body>
  <div class="container">
    {content_html}
  </div>
</body>
</html>"""


def render_home(races, summary):
    updated_str = jst_now().strftime("%H:%M")

    if not races:
        cards_html = '<div class="empty">条件に合うレースはありません</div>'
    else:
        cards_html = ""
        for r in races:
            checked_purchased = "checked" if r["purchased"] == 1 else ""
            checked_hit = "checked" if r["hit"] == 1 else ""
            payout_value = r["payout"] if r["payout"] else ""
            memo_value = r["memo"] if r["memo"] else ""

            cards_html += f"""
            <div class="card">
              <div class="time">{r['time']}</div>
              <div class="rating">{r['rating']}</div>
              <div class="row"><span class="label">会場・R</span><span>{r['venue']} {r['race_no']}</span></div>
              <div class="row"><span class="label">券種</span><span>{r['bet_type']}</span></div>
              <div class="row"><span class="label">買い目</span><span>{r['selection']}</span></div>
              <div class="row"><span class="label">金額</span><span>{r['amount']}円</span></div>

              <form method="post" action="/save" class="form">
                <input type="hidden" name="race_id" value="{r['id']}">

                <label class="checkline">
                  <input type="checkbox" name="purchased" value="1" {checked_purchased}>
                  買った
                </label>

                <label class="checkline">
                  <input type="checkbox" name="hit" value="1" {checked_hit}>
                  的中
                </label>

                <div class="input-row">
                  <label>払戻額</label>
                  <input type="number" name="payout" value="{payout_value}" placeholder="0">
                </div>

                <div class="input-row">
                  <label>メモ</label>
                  <input type="text" name="memo" value="{memo_value}" placeholder="見送りなど">
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
      <div class="sub">評価：★★★★☆・★★★★★ / 券種：2連単 / 各レース100円 / 今日これから始まるレース / 締切予定時刻が早い順</div>
      <div class="sub">更新時刻: {updated_str}</div>
      {external_line}

      <div class="nav">
        <a href="/refresh">候補を更新</a>
        <a href="/stats">今日の集計</a>
        <a href="/history">過去データ</a>
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
          <div class="summary-value">{summary['total_profit']}円</div>
        </div>
        <div class="summary-box">
          <div class="summary-label">回収率</div>
          <div class="summary-value">{summary['roi']}%</div>
        </div>
      </div>
    </div>

    {cards_html}
    """
    return render_layout("今日の買い候補", content)


def render_stats_page(race_date, summary, by_rating, by_venue):
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
              <td>{r['total_investment']}円</td>
              <td>{r['total_payout']}円</td>
              <td>{r['total_profit']}円</td>
              <td>{r['hit_rate']}%</td>
              <td>{r['roi']}%</td>
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
      <div class="title">集計</div>
      <div class="sub">対象日: {race_date}</div>
      <div class="nav">
        <a href="/">今日の候補</a>
        <a href="/history">過去データ</a>
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
          <div class="summary-value">{summary['total_profit']}円</div>
        </div>
        <div class="summary-box">
          <div class="summary-label">回収率</div>
          <div class="summary-value">{summary['roi']}%</div>
        </div>
      </div>
    </div>

    <div class="header"><div class="title">星別集計</div></div>
    {make_table(by_rating)}

    <div class="header"><div class="title">会場別集計</div></div>
    {make_table(by_venue)}
    """
    return render_layout("集計", content)


def render_history_page(dates):
    if not dates:
        list_html = '<div class="empty">過去データはありません</div>'
    else:
        items = ""
        for d in dates:
            items += f"""
            <div class="history-item">
              <div>{d}</div>
              <div><a href="/history/{d}">結果を見る</a></div>
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
            rows_html += f"""
            <tr>
              <td>{r['time']}</td>
              <td>{r['venue']}</td>
              <td>{r['race_no']}</td>
              <td>{r['rating']}</td>
              <td>{r['selection']}</td>
              <td>{'買い' if r['purchased'] == 1 else '見送り'}</td>
              <td>{'的中' if r['hit'] == 1 else '-'}</td>
              <td>{r['payout']}円</td>
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
                <th>評価</th>
                <th>買い目</th>
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
          <div class="summary-value">{summary['total_profit']}円</div>
        </div>
        <div class="summary-box">
          <div class="summary-label">回収率</div>
          <div class="summary-value">{summary['roi']}%</div>
        </div>
      </div>
    </div>
    {body}
    """
    return render_layout("過去データ詳細", content)


@app.route("/healthz")
def healthz():
    return "ok", 200


@app.route("/")
def index():
    races = get_today_races()
    summary = get_summary_by_date(today_text())
    return render_home(races, summary)


@app.route("/refresh")
def refresh():
    log("manual refresh called")
    races = build_candidates()
    upsert_candidates(races)
    return redirect("/")


@app.route("/save", methods=["POST"])
def save():
    race_id = int(request.form.get("race_id", "0"))
    purchased = 1 if request.form.get("purchased") == "1" else 0
    hit = 1 if request.form.get("hit") == "1" else 0
    payout_raw = request.form.get("payout", "").strip()
    payout = int(payout_raw) if payout_raw else 0
    memo = request.form.get("memo", "").strip()

    update_race_result(race_id, purchased, hit, payout, memo)
    return redirect("/")


@app.route("/stats")
def stats():
    race_date = today_text()
    summary = get_summary_by_date(race_date)
    by_rating = get_group_summary(race_date, "rating")
    by_venue = get_group_summary(race_date, "venue")
    return render_stats_page(race_date, summary, by_rating, by_venue)


@app.route("/history")
def history():
    dates = get_history_dates()
    return render_history_page(dates)


@app.route("/history/<race_date>")
def history_detail(race_date):
    races = get_races_by_date(race_date)
    summary = get_summary_by_date(race_date)
    return render_history_detail_page(race_date, races, summary)


init_db()


@app.before_request
def ensure_today_candidates():
    if request.path in {"/", "/stats"}:
        today_rows = get_today_races()
        log(f"before_request path={request.path} today_rows={len(today_rows)}")
        if not today_rows:
            log("today_rows is empty, auto build start")
            races = build_candidates()
            upsert_candidates(races)
            log("today_rows auto build end")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    log(f"app start port={port}")
    app.run(host="0.0.0.0", port=port, debug=False)
