
# app_freeze_closed_predictions_patch.py
#
# 使い方
# 1. app.py で `def get_saved_state_map_by_race(` を探す
#    → 下の `get_saved_state_map_by_race` に丸ごと置き換え
# 2. app.py に `def get_existing_row_map_by_race(` が無ければ追加
#    → 下の関数をそのまま追加
# 3. app.py で `def replace_today_candidates(` を探す
#    → 下の `replace_today_candidates` に丸ごと置き換え
#
# これで「締切後にAI予想が変わる問題」をかなり防げます。
# 締切後のレースは、前回保存されていた予想値を優先して保持します。
#
# 反映後は app.py を再デプロイ / 再起動してください。


def get_saved_state_map_by_race(race_date):
    conn = db_connect()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        '''
        SELECT
            id,
            race_date,
            venue,
            race_no,
            time,
            purchased,
            hit,
            payout,
            memo,
            purchased_selection_text
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
            "id": int(row.get("id") or 0),
            "time": str(row.get("time") or "").strip(),
            "purchased": int(row.get("purchased") or 0),
            "hit": int(row.get("hit") or 0),
            "payout": int(row.get("payout") or 0),
            "memo": str(row.get("memo") or "").strip(),
            "purchased_selection_text": str(row.get("purchased_selection_text") or "").strip(),
        }
    return saved_map


def get_existing_row_map_by_race(race_date):
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

    existing_map = {}
    for row in rows:
        key = (
            str(row["race_date"]).strip(),
            str(row["venue"]).strip(),
            str(row["race_no"]).strip(),
        )
        if key in existing_map:
            continue
        existing_map[key] = row
    return existing_map


def replace_today_candidates(cleaned):
    if not cleaned:
        return {"inserted": 0, "updated": 0, "deleted": 0, "frozen_closed": 0}

    race_date = str(cleaned[0]["race_date"]).strip()
    saved_map = get_saved_state_map_by_race(race_date)
    existing_map = get_existing_row_map_by_race(race_date)

    conn = db_connect()
    cur = conn.cursor()

    cur.execute("DELETE FROM races WHERE race_date = %s", (race_date,))
    deleted = cur.rowcount

    inserted = 0
    updated = 0
    frozen_closed = 0
    imported_at = jst_now_str()

    prediction_fields = [
        "time",
        "selection",
        "ai_score",
        "ai_rating",
        "ai_label",
        "final_rank",
        "ai_reasons",
        "exhibition",
        "exhibition_rank",
        "motor_rank",
        "ai_detail",
        "ai_selection",
        "ai_confidence",
        "ai_lane_score_text",
        "class_history_text",
        "player_names_text",
    ]

    for r in cleaned:
        key = (
            str(r["race_date"]).strip(),
            str(r["venue"]).strip(),
            str(r["race_no"]).strip(),
        )
        saved = saved_map.get(key, {})
        existing = existing_map.get(key, {})

        purchased = int(saved.get("purchased") or 0)
        purchased_selection_text = str(saved.get("purchased_selection_text") or "").strip()
        hit = int(saved.get("hit") or 0)
        payout = int(saved.get("payout") or 0)
        memo = str(saved.get("memo") or "").strip()

        old_time = str(existing.get("time") or saved.get("time") or "").strip()
        should_freeze_closed = bool(old_time) and (not is_not_started(old_time))

        row_data = {
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
            "player_names_text": str(r.get("player_names_text", "")).strip(),
        }

        if should_freeze_closed and existing:
            for field in prediction_fields:
                if field in existing:
                    row_data[field] = existing.get(field)
            frozen_closed += 1

        cur.execute(
            '''
            INSERT INTO races (
                race_date, time, venue, race_no, race_no_num,
                rating, bet_type, selection, amount,
                ai_score, ai_rating, ai_label, final_rank,
                ai_reasons, exhibition, exhibition_rank, motor_rank,
                ai_detail, ai_selection, ai_confidence, ai_lane_score_text, class_history_text,
                player_names_text,
                purchased, purchased_selection_text, hit, payout, memo, imported_at
            )
            VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s,
                %s, %s, %s, %s, %s, %s
            )
            ''',
            (
                row_data["race_date"],
                row_data["time"],
                row_data["venue"],
                row_data["race_no"],
                row_data["race_no_num"],
                row_data["rating"],
                row_data["bet_type"],
                row_data["selection"],
                row_data["amount"],
                row_data["ai_score"],
                row_data["ai_rating"],
                row_data["ai_label"],
                row_data["final_rank"],
                json.dumps(row_data.get("ai_reasons", []), ensure_ascii=False),
                json.dumps(row_data.get("exhibition", []), ensure_ascii=False),
                row_data["exhibition_rank"],
                row_data["motor_rank"],
                row_data["ai_detail"],
                row_data["ai_selection"],
                row_data["ai_confidence"],
                row_data["ai_lane_score_text"],
                row_data["class_history_text"],
                row_data["player_names_text"],
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

    log(
        f"replace_today_candidates inserted={inserted} updated={updated} "
        f"deleted={deleted} frozen_closed={frozen_closed}"
    )

    return {
        "inserted": inserted,
        "updated": updated,
        "deleted": deleted,
        "frozen_closed": frozen_closed,
    }
