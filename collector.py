# このファイルは current collector.py に貼る用のパッチです。
# 使い方:
# 1. collector.py で `def analyze_candidate(` を検索
# 2. その関数を、このファイル内の analyze_candidate に丸ごと置き換え
# 3. 保存して実行
#
# 目的:
# - 直近成績(1着側)は残す
# - 消えていた 2着 / 3着 の全国勝率・当地勝率・モーター2連率 を復活
# - 既存の級別3期や展示系はそのまま使う
#
# 前提:
# - 既存 collector.py に以下がすでにあること
#   - selection_triplets
#   - avg_stat
#   - score_to_ai_rating
#   - decide_final_rank
#   - class_history_score
#
# 追加メモ:
# - recent / top3 は boat_stats[lane] に recent, top3 で入っている前提
# - course / st を使う版ではありません。今回は「消えた 2着3着 材料の復活」が目的です。


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

    exhibition_times = exhibition_info.get("times", []) if exhibition_info else []
    exhibition_ranks = exhibition_info.get("ranks", {}) if exhibition_info else {}

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

        if top3_lanes and any(h == top3_lanes[0] for h in unique_heads):
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

    boat_stats = boat_stats or {}
    class_history_map = class_history_map or {}

    if boat_stats:
        head_stats = [boat_stats[h] for h in unique_heads if h in boat_stats]
        second_stats = [boat_stats[s] for s in unique_seconds if s in boat_stats]
        third_stats = [boat_stats[t] for t in unique_thirds if t in boat_stats]

        head_national = avg_stat(head_stats, "national_win")
        head_local = avg_stat(head_stats, "local_win")
        head_motor = avg_stat(head_stats, "motor2")
        head_boat = avg_stat(head_stats, "boat2")
        head_recent = avg_stat(head_stats, "recent")
        head_top3 = avg_stat(head_stats, "top3")

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

        if head_recent is not None:
            details.append(f"1着直近平均着順{round(head_recent, 2)}")
            if head_recent <= 2.4:
                score += 0.45
                reasons.append("1着候補の直近着順が良い")
            elif head_recent <= 3.0:
                score += 0.20
            elif head_recent >= 4.6:
                score -= 0.35
                reasons.append("1着候補の直近着順が悪い")

        if head_top3 is not None:
            details.append(f"1着直近3着内率{round(head_top3, 1)}")
            if head_top3 >= 70:
                score += 0.35
                reasons.append("1着候補の直近安定感が高い")
            elif head_top3 >= 55:
                score += 0.15
            elif head_top3 < 35:
                score -= 0.25
                reasons.append("1着候補の直近安定感が弱い")

        # ここを今回復活: 2着側の材料
        if second_national is not None:
            details.append(f"2着全国勝率平均{round(second_national, 2)}")
            if second_national >= 5.4:
                score += 0.25
                reasons.append("2着候補の全国勝率が高い")
            elif second_national >= 4.8:
                score += 0.10
            elif second_national < 4.2:
                score -= 0.20
                reasons.append("2着候補の全国勝率が弱い")

        if second_local is not None:
            details.append(f"2着当地勝率平均{round(second_local, 2)}")
            if second_local >= 5.2:
                score += 0.15
            elif second_local < 4.2:
                score -= 0.10

        if second_motor is not None:
            details.append(f"2着モーター2連率平均{round(second_motor, 1)}")
            if second_motor >= 38:
                score += 0.15
            elif second_motor < 28:
                score -= 0.10

        # ここも今回復活: 3着側の材料
        if third_national is not None:
            details.append(f"3着全国勝率平均{round(third_national, 2)}")
            if third_national >= 5.0:
                score += 0.10
            elif third_national < 4.0:
                score -= 0.10

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

    head_histories = [class_history_map[h] for h in unique_heads if h in class_history_map]
    second_histories = [class_history_map[s] for s in unique_seconds if s in class_history_map]

    avg_head_class = None
    avg_second_class = None

    if head_histories:
        head_class_scores = [class_history_score(x) for x in head_histories]
        avg_head_class = sum(head_class_scores) / len(head_class_scores)
        details.append(f"1着級別3期平均{round(avg_head_class, 2)}")

        head_current_classes = [x.get("current_class", "") for x in head_histories if x.get("current_class")]
        head_a1_count = sum(1 for x in head_current_classes if x == "A1")
        head_b2_count = sum(1 for x in head_current_classes if x == "B2")

        if avg_head_class >= 1.6:
            score += 0.95
            reasons.append("1着候補の級別3期傾向がかなり強い")
        elif avg_head_class >= 1.0:
            score += 0.55
            reasons.append("1着候補の級別3期傾向が良い")
        elif avg_head_class >= 0.5:
            score += 0.25
            reasons.append("1着候補の級別3期傾向がまずまず")
        elif avg_head_class <= -0.3:
            score -= 0.50
            reasons.append("1着候補の級別3期傾向が弱い")

        if head_a1_count >= 2:
            score += 0.32
            reasons.append("1着候補に現A1が複数いる")
        elif head_a1_count >= 1 and len(unique_heads) == 1:
            score += 0.18
            reasons.append("頭本線が現A1")

        if head_b2_count >= 1:
            score -= 0.22
            reasons.append("1着候補に現B2が含まれる")

    if second_histories:
        second_class_scores = [class_history_score(x) for x in second_histories]
        avg_second_class = sum(second_class_scores) / len(second_class_scores)
        details.append(f"2着級別3期平均{round(avg_second_class, 2)}")
        if avg_second_class >= 0.9:
            score += 0.18
        elif avg_second_class <= -0.2:
            score -= 0.12

    if avg_head_class is not None and avg_second_class is not None:
        class_gap = avg_head_class - avg_second_class
        details.append(f"1-2着級別差{round(class_gap, 2)}")
        if class_gap >= 0.7:
            score += 0.22
            reasons.append("1着候補の級別優位がある")
        elif class_gap <= -0.5:
            score -= 0.18
            reasons.append("1着候補の級別優位が薄い")

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

            if exhibition_spread is not None and head_time_gap_from_top is not None:
                if exhibition_spread >= 0.12 and head_time_gap_from_top <= 0.03:
                    score += 0.15
                    reasons.append("強風で展示タイム上位を少し強め評価")
                elif exhibition_spread >= 0.12 and head_time_gap_from_top >= 0.10:
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
        exhibition_rank_text = " / ".join([f"{lane}:{exhibition_ranks.get(lane, '-')}" for lane in range(1, 7)])

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
