        race_official_url = ""
        if official_url:
            race_official_url = re.sub(r"rno=\d+", f"rno={race_no}", official_url)

        official_data = {"rating": "", "selection": ""}
        if race_official_url:
            try:
                official_data = parse_official_expect(race_official_url)
            except Exception as e:
                log(f"{venue_name} {race_no}R parse_official_expect error: {e}")

        final_rating = official_data["rating"] or rating
        final_selection = official_data["selection"] or selection

        rows.append(
            {
                "venue": venue_name,
                "race_no": race_no,
                "rating": final_rating,
                "selection": final_selection,
                "official_url": race_official_url or official_url,
            }
        )
