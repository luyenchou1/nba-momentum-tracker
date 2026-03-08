#!/usr/bin/env python3
"""
Export NBA game data from SQLite to JSON for the chart.
Reads: nba_games.db
Writes: nba_chart_data.json
"""

import json
import sqlite3
from collections import defaultdict


def export(db_path="nba_games.db", out_path="nba_chart_data.json"):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Get last updated timestamp
    meta = conn.execute("SELECT value FROM meta WHERE key='last_updated'").fetchone()
    last_updated = meta["value"] if meta else None

    # Fetch all games ordered by team and game number
    rows = conn.execute("""
        SELECT team, conference, game_num, game_date, home_away, opponent,
               result, score, wins, losses
        FROM games
        ORDER BY team, game_num
    """).fetchall()
    conn.close()

    teams = defaultdict(lambda: {"conference": None, "games": []})
    for r in rows:
        t = teams[r["team"]]
        t["conference"] = r["conference"]
        t["games"].append({
            "game_num": r["game_num"],
            "date": r["game_date"],
            "home_away": r["home_away"],
            "opponent": r["opponent"],
            "result": r["result"],
            "score": r["score"],
            "wins": r["wins"],
            "losses": r["losses"],
        })

    data = {
        "lastUpdated": last_updated,
        "teams": dict(teams),
    }

    with open(out_path, "w") as f:
        json.dump(data, f, separators=(",", ":"))

    total_games = sum(len(t["games"]) for t in teams.values())
    print(f"Exported {len(teams)} teams, {total_games} games to {out_path}")


if __name__ == "__main__":
    export()
