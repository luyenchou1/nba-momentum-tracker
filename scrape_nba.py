#!/usr/bin/env python3
"""
NBA Game Log Scraper
Source: plaintextsports.com/nba/2025-2026/teams/{team-slug}
Writes to SQLite: nba_games.db
Run: python3 scrape_nba.py
Cron: 0 7 * * * /usr/bin/python3 /path/to/scrape_nba.py
"""

import re
import sqlite3
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup

TEAMS = {
    "atlanta-hawks": "ATL",
    "boston-celtics": "BOS",
    "brooklyn-nets": "BKN",
    "charlotte-hornets": "CHA",
    "chicago-bulls": "CHI",
    "cleveland-cavaliers": "CLE",
    "dallas-mavericks": "DAL",
    "denver-nuggets": "DEN",
    "detroit-pistons": "DET",
    "golden-state-warriors": "GSW",
    "houston-rockets": "HOU",
    "indiana-pacers": "IND",
    "los-angeles-clippers": "LAC",
    "los-angeles-lakers": "LAL",
    "memphis-grizzlies": "MEM",
    "miami-heat": "MIA",
    "milwaukee-bucks": "MIL",
    "minnesota-timberwolves": "MIN",
    "new-orleans-pelicans": "NOP",
    "new-york-knicks": "NYK",
    "oklahoma-city-thunder": "OKC",
    "orlando-magic": "ORL",
    "philadelphia-76ers": "PHI",
    "phoenix-suns": "PHX",
    "portland-trail-blazers": "POR",
    "sacramento-kings": "SAC",
    "san-antonio-spurs": "SAS",
    "toronto-raptors": "TOR",
    "utah-jazz": "UTA",
    "washington-wizards": "WAS",
}

BASE_URL = "https://plaintextsports.com/nba/2025-2026/teams/{slug}"

CONFERENCE = {
    "ATL": "East", "BOS": "East", "BKN": "East", "CHA": "East", "CHI": "East",
    "CLE": "East", "DET": "East", "IND": "East", "MIA": "East", "MIL": "East",
    "NYK": "East", "ORL": "East", "PHI": "East", "TOR": "East", "WAS": "East",
    "DAL": "West", "DEN": "West", "GSW": "West", "HOU": "West", "LAC": "West",
    "LAL": "West", "MEM": "West", "MIN": "West", "NOP": "West", "OKC": "West",
    "PHX": "West", "POR": "West", "SAC": "West", "SAS": "West", "UTA": "West",
}


def init_db(path="nba_games.db"):
    conn = sqlite3.connect(path)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS games (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            team        TEXT NOT NULL,
            conference  TEXT NOT NULL,
            game_num    INTEGER NOT NULL,
            game_date   TEXT,
            home_away   TEXT,
            opponent    TEXT,
            result      TEXT,
            score       TEXT,
            wins        INTEGER,
            losses      INTEGER,
            is_cup      INTEGER DEFAULT 0,
            UNIQUE(team, game_num)
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS meta (
            key   TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    conn.commit()
    return conn


def parse_team_page(html, abbr):
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text()
    games = []

    pattern = re.compile(
        r'(G(\d+)|CUP|CQF|CSF|CF):\s+'
        r'(\d+/\d+)\s+'
        r'([v@])\s+'
        r'(\S.*?)\s+'
        r'([WL])\s+'
        r'([\d\-]+(?:/\d?OT)?)\s+'
        r'(\d+)-(\d+)'
    )

    for m in pattern.finditer(text):
        raw_game_id = m.group(1)
        game_num_str = m.group(2)
        date_str = m.group(3)
        loc = m.group(4)
        opponent_raw = m.group(5).strip()
        result = m.group(6)
        score = m.group(7)
        wins = int(m.group(8))
        losses = int(m.group(9))

        is_cup = 1 if raw_game_id in ("CUP", "CQF", "CSF", "CF") else 0
        # Use cumulative record to derive true game number (wins+losses = games played)
        game_num = wins + losses

        try:
            month, day = map(int, date_str.split("/"))
            year = 2026 if month <= 6 else 2025
            game_date = f"{year}-{month:02d}-{day:02d}"
        except Exception:
            game_date = None

        home_away = "H" if loc == "v" else "A"
        opponent = re.sub(r'\s+', ' ', opponent_raw).strip()

        games.append({
            "team": abbr,
            "conference": CONFERENCE[abbr],
            "game_num": game_num,
            "game_date": game_date,
            "home_away": home_away,
            "opponent": opponent,
            "result": result,
            "score": score,
            "wins": wins,
            "losses": losses,
            "is_cup": is_cup,
        })

    return games


def fetch_team(slug, abbr, session):
    url = BASE_URL.format(slug=slug)
    try:
        resp = session.get(url, timeout=15)
        resp.raise_for_status()
        games = parse_team_page(resp.text, abbr)
        print(f"  {abbr}: {len(games)} games parsed")
        return games
    except Exception as e:
        print(f"  {abbr}: ERROR — {e}")
        return []


def upsert_games(conn, games):
    c = conn.cursor()
    for g in games:
        if g["game_num"] is None:
            continue
        c.execute("""
            INSERT INTO games
                (team, conference, game_num, game_date, home_away, opponent,
                 result, score, wins, losses, is_cup)
            VALUES
                (:team, :conference, :game_num, :game_date, :home_away, :opponent,
                 :result, :score, :wins, :losses, :is_cup)
            ON CONFLICT(team, game_num) DO UPDATE SET
                game_date  = excluded.game_date,
                home_away  = excluded.home_away,
                opponent   = excluded.opponent,
                result     = excluded.result,
                score      = excluded.score,
                wins       = excluded.wins,
                losses     = excluded.losses,
                is_cup     = excluded.is_cup
        """, g)
    conn.commit()


def update_meta(conn):
    c = conn.cursor()
    c.execute("""
        INSERT INTO meta (key, value) VALUES ('last_updated', ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
    """, (datetime.now().isoformat(),))
    conn.commit()


def main():
    print("Initializing DB...")
    conn = init_db("nba_games.db")

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; NBA-tracker/1.0)"
    })

    total = 0
    for slug, abbr in TEAMS.items():
        games = fetch_team(slug, abbr, session)
        upsert_games(conn, games)
        total += len(games)
        time.sleep(0.4)  # polite rate limiting

    update_meta(conn)
    conn.close()
    print(f"\nDone. {total} game records written to nba_games.db")


if __name__ == "__main__":
    main()
