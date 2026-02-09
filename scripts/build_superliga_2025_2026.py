import json
import os
from datetime import datetime, timezone
import requests

BASE = "https://www.thesportsdb.com/api/v1/json/123"
LEAGUE_ID = "4691"          # Romanian Liga I (TheSportsDB)
SEASON = "2025-2026"
OUTDIR = os.path.join("public", "superliga", SEASON)

def get_json(url: str) -> dict:
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()

def iso_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def norm_event(e: dict) -> dict:
    date = (e.get("dateEvent") or "").strip()
    time = (e.get("strTime") or "").strip()

    kickoff = None
    if date:
        kickoff = date + ("T" + time if time else "")

    def to_int(x):
        try:
            return int(x) if x is not None and str(x).strip() != "" else None
        except:
            return None

    home = (e.get("strHomeTeam") or "").strip()
    away = (e.get("strAwayTeam") or "").strip()
    hs = to_int(e.get("intHomeScore"))
    as_ = to_int(e.get("intAwayScore"))

    status = "scheduled"
    if hs is not None and as_ is not None:
        status = "finished"

    return {
        "idEvent": e.get("idEvent"),
        "season": SEASON,
        "round": e.get("intRound"),
        "dateEvent": e.get("dateEvent"),
        "strTime": e.get("strTime"),
        "kickoff_raw": kickoff,  # raw, fără timezone garantat
        "home": home,
        "away": away,
        "status": status,
        "score": {"home": hs, "away": as_},
        "venue": (e.get("strVenue") or "").strip() or None,
        "city": (e.get("strCity") or "").strip() or None,
        "event": {
            "strEvent": e.get("strEvent"),
            "strLeague": e.get("strLeague"),
        },
        "source": "TheSportsDB",
        "source_league_id": LEAGUE_ID,
    }

def main():
    os.makedirs(OUTDIR, exist_ok=True)

    # Fixtures viitoare
    next_url = f"{BASE}/eventsnextleague.php?id={LEAGUE_ID}"
    next_data = get_json(next_url)
    next_events = next_data.get("events") or []
    fixtures = [norm_event(e) for e in next_events]
    fixtures.sort(key=lambda x: (x.get("dateEvent") or "", x.get("strTime") or ""))

    # Rezultate trecute
    past_url = f"{BASE}/eventspastleague.php?id={LEAGUE_ID}"
    past_data = get_json(past_url)
    past_events = past_data.get("events") or []
    results = [norm_event(e) for e in past_events]
    results = [r for r in results if r["status"] == "finished"]
    results.sort(key=lambda x: (x.get("dateEvent") or "", x.get("strTime") or ""), reverse=True)

    # Clasament
    table_url = f"{BASE}/lookuptable.php?l={LEAGUE_ID}&s={SEASON}"
    table_data = get_json(table_url)
    table = table_data.get("table") or []

    standings = []
    for row in table:
        def to_int(v):
            try:
                return int(v) if v is not None and str(v).strip() != "" else None
            except:
                return None

        standings.append({
            "position": to_int(row.get("intRank")),
            "team": (row.get("strTeam") or "").strip(),
            "played": to_int(row.get("intPlayed")),
            "win": to_int(row.get("intWin")),
            "draw": to_int(row.get("intDraw")),
            "loss": to_int(row.get("intLoss")),
            "gf": to_int(row.get("intGoalsFor")),
            "ga": to_int(row.get("intGoalsAgainst")),
            "gd": to_int(row.get("intGoalDifference")),
            "points": to_int(row.get("intPoints")),
        })

    standings = [s for s in standings if s["team"]]
    standings.sort(key=lambda x: (x["position"] if x["position"] is not None else 10_000))

    meta = {
        "competition": "SuperLiga (Romanian Liga I)",
        "season": SEASON,
        "source": "TheSportsDB",
        "league_id": LEAGUE_ID,
        "generated_utc": iso_now(),
        "counts": {
            "fixtures": len(fixtures),
            "results": len(results),
            "standings_rows": len(standings),
        }
    }

    def write(name: str, obj):
        path = os.path.join(OUTDIR, name)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)

    write("fixtures.json", fixtures)
    write("results.json", results)
    write("standings.json", standings)
    write("meta.json", meta)

    print("OK", meta)

if __name__ == "__main__":
    main()
