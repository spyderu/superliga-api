import json
import os
from datetime import datetime, timezone
import requests

BASE = "https://www.thesportsdb.com/api/v1/json/123"
LEAGUE_ID = "4691"          # Romanian Liga I (TheSportsDB)
SEASON = "2025-2026"
OUTDIR = os.path.join("public", "superliga", SEASON)

def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def get_json(url: str) -> dict:
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, dict):
        # fallback: dacă serverul întoarce ceva neașteptat
        return {}
    return data

def to_int_safe(x):
    try:
        return int(x) if x is not None and str(x).strip() != "" else None
    except Exception:
        return None

def norm_event(e: dict) -> dict:
    if not isinstance(e, dict):
        e = {}

    date = (e.get("dateEvent") or "").strip()
    time = (e.get("strTime") or "").strip()

    kickoff = None
    if date:
        kickoff = date + ("T" + time if time else "")

    home = (e.get("strHomeTeam") or "").strip()
    away = (e.get("strAwayTeam") or "").strip()
    hs = to_int_safe(e.get("intHomeScore"))
    as_ = to_int_safe(e.get("intAwayScore"))

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

def dedupe_by_id(events: list[dict]) -> list[dict]:
    seen = set()
    uniq = []
    for ev in events:
        if not isinstance(ev, dict):
            continue
        k = ev.get("idEvent") or (ev.get("kickoff_raw"), ev.get("home"), ev.get("away"))
        if k in seen:
            continue
        seen.add(k)
        uniq.append(ev)
    return uniq

def safe_list(x):
    # TheSportsDB uneori întoarce null, nu listă
    return x if isinstance(x, list) else []

def write_json(path: str, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def main():
    os.makedirs(OUTDIR, exist_ok=True)

    # 1) NEXT EVENTS
    next_url = f"{BASE}/eventsnextleague.php?id={LEAGUE_ID}"
    next_data = get_json(next_url)
    next_events_raw = safe_list(next_data.get("events"))
    next_all = [norm_event(e) for e in next_events_raw]

    fixtures = [m for m in next_all if m.get("status") == "scheduled"]
    fixtures.sort(key=lambda x: (x.get("dateEvent") or "", x.get("strTime") or ""))

    # 2) PAST EVENTS
    past_url = f"{BASE}/eventspastleague.php?id={LEAGUE_ID}"
    past_data = get_json(past_url)
    past_events_raw = safe_list(past_data.get("events"))
    past_all = [norm_event(e) for e in past_events_raw]
    results = [r for r in past_all if r.get("status") == "finished"]

    # 3) finished din "next" (dacă apar)
    results.extend([m for m in next_all if m.get("status") == "finished"])

    results = dedupe_by_id(results)
    results.sort(key=lambda x: (x.get("dateEvent") or "", x.get("strTime") or ""), reverse=True)

    # 4) STANDINGS
    table_url = f"{BASE}/lookuptable.php?l={LEAGUE_ID}&s={SEASON}"
    table_data = get_json(table_url)
    table = safe_list(table_data.get("table"))

    standings = []
    for row in table:
        if not isinstance(row, dict):
            continue
        team = (row.get("strTeam") or "").strip()
        if not team:
            continue
        standings.append({
            "position": to_int_safe(row.get("intRank")),
            "team": team,
            "played": to_int_safe(row.get("intPlayed")),
            "win": to_int_safe(row.get("intWin")),
            "draw": to_int_safe(row.get("intDraw")),
            "loss": to_int_safe(row.get("intLoss")),
            "gf": to_int_safe(row.get("intGoalsFor")),
            "ga": to_int_safe(row.get("intGoalsAgainst")),
            "gd": to_int_safe(row.get("intGoalDifference")),
            "points": to_int_safe(row.get("intPoints")),
        })

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

    write_json(os.path.join(OUTDIR, "fixtures.json"), fixtures)
    write_json(os.path.join(OUTDIR, "results.json"), results)
    write_json(os.path.join(OUTDIR, "standings.json"), standings)
    write_json(os.path.join(OUTDIR, "meta.json"), meta)

    print("OK", meta)

if __name__ == "__main__":
    main()
