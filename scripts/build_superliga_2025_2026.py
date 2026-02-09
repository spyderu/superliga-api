import json
import os
from datetime import datetime, timezone
import requests

BASE = "https://www.thesportsdb.com/api/v1/json/123"
LEAGUE_ID = "4691"          # Romanian Liga I (TheSportsDB)
SEASON = "2025-2026"
OUTDIR = os.path.join("public", "superliga", SEASON)

# prag: dacă TheSportsDB îți dă sub N echipe în clasament, calculăm noi
MIN_STANDINGS_TEAMS = 12

def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def get_json(url: str) -> dict:
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, dict) else {}

def safe_list(x):
    return x if isinstance(x, list) else []

def to_int_safe(x):
    try:
        return int(x) if x is not None and str(x).strip() != "" else None
    except Exception:
        return None

def norm_team_name(name: str) -> str:
    # normalizare minimală; suficientă pentru chei interne
    return (name or "").strip()

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

def write_json(path: str, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def standings_from_lookuptable() -> list[dict]:
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
    return standings

def fetch_all_season_events() -> list[dict]:
    """
    Încercăm să luăm toate meciurile sezonului.
    1) eventsseason.php (dacă există)
    2) fallback: eventsround.php pe runde
    """
    # 1) încercare directă: tot sezonul
    season_url = f"{BASE}/eventsseason.php?id={LEAGUE_ID}&s={SEASON}"
    season_data = get_json(season_url)
    season_events = safe_list(season_data.get("events"))
    if season_events:
        return season_events

    # 2) fallback: runde (iterăm până la mai multe runde consecutive goale)
    all_events = []
    empty_streak = 0

    # Liga poate avea 30+ etape + play-off/out; punem o limită rezonabilă
    for rnd in range(1, 60):
        round_url = f"{BASE}/eventsround.php?id={LEAGUE_ID}&r={rnd}&s={SEASON}"
        round_data = get_json(round_url)
        round_events = safe_list(round_data.get("events"))

        if not round_events:
            empty_streak += 1
            # dacă avem 6 runde consecutive fără nimic, ne oprim
            if empty_streak >= 6:
                break
            continue

        empty_streak = 0
        all_events.extend(round_events)

    return all_events

def compute_standings_from_matches(matches: list[dict]) -> list[dict]:
    """
    Calculează clasamentul din meciuri FINISHED.
    Reguli standard: 3p victorie, 1p egal, 0p înfrângere.
    Tie-break simplu: puncte, golaveraj, goluri marcate, nume.
    """
    table = {}

    def ensure(team: str):
        team = norm_team_name(team)
        if team not in table:
            table[team] = {
                "team": team,
                "played": 0,
                "win": 0,
                "draw": 0,
                "loss": 0,
                "gf": 0,
                "ga": 0,
                "gd": 0,
                "points": 0,
            }

    for m in matches:
        if not isinstance(m, dict):
            continue

        ev = norm_event(m)
        if ev.get("status") != "finished":
            continue

        home = norm_team_name(ev.get("home"))
        away = norm_team_name(ev.get("away"))
        hs = ev.get("score", {}).get("home")
        as_ = ev.get("score", {}).get("away")

        if not home or not away:
            continue
        if hs is None or as_ is None:
            continue

        ensure(home)
        ensure(away)

        table[home]["played"] += 1
        table[away]["played"] += 1

        table[home]["gf"] += hs
        table[home]["ga"] += as_
        table[away]["gf"] += as_
        table[away]["ga"] += hs

        if hs > as_:
            table[home]["win"] += 1
            table[away]["loss"] += 1
            table[home]["points"] += 3
        elif hs < as_:
            table[away]["win"] += 1
            table[home]["loss"] += 1
            table[away]["points"] += 3
        else:
            table[home]["draw"] += 1
            table[away]["draw"] += 1
            table[home]["points"] += 1
            table[away]["points"] += 1

    # calc gd
    for t in table.values():
        t["gd"] = t["gf"] - t["ga"]

    rows = list(table.values())
    rows.sort(key=lambda x: (-x["points"], -x["gd"], -x["gf"], x["team"].lower()))

    # poziții 1..N
    for i, r in enumerate(rows, start=1):
        r["position"] = i

    return rows

def main():
    os.makedirs(OUTDIR, exist_ok=True)

    # 1) FIXTURES (next)
    next_url = f"{BASE}/eventsnextleague.php?id={LEAGUE_ID}"
    next_data = get_json(next_url)
    next_events_raw = safe_list(next_data.get("events"))
    next_all = [norm_event(e) for e in next_events_raw]

    fixtures = [m for m in next_all if m.get("status") == "scheduled"]
    fixtures.sort(key=lambda x: (x.get("dateEvent") or "", x.get("strTime") or ""))

    # 2) RESULTS (past + finished din next)
    past_url = f"{BASE}/eventspastleague.php?id={LEAGUE_ID}"
    past_data = get_json(past_url)
    past_events_raw = safe_list(past_data.get("events"))
    past_all = [norm_event(e) for e in past_events_raw]
    results = [r for r in past_all if r.get("status") == "finished"]
    results.extend([m for m in next_all if m.get("status") == "finished"])
    results = dedupe_by_id(results)
    results.sort(key=lambda x: (x.get("dateEvent") or "", x.get("strTime") or ""), reverse=True)

    # 3) STANDINGS (try lookuptable -> fallback compute)
    standings = standings_from_lookuptable()
    standings_source = "lookuptable"

    if len(standings) < MIN_STANDINGS_TEAMS:
        # fallback: calculăm din meciurile sezonului
        season_events = fetch_all_season_events()
        computed = compute_standings_from_matches(season_events)

        # dacă am reușit să obținem mai multe echipe decât lookuptable, folosim computed
        if len(computed) >= MIN_STANDINGS_TEAMS:
            standings = computed
            standings_source = "computed_from_matches"
        else:
            # dacă nici computed nu are destule echipe, păstrăm ce avem, dar marcăm sursa
            standings_source = "lookuptable_incomplete_and_computed_incomplete"

    meta = {
        "competition": "SuperLiga (Romanian Liga I)",
        "season": SEASON,
        "source": "TheSportsDB",
        "league_id": LEAGUE_ID,
        "generated_utc": iso_now(),
        "standings_source": standings_source,
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
