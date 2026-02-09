import json
import os
import re
import hashlib
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

SEASON = "2025-2026"
OUTDIR = os.path.join("public", "superliga", SEASON)

# Meciuri (fixtures+results) – sursă stabilă pentru GitHub Actions
READFOOTBALL_CAL_URL = "https://www.readfootball.com/en/football-romania/tournaments/liga-i/calendar.html"

# Clasament – lpf2.ro
LPF2_STANDINGS_URL = "https://lpf2.ro/"

UA = "Mozilla/5.0 (compatible; superliga-api-bot/2.0; +https://github.com/spyderu/superliga-api)"

RO_MONTH = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=1.2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=20)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent": UA})
    return s

SESSION = make_session()

def fetch_html(url: str) -> str:
    r = SESSION.get(url, timeout=(15, 30))
    r.raise_for_status()
    return r.text

def stable_dumps(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

def sha256_str(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def write_json_if_changed(path: str, obj) -> bool:
    new_str = stable_dumps(obj)
    new_hash = sha256_str(new_str)

    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                old_obj = json.load(f)
            old_str = stable_dumps(old_obj)
            old_hash = sha256_str(old_str)
            if old_hash == new_hash:
                return False
        except Exception:
            pass

    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    return True

def read_existing(path: str):
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def normalize_team(name: str) -> str:
    return re.sub(r"\s+", " ", name.strip())

def parse_readfootball_datetime(date_str: str, time_str: str) -> Tuple[str, str]:
    # date_str example: "9 Feb 2026"
    # time_str example: "21:00"
    parts = date_str.strip().split()
    dd = int(parts[0])
    mon = parts[1].lower()
    yyyy = int(parts[2])
    if mon not in RO_MONTH:
        raise ValueError(f"Unknown month token: {mon}")
    dt = datetime(yyyy, RO_MONTH[mon], dd, int(time_str[:2]), int(time_str[3:5]))
    return dt.strftime("%Y-%m-%d"), dt.strftime("%H:%M:%S")

def match_obj(round_no: int, dateEvent: str, timeEvent: str, home: str, away: str,
              hs: Optional[int], as_: Optional[int]) -> Dict:
    status = "scheduled" if (hs is None or as_ is None) else "finished"
    played = status == "finished"
    return {
        "idEvent": None,
        "season": SEASON,
        "round": str(round_no) if round_no is not None else None,
        "dateEvent": dateEvent,
        "strTime": timeEvent,
        "kickoff_raw": f"{dateEvent}T{timeEvent}",

        "home": home,
        "away": away,
        "status": status,
        "score": {"home": hs, "away": as_},

        # alias pt UI-uri
        "homeTeam": home,
        "awayTeam": away,
        "played": played,
        "intHomeScore": hs,
        "intAwayScore": as_,
        "event": {"strEvent": f"{home} vs {away}", "strLeague": "SuperLiga"},
        "source": "ReadFootball",
        "source_league_id": "readfootball.com",
    }

def extract_matches_from_readfootball() -> Tuple[List[Dict], str]:
    """
    Parsează calendarul ReadFootball.
    Observație: 'Tour statistics' apare după fiecare bloc de 8 meciuri => îl folosim ca delimiter de etapă.
    """
    try:
        html = fetch_html(READFOOTBALL_CAL_URL)
    except Exception as e:
        return [], f"readfootball_fetch_failed:{type(e).__name__}"

    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text("\n")
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    # pattern:
    # 9 Feb 2026, Mon  21:00 Dinamo Bucureşti  1 : 1  Universitatea Craiova
    # 13 Feb 2026, Fri  21:00 Petrolul 52 - : - Argeș
    rgx = re.compile(
        r"^(\d{1,2}\s+[A-Za-z]{3}\s+\d{4}),\s+\w+\s+(\d{1,2}:\d{2})\s+(.+?)\s+(\d+|-)\s*:\s*(\d+|-)\s+(.+)$"
    )

    matches: List[Dict] = []
    round_no = 1
    seen_any_in_round = False

    for ln in lines:
        if ln.lower().startswith("tour statistics"):
            # trecem la etapa următoare doar dacă am prins meciuri în etapa curentă
            if seen_any_in_round:
                round_no += 1
                seen_any_in_round = False
            continue

        m = rgx.match(ln)
        if not m:
            continue

        date_part = m.group(1)
        time_part = m.group(2)
        home = normalize_team(m.group(3))
        hs_tok = m.group(4)
        as_tok = m.group(5)
        away = normalize_team(m.group(6))

        try:
            dateEvent, timeEvent = parse_readfootball_datetime(date_part, time_part)
        except Exception:
            continue

        hs = None if hs_tok == "-" else int(hs_tok)
        as_ = None if as_tok == "-" else int(as_tok)

        matches.append(match_obj(round_no, dateEvent, timeEvent, home, away, hs, as_))
        seen_any_in_round = True

    # dedupe (ReadFootball mai repetă uneori la final “Previous/Next matches”)
    uniq = []
    seen = set()
    for mm in matches:
        key = (mm["dateEvent"], mm["strTime"], mm["home"], mm["away"])
        if key in seen:
            continue
        seen.add(key)
        uniq.append(mm)

    if len(uniq) < 20:
        return uniq, f"readfootball_incomplete:{len(uniq)}"
    return uniq, "readfootball_ok"

def parse_standings_from_lpf2() -> Tuple[List[Dict], str]:
    """
    Extrage PUNCTELE (nu 'Adevăr').
    Format tipic în text: ... 45-24 (21) 49 (25) (+11)
                         gf-ga  (gd) pts (??) (adevar)
    """
    try:
        html = fetch_html(LPF2_STANDINGS_URL)
    except Exception as e:
        return [], f"lpf2_fetch_failed:{type(e).__name__}"

    soup = BeautifulSoup(html, "lxml")
    tables = soup.find_all("table")

    best_rows = []
    for t in tables:
        txt = t.get_text(" ", strip=True).lower()
        if "pozitia" in txt and "puncte" in txt and ("meciuri" in txt or "victorii" in txt):
            best_rows = t.find_all("tr")
            break

    standings = []

    # pos TEAM played win draw loss gf-ga (gd) pts (x) (adevar)
    rgx = re.compile(
        r"^\s*(\d{1,2})\s+(.+?)\s+(\d{1,2})\s+(\d{1,2})\s+(\d{1,2})\s+(\d{1,2})\s+(\d{1,3})\s*-\s*(\d{1,3})"
        r"\s*\(\s*([+-]?\d+)\s*\)\s+(\d{1,3})"
        r"(?:\s*\(\s*(\d{1,3})\s*\))?"
        r"(?:\s*\(\s*([+-]?\d+)\s*\))?\s*$",
        re.UNICODE
    )

    if best_rows:
        candidates = [tr.get_text(" ", strip=True) for tr in best_rows]
    else:
        # fallback text
        candidates = [ln.strip() for ln in soup.get_text("\n").splitlines() if ln.strip()]

    for txt in candidates:
        txt = re.sub(r"\s+", " ", txt).strip()
        m = rgx.match(txt)
        if not m:
            continue
        pos = int(m.group(1))
        team = normalize_team(m.group(2))
        played = int(m.group(3))
        win = int(m.group(4))
        draw = int(m.group(5))
        loss = int(m.group(6))
        gf = int(m.group(7))
        ga = int(m.group(8))
        gd = int(m.group(9))
        points = int(m.group(10))
        adevar = int(m.group(12)) if m.group(12) is not None else None

        standings.append({
            "position": pos,
            "team": team,
            "played": played,
            "win": win,
            "draw": draw,
            "loss": loss,
            "gf": gf,
            "ga": ga,
            "gd": gd,
            "points": points,
            "adevar": adevar,
        })

    standings.sort(key=lambda x: x["position"])
    if len(standings) >= 12:
        return standings, "lpf2_ok"
    return standings, f"lpf2_incomplete:{len(standings)}"

def main():
    os.makedirs(OUTDIR, exist_ok=True)

    # MATCHES (ReadFootball)
    matches, matches_status = extract_matches_from_readfootball()
    if not matches:
        # fallback la fișierele vechi
        fixtures = read_existing(os.path.join(OUTDIR, "fixtures.json"))
        results = read_existing(os.path.join(OUTDIR, "results.json"))
    else:
        fixtures = [m for m in matches if m["status"] == "scheduled"]
        results = [m for m in matches if m["status"] == "finished"]
        fixtures.sort(key=lambda x: (x["dateEvent"], x["strTime"], x["home"], x["away"]))
        results.sort(key=lambda x: (x["dateEvent"], x["strTime"]), reverse=True)

    # STANDINGS (LPF2)
    standings, standings_status = parse_standings_from_lpf2()
    if not standings:
        prev = read_existing(os.path.join(OUTDIR, "standings.json"))
        if isinstance(prev, list) and prev:
            standings = prev

    meta = {
        "competition": "SuperLiga",
        "season": SEASON,
        "sources": {"matches": "readfootball.com", "standings": "lpf2.ro"},
        "generated_utc": iso_now(),
        "status": {"matches": matches_status, "standings": standings_status},
        "counts": {
            "fixtures": len(fixtures) if isinstance(fixtures, list) else 0,
            "results": len(results) if isinstance(results, list) else 0,
            "standings_rows": len(standings) if isinstance(standings, list) else 0,
        },
    }

    changed = False
    if isinstance(fixtures, list):
        changed |= write_json_if_changed(os.path.join(OUTDIR, "fixtures.json"), fixtures)
    if isinstance(results, list):
        changed |= write_json_if_changed(os.path.join(OUTDIR, "results.json"), results)
    if isinstance(standings, list):
        changed |= write_json_if_changed(os.path.join(OUTDIR, "standings.json"), standings)

    # meta doar dacă ceva s-a schimbat (evit commit spam)
    if changed:
        write_json_if_changed(os.path.join(OUTDIR, "meta.json"), meta)

    print("OK", meta)

if __name__ == "__main__":
    main()
