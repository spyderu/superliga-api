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

# LPF2: etape + clasament (stabil pe GitHub Actions)
LPF2_HOME = "https://lpf2.ro/"
LPF2_ETAPA_URL = "https://lpf2.ro/html/etape/Etapa-{n}.html"
LPF2_STANDINGS_URL = "https://lpf2.ro/"

PAST_ROUNDS = 2
FUTURE_ROUNDS = 4
MAX_ROUNDS = 30

UA = "Mozilla/5.0 (compatible; superliga-api-bot/3.0; +https://github.com/spyderu/superliga-api)"

RO_MONTH_FULL = {
    "ianuarie": 1,
    "februarie": 2,
    "martie": 3,
    "aprilie": 4,
    "mai": 5,
    "iunie": 6,
    "iulie": 7,
    "august": 8,
    "septembrie": 9,
    "octombrie": 10,
    "noiembrie": 11,
    "decembrie": 12,
}

def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=4, connect=4, read=4,
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

def norm_team(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip())

def parse_ro_datetime(date_str: str, time_str: str) -> Tuple[str, str]:
    # "06 Februarie 2026" + "17:00"
    m = re.match(r"^\s*(\d{1,2})\s+([A-Za-zăâîșțĂÂÎȘȚ]+)\s+(\d{4})\s*$", date_str.strip(), re.UNICODE)
    if not m:
        raise ValueError(f"Bad date: {date_str}")
    dd = int(m.group(1))
    mon = m.group(2).lower()
    yyyy = int(m.group(3))
    if mon not in RO_MONTH_FULL:
        raise ValueError(f"Unknown month: {mon}")
    hh, mm = time_str.strip().split(":")
    dt = datetime(yyyy, RO_MONTH_FULL[mon], dd, int(hh), int(mm))
    return dt.strftime("%Y-%m-%d"), dt.strftime("%H:%M:%S")

def match_obj(round_no: int, dateEvent: str, timeEvent: str, home: str, away: str,
              hs: Optional[int], as_: Optional[int]) -> Dict:
    status = "scheduled" if (hs is None or as_ is None) else "finished"
    played = status == "finished"
    return {
        "idEvent": None,
        "season": SEASON,
        "round": str(round_no),
        "dateEvent": dateEvent,
        "strTime": timeEvent,
        "kickoff_raw": f"{dateEvent}T{timeEvent}",

        "home": home,
        "away": away,
        "status": status,
        "score": {"home": hs, "away": as_},

        # alias-uri pentru aplicații
        "homeTeam": home,
        "awayTeam": away,
        "played": played,
        "intHomeScore": hs,
        "intAwayScore": as_,

        "venue": None,
        "city": None,
        "event": {"strEvent": f"{home} vs {away}", "strLeague": "SuperLiga"},
        "source": "LPF2",
        "source_league_id": "lpf2.ro",
    }

def get_current_round_from_lpf2_home() -> Tuple[int, str]:
    """
    Aproximare practică: "current_round" ~= max(Meciuri jucate) din clasamentul live.
    În etapa 26 se vede că multe echipe au 26 meciuri jucate (unele 25 din amânări). :contentReference[oaicite:3]{index=3}
    """
    try:
        html = fetch_html(LPF2_HOME)
    except Exception as e:
        return 1, f"home_fetch_failed:{type(e).__name__}"

    soup = BeautifulSoup(html, "lxml")
    txt = soup.get_text(" ", strip=True)

    # caută secvențe de tip "... Craiova26 14 8 4 ..."
    played_vals = [int(x) for x in re.findall(r"\b(1?\d|2\d|3\d)\b(?=\s+\d{1,2}\s+\d{1,2}\s+\d{1,2}\s+\d{1,3}-\d{1,3})", txt)]
    if not played_vals:
        return 1, "home_no_played_found"

    # max e de regulă etapa curentă
    return max(played_vals), "home_ok"

def extract_matches_from_lpf2_round(round_no: int) -> Tuple[List[Dict], str]:
    url = LPF2_ETAPA_URL.format(n=round_no)
    try:
        html = fetch_html(url)
    except Exception as e:
        return [], f"round_fetch_failed:{round_no}:{type(e).__name__}"

    soup = BeautifulSoup(html, "lxml")
    lines = [ln.strip() for ln in soup.get_text("\n").splitlines() if ln.strip()]

    matches: List[Dict] = []

    # Exemple în text (din pagina Etapa 26/27): :contentReference[oaicite:4]{index=4}
    # "06 Februarie 2026, 17:00 FC Arges 3-1 FC Hermannstadt"
    # "09 Februarie 2026, 20:00 Dinamo Bucuresti - CS Universitatea Craiova"
    rgx_finished = re.compile(r"^(\d{1,2}\s+[A-Za-zăâîșțĂÂÎȘȚ]+\s+\d{4}),\s*(\d{1,2}:\d{2})\s+(.+?)\s+(\d{1,2})-(\d{1,2})\s+(.+)$", re.UNICODE)
    rgx_sched = re.compile(r"^(\d{1,2}\s+[A-Za-zăâîșțĂÂÎȘȚ]+\s+\d{4}),\s*(\d{1,2}:\d{2})\s+(.+?)\s+-\s+(.+)$", re.UNICODE)

    for ln in lines:
        ln = re.sub(r"\s+", " ", ln).strip()

        m = rgx_finished.match(ln)
        if m:
            date_str, time_str, home, hs, as_, away = m.groups()
            try:
                dateEvent, timeEvent = parse_ro_datetime(date_str, time_str)
            except Exception:
                continue
            matches.append(match_obj(round_no, dateEvent, timeEvent, norm_team(home), norm_team(away), int(hs), int(as_)))
            continue

        m = rgx_sched.match(ln)
        if m:
            date_str, time_str, home, away = m.groups()
            try:
                dateEvent, timeEvent = parse_ro_datetime(date_str, time_str)
            except Exception:
                continue
            matches.append(match_obj(round_no, dateEvent, timeEvent, norm_team(home), norm_team(away), None, None))
            continue

    # dedupe
    uniq = []
    seen = set()
    for mm in matches:
        key = (mm["round"], mm["dateEvent"], mm["strTime"], mm["home"], mm["away"])
        if key in seen:
            continue
        seen.add(key)
        uniq.append(mm)

    return uniq, "round_ok"

def parse_standings_from_lpf2() -> Tuple[List[Dict], str]:
    """
    Fix: extrage PUNCTE reale (nu Adevăr).
    Format tipic: "46-25 (21)50 (25)(+11)" => points=50, adevar=+11. :contentReference[oaicite:5]{index=5}
    """
    try:
        html = fetch_html(LPF2_STANDINGS_URL)
    except Exception as e:
        return [], f"lpf2_fetch_failed:{type(e).__name__}"

    soup = BeautifulSoup(html, "lxml")
    txt = soup.get_text(" ", strip=True)

    rgx = re.compile(
        r"\b(\d{1,2})\s*([A-Za-z0-9ăâîșțĂÂÎȘȚ .'-]+?)\s+(\d{1,2})\s+(\d{1,2})\s+(\d{1,2})\s+(\d{1,2})\s+(\d{1,3})-(\d{1,3})\s*\(\s*([+-]?\d+)\s*\)\s*(\d{1,3})\s*\(\s*(\d{1,3})\s*\)\s*\(\s*([+-]?\d+)\s*\)",
        re.UNICODE
    )

    standings = []
    for m in rgx.finditer(txt):
        pos = int(m.group(1))
        team = norm_team(m.group(2))
        played = int(m.group(3))
        win = int(m.group(4))
        draw = int(m.group(5))
        loss = int(m.group(6))
        gf = int(m.group(7))
        ga = int(m.group(8))
        gd = int(m.group(9))
        points = int(m.group(10))
        adevar = int(m.group(12))

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

    # determinăm etapa curentă din lpf2 (după meciuri jucate în clasament)
    current_round, cr_status = get_current_round_from_lpf2_home()

    start_r = max(1, current_round - PAST_ROUNDS)
    end_r = min(MAX_ROUNDS, current_round + FUTURE_ROUNDS)

    all_matches: List[Dict] = []
    rounds_status = []

    # adunăm meciuri din etape lpf2
    try:
        for r in range(start_r, end_r + 1):
            ms, st = extract_matches_from_lpf2_round(r)
            rounds_status.append({str(r): st})
            all_matches.extend(ms)

        fixtures = [m for m in all_matches if m["status"] == "scheduled"]
        results = [m for m in all_matches if m["status"] == "finished"]

        fixtures.sort(key=lambda x: (x["dateEvent"], x["strTime"], x["home"], x["away"]))
        results.sort(key=lambda x: (x["dateEvent"], x["strTime"]), reverse=True)

        matches_status = f"lpf2_ok_rounds:{start_r}-{end_r}"
    except Exception as e:
        matches_status = f"lpf2_failed:{type(e).__name__}"
        fixtures = read_existing(os.path.join(OUTDIR, "fixtures.json"))
        results = read_existing(os.path.join(OUTDIR, "results.json"))

    # clasament
    standings, standings_status = parse_standings_from_lpf2()
    if not standings:
        prev = read_existing(os.path.join(OUTDIR, "standings.json"))
        if isinstance(prev, list) and prev:
            standings = prev

    meta = {
        "competition": "SuperLiga",
        "season": SEASON,
        "sources": {"matches": "lpf2.ro/html/etape", "standings": "lpf2.ro"},
        "generated_utc": iso_now(),
        "current_round": current_round,
        "rounds_fetched": {"from": start_r, "to": end_r},
        "status": {
            "current_round": cr_status,
            "matches": matches_status,
            "standings": standings_status,
            "rounds": rounds_status,
        },
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

    if changed:
        write_json_if_changed(os.path.join(OUTDIR, "meta.json"), meta)

    print("OK", meta)

if __name__ == "__main__":
    main()
