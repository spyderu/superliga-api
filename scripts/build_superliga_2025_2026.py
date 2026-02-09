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

# =========================
# VERSION MARKER (IMPORTANT)
# =========================
SCRIPT_VERSION = "2026-02-09_force_v2"

SEASON = "2025-2026"
OUTDIR = os.path.join("public", "superliga", SEASON)

LPF2_ETAPA_URL = "https://lpf2.ro/html/etape/Etapa-{n}.html"

PAST_ROUNDS = 2
FUTURE_ROUNDS = 4
MAX_ROUNDS = 30

UA = "Mozilla/5.0 (compatible; superliga-api-bot/4.1; +https://github.com/spyderu/superliga-api)"

RO_MONTH_FULL = {
    "ianuarie": 1, "februarie": 2, "martie": 3, "aprilie": 4, "mai": 5, "iunie": 6,
    "iulie": 7, "august": 8, "septembrie": 9, "octombrie": 10, "noiembrie": 11, "decembrie": 12,
}

# -------------------------
# HTTP
# -------------------------
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

# -------------------------
# IO helpers
# -------------------------
def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def stable_dumps(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

def sha256_str(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def write_json(path: str, obj) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

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

    write_json(path, obj)
    return True

def read_existing(path: str):
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

# -------------------------
# Parsing helpers
# -------------------------
def norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip())

def parse_ro_datetime(date_str: str, time_str: str) -> Tuple[str, str]:
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

def match_obj(round_no: int, dateEvent: str, timeEvent: str,
              home: str, away: str, hs: Optional[int], as_: Optional[int]) -> Dict:
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

        # alias pt aplicație
        "homeTeam": home,
        "awayTeam": away,
        "played": played,
        "intHomeScore": hs,
        "intAwayScore": as_,

        "source": "LPF2",
        "source_league_id": "lpf2.ro",
    }

RGX_FINISHED = re.compile(
    r"^(\d{1,2}\s+[A-Za-zăâîșțĂÂÎȘȚ]+\s+\d{4}),\s*(\d{1,2}:\d{2})\s+(.+?)\s+(\d{1,2})-(\d{1,2})\s+(.+)$",
    re.UNICODE
)
RGX_SCHEDULED = re.compile(
    r"^(\d{1,2}\s+[A-Za-zăâîșțĂÂÎȘȚ]+\s+\d{4}),\s*(\d{1,2}:\d{2})\s+(.+?)\s+-\s+(.+)$",
    re.UNICODE
)

def extract_lines_from_etapa(round_no: int) -> List[str]:
    html = fetch_html(LPF2_ETAPA_URL.format(n=round_no))
    soup = BeautifulSoup(html, "lxml")
    lines = [ln.strip() for ln in soup.get_text("\n").splitlines() if ln.strip()]
    return [norm_space(ln) for ln in lines]

def extract_matches_from_round(round_no: int) -> List[Dict]:
    lines = extract_lines_from_etapa(round_no)
    out: List[Dict] = []

    for ln in lines:
        m = RGX_FINISHED.match(ln)
        if m:
            date_str, time_str, home, hs, as_, away = m.groups()
            dateEvent, timeEvent = parse_ro_datetime(date_str, time_str)
            out.append(match_obj(round_no, dateEvent, timeEvent,
                                 norm_space(home), norm_space(away),
                                 int(hs), int(as_)))
            continue

        m = RGX_SCHEDULED.match(ln)
        if m:
            date_str, time_str, home, away = m.groups()
            dateEvent, timeEvent = parse_ro_datetime(date_str, time_str)
            out.append(match_obj(round_no, dateEvent, timeEvent,
                                 norm_space(home), norm_space(away),
                                 None, None))
            continue

    # dedupe
    uniq = []
    seen = set()
    for mm in out:
        key = (mm["round"], mm["dateEvent"], mm["strTime"], mm["home"], mm["away"])
        if key in seen:
            continue
        seen.add(key)
        uniq.append(mm)
    return uniq

def extract_standings_from_round(round_no: int) -> List[Dict]:
    """
    IMPORTANT: nu mai folosim 'textul întregii pagini'.
    Luăm doar liniile de după headerul tabelului și potrivim STRICT 16 rânduri.
    """
    lines = extract_lines_from_etapa(round_no)

    # găsim header-ul tabelului de clasament
    start = None
    for i, ln in enumerate(lines):
        if ln.lower().startswith("pozitia echipa meciuri victorii"):
            start = i + 1
            break
    if start is None:
        return []

    # rând de echipă în format LPF2 (din Etapa-26): ([lpf2.ro](https://lpf2.ro/html/etape/Etapa-26.html))
    # 1 CS Universitatea Craiova 26 14 8 4 46-25 (21) 50 (25) (+11)
    rgx = re.compile(
        r"^(\d{1,2})\s+(.+?)\s+(\d{1,2})\s+(\d{1,2})\s+(\d{1,2})\s+(\d{1,2})\s+(\d{1,3})-(\d{1,3})\s*\(\s*([-+]?\d+)\s*\)\s+(\d{1,3})\s*\(\s*(\d{1,3})\s*\)\s*\(\s*([-+]?\d+)\s*\)\s*$",
        re.UNICODE
    )

    standings: List[Dict] = []
    for ln in lines[start:]:
        # stop când se termină tabelul (după ce luăm 16)
        if len(standings) >= 16:
            break

        m = rgx.match(ln)
        if not m:
            continue

        pos = int(m.group(1))
        team = norm_space(m.group(2))
        played = int(m.group(3))
        win = int(m.group(4))
        draw = int(m.group(5))
        loss = int(m.group(6))
        gf = int(m.group(7))
        ga = int(m.group(8))
        gd = gf - ga
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
    return standings

def find_latest_round_with_16_standings() -> Tuple[int, str]:
    for r in range(MAX_ROUNDS, 0, -1):
        try:
            st = extract_standings_from_round(r)
            if len(st) == 16:
                return r, "latest_ok"
        except Exception:
            continue
    return 1, "latest_not_found"

def main():
    os.makedirs(OUTDIR, exist_ok=True)

    # 1) găsim etapa curentă (prima etapă de sus în jos cu 16 echipe în clasament)
    current_round, cr_status = find_latest_round_with_16_standings()

    # 2) standings (din etapa curentă)
    standings = extract_standings_from_round(current_round)
    if len(standings) != 16:
        prev = read_existing(os.path.join(OUTDIR, "standings.json"))
        if isinstance(prev, list) and len(prev) == 16:
            standings = prev

    # 3) meciuri din etape (curenta +/-)
    start_r = max(1, current_round - PAST_ROUNDS)
    end_r = min(MAX_ROUNDS, current_round + FUTURE_ROUNDS)

    all_matches: List[Dict] = []
    round_notes = []

    for r in range(start_r, end_r + 1):
        try:
            ms = extract_matches_from_round(r)
            round_notes.append({str(r): f"ok:{len(ms)}"})
            all_matches.extend(ms)
        except Exception as e:
            round_notes.append({str(r): f"fail:{type(e).__name__}"})

    # dacă nu avem minim 8 meciuri total, păstrăm vechile fișiere
    if len(all_matches) >= 8:
        fixtures = [m for m in all_matches if m["status"] == "scheduled"]
        results = [m for m in all_matches if m["status"] == "finished"]
        fixtures.sort(key=lambda x: (x["dateEvent"], x["strTime"], x["home"], x["away"]))
        results.sort(key=lambda x: (x["dateEvent"], x["strTime"]), reverse=True)
        matches_status = f"ok_total:{len(all_matches)}"
    else:
        fixtures = read_existing(os.path.join(OUTDIR, "fixtures.json")) or []
        results = read_existing(os.path.join(OUTDIR, "results.json")) or []
        matches_status = f"kept_old_parsed:{len(all_matches)}"

    meta = {
        "competition": "SuperLiga",
        "season": SEASON,
        "script_version": SCRIPT_VERSION,
        "generated_utc": iso_now(),
        "current_round": current_round,
        "rounds_fetched": {"from": start_r, "to": end_r},
        "status": {
            "current_round": cr_status,
            "matches": matches_status,
            "rounds": round_notes,
        },
        "counts": {
            "fixtures": len(fixtures),
            "results": len(results),
            "standings_rows": len(standings),
        },
    }

    # scriem fișierele (meta mereu, ca să vezi clar că a rulat codul)
    write_json_if_changed(os.path.join(OUTDIR, "standings.json"), standings)
    write_json_if_changed(os.path.join(OUTDIR, "fixtures.json"), fixtures)
    write_json_if_changed(os.path.join(OUTDIR, "results.json"), results)
    write_json_if_changed(os.path.join(OUTDIR, "meta.json"), meta)

    print("SCRIPT_VERSION:", SCRIPT_VERSION)
    print("OK", meta)

if __name__ == "__main__":
    main()
