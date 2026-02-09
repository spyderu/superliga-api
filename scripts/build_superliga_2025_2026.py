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

SCRIPT_VERSION = "2026-02-09_force_v3"

SEASON = "2025-2026"
OUTDIR = os.path.join("public", "superliga", SEASON)

LPF2_ETAPA_URL = "https://lpf2.ro/html/etape/Etapa-{n}.html"

PAST_ROUNDS = 2
FUTURE_ROUNDS = 4
MAX_ROUNDS = 30

UA = "Mozilla/5.0 (compatible; superliga-api-bot/4.3; +https://github.com/spyderu/superliga-api)"

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
    s = s.replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s.strip())
    return s

def clean_line_for_match(line: str) -> str:
    # LPF2 pune "Image" foarte des + "Image: Caseta..."
    line = norm_space(line)
    line = re.sub(r"\bImage\b", " ", line)
    line = norm_space(line)
    # taie la primele markere "Image:" (Caseta/Rezumat etc.)
    if "Image:" in line:
        line = line.split("Image:", 1)[0]
    # taie orice resturi de meniu
    for cut in ["Tweet", "Follow @", "Meniu Principal", "Clasamentul etapei", "Pozitia Echipa"]:
        if cut in line:
            line = line.split(cut, 1)[0]
    return norm_space(line)

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

def extract_lines_from_etapa(round_no: int) -> List[str]:
    html = fetch_html(LPF2_ETAPA_URL.format(n=round_no))
    soup = BeautifulSoup(html, "lxml")
    # IMPORTANT: păstrăm liniile așa cum vin din pagină (ele sunt pe un singur rând)
    lines = [ln for ln in soup.get_text("\n").splitlines() if ln.strip()]
    return [norm_space(ln) for ln in lines]

# MATCH regex (căutare în linie, nu match la început)
DATE_TIME = r"(\d{1,2}\s+[A-Za-zăâîșțĂÂÎȘȚ]+\s+\d{4}),\s*(\d{1,2}:\d{2})"

RGX_FINISHED_SEARCH = re.compile(
    DATE_TIME + r".*?([A-Za-z0-9ăâîșțĂÂÎȘȚ .'-]+?)\s+(\d{1,2})-(\d{1,2})\s+([A-Za-z0-9ăâîșțĂÂÎȘȚ .'-]+)",
    re.UNICODE
)
RGX_SCHEDULED_SEARCH = re.compile(
    DATE_TIME + r".*?([A-Za-z0-9ăâîșțĂÂÎȘȚ .'-]+?)\s+-\s+([A-Za-z0-9ăâîșțĂÂÎȘȚ .'-]+)",
    re.UNICODE
)

def cleanup_team(s: str) -> str:
    s = norm_space(s)
    # taie dacă apar resturi
    for cut in ["Caseta", "Rezumatul", "comentariile", "Image", "Tweet", "Follow", "Meniu"]:
        if cut in s:
            s = s.split(cut, 1)[0]
    return norm_space(s)

def extract_matches_from_round(round_no: int) -> List[Dict]:
    lines = extract_lines_from_etapa(round_no)
    out: List[Dict] = []

    for raw in lines:
        line = clean_line_for_match(raw)
        if not line:
            continue

        m = RGX_FINISHED_SEARCH.search(line)
        if m:
            date_str, time_str, home, hs, as_, away = m.groups()
            dateEvent, timeEvent = parse_ro_datetime(date_str, time_str)
            out.append(match_obj(round_no, dateEvent, timeEvent,
                                 cleanup_team(home), cleanup_team(away),
                                 int(hs), int(as_)))
            continue

        m = RGX_SCHEDULED_SEARCH.search(line)
        if m:
            date_str, time_str, home, away = m.groups()
            dateEvent, timeEvent = parse_ro_datetime(date_str, time_str)
            out.append(match_obj(round_no, dateEvent, timeEvent,
                                 cleanup_team(home), cleanup_team(away),
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
    lines = extract_lines_from_etapa(round_no)

    start = None
    for i, ln in enumerate(lines):
        if norm_space(ln).lower().startswith("pozitia echipa meciuri victorii"):
            start = i + 1
            break
    if start is None:
        return []

    # Format real LPF2: "1CS Universitatea Craiova26 14 8 4 46-25 (21)50 (25)(+11)" :contentReference[oaicite:2]{index=2}
    rgx = re.compile(
        r"^(\d{1,2})\s*([A-Za-z0-9ăâîșțĂÂÎȘȚ .'-]+?)\s*(\d{1,2})\s+(\d{1,2})\s+(\d{1,2})\s+(\d{1,2})\s+"
        r"(\d{1,3})-(\d{1,3})\s*\(\s*([-+]?\d+)\s*\)\s*(\d{1,3})\s*\(\s*(\d{1,3})\s*\)\s*\(\s*([-+]?\d+)\s*\)\s*$",
        re.UNICODE
    )

    standings: List[Dict] = []
    for ln in lines[start:]:
        ln = norm_space(ln)
        if not ln or ln.lower().startswith("## meniu"):
            break
        if ln.lower().startswith("playout"):
            continue

        m = rgx.match(ln)
        if not m:
            continue

        pos = int(m.group(1))
        team = cleanup_team(m.group(2))
        played = int(m.group(3))
        win = int(m.group(4))
        draw = int(m.group(5))
        loss = int(m.group(6))
        gf = int(m.group(7))
        ga = int(m.group(8))
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
            "gd": gf - ga,
            "points": points,
            "adevar": adevar,
        })

        if len(standings) >= 16:
            break

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

    current_round, cr_status = find_latest_round_with_16_standings()

    standings = extract_standings_from_round(current_round)
    if len(standings) != 16:
        prev = read_existing(os.path.join(OUTDIR, "standings.json"))
        if isinstance(prev, list) and len(prev) == 16:
            standings = prev

    start_r = max(1, current_round - PAST_ROUNDS)
    end_r = min(MAX_ROUNDS, current_round + FUTURE_ROUNDS)

    all_matches: List[Dict] = []
    round_notes = []
    for r in range(start_r, end_r + 1):
        ms = extract_matches_from_round(r)
        round_notes.append({str(r): f"ok:{len(ms)}"})
        all_matches.extend(ms)

    # dacă nu scoate nimic, NU suprascriem cu gol
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

    write_json_if_changed(os.path.join(OUTDIR, "standings.json"), standings)
    write_json_if_changed(os.path.join(OUTDIR, "fixtures.json"), fixtures)
    write_json_if_changed(os.path.join(OUTDIR, "results.json"), results)
    write_json_if_changed(os.path.join(OUTDIR, "meta.json"), meta)

    print("SCRIPT_VERSION:", SCRIPT_VERSION)
    print("OK", meta)

if __name__ == "__main__":
    main()
