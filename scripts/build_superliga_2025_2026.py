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

LPF2_HOME = "https://lpf2.ro/"
LPF2_ETAPA_URL = "https://lpf2.ro/html/etape/Etapa-{n}.html"

PAST_ROUNDS = 2
FUTURE_ROUNDS = 4
MAX_ROUNDS = 30

UA = "Mozilla/5.0 (compatible; superliga-api-bot/3.1; +https://github.com/spyderu/superliga-api)"

RO_MONTH_FULL = {
    "ianuarie": 1, "februarie": 2, "martie": 3, "aprilie": 4, "mai": 5, "iunie": 6,
    "iulie": 7, "august": 8, "septembrie": 9, "octombrie": 10, "noiembrie": 11, "decembrie": 12,
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
    # date_str: "06 Februarie 2026" | time_str: "17:00"
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

        # alias pt aplicație
        "homeTeam": home,
        "awayTeam": away,
        "played": played,
        "intHomeScore": hs,
        "intAwayScore": as_,

        "source": "LPF2",
        "source_league_id": "lpf2.ro",
    }

def extract_standings_from_lpf2() -> Tuple[List[Dict], str]:
    """
    IMPORTANT: nu mai parsăm textul întregii pagini.
    Căutăm un <table> care are headere: Pozitia, Echipa, Meciuri, ... Puncte
    și extragem doar rândurile lui.
    """
    try:
        html = fetch_html(LPF2_HOME)
    except Exception as e:
        return [], f"standings_fetch_failed:{type(e).__name__}"

    soup = BeautifulSoup(html, "lxml")

    target_table = None
    for t in soup.find_all("table"):
        hdr = t.get_text(" ", strip=True).lower()
        if "pozitia" in hdr and "echipa" in hdr and "puncte" in hdr:
            target_table = t
            break

    if target_table is None:
        return [], "standings_table_not_found"

    rows = target_table.find_all("tr")
    out: List[Dict] = []

    for tr in rows:
        tds = tr.find_all(["td", "th"])
        # ne așteptăm la ~9-10 coloane
        cells = [re.sub(r"\s+", " ", td.get_text(" ", strip=True)) for td in tds]
        if not cells:
            continue

        # prima coloană trebuie să fie poziție numerică
        if not re.fullmatch(r"\d{1,2}", cells[0]):
            continue

        # layout tipic: pos, echipa, meciuri, victorii, egaluri, infrangeri, golaveraj, puncte, adevar (uneori)
        pos = int(cells[0])
        team = norm_team(cells[1]) if len(cells) > 1 else ""
        played = int(cells[2]) if len(cells) > 2 and cells[2].isdigit() else None
        win = int(cells[3]) if len(cells) > 3 and cells[3].isdigit() else None
        draw = int(cells[4]) if len(cells) > 4 and cells[4].isdigit() else None
        loss = int(cells[5]) if len(cells) > 5 and cells[5].isdigit() else None

        gf = ga = gd = None
        if len(cells) > 6:
            mg = re.match(r"^\s*(\d{1,3})\s*-\s*(\d{1,3})\s*$", cells[6])
            if mg:
                gf = int(mg.group(1))
                ga = int(mg.group(2))
                gd = gf - ga

        points = None
        if len(cells) > 7 and re.fullmatch(r"-?\d{1,3}", cells[7]):
            points = int(cells[7])

        adevar = None
        if len(cells) > 8 and re.fullmatch(r"-?\d{1,3}", cells[8]):
            adevar = int(cells[8])

        if not team or points is None:
            continue

        out.append({
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

    if len(out) >= 12:
        out.sort(key=lambda x: x["position"])
        return out, "standings_ok"

    return out, f"standings_incomplete:{len(out)}"

def guess_current_round_from_standings(standings: List[Dict]) -> int:
    # etapă curentă aproximată = max(played) în clasament
    played_vals = [r.get("played") for r in standings if isinstance(r.get("played"), int)]
    return max(played_vals) if played_vals else 1

def extract_round_matches_lpf2(round_no: int) -> Tuple[List[Dict], str]:
    """
    Parsare structurată: încercăm să extragem din tabelele din Etapa-N.html.
    Dacă nu găsim, întoarcem [] și status, dar NU stricăm fișierele existente.
    """
    url = LPF2_ETAPA_URL.format(n=round_no)
    try:
        html = fetch_html(url)
    except Exception as e:
        return [], f"round_fetch_failed:{round_no}:{type(e).__name__}"

    soup = BeautifulSoup(html, "lxml")

    # Strângem toate textele rândurilor din tabele; în Etapa-* există de regulă tabele pentru meciuri.
    raw_rows = []
    for t in soup.find_all("table"):
        for tr in t.find_all("tr"):
            cells = [re.sub(r"\s+", " ", td.get_text(" ", strip=True)) for td in tr.find_all(["td", "th"])]
            if cells:
                raw_rows.append(cells)

    matches: List[Dict] = []
    # Heuristic: rând cu dată+oră + echipe + scor sau "-"
    # Acceptăm două formate:
    # - [data, ora, gazde, scor, oaspeti]
    # - [data, ora, gazde, hg, ag, oaspeti] (uneori scor separat)
    for cells in raw_rows:
        joined = " | ".join(cells).lower()
        if "statistici" in joined:
            continue

        # caută data ro + ora
        # ex: "06 Februarie 2026" și "17:00"
        date_cell = None
        time_cell = None
        for c in cells:
            if re.search(r"\b(ianuarie|februarie|martie|aprilie|mai|iunie|iulie|august|septembrie|octombrie|noiembrie|decembrie)\b", c.lower()):
                date_cell = c
            if re.fullmatch(r"\d{1,2}:\d{2}", c):
                time_cell = c

        if not date_cell or not time_cell:
            continue

        # echipe: luăm primele două celule care arată ca nume (au litere) și nu sunt data/ora
        teams = [c for c in cells if c != date_cell and c != time_cell and any(ch.isalpha() for ch in c)]
        if len(teams) < 2:
            continue
        home = norm_team(teams[0])
        away = norm_team(teams[1])

        # scor: căutăm "x y" sau "x-y" sau "-"
        hs = as_ = None
        score_found = False
        for c in cells:
            m = re.fullmatch(r"(\d{1,2})\s*-\s*(\d{1,2})", c)
            if m:
                hs, as_ = int(m.group(1)), int(m.group(2))
                score_found = True
                break
        if not score_found:
            # scor în două celule consecutive numeric
            nums = [c for c in cells if re.fullmatch(r"\d{1,2}", c)]
            if len(nums) >= 2:
                hs, as_ = int(nums[0]), int(nums[1])
                score_found = True
            else:
                # programat (fără scor)
                hs, as_ = None, None

        try:
            dateEvent, timeEvent = parse_ro_datetime(date_cell, time_cell)
        except Exception:
            continue

        matches.append(match_obj(round_no, dateEvent, timeEvent, home, away, hs, as_))

    # dedupe
    uniq = []
    seen = set()
    for m in matches:
        key = (m["round"], m["dateEvent"], m["strTime"], m["home"], m["away"])
        if key in seen:
            continue
        seen.add(key)
        uniq.append(m)

    # dacă nu am măcar 6-8 meciuri, considerăm că parsarea a eșuat
    if len(uniq) < 6:
        return [], f"round_parsed_too_few:{round_no}:{len(uniq)}"
    return uniq, "round_ok"

def main():
    os.makedirs(OUTDIR, exist_ok=True)

    # 1) STANDINGS – stabil (tabel)
    standings, standings_status = extract_standings_from_lpf2()
    if not standings:
        prev = read_existing(os.path.join(OUTDIR, "standings.json"))
        if isinstance(prev, list) and prev:
            standings = prev

    current_round = guess_current_round_from_standings(standings)
    start_r = max(1, current_round - PAST_ROUNDS)
    end_r = min(MAX_ROUNDS, current_round + FUTURE_ROUNDS)

    # 2) MATCHES – încercăm etapele din LPF2; dacă nu reușim, păstrăm vechile fișiere
    all_matches: List[Dict] = []
    rounds_status = []

    for r in range(start_r, end_r + 1):
        ms, st = extract_round_matches_lpf2(r)
        rounds_status.append({str(r): st})
        all_matches.extend(ms)

    if len(all_matches) >= 10:
        fixtures = [m for m in all_matches if m["status"] == "scheduled"]
        results = [m for m in all_matches if m["status"] == "finished"]
        fixtures.sort(key=lambda x: (x["dateEvent"], x["strTime"], x["home"], x["away"]))
        results.sort(key=lambda x: (x["dateEvent"], x["strTime"]), reverse=True)
        matches_status = f"matches_ok_rounds:{start_r}-{end_r}"
    else:
        fixtures = read_existing(os.path.join(OUTDIR, "fixtures.json")) or []
        results = read_existing(os.path.join(OUTDIR, "results.json")) or []
        matches_status = f"matches_kept_old (parsed={len(all_matches)})"

    meta = {
        "competition": "SuperLiga",
        "season": SEASON,
        "sources": {"matches": "lpf2.ro/html/etape", "standings": "lpf2.ro (table)"},
        "generated_utc": iso_now(),
        "current_round": current_round,
        "rounds_fetched": {"from": start_r, "to": end_r},
        "status": {
            "standings": standings_status,
            "matches": matches_status,
            "rounds": rounds_status,
        },
        "counts": {
            "fixtures": len(fixtures),
            "results": len(results),
            "standings_rows": len(standings),
        },
    }

    changed = False
    changed |= write_json_if_changed(os.path.join(OUTDIR, "standings.json"), standings)
    changed |= write_json_if_changed(os.path.join(OUTDIR, "fixtures.json"), fixtures)
    changed |= write_json_if_changed(os.path.join(OUTDIR, "results.json"), results)
    if changed:
        write_json_if_changed(os.path.join(OUTDIR, "meta.json"), meta)

    print("OK", meta)

if __name__ == "__main__":
    main()
