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

LPF_LIGA1_URL = "https://lpf.ro/liga-1"
LPF_ETAPA_URL = "https://lpf.ro/etape-liga-1/{round}"

LPF2_STANDINGS_URL = "https://lpf2.ro/"

PAST_ROUNDS = 2
FUTURE_ROUNDS = 3
MAX_ROUNDS = 30

UA = "Mozilla/5.0 (compatible; superliga-api-bot/1.3; +https://github.com/spyderu/superliga-api)"

RO_MONTH = {
    "ian": 1, "feb": 2, "mar": 3, "apr": 4, "mai": 5, "iun": 6,
    "iul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
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

def soup_text_lines(html: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text("\n")
    lines = [ln.strip() for ln in text.splitlines()]
    return [ln for ln in lines if ln]

def parse_current_round(lines: List[str]) -> int:
    for ln in lines:
        m = re.search(r"Clasament\s+SUPERLIGA\s*-\s*Etapa\s+(\d+)", ln, re.IGNORECASE)
        if m:
            return int(m.group(1))
    for ln in lines:
        m = re.search(r"\bEtapa\s+(\d{1,2})\b", ln, re.IGNORECASE)
        if m:
            return int(m.group(1))
    return 1

def is_datetime_line(ln: str) -> bool:
    return bool(re.match(r"^\d{1,2}\s+[a-zA-ZăâîșțĂÂÎȘȚ]{3}\s+\d{4},\s*\d{1,2}:\d{2}$", ln))

def parse_datetime_ro(ln: str) -> Optional[Tuple[str, str]]:
    m = re.match(r"^(\d{1,2})\s+([a-zA-ZăâîșțĂÂÎȘȚ]{3})\s+(\d{4}),\s*(\d{1,2}):(\d{2})$", ln)
    if not m:
        return None
    dd = int(m.group(1))
    mon = m.group(2).lower()
    yyyy = int(m.group(3))
    hh = int(m.group(4))
    mm = int(m.group(5))
    if mon not in RO_MONTH:
        return None
    dt = datetime(yyyy, RO_MONTH[mon], dd, hh, mm)
    return (dt.strftime("%Y-%m-%d"), dt.strftime("%H:%M:%S"))

def is_noise_line(ln: str) -> bool:
    l = ln.strip().lower()
    if l in {"data rezultat statistici", "statistici", "image", "etape tur", "etape retur"}:
        return True
    if l.startswith(("vineri", "sâmbătă", "sambata", "duminică", "duminica", "luni", "marți", "marti", "miercuri", "joi")):
        return True
    return False

def is_score_token(ln: str) -> bool:
    l = ln.strip()
    return l == "-" or re.fullmatch(r"\d{1,2}", l) is not None

def is_team_line(ln: str) -> bool:
    if is_noise_line(ln):
        return False
    if is_datetime_line(ln):
        return False
    l = ln.strip()
    if len(l) < 2:
        return False
    if re.match(r"^MJ\s+V\s+E", l):
        return False
    if re.search(r"\bEtapa\b", l, re.IGNORECASE):
        return False
    return any(ch.isalpha() for ch in l)

def match_obj(round_no: int, dateEvent: str, timeEvent: str, home: str, away: str, hs: Optional[int], as_: Optional[int]) -> Dict:
    status = "scheduled" if (hs is None or as_ is None) else "finished"
    played = status == "finished"
    return {
        "idEvent": None,
        "season": SEASON,
        "round": str(round_no),
        "dateEvent": dateEvent,
        "strTime": timeEvent,
        "kickoff_raw": f"{dateEvent}T{timeEvent}",

        # cheile folosite de noi
        "home": home,
        "away": away,
        "status": status,
        "score": {"home": hs, "away": as_},

        # ALIAS pentru aplicații care așteaptă alt schema
        "homeTeam": home,
        "awayTeam": away,
        "played": played,
        "intHomeScore": hs,
        "intAwayScore": as_,

        "venue": None,
        "city": None,
        "event": {"strEvent": f"{home} vs {away}", "strLeague": "SuperLiga"},
        "source": "LPF",
        "source_league_id": "lpf.ro",
    }

def extract_matches_from_round(round_no: int) -> List[Dict]:
    html = fetch_html(LPF_ETAPA_URL.format(round=round_no))
    lines = soup_text_lines(html)

    matches = []
    i = 0
    while i < len(lines):
        ln = lines[i]
        if is_datetime_line(ln):
            dt = parse_datetime_ro(ln)
            if not dt:
                i += 1
                continue
            dateEvent, timeEvent = dt

            j = i + 1
            while j < len(lines) and (is_noise_line(lines[j]) or is_score_token(lines[j])):
                j += 1
            if j >= len(lines) or not is_team_line(lines[j]):
                i += 1
                continue
            home = lines[j].strip()

            k = j + 1
            while k < len(lines) and not is_score_token(lines[k]):
                if is_noise_line(lines[k]) or re.fullmatch(r"\d{1,2}:\d{2}", lines[k].strip()):
                    k += 1
                    continue
                k += 1
            if k >= len(lines):
                i += 1
                continue
            hs_tok = lines[k].strip()

            a = k + 1
            while a < len(lines) and (is_noise_line(lines[a]) or is_score_token(lines[a]) or is_datetime_line(lines[a])):
                a += 1
            if a >= len(lines) or not is_team_line(lines[a]):
                i += 1
                continue
            away = lines[a].strip()

            b = a + 1
            while b < len(lines) and not is_score_token(lines[b]):
                if is_noise_line(lines[b]) or re.fullmatch(r"\d{1,2}:\d{2}", lines[b].strip()):
                    b += 1
                    continue
                b += 1
            if b >= len(lines):
                i += 1
                continue
            as_tok = lines[b].strip()

            hs = None if hs_tok == "-" else int(hs_tok)
            as_ = None if as_tok == "-" else int(as_tok)

            matches.append(match_obj(round_no, dateEvent, timeEvent, home, away, hs, as_))
            i = b + 1
            continue

        i += 1

    seen = set()
    uniq = []
    for m in matches:
        key = (m["round"], m["dateEvent"], m["strTime"], m["home"], m["away"])
        if key in seen:
            continue
        seen.add(key)
        uniq.append(m)
    return uniq

def parse_standings_from_lpf2() -> Tuple[List[Dict], str]:
    """
    Fix major: extrage PUNCTELE reale, nu "Adevăr".
    Căutăm explicit:
    ... gf-ga (...) POINTS (...) (ADEVĂR)
    """
    try:
        html = fetch_html(LPF2_STANDINGS_URL)
    except Exception as e:
        return [], f"lpf2_fetch_failed:{type(e).__name__}"

    soup = BeautifulSoup(html, "lxml")

    # găsește tabelul de clasament
    rows = None
    for t in soup.find_all("table"):
        txt = t.get_text(" ", strip=True).lower()
        if "pozitia" in txt and "puncte" in txt and ("meciuri" in txt or "victorii" in txt):
            rows = t.find_all("tr")
            break

    # fallback: parse din text brut
    if rows is None:
        lines = soup_text_lines(html)
        candidates = [ln for ln in lines if re.match(r"^\d{1,2}\s*\w", ln)]
    else:
        candidates = [tr.get_text(" ", strip=True) for tr in rows]

    standings = []

    # regex robust: pos team played win draw loss gf-ga (gd?) points (x) (adevar)
    rgx = re.compile(
        r"^\s*(\d{1,2})\s+(.+?)\s+(\d{1,2})\s+(\d{1,2})\s+(\d{1,2})\s+(\d{1,2})\s+(\d{1,3})\s*-\s*(\d{1,3})"
        r"(?:\s*\(\s*([+-]?\d+)\s*\))?"
        r"\s+(\d{1,3})"                      # POINTS (asta vrem!)
        r"(?:\s*\(\s*(\d{1,3})\s*\))?"
        r"(?:\s*\(\s*([+-]?\d+)\s*\))?\s*$",
        re.UNICODE
    )

    for txt in candidates:
        txt = re.sub(r"\s+", " ", txt).strip()
        m = rgx.match(txt)
        if not m:
            continue

        pos = int(m.group(1))
        team = m.group(2).strip()
        played = int(m.group(3))
        win = int(m.group(4))
        draw = int(m.group(5))
        loss = int(m.group(6))
        gf = int(m.group(7))
        ga = int(m.group(8))
        gd = gf - ga
        points = int(m.group(10))  # puncte reale
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
            "adevar": adevar,  # opțional, ca să-l ai dacă vrei
        })

    standings.sort(key=lambda x: x["position"])
    if len(standings) >= 12:
        return standings, "lpf2_ok"
    return standings, f"lpf2_incomplete:{len(standings)}"

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

def main():
    os.makedirs(OUTDIR, exist_ok=True)

    standings, standings_status = parse_standings_from_lpf2()

    fixtures = None
    results = None
    current_round = None
    matches_status = "not_started"
    rounds_fetched = None

    try:
        liga1_html = fetch_html(LPF_LIGA1_URL)
        liga1_lines = soup_text_lines(liga1_html)
        current_round = parse_current_round(liga1_lines)

        start_r = max(1, current_round - PAST_ROUNDS)
        end_r = min(MAX_ROUNDS, current_round + FUTURE_ROUNDS)
        rounds_fetched = {"from": start_r, "to": end_r}

        all_matches = []
        for r in range(start_r, end_r + 1):
            all_matches.extend(extract_matches_from_round(r))

        fixtures = [m for m in all_matches if m["status"] == "scheduled"]
        results = [m for m in all_matches if m["status"] == "finished"]

        fixtures.sort(key=lambda x: (x["dateEvent"], x["strTime"], x["home"], x["away"]))
        results.sort(key=lambda x: (x["dateEvent"], x["strTime"]), reverse=True)

        matches_status = f"lpf_ok_rounds:{start_r}-{end_r}"
    except Exception as e:
        matches_status = f"lpf_failed:{type(e).__name__}"
        fixtures = read_existing(os.path.join(OUTDIR, "fixtures.json"))
        results = read_existing(os.path.join(OUTDIR, "results.json"))

    if not standings:
        prev = read_existing(os.path.join(OUTDIR, "standings.json"))
        if isinstance(prev, list) and prev:
            standings = prev

    meta = {
        "competition": "SuperLiga (LPF + LPF2)",
        "season": SEASON,
        "sources": {"matches": "lpf.ro", "standings": "lpf2.ro"},
        "generated_utc": iso_now(),
        "current_round": current_round,
        "rounds_fetched": rounds_fetched,
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

    if changed:
        write_json_if_changed(os.path.join(OUTDIR, "meta.json"), meta)

    print("OK", meta)

if __name__ == "__main__":
    main()
