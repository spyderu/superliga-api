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

# Meciuri: lpf.ro (etape)
LPF_LIGA1_URL = "https://lpf.ro/liga-1"
LPF_ETAPA_URL = "https://lpf.ro/etape-liga-1/{round}"

# Clasament: lpf2.ro
LPF2_STANDINGS_URL = "https://lpf2.ro/"

PAST_ROUNDS = 2
FUTURE_ROUNDS = 3
MAX_ROUNDS = 30

UA = "Mozilla/5.0 (compatible; superliga-api-bot/1.2; +https://github.com/spyderu/superliga-api)"

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
    # timeout = (connect_timeout, read_timeout)
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

            # home
            j = i + 1
            while j < len(lines) and (is_noise_line(lines[j]) or is_score_token(lines[j])):
                j += 1
            if j >= len(lines) or not is_team_line(lines[j]):
                i += 1
                continue
            home = lines[j].strip()

            # home score
            k = j + 1
            while k < len(lines) and not is_score_token(lines[k]):
                if is_noise_line(lines[k]):
                    k += 1
                    continue
                if re.fullmatch(r"\d{1,2}:\d{2}", lines[k].strip()):
                    k += 1
                    continue
                k += 1
            if k >= len(lines):
                i += 1
                continue
            hs_tok = lines[k].strip()

            # away
            a = k + 1
            while a < len(lines) and (is_noise_line(lines[a]) or is_score_token(lines[a]) or is_datetime_line(lines[a])):
                a += 1
            if a >= len(lines) or not is_team_line(lines[a]):
                i += 1
                continue
            away = lines[a].strip()

            # away score
            b = a + 1
            while b < len(lines) and not is_score_token(lines[b]):
                if is_noise_line(lines[b]):
                    b += 1
                    continue
                if re.fullmatch(r"\d{1,2}:\d{2}", lines[b].strip()):
                    b += 1
                    continue
                b += 1
            if b >= len(lines):
                i += 1
                continue
            as_tok = lines[b].strip()

            hs = None if hs_tok == "-" else int(hs_tok)
            a_s = None if as_tok == "-" else int(as_tok)
            status = "scheduled" if (hs is None or a_s is None) else "finished"

            matches.append({
                "idEvent": None,
                "season": SEASON,
                "round": str(round_no),
                "dateEvent": dateEvent,
                "strTime": timeEvent,
                "kickoff_raw": f"{dateEvent}T{timeEvent}",
                "home": home,
                "away": away,
                "status": status,
                "score": {"home": hs, "away": a_s},
                "venue": None,
                "city": None,
                "event": {"strEvent": f"{home} vs {away}", "strLeague": "SuperLiga"},
                "source": "LPF",
                "source_league_id": "lpf.ro",
            })

            i = b + 1
            continue

        i += 1

    # dedupe
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
    try:
        html = fetch_html(LPF2_STANDINGS_URL)
    except Exception as e:
        return [], f"lpf2_fetch_failed:{type(e).__name__}"

    soup = BeautifulSoup(html, "lxml")

    # caută un tabel care conține "Pozitia" și "Puncte"
    best_rows = []
    for t in soup.find_all("table"):
        txt = t.get_text(" ", strip=True).lower()
        if "pozitia" in txt and "puncte" in txt and ("meciuri" in txt or "victorii" in txt):
            best_rows = t.find_all("tr")
            break

    standings = []

    def parse_row_text(txt: str) -> Optional[Dict]:
        txt = re.sub(r"\s+", " ", txt).strip()
        mpos = re.match(r"^(\d{1,2})\s*(.*)$", txt)
        if not mpos:
            return None
        pos = int(mpos.group(1))
        rest = mpos.group(2)

        # găsește primul număr = played
        mplayed = re.search(r"\b(\d{1,2})\b", rest)
        if not mplayed:
            return None
        team = rest[:mplayed.start()].strip()

        nums = re.findall(r"-?\d+", rest[mplayed.start():])
        if len(nums) < 5:
            return None

        played = int(nums[0])
        win = int(nums[1]) if len(nums) > 1 else None
        draw = int(nums[2]) if len(nums) > 2 else None
        loss = int(nums[3]) if len(nums) > 3 else None

        mg = re.search(r"(\d{1,2})\s*-\s*(\d{1,2})", rest)
        gf = ga = None
        if mg:
            gf = int(mg.group(1))
            ga = int(mg.group(2))

        points = int(nums[-1])
        gd = (gf - ga) if (gf is not None and ga is not None) else None

        if not team:
            return None

        return {
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
        }

    if best_rows:
        for tr in best_rows:
            row = parse_row_text(tr.get_text(" ", strip=True))
            if row:
                standings.append(row)
    else:
        # fallback text
        lines = soup_text_lines(html)
        for ln in lines:
            if re.match(r"^\d{1,2}\s*\w", ln):
                row = parse_row_text(ln)
                if row:
                    standings.append(row)

    standings = [s for s in standings if s.get("team")]
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
                return False  # no change
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

    # ---- STANDINGS (LPF2) ----
    standings, standings_status = parse_standings_from_lpf2()

    # ---- MATCHES (LPF) ----
    fixtures = None
    results = None
    current_round = None
    matches_status = "not_started"

    try:
        liga1_html = fetch_html(LPF_LIGA1_URL)
        liga1_lines = soup_text_lines(liga1_html)
        current_round = parse_current_round(liga1_lines)

        start_r = max(1, current_round - PAST_ROUNDS)
        end_r = min(MAX_ROUNDS, current_round + FUTURE_ROUNDS)

        all_matches = []
        for r in range(start_r, end_r + 1):
            all_matches.extend(extract_matches_from_round(r))

        fixtures = [m for m in all_matches if m["status"] == "scheduled"]
        results = [m for m in all_matches if m["status"] == "finished"]

        fixtures.sort(key=lambda x: (x["dateEvent"], x["strTime"], x["home"], x["away"]))
        results.sort(key=lambda x: (x["dateEvent"], x["strTime"]), reverse=True)

        matches_status = f"lpf_ok_rounds:{start_r}-{end_r}"
    except Exception as e:
        # dacă LPF blochează runner-ul, NU stricăm fișierele existente
        matches_status = f"lpf_failed:{type(e).__name__}"
        fixtures = read_existing(os.path.join(OUTDIR, "fixtures.json"))
        results = read_existing(os.path.join(OUTDIR, "results.json"))

    # dacă nici standings nu s-a putut lua, păstrăm vechiul
    if not standings:
        prev = read_existing(os.path.join(OUTDIR, "standings.json"))
        if isinstance(prev, list) and prev:
            standings = prev

    # meta: îl schimbăm doar dacă se schimbă efectiv datele (prin write_json_if_changed)
    meta = {
        "competition": "SuperLiga (LPF + LPF2)",
        "season": SEASON,
        "sources": {"matches": "lpf.ro", "standings": "lpf2.ro"},
        "generated_utc": iso_now(),
        "current_round": current_round,
        "status": {
            "matches": matches_status,
            "standings": standings_status,
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

    # scriem meta doar dacă s-a schimbat ceva (ca să nu facă commit mereu)
    if changed:
        write_json_if_changed(os.path.join(OUTDIR, "meta.json"), meta)

    print("OK", meta)

if __name__ == "__main__":
    main()
