import json
import os
import re
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple

import requests
from bs4 import BeautifulSoup

SEASON = "2025-2026"
OUTDIR = os.path.join("public", "superliga", SEASON)

LPF_LIGA1_URL = "https://lpf.ro/liga-1"
LPF_ETAPA_URL = "https://lpf.ro/etape-liga-1/{round}"

# Câte etape citim la fiecare rulare (ca să nu lovim site-ul cu 30 requesturi)
PAST_ROUNDS = 2
FUTURE_ROUNDS = 3
MAX_ROUNDS = 30  # sezon regulat tur-retur (LPF poate continua cu playoff/out, dar aici păstrăm 1..30)

UA = "Mozilla/5.0 (compatible; superliga-api-bot/1.0; +https://github.com/spyderu/superliga-api)"

RO_MONTH = {
    "ian": 1, "feb": 2, "mar": 3, "apr": 4, "mai": 5, "iun": 6,
    "iul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def fetch_html(url: str) -> str:
    r = requests.get(url, headers={"User-Agent": UA}, timeout=30)
    r.raise_for_status()
    return r.text

def html_to_lines(html: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text("\n")
    # normalize
    lines = [ln.strip() for ln in text.splitlines()]
    lines = [ln for ln in lines if ln]  # drop empty
    return lines

def parse_current_round(lines: List[str]) -> int:
    # Exemplu: "Clasament SUPERLIGA - Etapa 26"
    for ln in lines:
        m = re.search(r"Clasament\s+SUPERLIGA\s*-\s*Etapa\s+(\d+)", ln, re.IGNORECASE)
        if m:
            return int(m.group(1))

    # fallback: "Etapa 26 - Program"
    for ln in lines:
        m = re.search(r"Etapa\s+(\d+)\s*-\s*Program", ln, re.IGNORECASE)
        if m:
            return int(m.group(1))

    # fallback: dacă nu găsim, presupunem 1
    return 1

def is_datetime_line(ln: str) -> bool:
    # ex: "6 feb 2026, 17:00"
    return bool(re.match(r"^\d{1,2}\s+[a-zA-ZăâîșțĂÂÎȘȚ]{3}\s+\d{4},\s*\d{1,2}:\d{2}$", ln))

def parse_datetime_ro(ln: str) -> Optional[Tuple[str, str]]:
    # input: "6 feb 2026, 17:00" -> ("2026-02-06", "17:00:00")
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
    # filtrează zgomot tipic din pagini LPF
    bad = {
        "image", "statistici", "data rezultat statistici", "etape tur", "etape retur",
        "noutăți", "noutati", "utile", "contact", "superliga", "acasa", "acasă",
    }
    l = ln.strip().lower()
    if l in bad:
        return True
    # zilele săptămânii (RO)
    if l.startswith(("vineri", "sâmbătă", "sambata", "duminică", "duminica", "luni", "marți", "marti", "miercuri", "joi")):
        # ex: "Vineri, 6 feb 2026" (nu e datetime, e header)
        return True
    # headers
    if "locurile" in l and "play" in l:
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
    # nu vrem linii "MJ V E Î..." etc
    if re.match(r"^MJ\s+V\s+E", l):
        return False
    # team line: conține litere, nu e doar cifre
    has_letter = any(ch.isalpha() for ch in l)
    if not has_letter:
        return False
    # evită linii care sunt doar "Etapa 26" etc
    if re.search(r"\bEtapa\b", l, re.IGNORECASE):
        return False
    return True

def extract_standings_from_lines(lines: List[str]) -> List[Dict]:
    """
    Parsează blocul de clasament din pagina LPF:
    "Clasament SUPERLIGA - Etapa X"
    urmat de rânduri care încep cu poziția.
    """
    # găsește start
    start_idx = None
    for i, ln in enumerate(lines):
        if re.search(r"Clasament\s+SUPERLIGA\s*-\s*Etapa\s+\d+", ln, re.IGNORECASE):
            start_idx = i
            break
    if start_idx is None:
        return []

    # bloc până la "Locurile ..." sau final
    block = []
    for ln in lines[start_idx + 1:]:
        if re.search(r"Locurile\s+\d", ln, re.IGNORECASE):
            break
        block.append(ln)

    standings = []
    for ln in block:
        # rând valid începe cu poziție (ex: "1 UNIVERSITATEA CRAIOVA 25 14 ... 49")
        if not re.match(r"^\d{1,2}\s*", ln):
            continue

        # ia toate numerele cu poziții în string
        nums = list(re.finditer(r"-?\d+", ln))
        if len(nums) < 9:
            continue

        position = int(nums[0].group(0))
        played = int(nums[1].group(0))
        win = int(nums[2].group(0))
        draw = int(nums[3].group(0))
        loss = int(nums[4].group(0))
        gf = int(nums[5].group(0))
        ga = int(nums[6].group(0))
        gd = int(nums[-2].group(0))
        points = int(nums[-1].group(0))

        # team name = text între finalul poziției și începutul "played"
        team_raw = ln[nums[0].end():nums[1].start()].strip()
        # curăță caractere non-text evidente
        team = re.sub(r"[^\wăâîșțĂÂÎȘȚ .'\-]+", " ", team_raw).strip()
        team = re.sub(r"\s{2,}", " ", team)

        if not team:
            continue

        standings.append({
            "position": position,
            "team": team,
            "played": played,
            "win": win,
            "draw": draw,
            "loss": loss,
            "gf": gf,
            "ga": ga,
            "gd": gd,
            "points": points,
        })

    standings.sort(key=lambda x: x["position"])
    return standings

def extract_matches_from_lines(lines: List[str], round_no: int) -> List[Dict]:
    """
    Extrage meciurile din secțiunea de program a unei etape.
    Se bazează pe modelul: datetime -> home team -> score -> away team -> score.
    """
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

            # caută home team
            j = i + 1
            while j < len(lines) and (is_noise_line(lines[j]) or is_score_token(lines[j]) or (":" in lines[j] and not is_team_line(lines[j]))):
                j += 1
            if j >= len(lines) or not is_team_line(lines[j]):
                i += 1
                continue
            home = lines[j].strip()

            # caută home score (prima apariție de token scor după home)
            k = j + 1
            while k < len(lines) and not is_score_token(lines[k]):
                # skip "Statistici", imagini, etc
                if is_noise_line(lines[k]):
                    k += 1
                    continue
                # uneori apare ora "20:00" singură; o ignorăm
                if re.fullmatch(r"\d{1,2}:\d{2}", lines[k].strip()):
                    k += 1
                    continue
                k += 1
            if k >= len(lines):
                i += 1
                continue
            hs_tok = lines[k].strip()

            # caută away team
            a = k + 1
            while a < len(lines) and (is_noise_line(lines[a]) or is_score_token(lines[a]) or is_datetime_line(lines[a]) or re.fullmatch(r"\d{1,2}:\d{2}", lines[a].strip())):
                a += 1
            if a >= len(lines) or not is_team_line(lines[a]):
                i += 1
                continue
            away = lines[a].strip()

            # caută away score token după away
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

    # dedupe simplu (în caz de repetări în text)
    seen = set()
    uniq = []
    for m in matches:
        key = (m["round"], m["dateEvent"], m["strTime"], m["home"], m["away"])
        if key in seen:
            continue
        seen.add(key)
        uniq.append(m)
    return uniq

def write_json(path: str, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def main():
    os.makedirs(OUTDIR, exist_ok=True)

    # 1) Pagina principală LPF -> aflăm etapa curentă + luăm clasamentul complet
    liga1_html = fetch_html(LPF_LIGA1_URL)
    liga1_lines = html_to_lines(liga1_html)
    current_round = parse_current_round(liga1_lines)

    standings = extract_standings_from_lines(liga1_lines)

    # 2) Citim câteva etape în jurul etapei curente
    start_r = max(1, current_round - PAST_ROUNDS)
    end_r = min(MAX_ROUNDS, current_round + FUTURE_ROUNDS)

    all_matches = []
    for r in range(start_r, end_r + 1):
        html = fetch_html(LPF_ETAPA_URL.format(round=r))
        lines = html_to_lines(html)
        all_matches.extend(extract_matches_from_lines(lines, r))

    # 3) Separăm fixtures/results
    fixtures = [m for m in all_matches if m["status"] == "scheduled"]
    results = [m for m in all_matches if m["status"] == "finished"]

    fixtures.sort(key=lambda x: (x["dateEvent"], x["strTime"], x["home"], x["away"]))
    results.sort(key=lambda x: (x["dateEvent"], x["strTime"]), reverse=True)

    meta = {
        "competition": "SuperLiga (LPF)",
        "season": SEASON,
        "source": "LPF",
        "generated_utc": iso_now(),
        "current_round": current_round,
        "rounds_fetched": {"from": start_r, "to": end_r},
        "counts": {
            "fixtures": len(fixtures),
            "results": len(results),
            "standings_rows": len(standings),
        },
    }

    write_json(os.path.join(OUTDIR, "fixtures.json"), fixtures)
    write_json(os.path.join(OUTDIR, "results.json"), results)
    write_json(os.path.join(OUTDIR, "standings.json"), standings)
    write_json(os.path.join(OUTDIR, "meta.json"), meta)

    print("OK", meta)

if __name__ == "__main__":
    main()
