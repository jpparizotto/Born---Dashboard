# -*- coding: utf-8 -*-
"""
Relatório de ocupação por dia / modalidade / horário / período (EVO API)
Baseado no seu script anterior (coleta dia a dia, showFullWeek=false, carimbo _requestedDate).
Novidades:
- Quebra por períodos:
    Manhã  -> até 12:00
    Tarde  -> 12:00 até 17:30 (inclusive)
    Noite  -> após 17:30
- Seleção de datas no topo (DATE_FROM / DATE_TO). Se None, usa DAYS_AHEAD.
- CSV adicional por período.
"""

import base64
import requests
import datetime as dt
import csv
import os
from collections import defaultdict, Counter
from datetime import datetime
import sys
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass
# ========= CONFIG =========
EVO_USER = "evo"
EVO_TOKEN = "96FB2B6A-0098-433F-A9E1-96746C80FAF1"
BASE_URL = "https://evo-integracao.w12app.com.br/api/v1"
VERIFY_SSL = True

# Defina um intervalo manual aqui (YYYY-MM-DD). Se ambos forem None, usa DAYS_AHEAD.
DATE_FROM = "2025-10-29"  # ex.: "2025-10-28"
DATE_TO   = "2025-11-18"  # ex.: "2025-11-12"
DAYS_AHEAD = 15

WRITE_CSV = True
CSV_DIR = "evo_ocupacao"

# ========= AUTH =========
auth_str = f"{EVO_USER}:{EVO_TOKEN}"
b64_auth = base64.b64encode(auth_str.encode()).decode()
HEADERS = {
    "Authorization": f"Basic {b64_auth}",
    "Content-Type": "application/json",
    "Accept": "application/json",
}

# ========= HELPERS =========
def first(obj, *keys, default=None):
    for k in keys:
        if isinstance(obj, dict) and k in obj and obj[k] not in (None, "", []):
            return obj[k]
    return default

def get_json(url, params=None):
    r = requests.get(url, headers=HEADERS, params=params or {}, verify=VERIFY_SSL, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"GET {url} -> {r.status_code} | {r.text[:300]}")
    try:
        return r.json()
    except Exception as e:
        raise RuntimeError(f"Falha ao interpretar JSON de {url}: {e}\nCorpo: {r.text[:500]}")

def to_list(maybe_list_or_dict, key="data"):
    if isinstance(maybe_list_or_dict, list):
        return maybe_list_or_dict
    if isinstance(maybe_list_or_dict, dict):
        if key in maybe_list_or_dict and isinstance(maybe_list_or_dict[key], list):
            return maybe_list_or_dict[key]
        for v in maybe_list_or_dict.values():
            if isinstance(v, list):
                return v
    return []

def normalize_date_only(s):
    if not s:
        return s
    if isinstance(s, str) and "T" in s:
        return s.split("T", 1)[0]
    return s

def daterange_params(days=DAYS_AHEAD):
    today = dt.date.today()
    date_from = today.strftime("%Y-%m-%d")
    date_to = (today + dt.timedelta(days=days)).strftime("%Y-%m-%d")
    return {"dateFrom": date_from, "dateTo": date_to}

def each_date_list(date_from, date_to):
    d0 = dt.date.fromisoformat(date_from)
    d1 = dt.date.fromisoformat(date_to)
    cur = d0
    out = []
    while cur <= d1:
        out.append(cur.isoformat())
        cur += dt.timedelta(days=1)
    return out

def safe_int(x):
    try:
        return int(x)
    except Exception:
        return None

def hhmm_to_minutes(hhmm: str):
    try:
        hh, mm = hhmm[:5].split(":")
        return int(hh) * 60 + int(mm)
    except Exception:
        return None

def time_band(hhmm: str) -> str:
    """
    Retorna 'Manhã', 'Tarde' ou 'Noite' com base na hora de início do slot.
      - Manhã:   < 12:00
      - Tarde:   12:00 até 17:30 (inclusive)
      - Noite:   > 17:30
    """
    m = hhmm_to_minutes(hhmm)
    if m is None:
        return "Indefinido"
    noon = 12 * 60
    five_thirty = 17 * 60 + 30
    if m < noon:
        return "Manhã"
    elif noon <= m <= five_thirty:
        return "Tarde"
    else:
        return "Noite"

# ========= COLETA (MESMA LÓGICA BASE) =========
def listar_atividades():
    data = get_json(f"{BASE_URL}/activities")
    atividades = to_list(data, key="data")
    if not atividades:
        try:
            alt = get_json(f"{BASE_URL}/service")
            atividades = to_list(alt, key="data")
        except Exception:
            pass
    res = []
    for a in atividades:
        res.append({
            "name": first(a, "name", "description", "title", default="(Sem nome)"),
            "id": first(a, "id", "idActivity", "activityId", "ID", "Id"),
        })
    return res

def listar_id_branch():
    try:
        cfg = get_json(f"{BASE_URL}/configuration")
        lst = to_list(cfg, key="data")
        for b in lst:
            bid = first(b, "idBranch", "branchId", "id", "Id")
            if bid:
                return bid
    except Exception:
        pass
    return None

def fetch_agenda_dia(d_iso, id_branch=None):
    params = {
        "date": f"{d_iso}T00:00:00",
        "showFullWeek": "false"
    }
    if id_branch:
        params["idBranch"] = id_branch

    data = get_json(f"{BASE_URL}/activities/schedule", params=params)
    items = to_list(data, key="data")
    for it in items:
        it["_requestedDate"] = d_iso
    return items

def fetch_agenda_periodo(date_from, date_to):
    id_branch = listar_id_branch()
    all_items = []
    for d_iso in each_date_list(date_from, date_to):
        try:
            items = fetch_agenda_dia(d_iso, id_branch=id_branch)
            print(f"📅 Dia {d_iso}: {len(items)} itens")
            all_items.extend(items)
        except Exception as e:
            print(f"⚠️ Falha ao coletar {d_iso}: {e}")
    return all_items

# ========= NORMALIZAÇÃO PARA LINHAS =========
def materialize_rows(atividades, agenda_items):
    """
    Linhas normalizadas por slot:
      Data, Atividade, Início, Fim, Horario(HH:MM), Capacidade, Disponíveis, Bookados, Periodo, ActivityId
    OBS: usa _requestedDate como Data (prioridade).
    """
    rows = []
    act_names = {a["name"].strip().lower(): a for a in atividades if a["name"]}

    for h in agenda_items:
        act_name_item = first(h, "name", "activityDescription", "activityName", "description")
        if act_name_item:
            act_key = act_name_item.strip().lower()
            act_resolved = act_names.get(act_key)
            act_name_final = act_resolved["name"] if act_resolved else act_name_item
            act_id_final = act_resolved["id"] if act_resolved else first(h, "idActivity", "activityId", "id", "Id")
        else:
            act_name_final = "(Sem atividade)"
            act_id_final = first(h, "idActivity", "activityId", "id", "Id")

        date = first(h, "_requestedDate")
        if not date:
            date = normalize_date_only(first(h, "activityDate", "date", "classDate", "day", "scheduleDate"))

        hour_start = first(h, "startTime", "hourStart", "timeStart", "startHour")
        hour_end = first(h, "endTime", "hourEnd", "timeEnd", "endHour")

        # Rótulo HH:MM
        horario_label = None
        if hour_start and isinstance(hour_start, str):
            horario_label = hour_start[:5]
        else:
            horario_label = first(h, "time", "hour", default=None)

        capacity  = safe_int(first(h, "capacity", "spots", "vacanciesTotal", "maxStudents", "maxCapacity"))
        available = safe_int(first(h, "available", "vacancies"))
        enrolled  = safe_int(first(h, "ocupation", "spotsFilled", "occupied", "enrolled", "registrations"))

        if enrolled is not None:
            booked = enrolled
        elif capacity is not None and available is not None:
            booked = max(0, capacity - available)
        else:
            booked = None

        if date and horario_label:
            rows.append({
                "Data": date,
                "Atividade": act_name_final,
                "Início": hour_start,
                "Fim": hour_end,
                "Horario": horario_label,
                "Capacidade": capacity,
                "Disponíveis": available,
                "Bookados": booked,
                "Periodo": time_band(horario_label),
                "ActivityId": act_id_final
            })

    rows.sort(key=lambda r: (r["Data"], r["Horario"], r["Atividade"]))
    return rows

# ========= MÉTRICAS =========
def compute_metrics(rows):
    """
    metrics_by_day[Data] = {
        total_slots, total_capacity, total_booked, occ_pct,
        by_modality: { atividade -> {slots, capacity, booked, occ_pct} },
        by_hour:     { HH:MM    -> {slots, capacity, booked, occ_pct} },
        by_period:   { Periodo  -> {slots, capacity, booked, occ_pct} },
    }
    """
    by_day = {}

    for r in rows:
        d = r["Data"]
        by_day.setdefault(d, {
            "total_slots": 0,
            "total_capacity": 0,
            "total_booked": 0,
            "by_modality": defaultdict(lambda: {"slots": 0, "capacity": 0, "booked": 0}),
            "by_hour": defaultdict(lambda: {"slots": 0, "capacity": 0, "booked": 0}),
            "by_period": defaultdict(lambda: {"slots": 0, "capacity": 0, "booked": 0}),
        })

        cap = r["Capacidade"] if r["Capacidade"] is not None else 0
        booked = r["Bookados"] if r["Bookados"] is not None else 0
        per = r.get("Periodo") or "Indefinido"

        by_day[d]["total_slots"] += 1
        by_day[d]["total_capacity"] += cap
        by_day[d]["total_booked"] += booked

        md = by_day[d]["by_modality"][r["Atividade"]]
        md["slots"] += 1; md["capacity"] += cap; md["booked"] += booked

        hh = by_day[d]["by_hour"][r["Horario"]]
        hh["slots"] += 1; hh["capacity"] += cap; hh["booked"] += booked

        bp = by_day[d]["by_period"][per]
        bp["slots"] += 1; bp["capacity"] += cap; bp["booked"] += booked

    # porcentagens
    for d, agg in by_day.items():
        agg["occ_pct"] = (agg["total_booked"] / agg["total_capacity"] * 100) if agg["total_capacity"] else 0.0

        agg["by_modality"] = {
            k: {**v, "occ_pct": (v["booked"] / v["capacity"] * 100) if v["capacity"] else 0.0}
            for k, v in sorted(agg["by_modality"].items(), key=lambda kv: kv[0])
        }

        agg["by_hour"] = {
            k: {**v, "occ_pct": (v["booked"] / v["capacity"] * 100) if v["capacity"] else 0.0}
            for k, v in sorted(agg["by_hour"].items(), key=lambda kv: kv[0])
        }

        # Ordem definida: Manhã, Tarde, Noite, Indefinido
        period_order = {"Manhã": 0, "Tarde": 1, "Noite": 2, "Indefinido": 3}
        agg["by_period"] = {
            k: {**v, "occ_pct": (v["booked"] / v["capacity"] * 100) if v["capacity"] else 0.0}
            for k, v in sorted(agg["by_period"].items(), key=lambda kv: period_order.get(kv[0], 9))
        }


    return dict(sorted(by_day.items(), key=lambda kv: kv[0]))

# ========= CSVs =========
def ensure_dir(p):
    if not os.path.isdir(p):
        os.makedirs(p, exist_ok=True)

def save_csv_rows(rows, date_from, date_to):
    ensure_dir(CSV_DIR)
    path = os.path.abspath(os.path.join(CSV_DIR, f"slots_{date_from}_a_{date_to}.csv"))
    cols = ["Data", "Horario", "Periodo", "Atividade", "Início", "Fim", "Capacidade", "Disponíveis", "Bookados", "ActivityId"]
    tmp = path
    try:
        with open(tmp, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            for r in rows:
                w.writerow({c: r.get(c, "") for c in cols})
    except PermissionError:
        tmp = os.path.abspath(os.path.join(CSV_DIR, f"slots_{date_from}_a_{date_to}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"))
        with open(tmp, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            for r in rows:
                w.writerow({c: r.get(c, "") for c in cols})
    return tmp

def save_csv_metrics(metrics_by_day, date_from, date_to):
    ensure_dir(CSV_DIR)
    # 1) Resumo por dia
    path_summary = os.path.abspath(os.path.join(CSV_DIR, f"resumo_{date_from}_a_{date_to}.csv"))
    try:
        with open(path_summary, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["Data", "Slots", "Vagas", "Bookados", "Ocupacao%"])
            for d, m in metrics_by_day.items():
                w.writerow([d, m["total_slots"], m["total_capacity"], m["total_booked"], round(m["occ_pct"], 1)])
    except PermissionError:
        path_summary = os.path.abspath(os.path.join(CSV_DIR, f"resumo_{date_from}_a_{date_to}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"))
        with open(path_summary, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["Data", "Slots", "Vagas", "Bookados", "Ocupacao%"])
            for d, m in metrics_by_day.items():
                w.writerow([d, m["total_slots"], m["total_capacity"], m["total_booked"], round(m["occ_pct"], 1)])

    # 2) Ocupação por modalidade
    path_mod = os.path.abspath(os.path.join(CSV_DIR, f"ocupacao_por_modalidade_{date_from}_a_{date_to}.csv"))
    try:
        with open(path_mod, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["Data", "Atividade", "Slots", "Vagas", "Bookados", "Ocupacao%"])
            for d, m in metrics_by_day.items():
                for act, v in m["by_modality"].items():
                    w.writerow([d, act, v["slots"], v["capacity"], v["booked"], round(v["occ_pct"], 1)])
    except PermissionError:
        path_mod = os.path.abspath(os.path.join(CSV_DIR, f"ocupacao_por_modalidade_{date_from}_a_{date_to}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"))
        with open(path_mod, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["Data", "Atividade", "Slots", "Vagas", "Bookados", "Ocupacao%"])
            for d, m in metrics_by_day.items():
                for act, v in m["by_modality"].items():
                    w.writerow([d, act, v["slots"], v["capacity"], v["booked"], round(v["occ_pct"], 1)])

    # 3) Ocupação por horário
    path_hr = os.path.abspath(os.path.join(CSV_DIR, f"ocupacao_por_horario_{date_from}_a_{date_to}.csv"))
    try:
        with open(path_hr, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["Data", "Horario", "Slots", "Vagas", "Bookados", "Ocupacao%"])
            for d, m in metrics_by_day.items():
                for hr, v in m["by_hour"].items():
                    w.writerow([d, hr, v["slots"], v["capacity"], v["booked"], round(v["occ_pct"], 1)])
    except PermissionError:
        path_hr = os.path.abspath(os.path.join(CSV_DIR, f"ocupacao_por_horario_{date_from}_a_{date_to}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"))
        with open(path_hr, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["Data", "Horario", "Slots", "Vagas", "Bookados", "Ocupacao%"])
            for d, m in metrics_by_day.items():
                for hr, v in m["by_hour"].items():
                    w.writerow([d, hr, v["slots"], v["capacity"], v["booked"], round(v["occ_pct"], 1)])

    # 4) Ocupação por período (Manhã/Tarde/Noite)
    path_prd = os.path.abspath(os.path.join(CSV_DIR, f"ocupacao_por_periodo_{date_from}_a_{date_to}.csv"))
    try:
        with open(path_prd, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["Data", "Periodo", "Slots", "Vagas", "Bookados", "Ocupacao%"])
            for d, m in metrics_by_day.items():
                for prd, v in m["by_period"].items():
                    w.writerow([d, prd, v["slots"], v["capacity"], v["booked"], round(v["occ_pct"], 1)])
    except PermissionError:
        path_prd = os.path.abspath(os.path.join(CSV_DIR, f"ocupacao_por_periodo_{date_from}_a_{date_to}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"))
        with open(path_prd, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["Data", "Periodo", "Slots", "Vagas", "Bookados", "Ocupacao%"])
            for d, m in metrics_by_day.items():
                for prd, v in m["by_period"].items():
                    w.writerow([d, prd, v["slots"], v["capacity"], v["booked"], round(v["occ_pct"], 1)])

    return path_summary, path_mod, path_hr, path_prd

# ========= MAIN =========
def main():
    # 0) Período selecionado
    if DATE_FROM and DATE_TO:
        date_from, date_to = DATE_FROM, DATE_TO
    else:
        p = daterange_params(DAYS_AHEAD)
        date_from, date_to = p["dateFrom"], p["dateTo"]

    # 1) atividades
    atividades = listar_atividades()
    print("Atividades encontradas:")
    for a in atividades:
        print(f"- {a['name']} (id={a['id']})")

    print(f"\n Coletando grade de {date_from} a {date_to} ...")

    # 2) agenda dia a dia
    agenda_all = fetch_agenda_periodo(date_from, date_to)
    if not agenda_all:
        print("️ Nenhum item retornado pela rota de agenda.")
        return

    # 3) materializa slots
    rows = materialize_rows(atividades, agenda_all)
    print(f" Total de slots materializados: {len(rows)}")

    # 4) métricas
    metrics_by_day = compute_metrics(rows)

    # 5) prints (inclui Períodos)
    print("\n OCUPAÇÃO — PRÓXIMOS DIAS\n")
    for d, m in metrics_by_day.items():
        print(f"=== {d} ===")
        print(f"- Slots no dia: {m['total_slots']}")
        print(f"- Vagas no dia: {m['total_capacity']}")
        print(f"- Bookados: {m['total_booked']}")
        occ = (m['total_booked'] / m['total_capacity'] * 100) if m['total_capacity'] else 0.0
        print(f"- Ocupação do dia: {occ:.1f}%")

        print("  • Ocupação por período:")
        for prd, v in m["by_period"].items():
            pct = (v["booked"] / v["capacity"] * 100) if v["capacity"] else 0.0
            print(f"    - {prd}: {v['booked']}/{v['capacity']} ({pct:.1f}%) em {v['slots']} slots")

        print("  • Ocupação por modalidade:")
        for act, v in m["by_modality"].items():
            pct = (v["booked"] / v["capacity"] * 100) if v["capacity"] else 0.0
            print(f"    - {act}: {v['booked']}/{v['capacity']} ({pct:.1f}%) em {v['slots']} slots")

        print("  • Ocupação por horário:")
        for hr, v in m["by_hour"].items():
            pct = (v["booked"] / v["capacity"] * 100) if v["capacity"] else 0.0
            print(f"    - {hr}: {v['booked']}/{v['capacity']} ({pct:.1f}%) em {v['slots']} slots")
        print("")

    # 6) CSVs
    if WRITE_CSV:
        ensure_dir(CSV_DIR)
        path_rows = save_csv_rows(rows, date_from, date_to)
        path_summary, path_mod, path_hr, path_prd = save_csv_metrics(metrics_by_day, date_from, date_to)
        print("\n📄 CSVs salvos em:")
        print(f"- {path_rows}")
        print(f"- {path_summary}")
        print(f"- {path_mod}")
        print(f"- {path_hr}")
        print(f"- {path_prd}")

if __name__ == "__main__":
    main()


