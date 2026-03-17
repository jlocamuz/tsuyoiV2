import math
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import numpy as np

from aux_functions import *

# ================= CONFIG =================
BASE = "https://api-prod.humand.co/public/api/v1"
AUTH = "Basic NTEzMTQyNTp4bVJpTHNNMEVKbGhkV2dfbC00VWxTUzhVNmd1eDVIQw=="

TOLERANCIA_TARDANZA_SEG = 0
TOLERANCIA_RETIRO_SEG  = 0

START_DATE = "2026-01-01"
END_DATE   = "2026-01-14"

LIMIT_USERS = 50
LIMIT_DAYS  = 500
BATCH_SIZE  = 25
MAX_WORKERS = 8

TZ_AR = ZoneInfo("America/Argentina/Buenos_Aires")


NORMALIZAR_A_MINUTO = True


FLAG_COLS = [
    "Ausencia",
    "Tardanza -",
    "Retiro anticipado 2",
    "Trabajo Insuficiente",
    "Es Feriado",
    "Licencia"
]

CATEGORIAS = [
    "REGULAR",
    "NOCTURNA",
    "EXTRA",
    "EXTRA SABADO",
    "EXTRA AL 50",
    "EXTRA AL 100",
    "EXTRA DOMINGO"
]

# ================= SESSION =================
s = requests.Session()
s.headers.update({"Authorization": AUTH})

def get(url, params):
    r = s.get(url, params=params, timeout=60)
    r.raise_for_status()
    return r.json()

# ================= USERS =================
def fetch_users():
    first = get(f"{BASE}/users", {"page": 1, "limit": LIMIT_USERS})
    pages = math.ceil(first["count"] / LIMIT_USERS)

    users = first["users"]
    for p in range(2, pages + 1):
        users += get(f"{BASE}/users", {"page": p, "limit": LIMIT_USERS})["users"]

    user_map,  employee_ids = {},  []

    for u in users:
        if u.get("status") != "ACTIVE":
            continue
        emp = u.get("employeeInternalId")
        if not emp:
            continue

        employee_ids.append(emp)
        user_map[emp] = f"{u.get('lastName','')}, {u.get('firstName','')}"


    return employee_ids, user_map

# ================= CATEGORÍAS =================
def split_categorized_hours_basic(categorized_hours, categorias_validas):
    valid_upper = {c.upper(): c for c in categorias_validas}
    out = {f"HORAS_{c}": 0.0 for c in categorias_validas}

    for ch in categorized_hours or []:
        name = (ch.get("category", {}) or {}).get("name") or ""
        name_u = str(name).upper().strip()
        if name_u in valid_upper:
            label = valid_upper[name_u]
            out[f"HORAS_{label}"] += float(ch.get("hours") or 0)

    return {k: round(v, 2) for k, v in out.items()}

# ================= DAY SUMMARIES =================
def fetch_batch(emp_ids, user_map):
    rows = []
    page = 1

    while True:
        data = get(
            f"{BASE}/time-tracking/day-summaries",
            {
                "employeeIds": ",".join(emp_ids),
                "startDate": START_DATE,
                "endDate": END_DATE,
                "limit": LIMIT_DAYS,
                "page": page,
            },
        )

        items = data.get("items", [])
        if not items:
            break

        for it in items:
            emp = it.get("employeeId")
            ref = (it.get("referenceDate") or it.get("date") or "")[:10]
            if not ref:
                continue

            entries = it.get("entries") or []
            slots   = it.get("timeSlots") or []
            incid   = it.get("incidences") or []
            tor     = it.get("timeOffRequests") or []
            hol     = it.get("holidays") or []
            cat     = it.get("categorizedHours") or []

            flags = flags_incidencias_y_eventos(
                incidences=incid,
                time_off_requests=tor,
                holidays=hol
            )


            hours_obj = it.get("hours") or {}
            worked = float(hours_obj.get("worked") or 0)
            scheduled = float(hours_obj.get("scheduled") or 0)

            has_useful_data = any([entries, slots, incid, tor, hol, cat, worked > 0, scheduled > 0])
            if not has_useful_data:
                continue

            # Horario obligatorio (primer timeslot)
            sched_start = sched_end = pd.NaT
            if slots and isinstance(slots, list):
                d = datetime.strptime(ref, "%Y-%m-%d")
                s0 = slots[0] if slots else {}
                if isinstance(s0, dict):
                    if s0.get("startTime"):
                        try:
                            h, m = map(int, s0["startTime"].split(":"))
                            sched_start = datetime(d.year, d.month, d.day, h, m, tzinfo=TZ_AR)
                        except Exception:
                            sched_start = pd.NaT
                    if s0.get("endTime"):
                        try:
                            h, m = map(int, s0["endTime"].split(":"))
                            sched_end = datetime(d.year, d.month, d.day, h, m, tzinfo=TZ_AR)
                            if not pd.isna(sched_start) and sched_end < sched_start:
                                sched_end += timedelta(days=1)
                        except Exception:
                            sched_end = pd.NaT

            # Fichadas (entries) -> 1) primera/última para FICHADAS 2) pares Entrada/Salida (hasta 4) + site/comment
            real_start = real_end = pd.NaT

            # defaults (por si no hay nada)
            pares = []  # lista de dicts: {"in": dt, "out": dt, "in_site": str, "out_site": str, "in_comment": str, "out_comment": str}

            if entries and isinstance(entries, list):
                # ordenar por fecha/hora real (la API NO garantiza orden)
                entries_sorted = sorted(
                    [e for e in entries if isinstance(e, dict) and (e.get("time") or e.get("date"))],
                    key=lambda e: iso_to_dt(e.get("time") or e.get("date"), TZ_AR)
                )

                # primera START / última END (para FICHADAS)
                starts_dt = [
                    iso_to_dt(e.get("time") or e.get("date"), TZ_AR)
                    for e in entries_sorted
                    if e.get("type") == "START"
                ]
                ends_dt = [
                    iso_to_dt(e.get("time") or e.get("date"), TZ_AR)
                    for e in entries_sorted
                    if e.get("type") == "END"
                ]
                if starts_dt:
                    real_start = min(starts_dt)
                if ends_dt:
                    real_end = max(ends_dt)

                # armar pares START -> END (hasta 4)
                pending = None  # dict con info de la START pendiente
                for e in entries_sorted:
                    t = iso_to_dt(e.get("time") or e.get("date"), TZ_AR)
                    typ = (e.get("type") or "").upper().strip()

                    site_name = ((e.get("site") or {}).get("name")) or ""
                    comment = e.get("comment")
                    comment = "" if comment is None else str(comment)

                    if typ == "START":
                        # si había un START sin cerrar, lo dejamos como par incompleto (out=None)
                        if pending is not None:
                            pares.append({
                                "in": pending["t"], "out": pd.NaT,
                                "in_site": pending["site"], "out_site": "",
                                "in_comment": pending["comment"], "out_comment": ""
                            })
                            if len(pares) >= 4:
                                break

                        pending = {"t": t, "site": site_name, "comment": comment}

                    elif typ == "END":
                        if pending is None:
                            # END suelto (sin START previo): lo ignoramos
                            continue

                        pares.append({
                            "in": pending["t"], "out": t,
                            "in_site": pending["site"], "out_site": site_name,
                            "in_comment": pending["comment"], "out_comment": comment
                        })
                        pending = None

                        if len(pares) >= 4:
                            break

                # si terminó y quedó un START abierto, lo agregamos incompleto
                if len(pares) < 4 and pending is not None:
                    pares.append({
                        "in": pending["t"], "out": pd.NaT,
                        "in_site": pending["site"], "out_site": "",
                        "in_comment": pending["comment"], "out_comment": ""
                    })

            if NORMALIZAR_A_MINUTO:
                sched_start = floor_minute(sched_start)
                sched_end   = floor_minute(sched_end)
                real_start  = floor_minute(real_start)
                real_end    = floor_minute(real_end)
                # normalizar también los pares
                for p in pares:
                    p["in"] = floor_minute(p["in"])
                    p["out"] = floor_minute(p["out"])

            def _hhmm(dt):
                if dt is None or pd.isna(dt):
                    return ""
                return dt.strftime("%H:%M")


            cat_hours = split_categorized_hours_basic(cat, CATEGORIAS)

            row = {

                "ID": emp,
                "APELLIDO, NOMBRE": user_map.get(emp, ""),


                "FECHA": ref,
                "DIA": weekday_es(ref),

                "Ausencia": flags["Ausencia"],
                "Tardanza -": flags["Tardanza"],   # si querés diferenciar de la columna horas
                "Trabajo Insuficiente": flags["Trabajo Insuficiente"],
                "Es Feriado": flags["Es Feriado"],
                "Licencia": flags["Licencia"],

                "_weekday_api": (it.get("weekday") or "").upper().strip(),
                "_isWorkday_api": bool(it.get("isWorkday", True)),
                "_hasHoliday_api": bool(it.get("holidays") or []),

                "_ss": sched_start,
                "_se": sched_end,
                "_rs": real_start,
                "_re": real_end,

                "HORARIO_OBLIGATORIO": fmt_range(sched_start, sched_end),
                "FICHADAS": fmt_range(real_start, real_end),
                "OBSERVACIONES": build_observaciones(it),
                "PLANIFICADAS": scheduled,

                # 4 pares máximo
                "HORA_ENTRADA_1": _hhmm(pares[0]["in"])  if len(pares) > 0 else "",
                "HORA_SALIDA_1":  _hhmm(pares[0]["out"]) if len(pares) > 0 else "",
                "HORA_ENTRADA_2": _hhmm(pares[1]["in"])  if len(pares) > 1 else "",
                "HORA_SALIDA_2":  _hhmm(pares[1]["out"]) if len(pares) > 1 else "",
                "HORA_ENTRADA_3": _hhmm(pares[2]["in"])  if len(pares) > 2 else "",
                "HORA_SALIDA_3":  _hhmm(pares[2]["out"]) if len(pares) > 2 else "",
                "HORA_ENTRADA_4": _hhmm(pares[3]["in"])  if len(pares) > 3 else "",
                "HORA_SALIDA_4":  _hhmm(pares[3]["out"]) if len(pares) > 3 else "",

                "SITE_ENTRADA_1": pares[0]["in_site"]  if len(pares) > 0 else "",
                "SITE_SALIDA_1":  pares[0]["out_site"] if len(pares) > 0 else "",
                "SITE_ENTRADA_2": pares[1]["in_site"]  if len(pares) > 1 else "",
                "SITE_SALIDA_2":  pares[1]["out_site"] if len(pares) > 1 else "",
                "SITE_ENTRADA_3": pares[2]["in_site"]  if len(pares) > 2 else "",
                "SITE_SALIDA_3":  pares[2]["out_site"] if len(pares) > 2 else "",
                "SITE_ENTRADA_4": pares[3]["in_site"]  if len(pares) > 3 else "",
                "SITE_SALIDA_4":  pares[3]["out_site"] if len(pares) > 3 else "",

                "COMMENT_ENTRADA_1": pares[0]["in_comment"]  if len(pares) > 0 else "",
                "COMMENT_SALIDA_1":  pares[0]["out_comment"] if len(pares) > 0 else "",
                "COMMENT_ENTRADA_2": pares[1]["in_comment"]  if len(pares) > 1 else "",
                "COMMENT_SALIDA_2":  pares[1]["out_comment"] if len(pares) > 1 else "",
                "COMMENT_ENTRADA_3": pares[2]["in_comment"]  if len(pares) > 2 else "",
                "COMMENT_SALIDA_3":  pares[2]["out_comment"] if len(pares) > 2 else "",
                "COMMENT_ENTRADA_4": pares[3]["in_comment"]  if len(pares) > 3 else "",
                "COMMENT_SALIDA_4":  pares[3]["out_comment"] if len(pares) > 3 else "",

            }
            row.update(cat_hours)
            rows.append(row)

        if len(items) < LIMIT_DAYS:
            break
        page += 1

    return rows

def build_df(employee_ids, user_map):
    rows = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [
            ex.submit(fetch_batch, employee_ids[i:i + BATCH_SIZE], user_map)
            for i in range(0, len(employee_ids), BATCH_SIZE)
        ]
        for f in as_completed(futures):
            rows.extend(f.result())

    df = pd.DataFrame(rows)


    # ===============================
    # Normalización para TODAS las categorías
    # ===============================
    for cat in CATEGORIAS:
        col = f"HORAS_{cat}"
        df[col] = pd.to_numeric(df.get(col, 0), errors="coerce").fillna(0.0)


    df["HORAS_TRABAJADAS"] = (df["HORAS_REGULAR"] + df["HORAS_EXTRA"]).round(2)



    return df


def main():
    employee_ids, user_map = fetch_users()
    print(f"Usuarios ACTIVE: {len(employee_ids)}")

    df = build_df(employee_ids, user_map)

    # Ordenar por nombres técnicos
    df = df.sort_values(by=["ID", "FECHA"], ascending=[True, True]).reset_index(drop=True)

    df_export = df.copy()

    # =========================
    # Tardanza / Retiro anticipado (en df_export)
    # =========================
    df_export["TARDANZA"] = df_export.apply(
        lambda r: round(max(0.0, calc_delta_hours(r["_rs"], r["_ss"], TOLERANCIA_TARDANZA_SEG)), 2),
        axis=1
    )
    df_export["RETIRO ANTICIPADO"] = df_export.apply(
        lambda r: round(max(0.0, calc_delta_hours(r["_se"], r["_re"], TOLERANCIA_RETIRO_SEG)), 2),
        axis=1
    )

    # =========================
    # LLEGADA ANTICIPADA
    # =========================
    df_export["LLEGADA ANTICIPADA"] = df_export.apply(
        lambda r: max(0.0, calc_early_arrival_hours(r["_rs"], r["_ss"])),
        axis=1
    )



    # ===============================
# EXTRA SABADO / DOMINGO → vaciar si no vinieron
    # ===============================
    for c in ["HORAS_EXTRA SABADO", "HORAS_EXTRA DOMINGO"]:
        if c in df_export.columns:
            df_export[c] = pd.to_numeric(df_export[c], errors="coerce").fillna(0.0)
            df_export[c] = df_export[c].replace(0, "")


    rename_excel = {
        "ID": "ID",
        "APELLIDO, NOMBRE": "Apellido, Nombre",
        "FECHA": "Fecha",
        "DIA": "dia",

        "HORARIO_OBLIGATORIO": "Horario obligatorio",
        "FICHADAS": "Fichadas",
        "OBSERVACIONES": "Observaciones",
        "PLANIFICADAS": "Planificadas",

        "HORAS_TRABAJADAS": "Horas Trabajadas",
        "HORAS_REGULAR": "Horas Regulares",
        "HORAS_EXTRA": "Horas extra",
        "HORAS_NOCTURNA": "Horas Nocturnas",

        "HORAS_EXTRA AL 50": "Horas Extra 50%",
        "HORAS_EXTRA AL 100": "Horas Extra 100%",

        "LLEGADA ANTICIPADA": "Llegada anticipada",
        "TARDANZA": "Tardanza",
        "RETIRO ANTICIPADO": "Retiro Anticipado",
        "HORAS_EXTRA SABADO": "Extra Sábado",
        "HORAS_EXTRA DOMINGO": "Extra Domingo",

        "HORA_ENTRADA_1": "Hora de Entrada 1",
        "HORA_SALIDA_1":  "Hora de Salida 1",
        "HORA_ENTRADA_2": "Hora de Entrada 2",
        "HORA_SALIDA_2":  "Hora de Salida 2",
        "HORA_ENTRADA_3": "Hora de Entrada 3",
        "HORA_SALIDA_3":  "Hora de Salida 3",
        "HORA_ENTRADA_4": "Hora de Entrada 4",
        "HORA_SALIDA_4":  "Hora de Salida 4",

        "SITE_ENTRADA_1": "Site Entrada 1",
        "SITE_SALIDA_1":  "Site Salida 1",
        "SITE_ENTRADA_2": "Site Entrada 2",
        "SITE_SALIDA_2":  "Site Salida 2",
        "SITE_ENTRADA_3": "Site Entrada 3",
        "SITE_SALIDA_3":  "Site Salida 3",
        "SITE_ENTRADA_4": "Site Entrada 4",
        "SITE_SALIDA_4":  "Site Salida 4",

        "COMMENT_ENTRADA_1": "Comment Entrada 1",
        "COMMENT_SALIDA_1":  "Comment Salida 1",
        "COMMENT_ENTRADA_2": "Comment Entrada 2",
        "COMMENT_SALIDA_2":  "Comment Salida 2",
        "COMMENT_ENTRADA_3": "Comment Entrada 3",
        "COMMENT_SALIDA_3":  "Comment Salida 3",
        "COMMENT_ENTRADA_4": "Comment Entrada 4",
        "COMMENT_SALIDA_4":  "Comment Salida 4",


    }

    df_export = df_export.rename(columns=rename_excel)
    df_export = df_export.drop(
        columns=["_weekday_api", "_isWorkday_api", "_hasHoliday_api", "_ss", "_se", "_rs", "_re"],
        errors="ignore"
        )
    # =========================
    # PERMISOS (Redash) -> columna "Permisos Pedidos Aprobados"
    # =========================
    rows_perm = redash_fetch_rows(
        query_id=20036,
        api_key="tIuuysHOXHdN7WArCwGAFrJ9byyZrfBuY4svmMtS",
        do_refresh=False,
        refresh_wait_s=2.0,
        max_retries=3,
        timeout=30
    )

    perm_idx = build_permissions_index(rows_perm)

    df_export = apply_permissions_column(
        df_export=df_export,
        permissions_index=perm_idx,
        emp_col="ID",
        date_col="Fecha",
        out_col="Permisos Pedidos Aprobados"
    )


# =========================
# EXTRA 50% (resta llegada anticipada)
# =========================
    for c in ["Horas Extra 50%", "Llegada anticipada", "Horas Trabajadas"]:
        if c not in df_export.columns:
            df_export[c] = 0.0
        df_export[c] = pd.to_numeric(df_export[c], errors="coerce").fillna(0.0)

    mask_50 = df_export["Horas Extra 50%"] > 0

    # nuevo valor del extra 50 luego de restar llegada anticipada
    nuevo_50 = df_export.loc[mask_50, "Horas Extra 50%"] - df_export.loc[mask_50, "Llegada anticipada"]

    # lo que queda para Extra 50 (nunca negativo)
    df_export.loc[mask_50, "Horas Extra 50%"] = nuevo_50.clip(lower=0.0)

    # déficit (si nuevo < 0): sobrante de llegada anticipada que hay que restar a Horas Trabajadas
    deficit_50 = (-nuevo_50).clip(lower=0.0)

    df_export.loc[mask_50, "Horas Trabajadas"] = (
        df_export.loc[mask_50, "Horas Trabajadas"] - deficit_50
    ).clip(lower=0.0)

    # =========================
    # EXTRA 100% (sábado+domingo) + penalización (solo sábados)
    # =========================
    for c in ["Extra Sábado", "Extra Domingo", "Llegada anticipada", "Tardanza", "Retiro Anticipado"]:
        if c not in df_export.columns:
            df_export[c] = 0.0
        df_export[c] = pd.to_numeric(df_export[c], errors="coerce").fillna(0.0)

    base_100 = df_export["Extra Sábado"] + df_export["Extra Domingo"]
    mask_sat = df_export["Extra Sábado"] > 0  # SOLO sábados

    def penal_acumulativa_h(horas, umbral_min=6, bloque_min=30, bloque_h=0.5):
        mins = (horas * 60.0).clip(lower=0.0)
        exced = (mins - umbral_min).clip(lower=0.0)
        return np.where(exced > 0, bloque_h * (1.0 + np.floor(exced / bloque_min)), 0.0)

    # penalización SOLO sábados
    pen_tarde  = np.zeros(len(df_export), dtype=float)
    pen_retiro = np.zeros(len(df_export), dtype=float)

    pen_tarde[mask_sat]  = penal_acumulativa_h(df_export.loc[mask_sat, "Tardanza"])
    pen_retiro[mask_sat] = penal_acumulativa_h(df_export.loc[mask_sat, "Retiro Anticipado"])
    pen_total = pen_tarde + pen_retiro

    # Extra Penalizada (base_100 menos penalización SOLO en sábados)
    df_export["Extra Penalizada"] = base_100
    df_export.loc[mask_sat, "Extra Penalizada"] = (base_100[mask_sat] - pen_total[mask_sat]).clip(lower=0.0)

    # Penalizacion = cuánto se restó (solo sábados)
    df_export["Penalizacion"] = 0.0
    df_export.loc[mask_sat, "Penalizacion"] = (base_100[mask_sat] - df_export.loc[mask_sat, "Extra Penalizada"]).clip(lower=0.0)

    # Horas Extra 100% = Extra Penalizada - llegada anticipada (nunca negativo)
    df_export["Horas Extra 100%"] = 0.0
    mask_100 = df_export["Extra Penalizada"] > 0
    df_export.loc[mask_100, "Horas Extra 100%"] = (
        df_export.loc[mask_100, "Extra Penalizada"] - df_export.loc[mask_100, "Llegada anticipada"]
    ).clip(lower=0.0)



    # =========================
    # HORAS EXTRA (columna única): 100% si existe, sino 50%
    # =========================
    df_export["Horas extra"] = 0.0

    mask_50_final  = df_export["Horas Extra 50%"] > 0
    mask_100_final = df_export["Horas Extra 100%"] > 0

    # primero asigno 50, después 100 pisa si aplica
    df_export.loc[mask_50_final,  "Horas extra"] = df_export.loc[mask_50_final,  "Horas Extra 50%"]
    df_export.loc[mask_100_final, "Horas extra"] = df_export.loc[mask_100_final, "Horas Extra 100%"]

    # (si querés mantener tu regla de sábados para Horas Trabajadas)

    df_export["Retiro anticipado 2"] = np.where(
        pd.to_numeric(df_export["Retiro Anticipado"], errors="coerce").fillna(0.0) > 0,
        "Si",
        "No"
    )

    df_export["Horas extra redondeada"] = df_export["Horas extra"].apply(redondear_extra_media_hora)
    df_export["Horas extra al 50% redondeada"] = df_export["Horas Extra 50%"].apply(redondear_extra_media_hora)
    df_export["Horas extra al 100% redondeada"] = df_export["Horas Extra 100%"].apply(redondear_extra_media_hora)


    # asegurar numéricos
    # asegurar numéricos
    for c in ["Horas Trabajadas", "Horas Regulares", "Llegada anticipada", "Planificadas"]:
        df_export[c] = pd.to_numeric(df_export.get(c, 0.0), errors="coerce").fillna(0.0)

    # masks
    fecha_dt = pd.to_datetime(df_export["Fecha"], errors="coerce")
    mask_sabado = fecha_dt.dt.weekday.eq(5)                # sábado
    mask_almuerzo = (~mask_sabado) & (df_export["Planificadas"] >= 8)

    # -------------------------------------------------
    # Horas Trabajadas: siempre descuenta llegada anticipada
    # (NO se descuenta almuerzo acá)
    # -------------------------------------------------
    df_export["Horas Trabajadas"] = (
        df_export["Horas Trabajadas"] - df_export["Llegada anticipada"]
    ).clip(lower=0.0)

    # -------------------------------------------------
    # Columnas "- 1hs de almuerzo": por defecto NULAS
    # y solo se calculan cuando corresponde almuerzo
    # -------------------------------------------------
    df_export["Horas trabajadas - 1hs de almuerzo"] = np.nan
    df_export["Horas regulares - 1hs de almuerzo"] = np.nan

    df_export.loc[mask_almuerzo, "Horas trabajadas - 1hs de almuerzo"] = (
        df_export.loc[mask_almuerzo, "Horas Trabajadas"] - 1.0
    ).clip(lower=0.0)

    df_export.loc[mask_almuerzo, "Horas regulares - 1hs de almuerzo"] = (
        df_export.loc[mask_almuerzo, "Horas Regulares"] - 1.0
    ).clip(lower=0.0)


    df_export.loc[mask_sabado, "Horas Trabajadas"] = df_export.loc[mask_sabado, "Horas extra"]




    # =========================
    # Orden final EXACTO
    # =========================
    cols_final = [
        "ID",
        "Apellido, Nombre",
        "Fecha",
        "dia",
        "Horario obligatorio",
        "Planificadas",
        "Permisos Pedidos Aprobados",
        "Ausencia",
        "Tardanza -",
        "Trabajo Insuficiente",
        "Retiro anticipado 2",
        "Es Feriado",
        "Licencia",

        "Fichadas",
        "Site Entrada 1",

        "Hora de Entrada 1", 
        "Comment Entrada 1", 
        "Hora de Salida 1",
        "Comment Salida 1",

        "Site Entrada 2",
        "Hora de Entrada 2", 
        "Comment Entrada 2", 
        "Hora de Salida 2",
        "Comment Salida 2",



        "Site Entrada 3",
        "Hora de Entrada 3", 
        "Comment Entrada 3",        
        "Hora de Salida 3",
        "Comment Salida 3",




        "Llegada anticipada",
        "Observaciones",
        "Horas Trabajadas",
        "Horas trabajadas - 1hs de almuerzo",
        "Horas Regulares",
        "Horas regulares - 1hs de almuerzo",

        #"Extra Sábado",
        #"Extra Penalizada",
        #"Extra Domingo",
        "Horas extra",
        "Horas extra redondeada",
        "Horas Extra 50%",
        "Horas extra al 50% redondeada",
        "Penalizacion",
        "Horas Extra 100%",
        "Horas extra al 100% redondeada",

        "Horas Nocturnas",

        "Tardanza",
        "Retiro Anticipado"

    ]

    for c in cols_final:
        if c not in df_export.columns:
            df_export[c] = 0.0

    # =========================
    # Convertir 0 -> celda vacía SOLO para Excel
    # =========================

    cols_cero_vacio = [
        "Horas Trabajadas",
        "Horas Regulares",
        "Horas extra",
        "Horas Nocturnas",
        "Horas Extra 50%",
        "Horas Extra 100%",
        "Tardanza",
        "Retiro Anticipado",
        "Llegada anticipada",
        "Extra Penalizada",
        "Extra Domingo",
        "Extra Sábado",
        "Penalizacion",
        "Horas extra redondeada",
        "Horas extra al 100 redondeada",
        "Horas extra al 50% redondeada",
        "Horas extra al 100% redondeada",

        "Horas regulares - 1hs de almuerzo",
        "Horas trabajadas - 1hs de almuerzo"
    ]

    for c in cols_cero_vacio:
        if c in df_export.columns:
            df_export[c] = df_export[c].replace(0, "")

    df_export = df_export[cols_final]

    EXPORTAR_DECIMAL = False
    #“A estas columnas tratámelas como horas: formato, decimales, conversión, estilo”.
    COLS_HORAS_DETALLE = [
        "Horas Trabajadas",
        "Horas Regulares",
        "Horas extra",
        "Horas Nocturnas",
        "Horas Extra 50%",
        "Horas Extra 100%",
        "Tardanza",
        "Retiro Anticipado",
        "Llegada anticipada",
        "Extra Sábado",
        "Extra Domingo",
        "Extra Penalizada",
        "Penalizacion",
        "Horas extra redondeada",
        "Horas extra al 50% redondeada",
        "Horas extra al 100% redondeada"

    ]

    # decimales a horas  

    COLS_A_FORMATO_HHMM = [
        "Horas Trabajadas",
        "Horas Regulares",
        "Horas extra",
        "Horas Nocturnas",
        "Horas Extra 50%",
        "Horas Extra 100%",
        "Tardanza",
        "Retiro Anticipado",
        "Llegada anticipada",
        "Extra Sábado",
        "Extra Domingo",
        "Extra Penalizada",
        "Penalizacion",
        "Horas extra redondeada",
        "Horas extra al 50% redondeada",
        "Horas extra al 100% redondeada",
        "Horas trabajadas - 1hs de almuerzo",
        "Horas regulares - 1hs de almuerzo",

    ]

    for c in COLS_A_FORMATO_HHMM:
        if c in df_export.columns:
            df_export[c] = df_export[c].apply(decimal_to_hhmm)



    now = datetime.now()
    out = now.strftime("%Y-%m-%d_%H-%M-%S") + "_reporte_basico.xlsx"
    generated_at = now.strftime("%Y-%m-%d %H:%M")

    export_detalle_diario_excel(
        df_export=df_export,
        out=out,
        START_DATE=START_DATE,
        END_DATE=END_DATE,
        generated_at=generated_at,
        EXPORTAR_DECIMAL=EXPORTAR_DECIMAL,
        COLS_HORAS_DETALLE=COLS_HORAS_DETALLE,
    )

    # 👇 AHORA sí, el archivo existe
    colorear_flags_excel(
        path_xlsx=out,
        sheet_name="Detalle diario",
        flag_cols=FLAG_COLS
    )

    print("Excel generado:", out)

    

    print("Excel generado")


if __name__ == "__main__":
    main()