#!/usr/bin/env python3
"""
CRM Weekly Dashboard — Odoo Data Extractor
Extracts CRM pipeline, activities, per-executive stats,
PLUS funnel metrics, sales KPIs, churn, and SLA de entrega.
Outputs crm-data.json for the static dashboard on GitHub Pages.

Runs via GitHub Actions on the same schedule as extract_ceo.py.
Uses the same ODOO_URL / ODOO_DB / ODOO_USER / ODOO_KEY env vars.
"""
import xmlrpc.client
import json
import os
import re
from datetime import datetime, timedelta
from collections import defaultdict, Counter

ODOO_URL = os.environ.get("ODOO_URL", "https://tomenergy.cl")
ODOO_DB = os.environ.get("ODOO_DB", "PRODUCCION")
ODOO_USER = os.environ.get("ODOO_USER", "p@tomenergy.cl")
ODOO_KEY = os.environ.get("ODOO_KEY", "")

DIESEL_PRODUCT_ID = 14  # Diésel B1

# Meses en español — NIVEL MÓDULO. Antes solo existía como local en Recuperación,
# lo que causaba NameError en el cálculo de churn: churn_pct se computaba bien y
# el except lo pisaba de vuelta a 0 (KPI mostraba 0.0% junto a 10/229).
SPANISH_MONTHS = ["", "ene", "feb", "mar", "abr", "may", "jun",
                  "jul", "ago", "sep", "oct", "nov", "dic"]


def connect():
    common = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/common")
    uid = common.authenticate(ODOO_DB, ODOO_USER, ODOO_KEY, {})
    if not uid:
        raise Exception("Authentication failed.")
    models = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/object")
    print(f"Connected as uid={uid}")
    return models, uid


def sr(models, uid, model, domain, fields, limit=5000, offset=0, order="id desc"):
    return models.execute_kw(
        ODOO_DB, uid, ODOO_KEY, model, "search_read",
        [domain], {"fields": fields, "limit": limit, "offset": offset, "order": order}
    )


def s_count(models, uid, model, domain):
    return models.execute_kw(ODOO_DB, uid, ODOO_KEY, model, "search_count", [domain])


# ── Helpers ──
def safe_name(v):
    if isinstance(v, (list, tuple)) and len(v) >= 2:
        return v[1]
    return str(v) if v else "Sin asignar"

def safe_id(v):
    if isinstance(v, (list, tuple)) and len(v) >= 1:
        return v[0]
    if isinstance(v, (int, float)):
        return int(v)
    return None

def strip_html(text):
    return re.sub(r'<[^>]+>', '', text or '').strip()

import unicodedata
def norm_name(s):
    """Normalize a user name: lowercase, remove accents, sort words → canonical key."""
    if not s: return ""
    s = unicodedata.normalize('NFD', s.lower())
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
    return ' '.join(sorted(s.split()))

# Canonical vendedor names — short reference forms that _partialMatch uses in the frontend.
# Key = set of required words (norm_name'd); Value = canonical display name.
_CANONICAL_VENDORS = [
    ({"toro", "gonzalez", "sebastian", "enrique"}, "Toro González Sebastian Enrique"),
    ({"munoz", "encalada", "joaquin"}, "MUÑOZ ENCALADA JOAQUIN"),
    ({"comber", "sigall", "pauline"}, "Comber Sigall Pauline"),
    ({"aviles", "carolina"}, "Carolina Avilés"),
    ({"marquez", "marcela"}, "Marcela Márquez"),
    ({"bisquertt", "raul"}, "Raúl Bisquertt"),
    ({"boccardo", "mauro"}, "Mauro Boccardo"),
    ({"retamal", "rodrigo"}, "Rodrigo Retamal"),
    ({"manuel", "lopez"}, "Manuel López"),
    ({"nicolas", "gonzalez"}, "Nicolás Gonzalez"),
    ({"cristian", "jiroz"}, "Cristian Jiroz"),
    ({"diego", "varas"}, "Diego Varas"),
    ({"abraham", "urrutia"}, "Abraham Urrutia"),
]

def canonical_vendedor(name):
    """Resolve vendor name to canonical form via partial word matching."""
    if not name:
        return name
    nw = set(norm_name(name).split())
    for required_words, canonical in _CANONICAL_VENDORS:
        if required_words.issubset(nw):
            return canonical
    return name  # no match → keep original

def merge_by_user(d):
    """Merge dict entries whose keys normalize to the same name. Keeps the longest original key."""
    merged = {}
    key_map = {}  # norm → best original key
    for k, v in d.items():
        nk = norm_name(k)
        if nk in merged:
            merged[nk] += v
            if len(k) > len(key_map[nk]):
                key_map[nk] = k
        else:
            merged[nk] = v
            key_map[nk] = k
    return {key_map[nk]: v for nk, v in merged.items()}

def merge_by_user_lists(d):
    """Like merge_by_user but for dict of lists (concatenate instead of sum)."""
    merged = {}
    key_map = {}
    for k, v in d.items():
        nk = norm_name(k)
        if nk in merged:
            merged[nk].extend(v)
            if len(k) > len(key_map[nk]):
                key_map[nk] = k
        else:
            merged[nk] = list(v)
            key_map[nk] = k
    return {key_map[nk]: v for nk, v in merged.items()}


# ── ENAP week: Thursday to Wednesday ──
def get_enap_week(offset=0):
    today = datetime.now()
    days_since_thu = (today.weekday() - 3) % 7
    thu = today - timedelta(days=days_since_thu) - timedelta(weeks=offset)
    thu = thu.replace(hour=0, minute=0, second=0, microsecond=0)
    wed = thu + timedelta(days=6)
    return {
        "start": thu.strftime("%Y-%m-%d"),
        "end": wed.strftime("%Y-%m-%d"),
        "label": f"{thu.strftime('%d %b')} – {wed.strftime('%d %b')}",
        "thu": thu,
        "wed": wed,
    }

def get_month_range():
    today = datetime.now().date()
    first = today.replace(day=1)
    if today.month == 12:
        last = today.replace(year=today.year + 1, month=1, day=1) - timedelta(days=1)
    else:
        last = today.replace(month=today.month + 1, day=1) - timedelta(days=1)
    return first, last

def fdt_s(d):
    if isinstance(d, str): return f"{d} 00:00:00"
    return f"{d.strftime('%Y-%m-%d')} 00:00:00"

def fdt_e(d):
    if isinstance(d, str): return f"{d} 23:59:59"
    return f"{d.strftime('%Y-%m-%d')} 23:59:59"

def fmt(d):
    if isinstance(d, str): return d
    return d.strftime("%Y-%m-%d")


# ── Detect custom fields ──
def detect_custom_fields(models, uid):
    fields = models.execute_kw(
        ODOO_DB, uid, ODOO_KEY,
        "crm.lead", "fields_get", [],
        {"attributes": ["string", "type"]}
    )
    has = {
        "x_litros_estimados": "x_litros_estimados" in fields,
        "x_tipo_contacto": "x_tipo_contacto" in fields,
        "x_origen_oportunidad": "x_origen_oportunidad" in fields,
    }
    print(f"  Custom fields: {has}")
    return has


# ── Classify stage name ──
def classify_stage(name):
    n = (name or "").lower()
    if any(k in n for k in ["oportunidad", "new", "nuev"]): return "oportunidad"
    if any(k in n for k in ["contactado", "contact"]): return "contactado"
    if any(k in n for k in ["ruta", "visit"]): return "ruta"
    if any(k in n for k in ["cotizad", "propuesta", "quot"]): return "cotizado"
    if any(k in n for k in ["won", "ganad"]): return "won"
    if any(k in n for k in ["perdid", "lost"]): return "perdido"
    if any(k in n for k in ["durmiente", "dormant"]): return "durmiente"
    return "oportunidad"


# ==============================================================
# PART 1: ORIGINAL CRM PIPELINE EXTRACTION (unchanged logic)
# ==============================================================
def extract_crm_data(models, uid):
    print("Extracting CRM pipeline data...")
    week = get_enap_week()
    now = datetime.now()
    week_start = week["start"]

    cf = detect_custom_fields(models, uid)

    stages = sr(models, uid, "crm.stage", [], ["id", "name", "sequence"], limit=50, order="sequence asc")
    stage_map = {s["id"]: s["name"] for s in stages}
    stage_class = {s["id"]: classify_stage(s["name"]) for s in stages}

    fields = [
        "id", "name", "stage_id", "user_id", "partner_id",
        "expected_revenue", "create_date", "date_last_stage_update",
        "write_date", "probability", "type"
    ]
    if cf["x_litros_estimados"]: fields.append("x_litros_estimados")
    if cf["x_tipo_contacto"]: fields.append("x_tipo_contacto")
    if cf["x_origen_oportunidad"]: fields.append("x_origen_oportunidad")

    leads = sr(models, uid, "crm.lead",
               [["active", "=", True]],
               fields, limit=2000, order="date_last_stage_update desc")
    print(f"  {len(leads)} active leads")

    activities = []
    try:
        activities = sr(models, uid, "mail.activity",
                        [["res_model", "=", "crm.lead"]],
                        ["res_id", "res_name", "activity_type_id", "user_id",
                         "date_deadline", "summary", "state"],
                        limit=200, order="date_deadline desc")
        print(f"  {len(activities)} pending activities")
    except Exception as e:
        print(f"  Activities skipped: {e}")

    messages = []
    try:
        lead_ids = [l["id"] for l in leads[:200]]
        if lead_ids:
            messages = sr(models, uid, "mail.message",
                          [["model", "=", "crm.lead"],
                           ["res_id", "in", lead_ids],
                           ["message_type", "in", ["comment", "notification"]],
                           ["date", ">=", week_start]],
                          ["res_id", "date", "body", "author_id", "subtype_id"],
                          limit=300, order="date desc")
        print(f"  {len(messages)} messages this week")
    except Exception as e:
        print(f"  Messages skipped: {e}")

    def get_value(lead):
        if cf["x_litros_estimados"]:
            return lead.get("x_litros_estimados") or 0
        return lead.get("expected_revenue") or 0

    def _days_from(dt_str):
        if not dt_str: return 999
        try:
            dt = datetime.strptime(dt_str[:19], "%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError):
            try: dt = datetime.strptime(dt_str[:10], "%Y-%m-%d")
            except: return 999
        return (now - dt).days

    def days_since_stage(lead):
        # Pure age in current stage: only `date_last_stage_update`
        return _days_from(lead.get("date_last_stage_update"))

    def days_since_activity(lead):
        # ANY modification: notes, activities, field edits, stage changes
        return _days_from(lead.get("write_date") or lead.get("date_last_stage_update"))

    def is_terminal(stage_id):
        return stage_class.get(stage_id, "") in ("won", "perdido")

    pipeline = []
    won_deals = []
    for l in leads:
        sid = l["stage_id"][0] if l["stage_id"] else None
        cls = stage_class.get(sid, "oportunidad")
        d_stage = days_since_stage(l)
        d_activity = days_since_activity(l)
        entry = {
            "id": l["id"],
            "name": l["partner_id"][1] if l["partner_id"] else l["name"],
            "stage": l["stage_id"][1] if l["stage_id"] else "—",
            "stage_class": cls,
            "exec": l["user_id"][1] if l["user_id"] else "Sin asignar",
            "exec_id": l["user_id"][0] if l["user_id"] else 0,
            "value": round(get_value(l)),
            "days_in_stage": d_stage,
            "days_since_activity": d_activity,
            "last_update": (l.get("date_last_stage_update") or l.get("write_date") or "")[:10],
            "stage_update": (l.get("date_last_stage_update") or "")[:10],
            "write_date": (l.get("write_date") or "")[:10],
            "won_date": (l.get("date_last_stage_update") or l.get("write_date") or "")[:10],
            "partner_id": l["partner_id"][0] if l.get("partner_id") else None,
            "origin": l.get("x_origen_oportunidad") or "—",
            "created": (l.get("create_date") or "")[:10],
            "probability": l.get("probability", 0),
        }
        if cls == "won":
            won_deals.append(entry)
        elif cls == "perdido":
            continue
        else:
            pipeline.append(entry)

    # ── Fecha REAL del paso a Won vía mail.tracking.value ──
    # El cron re-estampa date_last_stage_update en batch (todas las won quedaban con la
    # misma fecha) → las semanas pasadas mostraban 0 Clientes Ganados aunque hubo cierres.
    try:
        _won_stage_names = [s["name"] for s in stages if classify_stage(s["name"]) == "won"]
        _won_ids = [w["id"] for w in won_deals]
        _wtrk = []
        for i in range(0, len(_won_ids), 400):
            _wtrk += sr(models, uid, "mail.tracking.value", [
                ["mail_message_id.model", "=", "crm.lead"],
                ["mail_message_id.res_id", "in", _won_ids[i:i+400]],
                ["new_value_char", "in", _won_stage_names],
            ], ["mail_message_id", "create_date"], limit=10000)
        _wmids = list({safe_id(t.get("mail_message_id")) for t in _wtrk if t.get("mail_message_id")})
        _wmid_res = {}
        for i in range(0, len(_wmids), 500):
            for m in sr(models, uid, "mail.message", [["id", "in", _wmids[i:i+500]]], ["res_id"], limit=1000):
                _wmid_res[m["id"]] = m.get("res_id")
        _won_real = {}
        for t in sorted(_wtrk, key=lambda x: x.get("create_date") or ""):
            lid = _wmid_res.get(safe_id(t.get("mail_message_id")))
            if lid:
                _won_real[lid] = (t.get("create_date") or "")[:10]
        _fixed = 0
        for w in won_deals:
            rd = _won_real.get(w["id"])
            if rd:
                w["won_date"] = rd
                w["stage_update"] = rd
                w["last_update"] = rd
                _fixed += 1
        print(f"  Fechas reales de Won (tracking): {_fixed}/{len(won_deals)}")
    except Exception as _e:
        print(f"  Tracking de Won skipped: {_e}")

    exec_map = {}
    for p in pipeline:
        eid = p["exec_id"]
        if eid not in exec_map:
            exec_map[eid] = {
                "name": p["exec"], "total": 0,
                "moved": 0, "moved_30d": 0,  # 7d (this ENAP week) / 30d windows of activity
                "stale": 0, "value": 0,
            }
        exec_map[eid]["total"] += 1
        exec_map[eid]["value"] += p["value"]
        # Activity = any modification (write_date), not just stage transition
        if p["last_update"] >= week_start: exec_map[eid]["moved"] += 1
        if p["days_since_activity"] <= 30: exec_map[eid]["moved_30d"] += 1
        if p["days_in_stage"] > 7: exec_map[eid]["stale"] += 1

    # Add won counts per executive
    won_by_exec = {}
    for w in won_deals:
        eid = w["exec_id"]
        won_by_exec[eid] = won_by_exec.get(eid, 0) + 1
        # Also ensure won execs appear in exec_map
        if eid not in exec_map:
            exec_map[eid] = {
                "name": w["exec"], "total": 0,
                "moved": 0, "moved_30d": 0, "stale": 0, "value": 0,
            }
    for eid, cnt in won_by_exec.items():
        exec_map[eid]["won"] = cnt

    executives = sorted(exec_map.values(), key=lambda x: -x["total"])

    created_this_week = sum(1 for l in leads if (l.get("create_date") or "")[:10] >= week_start)
    won_count = sum(1 for l in leads if stage_class.get(
        l["stage_id"][0] if l["stage_id"] else None, "") == "won")
    moved_count = sum(1 for p in pipeline if p["last_update"] >= week_start)
    stale_count = sum(1 for p in pipeline if p["days_in_stage"] > 7)
    total_value = sum(p["value"] for p in pipeline)

    stale_leads = sorted(
        [p for p in pipeline if p["days_in_stage"] > 7],
        key=lambda x: -x["days_in_stage"]
    )[:50]

    action_map = {
        "oportunidad": "Llamar o enviar mail",
        "contactado": "Agendar visita (Ruta)",
        "ruta": "Enviar cotización",
        "cotizado": "Seguimiento cotización",
    }
    for s in stale_leads:
        s["action"] = action_map.get(s["stage_class"], "Contactar")

    funnel = []
    for stg in stages:
        cls = classify_stage(stg["name"])
        if cls in ("won", "perdido"): continue
        # Contar por NOMBRE de etapa, no por clase: varias etapas comparten clase
        # (Oportunidad de Negocio / En Riesgo / No Cerrados) y salían todas con el total.
        count = sum(1 for p in pipeline if p["stage"] == stg["name"])
        if count > 0:
            funnel.append({"stage": stg["name"], "class": cls, "count": count})

    act_list = []
    for a in activities:
        act_list.append({
            "date": a.get("date_deadline") or "",
            "who": a["user_id"][1] if a.get("user_id") else "—",
            "type": a["activity_type_id"][1] if a.get("activity_type_id") else "Actividad",
            "summary": a.get("summary") or a.get("res_name") or "",
            "lead": a.get("res_name") or "",
            "state": a.get("state") or "",
        })

    # Noise phrases to skip (auto-generated Odoo messages, not real user activity)
    _noise = ["lead enrichment", "nuevo lead para el equipo", "new lead for", "stage changed",
              "enrichment could", "no company data", "meeting scheduled",
              "ganado autom", "oportunidad ganada", "opportunity won", "facturas pendientes",
              "proximo recordatorio", "próximo recordatorio", "cierre masivo de backlog",
              "reemplazo automatico", "lista de precios cambiada"]

    msg_list = []
    last_msg_by_lead = {}  # lead_id → latest REAL message (skip auto-generated noise)
    for m in messages:
        body = strip_html(m.get("body") or "")
        if len(body) < 3: continue
        # Skip auto-generated noise
        if any(noise in body.lower()[:80] for noise in _noise): continue
        msg_list.append({
            "date": (m.get("date") or "")[:16],
            "who": m["author_id"][1] if m.get("author_id") else "—",
            "desc": body[:200],
        })
        rid = m.get("res_id")
        if rid and rid not in last_msg_by_lead:
            last_msg_by_lead[rid] = body[:120]

    # Build activity map by lead ID (from mail.activity — To-Do's, calls, etc.)
    last_activity_by_lead = {}
    for a in activities:
        rid = a.get("res_id")
        if rid and rid not in last_activity_by_lead:
            summary = a.get("summary") or ""
            atype = a["activity_type_id"][1] if a.get("activity_type_id") else "Actividad"
            if summary and not any(n in summary.lower()[:60] for n in _noise):
                last_activity_by_lead[rid] = f"{atype}: {summary[:120]}"

    # Enrich pipeline entries with last note (prefer message, fallback to activity)
    for p in pipeline:
        note = last_msg_by_lead.get(p["id"], "")
        if not note:
            note = last_activity_by_lead.get(p["id"], "")
        p["last_note"] = note
    for w in won_deals:
        note = last_msg_by_lead.get(w["id"], "")
        if not note:
            note = last_activity_by_lead.get(w["id"], "")
        w["last_note"] = note

    # Enrich won_deals with first invoice date (inmune al cron de Odoo que pisa write_date)
    won_partner_ids = [w["partner_id"] for w in won_deals if w.get("partner_id")]
    first_inv_by_partner = {}
    if won_partner_ids:
        try:
            inv_rows = sr(models, uid, "account.move", [
                ["move_type", "=", "out_invoice"],
                ["state", "=", "posted"],
                ["partner_id", "in", won_partner_ids],
            ], ["partner_id", "invoice_date"], limit=5000, order="invoice_date asc")
            for inv in inv_rows:
                pid = inv["partner_id"][0] if inv.get("partner_id") else None
                if pid and pid not in first_inv_by_partner and inv.get("invoice_date"):
                    first_inv_by_partner[pid] = inv["invoice_date"]
            print(f"  Won deals: first invoice found for {len(first_inv_by_partner)}/{len(set(won_partner_ids))} partners")
        except Exception as e:
            print(f"  Won deals first_invoice skipped: {e}")
    for w in won_deals:
        pid = w.get("partner_id")
        w["first_invoice_date"] = first_inv_by_partner.get(pid, "") if pid else ""

    return {
        "has_litros": cf["x_litros_estimados"],
        "summary": {
            "active": len(pipeline),
            "new_this_week": created_this_week,
            "moved_this_week": moved_count,
            "stale_7d": stale_count,
            "won": won_count,
            "total_value": round(total_value),
        },
        "executives": executives,
        "funnel": funnel,
        "pipeline": sorted(pipeline, key=lambda x: x["days_in_stage"])[:150],
        "won_deals": sorted(
            {w["partner_id"]: w for w in reversed(won_deals) if w.get("partner_id")}.values(),
            key=lambda x: -(x.get("value") or x.get("avg_monthly_litros") or 0)
        ),
        "stale": stale_leads,
        "activities": act_list[:50],
        "messages": msg_list[:30],
    }


# ==============================================================
# PART 2: FUNNEL COMERCIAL (6 stages, 4 weeks)
# ==============================================================
def extract_funnel_data(models, uid):
    print("\nExtracting funnel comercial...")
    weeks_data = []

    # Pre-fetch ruta stage IDs for visit counting
    _stages = sr(models, uid, "crm.stage", [], ["id", "name"], limit=50)
    ruta_stage_ids = [s["id"] for s in _stages if classify_stage(s["name"]) == "ruta"]
    print(f"  Ruta stage IDs: {ruta_stage_ids}")

    for offset in range(4):
        wk = get_enap_week(offset)
        ws, we = wk["start"], wk["end"]
        label = wk["label"]
        print(f"  Semana {ws} -> {we}")

        # 1. Leads
        lead_count = s_count(models, uid, "crm.lead", [
            ["create_date", ">=", fdt_s(ws)],
            ["create_date", "<=", fdt_e(we)],
        ])
        _lead_fields = ["name", "partner_name", "user_id", "stage_id", "create_date", "expected_revenue"]
        lead_detail = sr(models, uid, "crm.lead", [
            ["create_date", ">=", fdt_s(ws)],
            ["create_date", "<=", fdt_e(we)],
        ], _lead_fields, limit=2000)

        leads_by_user = defaultdict(int)
        lead_rows = []
        for l in lead_detail:
            u = safe_name(l.get("user_id"))
            leads_by_user[u] += 1
            lead_rows.append({
                "name": l.get("name", ""),
                "empresa": l.get("partner_name", ""),
                "vendedor": u,
                "etapa": safe_name(l.get("stage_id")),
                "fecha": (l.get("create_date") or "")[:10],
                "litros": int(l.get("expected_revenue") or 0),  # TomEnergy: expected_revenue = litros estimados
            })

        # 2. Contactos efectivos — comentarios y correos REALES (excluye notificaciones del sistema/cron)
        contact_count = 0
        contacts_by_user = defaultdict(int)
        try:
            contact_domain = [
                ["date", ">=", fdt_s(ws)],
                ["date", "<=", fdt_e(we)],
                ["model", "=", "crm.lead"],
                ["message_type", "in", ["comment", "email"]],
            ]
            contact_count = s_count(models, uid, "mail.message", contact_domain)
            contact_msgs = sr(models, uid, "mail.message", contact_domain, ["res_id"], limit=5000)

            lead_ids = list(set(m.get("res_id") for m in contact_msgs if m.get("res_id")))
            if lead_ids:
                lead_user_map = {}
                for i in range(0, len(lead_ids), 200):
                    chunk = lead_ids[i:i+200]
                    leads_info = sr(models, uid, "crm.lead", [
                        ["id", "in", chunk],
                    ], ["id", "user_id"], limit=200)
                    for li in leads_info:
                        lead_user_map[li["id"]] = safe_name(li.get("user_id"))

                for m in contact_msgs:
                    rid = m.get("res_id")
                    u = lead_user_map.get(rid, "Sin asignar")
                    contacts_by_user[u] += 1
        except Exception as e:
            print(f"    Contactos efectivos skipped: {e}")

        # 2b. Ruta (visitas) — leads moved to "Ruta" stage this week
        ruta_count = 0
        ruta_by_user = defaultdict(int)
        if ruta_stage_ids:
            ruta_leads = sr(models, uid, "crm.lead", [
                ["stage_id", "in", ruta_stage_ids],
                ["date_last_stage_update", ">=", fdt_s(ws)],
                ["date_last_stage_update", "<=", fdt_e(we)],
                ["active", "=", True],
            ], ["user_id", "partner_id", "name"], limit=500)
            ruta_count = len(ruta_leads)
            for rl in ruta_leads:
                u = canonical_vendedor(safe_name(rl.get("user_id")))
                ruta_by_user[u] += 1

        # 3. Cotizaciones — solo las creadas por PERSONAS. Se excluye OdooBot (uid 1):
        # los crons generate_so (semanal/mensual/anual) crean ~970 borradores/mes que
        # inflaban el funnel y no son gestión comercial real.
        quote_domain = [
            ["create_date", ">=", fdt_s(ws)],
            ["create_date", "<=", fdt_e(we)],
            ["state", "in", ["draft", "sent"]],
            ["create_uid", "!=", 1],
        ]
        quote_count = s_count(models, uid, "sale.order", quote_domain)
        quote_detail = sr(models, uid, "sale.order", quote_domain,
                          ["name", "partner_id", "user_id", "amount_untaxed", "state", "create_date"], limit=20)

        # Get litros from sale.order.line for these quotes
        quote_ids = [q["id"] for q in quote_detail]
        quote_litros_map = {}
        if quote_ids:
            sol = sr(models, uid, "sale.order.line", [
                ["order_id", "in", quote_ids],
                ["product_id", "=", DIESEL_PRODUCT_ID],
            ], ["order_id", "product_uom_qty"], limit=500)
            for ln in sol:
                oid = safe_id(ln.get("order_id"))
                if oid:
                    quote_litros_map[oid] = quote_litros_map.get(oid, 0) + (ln.get("product_uom_qty", 0) or 0)

        quotes_by_user = defaultdict(int)
        quote_rows = []
        for q in quote_detail:
            u = safe_name(q.get("user_id"))
            quotes_by_user[u] += 1
            quote_rows.append({
                "name": q.get("name", ""),
                "cliente": safe_name(q.get("partner_id")),
                "vendedor": u,
                "monto": q.get("amount_untaxed", 0),
                "litros": round(quote_litros_map.get(q["id"], 0)),
                "estado": q.get("state", ""),
                "fecha": (q.get("create_date") or "")[:10],
            })

        # 4. Cotizaciones Gestionadas (invoice_status = 'no' = nada que facturar)
        managed_count = 0
        managed_by_user = defaultdict(int)
        managed_detail = []
        if quote_count:
            try:
                managed_orders = sr(models, uid, "sale.order", [
                    ["create_date", ">=", fdt_s(ws)],
                    ["create_date", "<=", fdt_e(we)],
                    ["invoice_status", "=", "no"],
                ], ["name", "partner_id", "user_id", "amount_untaxed", "create_date"], limit=200)
                managed_count = len(managed_orders)
                for o in managed_orders:
                    u = safe_name(o.get("user_id"))
                    managed_by_user[u] += 1
                    managed_detail.append({
                        "name": o.get("name", ""),
                        "cliente": safe_name(o.get("partner_id")),
                        "vendedor": u,
                        "monto": o.get("amount_untaxed", 0),
                        "fecha": (o.get("create_date") or "")[:10],
                    })
            except:
                pass
        followup_pct = min(round((managed_count / max(quote_count, 1)) * 100), 100) if quote_count else 0

        # 5. Cierres (nuevos clientes)
        close_orders = sr(models, uid, "sale.order", [
            ["date_order", ">=", fdt_s(ws)],
            ["date_order", "<=", fdt_e(we)],
            ["state", "=", "sale"],
        ], ["partner_id", "name", "date_order", "amount_untaxed", "user_id"], limit=200)

        close_count = 0
        close_by_user = defaultdict(int)
        close_detail = []
        seen_partners = set()
        for o in close_orders:
            pid = safe_id(o.get("partner_id"))
            if not pid or pid in seen_partners: continue
            seen_partners.add(pid)
            prev = s_count(models, uid, "sale.order", [
                ["partner_id", "=", pid],
                ["state", "=", "sale"],
                ["date_order", "<", fdt_s(ws)],
            ])
            if prev == 0:
                u = safe_name(o.get("user_id"))
                close_count += 1
                close_by_user[u] += 1
                close_detail.append({
                    "partner": safe_name(o.get("partner_id")),
                    "order": o.get("name", ""),
                    "amount": o.get("amount_untaxed", 0),
                    "vendedor": u,
                })

        print(f"    Leads:{lead_count} Contactos:{contact_count} Ruta:{ruta_count} Cotiz:{quote_count} Follow:{followup_pct}% Cierre:{close_count}")

        weeks_data.append({
            "week_start": ws,
            "week_end": we,
            "label": label,
            "is_current": offset == 0,
            "stages": {
                "leads":       {"value": lead_count, "goal": 15, "by_user": dict(leads_by_user), "detail": lead_rows},
                "contacto":    {"value": contact_count, "goal": 10, "by_user": dict(contacts_by_user)},
                "ruta":        {"value": ruta_count, "goal": 5, "by_user": dict(ruta_by_user)},
                "cotizacion":  {"value": quote_count, "goal": 8, "by_user": dict(quotes_by_user), "detail": quote_rows},
                "seguimiento": {"value": followup_pct, "goal": 100, "unit": "%", "count": managed_count, "by_user": dict(managed_by_user), "detail": managed_detail},
                "cierre":      {"value": close_count, "goal": 2, "by_user": dict(close_by_user), "detail": close_detail},
            }
        })

    # 6. Retencion 90d — based on posted invoices (actual billing), not sale orders
    wk0 = get_enap_week(0)
    wed_date = wk0["wed"]
    lookback_start = wed_date - timedelta(days=90)

    all_inv_90d = sr(models, uid, "account.move", [
        ["move_type", "=", "out_invoice"],
        ["state", "=", "posted"],
        ["invoice_date", ">=", lookback_start.strftime("%Y-%m-%d")],
        ["invoice_date", "<=", wed_date.strftime("%Y-%m-%d")],
    ], ["partner_id"], limit=10000)

    partner_order_count = Counter()
    for inv in all_inv_90d:
        pid = safe_id(inv.get("partner_id"))
        if pid:
            partner_order_count[pid] += 1

    ret_total = len(partner_order_count)
    retained_ids = {pid for pid, cnt in partner_order_count.items() if cnt >= 2}
    ret_pct = round((len(retained_ids) / ret_total) * 100) if ret_total > 0 else 0
    print(f"  Retencion 90d: {ret_pct}% ({len(retained_ids)}/{ret_total} clientes con 2+ facturas)")

    # Retention 90d is a rolling trailing metric — show the same value on every week tab
    retencion_payload = {
        "value": ret_pct, "goal": 90, "unit": "%",
        "total_evaluated": ret_total, "retained_count": len(retained_ids),
    }
    for w in weeks_data:
        w["stages"]["retencion"] = retencion_payload

    return weeks_data


# ==============================================================
# PART 3: SALES KPIs (monthly)
# ==============================================================
def extract_sales_data(models, uid, custom_start=None, custom_end=None, label_override=None):
    if custom_start and custom_end:
        m_start, m_end = custom_start, custom_end
    else:
        m_start, m_end = get_month_range()
    lbl = label_override or m_start.strftime("%B %Y")
    print(f"\nExtracting sales KPIs ({lbl})...")
    print(f"  Mes: {fmt(m_start)} -> {fmt(m_end)}")

    invoices = sr(models, uid, "account.move", [
        ["move_type", "=", "out_invoice"],
        ["state", "=", "posted"],
        ["invoice_date", ">=", fmt(m_start)],
        ["invoice_date", "<=", fmt(m_end)],
    ], ["name", "partner_id", "invoice_user_id", "amount_untaxed", "invoice_date", "margin_zone",
        "invoice_payment_term_id"], limit=2000)

    inv_ids = [i["id"] for i in invoices]
    inv_user_map = {i["id"]: canonical_vendedor(safe_name(i.get("invoice_user_id"))) for i in invoices}
    inv_margin_map = {i["id"]: i.get("margin_zone", 0) or 0 for i in invoices}
    inv_date_map = {i["id"]: i.get("invoice_date", "") for i in invoices}
    inv_term_map = {i["id"]: safe_name(i.get("invoice_payment_term_id")) for i in invoices}

    partner_ids = list(set(safe_id(i.get("partner_id")) for i in invoices if safe_id(i.get("partner_id"))))
    volume_partners = set()
    partner_zone = {}
    partner_name_map = {}
    partner_vat_map = {}
    partner_user_map = {}  # pid → vendedor from res.partner.user_id
    if partner_ids:
        for i in range(0, len(partner_ids), 200):
            chunk = partner_ids[i:i+200]
            partners = sr(models, uid, "res.partner", [
                ["id", "in", chunk],
            ], ["id", "name", "vat", "is_volume_client", "delivery_zone_id", "user_id"], limit=200)
            for p in partners:
                if p.get("is_volume_client"):
                    volume_partners.add(p["id"])
                zn = safe_name(p.get("delivery_zone_id"))
                if zn and zn != "False" and zn != "Sin asignar":
                    partner_zone[p["id"]] = zn
                partner_name_map[p["id"]] = p.get("name", "")
                partner_vat_map[p["id"]] = p.get("vat", "") or ""
                # Vendedor from partner (source of truth), canonicalized
                pu = canonical_vendedor(safe_name(p.get("user_id")))
                if pu:
                    partner_user_map[p["id"]] = pu
    inv_partner_map = {i["id"]: safe_id(i.get("partner_id")) for i in invoices}

    # Override inv_user_map: use partner.user_id (assigned vendedor) instead of invoice_user_id
    for inv_id, pid in inv_partner_map.items():
        pu = partner_user_map.get(pid)
        if pu:
            inv_user_map[inv_id] = pu

    litros_by_user = defaultdict(float)
    venta_by_user = defaultdict(float)
    litros_by_zone = defaultdict(float)
    venta_by_zone = defaultdict(float)
    margin_by_zone_venta = defaultdict(float)
    margin_by_zone_costo = defaultdict(float)
    total_litros = 0
    total_venta = 0

    retail_venta = 0
    retail_costo = 0
    volume_venta = 0
    volume_costo = 0
    retail_litros = 0
    volume_litros = 0
    litros_by_partner = defaultdict(float)
    margin_by_user_venta = defaultdict(float)
    margin_by_user_costo = defaultdict(float)

    if inv_ids:
        lines = sr(models, uid, "account.move.line", [
            ["move_id", "in", inv_ids],
            ["product_id", "=", DIESEL_PRODUCT_ID],
        ], ["move_id", "quantity", "price_unit", "price_subtotal"], limit=5000)

        for ln in lines:
            mid = safe_id(ln.get("move_id"))
            qty = ln.get("quantity", 0)
            sub = ln.get("price_subtotal", 0)
            user = inv_user_map.get(mid, "Sin asignar")
            margin = inv_margin_map.get(mid, 0)
            pid = inv_partner_map.get(mid)
            is_vol = pid in volume_partners

            litros_by_user[user] += qty
            venta_by_user[user] += sub
            total_litros += qty
            total_venta += sub
            if pid:
                litros_by_partner[pid] += qty
            margin_by_user_venta[user] += sub
            margin_by_user_costo[user] += sub * (1 - margin) if margin else sub

            zone = partner_zone.get(pid, "Sin zona")
            litros_by_zone[zone] += qty
            venta_by_zone[zone] += sub
            margin_by_zone_venta[zone] += sub
            margin_by_zone_costo[zone] += sub * (1 - margin) if margin else sub

            if is_vol:
                volume_venta += sub
                volume_costo += sub * (1 - margin) if margin else sub
                volume_litros += qty
            else:
                retail_venta += sub
                retail_costo += sub * (1 - margin) if margin else sub
                retail_litros += qty

    margin_retail_pct = round(((retail_venta - retail_costo) / retail_venta) * 100, 2) if retail_venta > 0 else 0
    margin_volume_pct = round(((volume_venta - volume_costo) / volume_venta) * 100, 2) if volume_venta > 0 else 0

    print(f"  Margen Retail: {margin_retail_pct}% | Margen Volumen: {margin_volume_pct}%")

    ncs = sr(models, uid, "account.move", [
        ["move_type", "=", "out_refund"],
        ["state", "=", "posted"],
        ["invoice_date", ">=", fmt(m_start)],
        ["invoice_date", "<=", fmt(m_end)],
    ], ["name", "invoice_user_id", "amount_untaxed", "partner_id", "margin_zone",
        "reversed_entry_id"], limit=500)

    nc_ids = [n["id"] for n in ncs]
    nc_user_map = {n["id"]: canonical_vendedor(safe_name(n.get("invoice_user_id"))) for n in ncs}
    nc_partner_map = {n["id"]: safe_id(n.get("partner_id")) for n in ncs}
    # Override NC user map with partner.user_id
    for nc_id, pid in nc_partner_map.items():
        pu = partner_user_map.get(pid)
        if pu:
            nc_user_map[nc_id] = pu
    nc_margin_map = {n["id"]: n.get("margin_zone", 0) or 0 for n in ncs}

    # ── Descontar NC ──
    # Estrategia: para litros, buscar en 3 fuentes (en orden):
    #   1. Líneas diesel en la propia NC
    #   2. Si no tiene → buscar litros diesel en la factura original (reversed_entry_id)
    #   3. Si tampoco → 0 litros (NC puramente monetaria)
    # Para venta: siempre usar amount_untaxed del header de la NC
    nc_litros_by_move = defaultdict(float)
    if nc_ids:
        nc_lines = sr(models, uid, "account.move.line", [
            ["move_id", "in", nc_ids],
            ["product_id", "=", DIESEL_PRODUCT_ID],
        ], ["move_id", "quantity"], limit=2000)
        for ln in nc_lines:
            mid = safe_id(ln.get("move_id"))
            nc_litros_by_move[mid] += abs(ln.get("quantity", 0))

    # For NCs with dummy qty (<=1 litro = NC por monto), get real litros from original invoice
    for nc in ncs:
        ncid = nc["id"]
        nc_qty = nc_litros_by_move.get(ncid, 0)
        rev_id = safe_id(nc.get("reversed_entry_id"))
        if rev_id and nc_qty <= 1:
            orig_lines = sr(models, uid, "account.move.line", [
                ["move_id", "=", rev_id],
                ["product_id", "=", DIESEL_PRODUCT_ID],
            ], ["quantity"], limit=20)
            orig_litros = sum(abs(oln.get("quantity", 0)) for oln in orig_lines)
            if orig_litros > nc_qty:
                nc_litros_by_move[ncid] = orig_litros
                print(f"    NC {nc.get('name','?')}: replaced dummy {nc_qty}L with original invoice litros {orig_litros}L (rev={rev_id})")

    nc_litros_total = 0
    nc_venta_total = 0
    for nc in ncs:
        ncid = nc["id"]
        user = nc_user_map.get(ncid, "Sin asignar")
        nc_margin = nc_margin_map.get(ncid, 0)
        nc_pid = nc_partner_map.get(ncid)
        sub = abs(nc.get("amount_untaxed", 0) or 0)
        qty = nc_litros_by_move.get(ncid, 0)

        litros_by_user[user] -= qty
        venta_by_user[user] -= sub
        total_litros -= qty
        total_venta -= sub
        margin_by_user_venta[user] -= sub
        margin_by_user_costo[user] -= sub * (1 - nc_margin) if nc_margin else sub
        if nc_pid:
            litros_by_partner[nc_pid] -= qty
        if nc_pid in volume_partners:
            volume_litros -= qty
        else:
            retail_litros -= qty
        nc_litros_total += qty
        nc_venta_total += sub
        print(f"    NC {nc.get('name','?')}: litros={qty} venta={sub} user={user} rev={safe_id(nc.get('reversed_entry_id'))}")

    # Also get ALL nc lines (any product) for debug
    nc_all_lines_debug = []
    if nc_ids:
        try:
            all_nc_lines = sr(models, uid, "account.move.line", [
                ["move_id", "in", nc_ids],
                ["product_id", "!=", False],
            ], ["move_id", "product_id", "quantity", "price_subtotal", "name"], limit=2000)
        except Exception:
            all_nc_lines = []
        for ln in all_nc_lines:
            nc_all_lines_debug.append({
                "nc_id": safe_id(ln.get("move_id")),
                "product": safe_name(ln.get("product_id")),
                "product_id": safe_id(ln.get("product_id")),
                "qty": ln.get("quantity", 0),
                "subtotal": ln.get("price_subtotal", 0),
                "desc": (ln.get("name") or "")[:60],
            })

    nc_debug = []
    for nc in ncs:
        nc_debug.append({
            "name": nc.get("name", ""),
            "user": nc_user_map.get(nc["id"], ""),
            "amount": abs(nc.get("amount_untaxed", 0) or 0),
            "partner": safe_name(nc.get("partner_id")),
            "litros_restados": nc_litros_by_move.get(nc["id"], 0),
            "reversed_entry_id": safe_id(nc.get("reversed_entry_id")),
            "lines": [l for l in nc_all_lines_debug if l["nc_id"] == nc["id"]],
        })

    print(f"  NC resumen: {len(ncs)} NCs, litros restados={round(nc_litros_total)}, venta restada={round(nc_venta_total)}")
    for ncd in nc_debug:
        print(f"    {ncd['name']}: L={ncd['litros_restados']} $={ncd['amount']} user={ncd['user']} rev={ncd['reversed_entry_id']} lines={len(ncd['lines'])}")

    # ── Clientes Nuevos: primera factura con Diesel B1 (product_id=14) en este mes ──
    new_cl_by_user = defaultdict(int)
    new_cl_detail = []
    new_cl_count = 0
    new_cl_litros_by_user = defaultdict(float)
    # Get all partner_ids that have diesel lines in THIS month's invoices
    diesel_partners_this_month = set()
    for ln in lines:
        mid = safe_id(ln.get("move_id"))
        pid = inv_partner_map.get(mid)
        if pid:
            diesel_partners_this_month.add(pid)
    seen = set()
    for pid in diesel_partners_this_month:
        if pid in seen: continue
        seen.add(pid)
        # Check if this partner had ANY previous invoice with Diesel B1 line
        prev_inv = sr(models, uid, "account.move", [
            ["move_type", "=", "out_invoice"],
            ["state", "=", "posted"],
            ["partner_id", "=", pid],
            ["invoice_date", "<", fmt(m_start)],
        ], ["id", "invoice_date"], limit=500, order="invoice_date desc")
        prev_ids = [i["id"] for i in prev_inv]
        # Origen: 'nuevo' = nunca facturó antes; 'recuperado' = tenía historia (vuelve de perdido/inactivo)
        last_prev_date = prev_inv[0].get("invoice_date", "") if prev_inv else ""
        had_diesel_before = False
        if prev_ids:
            # Check if any previous invoice had a diesel line
            for chunk_start in range(0, len(prev_ids), 200):
                chunk = prev_ids[chunk_start:chunk_start+200]
                prev_diesel = s_count(models, uid, "account.move.line", [
                    ["move_id", "in", chunk],
                    ["product_id", "=", DIESEL_PRODUCT_ID],
                ])
                if prev_diesel > 0:
                    had_diesel_before = True
                    break
        if not had_diesel_before:
            # Find the EARLIEST invoice for this partner in this month (first purchase date)
            partner_invs = [i for i in invoices if safe_id(i.get("partner_id")) == pid]
            partner_invs.sort(key=lambda x: x.get("invoice_date", "9999"))
            inv_match = partner_invs[0] if partner_invs else None
            if inv_match:
                partner_litros = max(litros_by_partner.get(pid, 0), 0)
                if partner_litros <= 0:
                    continue  # Skip clients with 0 net litros (NC reversed all sales)
                u = partner_user_map.get(pid) or safe_name(inv_match.get("invoice_user_id"))
                pname = safe_name(inv_match.get("partner_id"))
                first_date = inv_match.get("invoice_date", "")
                new_cl_by_user[u] += 1
                new_cl_count += 1
                new_cl_litros_by_user[u] += partner_litros
                new_cl_detail.append({
                    "cliente": pname, "vendedor": u, "fecha": first_date,
                    "litros": round(partner_litros),
                    "origen": "recuperado" if prev_ids else "nuevo",
                    "ultima_compra_previa": last_prev_date,
                })

    weekly_sales = []
    for offset in range(4):
        wk = get_enap_week(offset)
        ws_d = datetime.strptime(wk["start"], "%Y-%m-%d").date()
        we_d = datetime.strptime(wk["end"], "%Y-%m-%d").date()
        if we_d < m_start: continue
        actual_start = max(ws_d, m_start)
        actual_end = min(we_d, m_end)

        wk_inv = sr(models, uid, "account.move", [
            ["move_type", "=", "out_invoice"],
            ["state", "=", "posted"],
            ["invoice_date", ">=", fmt(actual_start)],
            ["invoice_date", "<=", fmt(actual_end)],
        ], ["id"], limit=5000)
        wk_ids = [i["id"] for i in wk_inv]
        wk_litros = 0
        wk_venta = 0
        if wk_ids:
            wk_lines = sr(models, uid, "account.move.line", [
                ["move_id", "in", wk_ids],
                ["product_id", "=", DIESEL_PRODUCT_ID],
            ], ["quantity", "price_subtotal"], limit=5000)
            for ln in wk_lines:
                wk_litros += ln.get("quantity", 0)
                wk_venta += ln.get("price_subtotal", 0)
        weekly_sales.append({
            "label": f"{actual_start.day}/{actual_start.month}-{actual_end.day}/{actual_end.month}",
            "litros": round(wk_litros),
            "venta_neta": round(wk_venta),
        })

    # Weekly history — 16 weeks back, ENAP commercial weeks (Thu-Wed), no month boundary
    weekly_history = []
    for offset in range(16):
        wk = get_enap_week(offset)
        ws_d = datetime.strptime(wk["start"], "%Y-%m-%d").date()
        we_d = datetime.strptime(wk["end"], "%Y-%m-%d").date()
        wk_inv = sr(models, uid, "account.move", [
            ["move_type", "=", "out_invoice"],
            ["state", "=", "posted"],
            ["invoice_date", ">=", fmt(ws_d)],
            ["invoice_date", "<=", fmt(we_d)],
        ], ["id", "partner_id", "invoice_user_id", "margin_zone"], limit=5000)
        wk_ids = [i["id"] for i in wk_inv]
        wk_user_map = {i["id"]: canonical_vendedor(safe_name(i.get("invoice_user_id"))) for i in wk_inv}
        # Fetch missing partner user_ids and override
        wk_pids = set(safe_id(wi.get("partner_id")) for wi in wk_inv if safe_id(wi.get("partner_id")))
        missing_pids = [p for p in wk_pids if p not in partner_user_map]
        if missing_pids:
            for ci in range(0, len(missing_pids), 200):
                chunk = missing_pids[ci:ci+200]
                _ps = sr(models, uid, "res.partner", [["id", "in", chunk]], ["id", "user_id"], limit=200)
                for _p in _ps:
                    _pu = canonical_vendedor(safe_name(_p.get("user_id")))
                    if _pu:
                        partner_user_map[_p["id"]] = _pu
        for wi in wk_inv:
            wp = safe_id(wi.get("partner_id"))
            if wp and wp in partner_user_map:
                wk_user_map[wi["id"]] = partner_user_map[wp]
        wk_margin_map = {i["id"]: i.get("margin_zone", 0) or 0 for i in wk_inv}
        wk_l = 0
        wk_lbu = defaultdict(float)
        wk_vbu = defaultdict(float)
        wk_mbu_venta = defaultdict(float)
        wk_mbu_costo = defaultdict(float)
        if wk_ids:
            wk_lines = sr(models, uid, "account.move.line", [
                ["move_id", "in", wk_ids],
                ["product_id", "=", DIESEL_PRODUCT_ID],
            ], ["move_id", "quantity", "price_subtotal"], limit=5000)
            for ln in wk_lines:
                q = ln.get("quantity", 0)
                s = ln.get("price_subtotal", 0)
                mid = safe_id(ln.get("move_id"))
                wk_l += q
                u = wk_user_map.get(mid, "Sin asignar")
                m = wk_margin_map.get(mid, 0)
                wk_lbu[u] += q
                wk_vbu[u] += s
                wk_mbu_venta[u] += s
                wk_mbu_costo[u] += s * (1 - m) if m else s
        # ── NC subtraction for weekly_history ──
        wk_nc_invs = sr(models, uid, "account.move", [
            ["move_type", "=", "out_refund"],
            ["state", "=", "posted"],
            ["invoice_date", ">=", fmt(ws_d)],
            ["invoice_date", "<=", fmt(we_d)],
        ], ["id", "partner_id", "invoice_user_id"], limit=1000)
        if wk_nc_invs:
            nc_id_map = {}
            for ni in wk_nc_invs:
                nid = ni["id"]
                np_ = safe_id(ni.get("partner_id"))
                if np_ and np_ in partner_user_map:
                    nc_id_map[nid] = partner_user_map[np_]
                else:
                    nc_id_map[nid] = canonical_vendedor(safe_name(ni.get("invoice_user_id")))
            nc_ids_ = [ni["id"] for ni in wk_nc_invs]
            wk_nc_lines = sr(models, uid, "account.move.line", [
                ["move_id", "in", nc_ids_],
                ["product_id", "=", DIESEL_PRODUCT_ID],
            ], ["move_id", "quantity"], limit=2000)
            for ln in wk_nc_lines:
                q = abs(ln.get("quantity", 0))
                mid = safe_id(ln.get("move_id"))
                u = nc_id_map.get(mid, "Sin asignar")
                wk_l -= q
                wk_lbu[u] -= q

        wk_margin_by_user = {}
        for u in wk_lbu:
            v = wk_mbu_venta.get(u, 0)
            c = wk_mbu_costo.get(u, 0)
            wk_margin_by_user[u] = round((1 - c / v) * 100, 1) if v > 0 else 0
        weekly_history.append({
            "label": f"{ws_d.day}/{ws_d.month}-{we_d.day}/{we_d.month}",
            "litros": round(wk_l),
            "litros_by_user": merge_by_user({k: round(v) for k, v in wk_lbu.items()}),
            "venta_by_user": merge_by_user({k: round(v) for k, v in wk_vbu.items()}),
            "margin_by_user": merge_by_user(wk_margin_by_user),
        })
    weekly_history.reverse()  # oldest first

    # ── Detalle por ejecutivo (para comisiones / mes vencido) ──
    detail_by_user = defaultdict(list)
    if inv_ids:
        for ln in lines:
            mid = safe_id(ln.get("move_id"))
            user = inv_user_map.get(mid, "Sin asignar")
            pid = inv_partner_map.get(mid)
            detail_by_user[user].append({
                "fecha": inv_date_map.get(mid, ""),
                "cliente": partner_name_map.get(pid, safe_name(next((i.get("partner_id") for i in invoices if i["id"] == mid), ""))),
                "rut": partner_vat_map.get(pid, ""),
                "litros": round(ln.get("quantity", 0)),
                "pv": round(ln.get("price_unit", 0), 2),
                "venta_neta": round(ln.get("price_subtotal", 0)),
                "zona": partner_zone.get(pid, "Sin zona"),
                "plazo_pago": inv_term_map.get(mid, "—"),
            })
    for u in detail_by_user:
        detail_by_user[u].sort(key=lambda x: x["fecha"])

    print(f"  Litros: {round(total_litros)} Facturas: {len(invoices)} NC: {len(ncs)}")
    print(f"  Detalle ejecutivo: {len(detail_by_user)} vendedores, {sum(len(v) for v in detail_by_user.values())} lineas")
    print(f"  Clientes nuevos: {new_cl_count}")

    # Ruta (visitas): leads que ENTRARON a etapa "Ruta" durante el periodo (flujo, no snapshot).
    # Fuente: mail.tracking.value (cambios de stage_id) — fechas reales, no date_last_stage_update (cron diario).
    ruta_visitas = 0
    ruta_visitas_by_user = defaultdict(int)
    try:
        rtv = sr(models, uid, "mail.tracking.value", [
            ["field_id.name", "=", "stage_id"],
            ["mail_message_id.model", "=", "crm.lead"],
            ["new_value_char", "=", "Ruta"],
            ["mail_message_id.date", ">=", fdt_s(m_start)],
            ["mail_message_id.date", "<=", fdt_e(m_end)],
        ], ["mail_message_id"], limit=20000)
        _ruta_msg_ids = list({t["mail_message_id"][0] for t in rtv if t.get("mail_message_id")})
        if _ruta_msg_ids:
            _ruta_msgs = sr(models, uid, "mail.message", [["id", "in", _ruta_msg_ids]],
                            ["id", "res_id"], limit=20000)
            _ruta_lead_ids = list({m["res_id"] for m in _ruta_msgs if m.get("res_id")})
            ruta_visitas = len(_ruta_lead_ids)
            if _ruta_lead_ids:
                _ruta_leads = sr(models, uid, "crm.lead", [["id", "in", _ruta_lead_ids]],
                                 ["id", "user_id"], limit=20000)
                for _l in _ruta_leads:
                    _u = canonical_vendedor(safe_name(_l.get("user_id"))) if _l.get("user_id") else "Sin asignar"
                    ruta_visitas_by_user[_u] += 1
        print(f"  Ruta (visitas): {ruta_visitas} leads entraron a Ruta")
    except Exception as _e:
        print(f"  Ruta visitas skipped: {_e}")

    return {
        "month_label": lbl,
        "month_start": fmt(m_start),
        "month_end": fmt(m_end),
        "ruta_visitas": ruta_visitas,
        "ruta_visitas_by_user": merge_by_user(dict(ruta_visitas_by_user)),
        "totals": {
            "total_litros": round(total_litros),
            "total_venta_neta": round(total_venta),
            "invoice_count": len(invoices),
            "nc_count": len(ncs),
            "nc_debug": nc_debug,
            "litros_by_user": merge_by_user({k: round(v) for k, v in litros_by_user.items()}),
            "venta_by_user": merge_by_user({k: round(v) for k, v in venta_by_user.items()}),
            "margin_retail_pct": margin_retail_pct,
            "margin_volume_pct": margin_volume_pct,
            "retail_litros": round(retail_litros),
            "volume_litros": round(volume_litros),
            "client_count": len(litros_by_partner),
            "retail_venta": round(retail_venta),
            "volume_venta": round(volume_venta),
            "litros_by_zone": {k: round(v) for k, v in sorted(litros_by_zone.items(), key=lambda x: -x[1])},
            "venta_by_zone": {k: round(v) for k, v in sorted(venta_by_zone.items(), key=lambda x: -x[1])},
            "margin_by_zone": {k: round((1 - margin_by_zone_costo[k] / margin_by_zone_venta[k]) * 100, 1) if margin_by_zone_venta[k] > 0 else 0 for k in litros_by_zone},
            "margin_by_user": merge_by_user({k: round((1 - margin_by_user_costo[k] / margin_by_user_venta[k]) * 100, 1) if margin_by_user_venta.get(k, 0) > 0 else 0 for k in litros_by_user}),
        },
        "new_clients": {
            "count": new_cl_count,
            "by_user": merge_by_user(dict(new_cl_by_user)),
            "litros_by_user": merge_by_user({k: round(v) for k, v in new_cl_litros_by_user.items()}),
            "detail": new_cl_detail,
        },
        "weekly": list(reversed(weekly_sales)),
        "weekly_history": weekly_history,
        "detail_by_user": merge_by_user_lists(detail_by_user),
    }


# ==============================================================
# PART 4: CHURN & RESCUE
# ==============================================================
LOST_THRESHOLD_DAYS = 270  # 9 months

def gather_latest_note(models, uid, pids):
    """Gestión MÁS RECIENTE por cliente, comparando POR FECHA entre:
       - chatter (mail.message comment/note) en res.partner y en sus crm.lead
       - actividades planeadas (mail.activity) en res.partner y crm.lead, por write_date (última edición)
       Devuelve {pid: {"body","date","author"}}. La más nueva gana, sea nota o actividad."""
    res = {}
    pids = [p for p in (pids or []) if p]
    if not pids:
        return res
    noise = ["lead enrichment", "nuevo lead para el equipo", "new lead for", "stage changed",
             "cambio de etapa", "oportunidad ganada", "oportunidad perdida", "se crea un nuevo canal",
             "ganado autom", "facturas pendientes", "proximo recordatorio", "próximo recordatorio",
             "fecha del proximo", "fecha del próximo", "opportunity won", "recordatorio ser",
             "cierre masivo de backlog", "reemplazo automatico", "lista de precios cambiada"]

    def _clean(b):
        # Odoo 18 registra notas/actividades como envoltura; extraer la gestión real.
        b = (b or "").strip()
        low = b.lower()
        if (low.startswith("actividades pendientes") or low.startswith("to-do done")
                or "done (originally assigned" in low) and ":" in b:
            b = b.split(":", 1)[1].strip()
        for tail in ("Original note:", "Feedback:", "feedback:"):
            idx = b.find(tail)
            if idx > 0:
                b = b[:idx].strip()
        return b

    def _ok(b):
        bl = b.lower()
        return len(b) > 3 and not any(n in bl for n in noise)

    def consider(pid, date, body, author):
        if not pid or not body:
            return
        body = body.strip()
        if not body:
            return
        cur = res.get(pid)
        if cur is None or (date or "") > (cur["date"] or ""):
            res[pid] = {"date": date or "", "body": body[:150], "author": author or ""}

    # Leads (todas las oportunidades) de estos clientes
    lead_to_pid = {}
    for l in sr(models, uid, "crm.lead",
                [["partner_id", "in", pids], ["active", "in", [True, False]]],
                ["id", "partner_id"], limit=20000):
        lp = safe_id(l.get("partner_id"))
        if lp:
            lead_to_pid[l["id"]] = lp
    lead_ids = list(lead_to_pid.keys())

    # Subtipos que cargan gestión REAL (Note = nota registrada, Activities = actividad
    # completada). Filtrar por subtipo evita traer tracking/cambios de etapa/won — que en
    # Odoo 18 son message_type='notification' y, sin filtrar, revientan el server con OOM.
    gest_subtypes = [s["id"] for s in sr(models, uid, "mail.message.subtype",
                     [["name", "in", ["Note", "Nota", "Activities", "Actividades"]]],
                     ["id"], limit=50)] or [2, 3]

    def _chunks(seq, n=100):
        for i in range(0, len(seq), n):
            yield seq[i:i + n]

    # 1. Chatter en res.partner (subtipos de gestión; Odoo 18 los guarda como 'notification').
    #    Batcheado por pids para no exceder memoria del servidor al serializar.
    for chunk in _chunks(pids):
        for m in sr(models, uid, "mail.message",
                    [["model", "=", "res.partner"], ["res_id", "in", chunk],
                     ["subtype_id", "in", gest_subtypes]],
                    ["res_id", "body", "date", "author_id"], limit=8000, order="date desc"):
            b = _clean(strip_html(m.get("body") or ""))
            if _ok(b):
                consider(m.get("res_id"), (m.get("date") or "")[:10], b,
                         safe_name(m.get("author_id")) if m.get("author_id") else "")
    # 2. Chatter en crm.lead
    for chunk in _chunks(lead_ids):
        for m in sr(models, uid, "mail.message",
                    [["model", "=", "crm.lead"], ["res_id", "in", chunk],
                     ["subtype_id", "in", gest_subtypes]],
                    ["res_id", "body", "date", "author_id"], limit=8000, order="date desc"):
            b = _clean(strip_html(m.get("body") or ""))
            if _ok(b):
                consider(lead_to_pid.get(m.get("res_id")), (m.get("date") or "")[:10], b,
                         safe_name(m.get("author_id")) if m.get("author_id") else "")
    # 3. Actividades planeadas en res.partner (write_date = última edición de la gestión)
    for chunk in _chunks(pids):
        for a in sr(models, uid, "mail.activity",
                    [["res_model", "=", "res.partner"], ["res_id", "in", chunk]],
                    ["res_id", "summary", "note", "write_date", "user_id"], limit=8000):
            b = (a.get("summary") or "").strip() or strip_html(a.get("note") or "")
            if b:
                consider(a.get("res_id"), (a.get("write_date") or "")[:10], b,
                         safe_name(a.get("user_id")) if a.get("user_id") else "Actividad")
    # 4. Actividades planeadas en crm.lead
    for chunk in _chunks(lead_ids):
        for a in sr(models, uid, "mail.activity",
                    [["res_model", "=", "crm.lead"], ["res_id", "in", chunk]],
                    ["res_id", "summary", "note", "write_date", "user_id"], limit=8000):
            b = (a.get("summary") or "").strip() or strip_html(a.get("note") or "")
            if b:
                consider(lead_to_pid.get(a.get("res_id")), (a.get("write_date") or "")[:10], b,
                         safe_name(a.get("user_id")) if a.get("user_id") else "Actividad")
    return res


def extract_churn_data(models, uid):
    print("\nExtracting Churn & Rescue data...")
    today = datetime.now().date()

    stages = sr(models, uid, "crm.stage", [], ["name"], limit=50)
    stage_map = {s["id"]: s["name"] for s in stages}
    durmiente_ids = [sid for sid, name in stage_map.items() if "durmiente" in name.lower()]
    perdido_ids = [sid for sid, name in stage_map.items() if "perdido" in name.lower() or "no cerrado" in name.lower()]

    print(f"  Stage IDs — Durmiente: {durmiente_ids}, Perdidos: {perdido_ids}")

    durmiente_leads = sr(models, uid, "crm.lead", [
        ["stage_id", "in", durmiente_ids],
        ["active", "=", True],
    ], ["partner_id", "user_id", "partner_name", "write_date", "create_date"], limit=5000)

    perdido_leads = sr(models, uid, "crm.lead", [
        ["stage_id", "in", perdido_ids],
        ["active", "=", True],
    ], ["partner_id", "user_id", "partner_name", "write_date", "create_date"], limit=5000)

    print(f"  CRM: {len(durmiente_leads)} durmientes, {len(perdido_leads)} perdidos")

    # ── Fecha REAL del cambio de etapa vía mail.tracking.value ──
    # El cron de Odoo re-estampa write_date/date_last_stage_update en batch (todos = hoy),
    # así que "nuevos durmientes/perdidos de la semana" mostraba SIEMPRE lo mismo.
    # El tracking del chatter conserva la fecha verdadera del cambio de etapa.
    _target_names = [stage_map[s] for s in durmiente_ids + perdido_ids]
    _lead_ids = [l["id"] for l in durmiente_leads + perdido_leads]
    _stage_real = {}  # lead_id -> 'YYYY-MM-DD' del último cambio a etapa durmiente/perdido
    try:
        _trk = []
        for i in range(0, len(_lead_ids), 400):
            _trk += sr(models, uid, "mail.tracking.value", [
                ["mail_message_id.model", "=", "crm.lead"],
                ["mail_message_id.res_id", "in", _lead_ids[i:i+400]],
                ["new_value_char", "in", _target_names],
            ], ["mail_message_id", "new_value_char", "create_date"], limit=20000)
        _mids = list({safe_id(t.get("mail_message_id")) for t in _trk if t.get("mail_message_id")})
        _mid_res = {}
        for i in range(0, len(_mids), 500):
            for m in sr(models, uid, "mail.message", [["id", "in", _mids[i:i+500]]], ["res_id"], limit=1000):
                _mid_res[m["id"]] = m.get("res_id")
        for t in sorted(_trk, key=lambda x: x.get("create_date") or ""):
            lid = _mid_res.get(safe_id(t.get("mail_message_id")))
            if lid:
                _stage_real[lid] = (t.get("create_date") or "")[:10]
        print(f"  Fechas reales de etapa (tracking): {len(_stage_real)}/{len(_lead_ids)} leads")
    except Exception as _e:
        print(f"  Tracking de etapa skipped: {_e}")

    month_start = today.replace(day=1)
    prev_month_end = month_start - timedelta(days=1)
    prev_month_start = prev_month_end.replace(day=1)

    curr_inv = sr(models, uid, "account.move", [
        ["move_type", "=", "out_invoice"],
        ["state", "=", "posted"],
        ["invoice_date", ">=", fmt(month_start)],
        ["invoice_date", "<=", fmt(today)],
    ], ["partner_id", "invoice_user_id"], limit=10000)

    prev_inv = sr(models, uid, "account.move", [
        ["move_type", "=", "out_invoice"],
        ["state", "=", "posted"],
        ["invoice_date", ">=", fmt(prev_month_start)],
        ["invoice_date", "<=", fmt(prev_month_end)],
    ], ["partner_id"], limit=10000)

    curr_month_partners = set(safe_id(i.get("partner_id")) for i in curr_inv if safe_id(i.get("partner_id")))

    # Build name set from current month invoices for matching when CRM lead has no partner_id
    curr_month_partner_names = set()
    for i in curr_inv:
        pname = safe_name(i.get("partner_id"))
        if pname and pname != "Sin asignar":
            curr_month_partner_names.add(pname.strip().upper())

    prev_month_partners = set(safe_id(i.get("partner_id")) for i in prev_inv if safe_id(i.get("partner_id")))
    prev_month_clients = len(prev_month_partners)

    # Fetch partner.user_id for all leads (source of truth for vendedor)
    _all_lead_pids = set()
    for _l in durmiente_leads + perdido_leads:
        _p = safe_id(_l.get("partner_id"))
        if _p:
            _all_lead_pids.add(_p)
    _churn_partner_user = {}
    batch_size = 200
    for i in range(0, len(list(_all_lead_pids)), batch_size):
        batch = list(_all_lead_pids)[i:i+batch_size]
        _ps = sr(models, uid, "res.partner", [["id", "in", batch]], ["id", "user_id"], limit=batch_size)
        for _p in _ps:
            _pu = canonical_vendedor(safe_name(_p.get("user_id")))
            if _pu:
                _churn_partner_user[_p["id"]] = _pu

    dormant_list = []
    dormant_by_user = Counter()
    rescued_dormant_list = []
    rescued_dormant_by_user = Counter()
    seen_dormant = set()

    for lead in durmiente_leads:
        pid = safe_id(lead.get("partner_id"))
        user = _churn_partner_user.get(pid) or canonical_vendedor(safe_name(lead.get("user_id")))
        name = safe_name(lead.get("partner_id")) or lead.get("partner_name", "?")
        name_key = name.strip().upper()
        if name_key in seen_dormant:
            continue
        seen_dormant.add(name_key)
        write_date = _stage_real.get(lead["id"]) or (lead.get("write_date") or "")[:10]

        invoiced = (pid and pid in curr_month_partners) or (name_key in curr_month_partner_names)

        if invoiced:
            rescued_dormant_list.append({"name": name, "user": user, "last_update": write_date, "partner_id": pid})
            rescued_dormant_by_user[user] += 1
        else:
            dormant_list.append({"name": name, "user": user, "last_update": write_date, "partner_id": pid})
            dormant_by_user[user] += 1

    # ── Avg monthly litros (8 months) for dormant clients ──
    eight_months_ago = (today.replace(day=1) - timedelta(days=1))  # end of prev month
    for _ in range(7):
        eight_months_ago = (eight_months_ago.replace(day=1) - timedelta(days=1))
    eight_months_start = eight_months_ago.replace(day=1)
    batch_size = 200

    dormant_pids = [c["partner_id"] for c in dormant_list if c.get("partner_id")]
    avg_litros_map = {}
    if dormant_pids:
        print(f"  Querying 8-month litros for {len(dormant_pids)} dormant partners ({fmt(eight_months_start)} → {fmt(today)})...")

        # Get invoices for these partners in the 8-month window
        all_inv_lines = []
        for i in range(0, len(dormant_pids), batch_size):
            batch = dormant_pids[i:i+batch_size]
            invs = sr(models, uid, "account.move", [
                ["move_type", "=", "out_invoice"],
                ["state", "=", "posted"],
                ["partner_id", "in", batch],
                ["invoice_date", ">=", fmt(eight_months_start)],
                ["invoice_date", "<=", fmt(today)],
            ], ["id", "partner_id"], limit=50000)

            inv_ids = [inv["id"] for inv in invs]
            inv_pid_map = {inv["id"]: safe_id(inv.get("partner_id")) for inv in invs}

            if inv_ids:
                lines = sr(models, uid, "account.move.line", [
                    ["move_id", "in", inv_ids],
                    ["product_id", "=", DIESEL_PRODUCT_ID],
                ], ["move_id", "quantity"], limit=50000)
                for ln in lines:
                    mid = safe_id(ln.get("move_id"))
                    pid_ln = inv_pid_map.get(mid)
                    if pid_ln:
                        avg_litros_map[pid_ln] = avg_litros_map.get(pid_ln, 0) + (ln.get("quantity", 0) or 0)

        # Credit notes — subtract
        for i in range(0, len(dormant_pids), batch_size):
            batch = dormant_pids[i:i+batch_size]
            ncs = sr(models, uid, "account.move", [
                ["move_type", "=", "out_refund"],
                ["state", "=", "posted"],
                ["partner_id", "in", batch],
                ["invoice_date", ">=", fmt(eight_months_start)],
                ["invoice_date", "<=", fmt(today)],
            ], ["id", "partner_id"], limit=10000)

            nc_ids = [nc["id"] for nc in ncs]
            nc_pid_map = {nc["id"]: safe_id(nc.get("partner_id")) for nc in ncs}

            if nc_ids:
                nc_lines = sr(models, uid, "account.move.line", [
                    ["move_id", "in", nc_ids],
                    ["product_id", "=", DIESEL_PRODUCT_ID],
                ], ["move_id", "quantity"], limit=10000)
                for ln in nc_lines:
                    mid = safe_id(ln.get("move_id"))
                    pid_ln = nc_pid_map.get(mid)
                    if pid_ln:
                        avg_litros_map[pid_ln] = avg_litros_map.get(pid_ln, 0) - (ln.get("quantity", 0) or 0)

        # Divide by 8 for average
        avg_litros_map = {pid: round(max(total, 0) / 8) for pid, total in avg_litros_map.items()}
        print(f"  Avg monthly litros computed for {len(avg_litros_map)} partners")

    # ── Last note for dormant + lost clients (shared logic) ──
    # Strategy: for each partner, find the most recent crm.lead, then get the
    # latest mail.message OR mail.activity on that lead.
    # This mirrors pipeline note logic and works even when partner has no direct messages.
    all_churn_pids = list(set(dormant_pids or []))
    last_note_by_pid = {}
    if all_churn_pids:
        try:
            _latest = gather_latest_note(models, uid, all_churn_pids)
            last_note_by_pid = {p: v["body"] for p, v in _latest.items()}
            print(f"  Churn notes found: {len(last_note_by_pid)}/{len(all_churn_pids)} partners")
        except Exception as e:
            print(f"  Churn notes skipped: {e}")

    for c in dormant_list:
        pid = c.get("partner_id")
        c["last_note"] = last_note_by_pid.get(pid, "")

    # Attach avg_monthly_litros and remove internal partner_id
    for c in dormant_list:
        c["avg_monthly_litros"] = avg_litros_map.get(c.get("partner_id"), 0)
        c.pop("partner_id", None)

    lost_list = []
    lost_by_user = Counter()
    rescued_lost_list = []
    rescued_lost_by_user = Counter()
    newly_lost = 0
    seen_lost = set()

    for lead in perdido_leads:
        pid = safe_id(lead.get("partner_id"))
        user = _churn_partner_user.get(pid) or canonical_vendedor(safe_name(lead.get("user_id")))
        name = safe_name(lead.get("partner_id")) or lead.get("partner_name", "?")
        name_key = name.strip().upper()
        if name_key in seen_lost:
            continue
        seen_lost.add(name_key)
        write_date = _stage_real.get(lead["id"]) or (lead.get("write_date") or "")[:10]

        if write_date >= fmt(month_start):
            newly_lost += 1

        invoiced = (pid and pid in curr_month_partners) or (name_key in curr_month_partner_names)

        if invoiced:
            rescued_lost_list.append({"name": name, "user": user, "last_update": write_date, "partner_id": pid})
            rescued_lost_by_user[user] += 1
        else:
            lost_list.append({"name": name, "user": user, "last_update": write_date, "partner_id": pid})
            lost_by_user[user] += 1

    # ── Avg monthly litros (8 months) for lost clients ──
    lost_pids = [c["partner_id"] for c in lost_list if c.get("partner_id")]
    avg_litros_lost = {}
    if lost_pids:
        # Lost clients by definition have no invoices in last 8 months —
        # use 24-month window to capture their last active period
        lost_lookback = today.replace(day=1) - timedelta(days=365*2)
        lost_lookback_start = lost_lookback.replace(day=1)
        print(f"  Querying 24-month litros for {len(lost_pids)} lost partners ({fmt(lost_lookback_start)} → {fmt(today)})...")
        for i in range(0, len(lost_pids), batch_size):
            batch = lost_pids[i:i+batch_size]
            invs = sr(models, uid, "account.move", [
                ["move_type", "=", "out_invoice"],
                ["state", "=", "posted"],
                ["partner_id", "in", batch],
                ["invoice_date", ">=", fmt(lost_lookback_start)],
                ["invoice_date", "<=", fmt(today)],
            ], ["id", "partner_id"], limit=50000)
            inv_ids_l = [inv["id"] for inv in invs]
            inv_pid_map_l = {inv["id"]: safe_id(inv.get("partner_id")) for inv in invs}
            if inv_ids_l:
                lines = sr(models, uid, "account.move.line", [
                    ["move_id", "in", inv_ids_l],
                    ["product_id", "=", DIESEL_PRODUCT_ID],
                ], ["move_id", "quantity"], limit=50000)
                for ln in lines:
                    mid = safe_id(ln.get("move_id"))
                    pid_ln = inv_pid_map_l.get(mid)
                    if pid_ln:
                        avg_litros_lost[pid_ln] = avg_litros_lost.get(pid_ln, 0) + (ln.get("quantity", 0) or 0)

        for i in range(0, len(lost_pids), batch_size):
            batch = lost_pids[i:i+batch_size]
            ncs_l = sr(models, uid, "account.move", [
                ["move_type", "=", "out_refund"],
                ["state", "=", "posted"],
                ["partner_id", "in", batch],
                ["invoice_date", ">=", fmt(lost_lookback_start)],
                ["invoice_date", "<=", fmt(today)],
            ], ["id", "partner_id"], limit=10000)
            nc_ids_l = [nc["id"] for nc in ncs_l]
            nc_pid_map_l = {nc["id"]: safe_id(nc.get("partner_id")) for nc in ncs_l}
            if nc_ids_l:
                nc_lines_l = sr(models, uid, "account.move.line", [
                    ["move_id", "in", nc_ids_l],
                    ["product_id", "=", DIESEL_PRODUCT_ID],
                ], ["move_id", "quantity"], limit=10000)
                for ln in nc_lines_l:
                    mid = safe_id(ln.get("move_id"))
                    pid_ln = nc_pid_map_l.get(mid)
                    if pid_ln:
                        avg_litros_lost[pid_ln] = avg_litros_lost.get(pid_ln, 0) - (ln.get("quantity", 0) or 0)

        avg_litros_lost = {pid: round(max(total, 0) / 24) for pid, total in avg_litros_lost.items()}
        print(f"  Avg monthly litros computed for {len(avg_litros_lost)} lost partners")

    # Lost notes: gestión más reciente para perdidos (chatter + actividades, por fecha)
    extra_pids = [p for p in lost_pids if p not in last_note_by_pid]
    if extra_pids:
        try:
            _latest_lost = gather_latest_note(models, uid, extra_pids)
            for _p, _v in _latest_lost.items():
                last_note_by_pid[_p] = _v["body"]
            still_p = [p for p in extra_pids if p not in last_note_by_pid]
            if still_p:
                for pc in sr(models, uid, "res.partner", [["id", "in", still_p]], ["id", "comment"], limit=5000):
                    cmt = strip_html(pc.get("comment") or "")
                    if pc.get("id") and pc["id"] not in last_note_by_pid and cmt:
                        last_note_by_pid[pc["id"]] = cmt[:150]
            print(f"  Lost notes extended: {len([p for p in lost_pids if p in last_note_by_pid])}/{len(lost_pids)}")
        except Exception as e:
            print(f"  Lost notes extension skipped: {e}")

    for c in lost_list:
        pid = c.get("partner_id")
        c["last_note"] = last_note_by_pid.get(pid, "")

    for c in lost_list:
        c["avg_monthly_litros"] = avg_litros_lost.get(c.get("partner_id"), 0)
        c.pop("partner_id", None)

    # ── Avg monthly litros (8 meses) para RESCATADOS (KPI Litros Rescatados en tab CS) ──
    _resc_all = rescued_dormant_list + rescued_lost_list
    _resc_pids = list({r["partner_id"] for r in _resc_all if r.get("partner_id")})
    _resc_litros = {}
    if _resc_pids:
        try:
            eight_start = today - timedelta(days=240)
            for mt, sign in (("out_invoice", 1), ("out_refund", -1)):
                _mv = sr(models, uid, "account.move", [
                    ["move_type", "=", mt], ["state", "=", "posted"],
                    ["partner_id", "in", _resc_pids],
                    ["invoice_date", ">=", fmt(eight_start)],
                ], ["id", "partner_id"], limit=5000)
                _mv_pid = {m["id"]: safe_id(m.get("partner_id")) for m in _mv}
                if _mv_pid:
                    for ln in sr(models, uid, "account.move.line", [
                        ["move_id", "in", list(_mv_pid.keys())],
                        ["product_id", "=", DIESEL_PRODUCT_ID],
                    ], ["move_id", "quantity"], limit=10000):
                        _p = _mv_pid.get(safe_id(ln.get("move_id")))
                        if _p:
                            _resc_litros[_p] = _resc_litros.get(_p, 0) + sign * (ln.get("quantity", 0) or 0)
            _resc_litros = {p: round(max(t, 0) / 8) for p, t in _resc_litros.items()}
        except Exception as _e:
            print(f"  Litros rescatados skipped: {_e}")
    for r in _resc_all:
        r["avg_monthly_litros"] = _resc_litros.get(r.get("partner_id"), 0)
    for r in rescued_lost_list:
        r.pop("partner_id", None)

    active_count = len(curr_month_partners | prev_month_partners)
    # ── Churn correcto: basado en FACTURACIÓN (no en etapa CRM, que el cron re-estampa) ──
    # Perdido(M) = cliente cuya última factura fue en el mes (M-9) → cruza los 9 meses en M,
    #              EXCLUYENDO estacionales (clientes con brecha histórica >9 meses que volvieron).
    # Actuales(M) = clientes distintos que facturaron ese mes calendario.
    # Churn(M) = Perdidos(M) / Actuales(M-1).  M = último mes calendario cerrado.
    def _ym(dstr):
        try:
            return int(dstr[:4]) * 12 + (int(dstr[5:7]) - 1)
        except Exception:
            return None

    churn_label = ""
    try:
        _first_this = today.replace(day=1)
        _last_closed = _first_this - timedelta(days=1)            # último día del mes cerrado
        M = _last_closed.year * 12 + (_last_closed.month - 1)
        M_prev = M - 1
        M_lost_origin = M - 9                                     # última compra que cruza 9 meses en M

        _hist_start = (_first_this - timedelta(days=930)).strftime("%Y-%m-01")
        ch_inv = sr(models, uid, "account.move", [
            ["move_type", "=", "out_invoice"],
            ["state", "=", "posted"],
            ["invoice_date", ">=", _hist_start],
        ], ["partner_id", "invoice_date"], limit=100000, order="invoice_date asc")

        months_by_pid = defaultdict(set)
        for inv in ch_inv:
            pid = safe_id(inv.get("partner_id"))
            ym = _ym(inv.get("invoice_date") or "")
            if pid and ym is not None:
                months_by_pid[pid].add(ym)

        actuales_prev = 0
        newly_lost_inv = 0
        seasonal_excl = 0
        for pid, mset in months_by_pid.items():
            if M_prev in mset:
                actuales_prev += 1
            if max(mset) == M_lost_origin:
                sm = sorted(mset)
                is_seasonal = any((sm[i + 1] - sm[i]) > 9 for i in range(len(sm) - 1))
                if is_seasonal:
                    seasonal_excl += 1
                else:
                    newly_lost_inv += 1

        newly_lost = newly_lost_inv
        prev_month_clients = actuales_prev
        churn_pct = round((newly_lost_inv / actuales_prev) * 100, 1) if actuales_prev > 0 else 0
        churn_label = f"{SPANISH_MONTHS[_last_closed.month]}-{_last_closed.strftime('%y')}"
        print(f"  Churn {churn_label}: {newly_lost_inv} perdidos / {actuales_prev} actuales = {churn_pct}% (excl. {seasonal_excl} estacionales)")
    except Exception as _e:
        churn_pct = 0
        print(f"  Churn calc (facturación) skipped: {_e}")
    total_rescued = len(rescued_dormant_list) + len(rescued_lost_list)
    # Denominator: dormant pool (actively rescuable) + those already rescued.
    # Perdidos (9+ months) are excluded because they're essentially unreachable and dilute the metric.
    # `rescued_lost` still counts in the numerator as a bonus win.
    total_at_risk = len(dormant_list) + len(rescued_dormant_list) + len(rescued_lost_list)
    rescue_pct = round((total_rescued / max(total_at_risk, 1)) * 100, 1)

    print(f"  Activos (2 meses): {active_count}")
    print(f"  Durmientes CRM: {len(dormant_list)} (+{len(rescued_dormant_list)} rescatados)")
    print(f"  Perdidos CRM: {len(lost_list)} (+{len(rescued_lost_list)} rescatados)")
    print(f"  Nuevos perdidos este mes: {newly_lost}")
    print(f"  Churn: {churn_pct}% ({newly_lost}/{prev_month_clients})")

    return {
        "summary": {
            "active": active_count,
            "dormant": len(dormant_list),
            "lost": len(lost_list),
            "newly_lost": newly_lost,
            "rescued_dormant": len(rescued_dormant_list),
            "rescued_lost": len(rescued_lost_list),
            "prev_month_clients": prev_month_clients,
            "churn_pct": churn_pct,
            "churn_label": churn_label,
            "rescue_pct": rescue_pct,
        },
        "by_user": {
            "dormant": dict(dormant_by_user),
            "lost": dict(lost_by_user),
            "rescued_dormant": dict(rescued_dormant_by_user),
            "rescued_lost": dict(rescued_lost_by_user),
        },
        "dormant_list": sorted(dormant_list, key=lambda x: x.get("avg_monthly_litros", 0), reverse=True)[:50],
        "lost_list": sorted(lost_list, key=lambda x: x.get("avg_monthly_litros", 0), reverse=True)[:30],
        "rescued_dormant_list": rescued_dormant_list[:20],
        "rescued_lost_list": rescued_lost_list[:20],
        # Full partner ID list for mantención calc (not displayed)
        "_rescued_dormant_partner_ids": [r.get("partner_id") for r in rescued_dormant_list if r.get("partner_id")],
    }


# ==============================================================
# PART 5: RESCUED CLIENTS (based on frecuencia_facturacion)
# ==============================================================
def parse_frecuencia_days(freq_str):
    """Parse frecuencia_facturacion char field into days.
    Examples: 'Semanal' → 7, 'Quincenal' → 15, 'Mensual' → 30,
              'Irregular (Promedio: 12 días)' → 12, 'Bimensual' → 60
    """
    if not freq_str:
        return None
    f = freq_str.strip().lower()
    if "semanal" in f and "bi" not in f and "quince" not in f:
        return 7
    if "quincenal" in f:
        return 15
    if "mensual" in f and "bi" not in f and "tri" not in f:
        return 30
    if "bimensual" in f or "bimestral" in f:
        return 60
    if "trimestral" in f:
        return 90
    # Irregular (Promedio: X días)
    m = re.search(r'(\d+)\s*d[ií]a', f)
    if m:
        return int(m.group(1))
    # Fallback: try to find any number
    m2 = re.search(r'(\d+)', f)
    if m2:
        return int(m2.group(1))
    return None


def extract_rescued_clients(models, uid):
    """
    Detect 'rescued' clients: bought this month BUT the gap since their
    previous invoice exceeded 2× their frecuencia_facturacion.
    Split into: Rescatados Durmientes (gap < 270d) and Rescatados Perdidos (gap ≥ 270d).
    Returns litros, vendedor (from this month's invoice), and client name.
    """
    print("\nExtracting Rescued Clients (frecuencia-based)...")
    today = datetime.now().date()
    month_start = today.replace(day=1)

    # 1. Get all invoices this month with partner + salesperson
    curr_invoices = sr(models, uid, "account.move", [
        ["move_type", "=", "out_invoice"],
        ["state", "=", "posted"],
        ["invoice_date", ">=", fmt(month_start)],
        ["invoice_date", "<=", fmt(today)],
    ], ["partner_id", "invoice_user_id", "invoice_date"], limit=10000)

    # Unique partners who bought this month
    curr_partner_ids = list(set(
        safe_id(i.get("partner_id")) for i in curr_invoices
        if safe_id(i.get("partner_id"))
    ))
    print(f"  Partners with invoices this month: {len(curr_partner_ids)}")

    if not curr_partner_ids:
        return {"rescued_durmientes": [], "rescued_perdidos": [], "summary": {}}

    # Map partner → salesperson from this month's invoice (first invoice found)
    partner_vendedor = {}
    for inv in curr_invoices:
        pid = safe_id(inv.get("partner_id"))
        if pid and pid not in partner_vendedor:
            partner_vendedor[pid] = canonical_vendedor(safe_name(inv.get("invoice_user_id")))

    # 2. Read frecuencia_facturacion from res.partner (batch)
    partner_freq = {}
    partner_names = {}
    batch_size = 200
    for i in range(0, len(curr_partner_ids), batch_size):
        batch = curr_partner_ids[i:i+batch_size]
        partners = sr(models, uid, "res.partner", [
            ["id", "in", batch],
        ], ["id", "name", "user_id", "frecuencia_facturacion"], limit=batch_size)
        for p in partners:
            freq_days = parse_frecuencia_days(p.get("frecuencia_facturacion"))
            if freq_days is not None:
                partner_freq[p["id"]] = freq_days
            partner_names[p["id"]] = p.get("name", "?")
            # Override vendedor with partner.user_id (source of truth)
            pu = canonical_vendedor(safe_name(p.get("user_id")))
            if pu:
                partner_vendedor[p["id"]] = pu

    print(f"  Partners with parseable frecuencia: {len(partner_freq)} / {len(curr_partner_ids)}")

    if not partner_freq:
        return {"rescued_durmientes": [], "rescued_perdidos": [], "summary": {}}

    # 3. For each partner with frecuencia, find the last invoice BEFORE this month
    rescued_durmientes = []
    rescued_perdidos = []
    pids_to_check = list(partner_freq.keys())

    for i in range(0, len(pids_to_check), batch_size):
        batch = pids_to_check[i:i+batch_size]
        # Get the most recent invoice before this month for each partner in batch
        prev_invs = sr(models, uid, "account.move", [
            ["move_type", "=", "out_invoice"],
            ["state", "=", "posted"],
            ["partner_id", "in", batch],
            ["invoice_date", "<", fmt(month_start)],
        ], ["partner_id", "invoice_date"], limit=50000, order="invoice_date desc")

        # Group by partner_id → take most recent
        last_invoice_by_partner = {}
        for inv in prev_invs:
            pid = safe_id(inv.get("partner_id"))
            if pid and pid not in last_invoice_by_partner:
                last_invoice_by_partner[pid] = inv.get("invoice_date", "")[:10]

        # 4. Check gap vs 2× frecuencia
        for pid in batch:
            freq_days = partner_freq[pid]
            threshold = freq_days * 2
            last_date_str = last_invoice_by_partner.get(pid)

            if not last_date_str:
                # No previous invoice → this is essentially a new client, skip
                continue

            try:
                last_date = datetime.strptime(last_date_str, "%Y-%m-%d").date()
            except (ValueError, TypeError):
                continue

            gap_days = (month_start - last_date).days

            if gap_days > threshold:
                entry = {
                    "name": partner_names.get(pid, "?"),
                    "vendedor": partner_vendedor.get(pid, "Sin asignar"),
                    "frecuencia_dias": freq_days,
                    "gap_dias": gap_days,
                    "ultima_factura_anterior": last_date_str,
                    "partner_id": pid,
                }
                if gap_days >= LOST_THRESHOLD_DAYS:
                    rescued_perdidos.append(entry)
                else:
                    rescued_durmientes.append(entry)

    # 5. Get litros this month for each rescued client
    rescued_pids = [r["partner_id"] for r in rescued_durmientes + rescued_perdidos]
    litros_by_rescued = {}
    if rescued_pids:
        for i in range(0, len(rescued_pids), batch_size):
            batch = rescued_pids[i:i+batch_size]
            invs = sr(models, uid, "account.move", [
                ["move_type", "=", "out_invoice"],
                ["state", "=", "posted"],
                ["partner_id", "in", batch],
                ["invoice_date", ">=", fmt(month_start)],
                ["invoice_date", "<=", fmt(today)],
            ], ["id", "partner_id"], limit=10000)
            inv_ids = [inv["id"] for inv in invs]
            inv_pid = {inv["id"]: safe_id(inv.get("partner_id")) for inv in invs}

            if inv_ids:
                lines = sr(models, uid, "account.move.line", [
                    ["move_id", "in", inv_ids],
                    ["product_id", "=", DIESEL_PRODUCT_ID],
                ], ["move_id", "quantity"], limit=10000)
                for ln in lines:
                    mid = safe_id(ln.get("move_id"))
                    pid = inv_pid.get(mid)
                    if pid:
                        litros_by_rescued[pid] = litros_by_rescued.get(pid, 0) + (ln.get("quantity", 0) or 0)

    # Attach litros and clean up
    for r in rescued_durmientes + rescued_perdidos:
        r["litros"] = round(litros_by_rescued.get(r["partner_id"], 0))
        r.pop("partner_id", None)

    rescued_durmientes.sort(key=lambda x: -x["litros"])
    rescued_perdidos.sort(key=lambda x: -x["litros"])

    total_litros_d = sum(r["litros"] for r in rescued_durmientes)
    total_litros_p = sum(r["litros"] for r in rescued_perdidos)

    print(f"  Rescatados Durmientes: {len(rescued_durmientes)} ({round(total_litros_d)} L)")
    print(f"  Rescatados Perdidos: {len(rescued_perdidos)} ({round(total_litros_p)} L)")

    return {
        "rescued_durmientes": rescued_durmientes[:50],
        "rescued_perdidos": rescued_perdidos[:30],
        "summary": {
            "count_durmientes": len(rescued_durmientes),
            "count_perdidos": len(rescued_perdidos),
            "litros_durmientes": round(total_litros_d),
            "litros_perdidos": round(total_litros_p),
            "total_rescued": len(rescued_durmientes) + len(rescued_perdidos),
            "total_litros": round(total_litros_d + total_litros_p),
        },
    }


# ==============================================================
# PART 5b: RECOVERY CLIENTS (Quick Wins)
# Clientes que compraron en 2025 (≥500 L/mes diesel B1) y no han
# facturado en 2026, separando los estacionales (cuyos meses de
# compra histórica aún no han llegado en 2026) del listado de
# recuperables activos. Top 50 por volumen 2025.
# ==============================================================
def extract_recovery_clients(models, uid):
    print("\nExtracting Recovery (Quick Wins)...")
    from collections import defaultdict
    today = datetime.now().date()
    current_month = today.month  # mes actual en 2026

    # ── 1. Facturas 2025 (sólo posted, sólo venta) ──
    inv_2025 = sr(models, uid, "account.move", [
        ["move_type", "=", "out_invoice"],
        ["state", "=", "posted"],
        ["invoice_date", ">=", "2025-01-01"],
        ["invoice_date", "<=", "2025-12-31"],
    ], ["id", "partner_id", "invoice_date"], limit=20000)
    print(f"  2025 invoices: {len(inv_2025)}")

    ids_2025 = [i["id"] for i in inv_2025]
    move_to_partner = {i["id"]: safe_id(i.get("partner_id")) for i in inv_2025}
    move_to_month = {i["id"]: int((i.get("invoice_date") or "2025-01-01")[5:7]) for i in inv_2025}

    # ── 2. Líneas Diesel B1 2025 → litros por partner Y litros por (partner, mes) ──
    lines_2025 = []
    for chunk_start in range(0, len(ids_2025), 200):
        chunk = ids_2025[chunk_start:chunk_start+200]
        lines_2025 += sr(models, uid, "account.move.line", [
            ["move_id", "in", chunk],
            ["product_id", "=", DIESEL_PRODUCT_ID],
        ], ["move_id", "quantity"], limit=20000)

    litros_partner_2025 = defaultdict(float)
    litros_partner_month_2025 = defaultdict(lambda: defaultdict(float))  # pid -> {month: litros}
    for ln in lines_2025:
        mid = safe_id(ln.get("move_id"))
        pid = move_to_partner.get(mid, 0)
        if not pid:
            continue
        qty = ln.get("quantity", 0) or 0
        litros_partner_2025[pid] += qty
        m = move_to_month.get(mid, 0)
        if m:
            litros_partner_month_2025[pid][m] += qty

    # avg mensual = total 2025 / 12 (sigue el patrón del codebase)
    avg_lpm_2025 = {pid: l / 12 for pid, l in litros_partner_2025.items()}

    # ── 3. Facturas 2026 (qué partners están activos) y litros diesel 2026 por partner ──
    inv_2026 = sr(models, uid, "account.move", [
        ["move_type", "=", "out_invoice"],
        ["state", "=", "posted"],
        ["invoice_date", ">=", "2026-01-01"],
    ], ["id", "partner_id", "invoice_date"], limit=20000)
    print(f"  2026 invoices: {len(inv_2026)}")

    move_to_partner_26 = {i["id"]: safe_id(i.get("partner_id")) for i in inv_2026}
    active_2026 = {pid for pid in move_to_partner_26.values() if pid}

    ids_2026 = [i["id"] for i in inv_2026]
    lines_2026 = []
    for chunk_start in range(0, len(ids_2026), 200):
        chunk = ids_2026[chunk_start:chunk_start+200]
        lines_2026 += sr(models, uid, "account.move.line", [
            ["move_id", "in", chunk],
            ["product_id", "=", DIESEL_PRODUCT_ID],
        ], ["move_id", "quantity"], limit=20000)

    litros_partner_2026 = defaultdict(float)
    for ln in lines_2026:
        mid = safe_id(ln.get("move_id"))
        pid = move_to_partner_26.get(mid, 0)
        if pid:
            litros_partner_2026[pid] += ln.get("quantity", 0) or 0

    # ── 4. Candidatos: ≥500 L/mes en 2025 (no exigimos cero en 2026, separamos por caída) ──
    candidates = [pid for pid, lpm in avg_lpm_2025.items()
                  if lpm >= 500 and pid != 0]
    print(f"  Candidates (≥500 L/mes 2025): {len(candidates)}")

    if not candidates:
        return {"recoverable": [], "seasonal": [], "summary": {
            "recoverable_count": 0, "seasonal_count": 0,
            "recoverable_lpm": 0, "seasonal_lpm": 0,
        }}

    # ── 5. Datos del partner (sin límite restrictivo) ──
    partners_raw = []
    for i in range(0, len(candidates), 200):
        partners_raw += sr(models, uid, "res.partner",
            [["id", "in", candidates[i:i+200]]],
            ["id", "name", "delivery_zone_id", "is_volume_client", "phone", "user_id", "comment"],
            limit=5000)
    pmap = {p["id"]: p for p in partners_raw}

    # ── 6. Último lead CRM por partner (para vendedor = crm.lead.user_id) ──
    crm_leads = sr(models, uid, "crm.lead", [
        ["partner_id", "in", candidates],
    ], ["id", "partner_id", "stage_id", "user_id", "write_date"], limit=10000, order="write_date desc")

    last_lead = {}        # pid -> {stage, exec, last_crm}
    rec_lead_to_pid = {}  # lead_id -> pid (todas las oportunidades del cliente)
    for l in crm_leads:
        pid = safe_id(l.get("partner_id"))
        if not pid:
            continue
        rec_lead_to_pid[l["id"]] = pid
        if pid in last_lead:
            continue
        last_lead[pid] = {
            "stage": safe_name(l.get("stage_id")) if l.get("stage_id") else "",
            "exec": canonical_vendedor(safe_name(l.get("user_id"))) if l.get("user_id") else "",
            "last_crm": (l.get("write_date") or "")[:10],
        }

    # ── 7. Gestión de cliente MÁS RECIENTE (chatter + actividades planeadas, por fecha) ──
    last_note = gather_latest_note(models, uid, candidates)

    # ── 8. Clasificar cada candidato: recoverable vs seasonal ──
    SPANISH_MONTHS = ["", "ene", "feb", "mar", "abr", "may", "jun",
                      "jul", "ago", "sep", "oct", "nov", "dic"]

    recoverable = []
    seasonal = []

    for pid in candidates:
        p = pmap.get(pid)
        if not p:
            continue
        pname = p.get("name", "") or ""
        if not pname or "Predeterminado" in pname:
            continue

        lpm = avg_lpm_2025[pid]
        litros_2025_total = litros_partner_2025[pid]
        litros_2026_total = litros_partner_2026.get(pid, 0)

        # Patrón mensual 2025: meses en que compró
        months_active_2025 = sorted(litros_partner_month_2025[pid].keys())
        months_count_2025 = len(months_active_2025)

        # Caída % vs período comparable: 2025 mismos meses transcurridos vs 2026 YTD
        # period_2025 = litros en meses 1..current_month en 2025
        period_2025 = sum(litros_partner_month_2025[pid].get(m, 0)
                          for m in range(1, current_month + 1))
        period_2026 = litros_2026_total
        if period_2025 > 0:
            caida_pct = round((1 - period_2026 / period_2025) * 100, 1)
        else:
            caida_pct = 0

        # ── Detectar estacionalidad ──
        # Estacional si: compró en ≤6 meses durante 2025 Y todos esos meses son > current_month
        # (es decir, la temporada todavía no empieza en 2026)
        is_seasonal = False
        season_start_month = None
        if months_count_2025 <= 6 and months_count_2025 > 0:
            # Si TODOS los meses de compra 2025 son posteriores al mes actual de 2026
            # → estacional esperando temporada
            if all(m > current_month for m in months_active_2025):
                is_seasonal = True
                season_start_month = min(months_active_2025)
            # Si el cliente tiene un patrón claramente estacional (ej. solo compra dic-mar)
            # y estamos fuera de temporada (ningún mes activo 2025 coincide con current_month
            # ni con los próximos 2 meses), también es estacional
            elif not any(abs(m - current_month) <= 1 for m in months_active_2025):
                # Patrón estacional pero la temporada está lejos
                is_seasonal = True
                # Encontrar el próximo mes de temporada que viene
                future_months = [m for m in months_active_2025 if m > current_month]
                if future_months:
                    season_start_month = min(future_months)
                else:
                    season_start_month = min(months_active_2025)  # próximo año

        # ── Filtro de inclusión ──
        # Sólo incluir si: no activo en 2026, O caída > 70% vs período comparable
        if pid in active_2026 and caida_pct < 70:
            continue  # Sigue comprando bien, no es recuperación

        note = last_note.get(pid, {})
        lead = last_lead.get(pid, {})
        # Respaldo de notas: chatter → Actividad planeada (mail.activity) → Notas internas (comment)
        _note_body = note.get("body", "")
        _note_author = note.get("author", "")
        _note_date = note.get("date", "")
        if not _note_body:
            _cmt = strip_html(p.get("comment") or "")[:150]
            if _cmt:
                _note_body = _cmt
                _note_author = "Notas internas"
                _note_date = ""

        item = {
            "id": pid,
            "name": pname,
            "zone": safe_name(p.get("delivery_zone_id")) if p.get("delivery_zone_id") else "",
            "is_volume": bool(p.get("is_volume_client", False)),
            "phone": p.get("phone", "") or "",
            "litros_2025": round(litros_2025_total),
            "lpm_2025": round(lpm),
            "litros_2026": round(litros_2026_total),
            "caida_pct": caida_pct,
            "months_active_2025": months_count_2025,
            "months_pattern": [SPANISH_MONTHS[m] for m in months_active_2025],
            "last_note": _note_body,
            "note_date": _note_date,
            "note_author": _note_author,
            "crm_stage": lead.get("stage", ""),
            "crm_exec": lead.get("exec", ""),
            "crm_last": lead.get("last_crm", ""),
        }

        if is_seasonal:
            item["season_start_month"] = season_start_month
            item["season_start_label"] = SPANISH_MONTHS[season_start_month] if season_start_month else ""
            seasonal.append(item)
        else:
            recoverable.append(item)

    # Ordenar por volumen 2025 descendente (siempre prioriza volumen)
    recoverable.sort(key=lambda x: -x["litros_2025"])
    seasonal.sort(key=lambda x: -x["litros_2025"])

    # Top 50 cada uno (decisión Pauline)
    recoverable_top = recoverable[:50]
    seasonal_top = seasonal[:50]

    print(f"  Recoverable: {len(recoverable)} (top 50 in output)")
    print(f"  Seasonal:    {len(seasonal)} (top 50 in output)")

    return {
        "recoverable": recoverable_top,
        "seasonal": seasonal_top,
        "summary": {
            "recoverable_count": len(recoverable),
            "seasonal_count": len(seasonal),
            "recoverable_lpm": round(sum(c["lpm_2025"] for c in recoverable)),
            "seasonal_lpm": round(sum(c["lpm_2025"] for c in seasonal)),
            "total_candidates": len(candidates),
        },
    }


# ==============================================================
# PART 6: CREDIT RISK DASHBOARD
# ==============================================================
def extract_credit_risk(models, uid):
    """
    Credit risk analysis:
    1. Clients with insufficient credit line (projected billing vs monto_credito)
    2. Credit risk score per client (morosidad + volume + margin + cobranza)
    """
    print("\nExtracting Credit Risk data...")
    today = datetime.now().date()
    month_start = today.replace(day=1)

    # ── 3-month window for consumption trend ──
    three_months_ago = month_start
    for _ in range(3):
        three_months_ago = (three_months_ago - timedelta(days=1)).replace(day=1)
    print(f"  Trend window: {fmt(three_months_ago)} → {fmt(today)}")

    # ── 1. Get partners with credit line ──
    credit_partners = sr(models, uid, "res.partner", [
        ["tiene_credito", "=", True],
        ["customer_rank", ">", 0],
    ], ["id", "name", "user_id", "monto_credito", "saldo_credito",
        "property_payment_term_id", "property_product_pricelist",
        "credit", "category_id",
        "delivery_zone_id", "is_volume_client",
        "group_consultek_id"],  # grupo holding para agregar riesgo
    limit=2000)

    print(f"  Partners with credit line: {len(credit_partners)}")
    if not credit_partners:
        return {"linea_insuficiente": [], "score_table": [], "summary": {}}

    partner_map = {p["id"]: p for p in credit_partners}
    partner_ids = list(partner_map.keys())

    # ── Build group map: group_id → list of partner_ids ──
    # Partners in the same group share risk exposure (holding structure)
    group_to_pids = {}  # group_id → [pid, ...]
    pid_to_group = {}   # pid → group_id
    for p in credit_partners:
        gid = p.get("group_consultek_id")
        gid = gid[0] if isinstance(gid, (list, tuple)) and gid else gid
        if gid:
            group_to_pids.setdefault(gid, []).append(p["id"])
            pid_to_group[p["id"]] = gid
    print(f"  Groups found: {len(group_to_pids)} (covering {len(pid_to_group)} partners)")

    # ── 1b. Resolve tag names to detect DICOM ──
    all_tag_ids = set()
    for p in credit_partners:
        for tid in (p.get("category_id") or []):
            all_tag_ids.add(tid)

    dicom_tag_ids = set()
    if all_tag_ids:
        tags = sr(models, uid, "res.partner.category", [
            ["id", "in", list(all_tag_ids)],
        ], ["id", "name"], limit=500)
        for t in tags:
            if "dicom" in (t.get("name") or "").lower():
                dicom_tag_ids.add(t["id"])
        print(f"  DICOM tag IDs: {dicom_tag_ids}")

    # Flag partners with DICOM
    partner_has_dicom = {}
    for p in credit_partners:
        has = bool(set(p.get("category_id") or []) & dicom_tag_ids)
        partner_has_dicom[p["id"]] = has
    dicom_count = sum(1 for v in partner_has_dicom.values() if v)
    print(f"  Partners with DICOM tag: {dicom_count}")

    # ── 2. Get invoices last 3 months for these partners ──
    batch_size = 200
    all_invoices = []
    for i in range(0, len(partner_ids), batch_size):
        batch = partner_ids[i:i+batch_size]
        # Ventana 3 meses + TODAS las facturas impagas (aunque sean más antiguas):
        # una factura impaga de 4+ meses es justamente la mora que el score debe ver.
        invs = sr(models, uid, "account.move", [
            ["move_type", "=", "out_invoice"],
            ["state", "=", "posted"],
            ["partner_id", "in", batch],
            ["invoice_date", "<=", fmt(today)],
            "|",
            ["invoice_date", ">=", fmt(three_months_ago)],
            "&",
            ["payment_state", "in", ["not_paid", "partial"]],
            ["amount_residual", ">", 0],
        ], ["id", "partner_id", "invoice_user_id", "amount_untaxed",
            "amount_residual", "payment_state", "invoice_date",
            "invoice_date_due", "margin_zone",
            "average_payment_days", "price_rango",
            "cobranza", "excepcion_line"],
        limit=10000)
        all_invoices.extend(invs)

    print(f"  Invoices (3 months): {len(all_invoices)}")

    # ── 3. Get diesel litros from invoice lines ──
    inv_ids = [inv["id"] for inv in all_invoices]
    inv_pid_map = {inv["id"]: safe_id(inv.get("partner_id")) for inv in all_invoices}
    litros_by_inv = {}
    if inv_ids:
        for i in range(0, len(inv_ids), 500):
            chunk = inv_ids[i:i+500]
            lines = sr(models, uid, "account.move.line", [
                ["move_id", "in", chunk],
                ["product_id", "=", DIESEL_PRODUCT_ID],
            ], ["move_id", "quantity", "price_subtotal"], limit=10000)
            for ln in lines:
                mid = safe_id(ln.get("move_id"))
                if mid:
                    prev = litros_by_inv.get(mid, {"litros": 0, "venta": 0})
                    prev["litros"] += ln.get("quantity", 0) or 0
                    prev["venta"] += ln.get("price_subtotal", 0) or 0
                    litros_by_inv[mid] = prev

    # ── 4. Aggregate per partner ──
    partner_stats = {}  # pid → stats
    for inv in all_invoices:
        pid = safe_id(inv.get("partner_id"))
        if not pid or pid not in partner_map:
            continue

        if pid not in partner_stats:
            partner_stats[pid] = {
                "litros_total": 0,
                "venta_total": 0,
                "invoice_count": 0,
                "avg_payment_days_sum": 0,
                "avg_payment_days_count": 0,
                "overdue_count": 0,
                "overdue_amount": 0,
                "siniestro_count": 0,
                "excepcion_count": 0,
                "margin_sum": 0,
                "margin_count": 0,
                "price_rango_sum": 0,
                "price_rango_count": 0,
                "cobranza_labels": [],
                "vendedor": safe_name(inv.get("invoice_user_id")),  # fallback, overridden by partner.user_id
            }

        s = partner_stats[pid]
        inv_date_str = (inv.get("invoice_date") or "")[:10]
        in_window = (inv_date_str >= fmt(three_months_ago)) if inv_date_str else True
        residual_apd = inv.get("amount_residual", 0) or 0

        # Litros/venta/count solo dentro de la ventana 3m (base de L/mes y proyección)
        inv_data = litros_by_inv.get(inv["id"], {"litros": 0, "venta": 0})
        if in_window:
            s["litros_total"] += inv_data["litros"]
            s["venta_total"] += inv_data["venta"]
            s["invoice_count"] += 1

        # Average payment days — días REALES:
        # Impaga (residual > 0): max(hoy − invoice_date, apd) — una impaga fresca
        # no puede reportar MENOS días que el comportamiento histórico del cliente.
        # Pagada: average_payment_days de Odoo (solo existe si está reconciliada).
        apd = inv.get("average_payment_days")
        if residual_apd > 0 and inv_date_str:
            try:
                days_open = (today - datetime.strptime(inv_date_str, "%Y-%m-%d").date()).days
                eff_days = max(days_open, apd or 0)
                if eff_days > 0:
                    s["avg_payment_days_sum"] += eff_days
                    s["avg_payment_days_count"] += 1
            except (ValueError, TypeError):
                pass
        elif apd and apd > 0:
            s["avg_payment_days_sum"] += apd
            s["avg_payment_days_count"] += 1

        # Overdue detection
        due_date = (inv.get("invoice_date_due") or "")[:10]
        residual = inv.get("amount_residual", 0) or 0
        if due_date and residual > 0:
            try:
                due_dt = datetime.strptime(due_date, "%Y-%m-%d").date()
                if today > due_dt:
                    s["overdue_count"] += 1
                    s["overdue_amount"] += residual
            except (ValueError, TypeError):
                pass

        # Margin y price_rango — solo ventana 3m (reflejan pricing reciente)
        if in_window:
            margin = inv.get("margin_zone", 0) or 0
            if margin:
                s["margin_sum"] += margin
                s["margin_count"] += 1

            pr = inv.get("price_rango", 0) or 0
            if pr:
                s["price_rango_sum"] += pr
                s["price_rango_count"] += 1

        # Cobranza (collection status)
        cobranza = safe_name(inv.get("cobranza"))
        if cobranza and cobranza != "False" and cobranza != "Sin asignar":
            s["cobranza_labels"].append(cobranza)
            if "siniestro" in cobranza.lower():
                s["siniestro_count"] += 1

        # Excepcion de linea
        exc = safe_name(inv.get("excepcion_line"))
        if exc and exc != "False" and exc != "Sin asignar":
            s["excepcion_count"] += 1

    # ── 5. Build risk table ──
    # Number of months in the window for averaging
    months_in_window = max(1, (today - three_months_ago).days / 30)

    linea_insuficiente = []
    score_table = []
    total_ar = sum(p.get("credit", 0) or 0 for p in credit_partners)

    for pid, stats in partner_stats.items():
        p = partner_map[pid]

        # ── Consolidación a nivel de grupo (group_consultek_id) ──
        # Si el partner pertenece a un grupo, agregar stats de todos los miembros del grupo
        # para reflejar la exposición real del holding. El score se muestra en el partner
        # con mayor crédito del grupo.
        gid = pid_to_group.get(pid)
        group_peers = [partner_map[gp] for gp in group_to_pids.get(gid, []) if gp != pid and gp in partner_map] if gid else []

        # Consolidated stats = own stats + all peers' stats
        cons_stats = dict(stats)  # copy
        for gp_id in group_to_pids.get(gid, []):
            if gp_id != pid and gp_id in partner_stats:
                gs = partner_stats[gp_id]
                for k in ["litros_total", "venta_total", "invoice_count",
                          "avg_payment_days_sum", "avg_payment_days_count",
                          "overdue_count", "overdue_amount", "siniestro_count",
                          "margin_sum", "margin_count"]:
                    cons_stats[k] = cons_stats.get(k, 0) + gs.get(k, 0)
                cons_stats["cobranza_labels"] = cons_stats.get("cobranza_labels", []) + gs.get("cobranza_labels", [])

        # Consolidated credit line = sum of all group members
        monto = sum((partner_map[gp].get("monto_credito", 0) or 0) for gp in group_to_pids.get(gid, [pid])) if gid else (p.get("monto_credito", 0) or 0)
        saldo = sum((partner_map[gp].get("saldo_credito", 0) or 0) for gp in group_to_pids.get(gid, [pid])) if gid else (p.get("saldo_credito", 0) or 0)
        ar_balance = sum((partner_map[gp].get("credit", 0) or 0) for gp in group_to_pids.get(gid, [pid])) if gid else (p.get("credit", 0) or 0)

        # is_volume: True si cualquier miembro del grupo es volumen
        is_volume_group = any(partner_map[gp].get("is_volume_client") for gp in group_to_pids.get(gid, [pid])) if gid else bool(p.get("is_volume_client"))

        group_label = f" (+{len(group_peers)} del grupo)" if group_peers else ""

        payment_term = safe_name(p.get("property_payment_term_id"))
        pricelist = safe_name(p.get("property_product_pricelist"))
        vendedor_partner = canonical_vendedor(safe_name(p.get("user_id")))  # vendedor asignado al cliente
        name = p.get("name", "?") + (f" [Grupo: {len(group_peers)+1}]" if group_peers else "")

        # ── Zona y tipo de venta (para calculadora de excepción) ──
        zona = safe_name(p.get("delivery_zone_id")) or "Sin zona"
        is_volume = is_volume_group  # usar flag consolidada del grupo

        # Parse payment term days (e.g., "30 Days", "Plazo 30 días")
        pt_days = 30  # default
        if payment_term:
            pt_match = re.search(r'(\d+)', payment_term)
            if pt_match:
                pt_days = int(pt_match.group(1))

        # Tipo de venta (para calculadora excepción): Volumen si is_volume,
        # Contado si plazo ≤1d o término menciona "prepago", Crédito en otro caso
        if is_volume:
            tipo_venta = "Volumen"
        elif pt_days <= 1 or (payment_term and "prepago" in payment_term.lower()):
            tipo_venta = "Contado"
        else:
            tipo_venta = "Crédito"

        # Averages — usando cons_stats (grupo consolidado si aplica)
        avg_monthly_litros = round(cons_stats["litros_total"] / months_in_window)
        avg_monthly_venta = round(cons_stats["venta_total"] / months_in_window)
        # Días de pago: MÁXIMO entre los promedios individuales del grupo, NO el
        # promedio consolidado — el peor pagador del holding define el riesgo.
        # (Promediar diluye: Palquibudis 40d + peers a 12d mostraba 23d).
        _member_ids = group_to_pids.get(gid, [pid]) if gid else [pid]
        _member_avgs = []
        for _mid in _member_ids:
            _ms = partner_stats.get(_mid)
            if _ms and _ms["avg_payment_days_count"] > 0:
                _member_avgs.append(_ms["avg_payment_days_sum"] / _ms["avg_payment_days_count"])
        avg_payment_days = round(max(_member_avgs), 1) if _member_avgs else 0.0
        avg_margin = round((cons_stats["margin_sum"] / max(cons_stats["margin_count"], 1)) * 100, 1)
        avg_price_rango = round(cons_stats["price_rango_sum"] / max(cons_stats["price_rango_count"], 1), 1)

        # ── Utilization % ──
        utilizacion = round(((monto - saldo) / max(monto, 1)) * 100, 1) if monto > 0 else 0

        # ── Projected monthly billing vs credit line ──
        avg_price_per_liter = cons_stats["venta_total"] / max(cons_stats["litros_total"], 1) if cons_stats["litros_total"] > 0 else 0
        projected_monthly = round(avg_monthly_litros * avg_price_per_liter)
        linea_ratio = round((projected_monthly / max(monto, 1)) * 100, 1) if monto > 0 else 999

        # ── RISK SCORE (0-100, higher = worse) ──
        # Component 1: Morosidad (0-40 pts)
        mora_ratio = avg_payment_days / max(pt_days, 1)
        score_mora = min(40, round(mora_ratio * 20))  # ratio 2.0 → 40pts

        # Component 2: Utilización crédito (0-25 pts)
        score_util = min(25, round(utilizacion / 4))  # 100% util → 25pts

        # Component 3: Cobranza / Siniestro (0-20 pts)
        score_cobranza = 0
        if cons_stats["siniestro_count"] > 0:
            score_cobranza = 20  # Siniestro = max risk
        elif cons_stats["overdue_count"] > 0:
            overdue_ratio = cons_stats["overdue_count"] / max(cons_stats["invoice_count"], 1)
            score_cobranza = min(15, round(overdue_ratio * 15))

        # Component 4: Margen bajo (0-15 pts) — lower margin = more risk
        score_margin = 0
        if avg_margin > 0:
            if avg_margin < 4:
                score_margin = 15
            elif avg_margin < 6:
                score_margin = 10
            elif avg_margin < 8:
                score_margin = 5

        # Component 5: DICOM (0-20 pts) — immediate high risk flag
        has_dicom = partner_has_dicom.get(pid, False)
        score_dicom = 20 if has_dicom else 0

        risk_score = score_mora + score_util + score_cobranza + score_margin + score_dicom

        # Risk level label
        if risk_score >= 60:
            risk_level = "Crítico"
        elif risk_score >= 40:
            risk_level = "Alto"
        elif risk_score >= 20:
            risk_level = "Medio"
        else:
            risk_level = "Bajo"

        entry = {
            "_pid": pid,
            "name": name,
            "vendedor": vendedor_partner or cons_stats["vendedor"],  # partner.user_id > invoice_user_id
            "monto_credito": monto,
            "saldo_credito": saldo,
            "utilizacion_pct": utilizacion,
            "avg_monthly_litros": avg_monthly_litros,
            "avg_monthly_venta": avg_monthly_venta,
            "projected_monthly": projected_monthly,
            "linea_ratio": linea_ratio,
            "avg_payment_days": avg_payment_days,
            "plazo_pago_dias": pt_days,
            "plazo_pago_label": payment_term or "—",
            "mora_ratio": round(mora_ratio, 2),
            "overdue_count": cons_stats["overdue_count"],
            "overdue_amount": round(cons_stats["overdue_amount"]),
            "siniestro_count": cons_stats["siniestro_count"],
            "excepcion_count": cons_stats["excepcion_count"],
            "has_dicom": has_dicom,
            "avg_margin_pct": avg_margin,
            "avg_price_rango": avg_price_rango,
            "pricelist": pricelist or "—",
            "zona": zona,
            "tipo_venta": tipo_venta,
            "ar_balance": round(ar_balance),
            "risk_score": risk_score,
            "risk_level": risk_level,
            "score_detail": {
                "morosidad": score_mora,
                "utilizacion": score_util,
                "cobranza": score_cobranza,
                "margen": score_margin,
                "dicom": score_dicom,
            },
        }

        score_table.append(entry)

        # Flag insufficient credit line (projected > 80% of line, or already > 80% utilized)
        if linea_ratio > 80 or utilizacion > 80:
            linea_insuficiente.append(entry)

    # Sort
    score_table.sort(key=lambda x: -x["risk_score"])
    linea_insuficiente.sort(key=lambda x: -x["linea_ratio"])

    # Summary stats
    criticos = sum(1 for s in score_table if s["risk_level"] == "Crítico")
    altos = sum(1 for s in score_table if s["risk_level"] == "Alto")
    medios = sum(1 for s in score_table if s["risk_level"] == "Medio")
    bajos = sum(1 for s in score_table if s["risk_level"] == "Bajo")
    total_siniestros = sum(s["siniestro_count"] for s in score_table)
    total_overdue = sum(s["overdue_amount"] for s in score_table)
    avg_util = round(sum(s["utilizacion_pct"] for s in score_table) / max(len(score_table), 1), 1)
    total_linea_insuf = len(linea_insuficiente)

    print(f"  Score: {criticos} Críticos, {altos} Altos, {medios} Medios, {bajos} Bajos")
    print(f"  Líneas insuficientes: {total_linea_insuf}")
    print(f"  Siniestros: {total_siniestros} | Monto vencido: ${total_overdue:,.0f}")

    # ── Last vendor note for credit risk clients (same pattern as dormants) ──
    _noise_cr = ["lead enrichment", "nuevo lead para el equipo", "new lead for", "stage changed",
                 "enrichment could", "no company data", "meeting scheduled"]
    cr_pids = [e["_pid"] for e in score_table if e.get("_pid")]
    cr_note_by_pid = {}
    if cr_pids:
        try:
            for i in range(0, len(cr_pids), batch_size):
                batch = cr_pids[i:i+batch_size]
                msgs = sr(models, uid, "mail.message", [
                    ["model", "=", "res.partner"],
                    ["res_id", "in", batch],
                    ["message_type", "in", ["comment", "email"]],
                ], ["res_id", "body", "date"], limit=500, order="date desc")
                for m in msgs:
                    pid_m = m.get("res_id")
                    if pid_m and pid_m not in cr_note_by_pid:
                        body = strip_html(m.get("body") or "")
                        if len(body) > 3 and not any(n in body.lower()[:60] for n in _noise_cr):
                            cr_note_by_pid[pid_m] = body[:150]
            print(f"  Credit risk notes found: {len(cr_note_by_pid)}/{len(cr_pids)}")
        except Exception as e:
            print(f"  Credit risk notes skipped: {e}")

    for e in score_table:
        e["last_note"] = cr_note_by_pid.get(e.get("_pid"), "")
        e.pop("_pid", None)
    for e in linea_insuficiente:
        e.pop("_pid", None)

    return {
        "linea_insuficiente": linea_insuficiente[:50],
        "score_table": score_table[:100],
        "summary": {
            "total_evaluados": len(score_table),
            "criticos": criticos,
            "altos": altos,
            "medios": medios,
            "bajos": bajos,
            "linea_insuficiente_count": total_linea_insuf,
            "total_siniestros": total_siniestros,
            "total_overdue_amount": round(total_overdue),
            "avg_utilizacion": avg_util,
        },
    }


# ==============================================================
# PART 7: SLA DE ENTREGA
# ==============================================================
def extract_sla_data(models, uid, m_start, m_end):
    """
    SLA de entrega: shipping_date en sale.order vs primera factura (account.move).
    Factura buscada por invoice_origin = order.name.
    SLA incumplido si fecha_primera_factura - shipping_date > 1 día.
    """
    print(f"\nExtracting SLA entrega ({fmt(m_start)} → {fmt(m_end)})...")

    orders = sr(models, uid, "sale.order", [
        ["state", "=", "sale"],
        ["date_order", ">=", fdt_s(m_start)],
        ["date_order", "<=", fdt_e(m_end)],
        ["shipping_date", "!=", False],
    ], ["name", "partner_id", "shipping_date"], limit=2000)

    print(f"  Órdenes con shipping_date: {len(orders)}")

    cumplidos = 0
    incumplidos = 0
    dias_list = []
    detalle_incumplidos = []

    # Traer todas las facturas del mes en chunks por invoice_origin para evitar N queries
    order_names = [o.get("name", "") for o in orders if o.get("name")]
    invoices_all = []
    if order_names:
        for i in range(0, len(order_names), 200):
            chunk = order_names[i:i+200]
            batch = sr(models, uid, "account.move", [
                ["invoice_origin", "in", chunk],
                ["move_type", "=", "out_invoice"],
                ["state", "=", "posted"],
                ["invoice_date", ">=", fmt(m_start)],
                ["invoice_date", "<=", fmt(m_end)],
            ], ["invoice_origin", "invoice_date"], limit=5000)
            invoices_all.extend(batch)

    # Agrupar por invoice_origin → lista de fechas, luego tomar la mínima
    inv_by_origin = defaultdict(list)
    for inv in invoices_all:
        origin = inv.get("invoice_origin", "")
        date_str = (inv.get("invoice_date") or "")[:10]
        if origin and date_str:
            inv_by_origin[origin].append(date_str)

    for o in orders:
        try:
            ship_str = (o.get("shipping_date") or "")[:10]
            if not ship_str:
                continue
            ship_dt = datetime.strptime(ship_str, "%Y-%m-%d").date()

            order_name = o.get("name", "")
            dates = sorted(inv_by_origin.get(order_name, []))
            if not dates:
                continue

            first_inv_date_str = dates[0]
            first_inv_dt = datetime.strptime(first_inv_date_str, "%Y-%m-%d").date()
            diff = (first_inv_dt - ship_dt).days

            dias_list.append(max(diff, 0))

            if diff <= 1:
                cumplidos += 1
            else:
                incumplidos += 1
                detalle_incumplidos.append({
                    "cliente": safe_name(o.get("partner_id")),
                    "orden": order_name,
                    "fecha_entrega": ship_str,
                    "fecha_factura": first_inv_date_str,
                    "dias_diff": diff,
                })
        except Exception:
            continue

    total = cumplidos + incumplidos
    cumplimiento_pct = round((cumplidos / total) * 100) if total > 0 else 0
    dias_promedio = round(sum(dias_list) / len(dias_list), 1) if dias_list else 0
    detalle_incumplidos.sort(key=lambda x: -x["dias_diff"])

    print(f"  Total evaluadas: {total} | Cumplidas: {cumplidos} | Incumplidas: {incumplidos} | SLA: {cumplimiento_pct}%")

    return {
        "cumplidos": cumplidos,
        "incumplidos": incumplidos,
        "cumplimiento_pct": cumplimiento_pct,
        "dias_promedio": dias_promedio,
        "total_evaluadas": total,
        "detalle": detalle_incumplidos[:50],
    }


# ==============================================================
# PART 8: OPERACIONES — SLA mes actual + litros entregados por camión/día
# ==============================================================
def extract_operaciones(models, uid):
    """Camión = stock.warehouse de la sale.order (HHPT-71, PHXC-44, ...).
    Entrega = factura posted, unida a la orden vía invoice_origin (mismo join del SLA)."""
    today = datetime.now().date()
    m_start = today.replace(day=1)
    sla_actual = extract_sla_data(models, uid, m_start, today)

    # SLA por semana comercial ENAP (mismas 4 semanas del selector del dashboard)
    # + LEAD TIME REAL: create_date del pedido → primera factura (fechas INMUTABLES,
    #   a diferencia de shipping_date que se fija cuando el despacho ya está resuelto).
    sla_semanas = []
    for offset in range(4):
        wk = get_enap_week(offset)
        ws_d = datetime.strptime(wk["start"], "%Y-%m-%d").date()
        we_d = datetime.strptime(wk["end"], "%Y-%m-%d").date()
        s = extract_sla_data(models, uid, ws_d, we_d)
        s["label"] = wk["label"]
        s["week_start"] = wk["start"]
        s["week_end"] = wk["end"]

        # PROMESA COMERCIAL: "entregas el mismo día para el 95% de los pedidos
        # informados antes de las 11 AM". Universo = pedidos humanos creados en la
        # semana antes de las 11:00 hora Chile; cumplido = 1ª factura el MISMO día.
        wk_orders = sr(models, uid, "sale.order", [
            ["state", "in", ["sale", "done"]],
            ["create_date", ">=", fdt_s(ws_d)],
            ["create_date", "<=", fdt_e(we_d)],
            ["create_uid", "!=", 1],
        ], ["name", "partner_id", "create_date"], limit=2000)
        _names = [o["name"] for o in wk_orders]
        _inv_by = defaultdict(list)
        for i in range(0, len(_names), 200):
            for v in sr(models, uid, "account.move", [
                ["invoice_origin", "in", _names[i:i+200]],
                ["move_type", "=", "out_invoice"], ["state", "=", "posted"],
            ], ["invoice_origin", "invoice_date"], limit=5000):
                _inv_by[v["invoice_origin"]].append(v["invoice_date"])

        # Odoo guarda create_date en UTC. Chile: UTC-4 en invierno (abr-sep), UTC-3 en verano.
        _cl_offset = 4 if 4 <= today.month <= 8 else 3
        prom_total = 0; prom_ok = 0; post11 = 0; en_curso = 0
        prom_incumplidas = []
        for o in wk_orders:
            created_cl = datetime.strptime(o["create_date"][:19], "%Y-%m-%d %H:%M:%S") - timedelta(hours=_cl_offset)
            pedido_dia = created_cl.date()
            ds = sorted(_inv_by.get(o["name"], []))
            if created_cl.hour >= 11:
                post11 += 1
                continue
            if not ds:
                if pedido_dia >= today:
                    en_curso += 1  # pedido de hoy aún en reparto: no evaluar todavía
                    continue
                prom_total += 1
                prom_incumplidas.append({"orden": o["name"], "cliente": safe_name(o.get("partner_id")),
                                         "pedido": created_cl.strftime("%Y-%m-%d %H:%M"),
                                         "facturado": "—", "dias": (today - pedido_dia).days})
                continue
            prom_total += 1
            first_inv = datetime.strptime(ds[0][:10], "%Y-%m-%d").date()
            if first_inv <= pedido_dia:
                prom_ok += 1
            else:
                prom_incumplidas.append({"orden": o["name"], "cliente": safe_name(o.get("partner_id")),
                                         "pedido": created_cl.strftime("%Y-%m-%d %H:%M"),
                                         "facturado": ds[0][:10], "dias": (first_inv - pedido_dia).days})
        prom_incumplidas.sort(key=lambda x: -x["dias"])
        s["prom_total"] = prom_total
        s["prom_ok"] = prom_ok
        s["prom_pct"] = round(prom_ok / prom_total * 100) if prom_total else 0
        s["prom_post11"] = post11
        s["prom_en_curso"] = en_curso
        s["prom_incumplidas"] = prom_incumplidas[:30]
        s["pedidos_semana"] = len(wk_orders)
        print(f"  Promesa {wk['label']}: {prom_ok}/{prom_total} mismo día ({s['prom_pct']}%) | pre-11am incumplidas: {len(prom_incumplidas)} | post-11am: {post11}")
        sla_semanas.append(s)

    d_start = today - timedelta(days=29)
    print(f"\nExtracting litros por camión ({fmt(d_start)} → {fmt(today)})...")
    invs = sr(models, uid, "account.move", [
        ["move_type", "=", "out_invoice"], ["state", "=", "posted"],
        ["invoice_date", ">=", fmt(d_start)], ["invoice_date", "<=", fmt(today)],
    ], ["id", "invoice_origin", "invoice_date", "partner_id"], limit=10000)

    # Zona de entrega por partner (para matriz litros/día por zona)
    _pids = sorted({safe_id(i.get("partner_id")) for i in invs if i.get("partner_id")})
    _pzone = {}
    for i in range(0, len(_pids), 400):
        for p in sr(models, uid, "res.partner", [["id", "in", _pids[i:i+400]]], ["delivery_zone_id"], limit=500):
            _pzone[p["id"]] = safe_name(p.get("delivery_zone_id"))

    # invoice_origin puede ser "S123" o "S123, S124": usar el primer nombre
    def _first_origin(o):
        return (o or "").split(",")[0].strip()

    origins = sorted({_first_origin(i.get("invoice_origin")) for i in invs if i.get("invoice_origin")})
    so_wh = {}
    for i in range(0, len(origins), 200):
        for s in sr(models, uid, "sale.order", [["name", "in", origins[i:i+200]]], ["name", "warehouse_id"], limit=500):
            so_wh[s["name"]] = safe_name(s.get("warehouse_id"))

    inv_ids = [i["id"] for i in invs]
    qty_by_inv = {}
    for i in range(0, len(inv_ids), 500):
        for ln in sr(models, uid, "account.move.line", [
            ["move_id", "in", inv_ids[i:i+500]], ["product_id", "=", DIESEL_PRODUCT_ID],
        ], ["move_id", "quantity"], limit=20000):
            mid = safe_id(ln.get("move_id"))
            qty_by_inv[mid] = qty_by_inv.get(mid, 0) + (ln.get("quantity") or 0)

    daily = defaultdict(float)   # (fecha, camion) -> litros
    daily_z = defaultdict(float)  # (fecha, zona) -> litros
    for inv in invs:
        q = qty_by_inv.get(inv["id"], 0)
        if q <= 0:
            continue
        f = inv["invoice_date"][:10]
        cam = so_wh.get(_first_origin(inv.get("invoice_origin"))) or "Sin camión"
        daily[(f, cam)] += q
        zona = _pzone.get(safe_id(inv.get("partner_id"))) or "Sin zona"
        daily_z[(f, zona)] += q

    rows = [{"fecha": f, "camion": c, "litros": round(l)} for (f, c), l in daily.items()]
    rows.sort(key=lambda r: (r["fecha"], r["camion"]))
    camiones = sorted({r["camion"] for r in rows})
    rows_z = [{"fecha": f, "zona": z, "litros": round(l)} for (f, z), l in daily_z.items()]
    rows_z.sort(key=lambda r: (r["fecha"], r["zona"]))
    zonas = sorted({r["zona"] for r in rows_z})
    print(f"  Entregas 30d: {len(rows)} filas día×camión | camiones: {camiones} | zonas: {zonas}")
    return {
        "sla": sla_actual,
        "sla_semanas": sla_semanas,
        "camiones_diario": rows,
        "camiones": camiones,
        "zonas_diario": rows_z,
        "zonas": zonas,
        "rango": {"desde": fmt(d_start), "hasta": fmt(today)},
    }


# ==============================================================
# MAIN
# ==============================================================
def main():
    print("=== CRM + Funnel + Ventas + SLA · Odoo Extraction ===")
    models, uid = connect()

    # Part 1: Original CRM
    crm = extract_crm_data(models, uid)

    # Part 2: Funnel comercial
    funnel_weeks = extract_funnel_data(models, uid)

    # Part 3: Sales KPIs (current month)
    ventas = extract_sales_data(models, uid)

    # Part 3b: Sales KPIs (previous month = cierre mes vencido)
    today = datetime.now().date()
    prev_m_end = today.replace(day=1) - timedelta(days=1)
    prev_m_start = prev_m_end.replace(day=1)
    ventas_prev = extract_sales_data(models, uid, prev_m_start, prev_m_end, prev_m_start.strftime("%B %Y"))

    # Part 3c: Historial mensual (resumen tab Mes Vencido) — reusa extract_sales_data
    # por mes para que litros/venta/márgenes cuadren con el titular (no suma semanas).
    BUDGET_2026 = {1: 1065753, 2: 1090372, 3: 1135242, 4: 1305689, 5: 1035293, 6: 866750,
                   7: 1107706, 8: 1084934, 9: 1052901, 10: 1354911, 11: 1394823, 12: 1754688}
    BUDGET_2025 = {1: 637139, 2: 645973, 3: 673983, 4: 846275, 5: 600862, 6: 451928,
                   7: 716270, 8: 730703, 9: 736331, 10: 857146, 11: 829032, 12: 850191}
    MESES_ES = {1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril", 5: "Mayo", 6: "Junio",
                7: "Julio", 8: "Agosto", 9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"}

    def _meta_mes(dt):
        return (BUDGET_2026 if dt.year >= 2026 else BUDGET_2025).get(dt.month, 0)

    def _month_row(sales, dt, parcial=False):
        t = sales.get("totals", {})
        meta = _meta_mes(dt)
        return {
            "label": f"{MESES_ES.get(dt.month, '')} {dt.year}",
            "parcial": parcial,
            "litros": t.get("total_litros", 0),
            "meta": meta,
            "ppto": round(meta / 1.12) if meta else 0,
            "retail_litros": t.get("retail_litros", 0),
            "volume_litros": t.get("volume_litros", 0),
            "margin_retail_pct": t.get("margin_retail_pct", 0),
            "margin_volume_pct": t.get("margin_volume_pct", 0),
            "client_count": t.get("client_count", 0),
        }

    N_MONTHS_HISTORY = 6
    monthly_history = [
        _month_row(ventas, today.replace(day=1), parcial=True),
        _month_row(ventas_prev, prev_m_start),
    ]
    _cur = prev_m_start
    for _ in range(N_MONTHS_HISTORY - 2):
        _cur = (_cur - timedelta(days=1)).replace(day=1)
        _last = (_cur.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
        _vh = extract_sales_data(models, uid, _cur, _last, _cur.strftime("%B %Y"))
        monthly_history.append(_month_row(_vh, _cur))
    print(f"  Historial mensual: {len(monthly_history)} meses")

    # Part 4: Churn & Rescue
    churn = extract_churn_data(models, uid)

    # Part 5: Rescued clients (frecuencia-based)
    rescued = extract_rescued_clients(models, uid)

    # Part 5b: Recovery (Quick Wins) — clientes 2025 que cayeron en 2026
    recovery = extract_recovery_clients(models, uid)

    # Part 6: Credit Risk
    credit_risk = extract_credit_risk(models, uid)

    # Part 6b: SLA entrega mes anterior
    sla_prev = extract_sla_data(models, uid, prev_m_start, prev_m_end)
    ventas_prev["sla"] = sla_prev

    # Part 6c: Operaciones — SLA mes actual + litros entregados por camión/día
    operaciones = extract_operaciones(models, uid)

    # Part 7: Pauline Comber "mantención" — absorbs the TomEnergy bucket into Comber's row.
    # Liters = TomEnergy liters + Comber liters (simple sum, no subtraction).
    # Venta neta y margen se suman/pondera. La fila "TomEnergy" se elimina del output.

    def _find_key(d, needle):
        """Find a key in dict by case-insensitive partial match on needle."""
        needle_lower = needle.lower().replace(" ", "")
        for k in d:
            if needle_lower in k.lower().replace(" ", ""):
                return k
        return None

    def _get_val(d, key):
        """Safely get a numeric value from dict, returns 0 if key is None or value is falsy."""
        if key is None:
            return 0
        val = d.get(key, 0)
        return val if val else 0

    def _patch_comber(vts):
        tot = vts.get("totals", {})
        lbu = tot.get("litros_by_user", {}) or {}
        vbu = tot.get("venta_by_user", {}) or {}
        mbu = tot.get("margin_by_user", {}) or {}
        nc = vts.get("new_clients", {}) or {}
        nc_lbu = nc.get("litros_by_user", {}) or {}
        nc_bu = nc.get("by_user", {}) or {}

        # Dynamic key search for BOTH TomEnergy and Comber (handles Odoo name variations)
        tom_key = _find_key(lbu, "tomenergy")
        com_key = _find_key(lbu, "comber")

        print(f"  [DEBUG] litros_by_user keys: {list(lbu.keys())}")
        print(f"  [DEBUG] TomEnergy key found: {tom_key!r}  value: {lbu.get(tom_key, 'N/A')}")
        print(f"  [DEBUG] Comber key found: {com_key!r}  value: {lbu.get(com_key, 'N/A')}")

        tom_l = _get_val(lbu, tom_key)
        com_l = _get_val(lbu, com_key)
        mantencion_l = tom_l + com_l

        tom_key_v = _find_key(vbu, "tomenergy")
        com_key_v = _find_key(vbu, "comber")
        tom_v = _get_val(vbu, tom_key_v)
        com_v = _get_val(vbu, com_key_v)
        merged_v = tom_v + com_v

        tom_key_m = _find_key(mbu, "tomenergy")
        com_key_m = _find_key(mbu, "comber")
        tom_m = _get_val(mbu, tom_key_m)
        com_m = _get_val(mbu, com_key_m)
        merged_m = round((tom_m * tom_v + com_m * com_v) / merged_v, 1) if merged_v > 0 else 0

        # Use the actual Comber key found in the dict (or fallback to a canonical name)
        final_key = com_key or "Comber Sigall Pauline"

        # Merge into Comber
        lbu[final_key] = mantencion_l
        vbu[final_key] = round(merged_v)
        mbu[final_key] = merged_m

        # Drop TomEnergy row (whatever its exact key name) — but only if different from Comber's key
        if tom_key and tom_key != final_key:
            lbu.pop(tom_key, None)
        if tom_key_v and tom_key_v != (com_key_v or final_key):
            vbu.pop(tom_key_v, None)
        if tom_key_m and tom_key_m != (com_key_m or final_key):
            mbu.pop(tom_key_m, None)

        # Remove TomEnergy from new-clients tallies too
        tom_key_nc = _find_key(nc_bu, "tomenergy")
        tom_key_ncl = _find_key(nc_lbu, "tomenergy")
        if tom_key_nc: nc_bu.pop(tom_key_nc, None)
        if tom_key_ncl: nc_lbu.pop(tom_key_ncl, None)

        tot["litros_by_user"] = lbu
        tot["venta_by_user"] = vbu
        tot["margin_by_user"] = mbu
        nc["litros_by_user"] = nc_lbu
        nc["by_user"] = nc_bu
        vts["new_clients"] = nc
        tot["comber_mantencion_detail"] = {
            "tomenergy_litros": tom_l,
            "comber_own_litros": com_l,
            "total": mantencion_l,
            "merged_venta_neta": round(merged_v),
            "merged_margin_pct": merged_m,
        }
        print(f"  Comber mantención: {mantencion_l} L  =  TomEnergy({tom_key}) {tom_l} + Comber({com_key}) {com_l}  · Margen: {merged_m}%")

    _patch_comber(ventas)
    _patch_comber(ventas_prev)

    # Load or create vendor goals
    goals_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "vendor-goals.json")
    vendor_goals = {}
    if os.path.exists(goals_path):
        try:
            with open(goals_path, "r", encoding="utf-8") as f:
                vendor_goals = json.load(f)
            print(f"\nvendor-goals.json loaded ({len(vendor_goals)} vendors)")
        except (json.JSONDecodeError, ValueError) as e:
            print(f"\n⚠️  vendor-goals.json MALFORMADO ({e}) — se ignora, se usan metas por defecto. ARREGLA EL JSON.")
            vendor_goals = {}
    else:
        users = set()
        for w in funnel_weeks:
            for stg in w["stages"].values():
                if isinstance(stg, dict) and "by_user" in stg:
                    users.update(stg["by_user"].keys())
        for u in ventas.get("totals", {}).get("litros_by_user", {}):
            users.add(u)
        users.discard("Sin asignar")
        for u in sorted(users):
            vendor_goals[u] = {
                "litros_mes": 0, "presupuesto_venta": 0, "margen_min": 0,
                "leads_semana": 15, "cotizaciones_semana": 8, "contactos_semana": 10,
                "clientes_nuevos_mes": 2, "retencion_90d": 90,
            }
        with open(goals_path, "w", encoding="utf-8") as f:
            json.dump(vendor_goals, f, ensure_ascii=False, indent=2)
        print(f"\nvendor-goals.json CREATED with template — edit targets manually!")

    # Patch April 2026 targets
    if "Toro González Sebastian Enrique" in vendor_goals:
        vendor_goals["Toro González Sebastian Enrique"]["litros_mes"] = 200000

    # ── Graduating: clients whose FIRST invoice was 150-210 days ago, assigned to ejecutivo/freelancer ──
    # Regla jul-2026: el cliente queda con el ejecutivo externo 180 días (6 meses, alineado a comisiones), antes 90.
    graduating = []
    try:
        # Search directly for first invoices in the 150-210 day window
        window_start = today - timedelta(days=210)
        window_end = today - timedelta(days=150)
        _exec_names = [
            "toro gonzález sebastian enrique", "muñoz encalada joaquin",
            "sebastian toro", "joaquin muñoz",  # cuentas .ext (nombre corto, no canonicaliza al largo)
            "carolina avilés", "marcela márquez", "raúl bisquertt", "rodrigo retamal",
            "manuel lópez", "nicolás gonzalez", "cristian jiroz", "diego varas",
            "abraham urrutia",
        ]

        # Get all invoices in the window (candidates for first invoice)
        window_invs = sr(models, uid, "account.move", [
            ["move_type", "=", "out_invoice"],
            ["state", "=", "posted"],
            ["invoice_date", ">=", fmt(window_start)],
            ["invoice_date", "<=", fmt(window_end)],
        ], ["partner_id", "invoice_date"], limit=3000)

        # Unique partner IDs in window
        window_pids = set()
        for inv in window_invs:
            pid = safe_id(inv.get("partner_id"))
            if pid:
                window_pids.add(pid)

        print(f"  Graduating window ({fmt(window_start)} to {fmt(window_end)}): {len(window_pids)} partners with invoices")

        grad_candidates = []
        pid_list = list(window_pids)
        for i in range(0, len(pid_list), 200):
            batch = pid_list[i:i+200]
            for pid in batch:
                # Find FIRST ever invoice for this partner
                first_inv = sr(models, uid, "account.move", [
                    ["move_type", "=", "out_invoice"],
                    ["state", "=", "posted"],
                    ["partner_id", "=", pid],
                ], ["invoice_date", "partner_id"], limit=1, order="invoice_date asc")
                if not first_inv:
                    continue
                first_date = first_inv[0].get("invoice_date", "")
                if not first_date:
                    continue
                try:
                    first_dt = datetime.strptime(first_date, "%Y-%m-%d").date()
                except Exception:
                    continue
                days = (today - first_dt).days
                # Only include if their FIRST invoice is within the 150-210d window
                if days < 150 or days > 210:
                    continue
                pname = safe_name(first_inv[0].get("partner_id"))
                # Get vendedor from res.partner.user_id (canonical rule)
                partner_rec = sr(models, uid, "res.partner", [["id", "=", pid]], ["user_id"], limit=1)
                vendedor = ""
                if partner_rec:
                    vendedor = canonical_vendedor(safe_name(partner_rec[0].get("user_id")))
                vn = norm_name(vendedor)
                is_exec = any(all(w in vn.split() for w in norm_name(av).split()) for av in _exec_names)
                if is_exec:
                    status = "pendiente" if days >= 180 else "proximo"
                    grad_candidates.append({
                        "name": pname,
                        "vendedor": vendedor,
                        "first_invoice": first_date,
                        "days_since": days,
                        "status": status,
                        "pid": pid,
                    })

        # Count invoices + litros diesel for grad candidates
        for g in grad_candidates:
            cnt = s_count(models, uid, "account.move", [
                ["move_type", "=", "out_invoice"],
                ["state", "=", "posted"],
                ["partner_id.name", "=", g["name"]],
            ])
            g["total_invoices"] = cnt
            lines = sr(models, uid, "account.move.line", [
                ["move_id.move_type", "=", "out_invoice"],
                ["move_id.state", "=", "posted"],
                ["move_id.partner_id", "=", g.pop("pid")],
                ["product_id", "=", DIESEL_PRODUCT_ID],
            ], ["quantity"], limit=500)
            g["litros"] = round(sum(l.get("quantity", 0) or 0 for l in lines))

        graduating = sorted(grad_candidates, key=lambda x: -x["days_since"])[:50]
        prox = sum(1 for g in graduating if g["status"] == "proximo")
        pend = sum(1 for g in graduating if g["status"] == "pendiente")
        print(f"  Graduating: {prox} próximos (150-179d) + {pend} pendientes (180+d)")
    except Exception as e:
        print(f"  Graduating skipped: {e}")

    # Merge everything into one JSON
    data = {
        "updated": datetime.now().isoformat(),
        "week": {"start": get_enap_week()["start"], "end": get_enap_week()["end"], "label": get_enap_week()["label"]},
        # Original CRM fields (backward compatible)
        "has_litros": True,  # expected_revenue stores litros in TomEnergy's Odoo
        "summary": crm["summary"],
        "executives": crm["executives"],
        "funnel": crm["funnel"],
        "pipeline": crm["pipeline"],
        "won_deals": crm["won_deals"],
        "stale": crm["stale"],
        "activities": crm["activities"],
        "messages": crm["messages"],
        # New fields
        "funnel_weeks": funnel_weeks,
        "ventas": ventas,
        "ventas_prev": ventas_prev,
        "monthly_history": monthly_history,
        "churn": churn,
        "rescued": rescued,
        "recovery": recovery,
        "credit_risk": credit_risk,
        "graduating": graduating,
        "operaciones": operaciones,
        "vendor_goals": vendor_goals,
        "company_goals": {
            "litros_mes": _meta_mes(today),
            "litros_cs_mes": 526505,
            "margen_retail": 8.5,
            "margen_volumen": 6.0,
            "month": f"{MESES_ES[today.month].lower()} {today.year}",
            "budget_monthly_2026": {str(k): v for k, v in BUDGET_2026.items()},
            "budget_monthly_2025": {str(k): v for k, v in BUDGET_2025.items()},
        },
        "funnel_goals": {
            "leads": {"goal": 15, "label": "Leads", "freq": "semanal"},
            "contacto": {"goal": 10, "label": "Contacto Efectivo", "freq": "semanal"},
            "cotizacion": {"goal": 8, "label": "Cotizacion", "freq": "semanal"},
            "seguimiento": {"goal": 100, "label": "Cotiz. Gestionadas", "unit": "%", "freq": "semanal"},
            "cierre": {"goal": 2, "label": "Cierre", "freq": "semanal"},
            "retencion": {"goal": 90, "label": "Retencion 90d", "unit": "%", "freq": "mensual"},
        },
    }

    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "crm-data.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    print(f"\ncrm-data.json written OK ({crm['summary']['active']} leads, {len(funnel_weeks)} funnel weeks)")


if __name__ == "__main__":
    main()
    
