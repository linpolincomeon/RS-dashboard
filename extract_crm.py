#!/usr/bin/env python3
"""
CRM Weekly Dashboard — Odoo Data Extractor
Extracts CRM pipeline, activities, and per-executive stats.
Outputs crm-data.json for the static dashboard on GitHub Pages.

Runs via GitHub Actions on the same schedule as extract_ceo.py.
Uses the same ODOO_URL / ODOO_DB / ODOO_USER / ODOO_KEY env vars.
"""
import xmlrpc.client
import json
import os
from datetime import datetime, timedelta


ODOO_URL = os.environ.get("ODOO_URL", "https://tomenergy.cl")
ODOO_DB = os.environ.get("ODOO_DB", "PRODUCCION")
ODOO_USER = os.environ.get("ODOO_USER", "p@tomenergy.cl")
ODOO_KEY = os.environ.get("ODOO_KEY", "")


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


# ── ENAP week: Thursday to Wednesday ──
def get_enap_week():
    today = datetime.now()
    days_since_thu = (today.weekday() - 3) % 7
    thu = today - timedelta(days=days_since_thu)
    thu = thu.replace(hour=0, minute=0, second=0, microsecond=0)
    wed = thu + timedelta(days=6)
    return {
        "start": thu.strftime("%Y-%m-%d"),
        "end": wed.strftime("%Y-%m-%d"),
        "label": f"{thu.strftime('%d %b')} – {wed.strftime('%d %b')}",
    }


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


def extract_crm_data(models, uid):
    print("Extracting CRM pipeline data...")
    week = get_enap_week()
    now = datetime.now()
    week_start = week["start"]

    # Custom fields
    cf = detect_custom_fields(models, uid)

    # Stages
    stages = sr(models, uid, "crm.stage", [], ["id", "name", "sequence"], limit=50, order="sequence asc")
    stage_map = {s["id"]: s["name"] for s in stages}
    stage_class = {s["id"]: classify_stage(s["name"]) for s in stages}

    # All active leads
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

    # Activities
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

    # Recent messages (this week)
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

    # ── Process leads ──
    def get_value(lead):
        if cf["x_litros_estimados"]:
            return lead.get("x_litros_estimados") or 0
        return lead.get("expected_revenue") or 0

    def days_since_update(lead):
        dt_str = lead.get("date_last_stage_update") or lead.get("write_date")
        if not dt_str:
            return 999
        try:
            dt = datetime.strptime(dt_str[:19], "%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError):
            try:
                dt = datetime.strptime(dt_str[:10], "%Y-%m-%d")
            except:
                return 999
        return (now - dt).days

    def is_terminal(stage_id):
        return stage_class.get(stage_id, "") in ("won", "perdido")

    # Build pipeline records
    pipeline = []
    for l in leads:
        sid = l["stage_id"][0] if l["stage_id"] else None
        if is_terminal(sid):
            continue
        days = days_since_update(l)
        pipeline.append({
            "id": l["id"],
            "name": l["partner_id"][1] if l["partner_id"] else l["name"],
            "stage": l["stage_id"][1] if l["stage_id"] else "—",
            "stage_class": stage_class.get(sid, "oportunidad"),
            "exec": l["user_id"][1] if l["user_id"] else "Sin asignar",
            "exec_id": l["user_id"][0] if l["user_id"] else 0,
            "value": round(get_value(l)),
            "days_in_stage": days,
            "last_update": (l.get("date_last_stage_update") or l.get("write_date") or "")[:10],
            "origin": l.get("x_origen_oportunidad") or "—",
            "created": (l.get("create_date") or "")[:10],
        })

    # Executive summary
    exec_map = {}
    for p in pipeline:
        eid = p["exec_id"]
        if eid not in exec_map:
            exec_map[eid] = {"name": p["exec"], "total": 0, "moved": 0, "stale": 0, "value": 0}
        exec_map[eid]["total"] += 1
        exec_map[eid]["value"] += p["value"]
        if p["last_update"] >= week_start:
            exec_map[eid]["moved"] += 1
        if p["days_in_stage"] > 7:
            exec_map[eid]["stale"] += 1

    executives = sorted(exec_map.values(), key=lambda x: -x["total"])

    # Summary stats
    created_this_week = sum(1 for l in leads if (l.get("create_date") or "")[:10] >= week_start)
    won_count = sum(1 for l in leads if stage_class.get(
        l["stage_id"][0] if l["stage_id"] else None, "") == "won")
    moved_count = sum(1 for p in pipeline if p["last_update"] >= week_start)
    stale_count = sum(1 for p in pipeline if p["days_in_stage"] > 7)
    total_value = sum(p["value"] for p in pipeline)

    # Stale leads (>7 days without movement)
    stale_leads = sorted(
        [p for p in pipeline if p["days_in_stage"] > 7],
        key=lambda x: -x["days_in_stage"]
    )[:50]

    # Suggested actions per stage
    action_map = {
        "oportunidad": "Llamar o enviar mail",
        "contactado": "Agendar visita (Ruta)",
        "ruta": "Enviar cotización",
        "cotizado": "Seguimiento cotización",
    }
    for s in stale_leads:
        s["action"] = action_map.get(s["stage_class"], "Contactar")

    # Stage funnel
    funnel = []
    for stg in stages:
        cls = classify_stage(stg["name"])
        if cls in ("won", "perdido"):
            continue
        count = sum(1 for p in pipeline if p["stage_class"] == cls)
        if count > 0:
            funnel.append({"stage": stg["name"], "class": cls, "count": count})

    # Process activities
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

    # Process messages (strip HTML)
    import re
    def strip_html(text):
        return re.sub(r'<[^>]+>', '', text or '').strip()

    msg_list = []
    for m in messages:
        body = strip_html(m.get("body") or "")
        if len(body) < 3:
            continue
        msg_list.append({
            "date": (m.get("date") or "")[:16],
            "who": m["author_id"][1] if m.get("author_id") else "—",
            "desc": body[:200],
        })

    return {
        "updated": now.isoformat(),
        "week": week,
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
        "stale": stale_leads,
        "activities": act_list[:50],
        "messages": msg_list[:30],
    }


def main():
    print("=== CRM Weekly Dashboard · Odoo Extraction ===")
    models, uid = connect()
    data = extract_crm_data(models, uid)

    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "crm-data.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"\ncrm-data.json written OK ({data['summary']['active']} active leads)")


if __name__ == "__main__":
    main()
