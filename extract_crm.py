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
            "last_update": (l.get("write_date") or l.get("date_last_stage_update") or "")[:10],
            "origin": l.get("x_origen_oportunidad") or "—",
            "created": (l.get("create_date") or "")[:10],
        }
        if cls == "won":
            won_deals.append(entry)
        elif cls == "perdido":
            continue
        else:
            pipeline.append(entry)

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
        count = sum(1 for p in pipeline if p["stage_class"] == cls)
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

    msg_list = []
    for m in messages:
        body = strip_html(m.get("body") or "")
        if len(body) < 3: continue
        msg_list.append({
            "date": (m.get("date") or "")[:16],
            "who": m["author_id"][1] if m.get("author_id") else "—",
            "desc": body[:200],
        })

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
        "won_deals": sorted(won_deals, key=lambda x: -(x.get("avg_monthly_litros") or 0)),
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
        lead_detail = sr(models, uid, "crm.lead", [
            ["create_date", ">=", fdt_s(ws)],
            ["create_date", "<=", fdt_e(we)],
        ], ["name", "partner_name", "user_id", "stage_id", "create_date"], limit=20)

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
            })

        # 2. Contactos efectivos
        contact_count = 0
        contacts_by_user = defaultdict(int)
        try:
            contact_msgs = sr(models, uid, "mail.message", [
                ["date", ">=", fdt_s(ws)],
                ["date", "<=", fdt_e(we)],
                ["model", "=", "crm.lead"],
                ["message_type", "in", ["comment", "email", "notification", "sms"]],
            ], ["res_id"], limit=2000)
            contact_count = len(contact_msgs)

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
        except:
            try:
                acts = sr(models, uid, "mail.activity", [
                    ["date_deadline", ">=", ws],
                    ["date_deadline", "<=", we],
                    ["res_model", "=", "crm.lead"],
                ], ["user_id"], limit=2000)
                contact_count = len(acts)
                for a in acts:
                    u = safe_name(a.get("user_id"))
                    contacts_by_user[u] += 1
            except:
                pass

        # 3. Cotizaciones
        quote_domain = [
            ["create_date", ">=", fdt_s(ws)],
            ["create_date", "<=", fdt_e(we)],
            ["state", "in", ["draft", "sent"]],
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

        # 4. Cotizaciones Confirmadas (quotes that became sale orders)
        confirmed_count = 0
        if quote_count:
            try:
                confirmed_count = s_count(models, uid, "sale.order", [
                    ["create_date", ">=", fdt_s(ws)],
                    ["create_date", "<=", fdt_e(we)],
                    ["state", "in", ["sale", "done"]],
                ])
            except:
                pass
        followup_pct = min(round((confirmed_count / max(quote_count, 1)) * 100), 100) if quote_count else 0

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

        print(f"    Leads:{lead_count} Contactos:{contact_count} Cotiz:{quote_count} Follow:{followup_pct}% Cierre:{close_count}")

        weeks_data.append({
            "week_start": ws,
            "week_end": we,
            "label": label,
            "is_current": offset == 0,
            "stages": {
                "leads":       {"value": lead_count, "goal": 15, "by_user": dict(leads_by_user), "detail": lead_rows},
                "contacto":    {"value": contact_count, "goal": 10, "by_user": dict(contacts_by_user)},
                "cotizacion":  {"value": quote_count, "goal": 8, "by_user": dict(quotes_by_user), "detail": quote_rows},
                "seguimiento": {"value": followup_pct, "goal": 100, "unit": "%"},
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
    ], ["name", "partner_id", "invoice_user_id", "amount_untaxed", "invoice_date", "margin_zone"], limit=2000)

    inv_ids = [i["id"] for i in invoices]
    inv_user_map = {i["id"]: safe_name(i.get("invoice_user_id")) for i in invoices}
    inv_margin_map = {i["id"]: i.get("margin_zone", 0) or 0 for i in invoices}

    partner_ids = list(set(safe_id(i.get("partner_id")) for i in invoices if safe_id(i.get("partner_id"))))
    volume_partners = set()
    partner_zone = {}
    if partner_ids:
        for i in range(0, len(partner_ids), 200):
            chunk = partner_ids[i:i+200]
            partners = sr(models, uid, "res.partner", [
                ["id", "in", chunk],
            ], ["id", "is_volume_client", "delivery_zone_id"], limit=200)
            for p in partners:
                if p.get("is_volume_client"):
                    volume_partners.add(p["id"])
                zn = safe_name(p.get("delivery_zone_id"))
                if zn and zn != "False" and zn != "Sin asignar":
                    partner_zone[p["id"]] = zn
    inv_partner_map = {i["id"]: safe_id(i.get("partner_id")) for i in invoices}

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
    litros_by_partner = defaultdict(float)
    margin_by_user_venta = defaultdict(float)
    margin_by_user_costo = defaultdict(float)

    if inv_ids:
        lines = sr(models, uid, "account.move.line", [
            ["move_id", "in", inv_ids],
            ["product_id", "=", DIESEL_PRODUCT_ID],
        ], ["move_id", "quantity", "price_subtotal"], limit=5000)

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
            else:
                retail_venta += sub
                retail_costo += sub * (1 - margin) if margin else sub

    margin_retail_pct = round(((retail_venta - retail_costo) / retail_venta) * 100, 2) if retail_venta > 0 else 0
    margin_volume_pct = round(((volume_venta - volume_costo) / volume_venta) * 100, 2) if volume_venta > 0 else 0

    print(f"  Margen Retail: {margin_retail_pct}% | Margen Volumen: {margin_volume_pct}%")

    ncs = sr(models, uid, "account.move", [
        ["move_type", "=", "out_refund"],
        ["state", "=", "posted"],
        ["invoice_date", ">=", fmt(m_start)],
        ["invoice_date", "<=", fmt(m_end)],
    ], ["name", "invoice_user_id", "amount_untaxed", "partner_id", "margin_zone", "invoice_date"], limit=500)

    nc_ids = [n["id"] for n in ncs]
    nc_name_map = {n["id"]: n.get("name", "?") for n in ncs}
    nc_user_map = {n["id"]: safe_name(n.get("invoice_user_id")) for n in ncs}
    nc_partner_map = {n["id"]: safe_id(n.get("partner_id")) for n in ncs}
    nc_margin_map = {n["id"]: n.get("margin_zone", 0) or 0 for n in ncs}
    nc_date_map = {n["id"]: (n.get("invoice_date") or "")[:10] for n in ncs}

    nc_debug_list = []  # Debug: track all NC diesel subtractions

    if nc_ids:
        nc_lines = sr(models, uid, "account.move.line", [
            ["move_id", "in", nc_ids],
            ["product_id", "=", DIESEL_PRODUCT_ID],
        ], ["move_id", "quantity", "price_subtotal"], limit=2000)

        for ln in nc_lines:
            mid = safe_id(ln.get("move_id"))
            qty = ln.get("quantity", 0)
            sub = ln.get("price_subtotal", 0)
            user = nc_user_map.get(mid, "Sin asignar")
            nc_margin = nc_margin_map.get(mid, 0)
            litros_by_user[user] -= qty
            venta_by_user[user] -= sub
            total_litros -= qty
            total_venta -= sub
            margin_by_user_venta[user] -= sub
            margin_by_user_costo[user] -= sub * (1 - nc_margin) if nc_margin else sub
            nc_pid = nc_partner_map.get(mid)
            if nc_pid:
                litros_by_partner[nc_pid] -= qty

            # Debug: record each NC diesel subtraction
            nc_debug_list.append({
                "nc_name": nc_name_map.get(mid, "?"),
                "nc_date": nc_date_map.get(mid, "?"),
                "user": user,
                "partner_id": nc_pid,
                "litros": round(qty, 1),
                "subtotal": round(sub),
            })

    print(f"  NC diesel lines: {len(nc_debug_list)} (total: {sum(d['litros'] for d in nc_debug_list):.0f} L subtracted)")

    new_cl_by_user = defaultdict(int)
    new_cl_detail = []
    new_cl_count = 0
    new_cl_litros_by_user = defaultdict(float)  # liters of NEW clients, per salesperson
    seen = set()
    for inv in invoices:
        pid = safe_id(inv.get("partner_id"))
        if not pid or pid in seen: continue
        seen.add(pid)
        prev = s_count(models, uid, "account.move", [
            ["move_type", "=", "out_invoice"],
            ["state", "=", "posted"],
            ["partner_id", "=", pid],
            ["invoice_date", "<", fmt(m_start)],
        ])
        if prev == 0:
            u = safe_name(inv.get("invoice_user_id"))
            pname = safe_name(inv.get("partner_id"))
            new_cl_by_user[u] += 1
            new_cl_count += 1
            partner_litros = max(litros_by_partner.get(pid, 0), 0)
            new_cl_litros_by_user[u] += partner_litros
            new_cl_detail.append({"cliente": pname, "vendedor": u, "fecha": inv.get("invoice_date", ""), "litros": round(partner_litros)})

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
        ], ["id"], limit=5000)
        wk_ids = [i["id"] for i in wk_inv]
        wk_l = 0
        if wk_ids:
            wk_lines = sr(models, uid, "account.move.line", [
                ["move_id", "in", wk_ids],
                ["product_id", "=", DIESEL_PRODUCT_ID],
            ], ["quantity"], limit=5000)
            for ln in wk_lines:
                wk_l += ln.get("quantity", 0)
        weekly_history.append({
            "label": f"{ws_d.day}/{ws_d.month}-{we_d.day}/{we_d.month}",
            "litros": round(wk_l),
        })
    weekly_history.reverse()  # oldest first

    print(f"  Litros: {round(total_litros)} Facturas: {len(invoices)} NC: {len(ncs)}")
    print(f"  Clientes nuevos: {new_cl_count}")

    return {
        "month_label": lbl,
        "month_start": fmt(m_start),
        "month_end": fmt(m_end),
        "totals": {
            "total_litros": round(total_litros),
            "total_venta_neta": round(total_venta),
            "invoice_count": len(invoices),
            "nc_count": len(ncs),
            "nc_debug": nc_debug_list,
            "litros_by_user": {k: round(v) for k, v in litros_by_user.items()},
            "venta_by_user": {k: round(v) for k, v in venta_by_user.items()},
            "margin_retail_pct": margin_retail_pct,
            "margin_volume_pct": margin_volume_pct,
            "retail_venta": round(retail_venta),
            "volume_venta": round(volume_venta),
            "litros_by_zone": {k: round(v) for k, v in sorted(litros_by_zone.items(), key=lambda x: -x[1])},
            "venta_by_zone": {k: round(v) for k, v in sorted(venta_by_zone.items(), key=lambda x: -x[1])},
            "margin_by_zone": {k: round((1 - margin_by_zone_costo[k] / margin_by_zone_venta[k]) * 100, 1) if margin_by_zone_venta[k] > 0 else 0 for k in litros_by_zone},
            "margin_by_user": {k: round((1 - margin_by_user_costo[k] / margin_by_user_venta[k]) * 100, 1) if margin_by_user_venta.get(k, 0) > 0 else 0 for k in litros_by_user},
        },
        "new_clients": {
            "count": new_cl_count,
            "by_user": dict(new_cl_by_user),
            "litros_by_user": {k: round(v) for k, v in new_cl_litros_by_user.items()},
            "detail": new_cl_detail,
        },
        "weekly": list(reversed(weekly_sales)),
        "weekly_history": weekly_history,
    }


# ==============================================================
# PART 4: CHURN & RESCUE
# ==============================================================
LOST_THRESHOLD_DAYS = 270  # 9 months

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
    ], ["partner_id", "user_id", "partner_name", "write_date", "create_date", "description"], limit=5000)

    perdido_leads = sr(models, uid, "crm.lead", [
        ["stage_id", "in", perdido_ids],
        ["active", "=", True],
    ], ["partner_id", "user_id", "partner_name", "write_date", "create_date", "description"], limit=5000)

    print(f"  CRM: {len(durmiente_leads)} durmientes, {len(perdido_leads)} perdidos")

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
    prev_month_partners = set(safe_id(i.get("partner_id")) for i in prev_inv if safe_id(i.get("partner_id")))
    prev_month_clients = len(prev_month_partners)

    dormant_list = []
    dormant_by_user = Counter()
    rescued_dormant_list = []
    rescued_dormant_by_user = Counter()

    for lead in durmiente_leads:
        pid = safe_id(lead.get("partner_id"))
        user = safe_name(lead.get("user_id"))
        name = safe_name(lead.get("partner_id")) or lead.get("partner_name", "?")
        write_date = (lead.get("write_date") or "")[:10]
        desc = lead.get("description") or ""
        # Clean HTML from description
        if desc and "<" in desc:
            desc = re.sub(r'<[^>]+>', ' ', desc).strip()
        desc = desc[:120] if desc else ""

        entry = {"name": name, "user": user, "last_update": write_date, "partner_id": pid, "notes": desc}
        if pid and pid in curr_month_partners:
            rescued_dormant_list.append(entry)
            rescued_dormant_by_user[user] += 1
        else:
            dormant_list.append(entry)
            dormant_by_user[user] += 1

    # ── Resolve DICOM tags for dormant/lost clients ──
    all_dormant_pids = [c["partner_id"] for c in dormant_list + rescued_dormant_list if c.get("partner_id")]
    partner_tags = {}
    batch_size = 200
    if all_dormant_pids:
        for i in range(0, len(all_dormant_pids), batch_size):
            batch = all_dormant_pids[i:i+batch_size]
            partners_info = sr(models, uid, "res.partner", [
                ["id", "in", batch],
            ], ["id", "category_id"], limit=batch_size)
            for p in partners_info:
                partner_tags[p["id"]] = p.get("category_id") or []

        # Resolve tag names
        all_tag_ids = set()
        for tags in partner_tags.values():
            all_tag_ids.update(tags)
        tag_names = {}
        if all_tag_ids:
            tags_data = sr(models, uid, "res.partner.category", [
                ["id", "in", list(all_tag_ids)],
            ], ["id", "name"], limit=500)
            tag_names = {t["id"]: t.get("name", "") for t in tags_data}

        # Build linea string per partner
        for c in dormant_list + rescued_dormant_list:
            pid = c.get("partner_id")
            if pid and pid in partner_tags:
                relevant_tags = [tag_names.get(tid, "") for tid in partner_tags[pid]
                                 if tag_names.get(tid, "")]
                # Filter to relevant tags (dicom, riesgo, etc)
                linea_tags = [t for t in relevant_tags if any(k in t.lower() for k in ["dicom", "riesgo", "crédito", "credito", "bloq", "mora"])]
                c["linea"] = ", ".join(linea_tags) if linea_tags else ""
            else:
                c["linea"] = ""
        print(f"  DICOM/tag enrichment done for {len(all_dormant_pids)} dormant partners")

    # ── Avg monthly litros (8 months) for dormant clients ──
    eight_months_ago = (today.replace(day=1) - timedelta(days=1))  # end of prev month
    for _ in range(7):
        eight_months_ago = (eight_months_ago.replace(day=1) - timedelta(days=1))
    eight_months_start = eight_months_ago.replace(day=1)

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

    # Attach avg_monthly_litros and remove internal partner_id
    for c in dormant_list:
        c["avg_monthly_litros"] = avg_litros_map.get(c.get("partner_id"), 0)
        c.pop("partner_id", None)

    lost_list = []
    lost_by_user = Counter()
    rescued_lost_list = []
    rescued_lost_by_user = Counter()
    newly_lost = 0

    for lead in perdido_leads:
        pid = safe_id(lead.get("partner_id"))
        user = safe_name(lead.get("user_id"))
        name = safe_name(lead.get("partner_id")) or lead.get("partner_name", "?")
        write_date = (lead.get("write_date") or "")[:10]

        if write_date >= fmt(month_start):
            newly_lost += 1

        if pid and pid in curr_month_partners:
            rescued_lost_list.append({"name": name, "user": user, "last_update": write_date})
            rescued_lost_by_user[user] += 1
        else:
            lost_list.append({"name": name, "user": user, "last_update": write_date, "partner_id": pid})
            lost_by_user[user] += 1

    # ── Avg monthly litros (8 months) for lost clients ──
    lost_pids = [c["partner_id"] for c in lost_list if c.get("partner_id")]
    avg_litros_lost = {}
    if lost_pids:
        # Reuse same 8-month window calculated for dormant
        print(f"  Querying 8-month litros for {len(lost_pids)} lost partners...")
        for i in range(0, len(lost_pids), batch_size):
            batch = lost_pids[i:i+batch_size]
            invs = sr(models, uid, "account.move", [
                ["move_type", "=", "out_invoice"],
                ["state", "=", "posted"],
                ["partner_id", "in", batch],
                ["invoice_date", ">=", fmt(eight_months_start)],
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
                ["invoice_date", ">=", fmt(eight_months_start)],
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

        avg_litros_lost = {pid: round(max(total, 0) / 8) for pid, total in avg_litros_lost.items()}
        print(f"  Avg monthly litros computed for {len(avg_litros_lost)} lost partners")

    for c in lost_list:
        c["avg_monthly_litros"] = avg_litros_lost.get(c.get("partner_id"), 0)
        c.pop("partner_id", None)

    active_count = len(curr_month_partners | prev_month_partners)
    churn_pct = round((newly_lost / prev_month_clients) * 100, 1) if prev_month_clients > 0 else 0
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
# PART 4b: HELPERS & AT-RISK CLIENTS
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
    m = re.search(r'(\d+)\s*d[ií]a', f)
    if m:
        return int(m.group(1))
    m2 = re.search(r'(\d+)', f)
    if m2:
        return int(m2.group(1))
    return None


def extract_at_risk(models, uid):
    """
    Clients whose days-since-last-purchase exceed 1.5× their frecuencia_facturacion
    but are NOT yet in CRM stage durmiente/perdido.  These are still recoverable.
    """
    print("\nExtracting At-Risk Clients...")
    today = datetime.now().date()
    batch_size = 200

    # 1. Read all active customers with frecuencia_facturacion
    all_partners = []
    offset = 0
    while True:
        batch = sr(models, uid, "res.partner", [
            ["frecuencia_facturacion", "!=", False],
            ["customer_rank", ">", 0],
        ], ["id", "name", "frecuencia_facturacion", "category_id"],
        limit=batch_size, offset=offset)
        all_partners.extend(batch)
        if len(batch) < batch_size:
            break
        offset += batch_size

    print(f"  Partners with frecuencia: {len(all_partners)}")

    # Parse frecuencia for each partner
    partner_map = {}
    for p in all_partners:
        freq_days = parse_frecuencia_days(p.get("frecuencia_facturacion"))
        if freq_days and freq_days <= 60:  # exclude estacionales (>60d)
            partner_map[p["id"]] = {
                "name": p.get("name", "?"),
                "freq": p.get("frecuencia_facturacion", ""),
                "freq_days": freq_days,
                "tags": p.get("category_id") or [],
            }

    pids = list(partner_map.keys())
    print(f"  Non-seasonal partners: {len(pids)}")

    if not pids:
        return {"count": 0, "total_litros_at_risk": 0, "clients": []}

    # 2. Find last invoice date for each partner (batch)
    last_inv_map = {}
    for i in range(0, len(pids), batch_size):
        batch = pids[i:i+batch_size]
        invs = sr(models, uid, "account.move", [
            ["move_type", "=", "out_invoice"],
            ["state", "=", "posted"],
            ["partner_id", "in", batch],
        ], ["partner_id", "invoice_date", "invoice_user_id"],
        limit=50000, order="invoice_date desc")
        for inv in invs:
            pid = safe_id(inv.get("partner_id"))
            if pid and pid not in last_inv_map:
                last_inv_map[pid] = {
                    "date": (inv.get("invoice_date") or "")[:10],
                    "user": safe_name(inv.get("invoice_user_id")),
                }

    # 3. Also get CRM lead user as fallback
    lead_user_map = {}
    for i in range(0, len(pids), batch_size):
        batch = pids[i:i+batch_size]
        leads = sr(models, uid, "crm.lead", [
            ["partner_id", "in", batch],
            ["active", "=", True],
        ], ["partner_id", "user_id", "stage_id"], limit=50000)
        for lead in leads:
            pid = safe_id(lead.get("partner_id"))
            if pid and pid not in lead_user_map:
                lead_user_map[pid] = {
                    "user": safe_name(lead.get("user_id")),
                    "stage": safe_name(lead.get("stage_id")),
                }

    # 4. 8-month avg litros
    eight_months_ago = (today.replace(day=1) - timedelta(days=1))
    for _ in range(7):
        eight_months_ago = (eight_months_ago.replace(day=1) - timedelta(days=1))
    eight_months_start = eight_months_ago.replace(day=1)

    avg_litros = {}
    for i in range(0, len(pids), batch_size):
        batch = pids[i:i+batch_size]
        invs = sr(models, uid, "account.move", [
            ["move_type", "=", "out_invoice"],
            ["state", "=", "posted"],
            ["partner_id", "in", batch],
            ["invoice_date", ">=", fmt(eight_months_start)],
            ["invoice_date", "<=", fmt(today)],
        ], ["id", "partner_id"], limit=50000)
        inv_ids = [inv["id"] for inv in invs]
        inv_pid = {inv["id"]: safe_id(inv.get("partner_id")) for inv in invs}
        if inv_ids:
            lines = sr(models, uid, "account.move.line", [
                ["move_id", "in", inv_ids],
                ["product_id", "=", DIESEL_PRODUCT_ID],
            ], ["move_id", "quantity"], limit=50000)
            for ln in lines:
                mid = safe_id(ln.get("move_id"))
                pid_ln = inv_pid.get(mid)
                if pid_ln:
                    avg_litros[pid_ln] = avg_litros.get(pid_ln, 0) + (ln.get("quantity", 0) or 0)
    avg_litros = {pid: round(max(total, 0) / 8) for pid, total in avg_litros.items()}

    # 5. Resolve DICOM tags
    all_tag_ids = set()
    for info in partner_map.values():
        all_tag_ids.update(info["tags"])
    tag_names = {}
    if all_tag_ids:
        tags_data = sr(models, uid, "res.partner.category", [
            ["id", "in", list(all_tag_ids)],
        ], ["id", "name"], limit=500)
        tag_names = {t["id"]: t.get("name", "") for t in tags_data}

    # 6. Filter at-risk: gap > 1.5× freq AND < 2.5× freq, exclude durmiente/perdido CRM stage
    at_risk_clients = []
    for pid, info in partner_map.items():
        inv_info = last_inv_map.get(pid)
        if not inv_info or not inv_info["date"]:
            continue
        try:
            last_date = datetime.strptime(inv_info["date"], "%Y-%m-%d").date()
        except ValueError:
            continue
        gap_days = (today - last_date).days
        threshold = info["freq_days"] * 1.5
        max_threshold = info["freq_days"] * 2.5

        if gap_days <= threshold or gap_days > max_threshold:
            continue

        # Skip if CRM stage is already durmiente/perdido
        lead_info = lead_user_map.get(pid, {})
        stage = (lead_info.get("stage") or "").lower()
        if any(k in stage for k in ["durmiente", "perdido", "no cerrado"]):
            continue

        user = inv_info.get("user") or lead_info.get("user") or "Sin asignar"
        monthly_litros = avg_litros.get(pid, 0)
        urgency = "alta" if gap_days > info["freq_days"] * 2 else "media"

        # DICOM / relevant tags
        relevant = [tag_names.get(tid, "") for tid in info["tags"] if tag_names.get(tid, "")]
        linea = ", ".join(t for t in relevant if any(k in t.lower() for k in ["dicom", "riesgo", "bloq", "mora"])) or ""

        at_risk_clients.append({
            "name": info["name"],
            "freq": info["freq"],
            "dias_sin_compra": gap_days,
            "threshold": round(threshold, 1),
            "avg_monthly_litros": monthly_litros,
            "user": user,
            "urgency": urgency,
            "linea": linea,
        })

    # Sort: alta first, then by avg_monthly_litros desc
    at_risk_clients.sort(key=lambda x: (0 if x["urgency"] == "alta" else 1, -x["avg_monthly_litros"]))
    total_litros = sum(c["avg_monthly_litros"] for c in at_risk_clients)

    print(f"  At-risk clients: {len(at_risk_clients)} ({total_litros} L/mes en juego)")
    return {
        "count": len(at_risk_clients),
        "total_litros_at_risk": total_litros,
        "clients": at_risk_clients,
    }


# ==============================================================
# PART 5: RESCUED CLIENTS (based on frecuencia_facturacion)
# ==============================================================
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
            partner_vendedor[pid] = safe_name(inv.get("invoice_user_id"))

    # 2. Read frecuencia_facturacion from res.partner (batch)
    partner_freq = {}
    partner_names = {}
    batch_size = 200
    for i in range(0, len(curr_partner_ids), batch_size):
        batch = curr_partner_ids[i:i+batch_size]
        partners = sr(models, uid, "res.partner", [
            ["id", "in", batch],
        ], ["id", "name", "frecuencia_facturacion"], limit=batch_size)
        for p in partners:
            freq_days = parse_frecuencia_days(p.get("frecuencia_facturacion"))
            if freq_days is not None:
                partner_freq[p["id"]] = freq_days
            partner_names[p["id"]] = p.get("name", "?")

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
    ], ["id", "name", "monto_credito", "saldo_credito",
        "property_payment_term_id", "property_product_pricelist",
        "credit", "category_id"],  # credit = AR balance, category_id = tags
    limit=2000)

    print(f"  Partners with credit line: {len(credit_partners)}")
    if not credit_partners:
        return {"linea_insuficiente": [], "score_table": [], "summary": {}}

    partner_map = {p["id"]: p for p in credit_partners}
    partner_ids = list(partner_map.keys())

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
        invs = sr(models, uid, "account.move", [
            ["move_type", "=", "out_invoice"],
            ["state", "=", "posted"],
            ["partner_id", "in", batch],
            ["invoice_date", ">=", fmt(three_months_ago)],
            ["invoice_date", "<=", fmt(today)],
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
                "vendedor": safe_name(inv.get("invoice_user_id")),
            }

        s = partner_stats[pid]
        inv_data = litros_by_inv.get(inv["id"], {"litros": 0, "venta": 0})
        s["litros_total"] += inv_data["litros"]
        s["venta_total"] += inv_data["venta"]
        s["invoice_count"] += 1

        # Average payment days
        apd = inv.get("average_payment_days")
        if apd and apd > 0:
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

        # Margin
        margin = inv.get("margin_zone", 0) or 0
        if margin:
            s["margin_sum"] += margin
            s["margin_count"] += 1

        # Price rango
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
        monto = p.get("monto_credito", 0) or 0
        saldo = p.get("saldo_credito", 0) or 0
        ar_balance = p.get("credit", 0) or 0
        payment_term = safe_name(p.get("property_payment_term_id"))
        pricelist = safe_name(p.get("property_product_pricelist"))
        name = p.get("name", "?")

        # Parse payment term days (e.g., "30 Days", "Plazo 30 días")
        pt_days = 30  # default
        if payment_term:
            pt_match = re.search(r'(\d+)', payment_term)
            if pt_match:
                pt_days = int(pt_match.group(1))

        # Averages
        avg_monthly_litros = round(stats["litros_total"] / months_in_window)
        avg_monthly_venta = round(stats["venta_total"] / months_in_window)
        avg_payment_days = round(stats["avg_payment_days_sum"] / max(stats["avg_payment_days_count"], 1), 1)
        avg_margin = round((stats["margin_sum"] / max(stats["margin_count"], 1)) * 100, 1)
        avg_price_rango = round(stats["price_rango_sum"] / max(stats["price_rango_count"], 1), 1)

        # ── Utilization % ──
        utilizacion = round(((monto - saldo) / max(monto, 1)) * 100, 1) if monto > 0 else 0

        # ── Projected monthly billing vs credit line ──
        avg_price_per_liter = stats["venta_total"] / max(stats["litros_total"], 1) if stats["litros_total"] > 0 else 0
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
        if stats["siniestro_count"] > 0:
            score_cobranza = 20  # Siniestro = max risk
        elif stats["overdue_count"] > 0:
            overdue_ratio = stats["overdue_count"] / max(stats["invoice_count"], 1)
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
            "name": name,
            "vendedor": stats["vendedor"],
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
            "overdue_count": stats["overdue_count"],
            "overdue_amount": round(stats["overdue_amount"]),
            "siniestro_count": stats["siniestro_count"],
            "excepcion_count": stats["excepcion_count"],
            "has_dicom": has_dicom,
            "avg_margin_pct": avg_margin,
            "avg_price_rango": avg_price_rango,
            "pricelist": pricelist or "—",
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
# PART 8: GRADUATING CLIENTS (new → fidelización after 3 months)
# ==============================================================
def extract_graduating(models, uid):
    """
    Clients whose FIRST invoice was ~3 months ago (80-100 days).
    These are transitioning from 'new client' to long-term 'fidelización'.
    """
    print("\nExtracting Graduating Clients...")
    today = datetime.now().date()
    # Window: first invoice between 80 and 100 days ago
    window_start = today - timedelta(days=100)
    window_end = today - timedelta(days=80)
    batch_size = 200

    # Get all invoices in that window
    invs = sr(models, uid, "account.move", [
        ["move_type", "=", "out_invoice"],
        ["state", "=", "posted"],
        ["invoice_date", ">=", fmt(window_start)],
        ["invoice_date", "<=", fmt(window_end)],
    ], ["partner_id", "invoice_user_id", "invoice_date"], limit=10000)

    # Unique partners
    partner_data = {}
    for inv in invs:
        pid = safe_id(inv.get("partner_id"))
        if pid and pid not in partner_data:
            partner_data[pid] = {
                "name": safe_name(inv.get("partner_id")),
                "vendedor": safe_name(inv.get("invoice_user_id")),
                "first_invoice": inv.get("invoice_date", "")[:10],
            }

    if not partner_data:
        print("  No candidates found")
        return []

    # For each candidate, check if it was truly their FIRST invoice
    graduating = []
    pids = list(partner_data.keys())
    for i in range(0, len(pids), batch_size):
        batch = pids[i:i+batch_size]
        for pid in batch:
            first_inv_date = partner_data[pid]["first_invoice"]
            # Count invoices BEFORE that date
            prev_count = s_count(models, uid, "account.move", [
                ["move_type", "=", "out_invoice"],
                ["state", "=", "posted"],
                ["partner_id", "=", pid],
                ["invoice_date", "<", first_inv_date],
            ])
            if prev_count > 0:
                continue  # Not their first invoice

            # Count total invoices since then
            total_invs = s_count(models, uid, "account.move", [
                ["move_type", "=", "out_invoice"],
                ["state", "=", "posted"],
                ["partner_id", "=", pid],
            ])

            days_since = (today - datetime.strptime(first_inv_date, "%Y-%m-%d").date()).days
            graduating.append({
                "name": partner_data[pid]["name"],
                "vendedor": partner_data[pid]["vendedor"],
                "first_invoice": first_inv_date,
                "days_since": days_since,
                "total_invoices": total_invs,
            })

    graduating.sort(key=lambda x: -x["total_invoices"])
    print(f"  Graduating clients: {len(graduating)}")
    return graduating


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

    # Part 4: Churn & Rescue
    churn = extract_churn_data(models, uid)

    # Part 4b: At-risk clients
    at_risk = extract_at_risk(models, uid)

    # Part 5: Rescued clients (frecuencia-based)
    rescued = extract_rescued_clients(models, uid)

    # Part 6: Credit Risk
    credit_risk = extract_credit_risk(models, uid)

    # Part 6b: SLA entrega mes anterior
    sla_prev = extract_sla_data(models, uid, prev_m_start, prev_m_end)
    ventas_prev["sla"] = sla_prev

    # Part 8: Graduating clients (new → fidelización)
    graduating = extract_graduating(models, uid)

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
        with open(goals_path, "r", encoding="utf-8") as f:
            vendor_goals = json.load(f)
        print(f"\nvendor-goals.json loaded ({len(vendor_goals)} vendors)")
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
        "churn": churn,
        "at_risk": at_risk,
        "rescued": rescued,
        "graduating": graduating,
        "credit_risk": credit_risk,
        "vendor_goals": vendor_goals,
        "company_goals": {
            "litros_mes": 1305689,
            "margen_retail": 8.5,
            "margen_volumen": 6.0,
            "month": "abril 2026",
            "budget_monthly_2026": {
                "1": 1065753, "2": 1090372, "3": 1135242, "4": 1305689,
                "5": 1035293, "6": 866750, "7": 1107706, "8": 1084934,
                "9": 1052901, "10": 1354911, "11": 1394823, "12": 1754688,
            },
            "budget_monthly_2025": {
                "1": 637139, "2": 645973, "3": 673983, "4": 846275,
                "5": 600862, "6": 451928, "7": 716270, "8": 730703,
                "9": 736331, "10": 857146, "11": 829032, "12": 850191,
            },
        },
        "funnel_goals": {
            "leads": {"goal": 15, "label": "Leads", "freq": "semanal"},
            "contacto": {"goal": 10, "label": "Contacto Efectivo", "freq": "semanal"},
            "cotizacion": {"goal": 8, "label": "Cotizacion", "freq": "semanal"},
            "seguimiento": {"goal": 100, "label": "Cotiz. Confirmadas", "unit": "%", "freq": "semanal"},
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
