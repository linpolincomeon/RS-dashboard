#!/usr/bin/env python3
"""
CRM Weekly Dashboard — Odoo Data Extractor
Extracts CRM pipeline, activities, per-executive stats,
PLUS funnel metrics and sales KPIs.
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

    def days_since_update(lead):
        dt_str = lead.get("date_last_stage_update") or lead.get("write_date")
        if not dt_str: return 999
        try:
            dt = datetime.strptime(dt_str[:19], "%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError):
            try: dt = datetime.strptime(dt_str[:10], "%Y-%m-%d")
            except: return 999
        return (now - dt).days

    def is_terminal(stage_id):
        return stage_class.get(stage_id, "") in ("won", "perdido")

    pipeline = []
    for l in leads:
        sid = l["stage_id"][0] if l["stage_id"] else None
        if is_terminal(sid): continue
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

    exec_map = {}
    for p in pipeline:
        eid = p["exec_id"]
        if eid not in exec_map:
            exec_map[eid] = {"name": p["exec"], "total": 0, "moved": 0, "stale": 0, "value": 0}
        exec_map[eid]["total"] += 1
        exec_map[eid]["value"] += p["value"]
        if p["last_update"] >= week_start: exec_map[eid]["moved"] += 1
        if p["days_in_stage"] > 7: exec_map[eid]["stale"] += 1

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

        # 2. Contactos
        contact_count = None
        try:
            contact_count = s_count(models, uid, "mail.message", [
                ["date", ">=", fdt_s(ws)],
                ["date", "<=", fdt_e(we)],
                ["model", "=", "crm.lead"],
                ["message_type", "=", "notification"],
            ])
        except:
            try:
                contact_count = s_count(models, uid, "mail.activity", [
                    ["date_deadline", ">=", ws],
                    ["date_deadline", "<=", we],
                    ["res_model", "=", "crm.lead"],
                ])
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
                "estado": q.get("state", ""),
                "fecha": (q.get("create_date") or "")[:10],
            })

        # 4. Seguimiento
        followup_pct = 0
        if quote_count:
            try:
                fu = s_count(models, uid, "mail.message", [
                    ["date", ">=", fdt_s(ws)],
                    ["date", "<=", fdt_e(we)],
                    ["model", "=", "sale.order"],
                ])
                followup_pct = min(round((fu / max(quote_count, 1)) * 100), 100)
            except:
                pass

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
                "contacto":    {"value": contact_count, "goal": 10},
                "cotizacion":  {"value": quote_count, "goal": 8, "by_user": dict(quotes_by_user), "detail": quote_rows},
                "seguimiento": {"value": followup_pct, "goal": 100, "unit": "%"},
                "cierre":      {"value": close_count, "goal": 2, "by_user": dict(close_by_user), "detail": close_detail},
            }
        })

    # 6. Retencion 90d (solo semana actual)
    # Definición: de los clientes que compraron en los últimos 90 días,
    # ¿qué % compró al menos 2 veces?
    wk0 = get_enap_week(0)
    wed_date = wk0["wed"]
    lookback_start = wed_date - timedelta(days=90)

    all_orders_90d = sr(models, uid, "sale.order", [
        ["date_order", ">=", fdt_s(lookback_start)],
        ["date_order", "<=", fdt_e(wed_date)],
        ["state", "=", "sale"],
    ], ["partner_id"], limit=5000)

    # Count orders per partner
    partner_order_count = Counter()
    for o in all_orders_90d:
        pid = safe_id(o.get("partner_id"))
        if pid:
            partner_order_count[pid] += 1

    ret_total = len(partner_order_count)  # unique clients who bought
    retained_ids = {pid for pid, cnt in partner_order_count.items() if cnt >= 2}
    ret_pct = round((len(retained_ids) / ret_total) * 100) if ret_total > 0 else 0
    print(f"  Retencion 90d: {ret_pct}% ({len(retained_ids)}/{ret_total} clientes con 2+ compras)")

    weeks_data[0]["stages"]["retencion"] = {
        "value": ret_pct, "goal": 90, "unit": "%",
        "total_evaluated": ret_total, "retained_count": len(retained_ids),
    }

    return weeks_data


# ==============================================================
# PART 3: SALES KPIs (monthly)
# ==============================================================
def extract_sales_data(models, uid):
    print("\nExtracting sales KPIs...")
    m_start, m_end = get_month_range()
    print(f"  Mes: {fmt(m_start)} -> {fmt(m_end)}")

    # Facturas
    invoices = sr(models, uid, "account.move", [
        ["move_type", "=", "out_invoice"],
        ["state", "=", "posted"],
        ["invoice_date", ">=", fmt(m_start)],
        ["invoice_date", "<=", fmt(m_end)],
    ], ["name", "partner_id", "invoice_user_id", "amount_untaxed", "invoice_date"], limit=2000)

    inv_ids = [i["id"] for i in invoices]
    inv_user_map = {i["id"]: safe_name(i.get("invoice_user_id")) for i in invoices}

    litros_by_user = defaultdict(float)
    venta_by_user = defaultdict(float)
    total_litros = 0
    total_venta = 0

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
            litros_by_user[user] += qty
            venta_by_user[user] += sub
            total_litros += qty
            total_venta += sub

    # NC (restar)
    ncs = sr(models, uid, "account.move", [
        ["move_type", "=", "out_refund"],
        ["state", "=", "posted"],
        ["invoice_date", ">=", fmt(m_start)],
        ["invoice_date", "<=", fmt(m_end)],
    ], ["name", "invoice_user_id", "amount_untaxed"], limit=500)

    nc_ids = [n["id"] for n in ncs]
    nc_user_map = {n["id"]: safe_name(n.get("invoice_user_id")) for n in ncs}

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
            litros_by_user[user] -= qty
            venta_by_user[user] -= sub
            total_litros -= qty
            total_venta -= sub

    # Clientes nuevos (primera factura en el mes)
    new_cl_by_user = defaultdict(int)
    new_cl_count = 0
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
            new_cl_by_user[u] += 1
            new_cl_count += 1

    # Ventas semanales del mes
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

    print(f"  Litros: {round(total_litros)} Venta: {round(total_venta)} Facturas: {len(invoices)} NC: {len(ncs)}")
    print(f"  Clientes nuevos: {new_cl_count}")

    return {
        "month_label": m_start.strftime("%B %Y"),
        "month_start": fmt(m_start),
        "month_end": fmt(m_end),
        "totals": {
            "total_litros": round(total_litros),
            "total_venta_neta": round(total_venta),
            "invoice_count": len(invoices),
            "nc_count": len(ncs),
            "litros_by_user": {k: round(v) for k, v in litros_by_user.items()},
            "venta_by_user": {k: round(v) for k, v in venta_by_user.items()},
        },
        "new_clients": {"count": new_cl_count, "by_user": dict(new_cl_by_user)},
        "weekly": list(reversed(weekly_sales)),
    }


# ==============================================================
# PART 4: CHURN & RESCUE (based on invoicing, not CRM)
# ==============================================================
def extract_churn_data(models, uid):
    """
    Durmiente (by invoicing):
      - Calculate avg purchase frequency over last 3 months
      - If avg freq < 30 days: dormant if last purchase > freq * 1.5
      - If avg freq >= 30 days: dormant if last purchase > freq * 1.3
    Churn = dormant client who never came back
    Rescue = dormant client who invoiced again
    """
    print("\nExtracting Churn & Rescue data...")
    today = datetime.now().date()
    three_months_ago = today - timedelta(days=90)
    six_months_ago = today - timedelta(days=180)  # need 6 months to calc 3-month frequency

    # Get all posted invoices for last 6 months to calculate frequency
    all_inv = sr(models, uid, "account.move", [
        ["move_type", "=", "out_invoice"],
        ["state", "=", "posted"],
        ["invoice_date", ">=", fmt(six_months_ago)],
        ["invoice_date", "<=", fmt(today)],
    ], ["partner_id", "invoice_date", "invoice_user_id"], limit=10000, order="invoice_date asc")

    # Group invoices by partner
    partner_invoices = defaultdict(list)
    partner_user = {}  # last known salesperson
    for inv in all_inv:
        pid = safe_id(inv.get("partner_id"))
        if not pid:
            continue
        inv_date = inv.get("invoice_date", "")
        if inv_date:
            partner_invoices[pid].append(inv_date)
        u = safe_name(inv.get("invoice_user_id"))
        if u and u != "Sin asignar":
            partner_user[pid] = u

    # Get partner names
    partner_names = {}
    for inv in all_inv:
        pid = safe_id(inv.get("partner_id"))
        if pid and pid not in partner_names:
            partner_names[pid] = safe_name(inv.get("partner_id"))

    # Classify each partner
    active_clients = []
    dormant_clients = []
    rescued_clients = []

    for pid, dates in partner_invoices.items():
        if len(dates) < 2:
            # Only 1 invoice ever in 6 months — check if within 3-month window
            last_date = datetime.strptime(dates[-1], "%Y-%m-%d").date()
            days_since = (today - last_date).days
            if days_since > 45:  # single-purchase client, conservative threshold
                dormant_clients.append({
                    "id": pid,
                    "name": partner_names.get(pid, "?"),
                    "user": partner_user.get(pid, "Sin asignar"),
                    "avg_freq": 0,
                    "days_since": days_since,
                    "last_date": dates[-1],
                    "invoices_6m": 1,
                })
            continue

        # Calculate intervals between purchases (using 3-month window for frequency)
        recent_dates = [d for d in dates if d >= fmt(three_months_ago)]
        if len(recent_dates) >= 2:
            intervals = []
            for i in range(1, len(recent_dates)):
                d1 = datetime.strptime(recent_dates[i-1], "%Y-%m-%d").date()
                d2 = datetime.strptime(recent_dates[i], "%Y-%m-%d").date()
                intervals.append((d2 - d1).days)
            avg_freq = sum(intervals) / len(intervals) if intervals else 0
        else:
            # Less than 2 invoices in 3 months — use all 6-month data
            intervals = []
            for i in range(1, len(dates)):
                d1 = datetime.strptime(dates[i-1], "%Y-%m-%d").date()
                d2 = datetime.strptime(dates[i], "%Y-%m-%d").date()
                intervals.append((d2 - d1).days)
            avg_freq = sum(intervals) / len(intervals) if intervals else 0

        last_date = datetime.strptime(dates[-1], "%Y-%m-%d").date()
        days_since = (today - last_date).days

        # Apply dormancy rule
        if avg_freq > 0:
            if avg_freq < 30:
                threshold = avg_freq * 1.5
            else:
                threshold = avg_freq * 1.3
        else:
            threshold = 45  # fallback

        is_dormant = days_since > threshold

        client_data = {
            "id": pid,
            "name": partner_names.get(pid, "?"),
            "user": partner_user.get(pid, "Sin asignar"),
            "avg_freq": round(avg_freq),
            "days_since": days_since,
            "threshold": round(threshold),
            "last_date": dates[-1],
            "invoices_6m": len(dates),
        }

        if is_dormant:
            # Check if they were dormant before but came back (rescued)
            # = they crossed the threshold at some point but have a recent invoice
            if days_since <= 30 and len(dates) >= 3:
                # Recently purchased but was dormant before — this is a rescue
                rescued_clients.append(client_data)
            else:
                dormant_clients.append(client_data)
        else:
            active_clients.append(client_data)

    # Sort dormants by days_since descending
    dormant_clients.sort(key=lambda x: -x["days_since"])

    # Aggregate by salesperson
    dormant_by_user = Counter()
    rescued_by_user = Counter()
    active_by_user = Counter()
    for c in dormant_clients:
        dormant_by_user[c["user"]] += 1
    for c in rescued_clients:
        rescued_by_user[c["user"]] += 1
    for c in active_clients:
        active_by_user[c["user"]] += 1

    total_with_history = len(active_clients) + len(dormant_clients) + len(rescued_clients)
    churn_pct = round((len(dormant_clients) / total_with_history) * 100) if total_with_history > 0 else 0
    rescue_pct = round((len(rescued_clients) / max(len(dormant_clients) + len(rescued_clients), 1)) * 100)

    print(f"  Clientes activos: {len(active_clients)}")
    print(f"  Durmientes: {len(dormant_clients)}")
    print(f"  Rescatados: {len(rescued_clients)}")
    print(f"  Churn rate: {churn_pct}%")

    return {
        "summary": {
            "total_clients": total_with_history,
            "active": len(active_clients),
            "dormant": len(dormant_clients),
            "rescued": len(rescued_clients),
            "churn_pct": churn_pct,
            "rescue_pct": rescue_pct,
        },
        "by_user": {
            "dormant": dict(dormant_by_user),
            "rescued": dict(rescued_by_user),
            "active": dict(active_by_user),
        },
        "dormant_list": dormant_clients[:50],  # top 50 for the table
        "rescued_list": rescued_clients[:20],
    }


# ==============================================================
# MAIN
# ==============================================================
def main():
    print("=== CRM + Funnel + Ventas · Odoo Extraction ===")
    models, uid = connect()

    # Part 1: Original CRM
    crm = extract_crm_data(models, uid)

    # Part 2: Funnel comercial
    funnel_weeks = extract_funnel_data(models, uid)

    # Part 3: Sales KPIs
    ventas = extract_sales_data(models, uid)

    # Part 4: Churn & Rescue
    churn = extract_churn_data(models, uid)

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

    # Merge everything into one JSON
    data = {
        "updated": datetime.now().isoformat(),
        "week": {"start": get_enap_week()["start"], "end": get_enap_week()["end"], "label": get_enap_week()["label"]},
        # Original CRM fields (backward compatible)
        "has_litros": crm["has_litros"],
        "summary": crm["summary"],
        "executives": crm["executives"],
        "funnel": crm["funnel"],
        "pipeline": crm["pipeline"],
        "stale": crm["stale"],
        "activities": crm["activities"],
        "messages": crm["messages"],
        # New fields
        "funnel_weeks": funnel_weeks,
        "ventas": ventas,
        "churn": churn,
        "vendor_goals": vendor_goals,
        "funnel_goals": {
            "leads": {"goal": 15, "label": "Leads", "freq": "semanal"},
            "contacto": {"goal": 10, "label": "Contacto Efectivo", "freq": "semanal"},
            "cotizacion": {"goal": 8, "label": "Cotizacion", "freq": "semanal"},
            "seguimiento": {"goal": 100, "label": "Seguimiento 48h", "unit": "%", "freq": "semanal"},
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
