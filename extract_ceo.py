#!/usr/bin/env python3
"""
CEO Dashboard — Odoo Data Extractor
Same auth pattern as extract_odoo.py. Outputs ceo-data.json.
"""
import xmlrpc.client
import json
import os
from datetime import datetime, timedelta

ODOO_URL = os.environ.get("ODOO_URL", "https://tomenergy.cl")
ODOO_DB = os.environ.get("ODOO_DB", "PRODUCCION")
ODOO_USER = os.environ.get("ODOO_USER", "made@tomenergy.cl")
ODOO_KEY = os.environ.get("ODOO_KEY", "f4188f3cbe069a9f5ce60325fa17a2c5333176d1")


def connect():
    common = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/common")
    uid = common.authenticate(ODOO_DB, ODOO_USER, ODOO_KEY, {})
    if not uid:
        raise Exception("Authentication failed.")
    models = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/object")
    print(f"Connected as uid={uid}")
    return models, uid


def sr(models, uid, model, domain, fields, limit=5000, offset=0):
    return models.execute_kw(
        ODOO_DB, uid, ODOO_KEY, model, "search_read",
        [domain], {"fields": fields, "limit": limit, "offset": offset}
    )


def fetch_all(models, uid, model, domain, fields):
    all_recs, offset = [], 0
    while True:
        batch = sr(models, uid, model, domain, fields, limit=2000, offset=offset)
        all_recs.extend(batch)
        if len(batch) < 2000:
            break
        offset += 2000
    return all_recs


def s_count(models, uid, model, domain):
    return models.execute_kw(ODOO_DB, uid, ODOO_KEY, model, "search_count", [domain])


def extract_sales(models, uid):
    print("Extracting sale orders (13 months)...")
    cutoff = (datetime.now() - timedelta(days=400)).strftime("%Y-%m-%d")
    orders = fetch_all(models, uid, "sale.order",
        [["state", "in", ["sale", "done"]], ["date_order", ">=", cutoff]],
        ["date_order", "amount_untaxed", "amount_total", "margin", "delivery_zone_id", "partner_id"])
    print(f"  {len(orders)} orders")

    now = datetime.now()
    this_month = f"{now.year}-{now.month:02d}"
    by_month, by_zone, by_client = {}, {}, {}
    tot_rev = tot_margin = tot_orders = tm_rev = tm_orders = tm_margin = 0

    for o in orders:
        m = o["date_order"][:7]
        zone = o["delivery_zone_id"][1] if o["delivery_zone_id"] else "Sin Zona"
        client = o["partner_id"][1] if o["partner_id"] else "N/A"
        cid = o["partner_id"][0] if o["partner_id"] else 0
        rev = o["amount_untaxed"] or 0
        mar = o["margin"] or 0

        by_month.setdefault(m, {"r": 0, "m": 0, "c": 0})
        by_month[m]["r"] += rev; by_month[m]["m"] += mar; by_month[m]["c"] += 1

        by_zone.setdefault(zone, {"r": 0, "m": 0, "c": 0})
        by_zone[zone]["r"] += rev; by_zone[zone]["m"] += mar; by_zone[zone]["c"] += 1

        k = f"{cid}|{client}"
        by_client.setdefault(k, {"name": client, "r": 0, "m": 0, "c": 0})
        by_client[k]["r"] += rev; by_client[k]["m"] += mar; by_client[k]["c"] += 1

        tot_rev += rev; tot_margin += mar; tot_orders += 1
        if m == this_month:
            tm_rev += rev; tm_orders += 1; tm_margin += mar

    monthly = [{"month": m, "rev": round(d["r"]), "margin": round(d["m"]), "orders": d["c"],
                "margin_pct": round(d["m"]/d["r"]*100, 1) if d["r"] > 0 else 0}
               for m, d in sorted(by_month.items())]

    zones = [{"zone": z, "rev": round(d["r"]), "margin": round(d["m"]), "orders": d["c"]}
             for z, d in sorted(by_zone.items(), key=lambda x: -x[1]["r"]) if z != "Sin Zona" or d["c"] >= 5]

    top_clients = sorted(by_client.values(), key=lambda x: -x["r"])[:25]
    clients = [{"name": c["name"], "rev": round(c["r"]), "margin": round(c["m"]), "orders": c["c"],
                "margin_pct": round(c["m"]/c["r"]*100, 1) if c["r"] > 0 else 0} for c in top_clients]

    active = len(by_client)
    total_cust = s_count(models, uid, "res.partner", [["customer_rank", ">", 0]])

    return {
        "monthly": monthly, "zones": zones, "top_clients": clients,
        "totals": {
            "rev": round(tot_rev), "margin": round(tot_margin), "orders": tot_orders,
            "margin_pct": round(tot_margin/tot_rev*100, 1) if tot_rev > 0 else 0,
            "this_month": this_month, "tm_rev": round(tm_rev), "tm_orders": tm_orders,
            "tm_margin": round(tm_margin), "tm_margin_pct": round(tm_margin/tm_rev*100, 1) if tm_rev > 0 else 0,
            "active_clients": active, "total_customers": total_cust,
            "avg_ticket": round(tot_rev/tot_orders) if tot_orders > 0 else 0,
            "avg_orders_per_client": round(tot_orders/active, 1) if active > 0 else 0
        }
    }


def extract_receivables(models, uid):
    print("Extracting accounts receivable...")
    invoices = fetch_all(models, uid, "account.move",
        [["move_type", "=", "out_invoice"], ["state", "=", "posted"],
         ["payment_state", "in", ["not_paid", "partial"]], ["amount_residual", ">", 0]],
        ["partner_id", "invoice_date_due", "amount_total", "amount_residual"])
    print(f"  {len(invoices)} open invoices")

    today = datetime.now()
    total_due = overdue = current = 0
    aging = {"0-30": 0, "31-60": 0, "61-90": 0, "90+": 0}
    debtor_map = {}

    for inv in invoices:
        res = inv["amount_residual"] or 0
        total_due += res
        due = datetime.strptime(inv["invoice_date_due"], "%Y-%m-%d") if inv["invoice_date_due"] else today
        days = (today - due).days
        if days > 0:
            overdue += res
            if days <= 30: aging["0-30"] += res
            elif days <= 60: aging["31-60"] += res
            elif days <= 90: aging["61-90"] += res
            else: aging["90+"] += res
        else:
            current += res
        cn = inv["partner_id"][1] if inv["partner_id"] else "N/A"
        debtor_map[cn] = debtor_map.get(cn, 0) + res

    debtors = [{"name": n, "amount": round(a)} for n, a in sorted(debtor_map.items(), key=lambda x: -x[1])[:20]]

    return {
        "open_invoices": len(invoices), "total_due": round(total_due),
        "current": round(current), "overdue": round(overdue),
        "pct_overdue": round(overdue/total_due*100, 1) if total_due > 0 else 0,
        "aging": {k: round(v) for k, v in aging.items()}, "top_debtors": debtors
    }


def extract_crm(models, uid):
    print("Extracting CRM pipeline...")
    total = s_count(models, uid, "crm.lead", [])
    open_l = s_count(models, uid, "crm.lead", [["active", "=", True], ["stage_id.is_won", "=", False]])
    won = s_count(models, uid, "crm.lead", [["stage_id.is_won", "=", True]])
    thirty_ago = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    new_30 = s_count(models, uid, "crm.lead", [["create_date", ">=", thirty_ago]])

    stages = sr(models, uid, "crm.stage", [], ["name", "sequence"], limit=20)
    by_stage = []
    for s in sorted(stages, key=lambda x: x.get("sequence", 0)):
        c = s_count(models, uid, "crm.lead", [["stage_id", "=", s["id"]], ["active", "=", True]])
        if c > 0:
            by_stage.append({"stage": s["name"], "count": c})

    return {"total": total, "open": open_l, "won": won, "new_30d": new_30, "by_stage": by_stage}


def main():
    print("=== CEO Dashboard · Odoo Extraction ===")
    models, uid = connect()
    sales = extract_sales(models, uid)
    receivables = extract_receivables(models, uid)
    crm = extract_crm(models, uid)

    data = {"updated": datetime.now().isoformat(), "sales": sales, "receivables": receivables, "crm": crm}
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ceo-data.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"\nceo-data.json written OK")


if __name__ == "__main__":
    main()
