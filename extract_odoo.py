#!/usr/bin/env python3
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
        by_month[m]["r"] += rev
        by_month[m]["m"] += mar
        by_month[m]["c"] += 1
        by_zone.setdefault(zone, {"r": 0, "m": 0, "c": 0})
        by_zone[zone]["r"] += rev
        by_zone[zone]["m"] += mar
        by_zone[zone]["c"] += 1
        k = f"{cid}|{client}"
        by_client.setdefault(k, {"name": client, "r": 0, "m": 0, "c": 0})
        by_client[k]["r"] += rev
        by_client[k]["m"] += mar
        by_client[k]["c"] += 1
        tot_rev += rev
        tot_margin += mar
        tot_orders += 1
        if m == this_month:
            tm_rev += rev
            tm_orders += 1
            tm_margin += mar
    monthly = []
    for m, d in sorted(by_month.items()):
        monthly.append({
            "month": m,
            "rev": round(d["r"]),
            "margin": round(d["m"]),
            "orders": d["c"],
            "margin_pct": round(d["m"] / d["r"] * 100, 1) if d["r"] > 0 else 0,
        })
    zones = []
    for z, d in sorted(by_zone.items(), key=lambda x: -x[1]["r"]):
        if z == "Sin Zona" and d["c"] < 5:
            continue
        zones.append({"name": z, "rev": round(d["r"]), "margin": round(d["m"]), "orders": d["c"]})
    top_clients = sorted(by_client.values(), key=lambda x: -x["r"])[:25]
    clients = []
    for d in top_clients:
        clients.append({
            "name": d["name"],
            "rev": round(d["r"]),
            "margin": round(d["m"]),
            "margin_pct": round(d["m"] / d["r"] * 100, 1) if d["r"] > 0 else 0,
            "orders": d["c"],
        })
    active_clients = len(by_client)
    total_customers = s_count(models, uid, "res.partner", [["customer_rank", ">", 0]])
    return {
        "monthly": monthly,
        "zones": zones,
        "top_clients": clients,
        "this_month": {"label": this_month, "rev": round(tm_rev), "orders": tm_orders, "margin": round(tm_margin)},
        "totals": {"rev": round(tot_rev), "margin": round(tot_margin), "orders": tot_orders,
                   "active_clients": active_clients, "total_customers": total_customers},
    }

def extract_ar(models, uid):
    print("Extracting accounts receivable...")
    invoices = fetch_all(models, uid, "account.move",
        [["move_type", "=", "out_invoice"], ["state", "=", "posted"],
         ["payment_state", "in", ["not_paid", "partial"]], ["amount_residual", ">", 0]],
        ["partner_id", "invoice_date_due", "amount_total", "amount_residual"])
    print(f"  {len(invoices)} open invoices")
    today = datetime.now().date()
    total_due = overdue = current = 0
    aging = {"0_30": 0, "31_60": 0, "61_90": 0, "90_plus": 0}
    debtor_map = {}
    for inv in invoices:
        res = inv["amount_residual"] or 0
        total_due += res
        due_str = inv["invoice_date_due"]
        days_over = (today - datetime.strptime(due_str, "%Y-%m-%d").date()).days if due_str else 0
        if days_over > 0:
            overdue += res
            if days_over <= 30:
                aging["0_30"] += res
            elif days_over <= 60:
                aging["31_60"] += res
            elif days_over <= 90:
                aging["61_90"] += res
            else:
                aging["90_plus"] += res
        else:
            current += res
        cn = inv["partner_id"][1] if inv["partner_id"] else "N/A"
        debtor_map[cn] = debtor_map.get(cn, 0) + res
    top_debtors = sorted(debtor_map.items(), key=lambda x: -x[1])[:20]
    debtors = [{"name": n, "amount": round(a)} for n, a in top_debtors]
    print(f"  Total due: ${total_due/1e6:.0f}M, overdue: ${overdue/1e6:.0f}M")
    return {
        "open_invoices": len(invoices),
        "total_due": round(total_due),
        "current": round(current),
        "overdue": round(overdue),
        "aging": {k: round(v) for k, v in aging.items()},
        "top_debtors": debtors,
    }

def main():
    print(f"=== CEO Dashboard · Odoo Extraction ===")
    print(f"Server: {ODOO_URL} | DB: {ODOO_DB}")
    models, uid = connect()
    sales = extract_sales(models, uid)
    ar = extract_ar(models, uid)
    data = {"updated": datetime.now().isoformat(), "sales": sales, "ar": ar}
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ceo-data.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Done. {sales['totals']['orders']} orders, {ar['open_invoices']} invoices")

if __name__ == "__main__":
    main()
