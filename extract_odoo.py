#!/usr/bin/env python3
"""
RS Dashboard — Odoo Data Extractor
Connects to Odoo via XML-RPC, extracts weekly + daily invoice data,
and outputs data.json for the static dashboard.

Usage:
  python extract_odoo.py

Requirements:
  pip install xmlrpc.client (built-in)

Configure your credentials below or via environment variables.
"""

import xmlrpc.client
import json
import os
from datetime import datetime, timedelta

# ══════════════════════════════════════════
# CONFIGURATION — update these or set env vars
# ══════════════════════════════════════════
ODOO_URL = os.environ.get("ODOO_URL", "https://tomenergy.cl")
ODOO_DB = os.environ.get("ODOO_DB", "PRODUCCION")
ODOO_USER = os.environ.get("ODOO_USER", "p@tomenergy.cl")
ODOO_KEY = os.environ.get("ODOO_KEY", "f4188f3cbe069a9f5ce60325fa17a2c5333176d1")

# Weekly budget for litros (from Sheets "meta venta")
WEEKLY_BUDGET = {
    "2026-01": 266438,  # January weekly
    "2026-02": 272593,  # February weekly
    "2026-03": 283810,  # March weekly
    "2026-04": 326422,  # April weekly (1,305,689 / ~4 weeks)
    "2026-05": 258823,  # May weekly
    "2026-06": 216688,  # June weekly
}

def get_week_budget(start_date_str):
    """Get weekly litros budget based on the month of the start date."""
    key = start_date_str[:7]  # "2026-03"
    return WEEKLY_BUDGET.get(key, 270000)  # default fallback


def connect():
    """Authenticate with Odoo via XML-RPC."""
    common = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/common")
    uid = common.authenticate(ODOO_DB, ODOO_USER, ODOO_KEY, {})
    if not uid:
        raise Exception("Authentication failed. Check your API key and DB name.")
    models = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/object")
    print(f"✓ Connected to Odoo as uid={uid}")
    return models, uid


def search_read(models, uid, model, domain, fields, limit=1000):
    """Helper for search_read calls."""
    return models.execute_kw(
        ODOO_DB, uid, ODOO_KEY,
        model, "search_read",
        [domain],
        {"fields": fields, "limit": limit}
    )


def get_week_ranges(n_weeks=12):
    """Generate Monday-Sunday week ranges for the last n weeks."""
    today = datetime.now()
    # Find this Monday
    monday = today - timedelta(days=today.weekday())
    monday = monday.replace(hour=0, minute=0, second=0, microsecond=0)

    weeks = []
    for w in range(n_weeks):
        start = monday - timedelta(weeks=w)
        end = start + timedelta(days=6)
        weeks.append({
            "start": start.strftime("%Y-%m-%d"),
            "end": end.strftime("%Y-%m-%d"),
            "label": f"{start.strftime('%d%b')}-{end.strftime('%d%b')}".lower(),
        })
    return weeks


def extract_weekly(models, uid):
    """Extract weekly invoice data + margins + payments."""
    weeks = get_week_ranges(12)
    results = []

    for i, wd in enumerate(weeks):
        print(f"  Week {i+1}/12: {wd['label']}...", end=" ")

        # Customer invoices
        invoices = search_read(models, uid, "account.move", [
            ["move_type", "=", "out_invoice"],
            ["state", "=", "posted"],
            ["invoice_date", ">=", wd["start"]],
            ["invoice_date", "<=", wd["end"]],
        ], ["amount_total", "amount_untaxed", "margin_zone", "partner_id"])

        ventas = sum(i["amount_total"] for i in invoices)
        neto = sum(i["amount_untaxed"] for i in invoices)
        clientes = len(set(i["partner_id"][0] for i in invoices if i["partner_id"]))

        # Weighted average margin
        sum_mn, sum_n = 0, 0
        for inv in invoices:
            if inv["margin_zone"] and inv["amount_untaxed"] > 0:
                sum_mn += inv["margin_zone"] * inv["amount_untaxed"]
                sum_n += inv["amount_untaxed"]
        margin = sum_mn / sum_n if sum_n > 0 else 0

        # Invoice lines for litros
        lines = search_read(models, uid, "account.move.line", [
            ["move_id.move_type", "=", "out_invoice"],
            ["move_id.state", "=", "posted"],
            ["move_id.invoice_date", ">=", wd["start"]],
            ["move_id.invoice_date", "<=", wd["end"]],
            ["display_type", "=", "product"],
        ], ["quantity", "price_subtotal"], 2000)

        litros = round(sum(l["quantity"] for l in lines))
        neto_lineas = sum(l["price_subtotal"] for l in lines)
        precio = round(neto_lineas / litros) if litros > 0 else 0

        # Payments received
        payments = search_read(models, uid, "account.payment", [
            ["payment_type", "=", "inbound"],
            ["state", "not in", ["draft", "cancel"]],
            ["date", ">=", wd["start"]],
            ["date", "<=", wd["end"]],
        ], ["amount", "journal_id"], 500)

        cheques, transf, factoring = 0, 0, 0
        for p in payments:
            j = (p["journal_id"][1] if p["journal_id"] else "").lower()
            if "cheque" in j:
                cheques += p["amount"]
            elif "factoring" in j:
                factoring += p["amount"]
            else:
                transf += p["amount"]
        recaud = sum(p["amount"] for p in payments)

        ppto = get_week_budget(wd["start"])

        results.append({
            "label": wd["label"],
            "start": wd["start"],
            "end": wd["end"],
            "ventas": round(ventas),
            "neto": round(neto),
            "litros": litros,
            "precio": precio,
            "margin": round(margin, 5),
            "facturas": len(invoices),
            "clientes": clientes,
            "recaud": round(recaud),
            "cheques": round(cheques),
            "transf": round(transf),
            "factoring": round(factoring),
            "ppto": ppto,
            "parcial": i == 0,  # current week is always partial
        })
        print(f"✓ {len(invoices)} fact, {litros}L, margin {margin:.2%}")

    return results


def extract_daily(models, uid, n_days=28):
    """Extract daily litros for the last n days."""
    results = []
    days_es = ["Lun", "Mar", "Mie", "Jue", "Vie", "Sab", "Dom"]

    for d in range(n_days):
        dt = datetime.now() - timedelta(days=d)
        date_str = dt.strftime("%Y-%m-%d")
        day_name = days_es[dt.weekday()]

        # Skip weekends
        if dt.weekday() >= 5:
            continue

        lines = search_read(models, uid, "account.move.line", [
            ["move_id.move_type", "=", "out_invoice"],
            ["move_id.state", "=", "posted"],
            ["move_id.invoice_date", "=", date_str],
            ["display_type", "=", "product"],
        ], ["quantity", "price_subtotal"], 500)

        litros = round(sum(l["quantity"] for l in lines))
        neto = sum(l["price_subtotal"] for l in lines)

        if litros > 0:
            results.append({
                "date": dt.strftime("%d%b").lower(),
                "day": day_name,
                "litros": litros,
                "neto": round(neto),
            })

    return results


def extract_bank_balances(models, uid):
    """Extract current bank/cash balances."""
    journals = search_read(models, uid, "account.journal", [
        ["type", "in", ["bank", "cash"]],
    ], ["name", "type", "default_account_id"])

    account_ids = [j["default_account_id"][0] for j in journals if j["default_account_id"]]

    # Use read_group to get balances
    balances = models.execute_kw(
        ODOO_DB, uid, ODOO_KEY,
        "account.move.line", "read_group",
        [[["account_id", "in", account_ids], ["parent_state", "=", "posted"]]],
        {"fields": ["balance:sum"], "groupby": ["account_id"], "lazy": True}
    )

    results = []
    for b in balances:
        name = b["account_id"][1] if b["account_id"] else "Unknown"
        # Clean up account name
        clean_name = name.split(" ", 2)[-1] if "." in name.split(" ")[0] else name
        results.append({
            "name": clean_name,
            "balance": round(b["balance"]),
        })

    return sorted(results, key=lambda x: -x["balance"])


def main():
    print("═══ RS Dashboard · Odoo Data Extraction ═══")
    print(f"Server: {ODOO_URL}")
    print(f"Database: {ODOO_DB}")
    print()

    models, uid = connect()

    print("\n📊 Extracting weekly data (12 weeks)...")
    weekly = extract_weekly(models, uid)

    print("\n📅 Extracting daily data (28 days)...")
    daily = extract_daily(models, uid)

    print("\n🏦 Extracting bank balances...")
    banks = extract_bank_balances(models, uid)
    total_cash = sum(b["balance"] for b in banks)
    print(f"  Total cash: ${total_cash:,.0f}")

    # Build output
    data = {
        "updated": datetime.now().isoformat(),
        "weeks": weekly,
        "daily": daily,
        "banks": banks,
        "total_cash": total_cash,
    }

    # Write JSON
    output_path = os.path.join(os.path.dirname(__file__), "data.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"\n✅ data.json written ({len(weekly)} weeks, {len(daily)} days, {len(banks)} banks)")
    print(f"   Path: {output_path}")


if __name__ == "__main__":
    main()
