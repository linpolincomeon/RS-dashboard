#!/usr/bin/env python3
"""
CEO Dashboard — Odoo Data Extractor
Extracts weekly (Thu-Wed) + receivables + CRM data.
Outputs ceo-data.json for the static dashboard on GitHub Pages.
"""
import xmlrpc.client
import json
import os
from datetime import datetime, timedelta

ODOO_URL = os.environ.get("ODOO_URL", "https://tomenergy.cl")
ODOO_DB = os.environ.get("ODOO_DB", "PRODUCCION")
ODOO_USER = os.environ.get("ODOO_USER", "p@tomenergy.cl")
ODOO_KEY = os.environ.get("ODOO_KEY", "f4188f3cbe069a9f5ce60325fa17a2c5333176d1")

# Weekly budget in litros (monthly / 4)
WEEKLY_BUDGET = {
    "2025-01": 159285, "2025-02": 161493, "2025-03": 168496,
    "2025-04": 211569, "2025-05": 150215, "2025-06": 112982,
    "2025-07": 179067, "2025-08": 182676, "2025-09": 184083,
    "2025-10": 214286, "2025-11": 207258, "2025-12": 212548,
    "2026-01": 266438, "2026-02": 272593, "2026-03": 283811,
    "2026-04": 326422, "2026-05": 258823, "2026-06": 216688,
    "2026-07": 276926, "2026-08": 271233, "2026-09": 263225,
    "2026-10": 338728, "2026-11": 348706, "2026-12": 438672,
}


def get_week_budget(start_date_str):
    key = start_date_str[:7]
    return WEEKLY_BUDGET.get(key, 270000)


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


# ── Week ranges: Thursday to Wednesday ──
def get_week_ranges(n_weeks=16):
    today = datetime.now()
    days_since_thu = (today.weekday() - 3) % 7
    this_thu = today - timedelta(days=days_since_thu)
    this_thu = this_thu.replace(hour=0, minute=0, second=0, microsecond=0)

    weeks = []
    for w in range(n_weeks):
        start = this_thu - timedelta(weeks=w)
        end = start + timedelta(days=6)
        label = f"{start.strftime('%d%b')}-{end.strftime('%d%b')}".lower()
        weeks.append({"start": start.strftime("%Y-%m-%d"),
                       "end": end.strftime("%Y-%m-%d"),
                       "label": label})
    return weeks


# ── WEEKLY EXTRACTION ──
def extract_weekly(models, uid):
    print("Extracting weekly data (16 weeks, Thu-Wed)...")
    weeks = get_week_ranges(16)
    results = []

    for i, wd in enumerate(weeks):
        print(f"  Week {i+1}/16: {wd['label']}...", end=" ")

        # Customer invoices
        invoices = sr(models, uid, "account.move", [
            ["move_type", "=", "out_invoice"],
            ["state", "=", "posted"],
            ["invoice_date", ">=", wd["start"]],
            ["invoice_date", "<=", wd["end"]],
        ], ["amount_total", "amount_untaxed", "margin_zone", "partner_id"])

        # Credit notes to subtract
        refunds = sr(models, uid, "account.move", [
            ["move_type", "=", "out_refund"],
            ["state", "=", "posted"],
            ["invoice_date", ">=", wd["start"]],
            ["invoice_date", "<=", wd["end"]],
        ], ["amount_total", "amount_untaxed"])

        ventas = sum(x["amount_total"] for x in invoices) - sum(r["amount_total"] for r in refunds)
        neto = sum(x["amount_untaxed"] for x in invoices) - sum(r["amount_untaxed"] for r in refunds)
        clientes = len(set(x["partner_id"][0] for x in invoices if x["partner_id"]))

        # Weighted average margin_zone
        sum_mn, sum_n = 0, 0
        for inv in invoices:
            mz = inv.get("margin_zone") or 0
            au = inv.get("amount_untaxed") or 0
            if mz and au > 0:
                sum_mn += mz * au
                sum_n += au
        margin = sum_mn / sum_n if sum_n > 0 else 0

        # Invoice lines for litros
        lines = sr(models, uid, "account.move.line", [
            ["move_id.move_type", "=", "out_invoice"],
            ["move_id.state", "=", "posted"],
            ["move_id.invoice_date", ">=", wd["start"]],
            ["move_id.invoice_date", "<=", wd["end"]],
            ["display_type", "=", "product"],
        ], ["quantity", "price_subtotal"], 5000)

        # Refund lines to subtract
        ref_lines = sr(models, uid, "account.move.line", [
            ["move_id.move_type", "=", "out_refund"],
            ["move_id.state", "=", "posted"],
            ["move_id.invoice_date", ">=", wd["start"]],
            ["move_id.invoice_date", "<=", wd["end"]],
            ["display_type", "=", "product"],
        ], ["quantity", "price_subtotal"], 2000)

        litros = round(sum(l["quantity"] for l in lines) - sum(l["quantity"] for l in ref_lines))
        neto_lineas = sum(l["price_subtotal"] for l in lines) - sum(l["price_subtotal"] for l in ref_lines)
        precio = round(neto_lineas / litros) if litros > 0 else 0

        # Recaudación from bank statement lines (Banco de Chile = journal 112)
        # This is the correct source — account.payment only has cheques en cartera
        RECAUD_JOURNALS = [112]  # Banco de Chile
        bsl = sr(models, uid, "account.bank.statement.line", [
            ["date", ">=", wd["start"]],
            ["date", "<=", wd["end"]],
            ["amount", ">", 0],
            ["journal_id", "in", RECAUD_JOURNALS],
        ], ["amount", "payment_ref", "journal_id"], 2000)

        cheques, transf, factoring = 0, 0, 0
        for line in bsl:
            ref = (line.get("payment_ref") or "").lower()
            if "fingo" in ref or "factoring" in ref or "factor" in ref:
                factoring += line["amount"]
            elif "dep.cheq" in ref or "dep. docto" in ref:
                cheques += line["amount"]
            else:
                transf += line["amount"]
        recaud = sum(line["amount"] for line in bsl)

        # Vendor bills (compras ENAP)
        compras = sr(models, uid, "account.move", [
            ["move_type", "=", "in_invoice"],
            ["state", "=", "posted"],
            ["invoice_date", ">=", wd["start"]],
            ["invoice_date", "<=", wd["end"]],
        ], ["amount_total"], 500)
        compras_enap = sum(c["amount_total"] for c in compras)

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
            "nc": len(refunds),
            "clientes": clientes,
            "recaud": round(recaud),
            "cheques": round(cheques),
            "transf": round(transf),
            "factoring": round(factoring),
            "compras_enap": round(compras_enap),
            "ppto": ppto,
            "parcial": i == 0,
        })
        print(f"{len(invoices)} fact, {litros}L, margin {margin:.2%}")

    return results


# ── DAILY SALES (last 16 business days) ──
def extract_daily(models, uid):
    print("Extracting daily sales (16 business days)...")
    results = []
    days_es = ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"]
    count = 0
    d = 0
    while count < 16:
        dt = datetime.now() - timedelta(days=d)
        d += 1
        if dt.weekday() >= 5:  # skip weekends
            continue
        date_str = dt.strftime("%Y-%m-%d")

        lines = sr(models, uid, "account.move.line", [
            ["move_id.move_type", "=", "out_invoice"],
            ["move_id.state", "=", "posted"],
            ["move_id.invoice_date", "=", date_str],
            ["display_type", "=", "product"],
        ], ["quantity", "price_subtotal"], 1000)

        litros = round(sum(l["quantity"] for l in lines))
        neto = round(sum(l["price_subtotal"] for l in lines))

        results.append({
            "date": dt.strftime("%d%b").lower(),
            "day": days_es[dt.weekday()],
            "litros": litros,
            "neto": neto,
        })
        count += 1
        print(f"  {date_str} ({days_es[dt.weekday()]}): {litros}L")

    return results


# ── BANK BALANCES ──
def extract_bank_balances(models, uid):
    print("Extracting bank balances...")
    journals = sr(models, uid, "account.journal", [
        ["type", "in", ["bank", "cash"]],
    ], ["name", "type", "default_account_id"])

    account_ids = [j["default_account_id"][0] for j in journals if j["default_account_id"]]

    balances = models.execute_kw(
        ODOO_DB, uid, ODOO_KEY,
        "account.move.line", "read_group",
        [[["account_id", "in", account_ids], ["parent_state", "=", "posted"]]],
        {"fields": ["balance:sum"], "groupby": ["account_id"], "lazy": True}
    )

    results = []
    for b in balances:
        name = b["account_id"][1] if b["account_id"] else "Unknown"
        clean_name = name.split(" ", 2)[-1] if "." in name.split(" ")[0] else name
        results.append({"name": clean_name, "balance": round(b["balance"])})

    return sorted(results, key=lambda x: -x["balance"])


# ── RECEIVABLES ──
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


# ── CRM ──
def extract_crm(models, uid):
    print("Extracting CRM pipeline...")
    total = s_count(models, uid, "crm.lead", [])
    open_l = s_count(models, uid, "crm.lead", [["active", "=", True], ["stage_id.is_won", "=", False]])
    won = s_count(models, uid, "crm.lead", [["stage_id.is_won", "=", True]])
    thirty_ago = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    new_30 = s_count(models, uid, "crm.lead", [["create_date", ">=", thirty_ago]])

    stages = sr(models, uid, "crm.stage", [], ["name", "sequence"], limit=20)
    by_stage = []
    for stg in sorted(stages, key=lambda x: x.get("sequence", 0)):
        c = s_count(models, uid, "crm.lead", [["stage_id", "=", stg["id"]], ["active", "=", True]])
        if c > 0:
            by_stage.append({"stage": stg["name"], "count": c})

    return {"total": total, "open": open_l, "won": won, "new_30d": new_30, "by_stage": by_stage}


# ── MAIN ──
def main():
    print("=== CEO Dashboard · Odoo Extraction ===")
    models, uid = connect()

    weekly = extract_weekly(models, uid)
    daily = extract_daily(models, uid)
    banks = extract_bank_balances(models, uid)
    total_cash = sum(b["balance"] for b in banks)
    receivables = extract_receivables(models, uid)
    crm = extract_crm(models, uid)

    data = {
        "updated": datetime.now().isoformat(),
        "weeks": weekly,
        "daily": daily,
        "banks": banks,
        "total_cash": total_cash,
        "receivables": receivables,
        "crm": crm,
    }

    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ceo-data.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"\nceo-data.json written OK ({len(weekly)} weeks, {len(banks)} banks)")


if __name__ == "__main__":
    main()
