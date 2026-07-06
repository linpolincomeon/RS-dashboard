#!/usr/bin/env python3
"""
CEO Dashboard — Odoo Data Extractor
Extracts weekly (Thu-Wed) sales, recaudación, compras, cheques, margins.
Outputs ceo-data.json for the static dashboard on GitHub Pages.

Data sources confirmed with TomEnergy accounting team (April 2026):
- Recaudación: account.bank.statement.line (Banco de Chile, journal 112)
- Cheques en cartera: account.move.line on journal 114
- Compras: account.move in_invoice for ENAP + ADQUIM + ADGREEN
- Margen contado/crédito: split by payment_term_id (1 day/prepago vs 15+ days)
- Cotizaciones canceladas: sale.order state=cancel
- Visitas: crm.lead in stage "Ruta" updated in the week
- Precio promedio: price_total / quantity on diesel B1 lines (bruto con IVA+IEC)
"""
import xmlrpc.client
import json
import os
from datetime import datetime, timedelta
from calendar import monthrange

ODOO_URL = os.environ.get("ODOO_URL", "https://tomenergy.cl")
ODOO_DB = os.environ.get("ODOO_DB", "PRODUCCION")
ODOO_USER = os.environ.get("ODOO_USER", "p@tomenergy.cl")
ODOO_KEY = os.environ.get("ODOO_KEY", "f4188f3cbe069a9f5ce60325fa17a2c5333176d1")

# Known IDs
BANCO_CHILE_JOURNAL = 112
CHEQUES_CARTERA_JOURNAL = 114
DIESEL_B1_PRODUCT = 14
ENAP_PARTNER = 5667
ADQUIM_PARTNER = 15299

# Monthly budgets — total, retail, volumen (from RS tab Google Sheet)
# Weeks per month for converting to weekly: ~4.33 but we use actual weeks
MONTHLY_BUDGET = {
    # 2024
    "2024-01": {"total": 578139, "retail": 0, "volumen": 0},
    "2024-02": {"total": 573930, "retail": 0, "volumen": 0},
    "2024-03": {"total": 587495, "retail": 0, "volumen": 0},
    "2024-04": {"total": 722391, "retail": 0, "volumen": 0},
    "2024-05": {"total": 470135, "retail": 0, "volumen": 0},
    "2024-06": {"total": 307000, "retail": 0, "volumen": 0},
    "2024-07": {"total": 516000, "retail": 0, "volumen": 0},
    "2024-08": {"total": 491306, "retail": 0, "volumen": 0},
    "2024-09": {"total": 459331, "retail": 0, "volumen": 0},
    "2024-10": {"total": 581133, "retail": 0, "volumen": 0},
    "2024-11": {"total": 470872, "retail": 0, "volumen": 0},
    "2024-12": {"total": 453294, "retail": 0, "volumen": 0},
    # 2025
    "2025-01": {"total": 595457, "retail": 0, "volumen": 0},
    "2025-02": {"total": 603713, "retail": 0, "volumen": 0},
    "2025-03": {"total": 629891, "retail": 0, "volumen": 0},
    "2025-04": {"total": 790911, "retail": 0, "volumen": 0},
    "2025-05": {"total": 561553, "retail": 0, "volumen": 0},
    "2025-06": {"total": 422363, "retail": 0, "volumen": 0},
    "2025-07": {"total": 669410, "retail": 0, "volumen": 0},
    "2025-08": {"total": 682900, "retail": 0, "volumen": 0},
    "2025-09": {"total": 688159, "retail": 0, "volumen": 0},
    "2025-10": {"total": 801070, "retail": 0, "volumen": 0},
    "2025-11": {"total": 774796, "retail": 0, "volumen": 0},
    "2025-12": {"total": 794572, "retail": 0, "volumen": 0},
    # 2026
    "2026-01": {"total": 951565, "retail": 742221, "volumen": 209344},
    "2026-02": {"total": 973546, "retail": 759366, "volumen": 214180},
    "2026-03": {"total": 1013609, "retail": 770343, "volumen": 243266},
    "2026-04": {"total": 1165794, "retail": 862687, "volumen": 303106},
    "2026-05": {"total": 924369, "retail": 665545, "volumen": 258823},
    "2026-06": {"total": 773884, "retail": 541719, "volumen": 232165},
    "2026-07": {"total": 989023, "retail": 672535, "volumen": 316487},
    "2026-08": {"total": 968691, "retail": 639336, "volumen": 329355},
    "2026-09": {"total": 940090, "retail": 601658, "volumen": 338432},
    "2026-10": {"total": 1209742, "retail": 871309, "volumen": 338432},
    "2026-11": {"total": 1245378, "retail": 906946, "volumen": 338432},
    "2026-12": {"total": 1566686, "retail": 1228254, "volumen": 338432},
}

# Factoring threshold — ignore small transfers that match pattern
FACTORING_MIN_AMOUNT = 1_000_000


def get_week_budget(start_date_str):
    """Convert monthly budget to weekly (÷ 4.33 weeks/month)."""
    key = start_date_str[:7]
    mb = MONTHLY_BUDGET.get(key, {"total": 270000, "retail": 0, "volumen": 0})
    wks = 4.33  # avg weeks per month
    return {
        "total": round(mb["total"] / wks),
        "retail": round(mb["retail"] / wks),
        "volumen": round(mb["volumen"] / wks),
    }


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


# ── Lookup helpers (run once at startup) ──
def lookup_supplier_ids(models, uid):
    """Find ADGREEN partner_id dynamically."""
    adgreen = sr(models, uid, "res.partner",
                 [["name", "ilike", "adgreen"]], ["id", "name"], limit=3)
    adgreen_id = adgreen[0]["id"] if adgreen else None
    ids = [ENAP_PARTNER, ADQUIM_PARTNER]
    if adgreen_id:
        ids.append(adgreen_id)
        print(f"  Suppliers: ENAP={ENAP_PARTNER}, ADQUIM={ADQUIM_PARTNER}, ADGREEN={adgreen_id}")
    else:
        print(f"  Suppliers: ENAP={ENAP_PARTNER}, ADQUIM={ADQUIM_PARTNER} (ADGREEN not found)")
    return ids


def lookup_payment_terms(models, uid):
    """Get all payment terms with name, days, and contado flag."""
    import re
    terms = sr(models, uid, "account.payment.term", [], ["id", "name"], limit=50)
    term_map = {}   # id -> {name, days, is_contado, label}
    contado_ids = []
    for t in terms:
        tid = t["id"]
        name = (t["name"] or "")
        low = name.lower()
        # Extract days from name
        m = re.search(r"(\d+)\s*d", low)
        if m:
            days = int(m.group(1))
        elif "prepago" in low or "inmediato" in low or "contado" in low:
            days = 0
        else:
            days = 30  # default assumption
        is_contado = days <= 1 or "prepago" in low or "inmediato" in low or "contado" in low
        if is_contado:
            contado_ids.append(tid)
        # Normalize label for grouping
        if days == 0 or "prepago" in low:
            label = "Prepago"
        elif days == 1:
            label = "1 Día"
        else:
            label = f"{days} Días"
        term_map[tid] = {"name": name, "days": days, "is_contado": is_contado, "label": label}
    print(f"  Payment terms: {len(term_map)} found, contado: {contado_ids}")
    return term_map, contado_ids


def lookup_ruta_stage_id(models, uid):
    """Find CRM stage 'Ruta'."""
    stages = sr(models, uid, "crm.stage", [["name", "ilike", "ruta"]], ["id", "name"], limit=3)
    if stages:
        print(f"  Ruta stage: id={stages[0]['id']}")
        return stages[0]["id"]
    return None


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


# ── Classify bank statement line into cheque / factoring / transfer ──
def classify_bsl(ref, amount):
    """
    Cheques: ref contains dep.cheq, dep. docto (but NOT 'efectivo')
    Factoring: Fingo, Go Capital, or Security-style 'Transferencia De Otro Banco Via Spav'
               (only if amount >= FACTORING_MIN_AMOUNT for Security)
    Transfer: everything else
    """
    r = (ref or "").lower()
    if ("dep.cheq" in r or "dep. docto" in r) and "efectivo" not in r:
        return "cheques"
    if "fingo" in r or "factoring" in r or "go capital" in r:
        return "factoring"
    if "otro banco via spav" in r and amount >= FACTORING_MIN_AMOUNT:
        return "factoring"
    return "transf"


# ── WEEKLY EXTRACTION ──
def extract_weekly(models, uid, supplier_ids, contado_term_ids, ruta_stage_id, term_map=None):
    print("Extracting weekly data (16 weeks, Thu-Wed)...")
    weeks = get_week_ranges(16)
    results = []

    for i, wd in enumerate(weeks):
        print(f"  Week {i+1}/16: {wd['label']}...", end=" ")

        # ── Customer invoices ──
        invoices = sr(models, uid, "account.move", [
            ["move_type", "=", "out_invoice"],
            ["state", "=", "posted"],
            ["invoice_date", ">=", wd["start"]],
            ["invoice_date", "<=", wd["end"]],
        ], ["amount_total", "amount_untaxed", "margin_zone",
            "partner_id", "invoice_payment_term_id"])

        # Credit notes
        refunds = sr(models, uid, "account.move", [
            ["move_type", "=", "out_refund"],
            ["state", "=", "posted"],
            ["invoice_date", ">=", wd["start"]],
            ["invoice_date", "<=", wd["end"]],
        ], ["amount_total", "amount_untaxed", "partner_id"])

        ventas = sum(x["amount_total"] for x in invoices) - sum(r["amount_total"] for r in refunds)
        neto = sum(x["amount_untaxed"] for x in invoices) - sum(r["amount_untaxed"] for r in refunds)
        clientes = len(set(x["partner_id"][0] for x in invoices if x["partner_id"]))

        # ── Lookup is_volume_client for partners in this week ──
        partner_ids_week = list(set(x["partner_id"][0] for x in invoices if x.get("partner_id")))
        volume_partner_ids = set()
        for poff in range(0, len(partner_ids_week), 200):
            batch = partner_ids_week[poff:poff + 200]
            partners = sr(models, uid, "res.partner", [["id", "in", batch]],
                           ["id", "is_volume_client"], limit=200)
            for p in partners:
                if p.get("is_volume_client"):
                    volume_partner_ids.add(p["id"])

        # Build invoice → partner map and invoice → is_volume map
        inv_partner = {}  # inv_id -> partner_id
        inv_is_vol = {}   # inv_id -> bool
        for inv in invoices:
            pid = inv["partner_id"][0] if inv.get("partner_id") else None
            inv_partner[inv["id"]] = pid
            inv_is_vol[inv["id"]] = pid in volume_partner_ids

        # Build refund → is_volume map (needed to split NC correctly into retail/vol)
        ref_is_vol = {}
        for r in refunds:
            pid = r["partner_id"][0] if r.get("partner_id") else None
            ref_is_vol[r["id"]] = pid in volume_partner_ids

        # ── Margin: overall + retail vs volumen (by is_volume_client) + per payment term ──
        sum_mn, sum_n = 0, 0
        sum_mn_ret, sum_n_ret = 0, 0    # retail (not volume)
        sum_mn_vol, sum_n_vol = 0, 0    # volumen (is_volume_client)
        fact_contado, fact_credito = 0, 0
        # Per-term tracking: {label: {sum_mn, sum_n, count, days}}
        by_term = {}
        for inv in invoices:
            mz = inv.get("margin_zone") or 0
            au = inv.get("amount_untaxed") or 0
            is_vol = inv_is_vol.get(inv["id"], False)
            term_id = inv.get("invoice_payment_term_id")
            tid = term_id[0] if term_id else None
            is_contado = tid in contado_term_ids
            # Resolve term info
            tinfo = (term_map or {}).get(tid, {"label": "Sin plazo", "days": 30})
            tlabel = tinfo["label"]
            tdays = tinfo["days"]
            if is_contado:
                fact_contado += 1
            else:
                fact_credito += 1
            if tlabel not in by_term:
                by_term[tlabel] = {"sum_mn": 0, "sum_n": 0, "count": 0, "days": tdays}
            by_term[tlabel]["count"] += 1
            if not mz or au <= 0:
                continue
            sum_mn += mz * au
            sum_n += au
            by_term[tlabel]["sum_mn"] += mz * au
            by_term[tlabel]["sum_n"] += au
            if is_vol:
                sum_mn_vol += mz * au
                sum_n_vol += au
            else:
                sum_mn_ret += mz * au
                sum_n_ret += au

        margin = sum_mn / sum_n if sum_n > 0 else 0
        margin_retail = sum_mn_ret / sum_n_ret if sum_n_ret > 0 else 0
        margin_volumen = sum_mn_vol / sum_n_vol if sum_n_vol > 0 else 0
        # Build per-term output
        margin_by_term = {}
        for lbl, bt in sorted(by_term.items(), key=lambda x: x[1]["days"]):
            m = bt["sum_mn"] / bt["sum_n"] if bt["sum_n"] > 0 else 0
            # Normalización: margin + (30 - days)/30 percentage points
            adj = (30 - bt["days"]) / 30 / 100  # convert pp to ratio
            norm = m + adj
            margin_by_term[lbl] = {
                "margin": round(m, 5),
                "normalizado": round(norm, 5),
                "count": bt["count"],
                "days": bt["days"],
            }

        # ── Invoice lines for litros + precio bruto ──
        lines = sr(models, uid, "account.move.line", [
            ["move_id.move_type", "=", "out_invoice"],
            ["move_id.state", "=", "posted"],
            ["move_id.invoice_date", ">=", wd["start"]],
            ["move_id.invoice_date", "<=", wd["end"]],
            ["display_type", "=", "product"],
        ], ["quantity", "price_subtotal", "price_total", "product_id", "move_id"], 5000)

        ref_lines = sr(models, uid, "account.move.line", [
            ["move_id.move_type", "=", "out_refund"],
            ["move_id.state", "=", "posted"],
            ["move_id.invoice_date", ">=", wd["start"]],
            ["move_id.invoice_date", "<=", wd["end"]],
            ["display_type", "=", "product"],
        ], ["quantity", "price_subtotal", "move_id"], 2000)

        litros = round(sum(l["quantity"] for l in lines) - sum(l["quantity"] for l in ref_lines))
        neto_lineas = sum(l["price_subtotal"] for l in lines) - sum(l["price_subtotal"] for l in ref_lines)
        precio_neto = round(neto_lineas / litros) if litros > 0 else 0

        # Litros split: retail vs volumen (by is_volume_client on partner)
        litros_retail, litros_volumen = 0, 0
        for l in lines:
            mid = l["move_id"][0] if l.get("move_id") else None
            if inv_is_vol.get(mid, False):
                litros_volumen += l["quantity"]
            else:
                litros_retail += l["quantity"]
        # Subtract NC lines from retail/vol split
        for l in ref_lines:
            mid = l["move_id"][0] if l.get("move_id") else None
            if ref_is_vol.get(mid, False):
                litros_volumen -= l["quantity"]
            else:
                litros_retail -= l["quantity"]
        litros_retail = round(litros_retail)
        litros_volumen = round(litros_volumen)

        # Precio bruto promedio (IVA+IEC) — solo diesel B1
        b1_lines = [l for l in lines
                     if l.get("product_id") and l["product_id"][0] == DIESEL_B1_PRODUCT
                     and l.get("quantity", 0) > 0]
        b1_total = sum(l.get("price_total", 0) for l in b1_lines)
        b1_qty = sum(l["quantity"] for l in b1_lines)
        precio_bruto = round(b1_total / b1_qty) if b1_qty > 0 else 0

        # ── Recaudación from BSL Banco de Chile ──
        bsl = sr(models, uid, "account.bank.statement.line", [
            ["date", ">=", wd["start"]],
            ["date", "<=", wd["end"]],
            ["amount", ">", 0],
            ["journal_id", "=", BANCO_CHILE_JOURNAL],
        ], ["amount", "payment_ref"], 2000)

        cheques, transf, factoring = 0, 0, 0
        for line in bsl:
            cat = classify_bsl(line.get("payment_ref"), line["amount"])
            if cat == "cheques":
                cheques += line["amount"]
            elif cat == "factoring":
                factoring += line["amount"]
            else:
                transf += line["amount"]
        recaud = sum(line["amount"] for line in bsl)

        # ── Compras (ENAP + ADQUIM + ADGREEN) ──
        compras = sr(models, uid, "account.move", [
            ["move_type", "=", "in_invoice"],
            ["state", "=", "posted"],
            ["invoice_date", ">=", wd["start"]],
            ["invoice_date", "<=", wd["end"]],
            ["partner_id", "in", supplier_ids],
        ], ["amount_total_in_currency_signed"], 500)
        compras_total = sum(abs(c.get("amount_total_in_currency_signed", 0)) for c in compras)

        # ── Cheques en cartera ──
        # "después de subir" = all cheques in cartera journal up to end of week
        cheq_cartera = sr(models, uid, "account.move.line", [
            ["journal_id", "=", CHEQUES_CARTERA_JOURNAL],
            ["parent_state", "=", "posted"],
            ["date", "<=", wd["end"]],
        ], ["debit", "credit"], 2000)
        cheq_cartera_saldo = sum(l["debit"] - l["credit"] for l in cheq_cartera)

        # "recibidos esta semana" = cheques entered in cartera during this week
        cheq_recibidos = sr(models, uid, "account.move.line", [
            ["journal_id", "=", CHEQUES_CARTERA_JOURNAL],
            ["parent_state", "=", "posted"],
            ["date", ">=", wd["start"]],
            ["date", "<=", wd["end"]],
            ["debit", ">", 0],
        ], ["debit"], 500)
        cheq_recibidos_total = sum(l["debit"] for l in cheq_recibidos)

        # ── Cotizaciones canceladas ──
        cotiz_cancel = s_count(models, uid, "sale.order", [
            ["state", "=", "cancel"],
            ["date_order", ">=", wd["start"]],
            ["date_order", "<=", wd["end"] + " 23:59:59"],
        ])

        # ── Visitas (leads in stage Ruta updated this week) ──
        visitas = 0
        if ruta_stage_id:
            visitas = s_count(models, uid, "crm.lead", [
                ["stage_id", "=", ruta_stage_id],
                ["write_date", ">=", wd["start"]],
                ["write_date", "<=", wd["end"] + " 23:59:59"],
            ])

        # ── Clientes nuevos (first invoice ever in this week) ──
        clientes_nuevos = 0
        seen_partners = set()
        for inv in invoices:
            pid = inv["partner_id"][0] if inv.get("partner_id") else None
            if not pid or pid in seen_partners:
                continue
            seen_partners.add(pid)
            prev = s_count(models, uid, "account.move", [
                ["move_type", "=", "out_invoice"],
                ["state", "=", "posted"],
                ["partner_id", "=", pid],
                ["invoice_date", "<", wd["start"]],
            ])
            if prev == 0:
                clientes_nuevos += 1

        wb = get_week_budget(wd["start"])

        results.append({
            "label": wd["label"],
            "start": wd["start"],
            "end": wd["end"],
            "ventas": round(ventas),
            "neto": round(neto),
            "litros": litros,
            "litros_retail": litros_retail,
            "litros_volumen": litros_volumen,
            "precio_neto": precio_neto,
            "precio_bruto": precio_bruto,
            "margin": round(margin, 5),
            "margin_retail": round(margin_retail, 5),
            "margin_volumen": round(margin_volumen, 5),
            "facturas": len(invoices),
            "nc": len(refunds),
            "clientes": clientes,
            "recaud": round(recaud),
            "cheques": round(cheques),
            "transf": round(transf),
            "factoring": round(factoring),
            "compras_odoo": round(compras_total),
            "cheq_cartera_saldo": round(cheq_cartera_saldo),
            "cheq_recibidos": round(cheq_recibidos_total),
            "facturas_contado": fact_contado,
            "facturas_credito": fact_credito,
            "margin_by_term": margin_by_term,
            "cotiz_canceladas": cotiz_cancel,
            "visitas": visitas,
            "clientes_nuevos": clientes_nuevos,
            "ppto": wb["total"],
            "ppto_retail": wb["retail"],
            "ppto_volumen": wb["volumen"],
            "parcial": i == 0,
        })
        print(f"{len(invoices)} fact ({fact_contado}c/{fact_credito}cr), {litros}L, margin {margin:.2%}, recaud {recaud/1e6:.1f}M, {clientes_nuevos} nuevos")

    return results


# ── DAILY SALES (last 16 business days) ──
def extract_daily(models, uid):
    print("Extracting daily sales (16 business days)...")
    results = []
    days_es = ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"]
    count, d = 0, 0
    while count < 16:
        dt = datetime.now() - timedelta(days=d)
        d += 1
        if dt.weekday() >= 5:
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
            "litros": litros, "neto": neto,
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


# ── SLA DELIVERY (sale.order + account.move) ──
# 6 zones: Talca, San Fernando, Curicó, Chillán, Rancagua, VI Costa
VALID_ZONES = {"Talca", "San Fernando", "Curicó", "Chillán", "Rancagua", "VI Costa"}


def extract_sla(models, uid, weeks):
    """Delivery SLA per zone per week — todas las semanas del selector.
    On time = invoice_date de la primera factura válida == shipping_date (mismo día).
    Para la semana actual: excluye pedidos con shipping_date > hoy (aún en curso).
    Ignora facturas con invoice_date < 2020-01-01 (datos corruptos)."""
    print("Extracting SLA delivery data (all weeks)...")
    today_str = datetime.now().strftime("%Y-%m-%d")
    sla_data = []

    for i, wd in enumerate(weeks):
        # Para semana actual limitar al día de hoy; semanas pasadas usar fin de semana
        cutoff = today_str if i == 0 else wd["end"]

        orders = sr(models, uid, "sale.order", [
            ["state", "in", ["sale", "done"]],
            ["shipping_date", ">=", wd["start"]],
            ["shipping_date", "<=", cutoff],
        ], ["id", "name", "shipping_date", "partner_id", "delivery_zone_id"], 2000)

        if not orders:
            sla_data.append({"label": wd["label"], "zones": {}})
            continue

        order_names = [o["name"] for o in orders]
        invoices = sr(models, uid, "account.move", [
            ["move_type", "=", "out_invoice"],
            ["state", "=", "posted"],
            ["invoice_origin", "in", order_names],
        ], ["invoice_origin", "invoice_date"], 5000)

        # Primera factura válida por orden (ignorar fechas corruptas < 2020)
        first_invoice = {}
        for inv in invoices:
            orig = inv.get("invoice_origin", "")
            idate = inv.get("invoice_date", "")
            if orig and idate and idate >= "2020-01-01":
                if orig not in first_invoice or idate < first_invoice[orig]:
                    first_invoice[orig] = idate

        zone_stats = {}
        for o in orders:
            shipping = o.get("shipping_date", "")
            if not shipping:
                continue
            shipping_day = shipping[:10]
            dz = o.get("delivery_zone_id")
            zone = dz[1] if dz else "Sin Zona"
            if zone not in zone_stats:
                zone_stats[zone] = {"total": 0, "on_time": 0, "late_clients": []}
            zone_stats[zone]["total"] += 1
            inv_date = first_invoice.get(o["name"])
            if inv_date and inv_date[:10] == shipping_day:
                zone_stats[zone]["on_time"] += 1
            else:
                pname = o["partner_id"][1] if o.get("partner_id") else "N/A"
                inv_display = inv_date[:10] if inv_date else "sin factura"
                zone_stats[zone]["late_clients"].append(
                    f"{pname} (prom: {shipping_day}, fact: {inv_display})")

        week_sla = {}
        for zone, st in sorted(zone_stats.items()):
            pct = round(st["on_time"] / st["total"] * 100, 2) if st["total"] > 0 else 100
            week_sla[zone] = {
                "total": st["total"],
                "on_time": st["on_time"],
                "late": st["total"] - st["on_time"],
                "pct": pct,
                "late_clients": st["late_clients"],
            }
        sla_data.append({"label": wd["label"], "zones": week_sla})
        total_p = sum(s["total"] for s in week_sla.values())
        total_ot = sum(s["on_time"] for s in week_sla.values())
        print(f"  {wd['label']}: {total_p} orders, {total_ot} on time")
    return sla_data


# ── CHURN: Clasificación correcta por facturación ──
# Definiciones operativas TomEnergy:
#   Durmiente: superó freq×1.5 (freq<30d) o freq×1.3 (freq≥30d), pero <270 días sin factura
#   Perdido:   sin factura 270+ días (9 meses)
#   Churn %:   perdidos NUEVOS este mes / clientes activos mes anterior
#
# Frecuencia se calcula desde historial real de facturas (últimos 3 meses activos),
# no desde campo frecuencia_facturacion (que puede estar vacío o desactualizado).

LOST_THRESHOLD_DAYS = 270   # 9 meses → Perdido
LOOKBACK_FREQ_MONTHS = 3    # meses para calcular frecuencia promedio
LOOKBACK_ALL_MONTHS = 30    # meses para traer todo el historial (perdidos incluidos)


def compute_freq_days(invoice_dates_sorted):
    """
    Calcula frecuencia promedio de compra en días desde lista de fechas ordenadas (str YYYY-MM-DD).
    Necesita al menos 2 facturas. Devuelve None si no hay suficientes datos.
    """
    if len(invoice_dates_sorted) < 2:
        return None
    dts = [datetime.strptime(d, "%Y-%m-%d") for d in invoice_dates_sorted]
    gaps = [(dts[i+1] - dts[i]).days for i in range(len(dts)-1)]
    return round(sum(gaps) / len(gaps))


def dormant_threshold(freq_days):
    """
    Umbral de días sin compra para clasificar como Durmiente.
    freq < 30d → freq × 1.5
    freq ≥ 30d → freq × 1.3
    """
    if freq_days < 30:
        return freq_days * 1.5
    return freq_days * 1.3


def classify_client(days_since, freq_days):
    """
    Devuelve 'activo', 'durmiente', o 'perdido'.
    days_since: días desde última factura
    freq_days: frecuencia promedio en días (puede ser None)
    """
    if days_since >= LOST_THRESHOLD_DAYS:
        return "perdido"
    if freq_days is not None and days_since > dormant_threshold(freq_days):
        return "durmiente"
    return "activo"


def extract_churn(models, uid):
    """
    Churn con definiciones correctas TomEnergy:
    - Durmiente: superó umbral de frecuencia pero <270 días sin factura
    - Perdido: 270+ días sin factura (churn real)
    - Churn %: perdidos nuevos este mes / activos mes anterior
    - Rescate durmiente: fidelización (Comber Sigall)
    - Rescate perdido: ejecutivo → cuenta como cliente nuevo
    """
    import re
    print("Extracting Churn (definiciones correctas TomEnergy)...")
    today = datetime.now()
    today_str = today.strftime("%Y-%m-%d")

    # ── 1. Traer TODO el historial necesario ──
    # Para detectar perdidos (270d) necesitamos hasta 30 meses atrás
    lookback_start = (today - timedelta(days=LOOKBACK_ALL_MONTHS * 31)).strftime("%Y-%m-%d")

    all_invs = fetch_all(models, uid, "account.move",
        [["move_type", "=", "out_invoice"],
         ["state", "=", "posted"],
         ["invoice_date", ">=", lookback_start]],
        ["partner_id", "invoice_date"])

    # ── 2. Construir historial por partner ──
    partner_dates = {}   # pid -> sorted list of invoice dates (str)
    partner_name = {}    # pid -> name
    for inv in all_invs:
        if not inv.get("partner_id"):
            continue
        pid = inv["partner_id"][0]
        name = inv["partner_id"][1] if isinstance(inv["partner_id"], (list, tuple)) else ""
        if "predeterminado" in name.lower():
            continue
        dt = inv.get("invoice_date", "")
        if not dt:
            continue
        partner_name[pid] = name
        partner_dates.setdefault(pid, [])
        partner_dates[pid].append(dt)

    for pid in partner_dates:
        partner_dates[pid] = sorted(set(partner_dates[pid]))

    print(f"  {len(partner_dates)} partners con historial de facturas")

    # ── 3. Para frecuencia: solo facturas en últimos 3 meses ──
    freq_cutoff = (today - timedelta(days=LOOKBACK_FREQ_MONTHS * 31)).strftime("%Y-%m-%d")

    partner_freq = {}  # pid -> freq_days (o None)
    for pid, dates in partner_dates.items():
        recent = [d for d in dates if d >= freq_cutoff]
        partner_freq[pid] = compute_freq_days(recent)

    # ── 4. Clasificar todos los partners ──
    dormant_list = []
    lost_list = []

    for pid, dates in partner_dates.items():
        last_date = dates[-1]
        days_since = (today - datetime.strptime(last_date, "%Y-%m-%d")).days
        freq = partner_freq.get(pid)
        cls = classify_client(days_since, freq)
        name = partner_name.get(pid, "")

        entry = {
            "partner_id": pid,
            "name": name,
            "last_invoice": last_date,
            "days_since": days_since,
            "freq_days": freq,
            "threshold": round(dormant_threshold(freq), 1) if freq else None,
        }

        if cls == "durmiente":
            dormant_list.append(entry)
        elif cls == "perdido":
            lost_list.append(entry)

    dormant_list.sort(key=lambda x: -x["days_since"])
    # Perdidos: ascendente — los recién perdidos (270d, 271d...) primero,
    # para que el truncado [:50] al escribir el JSON conserve los rescatables
    # y no los fósiles de 700+ días.
    lost_list.sort(key=lambda x: x["days_since"])

    print(f"  Durmientes: {len(dormant_list)}, Perdidos: {len(lost_list)}")

    # ── 5. Churn % mensual (últimos 6 meses) ──
    # Churn % = perdidos NUEVOS en mes M / activos al inicio del mes M
    # "Perdido nuevo en M" = cruzó 270d dentro de ese mes calendario
    print("  Calculando churn rate mensual...")
    churn_history = []
    for m_offset in range(6):
        ref = today.replace(day=1)
        for _ in range(m_offset):
            ref = (ref - timedelta(days=1)).replace(day=1)
        m_start = ref
        m_end = (ref + timedelta(days=32)).replace(day=1) - timedelta(days=1)
        m_label = ref.strftime("%b %Y")

        m_new_lost = 0
        m_active_start = 0

        for pid, dates in partner_dates.items():
            last_date = dates[-1]
            ld = datetime.strptime(last_date, "%Y-%m-%d")

            # ¿Tenía factura el mes anterior (activo al inicio de M)?
            prev_month_end = m_start - timedelta(days=1)
            prev_month_start = prev_month_end.replace(day=1)
            had_invoice_prev = any(
                prev_month_start.strftime("%Y-%m-%d") <= d <= prev_month_end.strftime("%Y-%m-%d")
                for d in dates
            )
            # Activo al inicio de M = no perdido aún al 1ro del mes
            days_at_start = (m_start - ld).days
            if days_at_start < LOST_THRESHOLD_DAYS:
                m_active_start += 1
                # ¿Se convirtió en perdido DURANTE este mes?
                days_at_end = (m_end - ld).days
                if days_at_end >= LOST_THRESHOLD_DAYS:
                    m_new_lost += 1

        m_pct = round(m_new_lost / m_active_start * 100, 1) if m_active_start > 0 else 0
        churn_history.append({
            "month": m_label,
            "pct": m_pct,
            "new_lost": m_new_lost,
            "active_start": m_active_start,
        })
        print(f"    {m_label}: {m_new_lost} nuevos perdidos / {m_active_start} activos = {m_pct}%")

    churn_history.reverse()

    # ── 6. Nuevos perdidos este mes (para KPI) ──
    this_month_start = today.replace(day=1)
    newly_lost_this_month = 0
    for pid, dates in partner_dates.items():
        ld = datetime.strptime(dates[-1], "%Y-%m-%d")
        lost_date = ld + timedelta(days=LOST_THRESHOLD_DAYS)
        if this_month_start <= lost_date <= today:
            newly_lost_this_month += 1

    current_churn_pct = churn_history[-1]["pct"] if churn_history else 0

    # ── 6. Enriquecer top 50 perdidos Y durmientes: litros, ejecutivo, lead CRM, gestión ──
    # Solo los que van al JSON — queries acotadas a esos partner_ids (una pasada para ambos).
    lost_top = lost_list[:50]
    dormant_top = dormant_list[:50]
    enrich_all = lost_top + dormant_top
    if enrich_all:
        pids_l = sorted({e["partner_id"] for e in enrich_all})
        # Litros Diésel en los ~6 meses previos a la última factura de cada cliente
        min_last = min(e["last_invoice"] for e in enrich_all)
        lit_cutoff = (datetime.strptime(min_last, "%Y-%m-%d") - timedelta(days=190)).strftime("%Y-%m-%d")
        dlines = fetch_all(models, uid, "account.move.line",
            [["partner_id", "in", pids_l],
             ["product_id", "=", DIESEL_B1_PRODUCT],
             ["parent_state", "=", "posted"],
             ["move_id.move_type", "=", "out_invoice"],
             ["date", ">=", lit_cutoff]],
            ["partner_id", "quantity", "date"])
        lit_by_pid = {}
        for ln in dlines:
            if not ln.get("partner_id"):
                continue
            lit_by_pid.setdefault(ln["partner_id"][0], []).append(
                (ln.get("date") or "", ln.get("quantity") or 0))
        # Ejecutivo ASIGNADO (res.partner.user_id) — ojo: no es la gestión real
        pinfo = sr(models, uid, "res.partner", [["id", "in", pids_l]], ["id", "user_id"], limit=300)
        exec_by_pid = {p["id"]: (p["user_id"][1] if p.get("user_id") else None) for p in pinfo}
        # Lead CRM más reciente por partner
        leads_l = fetch_all(models, uid, "crm.lead",
            [["partner_id", "in", pids_l]],
            ["id", "partner_id", "stage_id", "write_date"])
        lead_by_pid = {}
        pid_by_lead = {}
        for ldd in leads_l:
            if not ldd.get("partner_id"):
                continue
            lp = ldd["partner_id"][0]
            pid_by_lead[ldd["id"]] = lp
            prev = lead_by_pid.get(lp)
            if not prev or (ldd.get("write_date") or "") > (prev.get("write_date") or ""):
                lead_by_pid[lp] = ldd
        lead_ids_all = list(pid_by_lead.keys())
        # Última nota (comment) sobre el partner o sus leads — gestión REAL
        msgs = fetch_all(models, uid, "mail.message",
            [["message_type", "=", "comment"], "|",
             "&", ["model", "=", "res.partner"], ["res_id", "in", pids_l],
             "&", ["model", "=", "crm.lead"], ["res_id", "in", lead_ids_all or [0]]],
            ["model", "res_id", "date", "author_id"])
        last_note_by_pid = {}
        for m in msgs:
            pid_m = m["res_id"] if m.get("model") == "res.partner" else pid_by_lead.get(m.get("res_id"))
            if not pid_m:
                continue
            prev = last_note_by_pid.get(pid_m)
            if not prev or (m.get("date") or "") > (prev.get("date") or ""):
                last_note_by_pid[pid_m] = m
        # Actividades pendientes sobre el partner o sus leads
        acts = fetch_all(models, uid, "mail.activity",
            ["|",
             "&", ["res_model", "=", "res.partner"], ["res_id", "in", pids_l],
             "&", ["res_model", "=", "crm.lead"], ["res_id", "in", lead_ids_all or [0]]],
            ["res_model", "res_id", "summary", "date_deadline"])
        act_by_pid = {}
        for a in acts:
            pid_a = a["res_id"] if a.get("res_model") == "res.partner" else pid_by_lead.get(a.get("res_id"))
            if not pid_a:
                continue
            prev = act_by_pid.get(pid_a)
            if not prev or (a.get("date_deadline") or "9999") < (prev.get("date_deadline") or "9999"):
                act_by_pid[pid_a] = a
        # Anotar entradas (muta los dicts de lost_list/dormant_list, el [:50] los lleva)
        for e in enrich_all:
            pid_e = e["partner_id"]
            win_start = (datetime.strptime(e["last_invoice"], "%Y-%m-%d") - timedelta(days=183)).strftime("%Y-%m-%d")
            tot = sum(q for (d, q) in lit_by_pid.get(pid_e, []) if win_start <= d <= e["last_invoice"])
            e["litros_prom_mes"] = round(tot / 6)
            e["ejecutivo"] = exec_by_pid.get(pid_e)
            ldd = lead_by_pid.get(pid_e)
            e["crm_lead"] = (ldd["stage_id"][1] if ldd and ldd.get("stage_id") else ("Sí" if ldd else None))
            act = act_by_pid.get(pid_e)
            note = last_note_by_pid.get(pid_e)
            if act:
                e["gestion"] = f"⏰ {(act.get('summary') or 'Actividad')} · {act.get('date_deadline') or ''}"
            elif note:
                autor = note["author_id"][1] if note.get("author_id") else "?"
                e["gestion"] = f"Nota {str(note.get('date') or '')[:10]} · {autor}"
            else:
                e["gestion"] = None
        print(f"  Enriquecidos: {len(lost_top)} perdidos + {len(dormant_top)} durmientes (litros/ejecutivo/CRM/gestión)")

    return {
        "dormant_count": len(dormant_list),
        "lost_count": len(lost_list),
        "newly_lost_this_month": newly_lost_this_month,
        "churn_pct": current_churn_pct,
        "dormant": dormant_list[:50],
        "lost": lost_list[:50],
        "history": churn_history,
        "definition": f"Perdido = {LOST_THRESHOLD_DAYS}d sin factura | Durmiente = >freq×1.5 (<30d) o freq×1.3 (≥30d)",
    }


# ── SUPPLIER COMPLIANCE: ENAP + ADQUIM/ADGREEN commitments by plant ──
# Monthly committed volumes (litros) by plant — from contracts 2026
SUPPLIER_TARGETS = {
    "enap": {  # ENAP by plant
        "2026-01": {"Maipu": 188410, "San Fernando": 282615, "Linares": 385384},
        "2026-02": {"Maipu": 192762, "San Fernando": 289143, "Linares": 394286},
        "2026-03": {"Maipu": 200695, "San Fernando": 301042, "Linares": 410512},
        "2026-04": {"Maipu": 230827, "San Fernando": 346241, "Linares": 45000},
        "2026-05": {"Maipu": 183025, "San Fernando": 274538, "Linares": 45000},
        "2026-06": {"Maipu": 153229, "San Fernando": 229844, "Linares": 45000},
        "2026-07": {"Maipu": 195827, "San Fernando": 293740, "Linares": 45000},
        "2026-08": {"Maipu": 191801, "San Fernando": 287701, "Linares": 45000},
        "2026-09": {"Maipu": 186138, "San Fernando": 279207, "Linares": 45000},
        "2026-10": {"Maipu": 201974, "San Fernando": 302961, "Linares": 45000},
        "2026-11": {"Maipu": 190029, "San Fernando": 285043, "Linares": 388696},
        "2026-12": {"Maipu": 187284, "San Fernando": 280926, "Linares": 383081},
    },
    "adquim": {  # ADQUIM + ADGREEN by plant
        "2026-01": {"Maipu": 0, "Linares": 30000},
        "2026-02": {"Maipu": 0, "Linares": 30000},
        "2026-03": {"Maipu": 0, "Linares": 30000},
        "2026-04": {"Maipu": 0, "Linares": 472147},
        "2026-05": {"Maipu": 0, "Linares": 374369},
        "2026-06": {"Maipu": 0, "Linares": 313423},
        "2026-07": {"Maipu": 0, "Linares": 400554},
        "2026-08": {"Maipu": 0, "Linares": 392320},
        "2026-09": {"Maipu": 0, "Linares": 380736},
        "2026-10": {"Maipu": 0, "Linares": 413129},
        "2026-11": {"Maipu": 0, "Linares": 30000},
        "2026-12": {"Maipu": 0, "Linares": 30000},
    },
}


# Patio de carga por prefijo de patente (dato operacional Pauline, jul 2026).
# PO.picking_type_id nombra el camión (ej. "TJVS-53: Recepciones") → prefijo 2 letras.
# Patente no listada cae en "Sin mapear" — visible en la tabla, no se esconde.
PLANT_BY_TRUCK_PREFIX = {
    "PY": "Linares",        # Mario Marin
    "SH": "Linares",        # Roberto Urrutia
    "TY": "Linares",        # Patricio Garrido
    "VD": "San Fernando",   # Fernando Garroz
    "PH": "San Fernando",   # Jorge Aguilera
    "TJ": "San Fernando",   # Jose Luis Valenzuela
    "HH": "San Fernando",   # Sin conductor
}


def extract_enap_compliance(models, uid):
    """MTD purchases from ENAP + ADQUIM/ADGREEN vs monthly targets by plant, with projection."""
    import calendar
    print("Extracting supplier compliance (ENAP + ADQUIM)...")
    today = datetime.now()
    month_start = today.replace(day=1).strftime("%Y-%m-%d")
    month_key = today.strftime("%Y-%m")
    today_str = today.strftime("%Y-%m-%d")
    days_in_month = calendar.monthrange(today.year, today.month)[1]
    days_elapsed = today.day
    pct_month = days_elapsed / days_in_month

    # Lookup ADGREEN dynamically
    adgreen = sr(models, uid, "res.partner",
                 [["name", "ilike", "adgreen"]], ["id"], limit=3)
    adgreen_id = adgreen[0]["id"] if adgreen else None

    # Define supplier groups
    supplier_groups = {
        "enap": [ENAP_PARTNER],
        "adquim": [ADQUIM_PARTNER] + ([adgreen_id] if adgreen_id else []),
    }

    results = {}
    for group_name, partner_ids in supplier_groups.items():
        targets = SUPPLIER_TARGETS.get(group_name, {}).get(month_key, {})
        target_total = sum(targets.values())

        # MTD purchase invoices from these suppliers
        bills = sr(models, uid, "account.move", [
            ["move_type", "=", "in_invoice"],
            ["state", "=", "posted"],
            ["partner_id", "in", partner_ids],
            ["invoice_date", ">=", month_start],
            ["invoice_date", "<=", today_str],
        ], ["id", "invoice_origin"], 500)

        bill_ids = [b["id"] for b in bills]
        mtd_litros = 0
        litros_by_bill = {}
        if bill_ids:
            lines = sr(models, uid, "account.move.line", [
                ["move_id", "in", bill_ids],
                ["display_type", "=", "product"],
            ], ["move_id", "quantity"], 2000)
            for l in lines:
                mid = l["move_id"][0] if l.get("move_id") else None
                q = l.get("quantity") or 0
                mtd_litros += q
                if mid:
                    litros_by_bill[mid] = litros_by_bill.get(mid, 0) + q
            mtd_litros = round(mtd_litros)

        projected = round(mtd_litros / pct_month) if pct_month > 0 else 0
        compliance_pct = round(mtd_litros / target_total * 100, 1) if target_total > 0 else 0
        projected_pct = round(projected / target_total * 100, 1) if target_total > 0 else 0

        # Atribución por planta: factura → invoice_origin (PO) → picking_type (patente) → patio
        po_names = sorted({(b.get("invoice_origin") or "").split(",")[0].strip()
                           for b in bills if b.get("invoice_origin")})
        plant_by_po = {}
        if po_names:
            pos = sr(models, uid, "purchase.order",
                     [["name", "in", po_names]], ["name", "picking_type_id"], 500)
            for po in pos:
                pt_name = po["picking_type_id"][1] if po.get("picking_type_id") else ""
                plant_by_po[po["name"]] = PLANT_BY_TRUCK_PREFIX.get(pt_name[:2].upper(), "Sin mapear")

        mtd_by_plant = {}
        for b in bills:
            q = litros_by_bill.get(b["id"], 0)
            if not q:
                continue
            po_name = (b.get("invoice_origin") or "").split(",")[0].strip()
            plant = plant_by_po.get(po_name, "Sin mapear")
            mtd_by_plant[plant] = mtd_by_plant.get(plant, 0) + q

        # Per-plant breakdown: meta + MTD real + cumplimiento + proyección
        plants = {}
        for plant in sorted(set(targets.keys()) | set(mtd_by_plant.keys())):
            t = targets.get(plant, 0)
            m = round(mtd_by_plant.get(plant, 0))
            pr = round(m / pct_month) if pct_month > 0 else 0
            plants[plant] = {
                "target": t,
                "mtd": m,
                "compliance_pct": round(m / t * 100, 1) if t > 0 else 0,
                "projected": pr,
                "projected_pct": round(pr / t * 100, 1) if t > 0 else 0,
            }

        results[group_name] = {
            "partners": [str(p) for p in partner_ids],
            "target_total": target_total,
            "mtd_litros": mtd_litros,
            "compliance_pct": compliance_pct,
            "projected": projected,
            "projected_pct": projected_pct,
            "plants": plants,
        }
        print(f"  {group_name}: {mtd_litros}L MTD / {target_total}L target ({compliance_pct}%), proj {projected}L ({projected_pct}%)")

    # Combined total
    total_target = sum(r["target_total"] for r in results.values())
    total_mtd = sum(r["mtd_litros"] for r in results.values())
    total_proj = round(total_mtd / pct_month) if pct_month > 0 else 0

    return {
        "month": month_key,
        "days_elapsed": days_elapsed,
        "days_in_month": days_in_month,
        "suppliers": results,
        "total_target": total_target,
        "total_mtd": total_mtd,
        "total_compliance_pct": round(total_mtd / total_target * 100, 1) if total_target > 0 else 0,
        "total_projected": total_proj,
        "total_projected_pct": round(total_proj / total_target * 100, 1) if total_target > 0 else 0,
    }


# ── OPERACIONES (transport expenses) ──
TRANSPORT_BUDGET = {
    "2025-01": 11705955, "2025-02": 11858691, "2025-03": 15342984, "2025-04": 15321854,
    "2025-05": 11078731, "2025-06": 8503716, "2025-07": 13074085, "2025-08": 13323650,
    "2025-09": 14420942, "2025-10": 15509795, "2025-11": 15023726, "2025-12": 15389582,
    "2026-01": 12742955, "2026-02": 13074640, "2026-03": 16149608, "2026-04": 14517971,
    "2026-05": 10055583, "2026-06": 16920204, "2026-07": 12501767, "2026-08": 15375085,
    "2026-09": 12280892, "2026-10": 15623001, "2026-11": 10395006, "2026-12": 12172896,
}

TRANSPORT_ACCOUNTS = [
    "3.3.01.28",  # Arriendo Terreno para Camiones
    "3.3.01.30",  # Elementos Protección Personal (EPP)
    "3.1.01.02",  # Petróleo para Camiones
    "3.1.01.04",  # Seguros Pagados Camiones
    "3.1.01.06",  # Seguros Pagados Camionetas
    "3.1.01.08",  # Mantenciones Varias
    "3.3.01.18",  # Mantención y Reparación Camiones
    "3.3.01.29",  # Gastos Vehículos
    "3.3.01.52",  # Gastos de Traslados (TAG)
]

# Map account codes to display names
TRANSPORT_ACCOUNT_NAMES = {
    "3.3.01.28": "Arriendo",
    "3.3.01.30": "EPP",
    "3.1.01.02": "Petróleo",
    "3.1.01.04": "Seguros Camiones",
    "3.1.01.06": "Seguros Camionetas",
    "3.1.01.08": "Mantenciones Varias",
    "3.3.01.18": "Mantención y Reparación",
    "3.3.01.29": "Gastos Vehículos",
    "3.3.01.52": "Gastos Traslados (TAG)",
}

def extract_operaciones(models, uid):
    """Extract transport expenses (last 6 months) vs budget."""
    import calendar
    print("Extracting operaciones (transport expenses)...")
    today = datetime.now()

    # Build list of account IDs for these codes
    account_codes = TRANSPORT_ACCOUNTS
    account_id_map = {}  # code -> id

    for code in account_codes:
        accs = sr(models, uid, "account.account",
                  [["code", "=", code]], ["id", "code"], limit=1)
        if accs:
            account_id_map[code] = accs[0]["id"]

    print(f"  Found {len(account_id_map)} / {len(account_codes)} account codes")

    if not account_id_map:
        return {"months": []}

    # Get last 6 months
    months = []
    for m_offset in range(6):
        ref = today.replace(day=1)
        for _ in range(m_offset):
            ref = (ref - timedelta(days=1)).replace(day=1)
        m_start = ref.strftime("%Y-%m-%d")
        m_end = ((ref + timedelta(days=32)).replace(day=1) - timedelta(days=1)).strftime("%Y-%m-%d")
        m_key = ref.strftime("%Y-%m")
        m_label = ref.strftime("%b %Y").replace("May", "May").replace("Jun", "Jun").replace("Jul", "Jul")
        # Spanish month names
        month_es = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]
        m_label = f"{month_es[ref.month-1]} {ref.year}".replace(" ", " ")

        months.append({
            "month": m_key,
            "label": m_label,
            "start": m_start,
            "end": m_end,
        })

    months.reverse()  # oldest first

    # Query expenses for each month
    account_ids = list(account_id_map.values())
    results = []

    for m in months:
        # Query account.move.line for all transport accounts in this month
        lines = sr(models, uid, "account.move.line", [
            ["account_id", "in", account_ids],
            ["parent_state", "=", "posted"],
            ["date", ">=", m["start"]],
            ["date", "<=", m["end"]],
        ], ["debit", "account_id", "date"], 5000)

        # Sum debits by account code
        by_code = {}
        total = 0
        for line in lines:
            aid = line["account_id"][0] if line.get("account_id") else None
            debit = line.get("debit", 0)
            total += debit
            # Find code for this account ID
            code = None
            for c, id in account_id_map.items():
                if id == aid:
                    code = c
                    break
            if code:
                if code not in by_code:
                    by_code[code] = 0
                by_code[code] += debit

        # Build categories dict (grouped for display)
        categories = {
            "Petróleo": by_code.get("3.1.01.02", 0),
            "Mantención y Reparación": by_code.get("3.3.01.18", 0),
            "Gastos Traslados (TAG)": by_code.get("3.3.01.52", 0),
            "Seguros": by_code.get("3.1.01.04", 0) + by_code.get("3.1.01.06", 0),
            "Gastos Vehículos": by_code.get("3.3.01.29", 0),
            "Arriendo": by_code.get("3.3.01.28", 0),
            "EPP": by_code.get("3.3.01.30", 0),
            "Mantenciones Varias": by_code.get("3.1.01.08", 0),
        }

        budget = TRANSPORT_BUDGET.get(m["month"], 0)
        compliance_pct = round((total / budget * 100), 1) if budget > 0 else 0

        results.append({
            "month": m["month"],
            "label": m["label"],
            "budget": budget,
            "actual": round(total),
            "compliance_pct": compliance_pct,
            "categories": {k: round(v) for k, v in categories.items()},
        })

    return {"months": results}


# ── RIESGO VIGENTE (credit risk) ──
def normalize_rut(rut_str):
    """Normalize RUT: remove dots, dashes, CL prefix, spaces. Returns digit-only string."""
    if not rut_str:
        return None
    s = str(rut_str).upper().replace("CL", "").replace(".", "").replace("-", "").replace(" ", "").strip()
    return s if s else None


def load_avla_lines():
    """Load AVLA insured lines from avla-lines.json. Returns dict keyed by normalized RUT."""
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "avla-lines.json")
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        print("  WARNING: avla-lines.json no encontrado o inválido — usando cobertura $0")
        return {}, None
    # Re-key by normalized RUT
    normalized = {}
    for rut, info in data.get("lines", {}).items():
        nr = normalize_rut(rut)
        if nr:
            normalized[nr] = info
    return normalized, data.get("fecha_descarga")


def extract_riesgo(models, uid):
    """
    Riesgo Vigente con cobertura AVLA real (no credit_limit Odoo).
    Cubierto por cliente = min(deuda_cliente, cobertura_efectiva_avla_clp_cliente).
    Suma → cubierto total. CxC - cubierto = no_cubierto.
    """
    print("Extracting credit risk (Riesgo Vigente AVLA)...")
    avla_lines, avla_fecha = load_avla_lines()
    print(f"  AVLA lines cargadas: {len(avla_lines)} RUTs (fecha descarga: {avla_fecha})")

    invoices = fetch_all(models, uid, "account.move",
        [["move_type", "=", "out_invoice"], ["state", "=", "posted"],
         ["payment_state", "in", ["not_paid", "partial"]], ["amount_residual", ">", 0]],
        ["partner_id", "amount_total", "amount_residual"])

    # Get VAT (RUT) per partner
    partner_ids = list(set(inv["partner_id"][0] for inv in invoices if inv.get("partner_id")))
    partner_vat = {}
    for offset in range(0, len(partner_ids), 200):
        batch = partner_ids[offset:offset+200]
        partners = sr(models, uid, "res.partner", [["id", "in", batch]],
                       ["id", "vat"], limit=200)
        for p in partners:
            partner_vat[p["id"]] = normalize_rut(p.get("vat"))

    # Aggregate debt per partner
    partner_debt = {}
    for inv in invoices:
        pid = inv["partner_id"][0] if inv.get("partner_id") else None
        if not pid:
            continue
        pname = inv["partner_id"][1]
        if pid not in partner_debt:
            partner_debt[pid] = {"name": pname, "total": 0, "count": 0}
        partner_debt[pid]["total"] += inv["amount_residual"]
        partner_debt[pid]["count"] += 1

    cubierto_monto, cubierto_count = 0, 0
    no_cubierto_monto, no_cubierto_count = 0, 0
    matched_ruts = 0

    for pid, d in partner_debt.items():
        rut = partner_vat.get(pid)
        avla_info = avla_lines.get(rut) if rut else None
        if avla_info:
            matched_ruts += 1
            cobertura_clp = avla_info.get("cobertura_clp", 0)
            cubierto_cliente = min(d["total"], cobertura_clp)
            no_cubierto_cliente = d["total"] - cubierto_cliente
        else:
            cubierto_cliente = 0
            no_cubierto_cliente = d["total"]

        cubierto_monto += cubierto_cliente
        no_cubierto_monto += no_cubierto_cliente
        if cubierto_cliente > 0:
            cubierto_count += d["count"]
        if no_cubierto_cliente > 0:
            no_cubierto_count += d["count"]

    print(f"  Match RUT AVLA↔Odoo: {matched_ruts}/{len(partner_debt)} clientes con deuda")

    total_m = cubierto_monto + no_cubierto_monto
    total_c = cubierto_count + no_cubierto_count
    return {
        "cubierto": round(cubierto_monto),
        "cubierto_count": cubierto_count,
        "no_cubierto": round(no_cubierto_monto),
        "no_cubierto_count": no_cubierto_count,
        "pct_monto": round(no_cubierto_monto / total_m * 100, 2) if total_m > 0 else 0,
        "pct_count": round(no_cubierto_count / total_c * 100, 2) if total_c > 0 else 0,
        "avla_fecha_descarga": avla_fecha,
        "avla_ruts_matched": matched_ruts,
        "avla_ruts_total": len(avla_lines),
    }


# ── DSO / Rotacion CxC ──
# Replica EXACTA de la hoja del directorio (celda CD28):
#   Rotacion = PROMEDIO_3m(CxC) / Revenue_bruto(mes) * 30
# Numerador: saldo cuentas CxC clientes (1.1.04.05 + 1.1.04.11)
#            + cheques en cartera (1.1.05.07) al cierre de mes,
#            promediado con los 2 meses anteriores.
#            NOTA: los cheques salen de la CUENTA 1.1.05.07, NO del journal 114
#            (el journal 114 solo se usa en la seccion Cash Flow semanal).
# Denominador: Revenue BRUTO del mes = venta_neta(4.1.01.01) * 1.19 (IVA)
#              + IEC(4.2.01.02). NO es amount_untaxed neto.
CXC_ACCOUNT_CODES = ["1.1.04.05", "1.1.04.11"]   # cuentas por cobrar clientes
CHEQUES_ACCOUNT_CODE = "1.1.05.07"               # cheques en cartera (saldo contable)
# Cuentas de ingreso para el Revenue bruto (mismo origen que la hoja del directorio)
REVENUE_ACCOUNT_CODE = "4.1.01.01"   # INGRESOS POR VENTA CO (venta neta)
IEC_ACCOUNT_CODE = "4.2.01.02"       # IMPUESTO ESPECIFICO VENTAS (IEC)
IVA_RATE = 1.19                      # IVA 19% aplicado sobre la venta neta


def extract_dso(models, uid, n_months=6):
    """Rotacion CxC = promedio_CxC_3m / Revenue_bruto_mes * 30.

    Replica exacta de la hoja del directorio (celda CD28).

    Numerador: saldo cuentas CxC clientes (1.1.04.05 + 1.1.04.11)
               + cheques en cartera (cuenta 1.1.05.07) al cierre del mes,
               promediado con los 2 meses anteriores (suaviza fin de mes).

    Denominador: Revenue BRUTO del mes = venta neta (cuenta 4.1.01.01) * 1.19
               + IEC (cuenta 4.2.01.02). Normalizado a 30 dias.

    Tambien retorna cxc_hoy: planta impago HOY (CxC + cheques a fecha ejecucion).
    """
    print(f"Extracting DSO ({n_months} months)...")

    # 1. IDs cuentas CxC
    acc_ids = models.execute_kw(
        ODOO_DB, uid, ODOO_KEY, "account.account", "search",
        [[["code", "in", CXC_ACCOUNT_CODES]]]
    )
    if not acc_ids:
        print("  WARNING: cuentas CxC no encontradas, DSO omitido")
        return {"months": [], "cxc_hoy": 0}
    print(f"  Cuentas CxC clientes: {len(acc_ids)} encontradas (codes: {CXC_ACCOUNT_CODES})")

    # 1a. ID cuenta cheques en cartera (1.1.05.07) — saldo contable, NO journal 114
    cheq_acc_ids = models.execute_kw(
        ODOO_DB, uid, ODOO_KEY, "account.account", "search",
        [[["code", "=", CHEQUES_ACCOUNT_CODE]]]
    )
    if not cheq_acc_ids:
        print(f"  WARNING: cuenta cheques {CHEQUES_ACCOUNT_CODE} no encontrada (cheques = 0)")
    else:
        print(f"  Cuenta cheques {CHEQUES_ACCOUNT_CODE}: {cheq_acc_ids}")

    # 1b. IDs cuentas de ingreso (venta neta + IEC) para Revenue bruto
    rev_acc_ids = models.execute_kw(
        ODOO_DB, uid, ODOO_KEY, "account.account", "search",
        [[["code", "=", REVENUE_ACCOUNT_CODE]]]
    )
    iec_acc_ids = models.execute_kw(
        ODOO_DB, uid, ODOO_KEY, "account.account", "search",
        [[["code", "=", IEC_ACCOUNT_CODE]]]
    )
    if not rev_acc_ids:
        print(f"  WARNING: cuenta venta {REVENUE_ACCOUNT_CODE} no encontrada, DSO omitido")
        return {"months": [], "cxc_hoy": 0}
    if not iec_acc_ids:
        print(f"  WARNING: cuenta IEC {IEC_ACCOUNT_CODE} no encontrada")
    print(f"  Cuenta venta {REVENUE_ACCOUNT_CODE}: {rev_acc_ids} | IEC {IEC_ACCOUNT_CODE}: {iec_acc_ids}")

    def get_account_balance(account_ids, first_date, last_date):
        """Balance (abs) de cuentas de ingreso en el mes. Ingresos son credito,
        por eso se toma abs(credit - debit)."""
        if not account_ids:
            return 0
        rg = models.execute_kw(
            ODOO_DB, uid, ODOO_KEY, "account.move.line", "read_group",
            [[["account_id", "in", account_ids],
              ["parent_state", "=", "posted"],
              ["date", ">=", first_date],
              ["date", "<=", last_date]]],
            {"fields": ["debit", "credit"], "groupby": [], "lazy": False}
        )
        debit = rg[0].get("debit", 0) if rg else 0
        credit = rg[0].get("credit", 0) if rg else 0
        return abs(credit - debit)

    def get_cxc_at(cutoff_date):
        """Saldo CxC contable al cutoff."""
        rg = models.execute_kw(
            ODOO_DB, uid, ODOO_KEY, "account.move.line", "read_group",
            [[["account_id", "in", acc_ids],
              ["parent_state", "=", "posted"],
              ["date", "<=", cutoff_date]]],
            {"fields": ["debit", "credit"], "groupby": [], "lazy": False}
        )
        debit = rg[0].get("debit", 0) if rg else 0
        credit = rg[0].get("credit", 0) if rg else 0
        return max(debit - credit, 0)

    def get_cheques_at(cutoff_date):
        """Saldo cheques en cartera (cuenta 1.1.05.07) al cutoff.
        NO usa journal 114 (ese filtro daba 0 — los cheques estan en la cuenta)."""
        if not cheq_acc_ids:
            return 0
        rg = models.execute_kw(
            ODOO_DB, uid, ODOO_KEY, "account.move.line", "read_group",
            [[["account_id", "in", cheq_acc_ids],
              ["parent_state", "=", "posted"],
              ["date", "<=", cutoff_date]]],
            {"fields": ["debit", "credit"], "groupby": [], "lazy": False}
        )
        debit = rg[0].get("debit", 0) if rg else 0
        credit = rg[0].get("credit", 0) if rg else 0
        return max(debit - credit, 0)

    # 2. Generar N+2 meses cerrados (extra para calcular promedio 3m desde el primer mes)
    today = datetime.now()
    months = []
    d = today.replace(day=1) - timedelta(days=1)
    for _ in range(n_months + 2):
        y, m = d.year, d.month
        _, last = monthrange(y, m)
        months.append({
            "y": y, "m": m,
            "first": f"{y}-{m:02d}-01",
            "last": f"{y}-{m:02d}-{last:02d}",
            "label": f"{y}-{m:02d}",
        })
        d = d.replace(day=1) - timedelta(days=1)
    months.reverse()

    # 3. Calcular CxC+cheques y ventas para todos los meses
    raw = []
    for mo in months:
        cxc_acc = get_cxc_at(mo["last"])
        cheq = get_cheques_at(mo["last"])
        cxc_total = cxc_acc + cheq

        # Revenue bruto = venta neta (4.1.01.01) * 1.19 + IEC (4.2.01.02)
        venta_neta = get_account_balance(rev_acc_ids, mo["first"], mo["last"])
        iec = get_account_balance(iec_acc_ids, mo["first"], mo["last"])
        net_sales = venta_neta * IVA_RATE + iec
        raw.append({"label": mo["label"], "cxc_total": cxc_total, "cxc_acc": cxc_acc, "cheques": cheq, "net_sales": net_sales})

    # 4. DSO con promedio 3m (solo para los ultimos n_months)
    results = []
    for i in range(2, len(raw)):
        cur = raw[i]
        avg_cxc = round((raw[i]["cxc_total"] + raw[i-1]["cxc_total"] + raw[i-2]["cxc_total"]) / 3)
        net_sales = cur["net_sales"]
        dso = round(avg_cxc / net_sales * 30, 1) if net_sales > 0 else 0
        print(f"  {cur['label']}: CxC_prom3m=${avg_cxc:,.0f} | RevenueBruto=${net_sales:,.0f} | Rotacion={dso}d")
        results.append({
            "month": cur["label"],
            "cxc": round(cur["cxc_total"]),
            "cxc_acc": round(cur["cxc_acc"]),
            "cheques": round(cur["cheques"]),
            "avg_cxc_3m": avg_cxc,
            "net_sales": round(net_sales),
            "dso": dso,
        })

    # 5. CxC hoy (planta actual)
    today_str = today.strftime("%Y-%m-%d")
    cxc_hoy = get_cxc_at(today_str) + get_cheques_at(today_str)
    print(f"  CxC hoy (planta): ${cxc_hoy:,.0f}")

    return {"months": results, "cxc_hoy": round(cxc_hoy)}


# ── MAIN ──
def main():
    print("=== CEO Dashboard · Odoo Extraction ===")
    models, uid = connect()

    # Dynamic lookups
    print("Looking up IDs...")
    supplier_ids = lookup_supplier_ids(models, uid)
    term_map, contado_term_ids = lookup_payment_terms(models, uid)
    ruta_stage_id = lookup_ruta_stage_id(models, uid)

    weekly = extract_weekly(models, uid, supplier_ids, contado_term_ids, ruta_stage_id, term_map)
    daily = extract_daily(models, uid)
    banks = extract_bank_balances(models, uid)
    total_cash = sum(b["balance"] for b in banks)
    receivables = extract_receivables(models, uid)

    # Gerencia sections
    weeks = get_week_ranges(16)
    sla = extract_sla(models, uid, weeks)
    riesgo = extract_riesgo(models, uid)
    churn = extract_churn(models, uid)
    enap = extract_enap_compliance(models, uid)
    operaciones = extract_operaciones(models, uid)
    dso = extract_dso(models, uid)

    data = {
        "updated": datetime.now().isoformat(),
        "weeks": weekly,
        "daily": daily,
        "banks": banks,
        "total_cash": total_cash,
        "receivables": receivables,
        "sla": sla,
        "riesgo": riesgo,
        "churn": churn,
        "enap": enap,
        "operaciones": operaciones,
        "dso": dso,
        "gerencia_goals": {
            "margen_contado_meta": 0.085,
            "margen_credito_meta": 0.06,
            "visitas_semana": 30,
            "clientes_nuevos_semana": 2,
            "sla_target": 95,
        },
    }

    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ceo-data.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"\nceo-data.json written OK ({len(weekly)} weeks, {len(banks)} banks)")

    # ── Actualizar riesgo-historico.json (snapshot semanal) ──
    # Convención del archivo: una fila por semana, fecha = domingo de la semana.
    # Si ya existe esa semana, se actualiza (último valor del run del día gana).
    hist_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "riesgo-historico.json")
    today = datetime.now()
    days_to_sunday = (today.weekday() + 1) % 7  # weekday(): Mon=0..Sun=6
    week_sunday = today - timedelta(days=days_to_sunday)
    week_sunday_str = week_sunday.strftime("%Y-%m-%d")

    snapshot = {
        "fecha": week_sunday_str,
        "no_cubierto": riesgo.get("no_cubierto", 0),
        "cubierto": riesgo.get("cubierto", 0),
        "ventas_nc": riesgo.get("no_cubierto_count", 0),
        "ventas_c": riesgo.get("cubierto_count", 0),
    }

    try:
        with open(hist_path, "r", encoding="utf-8") as f:
            hist = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        hist = []

    # Update existing week or append
    idx = next((i for i, w in enumerate(hist) if w.get("fecha") == week_sunday_str), None)
    if idx is not None:
        hist[idx] = snapshot
        action = "updated"
    else:
        hist.append(snapshot)
        action = "appended"

    # Keep sorted by fecha and cap at 52 weeks
    hist.sort(key=lambda w: w.get("fecha", ""))
    hist = hist[-52:]

    with open(hist_path, "w", encoding="utf-8") as f:
        json.dump(hist, f, ensure_ascii=False, indent=2)
    print(f"riesgo-historico.json {action} for week {week_sunday_str} ({len(hist)} weeks total)")


if __name__ == "__main__":
    main()
