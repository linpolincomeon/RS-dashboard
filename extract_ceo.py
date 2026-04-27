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
    Factoring: Fingo, or Security-style 'Transferencia De Otro Banco Via Spav'
               (only if amount >= FACTORING_MIN_AMOUNT)
    Transfer: everything else
    """
    r = (ref or "").lower()
    if ("dep.cheq" in r or "dep. docto" in r) and "efectivo" not in r:
        return "cheques"
    if "fingo" in r or "factoring" in r:
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
        ], ["amount_total", "amount_untaxed"])

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
        ], ["quantity", "price_subtotal"], 2000)

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


# ── SLA DELIVERY (stock.picking by zone) ──
ZONE_MAP = {
    "rancagua": "Rancagua", "san fernando": "San Fdo", "san fdo": "San Fdo",
    "talca": "Talca", "curicó": "Curicó", "curico": "Curicó",
    "parral": "Parral", "vi costa": "VI Costa", "linares": "Linares",
}


def classify_zone(city_or_zone):
    """Map partner city/zone to dashboard zone."""
    if not city_or_zone:
        return "Otro"
    low = city_or_zone.lower().strip()
    for key, zone in ZONE_MAP.items():
        if key in low:
            return zone
    return "Otro"


def extract_sla(models, uid, weeks):
    """Delivery SLA per zone per week from stock.picking."""
    print("Extracting SLA delivery data...")
    sla_data = []
    for i, wd in enumerate(weeks[:1]):  # SLA for current week only
        # Outgoing deliveries completed this week
        pickings = sr(models, uid, "stock.picking", [
            ["picking_type_code", "=", "outgoing"],
            ["state", "=", "done"],
            ["date_done", ">=", wd["start"] + " 00:00:00"],
            ["date_done", "<=", wd["end"] + " 23:59:59"],
        ], ["partner_id", "scheduled_date", "date_done"], 2000)

        zone_stats = {}
        for p in pickings:
            pid = p.get("partner_id")
            if not pid:
                continue
            # Get partner city for zone classification
            partner = sr(models, uid, "res.partner", [["id", "=", pid[0]]],
                         ["city"], limit=1)
            city = partner[0].get("city", "") if partner else ""
            zone = classify_zone(city)
            if zone not in zone_stats:
                zone_stats[zone] = {"total": 0, "on_time": 0, "late_clients": []}
            zone_stats[zone]["total"] += 1
            # On time = delivered within 24h of scheduled
            sched = p.get("scheduled_date", "")
            done = p.get("date_done", "")
            if sched and done:
                try:
                    s_dt = datetime.strptime(sched[:19], "%Y-%m-%d %H:%M:%S")
                    d_dt = datetime.strptime(done[:19], "%Y-%m-%d %H:%M:%S")
                    if (d_dt - s_dt).total_seconds() <= 86400:  # 24h
                        zone_stats[zone]["on_time"] += 1
                    else:
                        pname = pid[1] if pid else "N/A"
                        zone_stats[zone]["late_clients"].append(pname)
                except (ValueError, TypeError):
                    zone_stats[zone]["on_time"] += 1  # assume on time if parse fails

        week_sla = {}
        for zone, st in sorted(zone_stats.items()):
            pct = round(st["on_time"] / st["total"] * 100, 2) if st["total"] > 0 else 100
            week_sla[zone] = {
                "total": st["total"],
                "on_time": st["on_time"],
                "late": st["total"] - st["on_time"],
                "pct": pct,
                "late_clients": st["late_clients"][:5],  # top 5
            }
        sla_data.append({"label": wd["label"], "zones": week_sla})
        total_p = sum(s["total"] for s in week_sla.values())
        total_ot = sum(s["on_time"] for s in week_sla.values())
        print(f"  {wd['label']}: {total_p} pickings, {total_ot} on time")
    return sla_data


# ── CHURN: frequency-based, using res.partner.frecuencia_facturacion ──
FREQ_MAP = {
    "diaria": 1, "diario": 1,
    "semanal": 7,
    "quincenal": 15,
    "mensual": 30,
    "bimensual": 60, "bimestral": 60,
    "trimestral": 90,
}
CHURN_MULTIPLIER = 2  # churned if days_since_last > freq * multiplier
IRREGULAR_DEFAULT_DAYS = 15


def parse_frecuencia(val):
    """Parse frecuencia_facturacion char field into expected days between purchases."""
    import re
    if not val:
        return None
    low = val.lower().strip()
    # Check known keywords first
    for key, days in FREQ_MAP.items():
        if key in low:
            return days
    # Irregular with number: "Irregular (cada 23 días)" or "Irregular (12.5)"
    m = re.search(r"(\d+[\.,]?\d*)", low)
    if m:
        return round(float(m.group(1).replace(",", ".")))
    # Irregular without number
    if "irregular" in low:
        return IRREGULAR_DEFAULT_DAYS
    return None


def extract_churn(models, uid):
    """
    Frequency-based churn: for each active customer, read their
    frecuencia_facturacion from res.partner, find their last invoice date,
    and flag as churned if days_since_last_invoice > frequency * 2.
    """
    print("Extracting churn data (frequency-based)...")
    today = datetime.now()

    # Get all partners with invoices in last 8 months
    eight_months_ago = (today - timedelta(days=240)).strftime("%Y-%m-%d")
    recent_invs = fetch_all(models, uid, "account.move",
        [["move_type", "=", "out_invoice"], ["state", "=", "posted"],
         ["invoice_date", ">=", eight_months_ago]],
        ["partner_id", "invoice_date"])

    # Build last_invoice_date per partner
    partner_last = {}  # pid -> last invoice date string
    for inv in recent_invs:
        pid = inv["partner_id"][0] if inv.get("partner_id") else None
        if not pid:
            continue
        dt = inv.get("invoice_date", "")
        if dt > partner_last.get(pid, ""):
            partner_last[pid] = dt

    active_pids = list(partner_last.keys())
    print(f"  {len(active_pids)} partners with invoices in last 8 months")

    # Read frecuencia_facturacion for all active partners
    partner_freq = {}  # pid -> {name, freq_raw, freq_days}
    for offset in range(0, len(active_pids), 200):
        batch = active_pids[offset:offset + 200]
        partners = sr(models, uid, "res.partner", [["id", "in", batch]],
                       ["id", "name", "frecuencia_facturacion"], limit=200)
        for p in partners:
            freq_raw = p.get("frecuencia_facturacion") or ""
            freq_days = parse_frecuencia(freq_raw)
            partner_freq[p["id"]] = {
                "name": p["name"],
                "freq_raw": freq_raw,
                "freq_days": freq_days,
            }

    # Determine churned partners
    churned = []
    active_count = 0
    today_str = today.strftime("%Y-%m-%d")
    for pid, last_date in partner_last.items():
        info = partner_freq.get(pid)
        if not info or not info["freq_days"]:
            continue  # skip partners without frequency data
        active_count += 1
        days_since = (today - datetime.strptime(last_date, "%Y-%m-%d")).days
        threshold = info["freq_days"] * CHURN_MULTIPLIER
        if days_since > threshold:
            churned.append({
                "name": info["name"],
                "freq": info["freq_raw"],
                "freq_days": info["freq_days"],
                "last_invoice": last_date,
                "days_since": days_since,
                "threshold": threshold,
            })

    churned.sort(key=lambda x: -x["days_since"])
    pct = round(len(churned) / active_count * 100, 1) if active_count > 0 else 0
    print(f"  Churn: {len(churned)} of {active_count} active clients ({pct}%)")

    # ── Monthly churn history (last 6 months) ──
    # For each past month, calculate: how many active clients exceeded their frequency threshold
    print("  Calculating monthly churn history...")
    churn_history = []
    for m_offset in range(6):
        # Month boundaries
        ref = today.replace(day=1)
        for _ in range(m_offset):
            ref = (ref - timedelta(days=1)).replace(day=1)
        m_end = (ref + timedelta(days=32)).replace(day=1) - timedelta(days=1)
        m_label = ref.strftime("%b %Y")
        m_end_str = m_end.strftime("%Y-%m-%d")

        # Count churned at end of that month
        m_churned = 0
        m_active = 0
        for pid, last_date in partner_last.items():
            info = partner_freq.get(pid)
            if not info or not info["freq_days"]:
                continue
            days_since_at_month = (m_end - datetime.strptime(last_date, "%Y-%m-%d")).days
            if days_since_at_month < 0:
                continue  # invoice was after this month
            m_active += 1
            threshold = info["freq_days"] * CHURN_MULTIPLIER
            if days_since_at_month > threshold:
                m_churned += 1
        m_pct = round(m_churned / m_active * 100, 1) if m_active > 0 else 0
        churn_history.append({"month": m_label, "pct": m_pct, "churned": m_churned, "active": m_active})
        print(f"    {m_label}: {m_churned}/{m_active} = {m_pct}%")

    churn_history.reverse()  # oldest first for chart

    return {
        "active_clients": active_count,
        "churned_count": len(churned),
        "pct": pct,
        "churned": churned[:30],
        "multiplier": CHURN_MULTIPLIER,
        "history": churn_history,
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
        ], ["id"], 500)

        bill_ids = [b["id"] for b in bills]
        mtd_litros = 0
        if bill_ids:
            lines = sr(models, uid, "account.move.line", [
                ["move_id", "in", bill_ids],
                ["display_type", "=", "product"],
            ], ["quantity"], 2000)
            mtd_litros = round(sum(l["quantity"] for l in lines))

        projected = round(mtd_litros / pct_month) if pct_month > 0 else 0
        compliance_pct = round(mtd_litros / target_total * 100, 1) if target_total > 0 else 0
        projected_pct = round(projected / target_total * 100, 1) if target_total > 0 else 0

        # Per-plant breakdown (targets only — actual by plant requires warehouse info)
        plants = {}
        for plant, target_l in targets.items():
            plants[plant] = {"target": target_l}

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


# ── RIESGO VIGENTE (credit risk) ──
def extract_riesgo(models, uid):
    """Uncovered vs covered receivable amounts (credit limit check)."""
    print("Extracting credit risk (Riesgo Vigente)...")
    invoices = fetch_all(models, uid, "account.move",
        [["move_type", "=", "out_invoice"], ["state", "=", "posted"],
         ["payment_state", "in", ["not_paid", "partial"]], ["amount_residual", ">", 0]],
        ["partner_id", "amount_total", "amount_residual"])

    # Get credit limits per partner
    partner_ids = list(set(inv["partner_id"][0] for inv in invoices if inv.get("partner_id")))
    partner_limits = {}
    for offset in range(0, len(partner_ids), 200):
        batch = partner_ids[offset:offset+200]
        partners = sr(models, uid, "res.partner", [["id", "in", batch]],
                       ["id", "credit_limit"], limit=200)
        for p in partners:
            partner_limits[p["id"]] = p.get("credit_limit", 0)

    # Aggregate per partner
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
    for pid, d in partner_debt.items():
        limit = partner_limits.get(pid, 0)
        if limit > 0 and d["total"] <= limit:
            cubierto_monto += d["total"]
            cubierto_count += d["count"]
        else:
            no_cubierto_monto += d["total"]
            no_cubierto_count += d["count"]

    total_m = cubierto_monto + no_cubierto_monto
    total_c = cubierto_count + no_cubierto_count
    return {
        "cubierto": round(cubierto_monto),
        "cubierto_count": cubierto_count,
        "no_cubierto": round(no_cubierto_monto),
        "no_cubierto_count": no_cubierto_count,
        "pct_monto": round(no_cubierto_monto / total_m * 100, 2) if total_m > 0 else 0,
        "pct_count": round(no_cubierto_count / total_c * 100, 2) if total_c > 0 else 0,
    }


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


if __name__ == "__main__":
    main()
