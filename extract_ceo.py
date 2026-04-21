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

# Weekly budget in litros
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

# Factoring threshold — ignore small transfers that match pattern
FACTORING_MIN_AMOUNT = 1_000_000


def get_week_budget(start_date_str):
    return WEEKLY_BUDGET.get(start_date_str[:7], 270000)


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

        # ── Margin: overall + contado vs crédito + per payment term ──
        sum_mn, sum_n = 0, 0
        sum_mn_c, sum_n_c = 0, 0   # contado
        sum_mn_cr, sum_n_cr = 0, 0  # crédito
        fact_contado, fact_credito = 0, 0
        # Per-term tracking: {label: {sum_mn, sum_n, count, days}}
        by_term = {}
        for inv in invoices:
            mz = inv.get("margin_zone") or 0
            au = inv.get("amount_untaxed") or 0
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
            if is_contado:
                sum_mn_c += mz * au
                sum_n_c += au
            else:
                sum_mn_cr += mz * au
                sum_n_cr += au

        margin = sum_mn / sum_n if sum_n > 0 else 0
        margin_contado = sum_mn_c / sum_n_c if sum_n_c > 0 else 0
        margin_credito = sum_mn_cr / sum_n_cr if sum_n_cr > 0 else 0
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
        ], ["quantity", "price_subtotal", "price_total", "product_id"], 5000)

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

        ppto = get_week_budget(wd["start"])

        results.append({
            "label": wd["label"],
            "start": wd["start"],
            "end": wd["end"],
            "ventas": round(ventas),
            "neto": round(neto),
            "litros": litros,
            "precio_neto": precio_neto,
            "precio_bruto": precio_bruto,
            "margin": round(margin, 5),
            "margin_contado": round(margin_contado, 5),
            "margin_credito": round(margin_credito, 5),
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
            "ppto": ppto,
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
    for i, wd in enumerate(weeks[:8]):  # SLA for last 8 weeks only
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

    data = {
        "updated": datetime.now().isoformat(),
        "weeks": weekly,
        "daily": daily,
        "banks": banks,
        "total_cash": total_cash,
        "receivables": receivables,
        "sla": sla,
        "riesgo": riesgo,
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
