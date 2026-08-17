#!/usr/bin/env python3
"""
extract_costos.py — Gasto real por centro de costo (camión)
-----------------------------------------------------------
Lee las líneas analíticas del plan "Camiones" (account.analytic.line) y los
totales del libro mayor (account.move.line) de las cuentas de gasto de flota,
clasifica por categoría (según cuenta contable + palabras clave de la glosa)
y escribe costos-data.json para costos.html.

El presupuesto NO viene de Odoo: vive en presupuesto-camiones.json (manual,
desde "Proyectos y Control.xlsx" hoja Presupuesto Operaciones 2026, cuyo total
es la línea Transporte aprobada por el directorio en el Plan 2026).

Secrets requeridos: ODOO_URL, ODOO_DB, ODOO_USER, ODOO_KEY.
"""

import os
import json
import unicodedata
import xmlrpc.client
from datetime import datetime, timezone

ODOO_URL  = os.environ["ODOO_URL"].rstrip("/")
ODOO_DB   = os.environ["ODOO_DB"]
ODOO_USER = os.environ["ODOO_USER"]
ODOO_KEY  = os.environ["ODOO_KEY"]

OUTPUT_PATH = os.environ.get("COSTOS_OUTPUT", "costos-data.json")

YEAR = 2026
MESES = [f"{YEAR}-{m:02d}" for m in range(1, 13)]

DIESEL_PRODUCT_ID = 14

# Cuentas contables de gasto de flota (código -> nombre corto para el dashboard).
# Si aparece gasto de camiones en una cuenta nueva, agregarla aquí.
CUENTAS_FLOTA = {
    "3.1.01.02": "Petróleo Camiones",
    "3.1.01.04": "Seguros Camiones",
    "3.1.01.07": "Seguros Estanques",
    "3.1.01.08": "Mantenciones Varias",
    "3.3.01.18": "Mantención y Reparación",
    "3.3.01.28": "Arriendo Sitios",
    "3.3.01.29": "Gastos Vehículos",
    "3.3.01.52": "Gastos de Traslados",
}

# Cuentas cuyo detalle se sub-clasifica por glosa (mantención y traslados).
CUENTAS_MANTENCION = {"3.1.01.08", "3.3.01.18", "3.3.01.29"}
CUENTA_TRASLADOS = "3.3.01.52"


def _norm(s):
    """minúsculas sin tildes para matchear palabras clave en glosas."""
    s = unicodedata.normalize("NFD", s or "")
    return "".join(c for c in s if unicodedata.category(c) != "Mn").lower()


def categoria(codigo, glosa):
    """Categoría de dashboard a partir de la cuenta contable + glosa."""
    g = _norm(glosa)
    if codigo == "3.1.01.02":
        return "Petróleo"
    if codigo in ("3.1.01.04", "3.1.01.07"):
        return "Seguros"
    if codigo == "3.3.01.28":
        return "Arriendo Sitios"
    if codigo == CUENTA_TRASLADOS:
        if "peaje" in g or "tag " in g or g.endswith("tag") or "autopista" in g:
            return "TAG y Peajes"
        return "Traslados"
    if codigo in CUENTAS_MANTENCION:
        if any(k in g for k in ("neumatico", "ntco", "vulca", "llanta", "alineacion")):
            return "Neumáticos"
        if "hermeticidad" in g:
            return "Hermeticidad"
        if "revision tecnica" in g or "rev tecnica" in g or "rev. tecnica" in g:
            return "Revisión Técnica"
        if "extintor" in g:
            return "Extintores"
        if "antiderrame" in g or "epp" in g:
            return "EPP"
        return "Mantención"
    return "Otros"


def odoo_connect():
    common = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/common")
    uid = common.authenticate(ODOO_DB, ODOO_USER, ODOO_KEY, {})
    if not uid:
        raise SystemExit("❌ XML-RPC auth failed — revisa ODOO_USER / ODOO_KEY")
    models = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/object", allow_none=True)
    return uid, models


def sr(models, uid, model, domain, fields, limit=20000, order=None):
    kw = {"fields": fields, "limit": limit}
    if order:
        kw["order"] = order
    return models.execute_kw(ODOO_DB, uid, ODOO_KEY, model, "search_read", [domain], kw)


def mes_idx(date_str):
    """índice 0-11 dentro de MESES, o None si cae fuera del año."""
    m = date_str[:7]
    return MESES.index(m) if m in MESES else None


def main():
    uid, models = odoo_connect()

    # -- cuentas contables -------------------------------------------------
    accs = sr(models, uid, "account.account",
              [["code", "in", list(CUENTAS_FLOTA)]], ["id", "code"])
    acc_code = {a["id"]: a["code"] for a in accs}
    if len(acc_code) != len(CUENTAS_FLOTA):
        faltan = set(CUENTAS_FLOTA) - set(acc_code.values())
        raise SystemExit(f"❌ cuentas no encontradas en Odoo: {faltan}")

    # -- GL: total flota por cuenta × mes (incluye asientos SIN analítica) --
    gl = sr(models, uid, "account.move.line",
            [["account_id", "in", list(acc_code)],
             ["date", ">=", f"{YEAR}-01-01"], ["date", "<=", f"{YEAR}-12-31"],
             ["parent_state", "=", "posted"]],
            ["date", "balance", "account_id"])
    flota_gl = {c: [0.0] * 12 for c in CUENTAS_FLOTA}
    for l in gl:
        i = mes_idx(l["date"])
        if i is not None:
            flota_gl[acc_code[l["account_id"][0]]][i] += l["balance"]

    # -- Analítica: gasto por camión × cuenta × mes + movimientos ----------
    al = sr(models, uid, "account.analytic.line",
            [["general_account_id", "in", list(acc_code)],
             ["date", ">=", f"{YEAR}-01-01"], ["date", "<=", f"{YEAR}-12-31"]],
            ["date", "amount", "name", "account_id", "general_account_id"],
            order="date asc")
    camiones = {}
    asignado = {c: [0.0] * 12 for c in CUENTAS_FLOTA}
    movimientos = []
    for l in al:
        i = mes_idx(l["date"])
        if i is None or not l["account_id"]:
            continue
        patente = l["account_id"][1]
        codigo = acc_code[l["general_account_id"][0]]
        monto = -l["amount"]  # analítica de gasto viene negativa
        cat = categoria(codigo, l["name"] or "")
        cam = camiones.setdefault(patente, {"por_categoria": {}, "total": [0.0] * 12})
        cam["por_categoria"].setdefault(cat, [0.0] * 12)[i] += monto
        cam["total"][i] += monto
        asignado[codigo][i] += monto
        movimientos.append({
            "d": l["date"], "camion": patente, "cat": cat,
            "glosa": (l["name"] or "").replace("\n", " ").strip()[:80],
            "monto": round(monto),
        })

    # -- GL sin distribución analítica (queda visible, no se esconde) ------
    sin_asignar = {}
    for c in CUENTAS_FLOTA:
        delta = [flota_gl[c][i] - asignado[c][i] for i in range(12)]
        if any(abs(d) > 1000 for d in delta):
            sin_asignar[c] = [round(d) for d in delta]

    # -- Litros diesel facturados flota por mes (para $/litro real) --------
    inv = sr(models, uid, "account.move.line",
             [["product_id", "=", DIESEL_PRODUCT_ID],
              ["date", ">=", f"{YEAR}-01-01"], ["date", "<=", f"{YEAR}-12-31"],
              ["parent_state", "=", "posted"],
              ["move_id.move_type", "in", ["out_invoice", "out_refund"]]],
             ["date", "quantity", "move_type"])
    litros = [0.0] * 12
    for l in inv:
        i = mes_idx(l["date"])
        if i is not None:
            # NC se restan manualmente (Odoo no las netea)
            litros[i] += -l["quantity"] if l["move_type"] == "out_refund" else l["quantity"]

    out = {
        "generated_utc": datetime.now(timezone.utc).isoformat(timespec="minutes"),
        "year": YEAR,
        "meses": MESES,
        "cuentas": CUENTAS_FLOTA,
        "flota_gl": {c: [round(v) for v in vals] for c, vals in flota_gl.items()},
        "camiones": {
            p: {
                "por_categoria": {k: [round(x) for x in v]
                                  for k, v in cam["por_categoria"].items()},
                "total": [round(x) for x in cam["total"]],
            }
            for p, cam in sorted(camiones.items())
        },
        "sin_asignar": sin_asignar,
        "litros_flota": [round(v) for v in litros],
        "movimientos": movimientos,
    }

    # -- validación mínima antes de escribir --------------------------------
    mes_actual = datetime.now(timezone.utc).month - 1  # índice
    total_ytd = sum(sum(v[:mes_actual + 1]) for v in out["flota_gl"].values())
    if total_ytd < 10_000_000:
        raise SystemExit(f"❌ gasto flota YTD implausible (${total_ytd:,.0f}) — no escribo JSON")
    if len(out["camiones"]) < 5:
        raise SystemExit(f"❌ solo {len(out['camiones'])} camiones con gasto — no escribo JSON")

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, separators=(",", ":"))
    print(f"✅ {OUTPUT_PATH}: {len(out['camiones'])} camiones, "
          f"{len(movimientos)} movimientos, gasto YTD ${total_ytd:,.0f}")


if __name__ == "__main__":
    main()
