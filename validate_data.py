#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Validación de plausibilidad de los JSON del pipeline (monitoreo de CALIDAD).

Uso: python3 validate_data.py archivo1.json [archivo2.json ...]

Se ejecuta en GitHub Actions DESPUÉS del extract y ANTES del commit.
Si algún check falla, sale con exit 1 → el job falla → NO se commitea el dato
malo (el dashboard conserva el dato anterior) y GitHub manda el email de falla.

Filosofía: convertir "no explotó" en "el dato es plausible". Los umbrales son
pisos MUY conservadores (≈ la mitad del peor valor histórico observado) para
detectar datos rotos/vacíos sin dar falsas alarmas por semanas flojas.
Si el negocio cambia de escala, ajustar aquí (sección UMBRALES).

Comparación contra el dato de ayer: se usa `git show HEAD:<archivo>` (en CI el
extract todavía no está commiteado, así que HEAD = último dato bueno).
"""

import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone

# ---------------------------------------------------------------- UMBRALES --
FRESCURA_HORAS = 26            # `updated` debe ser de las últimas N horas
ENCOGIMIENTO_MAX = 0.50        # falla si el archivo pesa menos del 50% que ayer

# route-data.json son los pedidos DEL DÍA: fin de semana o madrugada sin
# pedidos lo dejan casi vacío (≈85 bytes vs ~7k de un día normal) y el check
# de encogimiento botaba el cron sábado/domingo/lunes temprano (runs 118-120).
SIN_CHECK_ENCOGIMIENTO = {"route-data.json"}

# crm-data.json (histórico jul-2026: semanas completas 116k–257k litros,
# 1.415 clientes activos, 27 ejecutivos, funnel 7 etapas / ~1.278 leads)
CRM_MIN_LITROS_SEMANA_COMPLETA = 50_000   # última semana ENAP cerrada
CRM_MIN_LITROS_MES_ANTERIOR = 300_000     # meses reales: 683k–1.0M
CRM_MIN_CLIENTES_ACTIVOS = 700
CRM_MIN_EJECUTIVOS = 5
CRM_MIN_ETAPAS_FUNNEL = 5
CRM_MIN_LEADS_FUNNEL = 100

# ceo-data.json (histórico: 16 semanas, CxC ~$605M / 268 facturas abiertas)
CEO_MIN_SEMANAS = 10
CEO_MIN_LITROS_SEMANA_COMPLETA = 50_000
CEO_MIN_CXC_TOTAL = 50_000_000
CEO_MIN_FACTURAS_ABIERTAS = 40

# costos-data.json (histórico ago-2026: gasto flota YTD ~$120M, 7 camiones,
# ~560 movimientos analíticos, litros YTD ~6.4M)
COSTOS_MIN_GASTO_YTD = 10_000_000
COSTOS_MIN_CAMIONES = 5
COSTOS_MIN_MOVIMIENTOS = 50
# -----------------------------------------------------------------------------

errores = []
avisos = []


def fail(archivo, msg):
    errores.append(f"❌ {archivo}: {msg}")


def warn(archivo, msg):
    avisos.append(f"⚠️  {archivo}: {msg}")


def tamano_anterior(archivo):
    """Bytes del archivo en HEAD (el último commit, o sea el dato de ayer)."""
    try:
        out = subprocess.run(
            ["git", "show", f"HEAD:{archivo}"],
            capture_output=True, check=True,
        )
        return len(out.stdout)
    except subprocess.CalledProcessError:
        return None  # archivo nuevo, sin versión previa


def contenido_anterior(archivo):
    try:
        out = subprocess.run(
            ["git", "show", f"HEAD:{archivo}"],
            capture_output=True, check=True,
        )
        return json.loads(out.stdout)
    except (subprocess.CalledProcessError, json.JSONDecodeError):
        return None


def check_frescura(archivo, ts_str):
    """El timestamp de generación debe ser reciente (el extract corrió recién)."""
    if not ts_str:
        fail(archivo, "sin timestamp de generación (updated/generated_at)")
        return
    try:
        ts = datetime.fromisoformat(str(ts_str))
    except ValueError:
        fail(archivo, f"timestamp de generación ilegible: {ts_str!r}")
        return
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)  # los extracts estampan hora UTC en CI
    edad = datetime.now(timezone.utc) - ts
    if edad > timedelta(hours=FRESCURA_HORAS):
        fail(archivo, f"dato viejo: generado hace {edad.total_seconds()/3600:.1f}h "
                      f"(máx {FRESCURA_HORAS}h) — ¿el extract escribió el archivo?")


def check_encogimiento(archivo, data_bytes):
    prev = tamano_anterior(archivo)
    if prev is None or prev == 0:
        warn(archivo, "sin versión previa en git — se omite check de tamaño")
        return
    ratio = len(data_bytes) / prev
    if ratio < ENCOGIMIENTO_MAX:
        fail(archivo, f"el archivo se encogió a {ratio:.0%} del anterior "
                      f"({prev:,} → {len(data_bytes):,} bytes) — dato probablemente incompleto")


# ------------------------------------------------------------- por archivo --

def check_crm(archivo, d):
    check_frescura(archivo, d.get("updated"))

    # Semana comercial vigente (jueves–miércoles)
    week = d.get("week") or {}
    if not (week.get("start") and week.get("end")):
        fail(archivo, "bloque `week` sin start/end")

    # Cartera activa
    activos = (d.get("summary") or {}).get("active", 0)
    if activos < CRM_MIN_CLIENTES_ACTIVOS:
        fail(archivo, f"solo {activos} clientes activos en summary "
                      f"(mín {CRM_MIN_CLIENTES_ACTIVOS}; lo normal es ~1.400)")

    if len(d.get("executives") or []) < CRM_MIN_EJECUTIVOS:
        fail(archivo, f"solo {len(d.get('executives') or [])} ejecutivos "
                      f"(mín {CRM_MIN_EJECUTIVOS})")

    # Funnel CRM
    funnel = d.get("funnel") or []
    leads_funnel = sum(e.get("count", 0) for e in funnel)
    if len(funnel) < CRM_MIN_ETAPAS_FUNNEL or leads_funnel < CRM_MIN_LEADS_FUNNEL:
        fail(archivo, f"funnel implausible: {len(funnel)} etapas / {leads_funnel} leads "
                      f"(mín {CRM_MIN_ETAPAS_FUNNEL} etapas y {CRM_MIN_LEADS_FUNNEL} leads)")

    # Litros: OJO, los acumulados del MES pueden ser 0 legítimamente los
    # primeros días del mes (ver commit c47303e). Se valida contra la última
    # semana ENAP COMPLETADA (la penúltima entrada; la última es la vigente).
    hist = (d.get("ventas") or {}).get("weekly_history") or []
    if len(hist) < 8:
        fail(archivo, f"weekly_history con solo {len(hist)} semanas (mín 8)")
    elif (hist[-2].get("litros") or 0) < CRM_MIN_LITROS_SEMANA_COMPLETA:
        fail(archivo, f"última semana completa ({hist[-2].get('label')}) con "
                      f"{hist[-2].get('litros'):,} litros "
                      f"(mín {CRM_MIN_LITROS_SEMANA_COMPLETA:,}; histórico 116k–257k)")

    # Mes anterior cerrado (monthly_history viene ordenado: [0]=mes en curso)
    mensual = d.get("monthly_history") or []
    if len(mensual) >= 2 and (mensual[1].get("litros") or 0) < CRM_MIN_LITROS_MES_ANTERIOR:
        fail(archivo, f"mes anterior ({mensual[1].get('label')}) con "
                      f"{mensual[1].get('litros'):,} litros "
                      f"(mín {CRM_MIN_LITROS_MES_ANTERIOR:,}; histórico 683k–1.0M)")

    # Bloques estructurales que consume crm-sales.html
    for key in ("churn", "recovery", "operaciones", "pipeline", "won_deals"):
        if not d.get(key):
            fail(archivo, f"bloque `{key}` vacío o ausente")

    ops = d.get("operaciones") or {}
    if ops and not ops.get("sla_semanas"):
        fail(archivo, "operaciones.sla_semanas vacío (tab Operaciones quedaría en blanco)")


def check_ceo(archivo, d):
    check_frescura(archivo, d.get("updated"))

    weeks = d.get("weeks") or []
    if len(weeks) < CEO_MIN_SEMANAS:
        fail(archivo, f"solo {len(weeks)} semanas (mín {CEO_MIN_SEMANAS})")
    else:
        # Las weeks vienen ordenadas de más nueva a más vieja; la vigente trae
        # parcial=True. Se valida la última semana CERRADA.
        completas = [w for w in weeks if not w.get("parcial")]
        if not completas:
            fail(archivo, "ninguna semana cerrada (parcial=False) en `weeks`")
        elif (completas[0].get("litros") or 0) < CEO_MIN_LITROS_SEMANA_COMPLETA:
            fail(archivo, f"última semana cerrada ({completas[0].get('label')}) con "
                          f"{completas[0].get('litros'):,} litros "
                          f"(mín {CEO_MIN_LITROS_SEMANA_COMPLETA:,})")

    if (d.get("total_cash") or 0) <= 0:
        fail(archivo, f"total_cash = {d.get('total_cash')} — lectura de bancos rota")
    if len(d.get("banks") or []) < 3:
        fail(archivo, f"solo {len(d.get('banks') or [])} bancos (mín 3)")

    rec = d.get("receivables") or {}
    if (rec.get("total_due") or 0) < CEO_MIN_CXC_TOTAL:
        fail(archivo, f"CxC total ${rec.get('total_due', 0):,} "
                      f"(mín ${CEO_MIN_CXC_TOTAL:,}; lo normal es ~$600M)")
    if (rec.get("open_invoices") or 0) < CEO_MIN_FACTURAS_ABIERTAS:
        fail(archivo, f"solo {rec.get('open_invoices', 0)} facturas abiertas "
                      f"(mín {CEO_MIN_FACTURAS_ABIERTAS})")

    riesgo = d.get("riesgo") or {}
    if (riesgo.get("cubierto", 0) + riesgo.get("no_cubierto", 0)) <= 0:
        fail(archivo, "riesgo AVLA: cubierto + no_cubierto = 0 — match RUT↔Odoo roto")

    for key in ("sla", "churn", "enap"):
        if not d.get(key):
            fail(archivo, f"bloque `{key}` vacío o ausente")


def check_route(archivo, d):
    # Route es chico y volátil (día sin pedidos = rutas vacías legítimas):
    # solo estructura y frescura.
    check_frescura(archivo, d.get("generated_at"))
    if "routes" not in d or not isinstance(d.get("routes"), list):
        fail(archivo, "sin lista `routes`")
    if not isinstance(d.get("total_orders"), int):
        fail(archivo, "sin `total_orders`")


def check_riesgo_historico(archivo, d):
    # Histórico append-only: NUNCA debe achicarse (protege la serie con el
    # quiebre metodológico AVLA de jul-2026 documentado en CLAUDE.md).
    if not isinstance(d, list):
        fail(archivo, "no es una lista")
        return
    prev = contenido_anterior(archivo)
    if isinstance(prev, list) and len(d) < len(prev):
        fail(archivo, f"el histórico se ACHICÓ: {len(prev)} → {len(d)} snapshots "
                      f"— jamás debe perder entradas")


def check_costos(archivo, d):
    check_frescura(archivo, d.get("generated_utc"))
    camiones = d.get("camiones") or {}
    if len(camiones) < COSTOS_MIN_CAMIONES:
        fail(archivo, f"solo {len(camiones)} camiones con gasto "
                      f"(mínimo {COSTOS_MIN_CAMIONES})")
    gasto_ytd = sum(sum(v) for v in (d.get("flota_gl") or {}).values())
    if gasto_ytd < COSTOS_MIN_GASTO_YTD:
        fail(archivo, f"gasto flota YTD implausible: ${gasto_ytd:,.0f} "
                      f"(mínimo ${COSTOS_MIN_GASTO_YTD:,.0f})")
    if len(d.get("movimientos") or []) < COSTOS_MIN_MOVIMIENTOS:
        fail(archivo, f"solo {len(d.get('movimientos') or [])} movimientos analíticos")
    if len(d.get("meses") or []) != 12:
        fail(archivo, "bloque `meses` no tiene 12 meses")


CHECKS = {
    "crm-data.json": check_crm,
    "costos-data.json": check_costos,
    "ceo-data.json": check_ceo,
    "route-data.json": check_route,
    "riesgo-historico.json": check_riesgo_historico,
}


def validar(archivo):
    try:
        with open(archivo, "rb") as f:
            raw = f.read()
    except FileNotFoundError:
        fail(archivo, "el archivo NO EXISTE — el extract no lo escribió")
        return
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        fail(archivo, f"JSON inválido: {e}")
        return

    if archivo not in SIN_CHECK_ENCOGIMIENTO:
        check_encogimiento(archivo, raw)

    checker = CHECKS.get(archivo)
    if checker:
        checker(archivo, data)
    else:
        warn(archivo, "sin checks específicos definidos — solo JSON válido + tamaño")


def main():
    archivos = sys.argv[1:]
    if not archivos:
        print("Uso: python3 validate_data.py archivo1.json [archivo2.json ...]")
        sys.exit(2)

    for archivo in archivos:
        validar(archivo)

    for a in avisos:
        print(a)
    if errores:
        print(f"\n{'='*60}\nVALIDACIÓN FALLÓ — el dato NO se commitea:\n")
        for e in errores:
            print(e)
        print(f"\nEl dashboard conserva el último dato bueno. Revisar el extract\n"
              f"o, si el negocio cambió de escala, ajustar umbrales en validate_data.py.")
        sys.exit(1)

    print(f"✅ Validación OK: {', '.join(archivos)}")


if __name__ == "__main__":
    main()
