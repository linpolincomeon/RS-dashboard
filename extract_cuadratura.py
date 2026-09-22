#!/usr/bin/env python3
"""Cuadratura Odoo <-> SimpliRoute -> cuadratura-data.json

Control operacional INTRADÍA (cron horario en jornada laboral):
  1. RUTAS: todo lo aprobado en Odoo (picking OUT pendiente con fecha de hoy
     o atrasado, con sale.order) debe tener visita planificada en SimpliRoute.
     También al revés: visitas en ruta cuya orden no está aprobada en Odoo.
  2. LITROS: por sale.order, litros pedidos (línea diésel de la SO) vs
     entregados en SimpliRoute (checkout) vs facturados en Odoo (facturas
     posted por invoice_origin, restando NC). Al final del día ambos totales
     deben cuadrar; lo entregado sin factura queda como "por facturar".

Join: `reference` de la visita trae picking + sale.order ("TJVS/OUT/01813 -
S36464"). El match primario es la S# (una re-planificación crea picking nuevo
pero conserva la orden). Consultas livianas a Odoo (4-6 search_read chicos):
no compite con los extractores nocturnos.

⚠ FALLA_ES_ENTREGA (SUGAL, ARRIGONI): su facturación impide cerrar la visita
  en la app — failed/partial de esos clientes cuentan como entrega (misma
  regla que extract_simpliroute.py). Sus litros de visita no son confiables:
  la cuadratura usa los litros pedidos de la SO como fallback.
⚠ Los litros por visita vienen de extra_field_values.litros y muchas veces
  quedan vacíos hasta el checkout: mientras la visita está pending se usa el
  pedido de la SO.

Token SimpliRoute: env SIMPLIROUTE_TOKEN o ~/.simpliroute_token.
Odoo: ODOO_URL/ODOO_DB/ODOO_USER/ODOO_KEY (key local: ~/.odoo_key).
"""
import json
import os
import re
import sys
import time
import urllib.request
import urllib.error
import xmlrpc.client
import datetime as dt
from collections import defaultdict
from zoneinfo import ZoneInfo

TZ = ZoneInfo('America/Santiago')
DIESEL_PRODUCT_ID = 14
VENTANA_DIAS = 7          # cuadratura de litros (hoy incluido)
TOL_LITROS = 1.0          # diferencia tolerada entregado vs facturado
FALLA_ES_ENTREGA = ('SUGAL', 'ARRIGONI')  # mantener igual a extract_simpliroute.py

ODOO_URL = os.environ.get('ODOO_URL', 'https://tomenergy.cl')
ODOO_DB = os.environ.get('ODOO_DB', 'PRODUCCION')
ODOO_USER = os.environ.get('ODOO_USER', 'p@tomenergy.cl')
ODOO_KEY = os.environ.get('ODOO_KEY', '')
if not ODOO_KEY:
    try:
        ODOO_KEY = open(os.path.expanduser('~/.odoo_key')).read().strip()
    except FileNotFoundError:
        sys.exit('ERROR: sin ODOO_KEY (env o ~/.odoo_key)')

SR_TOKEN = os.environ.get('SIMPLIROUTE_TOKEN', '').strip()
if not SR_TOKEN:
    try:
        SR_TOKEN = open(os.path.expanduser('~/.simpliroute_token')).read().strip()
    except FileNotFoundError:
        sys.exit('ERROR: sin SIMPLIROUTE_TOKEN (env o ~/.simpliroute_token)')


# ── helpers Odoo ────────────────────────────────────────────────────────────
def connect():
    common = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/common')
    uid = common.authenticate(ODOO_DB, ODOO_USER, ODOO_KEY, {})
    if not uid:
        sys.exit('ERROR: autenticación Odoo falló')
    return xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/object'), uid


def sr_odoo(models, uid, model, domain, fields, limit=5000, order='id desc'):
    waits = [15, 30, 60]
    for attempt in range(4):
        try:
            return models.execute_kw(
                ODOO_DB, uid, ODOO_KEY, model, 'search_read',
                [domain], {'fields': fields, 'limit': limit, 'order': order})
        except (xmlrpc.client.ProtocolError, ConnectionError, OSError) as e:
            if attempt == 3:
                raise
            print(f'  [retry {attempt+1}] {model}: {e}', file=sys.stderr)
            time.sleep(waits[attempt])


def safe_name(m2o):
    return m2o[1] if isinstance(m2o, (list, tuple)) and len(m2o) > 1 else ''


# ── helpers SimpliRoute (mismos de extract_simpliroute.py) ──────────────────
def sr_get(path, reintentos=3):
    for i in range(reintentos):
        try:
            req = urllib.request.Request(
                'https://api.simpliroute.com/v1' + path,
                headers={'Authorization': 'Token ' + SR_TOKEN})
            with urllib.request.urlopen(req, timeout=45) as r:
                return json.loads(r.read().decode())
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as e:
            code = getattr(e, 'code', None)
            if code and 400 <= code < 500:
                raise
            if i == reintentos - 1:
                raise
            time.sleep(3 * (i + 1))


def litros_de(v):
    extra = v.get('extra_field_values') or {}
    raw = str(extra.get('litros', '')).strip()
    # 1° el número tal cual; solo si falla, limpiar formato chileno ("5.700").
    # OJO: nunca al revés — replace('.','') convierte 5700.0 en 57000 (×10).
    for candidato in (raw, raw.replace('.', '').replace(',', '.')):
        try:
            return float(candidato)
        except (TypeError, ValueError):
            pass
    try:
        return float(v.get('load') or 0)
    except (TypeError, ValueError):
        return 0.0


def hora_local(iso_utc):
    if not iso_utc:
        return None
    try:
        t = dt.datetime.fromisoformat(str(iso_utc).replace('Z', '+00:00'))
        return t.astimezone(TZ).strftime('%H:%M')
    except ValueError:
        return None


def main():
    ahora = dt.datetime.now(TZ)
    hoy = ahora.date()
    # Odoo guarda datetimes en UTC. Chile: UTC-4 invierno (abr-sep por regla del
    # pipeline, ver extract_crm.py), UTC-3 verano.
    off = 4 if 4 <= hoy.month <= 8 else 3
    fin_hoy_utc = (dt.datetime.combine(hoy, dt.time(23, 59, 59))
                   + dt.timedelta(hours=off)).strftime('%Y-%m-%d %H:%M:%S')

    # ── SimpliRoute: visitas de la ventana + mañana (rutas adelantadas) ──
    visitas = []
    for i in range(-1, VENTANA_DIAS):  # -1 = mañana
        f = (hoy - dt.timedelta(days=i)).isoformat()
        for v in sr_get(f'/routes/visits/?planned_date={f}') or []:
            ref = str(v.get('reference') or '')
            m_so = re.search(r'\bS\d+\b', ref)
            so = m_so.group(0) if m_so else None
            m_pk = re.search(r'\b[A-Z]+/OUT/\d+\b', ref)
            pick = m_pk.group(0) if m_pk else None
            cliente = str(v.get('title') or '').strip()
            especial = (v.get('status') in ('failed', 'partial') and
                        any(c in cliente.upper() for c in FALLA_ES_ENTREGA))
            visitas.append(dict(
                fecha=f, so=so, picking=pick, cliente=cliente,
                status=v.get('status'), especial=especial,
                litros=litros_de(v), ref=ref,
                checkout=hora_local(v.get('checkout_time')),
            ))

    models, uid = connect()

    # ── 1. RUTAS: aprobado en Odoo vs planificado en Simpli ──
    # Pendiente = picking OUT no hecho/cancelado con fecha programada hasta hoy
    # (incluye atrasados de días previos: siguen debiendo salir en ruta).
    picks = sr_odoo(models, uid, 'stock.picking', [
        ['picking_type_code', '=', 'outgoing'],
        ['state', 'in', ['confirmed', 'waiting', 'assigned']],
        ['scheduled_date', '<=', fin_hoy_utc],
    ], ['name', 'origin', 'state', 'scheduled_date', 'partner_id'], limit=500)
    # Solo despachos de venta (origin S#); devoluciones y transferencias quedan fuera.
    picks = [p for p in picks if re.fullmatch(r'S\d+', str(p.get('origin') or ''))]

    so_names = sorted({p['origin'] for p in picks})
    # visitas útiles (no canceladas) indexadas por SO y por picking
    v_por_so, v_por_pick = defaultdict(list), defaultdict(list)
    for v in visitas:
        if v['status'] == 'canceled':
            continue
        if v['so']:
            v_por_so[v['so']].append(v)
        if v['picking']:
            v_por_pick[v['picking']].append(v)

    # litros pedidos por SO (línea diésel) — para pendientes Y para la cuadratura
    sos_visitas = sorted({v['so'] for v in visitas if v['so']})
    todos_sos = sorted(set(so_names) | set(sos_visitas))
    pedido_por_so = defaultdict(float)
    camion_por_so = {}
    for i in range(0, len(todos_sos), 200):
        chunk = todos_sos[i:i + 200]
        for l in sr_odoo(models, uid, 'sale.order.line', [
            ['order_id.name', 'in', chunk], ['product_id', '=', DIESEL_PRODUCT_ID],
        ], ['order_id', 'product_uom_qty'], limit=2000):
            pedido_por_so[safe_name(l['order_id']).split(' ')[0]] += l['product_uom_qty'] or 0
    so_rows = sr_odoo(models, uid, 'sale.order', [['name', 'in', todos_sos]],
                      ['name', 'state', 'partner_id', 'warehouse_id'], limit=2000)
    so_por_nombre = {o['name']: o for o in so_rows}
    for o in so_rows:
        camion_por_so[o['name']] = safe_name(o.get('warehouse_id'))

    hoy_s = hoy.isoformat()
    manana_s = (hoy + dt.timedelta(days=1)).isoformat()
    pendientes = []
    for p in sorted(picks, key=lambda x: x['scheduled_date']):
        so = p['origin']
        vs = v_por_so.get(so, []) or v_por_pick.get(p['name'], [])
        v_hoy = [v for v in vs if v['fecha'] == hoy_s]
        v_man = [v for v in vs if v['fecha'] == manana_s]
        if v_hoy:
            estado, v = 'ruteada', v_hoy[0]
        elif v_man:
            estado, v = 'ruteada_manana', v_man[0]
        else:
            estado, v = 'sin_ruta', None
        sched_cl = (dt.datetime.strptime(p['scheduled_date'][:19], '%Y-%m-%d %H:%M:%S')
                    - dt.timedelta(hours=off))
        pendientes.append(dict(
            picking=p['name'], so=so, cliente=safe_name(p.get('partner_id')),
            camion=camion_por_so.get(so, ''),
            litros=round(pedido_por_so.get(so, 0)),
            programado=sched_cl.strftime('%d-%m %H:%M'),
            atrasado=sched_cl.date() < hoy,
            estado=estado,
            visita_status=v['status'] if v else None,
        ))

    # Huérfanas: visitas de HOY activas cuya orden no está aprobada en Odoo
    # (SO cancelada/borrador o inexistente). Las sin referencia van aparte.
    huerfanas, sin_ref = [], []
    for v in visitas:
        if v['fecha'] != hoy_s or v['status'] == 'canceled':
            continue
        if not v['so']:
            if 'devoluc' not in v['ref'].lower():
                sin_ref.append(dict(cliente=v['cliente'], ref=v['ref'],
                                    status=v['status'], litros=round(v['litros'])))
            continue
        o = so_por_nombre.get(v['so'])
        if not o or o['state'] not in ('sale', 'done'):
            huerfanas.append(dict(so=v['so'], cliente=v['cliente'],
                                  estado_odoo=(o['state'] if o else 'no existe'),
                                  status=v['status'], litros=round(v['litros'])))

    # ── 2. LITROS: entregado (Simpli) vs facturado (Odoo), por SO ──
    # Facturas posted por invoice_origin; NC (out_refund) se restan — Odoo no
    # las netea (convención del repo).
    fact_por_so = defaultdict(float)
    facturas_por_so = defaultdict(list)
    fecha_fact_por_so = {}
    inv_rows = []
    for i in range(0, len(todos_sos), 200):
        inv_rows += sr_odoo(models, uid, 'account.move', [
            ['invoice_origin', 'in', todos_sos[i:i + 200]],
            ['move_type', 'in', ['out_invoice', 'out_refund']],
            ['state', '=', 'posted'],
        ], ['name', 'invoice_origin', 'invoice_date', 'move_type'], limit=2000)
    if inv_rows:
        inv_ids = [r['id'] for r in inv_rows]
        signo = {r['id']: (-1 if r['move_type'] == 'out_refund' else 1) for r in inv_rows}
        origen = {r['id']: r['invoice_origin'] for r in inv_rows}
        for i in range(0, len(inv_ids), 200):
            for l in sr_odoo(models, uid, 'account.move.line', [
                ['move_id', 'in', inv_ids[i:i + 200]],
                ['product_id', '=', DIESEL_PRODUCT_ID],
            ], ['move_id', 'quantity'], limit=5000):
                mid = l['move_id'][0]
                fact_por_so[origen[mid]] += signo[mid] * (l['quantity'] or 0)
        for r in inv_rows:
            facturas_por_so[r['invoice_origin']].append(r['name'])
            if r['move_type'] == 'out_invoice':
                fecha_fact_por_so.setdefault(r['invoice_origin'], r['invoice_date'])

    # una fila por SO ENTREGADA en la ventana (completed/especial/partial)
    ent_por_so = defaultdict(lambda: dict(litros=0.0, fechas=set(), n=0,
                                          especial=False, parcial=False, cliente=''))
    for v in visitas:
        if v['fecha'] > hoy_s or not v['so']:
            continue
        if v['status'] == 'completed' or v['especial'] or v['status'] == 'partial':
            e = ent_por_so[v['so']]
            e['litros'] += v['litros']
            e['fechas'].add(v['fecha'])
            e['n'] += 1
            e['especial'] |= v['especial']
            e['parcial'] |= (v['status'] == 'partial' and not v['especial'])
            e['cliente'] = e['cliente'] or v['cliente']

    por_so = []
    for so, e in ent_por_so.items():
        pedido = pedido_por_so.get(so, 0)
        # litros de la visita vacíos (SUGAL, o checkout sin dato) → vale el pedido
        entregado = e['litros'] if e['litros'] > 0 else pedido
        facturado = fact_por_so.get(so, 0)
        if facturado <= 0 and so not in facturas_por_so:
            estado = 'por_facturar'
        elif abs(entregado - facturado) > TOL_LITROS:
            estado = 'diferencia'
        else:
            estado = 'ok'
        por_so.append(dict(
            so=so, fecha=min(e['fechas']), cliente=e['cliente'],
            camion=camion_por_so.get(so, ''),
            especial=e['especial'], parcial=e['parcial'], visitas=e['n'],
            pedido=round(pedido), entregado=round(entregado),
            facturado=round(facturado),
            facturas=sorted(set(facturas_por_so.get(so, []))),
            estado=estado,
        ))
    por_so.sort(key=lambda x: (x['fecha'], x['so']), reverse=True)

    # totales por día (de lo entregado el día X: cuánto ya está facturado)
    dias = []
    for i in range(VENTANA_DIAS):
        f = (hoy - dt.timedelta(days=i)).isoformat()
        del_dia = [r for r in por_so if r['fecha'] == f]
        dias.append(dict(
            fecha=f,
            entregas=len(del_dia),
            entregado=sum(r['entregado'] for r in del_dia),
            facturado=sum(r['facturado'] for r in del_dia),
            por_facturar=sum(r['entregado'] for r in del_dia
                             if r['estado'] == 'por_facturar'),
            sin_factura=sum(1 for r in del_dia if r['estado'] == 'por_facturar'),
        ))

    # Facturado HOY fuera de SimpliRoute (diésel posted hoy cuyo origen no tuvo
    # visita en la ventana): la otra dirección de la cuadratura del día.
    inv_hoy = sr_odoo(models, uid, 'account.move', [
        ['move_type', '=', 'out_invoice'], ['state', '=', 'posted'],
        ['invoice_date', '=', hoy_s],
    ], ['name', 'invoice_origin', 'partner_id'], limit=1000)
    fuera_simpli, fact_hoy_total = [], 0.0
    if inv_hoy:
        ids_hoy = [r['id'] for r in inv_hoy]
        qty_hoy = defaultdict(float)
        for i in range(0, len(ids_hoy), 200):
            for l in sr_odoo(models, uid, 'account.move.line', [
                ['move_id', 'in', ids_hoy[i:i + 200]],
                ['product_id', '=', DIESEL_PRODUCT_ID],
            ], ['move_id', 'quantity'], limit=5000):
                qty_hoy[l['move_id'][0]] += l['quantity'] or 0
        con_visita = set(ent_por_so)
        for r in inv_hoy:
            q = qty_hoy.get(r['id'], 0)
            if q <= 0:
                continue  # factura sin diésel
            fact_hoy_total += q
            if (r.get('invoice_origin') or '') not in con_visita:
                fuera_simpli.append(dict(
                    factura=r['name'], so=r.get('invoice_origin') or '—',
                    cliente=safe_name(r.get('partner_id')), litros=round(q)))

    entregado_hoy = sum(r['entregado'] for r in por_so if r['fecha'] == hoy_s)

    # ── validación integrada ──
    comp_ventana = sum(1 for r in por_so)
    if comp_ventana < 5 and hoy.weekday() < 5:
        sys.exit(f'ERROR: solo {comp_ventana} entregas en {VENTANA_DIAS} días — '
                 f'¿API SimpliRoute rota? No se escribe el JSON')

    out = dict(
        generated_utc=dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%dT%H:%M+00:00'),
        hoy=hoy_s,
        desde=(hoy - dt.timedelta(days=VENTANA_DIAS - 1)).isoformat(),
        rutas=dict(
            pendientes=pendientes,
            huerfanas=huerfanas,
            sin_ref=sin_ref,
            resumen=dict(
                total=len(pendientes),
                ruteadas=sum(1 for p in pendientes if p['estado'] == 'ruteada'),
                ruteadas_manana=sum(1 for p in pendientes if p['estado'] == 'ruteada_manana'),
                sin_ruta=sum(1 for p in pendientes if p['estado'] == 'sin_ruta'),
                huerfanas=len(huerfanas),
            ),
        ),
        litros=dict(
            por_so=por_so,
            dias=dias,
            por_facturar_total=sum(r['entregado'] for r in por_so
                                   if r['estado'] == 'por_facturar'),
            entregado_hoy=round(entregado_hoy),
            facturado_hoy=round(fact_hoy_total),
            fuera_simpli=fuera_simpli,
        ),
    )
    with open('cuadratura-data.json', 'w', encoding='utf-8') as fh:
        json.dump(out, fh, ensure_ascii=False, indent=1)

    r = out['rutas']['resumen']
    print(f"cuadratura-data.json  {out['desde']} -> {hoy_s}  "
          f"({ahora.strftime('%H:%M')} Chile)")
    print(f"  rutas: {r['total']} pendientes · {r['ruteadas']} ruteadas · "
          f"{r['sin_ruta']} SIN RUTA · {r['huerfanas']} huérfanas")
    print(f"  litros hoy: entregado {out['litros']['entregado_hoy']:,} L · "
          f"facturado {out['litros']['facturado_hoy']:,.0f} L · "
          f"por facturar (7d) {out['litros']['por_facturar_total']:,} L")
    for x in por_so:
        if x['estado'] != 'ok':
            print(f"    {x['fecha']} {x['so']} {x['cliente'][:28]:28} "
                  f"ent {x['entregado']:>6,} fact {x['facturado']:>6,} [{x['estado']}]")


if __name__ == '__main__':
    main()
