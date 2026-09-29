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
import http.client
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
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError,
                http.client.IncompleteRead) as e:
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


def cam2(nombre):
    """'HHPT-71' / 'HHPT-71: Recepciones' / 'VD/Existencias' -> 'HH' / 'VD'."""
    return str(nombre or '').strip().upper()[:2]


def extract_cierre(models, uid, hoy, off, visitas, picks, pedido_por_so_ref):
    """Checklist del CIERRE DIARIO (instructivo operativo, fases 2-6), solo lectura.

    Cada fase devuelve solo lo que NO cuadra: el operador corrige eso en Odoo
    en vez de recorrer las 5 pestañas. Fase 1 (CC ENAP/Adquim) y el lado Drive
    de la fase 5 no son legibles desde aquí (portal ENAP / Sheet sin credenciales).
    """
    def a_utc(d, h='00:00:00'):
        return (dt.datetime.strptime(f'{d} {h}', '%Y-%m-%d %H:%M:%S')
                + dt.timedelta(hours=off)).strftime('%Y-%m-%d %H:%M:%S')

    def a_cl(s):
        return (dt.datetime.strptime(s[:19], '%Y-%m-%d %H:%M:%S')
                - dt.timedelta(hours=off))

    hoy_s = hoy.isoformat()
    manana_s = (hoy + dt.timedelta(days=1)).isoformat()
    ayer_s = (hoy - dt.timedelta(days=1)).isoformat()

    # ── Fase 2: compras (cargas) de ayer y hoy + recepciones sin validar ──
    pos = sr_odoo(models, uid, 'purchase.order', [
        ['state', 'in', ['purchase', 'done']],
        '|', ['date_order', '>=', a_utc(ayer_s)], ['receipt_status', '!=', 'full'],
    ], ['name', 'partner_id', 'picking_type_id', 'date_order', 'receipt_status',
         'picking_ids'], limit=500)
    # OC duplicada y revertida: su recepción tiene una devolución (OUT) validada
    devueltas = set()
    pk_ids = sorted({i for p in pos for i in p.get('picking_ids') or []})
    if pk_ids:
        pk_out = {k['id'] for k in sr_odoo(models, uid, 'stock.picking', [
            ['id', 'in', pk_ids], ['picking_type_code', '=', 'outgoing'],
            ['state', '=', 'done']], ['id'], limit=2000)}
        devueltas = {p['id'] for p in pos if set(p.get('picking_ids') or []) & pk_out}
    po_lin = defaultdict(lambda: dict(litros=0.0, correccion=None, recibido=0.0, precio=0.0))
    if pos:
        for l in sr_odoo(models, uid, 'purchase.order.line', [
            ['order_id', 'in', [p['id'] for p in pos]],
            ['product_id', 'in', [DIESEL_PRODUCT_ID, 15]],   # 15 = Corrección Diesel
        ], ['order_id', 'product_id', 'product_qty', 'qty_received', 'price_unit'],
                limit=2000):
            x = po_lin[l['order_id'][0]]
            if l['product_id'][0] == DIESEL_PRODUCT_ID:
                x['litros'] += l['product_qty'] or 0
                x['recibido'] += l['qty_received'] or 0
                x['precio'] = l['price_unit'] or 0
            else:
                x['correccion'] = (x['correccion'] or 0) + (l['product_qty'] or 0)
    compras = []
    for p in pos:
        if p['id'] not in po_lin:
            continue  # compra que no es diésel (activos, servicios)
        x = po_lin[p['id']]
        f = a_cl(p['date_order'])
        compras.append(dict(
            po=p['name'], proveedor=safe_name(p.get('partner_id')),
            cam=cam2(safe_name(p.get('picking_type_id'))),
            fecha=f.date().isoformat(), hora=f.strftime('%H:%M'),
            litros=round(x['litros']),
            correccion=None if x['correccion'] is None else round(x['correccion']),
            litros_real=round(x['litros'] + (x['correccion'] or 0)),
            precio=round(x['precio'], 2),
            recibida=p.get('receipt_status') == 'full',
            devuelta=p['id'] in devueltas,
        ))
    compras.sort(key=lambda c: (c['fecha'], c['hora']), reverse=True)

    # Facturas de proveedor (DTE): las que llegan solas quedan en BORRADOR sin
    # vínculo a la compra (línea "DIESEL" en m³ o "Del Giro" por el total).
    # Vinculada = la compra tiene invoice_ids; si no, se busca un borrador del
    # mismo proveedor con los mismos litros (±10 L por el redondeo del m³).
    po_by_name = {p['name']: p for p in pos}
    ligadas = sr_odoo(models, uid, 'purchase.order', [
        ['id', 'in', [p['id'] for p in pos]]], ['name', 'invoice_ids'], limit=500)
    inv_ids = sorted({i for p in ligadas for i in p['invoice_ids']})
    partners = sorted({p['partner_id'][0] for p in pos if p.get('partner_id')})
    bills = sr_odoo(models, uid, 'account.move', ['|', ['id', 'in', inv_ids], '&', '&',
        ['move_type', '=', 'in_invoice'], ['state', '=', 'draft'],
        ['partner_id', 'in', partners]],
        ['name', 'state', 'partner_id', 'amount_total', 'l10n_latam_document_number',
         'invoice_date'], limit=500)
    bill_lit = defaultdict(float)
    if bills:
        for l in sr_odoo(models, uid, 'account.move.line', [
            ['move_id', 'in', [b['id'] for b in bills]], ['display_type', '=', 'product'],
        ], ['move_id', 'product_id', 'quantity', 'name'], limit=5000):
            q = l['quantity'] or 0
            nombre = str(l.get('name') or '').lower()
            if l.get('product_id') and l['product_id'][0] in (DIESEL_PRODUCT_ID, 15):
                bill_lit[l['move_id'][0]] += q
            elif 'diesel' in nombre:          # DTE ENAP: cantidad en m³
                bill_lit[l['move_id'][0]] += q * 1000 if q < 100 else q
            elif 'giro' in nombre and q > 1:     # "Del Giro" x litros (1 = monto global)
                bill_lit[l['move_id'][0]] += q
    bill_by_id = {b['id']: b for b in bills}
    usadas = set()
    for p in ligadas:
        for i in p['invoice_ids']:
            usadas.add(i)
    # 1° las compras ya vinculadas; 2° borradores por mejor calce de litros
    # (asignación global por menor diferencia: 9.993 y 10.000 no se cruzan)
    sin_lig = []
    for c in compras:
        lig = next((p for p in ligadas if p['name'] == c['po']), None)
        b = next((bill_by_id[i] for i in (lig['invoice_ids'] if lig else []) if i in bill_by_id), None)
        if b:
            c.update(factura=b.get('l10n_latam_document_number') or b['name'],
                     factura_estado=b['state'], factura_total=round(b['amount_total']),
                     factura_litros=round(bill_lit.get(b['id'], 0)) or None)
        else:
            c.update(factura=None, factura_estado='sin_factura',
                     factura_total=None, factura_litros=None)
            sin_lig.append(c)
    borr = [x for x in bills if x['id'] not in usadas and x['state'] == 'draft']
    pares = sorted(
        (abs(bill_lit.get(x['id'], 0) - c['litros_real']), i, j)
        for i, c in enumerate(sin_lig) for j, x in enumerate(borr)
        if x['partner_id'][0] == po_by_name[c['po']]['partner_id'][0]
        and bill_lit.get(x['id'], 0) > 0
        and abs(bill_lit.get(x['id'], 0) - c['litros_real']) <= 10)
    ci, bj = set(), set()
    for _, i, j in pares:
        if i in ci or j in bj:
            continue
        ci.add(i); bj.add(j)
        x = borr[j]
        sin_lig[i].update(factura=x.get('l10n_latam_document_number') or '?',
                          factura_estado='borrador_sin_vincular',
                          factura_total=round(x['amount_total']),
                          factura_litros=round(bill_lit.get(x['id'], 0)))
    # borradores que no calzaron con ninguna compra (ej. "Del Giro" sin litros)
    borradores_sueltos = [dict(
        folio=x.get('l10n_latam_document_number') or '?',
        proveedor=safe_name(x.get('partner_id')), fecha=x.get('invoice_date') or '',
        total=round(x['amount_total']), litros=round(bill_lit.get(x['id'], 0)) or None,
    ) for j, x in enumerate(borr) if j not in bj
        and (x.get('invoice_date') or '') >= ayer_s]

    # ── Fase 3: traspasos entre bodegas sin terminar ──
    ints = sr_odoo(models, uid, 'stock.picking', [
        ['picking_type_code', '=', 'internal'],
        ['state', 'not in', ['done', 'cancel']],
    ], ['name', 'state', 'scheduled_date', 'location_id', 'location_dest_id', 'origin'],
        limit=500, order='scheduled_date desc')
    int_qty = defaultdict(float)
    if ints:
        for mv in sr_odoo(models, uid, 'stock.move', [
            ['picking_id', 'in', [i['id'] for i in ints]],
            ['product_id', '=', DIESEL_PRODUCT_ID],
        ], ['picking_id', 'product_uom_qty'], limit=2000):
            int_qty[mv['picking_id'][0]] += mv['product_uom_qty'] or 0
    traspasos = [dict(
        nombre=i['name'], estado=i['state'],
        fecha=a_cl(i['scheduled_date']).strftime('%Y-%m-%d'),
        origen=cam2(safe_name(i.get('location_id'))),
        destino=cam2(safe_name(i.get('location_dest_id'))),
        litros=round(int_qty.get(i['id'], 0)),
    ) for i in ints]

    # ── Fase 4: ventas — Cantidad = Entregado = Facturado, por orden ──
    # Referencia = litros entregados en SimpliRoute (verificado 29-sep: calzan al
    # litro con la planilla de bodegas en los 5 camiones que la llenaron). Por
    # cada orden se dice QUÉ corregir, con las reglas de la hoja 4B.
    ent = defaultdict(lambda: dict(litros=0.0, cams=set(), sin_camion=False, hoy=False))
    for v in visitas:
        if v['so'] and v['fecha'] <= hoy_s and (
                v['status'] in ('completed', 'partial') or v['especial']):
            e = ent[v['so']]
            e['litros'] += v['litros']
            if v['cam']:
                e['cams'].add(v['cam'])
            else:
                e['sin_camion'] = True
            e['hoy'] |= v['fecha'] == hoy_s

    # salidas pendientes = filtro "Confirmación salidas" (id 114) hasta hoy
    pend_qty = defaultdict(float)
    if picks:
        for mv in sr_odoo(models, uid, 'stock.move', [
            ['picking_id', 'in', [p['id'] for p in picks]],
            ['product_id', '=', DIESEL_PRODUCT_ID],
        ], ['picking_id', 'product_uom_qty'], limit=2000):
            pend_qty[mv['picking_id'][0]] += mv['product_uom_qty'] or 0
    salidas = []
    for p in sorted(picks, key=lambda x: x['scheduled_date']):
        salidas.append(dict(
            picking=p['name'], so=p['origin'], cliente=safe_name(p.get('partner_id')),
            cam=cam2(p['name']), fecha=a_cl(p['scheduled_date']).strftime('%Y-%m-%d'),
            litros=round(pend_qty.get(p['id'], 0)),
            entregada_simpli=p['origin'] in ent,
        ))
    pend_por_so = {s['so']: s for s in salidas}

    sos = sorted(set(ent) | set(pend_por_so))
    so_info = {}
    lineas = defaultdict(lambda: dict(cantidad=0.0, entregado=0.0, facturado=0.0))
    desc_por_so = defaultdict(lambda: defaultdict(float))   # SO -> camión Odoo -> L
    for i in range(0, len(sos), 200):
        chunk = sos[i:i + 200]
        for o in sr_odoo(models, uid, 'sale.order', [['name', 'in', chunk]],
                         ['name', 'state', 'partner_id', 'warehouse_id', 'invoice_status',
                          'shipping_date'], limit=2000):
            so_info[o['name']] = o
        for l in sr_odoo(models, uid, 'sale.order.line', [
            ['order_id.name', 'in', chunk], ['product_id', '=', DIESEL_PRODUCT_ID],
        ], ['order_id', 'product_uom_qty', 'qty_delivered', 'qty_invoiced'], limit=5000):
            x = lineas[safe_name(l['order_id']).split(' ')[0]]
            x['cantidad'] += l['product_uom_qty'] or 0
            x['entregado'] += l['qty_delivered'] or 0
            x['facturado'] += l['qty_invoiced'] or 0
        # de qué bodega descontó Odoo lo ya validado (neto de devoluciones)
        for mv in sr_odoo(models, uid, 'stock.move', [
            ['origin', 'in', chunk], ['product_id', '=', DIESEL_PRODUCT_ID],
            ['state', '=', 'done'],
        ], ['origin', 'location_id', 'location_dest_id', 'quantity'], limit=5000):
            src, dst = safe_name(mv['location_id']), safe_name(mv['location_dest_id'])
            if 'Existencias' in src and 'Customers' in dst:
                desc_por_so[mv['origin']][cam2(src)] += mv['quantity'] or 0
            elif 'Customers' in src and 'Existencias' in dst:
                desc_por_so[mv['origin']][cam2(dst)] -= mv['quantity'] or 0

    L = lambda q: f"{round(q):,}".replace(',', '.')
    ventas, bodega_ventana = [], 0
    ajuste_cam = defaultdict(float)   # efecto en el saldo Odoo de aplicar las correcciones
    for so in sos:
        o = so_info.get(so)
        if not o or o['state'] != 'sale':
            continue  # canceladas/cotización: las cubre "huérfanas" de rutas
        x = lineas.get(so, dict(cantidad=0, entregado=0, facturado=0))
        C, E, F = x['cantidad'], x['entregado'], x['facturado']
        cliente = safe_name(o.get('partner_id'))
        especial = any(c in cliente.upper() for c in FALLA_ES_ENTREGA)
        e = ent.get(so)
        pend = pend_por_so.get(so)
        cams_s = sorted(e['cams']) if e else []
        cam_s = cams_s[0] if len(cams_s) == 1 else None
        # litros Simpli vacíos (checkout sin dato) o facturación especial → pedido
        S = None
        if e:
            S = C if (especial or e['litros'] <= 0) else e['litros']
        desc = {c: q for c, q in desc_por_so.get(so, {}).items() if abs(q) > TOL_LITROS}
        acc, prob = [], []

        if pend and not e:
            prob.append('sin_entrega')
            acc.append(f"Salida {pend['picking']} abierta pero SimpliRoute no la muestra "
                       f"entregada: si no salió, reprogramar; si salió, validar")
        if e and e['sin_camion'] and not cams_s:
            prob.append('visita_sin_camion')
            acc.append('La visita en SimpliRoute no tiene camión asignado: confirmar qué camión entregó')
        if cam_s:
            for c, q in desc.items():
                if c != cam_s and q > 0:
                    prob.append('bodega')
                    if (e and e['hoy']) or pend:
                        ajuste_cam[c] += q
                        ajuste_cam[cam_s] -= q
                    acc.append(f"Odoo descontó {L(q)} L de {c} pero lo entregó {cam_s}: "
                               f"traspaso {c}→{cam_s} por {L(q)} L")
        if S is not None:
            if E < S - TOL_LITROS:
                prob.append('falta_validar')
                if (e['hoy'] or pend) and (cam_s or pend):
                    ajuste_cam[cam_s or pend['cam']] -= S - E
                if pend and cam_s and pend['cam'] != cam_s:
                    acc.append(f"Cambiar Almacén a {cam_s} (anexo A5) y luego validar "
                               f"{pend['picking']} por {L(S - E)} L")
                elif pend:
                    acc.append(f"Validar {pend['picking']} por {L(S - E)} L")
                else:
                    acc.append(f"Faltan {L(S - E)} L por validar y no hay salida abierta: revisar movimientos")
            elif E > S + TOL_LITROS:
                prob.append('validado_de_mas')
                acc.append(f"Odoo validó {L(E)} L y SimpliRoute entregó {L(S)} L: revisar el movimiento")
            if C > S + TOL_LITROS:
                prob.append('cantidad')
                extra = f" (cierra la salida pendiente de {L(pend['litros'])} L)" if (
                    pend and E >= S - TOL_LITROS) else ''
                acc.append(f"Bajar Cantidad de {L(C)} a {L(S)} L en la orden{extra}")
            elif C < S - TOL_LITROS:
                prob.append('cantidad')
                acc.append(f"Subir Cantidad de {L(C)} a {L(S)} L" + (
                    '' if E >= S - TOL_LITROS else ' (primero validar lo entregado)'))
            if F < S - TOL_LITROS:
                prob.append('por_facturar')
                acc.append(f"Facturar {L(S - F)} L" + (' (Sugal: revisar en Ventas)' if especial else ''))
            elif F > S + TOL_LITROS:
                prob.append('facturado_de_mas')
                acc.append(f"Facturado {L(F)} L > entregado {L(S)} L: revisar la factura "
                           f"(Facturado no se toca salvo lo realmente facturado)")
        if 'bodega' in prob:
            bodega_ventana += 1
        if not acc or not ((e and e['hoy']) or pend):
            continue
        ventas.append(dict(
            so=so, cliente=cliente, especial=especial,
            cam=cam_s or (pend['cam'] if pend else '') or '?',
            cam_simpli=cams_s, cam_odoo=sorted(c for c, q in desc.items() if q > 0),
            picking=pend['picking'] if pend else None,
            fecha_entrega=o.get('shipping_date') or '',
            simpli=None if S is None else round(S),
            cantidad=round(C), entregado=round(E), facturado=round(F),
            problemas=sorted(set(prob)), acciones=acc,
        ))
    ventas.sort(key=lambda r: (r['cam'], r['so']))
    entregadas_hoy = {so for so, e in ent.items() if e['hoy']}

    # litros por camión: entregado Simpli hoy (lo que la planilla debiera decir)
    simpli_cam_hoy = defaultdict(float)
    for v in visitas:
        if v['fecha'] == hoy_s and v['so'] and (
                v['status'] in ('completed', 'partial') or v['especial']):
            simpli_cam_hoy[v['cam'] or '?'] += v['litros'] if v['litros'] > 0 else (
                pedido_por_so_ref.get(v['so'], 0))

    # "Por facturar › Órdenes a facturar" debe quedar vacío al cierre
    a_facturar = sr_odoo(models, uid, 'sale.order', [
        ['state', '=', 'sale'], ['invoice_status', '=', 'to invoice'],
    ], ['name', 'partner_id', 'warehouse_id', 'shipping_date'], limit=500,
        order='shipping_date asc')
    ordenes_a_facturar = [dict(
        so=o['name'], cliente=safe_name(o.get('partner_id')),
        cam=cam2(safe_name(o.get('warehouse_id'))),
        fecha_entrega=o.get('shipping_date') or '',
        especial=any(c in safe_name(o.get('partner_id')).upper() for c in FALLA_ES_ENTREGA),
    ) for o in a_facturar]

    # ── Fase 5: saldo cierre Odoo por camión (Inventario › Ubicaciones) ──
    whs = sr_odoo(models, uid, 'stock.warehouse', [], ['name', 'lot_stock_id'], limit=50)
    loc_cam = {w['lot_stock_id'][0]: cam2(w['name']) for w in whs if w.get('lot_stock_id')}
    saldo = defaultdict(float)
    for q in sr_odoo(models, uid, 'stock.quant', [
        ['product_id', '=', DIESEL_PRODUCT_ID], ['location_id', 'in', list(loc_cam)],
    ], ['location_id', 'quantity'], limit=500):
        saldo[loc_cam[q['location_id'][0]]] += q['quantity'] or 0
    saldos = [dict(cam=c, odoo=round(saldo.get(c, 0)),
                   ajuste=round(ajuste_cam.get(c, 0)),
                   esperado=round(saldo.get(c, 0) + ajuste_cam.get(c, 0)),
                   ventas_simpli_hoy=round(simpli_cam_hoy.get(c, 0)))
              for c in sorted(set(loc_cam.values())) if c != 'ES']

    # ── Fase 6: pedidos de mañana ──
    tap = sr_odoo(models, uid, 'sale.order', [
        ['state', '=', 'to_approve'], ['shipping_date', '<=', manana_s],
    ], ['name', 'partner_id', 'warehouse_id', 'shipping_date', 'main_exception_id'],
        limit=500, order='shipping_date asc')
    por_aprobar = [dict(
        so=o['name'], cliente=safe_name(o.get('partner_id')),
        cam=cam2(safe_name(o.get('warehouse_id'))),
        fecha_entrega=o.get('shipping_date') or '',
        excepcion=safe_name(o.get('main_exception_id')),
    ) for o in tap]
    validadas_manana = sr_odoo(models, uid, 'sale.order', [
        ['state', '=', 'sale'], ['shipping_date', '=', manana_s],
        ['invoice_status', '=', 'to invoice'],
    ], ['name'], limit=500)
    visitas_manana = [v for v in visitas if v['fecha'] == manana_s and v['status'] != 'canceled']

    return dict(
        fase2=dict(compras=compras, borradores_sueltos=borradores_sueltos,
                   sin_recibir=sum(1 for c in compras if not c['recibida'])),
        fase3=dict(traspasos=traspasos),
        fase4=dict(salidas=salidas, ventas=ventas, a_facturar=ordenes_a_facturar,
                   entregadas_hoy=len(entregadas_hoy), bodega_ventana=bodega_ventana,
                   revisadas_ventana=len(sos),
                   salidas_entregadas=sum(1 for s in salidas if s['entregada_simpli'])),
        fase5=dict(saldos=saldos),
        fase6=dict(por_aprobar=por_aprobar,
                   con_excepcion=sum(1 for o in por_aprobar if o['excepcion']),
                   validadas_manana=len(validadas_manana),
                   visitas_manana=len(visitas_manana),
                   fecha=manana_s),
    )


def main():
    ahora = dt.datetime.now(TZ)
    hoy = ahora.date()
    # Odoo guarda datetimes en UTC. Chile: UTC-4 invierno (abr-sep por regla del
    # pipeline, ver extract_crm.py), UTC-3 verano.
    off = 4 if 4 <= hoy.month <= 8 else 3
    fin_hoy_utc = (dt.datetime.combine(hoy, dt.time(23, 59, 59))
                   + dt.timedelta(hours=off)).strftime('%Y-%m-%d %H:%M:%S')

    # ── SimpliRoute: vehículos (patente -> código de 2 letras) ──
    veh_cod = {v['id']: str(v.get('name') or '').strip().upper()[:2]
               for v in sr_get('/routes/vehicles/') or []}

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
                cam=veh_cod.get(v.get('vehicle'), ''),
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

    cierre = extract_cierre(models, uid, hoy, off, visitas, picks, pedido_por_so)

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
        cierre=cierre,
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
    c = out['cierre']
    print(f"  cierre: {c['fase2']['sin_recibir']} compras sin recibir · "
          f"{len(c['fase3']['traspasos'])} traspasos abiertos · "
          f"{len(c['fase4']['salidas'])} salidas pendientes · "
          f"{len(c['fase4']['ventas'])} órdenes con problema · "
          f"{c['fase6']['con_excepcion']} con excepción mañana")
    print('  saldos Odoo: ' + ' · '.join(f"{x['cam']} {x['odoo']:,}" for x in c['fase5']['saldos']))
    for x in por_so:
        if x['estado'] != 'ok':
            print(f"    {x['fecha']} {x['so']} {x['cliente'][:28]:28} "
                  f"ent {x['entregado']:>6,} fact {x['facturado']:>6,} [{x['estado']}]")


if __name__ == '__main__':
    main()
