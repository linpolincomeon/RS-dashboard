#!/usr/bin/env python3
"""Entregas SimpliRoute -> simpliroute-data.json

Fuente: API REST de SimpliRoute (https://api.simpliroute.com/v1/). Solo GET.
Token: env SIMPLIROUTE_TOKEN (Actions) o ~/.simpliroute_token (local).
NO pasa por Odoo, pero cruza con él: `reference` de cada visita trae el
picking + sale.order de Odoo ("TJVS/OUT/01813 - S36464") y
`extra_field_values.rut` el RUT del cliente.

Qué trae el JSON:
    vehiculos   patente/capacidad por camión (código corto = 2 primeras letras)
    choferes    id SimpliRoute -> nombre/corto
    diario      por (fecha, camión): entregas por status, litros, jornada real
                (salida a ruta `on_its_way`, primer/último checkout GPS)
    fallidas    entregas failed/partial con motivo (checkout_comment)
    visitas     detalle slim de la ventana (para drill-down en el dashboard)

⚠ Los litros por entrega vienen de extra_field_values.litros (fallback `load`).
  litros_entregados solo suma status `completed`; las parciales van aparte
  (la API trae quantity_delivered por item pero aún no se valida en terreno).
⚠ FALLA_ES_ENTREGA: clientes cuyo flujo de facturación impide cerrar la visita
  en la app — el chofer marca "failed" y anota los litros en el comentario,
  pero la entrega SÍ ocurrió (confirmado por Pauline 17-sep, caso SUGAL:
  0 completadas de 76 visitas, motivos "500 litros entregados", etc.).
  Esas fallidas se reclasifican: cuentan como entregadas (flag `especial`)
  y NO aparecen en `fallidas`.
Horas en hora de Chile (checkout_time viene en UTC).
"""
import json
import os
import re
import sys
import time
import urllib.request
import urllib.error
import datetime as dt
from collections import defaultdict
from zoneinfo import ZoneInfo

BASE = 'https://api.simpliroute.com/v1'
VENTANA_DIAS = 45
TZ = ZoneInfo('America/Santiago')

# validación integrada (no escribir el JSON si el dato viene roto)
MIN_COMPLETADAS_7D = 20      # operación real: ~20-25 completadas/día
MIN_LITROS_7D = 30_000       # un solo día normal ya supera esto
MIN_VEHICULOS = 5
MAX_DIAS_CON_ERROR = 5       # tolerancia de errores HTTP en la ventana

# "failed" que en realidad es entrega (facturación distinta, ver docstring)
FALLA_ES_ENTREGA = ('SUGAL',)


def token():
    t = os.environ.get('SIMPLIROUTE_TOKEN', '').strip()
    if not t:
        try:
            t = open(os.path.expanduser('~/.simpliroute_token')).read().strip()
        except FileNotFoundError:
            sys.exit('ERROR: sin token (env SIMPLIROUTE_TOKEN o ~/.simpliroute_token)')
    return t


TOKEN = token()


def get(path, reintentos=3):
    """GET con reintentos ante 5xx/timeout (mismo espíritu que sr() de Odoo)."""
    for i in range(reintentos):
        try:
            req = urllib.request.Request(
                BASE + path, headers={'Authorization': 'Token ' + TOKEN})
            with urllib.request.urlopen(req, timeout=45) as r:
                return json.loads(r.read().decode())
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as e:
            code = getattr(e, 'code', None)
            if code and 400 <= code < 500:
                raise  # error de auth/request: reintentar no sirve
            if i == reintentos - 1:
                raise
            time.sleep(3 * (i + 1))


def hora_local(iso_utc):
    """'2026-09-16T23:37:14.181000Z' -> 'HH:MM' hora Chile (None si vacío)."""
    if not iso_utc:
        return None
    try:
        t = dt.datetime.fromisoformat(str(iso_utc).replace('Z', '+00:00'))
        return t.astimezone(TZ).strftime('%H:%M')
    except ValueError:
        return None


def litros_de(v):
    extra = v.get('extra_field_values') or {}
    try:
        return float(str(extra.get('litros', '')).replace('.', '').replace(',', '.'))
    except (TypeError, ValueError):
        pass
    try:
        return float(v.get('load') or 0)
    except (TypeError, ValueError):
        return 0.0


def main():
    # ── maestros ──
    vehiculos = {}
    for v in get('/routes/vehicles/'):
        patente = str(v.get('name') or '').strip().upper()
        cam = patente.split('-')[0][:2]  # HHPT-71 -> HH
        vehiculos[v['id']] = dict(cam=cam, patente=patente,
                                  capacidad=v.get('capacity'))
    if len(vehiculos) < MIN_VEHICULOS:
        sys.exit(f'ERROR: solo {len(vehiculos)} vehículos en SimpliRoute — ¿token/cuenta?')

    choferes = {}
    for d in get('/accounts/drivers/'):
        nombre = str(d.get('name') or '').strip()
        choferes[d['id']] = dict(nombre=nombre,
                                 corto=nombre.split()[0] if nombre else '?',
                                 user=d.get('username') or '')
    # apodos de siempre (los dos Aguilera son Nino y Tato, no "Jorge")
    cortos_user = {'jose@': 'José Luis', 'nino@': 'Nino',
                   'tato@': 'Tato', 'jorgerojas@': 'Jorge R.'}
    for c in choferes.values():
        for pref, corto in cortos_user.items():
            if c['user'].startswith(pref):
                c['corto'] = corto

    # ── visitas de la ventana ──
    hoy = dt.datetime.now(TZ).date()
    visitas, errores_dias = [], 0
    for i in range(VENTANA_DIAS):
        f = (hoy - dt.timedelta(days=i)).isoformat()
        try:
            dia = get(f'/routes/visits/?planned_date={f}')
        except Exception as e:
            errores_dias += 1
            print(f'  ⚠ {f}: {e}', file=sys.stderr)
            continue
        for v in dia or []:
            veh = vehiculos.get(v.get('vehicle')) or {}
            cho = choferes.get(v.get('driver')) or {}
            extra = v.get('extra_field_values') or {}
            ref = str(v.get('reference') or '')
            m = re.search(r'\bS\d+\b', ref)
            so = m.group(0) if m else None
            cliente = str(v.get('title') or '').strip()
            especial = (v.get('status') == 'failed' and
                        any(c in cliente.upper() for c in FALLA_ES_ENTREGA))
            visitas.append(dict(
                id=v.get('id'),
                fecha=f,
                cam=veh.get('cam'),
                patente=veh.get('patente'),
                chofer=cho.get('corto'),
                status=v.get('status'),
                especial=especial,
                litros=litros_de(v),
                cliente=cliente,
                rut=str(extra.get('rut') or '').strip(),
                ref=ref, so=so,
                salida=hora_local(v.get('on_its_way')),
                checkout=hora_local(v.get('checkout_time')),
                motivo=(str(v.get('checkout_comment') or '').strip()
                        or str(v.get('checkout_observation') or '').strip()),
            ))
    if errores_dias > MAX_DIAS_CON_ERROR:
        sys.exit(f'ERROR: {errores_dias} días con error de API — no se escribe el JSON')

    # ── agregado diario por camión ──
    grupos = defaultdict(list)
    for v in visitas:
        if v['cam']:
            grupos[(v['fecha'], v['cam'])].append(v)

    diario = []
    for (f, cam), vs in sorted(grupos.items(), reverse=True):
        cuenta = defaultdict(int)
        for v in vs:
            cuenta['especial' if v['especial'] else v['status']] += 1
        checkouts = sorted(v['checkout'] for v in vs
                           if v['checkout'] and v['status'] in ('completed', 'partial', 'failed'))
        salidas = sorted(v['salida'] for v in vs if v['salida'])
        chofer = next((v['chofer'] for v in vs if v['chofer']), None)
        diario.append(dict(
            fecha=f, cam=cam, chofer=chofer,
            completadas=cuenta['completed'] + cuenta['especial'],
            especiales=cuenta['especial'],   # SUGAL y afines (failed reclasificado)
            parciales=cuenta['partial'],
            fallidas=cuenta['failed'], canceladas=cuenta['canceled'],
            pendientes=cuenta['pending'],
            litros_entregados=round(sum(v['litros'] for v in vs
                                        if v['status'] == 'completed' or v['especial'])),
            litros_planificados=round(sum(v['litros'] for v in vs
                                          if v['status'] not in ('canceled',))),
            salida=salidas[0] if salidas else None,
            primer_checkout=checkouts[0] if checkouts else None,
            ultimo_checkout=checkouts[-1] if checkouts else None,
        ))

    fallidas = [dict(fecha=v['fecha'], cam=v['cam'], chofer=v['chofer'],
                     cliente=v['cliente'], litros=v['litros'], hora=v['checkout'],
                     status=v['status'], motivo=v['motivo'], so=v['so'])
                for v in visitas
                if v['status'] in ('failed', 'partial') and not v['especial']]

    # ── validación integrada ──
    corte7 = (hoy - dt.timedelta(days=7)).isoformat()
    comp7 = [v for v in visitas
             if v['fecha'] >= corte7 and (v['status'] == 'completed' or v['especial'])]
    if len(comp7) < MIN_COMPLETADAS_7D:
        sys.exit(f'ERROR: solo {len(comp7)} entregas completadas en 7 días '
                 f'(mín {MIN_COMPLETADAS_7D}) — no se escribe el JSON')
    litros7 = sum(v['litros'] for v in comp7)
    if litros7 < MIN_LITROS_7D:
        sys.exit(f'ERROR: {litros7:,.0f} L completados en 7 días '
                 f'(mín {MIN_LITROS_7D:,}) — no se escribe el JSON')

    out = dict(
        generated_utc=dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%dT%H:%M+00:00'),
        fuente='API SimpliRoute · /routes/visits por planned_date',
        desde=(hoy - dt.timedelta(days=VENTANA_DIAS - 1)).isoformat(),
        hasta=hoy.isoformat(),
        vehiculos=[dict(id=k, **v) for k, v in sorted(vehiculos.items())],
        choferes=[dict(id=k, **v) for k, v in sorted(choferes.items())],
        diario=diario,
        fallidas=fallidas,
        visitas=sorted(visitas, key=lambda v: (v['fecha'], v['cam'] or ''),
                       reverse=True),
    )

    with open('simpliroute-data.json', 'w', encoding='utf-8') as fh:
        json.dump(out, fh, ensure_ascii=False, indent=1)

    print(f"simpliroute-data.json  {out['desde']} -> {out['hasta']}")
    print(f"  {len(visitas)} visitas · {len(comp7)} completadas 7d "
          f"({litros7:,.0f} L) · {len(fallidas)} fallidas/parciales en ventana")
    for d in diario[:8]:
        print(f"  {d['fecha']} {d['cam']:3} {d['chofer'] or '?':10} "
              f"✓{d['completadas']:2} ✗{d['fallidas']:2}  "
              f"{d['litros_entregados']:7,} L  "
              f"{d['salida'] or '--:--'}→{d['ultimo_checkout'] or '--:--'}")


if __name__ == '__main__':
    main()
