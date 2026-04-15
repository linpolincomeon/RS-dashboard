# Ruta del día — integración

## Qué pulls

`sale.order` con:
- `state in ('sale','done')`
- `invoice_status = 'to invoice'`  ← "órdenes a facturar"

Agrupa por `stock.warehouse.code` (SH, HH…). Cada grupo parte de su ENAP de origen, optimiza round-trip con nearest-neighbor sobre lat/lng, y escribe hasta 9 paradas por link de Google Maps (si hay más, divide en tramos 1/N, 2/N…).

## Archivos

| Archivo | Dónde va |
|---|---|
| `extract_route.py` | raíz del repo `linpolincomeon/RS-dashboard/` |
| `route.html` | raíz del repo, junto a `crm-weekly.html` |
| `route-data.json` | lo genera el script, commit automático |

## Config a revisar (dentro de `extract_route.py`)

```python
WAREHOUSE_ORIGINS = {
    "SH": {"label": "ENAP San Fernando", "address": "ENAP Refinerías, San Fernando, …"},
    "HH": {"label": "ENAP Linares",      "address": "ENAP, Linares, …"},
    # agrega otros códigos si existen
}
```

Si aparece un código no mapeado lo verás en el log como `UNMAPPED` — agrega el mapping y re-corre.

## GitHub Actions

Agrega un step al workflow existente (junto a `extract_crm.py`):

```yaml
- name: Build route
  env:
    ODOO_URL:  ${{ secrets.ODOO_URL }}
    ODOO_DB:   ${{ secrets.ODOO_DB }}
    ODOO_USER: ${{ secrets.ODOO_USER }}
    ODOO_KEY:  ${{ secrets.ODOO_KEY }}
  run: python extract_route.py
```

Recordatorio del bug conocido: pasa los secrets al **step**, no al **job** (ver sección 9 del briefing).

## URL live

`linpolincomeon.github.io/RS-dashboard/route.html`

El Ops guy abre la URL, ve dos cards (ENAP San Fernando / ENAP Linares), cada una con botón "Abrir en Maps" + tabla de paradas con dirección, teléfono, compromiso y monto.

## Notas

- **Geocoding**: usa `res.partner.partner_latitude/longitude` si están poblados en Odoo (lo más rápido). Si no, fallback a Nominatim (1 req/s, gratis). Para acelerar, podemos poblar lat/lng en los partners una vez.
- **Optimización real**: nearest-neighbor desde el origen. No es óptimo TSP, pero es pragmático y predecible. Si quieres 2-opt/OR-Tools lo agrego, pero sumas dependencias al workflow.
- **Límite Maps**: URL `/dir/?api=1` admite ~9 waypoints. Para zonas con más, partimos en tramos consecutivos (orden NN preservado entre tramos).
