#!/usr/bin/env python3
"""
extract_route.py — Ruta del día para Ops
---------------------------------------
Pulls sale.order records in 'to invoice' state from Odoo PRODUCCION,
groups them by stock.warehouse.code (SH, HH, ...), optimizes a round-trip
route from the corresponding ENAP origin, and writes route-data.json +
Google Maps links ready for the Ops guy.

Runs in the same GitHub Actions pipeline as extract_crm.py / extract_ceo.py.
Secrets required: ODOO_URL, ODOO_DB, ODOO_USER, ODOO_KEY.
"""

import os
import json
import math
import time
import urllib.parse
import urllib.request
import xmlrpc.client
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Config — AJUSTAR si cambian los almacenes o se suman orígenes
# ---------------------------------------------------------------------------
# warehouse.code en Odoo -> dirección de origen (ENAP) para el link de Maps.
# Si aparece un código nuevo no mapeado, el script lo imprime en el log y
# agrupa esas órdenes en "UNMAPPED" para que lo revises.
WAREHOUSE_ORIGINS = {
    "SH": {
        "label": "ENAP San Fernando",
        "address": "ENAP Refinerías, San Fernando, Región de O'Higgins, Chile",
    },
    "HH": {
        "label": "ENAP Linares",
        "address": "ENAP, Linares, Región del Maule, Chile",
    },
    # agrega aquí otros códigos (ej. "RG": {"label": "...", "address": "..."})
}

ODOO_URL  = os.environ["ODOO_URL"].rstrip("/")
ODOO_DB   = os.environ["ODOO_DB"]
ODOO_USER = os.environ["ODOO_USER"]
ODOO_KEY  = os.environ["ODOO_KEY"]

OUTPUT_PATH = os.environ.get("ROUTE_OUTPUT", "route-data.json")

# Google Maps /dir/ URL acepta hasta 9 waypoints en modo gratis.
# Si una zona tiene >9 paradas, dividimos en sub-rutas consecutivas.
MAX_WAYPOINTS_PER_LINK = 9

# Geocoder fallback — solo se usa si la partner no tiene lat/lng en Odoo.
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_HEADERS = {"User-Agent": "TomEnergy-RouteBuilder/1.0 (p@tomenergy.cl)"}


# ---------------------------------------------------------------------------
# Odoo helpers
# ---------------------------------------------------------------------------
def odoo_connect():
    common = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/common")
    uid = common.authenticate(ODOO_DB, ODOO_USER, ODOO_KEY, {})
    if not uid:
        raise SystemExit("❌ XML-RPC auth failed — revisa ODOO_USER / ODOO_KEY")
    models = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/object", allow_none=True)
    return uid, models


def search_read(models, uid, model, domain, fields, limit=False):
    return models.execute_kw(
        ODOO_DB, uid, ODOO_KEY, model, "search_read",
        [domain], {"fields": fields, "limit": limit or 0},
    )


def read_ids(models, uid, model, ids, fields):
    if not ids:
        return []
    return models.execute_kw(
        ODOO_DB, uid, ODOO_KEY, model, "read",
        [list(ids)], {"fields": fields},
    )


# ---------------------------------------------------------------------------
# Geocoding
# ---------------------------------------------------------------------------
_geocache = {}

def geocode(address):
    """Return (lat, lng) or None. Uses Nominatim free tier — 1 req/sec."""
    if not address:
        return None
    if address in _geocache:
        return _geocache[address]
    q = urllib.parse.urlencode({"q": address, "format": "json", "limit": 1,
                                "countrycodes": "cl"})
    try:
        req = urllib.request.Request(f"{NOMINATIM_URL}?{q}", headers=NOMINATIM_HEADERS)
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read().decode())
        time.sleep(1.0)  # respetar rate limit
        if data:
            lat, lng = float(data[0]["lat"]), float(data[0]["lon"])
            _geocache[address] = (lat, lng)
            return lat, lng
    except Exception as e:
        print(f"⚠️  geocode fail {address!r}: {e}")
    _geocache[address] = None
    return None


# ---------------------------------------------------------------------------
# Route optimization (nearest-neighbor from origin, round-trip)
# ---------------------------------------------------------------------------
def haversine(a, b):
    (lat1, lon1), (lat2, lon2) = a, b
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    s = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return 2 * R * math.asin(math.sqrt(s))


def optimize_nn(origin_coord, stops):
    """Nearest-neighbor TSP heuristic. stops = list of dicts with 'coord'."""
    remaining = [s for s in stops if s.get("coord")]
    unmappable = [s for s in stops if not s.get("coord")]
    ordered = []
    cur = origin_coord
    while remaining:
        nxt = min(remaining, key=lambda s: haversine(cur, s["coord"]))
        ordered.append(nxt)
        cur = nxt["coord"]
        remaining.remove(nxt)
    # append stops sin coord al final — el Ops guy los ubica manualmente
    ordered.extend(unmappable)
    return ordered


# ---------------------------------------------------------------------------
# Google Maps URL builder
# ---------------------------------------------------------------------------
def maps_link(origin_addr, dest_addr, waypoints_addrs):
    params = {
        "api": "1",
        "origin": origin_addr,
        "destination": dest_addr,
        "travelmode": "driving",
    }
    if waypoints_addrs:
        params["waypoints"] = "|".join(waypoints_addrs)
    return "https://www.google.com/maps/dir/?" + urllib.parse.urlencode(params, safe="|,")


def build_links(origin_addr, ordered_stops):
    """Split en chunks de ≤9 waypoints; última parada = destino, resto = waypoints.
    Para round-trip: cada chunk vuelve al origen como destino final del día."""
    links = []
    if not ordered_stops:
        return links

    # Chunk paradas en grupos de hasta MAX_WAYPOINTS_PER_LINK
    chunk_size = MAX_WAYPOINTS_PER_LINK
    chunks = [ordered_stops[i:i + chunk_size]
              for i in range(0, len(ordered_stops), chunk_size)]

    for idx, chunk in enumerate(chunks, 1):
        waypoints = [s["address"] for s in chunk]
        # round-trip: origen como destino final; todas las paradas van en waypoints
        url = maps_link(origin_addr, origin_addr, waypoints)
        links.append({
            "part": idx,
            "of": len(chunks),
            "stops": len(chunk),
            "url": url,
        })
    return links


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    uid, models = odoo_connect()
    print(f"✅ Odoo conectado — uid={uid}")

    # Órdenes "a facturar": state confirmado + invoice_status pendiente
    domain = [
        ("state", "in", ["sale", "done"]),
        ("invoice_status", "=", "to invoice"),
    ]
    so_fields = [
        "name", "partner_id", "partner_shipping_id",
        "warehouse_id", "commitment_date", "shipping_date",
        "date_order", "amount_untaxed",
    ]
    orders = search_read(models, uid, "sale.order", domain, so_fields)
    print(f"📦 {len(orders)} órdenes a facturar")

    # Warehouses — code por id
    wh_ids = {o["warehouse_id"][0] for o in orders if o.get("warehouse_id")}
    wh_rows = read_ids(models, uid, "stock.warehouse", wh_ids, ["code", "name"])
    wh_by_id = {w["id"]: w for w in wh_rows}

    # Partners (shipping) — dirección + lat/lng + phone
    ship_ids = {o["partner_shipping_id"][0] for o in orders if o.get("partner_shipping_id")}
    partner_fields = [
        "name", "contact_address", "street", "street2", "city", "state_id",
        "zip", "phone", "mobile",
        "partner_latitude", "partner_longitude",
    ]
    partners = read_ids(models, uid, "res.partner", ship_ids, partner_fields)
    partner_by_id = {p["id"]: p for p in partners}

    # Agrupar por warehouse.code
    groups = {}  # code -> list of stops
    for o in orders:
        wh_id = o["warehouse_id"][0] if o.get("warehouse_id") else None
        code = wh_by_id.get(wh_id, {}).get("code") or "UNMAPPED"
        ship = partner_by_id.get(o["partner_shipping_id"][0] if o.get("partner_shipping_id") else 0, {})

        address = (ship.get("contact_address") or "").replace("\n", ", ").strip(", ")
        if not address:
            address = ", ".join(filter(None, [
                ship.get("street"), ship.get("street2"), ship.get("city"),
                ship.get("zip"), "Chile",
            ]))

        coord = None
        lat, lng = ship.get("partner_latitude"), ship.get("partner_longitude")
        if lat and lng and (lat, lng) != (0.0, 0.0):
            coord = (lat, lng)
        elif address:
            coord = geocode(address)

        stop = {
            "order": o["name"],
            "customer": o["partner_id"][1] if o.get("partner_id") else "",
            "address": address or "(sin dirección)",
            "phone": ship.get("phone") or ship.get("mobile") or "",
            "commitment_date": o.get("commitment_date") or o.get("shipping_date") or o.get("date_order"),
            "amount_untaxed": o.get("amount_untaxed") or 0,
            "coord": coord,
        }
        groups.setdefault(code, []).append(stop)

    # Construir output por grupo
    output_routes = []
    for code, stops in sorted(groups.items()):
        origin_cfg = WAREHOUSE_ORIGINS.get(code, {
            "label": f"(origen no mapeado: {code})",
            "address": "",
        })
        origin_addr = origin_cfg["address"]
        origin_coord = geocode(origin_addr) if origin_addr else None

        if origin_coord:
            ordered = optimize_nn(origin_coord, stops)
        else:
            ordered = stops  # sin optimizar si no hay origen geocodificado

        links = build_links(origin_addr or stops[0]["address"], ordered) if origin_addr else []

        output_routes.append({
            "warehouse_code": code,
            "origin": origin_cfg,
            "stop_count": len(ordered),
            "unmapped_coords": sum(1 for s in ordered if not s["coord"]),
            "maps_links": links,
            "stops": [
                {k: v for k, v in s.items() if k != "coord"}
                for s in ordered
            ],
        })

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_orders": len(orders),
        "routes": output_routes,
    }
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, default=str)

    print(f"💾 {OUTPUT_PATH} escrito — {len(output_routes)} rutas")
    for r in output_routes:
        print(f"   {r['warehouse_code']:8}  {r['stop_count']:3} paradas  "
              f"({r['unmapped_coords']} sin coords)  "
              f"{len(r['maps_links'])} link(s)")


if __name__ == "__main__":
    main()
