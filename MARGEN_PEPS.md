# Margen Real PEPS vs `margin_zone` — análisis 06-10-2026

Documento de respaldo del KPI "Margen Real PEPS" del CEO dashboard. Leer antes de tocar
`extract_margen_fifo()` en `extract_ceo.py` o de discutir márgenes retail con el equipo.

## 1. Qué es cada margen

| | `margin_zone` (oficial comercial) | Margen Real PEPS |
|---|---|---|
| Fórmula | `(precio final − Costo Enap de la zona) / precio final`, por factura | `(venta final − costo de la capa de compra) / venta final`, por semana |
| Costo | **del día**: campo `cost_price` ("Costo Enap") de `partner.delivery.zone`, que replica el total/L facturado por ENAP vigente (15–30 sep 2026: $1.266 Talca/Curicó/Chillán · $1.277 RM/Rancagua/San Fdo/Costa; desde 01-oct $1.360 / $1.371). Sin historial en Odoo. | **de la factura de compra con que se compró ese stock**, consumido en orden de llegada (primero entra, primero sale) hasta agotarlo |
| Base de precio | $/L final de factura (con IVA e IEC) | igual: compra = `amount_total` factura ÷ litros; venta = `price_total` de línea |
| Población | todas las facturas con `margin_zone`, ponderadas por neto | solo líneas Diesel B1 (99,98% de la venta), ponderadas por precio final |
| Lo controla | el vendedor (precio vs costo del día) → base de metas 8,5% / 6% y de comisiones | la empresa (compra + venta) → verdad del P&L |
| Dónde se muestra | dashboards comerciales (`crm-sales.html`, Mes Vencido, metas por ejecutivo) y CEO | solo CEO dashboard |

**Nota de nombre:** Pauline lo llamó "LIFO", pero lo que describió ("el stock comprado a $1.300 el
miércoles se sigue vendiendo a costo $1.300 el jueves aunque ENAP suba") es **PEPS/FIFO**. LIFO
sería lo contrario (costear primero lo último comprado = costo nuevo). **En el dashboard la etiqueta
visible es "LIFO"** (pedido Pauline 06-10: el equipo no conoce "PEPS"); el método implementado y los
campos del JSON (`*_fifo`) son primero-entra-primero-sale. No cambiar la etiqueta sin su OK.

## 2. ¿`margin_zone` es FIFO? No — es costo de reposición del día

Prueba: en semanas con costo ENAP plano ambos calzan a ±0,3pt; en semanas de alza o baja se
separan 5pt o más.

| Semana | Retail zona | Retail PEPS | Volumen zona | Volumen PEPS | Costo ENAP |
|---|---|---|---|---|---|
| 01–07 oct | 7,6% | **14,0%** | 6,2% | **12,5%** | alza 01-oct |
| 24–30 sep | 7,8% | 6,8% | 6,3% | 5,3% | plano + compras spot caras (ver §5) |
| 17–23 sep | 8,8% | 8,6% | 6,6% | 6,3% | plano |
| 10–16 sep | 8,2% | **13,5%** | 5,3% | **9,2%** | alza |
| 03–09 sep | 9,0% | 9,0% | 6,7% | 6,6% | plano |
| 27 ago–02 sep | 9,6% | 9,5% | 7,3% | 7,0% | plano |
| 20–26 ago | 8,0% | **13,3%** | 6,1% | **11,4%** | alza |
| 13–19 ago | 9,5% | 9,3% | 7,2% | 6,9% | plano |
| 06–12 ago | 10,0% | 9,9% | 6,5% | 6,1% | plano |
| 16–22 jul | 10,8% | 10,5% | 5,2% | 4,4% | plano |
| 09–15 jul | 10,8% | 8,6% | 5,4% | 1,9% | **baja** de precio |

Mensual la diferencia neta es ±1pt (las alzas y bajas se compensan); semanal es grande.

## 3. ¿Por qué el margen retail se veía tan bajo las últimas semanas?

Tres cosas distintas mezcladas:

**a) 01–07 oct (7,6%) es artefacto del costeo.** El 30-sep se compraron ~330k L a $1.277 que
se vendieron después del alza a $1.479 retail. Margen real 14,0% ($207/L); la zona lo costea a
$1.366 y muestra $113/L.

**b) La caída de % desde julio es mayormente aritmética.** El margen retail en **pesos por
litro** se mantuvo casi fijo (~$120/L) mientras ENAP subía $200/L. El equipo cotiza en $/L sobre
bomba, así que es esperable: la meta en % se vuelve más exigente cada vez que ENAP sube.

| Semana (costo plano) | Precio retail $/L | Costo $/L | Margen $/L | Margen % |
|---|---|---|---|---|
| 16–22 jul | 1.189 | 1.065 | 124 | 10,5% |
| 06–12 ago | 1.207 | 1.088 | 119 | 9,9% |
| 03–09 sep | 1.302 | 1.184 | 118 | 9,0% |
| 17–23 sep | 1.396 | 1.276 | 120 | 8,6% |
| **24–30 sep** | **1.379** | 1.274* | **105** | **7,6%** |

\* costo ENAP; con las compras spot de esa semana el costo PEPS sube a $1.285 y el retail queda en 6,8%.

**c) La última semana de sep sí es deterioro real.** Costo ENAP igual que la semana anterior y
el precio retail bajó $17/L. Eso no lo explica ningún método de costeo: es precio (concesiones,
mix de zona o clientes grandes dentro de retail). Pendiente revisar qué se vendió más barato.

## 4. Cómo se calcula el PEPS (`extract_margen_fifo`)

- **Pool único de compañía**, no por camión: los camiones rotan stock en 1–3 días y el kardex
  por camión tiene descuadres (caso PY +4.600 L, 05-oct).
- **Capas de compra:** toda factura de compra `posted` de **cualquier proveedor** con una línea de
  diésel ≥1.000 L, desde `FIFO_START = 2026-04-01` (antes las compras venían como "Del Giro" sin
  litros). Litros = líneas de diésel de la factura; $ = `amount_total` de la factura. NC de
  proveedor restan. Ajustes de precio sin litros ("Del Giro" qty 1) se ignoran. Líneas de relleno
  contable (precio >1,3× la línea principal) no cuentan como litros.
- **Consumo:** día a día, las compras del día entran antes que las ventas del día; litros de
  venta = líneas Diesel B1 facturadas (NC restan; NC con litros devuelven al frente del pool).
- **Sin capa disponible:** se costea al último precio de compra y `fifo_cobertura` baja de 1
  (el KPI avisa ⚠ si pasa de 5%). Hoy no ocurre en ninguna semana.
- **Campos en `weeks[]` de `ceo-data.json`:** `margin_fifo`, `margin_fifo_retail`,
  `margin_fifo_volumen`, `costo_fifo_l`, `margen_fifo_l`, `fifo_cobertura`.

### ⚠ Errores cometidos y corregidos el mismo día (no repetir)

1. **Usar el neto de línea de las compras.** En ENAP el neto oscila con el componente variable del
   impuesto específico (sep-2026: $1.166 → $1.240 → $1.340 con total fijo en $1.277/L). Generaba
   semanas falsas. Siempre `amount_total` ÷ litros.
2. **Usar `price_total` de línea en compras.** Sale al doble (impuestos de compra IVA + "Específico
   Compras" 90,78% aplicados por línea). Solo sirve `amount_total` de la factura.
3. **Decir que "la tabla de zona iba desfasada".** Era falso, derivado del error 1. El Costo Enap de
   zona replica el total/L ENAP.
4. **Lista fija de proveedores** (ENAP/Adquim/Adgreen). Faltaban las compras spot (§5).

## 5. Proveedores de diésel (abr–oct 2026)

| Proveedor | Litros | Nota |
|---|---|---|
| ENAP Refinerías | 3.134.590 | |
| Adquim | 2.008.000 | incluye lotes "PD B1 / PD A1 NU-1202" de 35.000 L |
| PRD – Adgreen | 115.000 | |
| **Esmax Distribución = Aramco** | 21.832 | 29-sep, cargas SH 9.000 + TY 12.832. La ficha "aramco" (id 17046) no tiene facturas; la glosa de marzo dice "Aramco ProF Diesel" |
| **JLC = José Luis Capdevila Honorato** | 15.000 | 28-sep, $1.266/L |
| **Inversiones HN Ltda** | 15.000 | 28-sep, $1.283/L |

Fuera del pool (correcto): Copec y las facturas chicas de Esmax = petróleo de los camiones
(líneas de ~100 L, van a `costos.html`).

**Factura Esmax FAC 2781542 (29-sep) cuadrada a mano:** total redondo $31.500.000 con una línea
"Diesel" 3.848,54 × $2.401 = $9,2M que no son litros (el kardex muestra solo las dos recepciones).
Precio real **$1.443/L, +13% sobre ENAP ese día** → la compra spot de fin de mes costó ~$3,6M extra
y bajó el PEPS de la semana 24–30 sep de 6,9% a 6,1%.

## 6. Pendientes / decisiones abiertas

- ✅ **Implementado 06-10 (OK Pauline):** en el CEO dashboard PEPS es la cifra principal de Margen
  Retail / Volumen / Compañía; semáforo vs meta sobre el **acumulado de 4 semanas** (semanal en
  alzas da 13% y en bajas 4%); `margin_zone` como "oficial (zona)" en el subtítulo; **margen $/L**
  PEPS en cada tarjeta. Tabla semanal con PEPS R / PEPS V. Dashboards comerciales sin cambios.
- Delta del KPI PEPS es variación relativa (▲105%) por convención de los otros KPIs; candidato a
  pasar a puntos.
- Modelo con 116k L en capas vs 55k L en `stock.quant`: ~½ día de venta de exceso, sin efecto en
  márgenes; sospecha de facturas de compra duplicadas/devueltas (OC P00873–879 del 29-sep).
- Semana 24–30 sep: identificar qué ventas retail bajaron $17/L.
- Los campos PEPS aparecen en el dashboard live solo tras correr `update-ceo-data.yml`.
