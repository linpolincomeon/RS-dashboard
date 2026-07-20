# CLAUDE.md — RS-dashboard (TomEnergy)

Contexto obligatorio para trabajar en este repo. Léelo completo antes de tocar código.
Última actualización: julio 2026. Al cerrar una sesión con cambios relevantes, actualiza la sección "Estado actual".

## Qué es este proyecto

Dashboards estáticos para TomEnergy (distribuidor de diésel B2B, Chile — RM, VI y VII Región).
Pipeline: scripts Python (XML-RPC contra Odoo 18 Enterprise) → JSON estáticos → HTML + Chart.js servidos por GitHub Pages.
Owner: Pauline Vial Comber (CEO). Trabaja directamente en el código, sin developer intermedio.
Comunicación siempre en español. Respuestas directas, sin relleno.

- Repo: `linpolincomeon/RS-dashboard` (público)
- URL live: https://linpolincomeon.github.io/RS-dashboard/
- Instancia Odoo: https://tomenergy.cl · DB: `PRODUCCION`

## Stack y archivos

| Script | Output | Consumidor |
|---|---|---|
| `extract_crm.py` | `crm-data.json` | `crm-sales.html` (dashboard PRINCIPAL) |
| `extract_ceo.py` | `ceo-data.json`, `riesgo-historico.json` | `ceo-dashboard.html` |
| `extract_route.py` | `route-data.json` | `route.html` |
| `odoo_mover_en_riesgo.py` | `riesgo-snapshots.json` | tab Riesgo |
| — | `avla-lines.json` (manual, desde portal AVLA) | cobertura CxC en CEO dashboard |
| — | `vendor-goals.json` (manual) | metas por ejecutivo |

Otros: `crm-weekly.html` (Pipeline/Funnel), `board-dashboard.html` (standalone SheetJS, lee Excel local vía file picker, no depende del pipeline).

`reporte_chofer_conectado.html`: formulario móvil para choferes (reporte de bodega: inicio/carga/venta/traslado/cierre por camión). **NO usa el pipeline Odoo.** Envía por `fetch` `mode:'no-cors'` + body JSON a un Google Apps Script webhook, que escribe en la planilla `Bodegas_por_Camion_v2` (Sheet ID `11UTBZyBicW3HJcwnPpxyK...`). ⚠ El webhook DEBE recibir JSON en el body (`e.postData.contents`); NO cambiar a form submit/iframe (rompe el `JSON.parse` del `doPost`). El `no-cors` no bloquea la escritura, solo impide leer la respuesta. URL del deployment activo: `.../AKfycbyEEI4ujT7...`. Payload debe incluir `otro_camion` (el script lo usa para el movimiento espejo de traslados con bodega OTRA).

Frontend sin framework. Fuente DM Sans. Background `#f7f6f3`, cards blancas, acento `#c8360e`.

## CI / GitHub Actions

- Único secret: `ODOO_KEY`. El resto son literales en el YAML (`ODOO_URL=https://tomenergy.cl`, `ODOO_DB=PRODUCCION`, `ODOO_USER=p@tomenergy.cl`).
- Regla: pasar `env` al **step**, nunca al nivel de job (sobreescribe secrets con strings vacíos).
- Crons escalonados por incidente OOM (jul 2026): 03:00, 03:30, 04:00 hora Chile. NO volver a alinearlos.
- Conflictos de Actions sobre JSON: `git pull --rebase -X theirs origin main`.

## Constantes y modelos Odoo

- Diésel B1: `product_id = 14` (`DIESEL_PRODUCT_ID`)
- ENAP `partner_id = 5667` · Adquim `partner_id = 15299`
- Cheques en Cartera (journal): `id = 114`
- Vendedor: `res.partner.user_id` — SIEMPRE. Nunca `invoice_user_id`.
- Margen por factura: `margin_zone` en `account.move` (decimal 0–1)
- Crédito: `tiene_credito`, `monto_credito`, `saldo_credito` en `res.partner`
- Custom: `x_litros_estimados`, `x_origen_oportunidad`, `group_consultek_id`, `is_volume_client`, `delivery_zone_id`
- SLA: `shipping_date` (sale.order) vs `invoice_date` de la 1ra factura posted donde `invoice_origin = sale.order.name`. Excluir órdenes de hoy y fechas pre-2020.

### ⚠ Convenciones críticas (no negociables)

1. **`crm.lead.expected_revenue` NO son pesos: son LITROS MENSUALES esperados del lead.** Decisión deliberada de Pauline. Toda lectura en dashboards se interpreta como litros/mes.
2. **Las NC (`out_refund`) se restan manualmente** — Odoo no las netea.
3. **Ejecutivo real ≠ `user_id`.** La gestión real se determina por quién registra actividad (`mail.message` sobre `res.partner` o `crm.lead`), no por la cuenta asignada.
4. **Semana comercial: JUEVES a MIÉRCOLES** (calendario ENAP). Aplica a todo.
5. Odoo 18 estampa `write_date`/`date_last_stage_update` iguales en batch del cron 07:30 — usar `stage_update` en `getLeadDate()`.

## Lógica comercial

- Segmentación: Retail = `is_volume_client` False (meta margen 12%) · Volumen = True (meta 9%). Cliente "Predeterminado" excluido de todo.
- Margen %: `(Venta_neta − Costo_neto)/Venta_neta × 100`; `Costo_neto = Venta_neta × (1 − margin_zone)`
- Precio neto: `(bruto − IEC)/(1+IVA)` · IEC = $104.5749/lt · IVA = 19%
- Churn %: perdidos nuevos del mes / clientes activos mes anterior. Perdido = 9 meses (270 días) sin factura posted.
- Durmancia dinámica: freq <30d → durmiente si supera freq×1.5 sin comprar; freq ≥30d → freq×1.3.
- Rescate Durmiente → Fidelización (Comber Sigall). Rescate Perdido → ejecutivo comercial, cuenta como cliente NUEVO.
- Riesgo crediticio (score 0–100, mayor = peor): Morosidad 40 · Utilización 25 · Cobranza/Siniestro 20 · Margen 15. Crítico ≥60, Alto ≥40, Medio ≥20. Consolidación por grupo vía `group_consultek_id`.
- Cobertura AVLA: por RUT, `min(deuda_cliente, cobertura_efectiva_avla_clp)`. Cobertura efectiva = `monto_aprobado_UF × uf_value × (cobertura%/100)`. ⚠ El % (80% innominados / 90% nominados) se aplica **desde 2026-07-08**; los avla-lines.json previos usaban 100% nominal (ver Estado actual).
- Calculadora de excepción: `minimo = base_min_zona + (−dias_real + 30) × C10 / 30`; `excepcion = minimo − 10`; C10 = −16. Usa días REALES (`avg_payment_days`).

## Equipo activo (junio 2026 — `ALLOWED_VENDORS`)

Formales: Joaquín (Muñoz Encalada), Comber Sigall Pauline (fidelización/CS; meta mantención cambia mensualmente). Carlos Labbe también cuenta como ejecutivo formal en la KPI "Litros Ejecutivos".
Manuel Santana: **removido de los dashboards el 2026-07-10** (quitado de `ALLOWED_VENDORS`, `EJECUTIVOS`, `EXEC_COUNT` 4→3 y canonical de `extract_crm.py`). No tenía facturas de julio, sin impacto en números del mes.
Freelancers: Sebastián Toro (sin meta), Cristian Jiroz, Manuel López Allende, Carolina Avilés, Diego Varas, Marcela Márquez, Raúl Bisquertt, Nicolás Gonzalez, Rodrigo Retamal.
Removidos de TODA referencia: Fernando Jullian, Yeniré Ron, André De Trenqualye, Vanessa Vázquez, Julio Phillipi, Turner Fabres Antonio.
Las dos Paulines: **Vial Comber** = CEO (NO aparece en dashboards de ventas) · **Comber Sigall** = madre, fidelización (sí aparece en tab CS).

## Reglas de código

- Ediciones quirúrgicas: diff mínimo sobre el archivo existente. No reescribir archivos completos salvo pedido explícito.
- Nombres canónicos: `canonical_vendedor()` (Python) / `canonicalName()` (JS). `safe_id()`/`safe_name()` para Many2one con `False`.
- `sr()` firma: `sr(models, uid, model, domain, fields, limit=, order=)` — db/key son globales.
- `limit=500` trunca silenciosamente. Usar 5000–20000.
- Dominio Odoo = lista de triplets: `[["move_type","=","out_invoice"]] + base_domain`.
- Verificar nombres de campos contra el JSON live antes de referenciarlos (ej. `w.litros` vs `w.value`).
- Validación antes de commit: `python3 -m py_compile` en scripts + checks estructurales en HTML (balance de divs).
- Commits: mensaje descriptivo en español, un tema por commit. Nunca force push a main. Confirmar con Pauline antes de push si el cambio toca lógica de negocio (fórmulas, filtros de vendedores, definiciones de churn).
- Pages tiene lag de deploy: verificar cambios vía `raw.githubusercontent.com`, no la URL live.

## ⚠ Bugs abiertos / zonas prohibidas

- **DSO en `extract_ceo.py`: BUG CONFIRMADO — no confiar en valores.** `read_group` sobre `account.move.line` (debit−credit) no netea pagos reconciliados en Odoo 18. Fix correcto: `amount_residual` para saldos actuales; `account.partial.reconcile` con fecha fin-de-mes para histórico. No "arreglar" de otra forma.
- **Bloque de auto-actualización AVLA en `extract_ceo.py` está DESHABILITADO a propósito** (generaba entradas falsas en `riesgo-historico.json`, ej. 14/06 con 100% no cubierto por mismatch de filename `Avla Lines` vs `avla-lines.json`). No reactivar sin instrucción explícita.
- `dias_facturacion` en `res.partner` está roto (probablemente usa `write_date`). Calcular días desde `account.move`.

## Tabla de zonas Mantenedor (hardcoded en ceo-dashboard.html)

Actualizar aquí Y en el HTML cuando ENAP cambie precios. Valores de mayo 2026 — verificar vigencia contra Mantenedor antes de usar.

| Zona | Bomba | Cred Mín | Vol Mín | Cont Mín |
|---|---|---|---|---|
| Rancagua | 1561 | 1504 | 1465 | 1488 |
| San Fernando | 1551 | 1504 | 1465 | 1488 |
| VI Costa | 1526 | 1504 | 1465 | 1488 |
| Talca | 1503 | 1494 | 1454 | 1478 |
| Curicó | 1503 | 1494 | 1454 | 1478 |
| Chillán | 1503 | 1494 | 1454 | 1478 |
| Región Metropolitana | 1561 | 1504 | 1465 | 1488 |

## Documentación adicional

Si la tarea toca recaudación, cash flow, calculadora de excepción o debugging histórico: leer `docs/REFERENCIA.md` antes de escribir código.

## Estado actual (julio 2026)

- `crm-sales.html`: funcionando. Tab Recuperación completado, columna Actividad live.
- SLA reescrito y funcionando (join `invoice_origin`), respeta selector de semana.
- Board dashboard operativo (histórico 2011–2026).
- Churn del CEO dashboard alineado a definiciones correctas.
- **AVLA (2026-07-08):** `avla-lines.json` regenerado desde export `INSURED_LINES` del portal (descarga 08/07/2026, UF 40842.07). Filtro: solo líneas `Actual` + estado `Aprobada`/`Aprobada parcialmente` → 706 RUTs. **Cambio de metodología:** ahora se aplica el % de cobertura (80/90) → cobertura total $6.920M (vs $6.683M en junio a 100%). ⚠ Esto crea una **discontinuidad en `riesgo-historico.json`**: los snapshots ≤ jun-2026 usan cobertura nominal al 100% y NO son comparables directos con jul-2026 en adelante. No re-escribir el histórico previo (los valores viejos eran correctos para su metodología); al comparar tendencia de "% no cubierto", tener presente el quiebre en esta fecha.
- **Pendiente prioritario:** correr `extract_ceo.py` para propagar la nueva cobertura AVLA a `ceo-data.json`/dashboard y verificar el match RUT↔Odoo; recién entonces evaluar re-habilitar la auto-actualización AVLA (sigue deshabilitada a propósito).
- **Santana + meta Julio (2026-07-10):** eliminado Manuel Santana de todos los dashboards (ver sección Equipo activo). `EXEC_COUNT` 4→3 en `crm-sales.html` y `crm-weekly.html` (metas de equipo del funnel se escalan sobre 3 ejecutivos). **Fix meta mensual:** `company_goals.litros_mes`/`month` estaban hardcodeados en junio (866.750) y no se bumpeaban al cambiar de mes; ahora son dinámicos vía `_meta_mes(today)` + mes actual sobre `BUDGET_2026`. Julio muestra 1.107.706. Auto-mantenido de aquí en adelante.
- **RUT representante legal (2026-07-17):** el campo Studio `x_studio_rut_representante_legal` en `res.partner` era requerido para toda empresa (vista Studio id 3616), lo que bloqueaba guardar fichas de clientes antiguos sin el dato. Cambiado vía XML-RPC a `required="is_company and not id"`: obligatorio solo al CREAR una empresa nueva; las existentes se editan sin restricción. Cambio hecho directo en Odoo (no vive en este repo).
- **Fix "sin gestión" (2026-07-17):** `gather_latest_note()` solo miraba `message_type in ['comment','note']`, pero **Odoo 18 registra las notas y actividades completadas como `message_type='notification'`** (subtipo Note/Activities) → descartaba toda la gestión de clientes ganados/sin actividad abierta (ej. INVINA, ECONUT salían "Sin gestión" teniéndola). Fix: filtrar por **subtipo** (Note + Activities) en vez de message_type, con `_clean()` que desenvuelve "Actividades pendientes hecho :"/"To-Do done :" y recorta "Feedback:", y lista de ruido ampliada (ganado automáticamente, facturas pendientes, cierre masivo, etc.). ⚠ Quitar el filtro sin batchear **revienta el server con OOM** (245 pids × todos los mensajes con body) — por eso va **batcheado (chunks de 100) + `limit=8000` + filtro de subtipo**. Impacto: Recuperación pasó de 2 "sin gestión" a 0; corrige subconteo global. Mismo ruido aplicado al `_noise` de notas won/pipeline.
- ✅ **Seguridad — key rotada (2026-07-17):** la `ODOO_KEY` que estaba expuesta en el historial de git (commit `90de552`) fue **revocada en Odoo** (verificado: ya no autentica). Secret `ODOO_KEY` en GitHub actualizado con key nueva y workflow verificado en verde. ⚠ **La key nueva VENCE el 12/01/2027** (creada con 6 meses): si en enero 2027 los crons empiezan a fallar con "Authentication failed", es esto — crear key nueva en Odoo y actualizar el secret. Los secrets sin uso (`ODOO_API_KEY`, `ODOO_API_KEY25MAR`, `ODOO_DB`, `ODOO_USER`) se eliminaron de GitHub; el único vigente es `ODOO_KEY`. Pendiente menor: activar 2FA en la cuenta Odoo de Pauline.
- **Modelo comisiones freelancers (2026-07-17):** en diseño. Simulación antes/después en `~/Desktop/Simulacion_comisiones_freelancers.xlsx` (datos Odoo al 17-jul, hojas Resumen/Clientes/Mensual/Parámetros con celdas editables). Contrato actual: tablas $/lt por diferencial venta-vs-bomba (Tabla 1 Central / Tabla 2 Otras, decodificadas del docx), solo 6 primeros meses por cliente captado, contrato máx 3 meses. Propuesta: mismas tablas meses 1–6 + cola 50% meses 7–12 + bono retención 90d + contrato sin tope de 3 meses. Identificación freelancers: login `*.ext@tomenergy.cl` (ojo: Garroz es `.ext` pero está en NÓMINA). Las bombas viven en `partner.delivery.zone` (`lst_price` neto; bruto = neto×1,19+IEC) **sin histórico** — calibración histórica solo contra boletas reales (BHE/FAC proveedor; Carolina may-26 calza exacto al peso). Detectado: Manuel López vende desde mayo sin boletas registradas; Toro factura como proveedor Y tuvo liquidación en nómina (may-26); key local para scripts en `~/.odoo_key`.
- **Tab Ejecutivos = solo freelancers (2026-07-20):** en `crm-sales.html` se sacaron Toro y Muñoz del tab Ejecutivos; la lista (`EJECUTIVOS_ALL`) ahora es solo freelancers + Abraham Urrutia (`abraham.ext@tomenergy.cl`, ingresó 14-jul, agregado también a `FREELANCERS` y a `_CANONICAL_VENDORS` del extract). El tab se filtra dinámicamente con `execHasMovement()`: sin movimiento (litros semanales, won, cliente nuevo o actividad CRM) en las últimas 4 semanas → no aparece el botón. ⚠ `pipeline` en el JSON viene truncado a 150 leads y el cron deja todos con `days_in_stage=0`, así que el filtro usa además `executives[].moved_30d` (agregado sobre TODOS los leads). NO se tocó `ALLOWED_VENDORS` ni `EXEC_COUNT` (KPIs de equipo siguen igual). Además: columna "Prom. Mensual" (litros) en "Clientes Perdidos Este Mes" (Fidelización), y en "Transición a Fidelización" la columna Facturas (n° órdenes) se reemplazó por Litros (`graduating[].litros` nuevo en `extract_crm.py`, suma de líneas diesel B1; `_exec_names` del graduating ampliado a todos los freelancers).
- **Reporte chofer (2026-07-10):** `reporte_chofer_conectado.html` funcionando. Se había roto al cambiar el envío a form submit/iframe (el `doPost` hace `JSON.parse(e.postData.contents)` y el form manda `payload=...` url-encoded → explotaba) y al apuntar a un deployment equivocado. Revertido a `fetch` no-cors + body JSON al deployment activo `AKfycbyEEI...`, con `otro_camion` en el payload. Ver detalle en la sección de archivos.
- Backlog: filtro "sin gestión" en Recuperación · asignar ejecutivo desde dashboard (Opción C browser-login vs D serverless, sin decidir) · export CSV recuperación · cron de durmancia en Odoo · fix `dias_facturacion` · orden de tags (workaround prefijos ZONA-) · **hacer el `doPost` del webhook chofer a prueba de balas** (que lea tanto body JSON como `e.parameter.payload` sin romperse).
