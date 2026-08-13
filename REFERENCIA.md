# Referencia técnica — RS-dashboard (TomEnergy)

Documentación de soporte. NO se carga automáticamente: leer solo cuando la tarea toca recaudación, cash flow, calculadora de excepción o debugging histórico.
Fuente: rescatado del CONTEXT.md v3 (mayo 2026) — depurado de secciones obsoletas (churn viejo, DSO "validado", lista de vendedores desactualizada). Para definiciones vigentes, manda SIEMPRE el CLAUDE.md de la raíz.

---

## 1. Origen de cada campo del Excel RS (Q&A equipo Contabilidad/Comercial)

Matriz del origen real de cada dato del Excel original. Referencia obligada antes de tocar extractores de recaudación o cash flow.

### Recaudación
- **Recaudación Total (fila 17):** solo se calcula del extracto Banco de Chile. Se descarga el rango de fechas y se filtra por "depósitos".
- **Transferencias/depósitos Chile (fila 18):** Recaudación total − cheques depositados − factoring.
- **Transferencias/depósitos Itaú (fila 19):** casi sin uso; solo cuando crédito/factoring cae ahí. Candidata a eliminarse.
- **Cheques depositados (fila 20):** filtro en "etiqueta" por `dep.cheq`, `dep. docto` o relacionado a cheque. NO incluye depósitos de efectivo.
- **Factoring (fila 21):** todo lo que viene de **Fingo SPA**. El otro proveedor (**Security**) llega como "Transferencia De Otro Banco Via Spav" — solo contar montos grandes, NO pagos normales con esa etiqueta.
- **Rescate/crédito (fila 22):** manual. No automatizable.

### Cheques en cartera
- **Total cheques antes de subir (fila 24):** = "Total después de subir" de la semana anterior.
- **Total cheques después de subir (fila 25):** "subir cheques" = registrar factura pagada con "cheque en cartera". Suma total de cheques no depositados. Ubicación en Odoo: Contabilidad → Apuntes contables → filtro compartido "cheques en cartera" (**journal id 114**).
- **Cheques recibidos en la semana (fila 26):** cheques del journal 114 cuya fecha de ingreso a Odoo cayó en la semana.

### Caja
- **Caja Disponible (fila 15):** saldo contable Banco Chile al domingo + cta. corriente ENAP (portal ENAP, manual) + saldo Santander al domingo + cta. corriente Adquim (interno, manual). Requiere componente manual.

### Compras
- **Compras ENAP (fila 29):** `amount_total_in_currency_signed` de proveedores ENAP, ADQUIM, ADGREEN.
- **COMPRA Odoo (fila 30):** mismo dato directo de Odoo. La diferencia: "Compras ENAP" suele llenarse manualmente (cuenta corriente offline).

### Gerencia
- **Margen Contado vs Crédito:** Contado = término "1 día" o "prepago". Crédito = 15, 21, 30, 45, 60, 90, 150 días.
- **Visitas reales:** manual hoy. Pendiente: calcular desde CRM etapa "ruta" (actualizados/agregados en la semana).
- **Cotizaciones canceladas:** Ventas → Cotizaciones → Canceladas → filtrar por fecha de orden.
- **Precio venta promedio (fila 13):** campo `invoice_price` (con IVA + IEC). Solo Diésel B1.

### Estado de automatización (mayo 2026)
| Campo | Estado |
|---|---|
| Litros vendidos | ✅ Match Odoo (`account.move.line.quantity`) |
| Ventas en $ | ✅ Match (`account.move`, amount_total − NC) |
| Margen bruto | ✅ `margin_zone` ponderado |
| Recaudación total | ✅ `account.bank.statement.line` (Banco de Chile) |
| Subcategorías recaudación | ⚠️ Por ref pattern, ~5% error |
| Compras ENAP | ⚠️ Usa todas in_invoice; verificar filtro ENAP+ADQUIM+ADGREEN |
| Rescate/crédito | ❌ Manual |
| Campos Gerencia (visitas, cot. canceladas, margen contado/crédito) | ❌ Pendientes |

---

## 2. Calculadora de excepción — detalle Mantenedor

La fórmula vigente está en CLAUDE.md. Detalle adicional (actualizado ago-2026, Mantenedor en NETOS):

- `base_min_zona` = `ENAP_neto/(1−margen_min)` según tipo: Crédito (7,5%) = col I · Volumen (5,5%) = col K · Contado (6,5%) = col M. Desde ago-2026 las columnas I/K/M del Mantenedor SON esa fórmula (antes eran valores pre-calculados en bruto — la advertencia vieja de "no calcular precio implícito" ya no aplica).
- **Inferencia de tipo_venta** (acordada mayo 2026): `is_volume_client = true` → Volumen (flag gana) · `avg_payment_days ≤ 7` → Contado · 8–60 días → Crédito.
- Días usados = REALES (`avg_payment_days`), no pactados. Semáforo estricto por diseño.
- RM sin fila propia en Mantenedor → usa precio San Fernando ("Paine y Lampa se cotizan a precio San Fernando").

Sanity checks validados contra las fórmulas del Sheet (ago-2026, netos):
- San Fernando / Crédito / 30 días → Mínimo $1.181 / Excepción $1.171 / B−73 ✓
- Talca / Crédito / 30 días → Mínimo $1.169 / Excepción $1.159 / B−36 ✓
- (Históricos pre-ago-2026, tabla bruta: VI Costa/Crédito/60d → $1.510 B−16 · Curicó/Volumen/7d → $1.432 B−71 — ya no reproducibles con la tabla nueva.)

---

## 3. Bugs históricos resueltos (no repetir soluciones fallidas)

| Bug | Solución |
|---|---|
| Archivo corrompido en repo | `git checkout <commit-hash> -- <archivo>` |
| `const dy` declarado dos veces en JS | Renombrar una variable |
| Workflow `env:` sobreescribe secrets con string vacío | Mover env al step, no al job |
| Autenticación XML-RPC falla silenciosamente | Verificar que `uid` retornado no sea `False` |
| Canvas null en renderMesVencido | `getElementById` ejecutado antes de `innerHTML`; verificar null antes de Chart.js |
| Route: "unsupported XML-RPC protocol" | `ODOO_URL` vacío; usar literales en YAML |
| Cash Flow: doble $ en moneda | `fM()` ya devuelve $X.XM; usar `fNoSign()` para concatenar |
| Cash Flow: `kpi()` borrada al insertar tab | Restaurar firma `function kpi(label,val,delta,cls,sub){...}` |
| Excepción: signo C10 invertido | C10 = −16 (negativo). `minimo = base + (−días + 30) × C10 / 30` |
| Excepción: `\|\|` falla con avg_payment_days=0 | Usar `!= null ? :` para que 0 no caiga al fallback |
| DSO: query account.move.line gigante | `read_group` suma en servidor (⚠️ pero ver bug DSO abierto en CLAUDE.md — read_group NO netea reconciliados) |

---

## 4. Consulta Odoo rápida vía browser

Desde la consola del browser logueada en Odoo (Chrome: escribir "allow pasting" una vez por sesión DevTools):

```javascript
fetch('/web/dataset/call_kw', {
  method: 'POST',
  headers: {'Content-Type': 'application/json'},
  credentials: 'include',
  body: JSON.stringify({
    jsonrpc: '2.0', method: 'call', id: 1,
    params: {
      model: 'sale.order',
      method: 'search_read',
      args: [[['invoice_status','=','to invoice']]],
      kwargs: { fields: ['name','partner_shipping_id','warehouse_id'], limit: 5 }
    }
  })
}).then(r=>r.json()).then(out => { copy(out); console.log(out); })
```

`copy(out)` deja el JSON en el portapapeles.

---

## 5. Bot Telegram (Fase 1 — nunca deployado, archivado)

Código entregado mayo 2026, deploy en Render.com pendiente y abandonado. Si se retoma:
- Repo separado sugerido: `tomenergy-bot/` (bot.py con python-telegram-bot + Anthropic, render.yaml Background Worker plan free)
- Tools de lectura sobre los JSON: get_dashboard_summary, list_clients_by_margin_band, search_client, get_weekly_breakdown, get_credit_risk_detail, get_executives_performance
- Env vars: TELEGRAM_TOKEN, ANTHROPIC_API_KEY, ALLOWED_USERS (⚠️ vacío = acepta a todos, inseguro), DASHBOARD_BASE_URL
- Roadmap: Fase 2 = queries vivos XML-RPC · Fase 3 = acciones + alertas proactivas
