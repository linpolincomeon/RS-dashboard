"""
AUTOMATIZACIÓN — Mover leads a etapa "En Riesgo"
=================================================
Cron diario que mueve leads CRM a la etapa "En Riesgo" cuando el
partner supera 1.5× su frecuencia_facturacion.

Excluye:
- Estacionales (frecuencia > 60 días)
- Leads ya en etapas terminales (Ganado, Perdido)
- Leads que ya están en "En Riesgo"

INSTALACIÓN:
1. Ir a Ajustes > Técnico > Automatización > Acciones planificadas
2. Crear nueva acción planificada:
   - Nombre: "Mover leads a En Riesgo"
   - Modelo: Lead/Oportunidad (crm.lead)
   - Intervalo: 1 Día
   - Siguiente ejecución: mañana a las 06:30
   - Código Python: (pegar el contenido de abajo)

NOTA: La etapa "En Riesgo" ya debe existir en CRM > Configuración > Etapas.
Si no existe, el script la crea automáticamente.
"""

# ═══════════════════════════════════════════════════════════════
# COPIAR DESDE AQUÍ para pegar en Odoo > Acciones Planificadas
# ═══════════════════════════════════════════════════════════════

import re
from datetime import datetime, timedelta

def _parse_freq(freq_str):
    """Parse frecuencia_facturacion → days."""
    if not freq_str:
        return None
    f = freq_str.strip().lower()
    if "semanal" in f and "bi" not in f and "quince" not in f:
        return 7
    if "quincenal" in f:
        return 15
    if "mensual" in f and "bi" not in f and "tri" not in f:
        return 30
    if "bimensual" in f or "bimestral" in f:
        return 60
    if "trimestral" in f:
        return 90
    m = re.search(r'(\d+)\s*d[ií]a', f)
    if m:
        return int(m.group(1))
    m2 = re.search(r'(\d+)', f)
    if m2:
        return int(m2.group(1))
    return None

today = datetime.now().date()

# Buscar o crear la etapa "En Riesgo"
en_riesgo_stage = env['crm.stage'].search([('name', 'ilike', 'En Riesgo')], limit=1)
if not en_riesgo_stage:
    # Buscar la etapa Durmiente para poner En Riesgo justo antes
    durmiente = env['crm.stage'].search([('name', 'ilike', 'Durmiente')], limit=1)
    seq = durmiente.sequence - 1 if durmiente else 50
    en_riesgo_stage = env['crm.stage'].create({
        'name': 'En Riesgo',
        'sequence': seq,
        'fold': False,
    })
    log(f"Etapa 'En Riesgo' creada con sequence={seq}", level='info')

# Etapas terminales que no se deben tocar
terminal_names = ['ganado', 'won', 'perdido', 'lost', 'no cerrado']
terminal_stages = env['crm.stage'].search([])
terminal_ids = [s.id for s in terminal_stages if any(t in s.name.lower() for t in terminal_names)]
# También excluir la propia etapa En Riesgo y Durmiente (ya están ahí)
skip_stages = terminal_ids + [en_riesgo_stage.id]
durmiente_stage = env['crm.stage'].search([('name', 'ilike', 'Durmiente')], limit=1)
if durmiente_stage:
    skip_stages.append(durmiente_stage.id)

# Buscar leads activos que NO estén en etapas terminales/skip
leads = env['crm.lead'].search([
    ('active', '=', True),
    ('stage_id', 'not in', skip_stages),
    ('partner_id', '!=', False),
])

moved_count = 0

for lead in leads:
    partner = lead.partner_id
    if not partner:
        continue

    freq_days = _parse_freq(partner.frecuencia_facturacion)
    if not freq_days:
        continue

    # Excluir estacionales (frecuencia > 60 días)
    if freq_days > 60:
        continue

    threshold_days = int(freq_days * 1.5)

    # Buscar última factura de este partner
    last_invoice = env['account.move'].search([
        ('move_type', '=', 'out_invoice'),
        ('state', '=', 'posted'),
        ('partner_id', '=', partner.id),
    ], order='invoice_date desc', limit=1)

    if not last_invoice or not last_invoice.invoice_date:
        continue

    gap_days = (today - last_invoice.invoice_date).days

    if gap_days > threshold_days:
        # Mover a En Riesgo
        lead.stage_id = en_riesgo_stage.id
        moved_count += 1

log(f"En Riesgo: {moved_count} leads movidos de {len(leads)} evaluados", level='info')

# ═══════════════════════════════════════════════════════════════
# FIN DEL CÓDIGO PARA PEGAR EN ODOO
# ═══════════════════════════════════════════════════════════════
