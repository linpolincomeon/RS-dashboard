"""
ALERTA TEMPRANA — ir.cron para Odoo 18
======================================
Cron diario que detecta clientes cuyo gap desde última factura supera
1.5× su frecuencia_facturacion y crea una actividad automática al
vendedor asignado en el lead CRM.

INSTALACIÓN:
1. Ir a Ajustes > Técnico > Automatización > Acciones planificadas
2. Crear nueva acción planificada:
   - Nombre: "Alerta Temprana - Clientes en Riesgo"
   - Modelo: Contacto (res.partner)
   - Intervalo: 1 Día
   - Siguiente ejecución: mañana a las 06:00
   - Código Python: (pegar el contenido de execute_alerta_temprana abajo)

ALTERNATIVA: Copiar solo el bloque de código dentro de la función
execute_alerta_temprana() y pegarlo directamente en el campo
"Código Python" de la acción planificada.
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

# Buscar partners activos con frecuencia_facturacion definida
partners = env['res.partner'].search([
    ('frecuencia_facturacion', '!=', False),
    ('customer_rank', '>', 0),
])

activity_type = env.ref('mail.mail_activity_data_todo', raise_if_not_found=False)
if not activity_type:
    activity_type = env['mail.activity.type'].search([('name', 'ilike', 'To Do')], limit=1)

created_count = 0

for partner in partners:
    freq_days = _parse_freq(partner.frecuencia_facturacion)
    if not freq_days:
        continue

    # Excluir estacionales (frecuencia > 60 días)
    # Descomentar la siguiente línea si se quiere excluir estacionales:
    # if freq_days > 60: continue

    threshold_days = int(freq_days * 1.5)

    # Buscar última factura posted de este partner
    last_invoice = env['account.move'].search([
        ('move_type', '=', 'out_invoice'),
        ('state', '=', 'posted'),
        ('partner_id', '=', partner.id),
    ], order='invoice_date desc', limit=1)

    if not last_invoice:
        continue

    last_date = last_invoice.invoice_date
    if not last_date:
        continue

    gap_days = (today - last_date).days

    if gap_days <= threshold_days:
        continue  # Dentro de rango normal

    # Buscar el vendedor asignado: primero del lead CRM, luego invoice_user_id
    assigned_user = None

    # Buscar lead CRM activo de este partner
    lead = env['crm.lead'].search([
        ('partner_id', '=', partner.id),
        ('active', '=', True),
    ], order='write_date desc', limit=1)

    if lead and lead.user_id:
        assigned_user = lead.user_id
    elif last_invoice.invoice_user_id:
        assigned_user = last_invoice.invoice_user_id

    if not assigned_user:
        continue  # Sin vendedor asignado, no podemos crear actividad

    # Verificar que no haya ya una actividad pendiente similar (evitar duplicados)
    existing = env['mail.activity'].search([
        ('res_model', '=', 'res.partner'),
        ('res_id', '=', partner.id),
        ('user_id', '=', assigned_user.id),
        ('summary', 'ilike', 'días sin comprar'),
    ], limit=1)

    if existing:
        continue  # Ya tiene actividad pendiente

    # Crear la actividad
    summary = f"⚠️ {partner.name} lleva {gap_days} días sin comprar (freq: {freq_days}d)"

    env['mail.activity'].create({
        'res_model_id': env['ir.model']._get_id('res.partner'),
        'res_id': partner.id,
        'activity_type_id': activity_type.id if activity_type else False,
        'user_id': assigned_user.id,
        'date_deadline': today + timedelta(days=2),
        'summary': summary,
        'note': f'<p>El cliente <strong>{partner.name}</strong> tiene frecuencia de compra '
                f'cada {freq_days} días pero lleva <strong>{gap_days} días</strong> sin factura. '
                f'Última factura: {last_date.strftime("%d/%m/%Y")}.</p>'
                f'<p>Contactar para retener.</p>',
    })
    created_count += 1

log(f"Alerta Temprana: {created_count} actividades creadas de {len(partners)} partners evaluados", level='info')

# ═══════════════════════════════════════════════════════════════
# FIN DEL CÓDIGO PARA PEGAR EN ODOO
# ═══════════════════════════════════════════════════════════════
