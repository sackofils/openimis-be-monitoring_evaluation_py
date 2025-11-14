import logging
from django.db import transaction
from .models import Indicator, IndicatorValue, MonitoringSubmission, MonitoringLog

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------
# Utilitaire d’enregistrement de valeur
# --------------------------------------------------------------------

def _save_value(indicator, start, end, value, region=None, gender=None, source="KoboForm"):
    """
    Crée ou met à jour une valeur d’indicateur pour une période donnée.
    """
    IndicatorValue.objects.update_or_create(
        indicator=indicator,
        period_start=start,
        period_end=end,
        region_code=region,
        gender=gender,
        defaults=dict(
            value=value,
            source=source,
            validated=True,
        ),
    )
    logger.debug(f"[ME] {indicator.code}: sauvegardé ({start}→{end}) = {value}")


# --------------------------------------------------------------------
# Formules ODP / IRI (issues des formulaires Kobo)
# --------------------------------------------------------------------

def calc_ODP_002(indicator, start, end):
    """ODP.002 – Bénéficiaires TMU urgences COVID/chocs"""
    qs = MonitoringSubmission.objects.filter(
        form_type="TMU_TMR",
        submitted_at__range=(start, end),
        json_ext__type_transfert="TMU",
        is_deleted=False,
    ).exclude(beneficiary_id=None)
    count = qs.values("beneficiary_id").distinct().count()
    _save_value(indicator, start, end, count)

def calc_ODP_003(indicator, start, end):
    """ODP.003 – Bénéficiaires TMU urgences (femmes)"""
    qs = MonitoringSubmission.objects.filter(
        form_type="TMU_TMR",
        submitted_at__range=(start, end),
        json_ext__type_transfert="TMU",
        json_ext__genre="F",
        is_deleted=False,
    ).exclude(beneficiary_id=None)
    count = qs.values("beneficiary_id").distinct().count()
    _save_value(indicator, start, end, count, gender="F")

def calc_ODP_004(indicator, start, end):
    """ODP.004 – Bénéficiaires TM réguliers NAFA"""
    qs = MonitoringSubmission.objects.filter(
        form_type="TMU_TMR",
        submitted_at__range=(start, end),
        json_ext__type_transfert="TMR",
        is_deleted=False,
    ).exclude(beneficiary_id=None)
    count = qs.values("beneficiary_id").distinct().count()
    _save_value(indicator, start, end, count)

def calc_ODP_005(indicator, start, end):
    """ODP.005 – Bénéficiaires TM réguliers NAFA (femmes)"""
    qs = MonitoringSubmission.objects.filter(
        form_type="TMU_TMR",
        submitted_at__range=(start, end),
        json_ext__type_transfert="TMR",
        json_ext__genre="F",
        is_deleted=False,
    ).exclude(beneficiary_id=None)
    count = qs.values("beneficiary_id").distinct().count()
    _save_value(indicator, start, end, count, gender="F")

def calc_ODP_006(indicator, start, end):
    """ODP.006 – Bénéficiaires de programmes de filets sociaux"""
    qs = MonitoringSubmission.objects.filter(
        form_type="TMU_TMR",
        submitted_at__range=(start, end),
        is_deleted=False,
    ).exclude(beneficiary_id=None)
    count = qs.values("beneficiary_id").distinct().count()
    _save_value(indicator, start, end, count)

def calc_IRI_001(indicator, start, end):
    """IRI.001 – TM d'urgence reçus à temps"""
    qs = MonitoringSubmission.objects.filter(
        form_type="TMU_TMR",
        submitted_at__range=(start, end),
        json_ext__type_transfert="TMU",
        is_deleted=False,
    )
    on_time = qs.filter(json_ext__paiement_a_temps=True).values("beneficiary_id").distinct().count()
    total = qs.exclude(beneficiary_id=None).values("beneficiary_id").distinct().count() or 1
    pct = round((on_time / total) * 100, 2)
    _save_value(indicator, start, end, pct)

def calc_IRI_007(indicator, start, end):
    """IRI.007 – TM réguliers bénéficiant de mesures de résilience"""
    qs = MonitoringSubmission.objects.filter(
        form_type="SERE_NAFA",
        submitted_at__range=(start, end),
        json_ext__a_beneficie_ma=True,
        is_deleted=False,
    ).exclude(beneficiary_id=None)
    count = qs.values("beneficiary_id").distinct().count()
    _save_value(indicator, start, end, count)

def calc_IRI_009(indicator, start, end):
    """IRI.009 – Femmes bénéficiaires possédant un compte bancaire mobile"""
    qs = MonitoringSubmission.objects.filter(
        form_type="TMU_TMR",
        submitted_at__range=(start, end),
        json_ext__genre="F",
        is_deleted=False,
    )
    with_account = qs.filter(json_ext__has_mobile_account=True).values("beneficiary_id").distinct().count()
    total = qs.exclude(beneficiary_id=None).values("beneficiary_id").distinct().count() or 1
    pct = round((with_account / total) * 100, 2)
    _save_value(indicator, start, end, pct, gender="F")

def calc_IRI_012(indicator, start, end):
    """IRI.012 – % plaintes traitées dans les délais (KPI GRM)"""
    qs = MonitoringSubmission.objects.filter(
        form_type="GRIEVANCE_KPI",
        submitted_at__range=(start, end),
        is_deleted=False,
    )
    if not qs.exists():
        return
    agg = qs.first().json_ext or {}
    total, in_time = agg.get("total", 0), agg.get("in_time", 0)
    pct = round((in_time / (total or 1)) * 100, 2)
    _save_value(indicator, start, end, pct)


# --------------------------------------------------------------------
# Registre des formules
# --------------------------------------------------------------------

FORMULAS = {
    "ODP_002": calc_ODP_002,
    "ODP_003": calc_ODP_003,
    "ODP_004": calc_ODP_004,
    "ODP_005": calc_ODP_005,
    "ODP_006": calc_ODP_006,
    "IRI_001": calc_IRI_001,
    "IRI_007": calc_IRI_007,
    "IRI_009": calc_IRI_009,
    "IRI_012": calc_IRI_012,
}


# --------------------------------------------------------------------
# Calcul principal (Kobo / ME)
# --------------------------------------------------------------------

@transaction.atomic
def calculate_me_indicators_for_period(period_start, period_end, user=None):
    """
    Calcule tous les indicateurs automatiques Kobo (ODP/IRI) pour la période.
    """
    indicators = Indicator.objects.filter(is_active=True, is_automatic=True)
    computed, errors = 0, []

    logger.info(f"[ME] Démarrage recalcul indicateurs Kobo ({period_start} → {period_end})")

    for ind in indicators:
        try:
            fn = FORMULAS.get(ind.formula_key)
            if fn:
                fn(ind, period_start, period_end)
                computed += 1
        except Exception as e:
            msg = f"{ind.code}: {e}"
            errors.append(msg)
            logger.error(f"[ME] {msg}")

    MonitoringLog.objects.create(
        period_start=period_start,
        period_end=period_end,
        indicators_count=computed,
        success=(len(errors) == 0),
        error_details="\n".join(errors) if errors else None,
        executed_by=user,
    )

    logger.info(f"[ME] Recalcul terminé ({computed} indicateurs, erreurs={len(errors)})")
    return computed
