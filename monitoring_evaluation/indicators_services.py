import logging
from django.apps import apps
from django.db.models import Sum
from django.db import transaction
from .models import Indicator, MonitoringLog
from .formula_utils import (
    reset_indicator_audit_user,
    save_indicator_value as _save_value,
    set_indicator_audit_user,
)
from .formulas.coaching import (
    calc_PIP_026, calc_PIP_027, calc_PIP_028, calc_PIP_029,
)
from .formulas.pip import (
    calc_PIP_011, calc_PIP_013, calc_PIP_014, calc_PIP_015, calc_PIP_016,
    calc_PIP_017, calc_PIP_018, calc_PIP_025,
)
from .formulas.result_framework import (
    calc_IRI_012, calc_ODP_002, calc_ODP_003, calc_ODP_004, calc_ODP_005,
    calc_ODP_006,
)


logger = logging.getLogger(__name__)


# --------------------------------------------------------------------
# Utilitaire d’enregistrement de valeur
# --------------------------------------------------------------------

def _safe_percent(num, den):
    return round((num / den) * 100, 2) if den else 0.0


def compute_indicator_from_datasource(indicator, start, end):
    ds = indicator.data_source
    if not ds or not ds.is_active:
        return

    Model = apps.get_model(ds.module, ds.model)

    qs = Model.objects.filter(**{
        f"{ds.date_field}__range": (start, end)
    })

    if ds.filters:
        qs = qs.filter(**ds.filters)

    if ds.aggregation == "COUNT":
        value = qs.count()

    elif ds.aggregation == "COUNT_DISTINCT":
        value = qs.values(ds.distinct_field).distinct().count()

    elif ds.aggregation == "SUM":
        value = qs.aggregate(
            total=Sum(ds.value_field)
        )["total"] or 0

    elif ds.aggregation == "PERCENT":
        num = qs.filter(**(ds.numerator_filters or {})) \
                .values(ds.distinct_field).distinct().count()

        den = qs.filter(**(ds.denominator_filters or {})) \
                .values(ds.distinct_field).distinct().count()

        value = _safe_percent(num, den)

    else:
        raise ValueError(f"Aggregation inconnue: {ds.aggregation}")

    _save_value(indicator, start, end, value)

FORMULAS = {
    "IRI_012": calc_IRI_012,
    "ODP_002": calc_ODP_002,
    "ODP_003": calc_ODP_003,
    "ODP_004": calc_ODP_004,
    "ODP_005": calc_ODP_005,
    "ODP_006": calc_ODP_006,
    "PIP_11": calc_PIP_011,
    "PIP_13": calc_PIP_013,
    "PIP_14": calc_PIP_014,
    "PIP_15": calc_PIP_015,
    "PIP_16": calc_PIP_016,
    "PIP_17": calc_PIP_017,
    "PIP_18": calc_PIP_018,
    "PIP_25": calc_PIP_025,
    "PIP_26": calc_PIP_026,
    "PIP_27": calc_PIP_027,
    "PIP_28": calc_PIP_028,
    "PIP_29": calc_PIP_029,
}

@transaction.atomic
def calculate_me_indicators_for_period(start, end, user=None, indicators=None):
    base_queryset = indicators if indicators is not None else Indicator.objects.all()
    datasource_indicators = base_queryset.filter(
        is_active=True,
        method='AUTOMATIQUE',
        data_source__is_active=True,
    )

    audit_token = set_indicator_audit_user(user)
    computed = 0
    computed_ids = set()
    errors = []

    for ind in datasource_indicators:
        try:
            compute_indicator_from_datasource(ind, start, end)
            computed += 1
            computed_ids.add(ind.id)
        except Exception as e:
            msg = f"{ind.code}: {e}"
            errors.append(msg)
            logger.error(f"[ME] {msg}")

    formula_indicators = base_queryset.filter(
        is_active=True,
        method='AUTOMATIQUE',
        formula__isnull=False,
    ).exclude(id__in=computed_ids)
    for ind in formula_indicators:
        try:
            fn = FORMULAS.get(ind.formula)
            if fn is None:
                raise ValueError(f'Formule inconnue: {ind.formula}')
            fn(ind, start, end)
            computed += 1
        except Exception as e:
            msg = f"{ind.code}: {e}"
            errors.append(msg)
            logger.error(f"[ME] {msg}")


    reset_indicator_audit_user(audit_token)
    monitoring = MonitoringLog(
        period_start=start,
        period_end=end,
        indicators_count=computed,
        success=(len(errors) == 0),
        error_details="\n".join(errors) if errors else None,
        executed_by=user,
    )
    monitoring.save(user=user)

    return computed
