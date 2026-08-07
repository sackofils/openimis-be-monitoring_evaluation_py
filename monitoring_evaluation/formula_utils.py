import logging
from contextvars import ContextVar

from .models import IndicatorValue


logger = logging.getLogger(__name__)
_audit_user = ContextVar('monitoring_evaluation_audit_user', default=None)


def set_indicator_audit_user(user):
    return _audit_user.set(user)


def reset_indicator_audit_user(token):
    _audit_user.reset(token)


def save_indicator_value(
    indicator,
    start,
    end,
    value,
    region=None,
    gender=None,
    source="SYSTEM",
):
    indicator_value = IndicatorValue.objects.filter(
        indicator=indicator,
        period_start=start,
        period_end=end,
        region_code=region,
        gender=gender,
    ).first()

    if indicator_value is None:
        indicator_value = IndicatorValue(
            indicator=indicator,
            period_start=start,
            period_end=end,
            region_code=region,
            gender=gender,
            value=value,
            source=source,
            validated=True,
        )
        audit_user = _audit_user.get()
        if audit_user is not None:
            indicator_value.save(user=audit_user)
        else:
            indicator_value.save(username='Admin')
        logger.info("[ME] %s created for %s to %s", indicator.code, start, end)
        return indicator_value

    changed = False
    for field, new_value in (
        ("value", value),
        ("source", source),
        ("validated", True),
    ):
        if getattr(indicator_value, field) != new_value:
            setattr(indicator_value, field, new_value)
            changed = True

    if changed:
        audit_user = _audit_user.get()
        if audit_user is not None:
            indicator_value.save(user=audit_user)
        else:
            indicator_value.save(username='Admin')
        logger.info("[ME] %s updated for %s to %s", indicator.code, start, end)

    return indicator_value
