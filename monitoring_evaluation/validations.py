import re

from django.core.exceptions import ValidationError
from django.utils.translation import gettext as _
from django.contrib.contenttypes.models import ContentType

from core.models import User
from core.validation import BaseModelValidation, ObjectExistsValidationMixin
from monitoring_evaluation.models import Indicator, IndicatorValue


class IndicatorValidation(BaseModelValidation):
    OBJECT_TYPE = Indicator

    @classmethod
    def validate_create(cls, user, **data):
        errors = []

        unique_code_errors = validate_indicator_unique_code(data)
        for error in unique_code_errors:
            errors.append(ValidationError(error, code='unique_code_error'))

        if errors:
            raise ValidationError(errors)

        super().validate_create(user, **data)

    @classmethod
    def validate_update(cls, user, **data):
        errors = []

        #unique_code_errors = validate_indicator_unique_code(data)
        #for error in unique_code_errors:
        #    errors.append(ValidationError(error, code='unique_code_error'))

        if errors:
            raise ValidationError(errors)

        super().validate_update(user, **data)


def validate_indicator_unique_code(data):
    code = data.get('code')
    id = data.get('id')

    if not code:
        return []

    _queryset = Indicator.objects.filter(code=code)
    if id:
        _queryset.exclude(id=id)
    if _queryset.exists():
        return [{"message": _("validations.InddicatorValidation.validate_indicator_unique_code") % {"code": code}}]
    return []

class IndicatorValueValidation(BaseModelValidation):
    """
    Validation de base (structure openIMIS) pour IndicatorValue.
    - Unicité (period_start, period_end, indicator, region_code, gender)
    - Champs obligatoires selon type de valeur
    """
    OBJECT_TYPE = IndicatorValue

    # -------------------------------------------------
    # CREATE
    # -------------------------------------------------
    @classmethod
    def validate_create(cls, user, **data):
        errors = []

        # Vérifie unicité des valeurs pour la combinaison
        unique_errors = validate_indicator_value_uniqueness(data)
        errors.extend(ValidationError(e, code="unique_indicator_value") for e in unique_errors)

        # Vérifie cohérence valeur numérique vs qualitative
        value_errors = validate_value_fields(data)
        errors.extend(ValidationError(e, code="invalid_value_input") for e in value_errors)

        if errors:
            raise ValidationError(errors)

        return super().validate_create(user, **data)

    # -------------------------------------------------
    # UPDATE
    # -------------------------------------------------
    @classmethod
    def validate_update(cls, user, **data):
        errors = []

        unique_errors = validate_indicator_value_uniqueness(data)
        errors.extend(ValidationError(e, code="unique_indicator_value") for e in unique_errors)

        value_errors = validate_value_fields(data)
        errors.extend(ValidationError(e, code="invalid_value_input") for e in value_errors)

        if errors:
            raise ValidationError(errors)

        return super().validate_update(user, **data)

    @classmethod
    def validate_validate(cls, user, **data):
        """
        Validation métier avant de marquer une valeur comme 'validated'.
        """
        #if not user.has_perm("monitoring_evaluation.change_indicatorvalue"):
        #    raise ValidationError(_("Unauthorized"))

        value_id = data.get("id")
        if not value_id:
            raise ValidationError(_("Missing id for validation"))

        if not IndicatorValue.objects.filter(id=value_id).exists():
            raise ValidationError(_("IndicatorValue not found"))


# ==========================================================
# HELPERS (séparés pour être réutilisables)
# ==========================================================

def validate_indicator_value_uniqueness(data):
    """
    Vérifie l’unicité du tuple :
    (indicator, period_start, period_end, region_code, gender)
    """
    indicator_id = data.get("indicator_id") or data.get("indicator")
    period_start = data.get("period_start")
    period_end = data.get("period_end")
    region_code = data.get("region_code")
    gender = data.get("gender")
    id = data.get("id")

    if not indicator_id or not period_start or not period_end:
        return []

    qs = IndicatorValue.objects.filter(
        indicator_id=indicator_id,
        period_start=period_start,
        period_end=period_end,
        region_code=region_code,
        is_deleted=False,
        gender=gender,
    )

    if id:
        qs = qs.exclude(id=id)

    if qs.exists():
        return [{
            "message": _(
                "Il existe déjà une valeur enregistrée pour cet indicateur sur cette période et cette ventilation."
            )
        }]

    return []


def validate_value_fields(data):
    """
    Règles génériques :
    - value (float) XOR qualitative_value (str)
    - au moins une valeur doit être fournie
    - si value est fourni, qualitative_value ne doit pas l’être
    """
    value = data.get("value")
    qualitative_value = data.get("qualitative_value")

    errors = []

    # On doit avoir soit value, soit qualitative_value
    if value is None and not qualitative_value:
        errors.append({
            "message": _("Vous devez fournir une valeur numérique ou une valeur qualitative.")
        })

    # Interdit de fournir les deux
    if value is not None and qualitative_value:
        errors.append({
            "message": _("Vous ne pouvez pas fournir une valeur numérique et une valeur qualitative simultanément.")
        })

    return errors
