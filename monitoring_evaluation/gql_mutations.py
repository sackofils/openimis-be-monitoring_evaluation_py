import graphene
from django.core.exceptions import PermissionDenied
from django.utils.translation import gettext as _
from datetime import date

from core.schema import OpenIMISMutation
from .models import Indicator, IndicatorValue, MonitoringLog
from .services import calculate_indicators_for_period

from django.db.models import Count, Sum, Q
from core.models import User
from .services import _FORMULAS, compute_indicator_value


@transaction.atomic
def calculate_indicators_for_period(period_start: date, period_end: date, ctx=None) -> int:
    """
    Calcule tous les indicateurs automatiques actifs pour une période donnée
    et consigne l'exécution dans le journal MonitoringLog.
    """
    user = ctx.get("user") if ctx else None
    log = MonitoringLog.objects.create(
        period_start=period_start,
        period_end=period_end,
        executed_by=user,
        success=True,
        indicators_count=0,
    )

    indicators = Indicator.objects.filter(is_automatic=True, is_active=True)
    count = 0
    errors = []

    for ind in indicators:
        try:
            defaults = compute_indicator_value(ind, period_start, period_end, ctx)
            IndicatorValue.objects.update_or_create(
                indicator=ind,
                period_start=period_start,
                period_end=period_end,
                defaults={**defaults, "validated": False},
            )
            count += 1
        except Exception as e:
            errors.append(f"{ind.code}: {str(e)}")

    # mise à jour du log
    log.indicators_count = count
    if errors:
        log.success = False
        log.error_details = "\n".join(errors)
    log.save(user=user if user else None)

    return count


# ========================================
# Création d’un indicateur
# ========================================

class CreateIndicatorMutation(OpenIMISMutation):
    """
    Mutation pour créer un nouvel indicateur du cadre de résultats.
    """

    class Input:
        code = graphene.String(required=True)
        name = graphene.String(required=True)
        description = graphene.String()
        unit = graphene.String(required=True)
        frequency = graphene.String(required=True)
        disaggregation = graphene.String()
        data_source = graphene.String()
        collection_method = graphene.String()
        responsible = graphene.String()
        calculation_method = graphene.String()
        quality_review = graphene.String()
        budget_notes = graphene.String()
        is_automatic = graphene.Boolean(default_value=False)
        is_active = graphene.Boolean(default_value=True)

    @classmethod
    def mutate(cls, root, info, **data):
        user = info.context.user
        if not user.has_perms('monitoring_evaluation.add_indicator'):
            raise PermissionDenied(_("Unauthorized"))

        indicator = Indicator(**data)
        indicator.save(user=user)
        return None


# ========================================
# Ajout manuel d’une valeur d’indicateur
# ========================================

class CreateIndicatorValueMutation(OpenIMISMutation):
    """
    Permet d’enregistrer manuellement une valeur d’indicateur (cas non automatisé).
    """

    class Input:
        indicator_id = graphene.Int(required=True)
        period_start = graphene.String(required=True)
        period_end = graphene.String(required=True)
        region_code = graphene.String()
        gender = graphene.String()
        value = graphene.Float()
        qualitative_value = graphene.String()
        source = graphene.String()
        validated = graphene.Boolean(default_value=False)

    @classmethod
    def mutate(cls, root, info, **data):
        user = info.context.user
        if not user.has_perms('monitoring_evaluation.add_indicatorvalue'):
            raise PermissionDenied(_("Unauthorized"))

        try:
            indicator = Indicator.objects.get(id=data.pop("indicator_id"))
        except Indicator.DoesNotExist:
            raise PermissionDenied(_("Indicateur introuvable"))

        # Conversion dates ISO
        data["period_start"] = date.fromisoformat(data["period_start"])
        data["period_end"] = date.fromisoformat(data["period_end"])

        value = IndicatorValue(indicator=indicator, **data)
        value.save(user=user)
        return None


# ========================================
# Recalcul automatique des indicateurs
# ========================================

class RecalculateIndicatorsMutation(OpenIMISMutation):
    """
    Lance le recalcul automatique des indicateurs pour une période donnée.
    """

    class Input:
        period_start = graphene.String(required=True)
        period_end = graphene.String(required=True)

    updated_count = graphene.Int()

    @classmethod
    def mutate(cls, root, info, **data):
        user = info.context.user
        if not user.has_perms('monitoring_evaluation.change_indicatorvalue'):
            raise PermissionDenied(_("Unauthorized"))

        ps = date.fromisoformat(data["period_start"])
        pe = date.fromisoformat(data["period_end"])

        count = calculate_indicators_for_period(ps, pe, ctx={"user": user})
        return RecalculateIndicatorsMutation(updated_count=count)


# ========================================
# Mutation root exportable
# ========================================

class Mutation(graphene.ObjectType):
    create_indicator = CreateIndicatorMutation.Field()
    create_indicator_value = CreateIndicatorValueMutation.Field()
    recalculate_indicators = RecalculateIndicatorsMutation.Field()
