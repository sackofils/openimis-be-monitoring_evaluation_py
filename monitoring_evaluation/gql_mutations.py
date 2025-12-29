import graphene
from django.core.exceptions import PermissionDenied
from django.utils.translation import gettext as _
from datetime import date

from core.gql.gql_mutations.base_mutation import BaseHistoryModelCreateMutationMixin, BaseMutation, \
    BaseHistoryModelUpdateMutationMixin, BaseHistoryModelDeleteMutationMixin
from core.schema import OpenIMISMutation
from .models import Indicator, IndicatorValue, MonitoringLog
from django.db import transaction

from django.db.models import Count, Sum, Q
from core.models import User
#from .indicators_services import _FORMULAS, compute_indicator_value, calculate_indicators_for_period
from .services import IndicatorService, IndicatorValueService
from django.core.exceptions import ValidationError



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

class CreateIndicatorInput(OpenIMISMutation.Input):
    code = graphene.String(required=True)
    name = graphene.String(required=True)
    description = graphene.String()
    unit = graphene.String(required=True)
    frequency = graphene.String(required=True)
    calculation_method = graphene.String(required=True)
    type = graphene.String(required=True)
    status = graphene.String(required=True)
    module = graphene.String()
    target = graphene.Float()
    method = graphene.String()
    category = graphene.String()
    formula = graphene.String()
    calculation_method = graphene.String()
    is_automatic = graphene.Boolean(default_value=False)
    is_active = graphene.Boolean(default_value=True)

class CreateIndicatorMutation(BaseHistoryModelCreateMutationMixin, BaseMutation):
    """
    Mutation pour créer un nouvel indicateur du cadre de résultats.
    """
    _mutation_class = "CreateIndicatorMutation"
    _mutation_module = "monitoring_evaluation"
    _model = Indicator

    @classmethod
    def _validate_mutation(cls, user, **data):
        super()._validate_mutation(user, **data)

    @classmethod
    def _mutate(cls, user, **data):
        client_mutation_id = data.pop('client_mutation_id')
        if "client_mutation_label" in data:
            data.pop('client_mutation_label')

        service = IndicatorService(user)
        response = service.create(data)

        if not response['success']:
            return response
        return None

    class Input(CreateIndicatorInput):
        pass

class UpdateIndicatorInput(OpenIMISMutation.Input):
    id = graphene.String(required=True)

    code = graphene.String(required=True)
    name = graphene.String(required=True)
    description = graphene.String()

    type = graphene.String(required=True)
    unit = graphene.String(required=True)
    frequency = graphene.String(required=True)

    target = graphene.Float()
    module = graphene.String()
    formula = graphene.String()
    method = graphene.String()
    category = graphene.String()
    calculation_method = graphene.String()

    status = graphene.String(required=True)
    is_automatic = graphene.Boolean()
    is_active = graphene.Boolean()

class UpdateIndicatorMutation(BaseHistoryModelUpdateMutationMixin, BaseMutation):
    """
    Met à jour un indicateur existant via IndicatorService.
    """
    _mutation_class = "UpdateIndicatorMutation"
    _mutation_module = "monitoring_evaluation"
    _model = Indicator

    class Input(UpdateIndicatorInput):
        pass

    @classmethod
    def _validate_mutation(cls, user, **data):
        #if not user.has_perm("monitoring_evaluation.change_indicator"):
        #    raise PermissionDenied(_("Unauthorized"))
        super()._validate_mutation(user, **data)

    @classmethod
    def _mutate(cls, user, **data):
        client_mutation_id = data.pop("client_mutation_id", None)
        if "client_mutation_label" in data:
            data.pop("client_mutation_label")

        service = IndicatorService(user)
        response = service.update(data)

        if not response.get("success"):
            return response

        return None


class DeleteIndicatorInput(OpenIMISMutation.Input):
    id = graphene.String(required=True)


class DeleteIndicatorMutation(BaseHistoryModelDeleteMutationMixin, BaseMutation):
    """
    Supprime un indicateur (et ses valeurs via cascade).
    """
    _mutation_class = "DeleteIndicatorMutation"
    _mutation_module = "monitoring_evaluation"
    _model = Indicator

    class Input(DeleteIndicatorInput):
        pass


class DuplicateIndicatorInput(OpenIMISMutation.Input):
    id = graphene.String(required=True)
    new_code = graphene.String(required=True)


class DuplicateIndicatorMutation(BaseHistoryModelCreateMutationMixin, BaseMutation):
    """
    Duplique un indicateur en créant un nouvel enregistrement brouillon.
    """
    _mutation_class = "DuplicateIndicatorMutation"
    _mutation_module = "monitoring_evaluation"

    new_id = graphene.String()

    class Input(DuplicateIndicatorInput):
        pass

    @classmethod
    def _validate_mutation(cls, user, **data):
        #if not user.has_perm("monitoring_evaluation.add_indicator"):
        #    raise PermissionDenied(_("Unauthorized"))
        super()._validate_mutation(user, **data)

    @classmethod
    def _mutate(cls, user, **data):
        # Extraction clientMutationId et label
        client_mutation_id = data.pop("client_mutation_id", None)
        if "client_mutation_label" in data:
            data.pop("client_mutation_label")

        indicator_gid = data.get("id")
        new_code = data.get("new_code")

        # Conversion Relay → instance Django
        instance = Indicator.objects.get(id=indicator_gid)

        # Vérification unicité du code
        if Indicator.objects.filter(code=new_code).exists():
            raise ValidationError(_(f"Le code '{new_code}' existe déjà."))

        # Construire les données du nouvel indicateur
        new_data = {
            "code": new_code,
            "name": instance.name,
            "description": instance.description,
            "type": instance.type,
            "unit": instance.unit,
            "frequency": instance.frequency,
            "target": instance.target,
            "module": instance.module,
            "formula": instance.formula,
            "method": instance.method,
            "calculation_method": instance.calculation_method,
            "status": "BROUILLON",
            "is_automatic": instance.is_automatic,
            "is_active": True,
        }

        # Service Layer openIMIS
        service = IndicatorService(user)
        response = service.create(new_data)

        if not response.get("success"):
            return response

        return None


# ========================================
# Ajout manuel d’une valeur d’indicateur
# ========================================

class CreateManualIndicatorValueInput(OpenIMISMutation.Input):
    indicator_id = graphene.String(required=True)
    period_start = graphene.String(required=True)
    period_end = graphene.String(required=True)
    region_code = graphene.String()
    gender = graphene.String()
    value = graphene.Float()
    qualitative_value = graphene.String()
    source = graphene.String()
    validated = graphene.Boolean(default_value=False)

class UpdateManualIndicatorValueInput(OpenIMISMutation.Input):
  id = graphene.String(required=True)
  period_start = graphene.String()
  period_end = graphene.String()
  value = graphene.Float()
  source = graphene.String()
  region_code = graphene.String()
  gender = graphene.String()

class CreateManualIndicatorValueMutation(BaseHistoryModelCreateMutationMixin, BaseMutation):
    """
    Mutation pour créer une valeur d’indicateur (saisie manuelle).
    Format identique à CreateIndicatorMutation.
    """
    _mutation_class = "CreateManualIndicatorValueMutation"
    _mutation_module = "monitoring_evaluation"
    _model = IndicatorValue

    # Pour que le FE puisse récupérer la valeur via
    # data.createManualIndicatorValue.value
    value = graphene.Float()

    class Input(CreateManualIndicatorValueInput):
        pass

    @classmethod
    def _validate_mutation(cls, user, **data):
        # Validation générique + rules via BaseModelValidation
        super()._validate_mutation(user, **data)

    @classmethod
    def _mutate(cls, user, **data):
        # extraction clientMutationId / label
        client_mutation_id = data.pop("client_mutation_id", None)
        if "client_mutation_label" in data:
            data.pop("client_mutation_label")

        service = IndicatorValueService(user)
        response = service.create(data)

        if not response.get("success"):
            return response  # géré par middleware openIMIS

        # iv = response["data"]
        return None

class UpdateManualIndicatorValueMutation(BaseHistoryModelUpdateMutationMixin, BaseMutation):
    """
    Met à jour une valeur manuelle existante.
    """
    _mutation_class = "UpdateManualIndicatorValueMutation"
    _mutation_module = "monitoring_evaluation"
    _model = IndicatorValue

    value = graphene.Float()

    class Input(UpdateManualIndicatorValueInput):
        pass

    @classmethod
    def _mutate(cls, user, **data):
        client_mutation_id = data.pop("client_mutation_id", None)
        if "client_mutation_label" in data:
            data.pop("client_mutation_label")

        service = IndicatorValueService(user)
        response = service.update(data)

        if not response.get("success"):
            return response

        # iv = response["data"]
        # return UpdateManualIndicatorValueMutation(
        #    value=iv.get("value"),
        #    client_mutation_id=client_mutation_id,
        #)

class DeleteManualIndicatorValueInput(OpenIMISMutation.Input):
    id = graphene.ID(required=True)


class DeleteManualIndicatorValueMutation(BaseHistoryModelDeleteMutationMixin, BaseMutation):
    """
    Supprime une valeur d’indicateur.
    """
    _mutation_class = "DeleteManualIndicatorValueMutation"
    _mutation_module = "monitoring_evaluation"
    _model = IndicatorValue

    ok = graphene.Boolean()

    class Input(DeleteManualIndicatorValueInput):
        pass


class ValidateManualIndicatorValueInput(OpenIMISMutation.Input):
    id = graphene.ID(required=True)


class ValidateManualIndicatorValueMutation(BaseHistoryModelCreateMutationMixin, BaseMutation):
    """
    Marque une valeur comme validée (validated=True).
    """
    _mutation_class = "ValidateManualIndicatorValueMutation"
    _mutation_module = "monitoring_evaluation"

    value = graphene.Float()

    class Input(ValidateManualIndicatorValueInput):
        pass

    @classmethod
    def _mutate(cls, user, **data):
        client_mutation_id = data.pop("client_mutation_id", None)
        if "client_mutation_label" in data:
            data.pop("client_mutation_label")

        service = IndicatorValueService(user)
        response = service.validate_value(data)

        if not response.get("success"):
            return response

        return None
        #iv = response["data"]
        #return ValidateManualIndicatorValueMutation(
        #    value=iv.get("value"),
        #    client_mutation_id=client_mutation_id,
        #)

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


