import graphene
from django.core.exceptions import PermissionDenied
from django.utils.translation import gettext as _
from datetime import date

from core.gql.gql_mutations.base_mutation import BaseHistoryModelCreateMutationMixin, BaseMutation, \
    BaseHistoryModelUpdateMutationMixin, BaseHistoryModelDeleteMutationMixin
from core.schema import OpenIMISMutation
from .apps import MonitoringEvaluationConfig
from .models import Indicator, IndicatorValue

from .services import IndicatorService, IndicatorValueService
from django.core.exceptions import ValidationError



def _require_permissions(user, permissions):
    if not user.has_perms(permissions):
        raise PermissionDenied(_('Unauthorized'))


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
        _require_permissions(user, MonitoringEvaluationConfig.gql_mutation_indicators_add_perms)

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id')
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
        super()._validate_mutation(user, **data)
        _require_permissions(user, MonitoringEvaluationConfig.gql_mutation_indicators_update_perms)

    @classmethod
    def _mutate(cls, user, **data):
        data.pop("client_mutation_id", None)
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

    @classmethod
    def _validate_mutation(cls, user, **data):
        super()._validate_mutation(user, **data)
        _require_permissions(user, MonitoringEvaluationConfig.gql_mutation_indicators_delete_perms)

    @classmethod
    def _mutate(cls, user, **data):
        data = dict(data)
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)
        data['user'] = user
        response = IndicatorService(user).delete(data)
        return None if response.get('success') else response


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
        super()._validate_mutation(user, **data)
        _require_permissions(user, MonitoringEvaluationConfig.gql_mutation_indicators_duplicate_perms)

    @classmethod
    def _mutate(cls, user, **data):
        # Extraction clientMutationId et label
        data.pop("client_mutation_id", None)
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
            "category": instance.category,
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
        _require_permissions(user, MonitoringEvaluationConfig.gql_mutation_indicators_add_value_perms)

    @classmethod
    def _mutate(cls, user, **data):
        # extraction clientMutationId / label
        data.pop("client_mutation_id", None)
        if "client_mutation_label" in data:
            data.pop("client_mutation_label")

        service = IndicatorValueService(user)
        response = service.create(data)

        if not response.get("success"):
            return response  # géré par middleware openIMIS
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
    def _validate_mutation(cls, user, **data):
        super()._validate_mutation(user, **data)
        _require_permissions(user, MonitoringEvaluationConfig.gql_mutation_indicators_edit_value_perms)

    @classmethod
    def _mutate(cls, user, **data):
        data.pop("client_mutation_id", None)
        if "client_mutation_label" in data:
            data.pop("client_mutation_label")

        service = IndicatorValueService(user)
        response = service.update(data)

        if not response.get("success"):
            return response

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

    @classmethod
    def _validate_mutation(cls, user, **data):
        super()._validate_mutation(user, **data)
        _require_permissions(user, MonitoringEvaluationConfig.gql_mutation_indicators_delete_value_perms)

    @classmethod
    def _mutate(cls, user, **data):
        data = dict(data)
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)
        data['user'] = user
        response = IndicatorValueService(user).delete(data)
        return None if response.get('success') else response


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
    def _validate_mutation(cls, user, **data):
        super()._validate_mutation(user, **data)
        _require_permissions(user, MonitoringEvaluationConfig.gql_mutation_indicators_validate_value_perms)

    @classmethod
    def _mutate(cls, user, **data):
        data.pop("client_mutation_id", None)
        if "client_mutation_label" in data:
            data.pop("client_mutation_label")

        service = IndicatorValueService(user)
        response = service.validate_value(data)

        if not response.get("success"):
            return response

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
        if not user.has_perms(MonitoringEvaluationConfig.gql_mutation_indicators_recalculate_perms):
            raise PermissionDenied(_("Unauthorized"))

        ps = date.fromisoformat(data["period_start"])
        pe = date.fromisoformat(data["period_end"])

        from .indicators_services import calculate_me_indicators_for_period

        count = calculate_me_indicators_for_period(ps, pe, user=user)
        return RecalculateIndicatorsMutation(updated_count=count)

